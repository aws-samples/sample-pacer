//! Binary entrypoint: wires config, cache, backend client, proxy, cluster
//! tier, and the admin/S3 listeners together (see `pacer_daemon` for the parts).

use anyhow::Context;
use std::sync::Arc;

use pacer_daemon::staging::StagingArea;
use pacer_daemon::{
    auth, cachefill, config, coordinate, health, listen, memory_budget, metrics, peer, proxy,
    shutdown,
};
use s3s::service::S3ServiceBuilder;
use tracing::{info, warn};

/// jemalloc on Linux, for the disk tier's read buffers.
///
/// foyer allocates one PAGE-aligned buffer the size of the whole entry per
/// disk-tier read and drops it as soon as the entry is deserialized — 16 MiB per
/// chunk hit, with no buffer pool. glibc satisfies allocations that large with
/// `mmap` and hands them back with `munmap`, so every hit re-faults 16 MiB of
/// fresh pages; jemalloc keeps the span in its arena and reuses it. Bounded
/// upside by construction: `foyer_storage_disk_io_duration` is measured with the
/// buffer already allocated, so this cost lives in the ~2.7 ms that a 45.4 ms hit
/// spends outside the io and the deserialize.
#[cfg(target_os = "linux")]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Filter used when `RUST_LOG` is unset or unparseable. `info` because every line
/// the daemon emits at that level is a state change an operator would want in a
/// post-mortem, and `debug` on the proxy is per-chunk.
const DEFAULT_LOG_FILTER: &str = "info";

/// Install the global subscriber in the configured encoding (ADR-0036).
///
/// Filtering stays `RUST_LOG`'s job — that is `tracing_subscriber`'s own contract,
/// the chart already renders it from `config.logLevel`, and re-spelling it as a
/// `PACER_*` name would create two ways to say one thing.
///
/// `target` and thread identity are off in both encodings: the module path of a
/// log line is not what identifies it (the message and its fields are), and a
/// thread id is noise in a process whose work is spread over a tokio pool by
/// design. Turning them on is a code change, deliberately — they are debugging
/// aids, not fleet telemetry.
fn init_logging(format: config::LogFormat) {
    let builder = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| DEFAULT_LOG_FILTER.into()),
        )
        .with_target(false)
        .with_thread_ids(false)
        .with_thread_names(false);
    match format {
        // `flatten_event` lifts an event's own fields to the top level, so
        // `kubectl logs | jq '.chunk_key'` works instead of `.fields.chunk_key`;
        // the current span comes along as one object while the full ancestor list
        // does not, because nothing here nests spans deeply enough for the array
        // to be worth the width on every line.
        config::LogFormat::Json => builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(false)
            .init(),
        config::LogFormat::Text => builder.init(),
    }
}

fn main() -> anyhow::Result<()> {
    // The encoding is resolved on its own, first: the subscriber has to exist
    // before `Config::load`, which warns about several misconfigurations that
    // would otherwise be dropped on the floor (see `config::log_format`).
    init_logging(config::log_format());

    let cfg = config::Config::load()?;
    // Named fields, never `?cfg`. `Config` holds the ADR-0006 placeholder
    // credentials (`placeholder_access_key` / `placeholder_secret_key`), so a
    // whole-struct `Debug` print puts a field named `*_secret_key` in the log —
    // harmless while those values are the documented placeholders, and a
    // credential leak the moment anything real is substituted. An allowlist makes
    // a newly added sensitive field invisible here by default; `?cfg` would make
    // it logged by default. Nothing whose name contains `secret`, `key` or
    // `password` belongs in this call, whatever its current value.
    info!(
        listen_addr = %cfg.listen_addr,
        admin_addr = %cfg.admin_addr,
        cache_dir = %cfg.cache.dir.display(),
        mem_capacity = cfg.cache.mem_capacity,
        disk_capacity = cfg.cache.disk_capacity,
        chunk_size = cfg.chunk.chunk_size(),
        backend_type = ?cfg.backend.backend_type,
        cluster = cfg.cluster.is_some(),
        delivery = cfg.delivery.enabled,
        // The read-path tuning, logged because a benchmark arm that silently
        // failed to apply one of these is indistinguishable from its own control.
        // `0` means "foyer's default" for each of the three counts.
        block_size = cfg.cache.block_size,
        flushers = cfg.cache.tuning.flushers,
        reclaimers = cfg.cache.tuning.reclaimers,
        submit_queue_threshold = cfg.cache.tuning.submit_queue_threshold,
        storage_runtime_threads = cfg.cache.tuning.storage_runtime_threads,
        promotion = ?cfg.promotion,
        // The ADR-0036 bounds, logged for the same reason as the read-path
        // tuning: a pod whose connection cap or drain deadline is not what the
        // operator set looks exactly like one where it is.
        log_format = %cfg.log_format,
        max_connections = cfg.listen.max_connections,
        s3_header_timeout_secs = cfg.listen.header_timeout.as_secs(),
        s3_idle_timeout_secs = cfg.listen.idle_timeout.as_secs(),
        drain_timeout_secs = cfg.shutdown_drain_timeout.as_secs(),
        "starting pacer-daemon"
    );

    // Quality item R3, and it is HERE — before the runtime, the cache, the arenas and
    // the slab — on purpose: the check's whole value is that nothing has been mapped
    // yet, so a configuration that cannot fit this container's cgroup limit costs a
    // refused pod start instead of an OOM kill that takes a warm cache with it and
    // reads at the client as a truncated body. One call, so the rest of this file
    // stays free for the wiring around it.
    memory_budget::enforce_at_startup(&cfg)?;

    let rt = build_runtime(cfg.worker_threads)?;
    // The RDMA serve path runs on its own runtime so client-role S3-proxy work
    // on the main runtime can never starve completion-pump wakeups or holder
    // WRITE posts (planning/15 all-hammer contention). It is built HERE, in the
    // sync entrypoint, and kept alive for the whole process: dropping a runtime
    // inside another runtime's async context panics, so it must outlive — and
    // be dropped outside — `rt.block_on`.
    #[cfg(feature = "efa")]
    {
        // Only cluster mode brings up the EFA plane; a single-node daemon
        // needs no second runtime and is handed the main handle (never used —
        // no transport is built to spawn onto it).
        if cfg.cluster.is_some() {
            let rdma_rt = build_runtime(cfg.rdma_worker_threads)?;
            let handle = rdma_rt.handle().clone();
            return rt.block_on(run(cfg, handle));
        }
        let handle = rt.handle().clone();
        rt.block_on(run(cfg, handle))
    }
    #[cfg(not(feature = "efa"))]
    {
        let handle = rt.handle().clone();
        rt.block_on(run(cfg, handle))
    }
}

/// Build a multi-thread tokio runtime with `enable_all`. `worker_threads == 0`
/// keeps the tokio default (one worker per core); any positive value pins it.
///
/// # Errors
///
/// [`tokio::runtime::Builder::build`] failing (e.g. the OS refusing to spawn
/// the worker threads).
fn build_runtime(worker_threads: usize) -> std::io::Result<tokio::runtime::Runtime> {
    let mut rt = tokio::runtime::Builder::new_multi_thread();
    rt.enable_all();
    if worker_threads > 0 {
        rt.worker_threads(worker_threads);
    }
    rt.build()
}

/// Run the daemon on the current (main) runtime. `rdma_runtime` is the handle
/// of the dedicated RDMA serve-path runtime the EFA completion pump and holder
/// WRITEs are spawned onto (see `main`); ignored on non-EFA builds and by
/// single-node daemons, which never build an EFA plane.
/// State what the chunk tier is actually running, at startup.
///
/// **The read ceiling and the read shape are here because they are the two settings an arm
/// cannot otherwise confirm engaged.** Every other term of a store arm announces itself from
/// outside — the slab logs its frame count, the index logs its slots, the scrape carries the
/// tier's counters — while a ceiling that silently resolved to `unlimited` would produce an arm
/// that measured its own control and published it as the treatment. That is the most frequent
/// failure in this repo's trap list, so the daemon says which one it got rather than leaving it
/// to be inferred from a service-versus-queue split after the fact.
/// Refuse to serve with `diskTier=store` on a node that has no ADR-0028 slab.
///
/// # Why a refusal and not a warning
///
/// The store's read is `O_DIRECT` into a registered frame. `O_DIRECT` requires a page-aligned
/// buffer; a heap `Vec`'s alignment is its element's, so with no slab
/// [`pacer_cache::store`] abandons the direct descriptor and reads through the **buffered**
/// one on every hit — 26.9-34.9 ms of service time per 16 MiB against 17.3 direct, and
/// through the page cache this tier exists to bypass. That is not a degraded store; it is
/// roughly half the rate of the thing the operator asked for, and it is invisible in every
/// figure except `pacer_cache_slab_heap_fallbacks_total`. A tier that quietly delivers half
/// its measured rate is how this repo has published a control as a treatment before, and the
/// chart's own default is `efa.enabled: false`, so the misconfiguration is one `--set` away.
///
/// The alternative — `foyer` — is *measured*, on the same node, in the same arm
/// (`bench/ladder/results/mountpoint-vs-pacer.md`). Buffered `store` is measured against
/// neither. So there is a right answer here and the operator should pick it knowingly.
///
/// # Why it cannot fire on a healthy node
///
/// [`pacer_cache::frames::has_frame_source`] is false only where no slab was installed at
/// all, which the chart couples to `efa.hugepages` — the same condition `pacer.diskTier`
/// defaults `store` on. Transient frame exhaustion is a different state, keeps its heap
/// fallback, and is deliberately not checked here: a read must still be served.
///
/// # Errors
///
/// When the effective tier is the store and this node installed no frame source.
fn assert_store_has_a_slab(tier: &pacer_cache::tier::ChunkTier) -> anyhow::Result<()> {
    // The two facts, read at the one place that has both. The decision itself is
    // [`store_without_slab_is_fatal`], so its truth table is testable without a real store or
    // a process-wide frame source.
    store_without_slab_is_fatal(tier.has_store(), pacer_cache::frames::has_frame_source())
}

/// The decision behind [`assert_store_has_a_slab`], over the two facts it reads.
///
/// Pure, and separate from its caller for one reason: `frames::has_frame_source` is a
/// process-wide `OnceLock` that cannot be un-installed, so a test driving the real predicate
/// could assert one arm of this table per test BINARY. Here all four are one test.
///
/// # Errors
///
/// When `has_store` and not `has_slab`. Every other combination is fine: no store means the
/// slab is irrelevant to the disk tier, and a slab with foyer is simply the RAM tier doing
/// its ADR-0028 job.
fn store_without_slab_is_fatal(has_store: bool, has_slab: bool) -> anyhow::Result<()> {
    if has_store && !has_slab {
        anyhow::bail!(
            "config.diskTier=store, but this node has NO ADR-0028 cache slab, so every store \
             read would fall back to BUFFERED I/O (26.9-34.9 ms per 16 MiB against 17.3 \
             direct, through the page cache the store exists to bypass) and only \
             pacer_cache_slab_heap_fallbacks_total would say so. Refusing rather than \
             serving half the rate that was asked for. Either give the node a slab — set \
             efa.enabled=true and efa.hugepages (the chart derives the slab from it, and \
             defaults diskTier=store wherever it is set) — or choose the tier that IS \
             measured without one, config.diskTier=foyer. Buffered store is measured \
             against neither."
        );
    }
    Ok(())
}

fn log_cache_ready(cfg: &config::Config, tier: &pacer_cache::tier::ChunkTier) {
    info!(
        dir = %cfg.cache.dir.display(),
        disk_tier = if tier.has_store() { "store" } else { "foyer" },
        verify_chunk_body = cfg.verify_chunk_body,
        store_read_concurrency = if cfg.store_read_concurrency == 0 {
            "unlimited".to_owned()
        } else {
            cfg.store_read_concurrency.to_string()
        },
        store_read_shape = ?cfg.store_read_shape,
        "cache ready"
    );
}

async fn run(
    cfg: config::Config,
    #[cfg_attr(not(feature = "efa"), allow(unused_variables))] rdma_runtime: tokio::runtime::Handle,
) -> anyhow::Result<()> {
    let metrics = metrics::Metrics::new()?;
    // Publish the same terms `main` just checked, so a dashboard can put them beside
    // `pacer_cgroup_memory_max_bytes` and see the margin the startup check measured
    // once. Recomputed rather than threaded down from `main`: it is pure arithmetic
    // over the configuration, and a second argument here would collide with every
    // other change to this signature.
    metrics.set_memory_budget(&memory_budget::pinned_budget(
        &cfg,
        memory_budget::EfaArenas::detect(&cfg),
    ));
    // Resolve the cache directory's backing devices once, so every disk-tier rate this
    // daemon reports has a `/proc/diskstats` delta beside it. Before the cache is built:
    // the answer depends only on the mount, and doing it here means no arm can start
    // serving with the check unwired.
    metrics.set_cache_dir(&cfg.cache.dir);

    // foyer registers its cache metrics into the same Prometheus registry, so
    // /metrics exposes daemon counters and foyer internals together.
    let foyer_registry: mixtrics::metrics::BoxedRegistry = Box::new(
        mixtrics::registry::prometheus_0_14::PrometheusMetricsRegistry::new(
            metrics.registry().clone(),
        ),
    );
    let cache = pacer_cache::build_chunk_cache_with_metrics(&cfg.cache, foyer_registry)
        .await
        .context("building hybrid cache")?;
    let tier = build_chunk_tier(&cfg, cache).await?;
    // Wire the store's atomics to the scrape BEFORE anything can read a chunk, so a
    // gate's numbers cannot be missing their first requests.
    if let Some(store) = tier.store() {
        metrics.set_chunk_store(store.clone());
    }
    log_cache_ready(&cfg, &tier);

    let backend = pacer_backend::build_client(&cfg.backend).await;

    let delivery_quota = build_delivery_quota(&cfg, &metrics);

    let mut proxy = proxy::PacerProxy::new(
        backend.clone(),
        tier.clone(),
        metrics.clone(),
        cfg.min_object_size,
        cfg.max_object_size,
        cfg.chunk,
        cfg.fill_parallelism,
    )
    // Client-facing bucket aliases (ADR-0002); an empty map is every bucket
    // resolving to itself.
    .with_bucket_map(cfg.bucket_map.clone())
    // Gate Express-only write-path normalization on the configured backend
    // shape (ADR-0023); defaults to Express when the knob is unset.
    .with_backend_type(cfg.backend.backend_type)
    .with_delivery(cfg.delivery.clone(), delivery_quota)
    // ADR-0039: whether an If-Match GET may reach the cache at all.
    .with_conditional_get_from_cache(cfg.conditional_get_from_cache)
    // ADR-0040: whether concurrent readers of one cold chunk share its backend read.
    .with_fill_coalesce(cfg.fill_coalesce);

    // Cluster tier (Phase 2): membership → SharedRing; peer gRPC server;
    // ring-aware miss path in the proxy (ADR-0012). Phase 3: an EFA
    // maintenance sweep task, only when the `efa` feature is built in AND this
    // node's capability probe succeeds (ADR-0018).
    // Raised by SIGTERM/SIGINT; watched by both listeners so a rolling update
    // drains instead of severing in-flight requests (ADR-0036).
    let shutdown = shutdown::Shutdown::new();
    let mut tasks = ClusterTasks::default();
    if let Some(cluster_cfg) = &cfg.cluster {
        (proxy, tasks) = enable_cluster(
            &cfg,
            cluster_cfg,
            proxy,
            (tier.clone(), backend),
            &metrics,
            &rdma_runtime,
            shutdown.signal(),
        )?;
    }

    // AFTER the cluster block, because that is where the slab is installed
    // (`install_promotion_frames`), and BEFORE anything binds: a node that cannot honour the
    // tier it was configured with must fail to start, not serve half the rate at it.
    assert_store_has_a_slab(&tier)?;

    let s3_service = build_s3_service(proxy, &cfg);
    let admin = tokio::spawn(health::serve(cfg.admin_addr.clone(), metrics.clone()));
    let mut s3 = tokio::spawn(listen::serve_s3(
        cfg.listen_addr.clone(),
        s3_service,
        cfg.listen,
        metrics.clone(),
        shutdown.signal(),
    ));

    info!(admin = %cfg.admin_addr, s3 = %cfg.listen_addr, "pacer-daemon up");
    // A shutdown signal is the ONE orderly exit. Any server task ending on its
    // own is still fatal: the pod restarts whole (kubelet), never limps along
    // without its peer plane or membership watch.
    tokio::select! {
        _ = shutdown.on_signal() => {}
        r = admin => r??,
        r = &mut s3 => r??,
        r = async { tasks.peer.take().unwrap().await }, if tasks.peer.is_some() => r??,
        r = async { tasks.membership.take().unwrap().await }, if tasks.membership.is_some() => r??,
        r = async { tasks.efa_maintenance.take().unwrap().await }, if tasks.efa_maintenance.is_some() => r?,
        r = async { tasks.reannounce.take().unwrap().await }, if tasks.reannounce.is_some() => r?,
        r = async { tasks.reap.take().unwrap().await }, if tasks.reap.is_some() => r?,
    }
    drain(cfg.shutdown_drain_timeout, s3, tasks, &tier, &metrics).await;
    Ok(())
}

/// Build the client-memory delivery quota (ADR-0026) and wire it into `/metrics`.
///
/// Built once and shared with the metrics layer, so the gauge and the admission
/// decision cannot drift apart. Extracted from [`run`] for length only.
fn build_delivery_quota(
    cfg: &config::Config,
    metrics: &metrics::Metrics,
) -> Arc<pacer_daemon::delivery::DeliveryQuota> {
    let quota = Arc::new(pacer_daemon::delivery::DeliveryQuota::new(
        cfg.delivery.pinned_bytes_max,
        cfg.delivery.max_target_bytes,
    ));
    metrics.set_delivery_quota(Arc::clone(&quota));
    if cfg.delivery.enabled {
        info!(
            shm_dir = %cfg.delivery.shm_dir.display(),
            max_target_bytes = cfg.delivery.max_target_bytes,
            pinned_bytes_max = cfg.delivery.pinned_bytes_max,
            "client-memory delivery enabled (ADR-0026)"
        );
    }
    quota
}

/// Let in-flight work finish, then stop (ADR-0036).
///
/// Order matters and is the whole content of this function:
///
/// 1. The two servers were handed the shutdown signal, so by now they have
///    stopped accepting. Awaiting their tasks awaits their *watched connections*
///    — the requests a client is mid-way through.
/// 2. The background sweeps are aborted rather than awaited. Each is an infinite
///    `interval` loop (re-announce, EFA maintenance, staged-chunk reap,
///    membership watch) holding nothing a client is waiting on, so awaiting them
///    would simply burn the whole deadline.
/// 3. The cache is closed LAST, after nothing can still be writing to it, so
///    foyer's in-flight flush and reclaim tasks are the only thing left to wait
///    for.
///
/// Never returns an error: every step is best-effort by construction, and a drain
/// that failed still has to exit 0 — a non-zero exit during a rolling update is
/// reported as a crash-looping pod, which is a worse signal than a logged warning
/// about a slow flush.
///
/// The EFA completion reapers are NOT stopped here. Since planning/19 D5 each rail
/// owns a pinned thread with its own current-thread runtime and no cancellation
/// handle, and giving it one means changing `pacer-transport`; they hold no client
/// work (a holder's WRITE completes or its requester falls back), so process exit
/// is their teardown. Named here so the omission is a decision and not an oversight.
async fn drain(
    timeout: std::time::Duration,
    s3: tokio::task::JoinHandle<anyhow::Result<()>>,
    tasks: ClusterTasks,
    tier: &pacer_cache::tier::ChunkTier,
    metrics: &metrics::Metrics,
) {
    let ClusterTasks {
        peer,
        membership,
        efa_maintenance,
        reannounce,
        reap,
    } = tasks;
    info!(
        drain_timeout_secs = timeout.as_secs(),
        s3_connections = metrics.listener.connections_active.get(),
        peer_plane = peer.is_some(),
        "drain started"
    );
    let listeners_drained = shutdown::drain_listeners(timeout, s3, peer).await;
    if let Some(membership) = membership {
        membership.abort();
    }
    for sweep in [efa_maintenance, reannounce, reap].into_iter().flatten() {
        sweep.abort();
    }
    if let Err(e) = tier.cache().close().await {
        warn!(error = %e, "closing the cache tier during drain");
    }
    info!(
        listeners_drained,
        s3_connections = metrics.listener.connections_active.get(),
        "drain complete"
    );
}

/// Peer gRPC server task (cluster mode only).
type PeerTask = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;
/// EndpointSlice membership watch task (K8s membership only).
type MembershipTask = tokio::task::JoinHandle<anyhow::Result<()>>;
/// EFA maintenance sweep task — peer handshakes plus client-edge expiry (cluster
/// mode, `efa` feature, probe succeeded).
type EfaMaintenanceTask = tokio::task::JoinHandle<()>;
/// Directory re-announce healer task (cluster mode): re-asserts this node's
/// held/admitted chunks to their homes on a fixed interval (ADR-0017).
type ReannounceTask = tokio::task::JoinHandle<()>;
/// Staged-chunk reaper task (cluster mode, scatter enabled): returns budget held
/// by an upload whose coordinator died before committing or aborting (ADR-0032).
/// Joined with the rest so its disappearance restarts the pod rather than leaving
/// a node that silently refuses every later write.
type ReapTask = tokio::task::JoinHandle<()>;

/// The background tasks cluster mode starts.
///
/// A struct rather than a tuple because the list grows: at six positional `Option`s
/// it had pushed both [`run`] and [`enable_cluster`] past the line budget, and a
/// caller destructuring them could not tell one from another. Every field is
/// `Option` because a task exists only when its feature does — no membership watch
/// with static peers, no handshake sweep without EFA, no reaper without the scatter.
#[derive(Default)]
struct ClusterTasks {
    /// Peer gRPC server. The one that always exists in cluster mode.
    peer: Option<PeerTask>,
    /// EndpointSlice membership watch (K8s membership only).
    membership: Option<MembershipTask>,
    /// EFA maintenance sweep (`efa` feature, probe succeeded).
    efa_maintenance: Option<EfaMaintenanceTask>,
    /// Directory re-announce healer.
    reannounce: Option<ReannounceTask>,
    /// Staged-chunk reaper (scatter enabled).
    reap: Option<ReapTask>,
}

/// File holding this node's admission-generation high-water mark (ADR-0017 R4),
/// a sibling of the foyer cache blocks. Reloaded at startup so a restarted pod
/// on the same K8s node vends generations ABOVE its prior instance's, instead
/// of resetting to 0 and having every re-admit dropped as stale at the home.
/// Lives in the cache dir because foyer reuses that dir across restarts (never
/// wipes it), so the mark survives exactly as long as the chunks it describes.
const ADMISSION_GENERATION_FILE: &str = "pacer-admission-generation";

/// Build the layer-1 admission gate (ADR-0016). Its byte budget is a fraction
/// of the hybrid cache's total capacity (RAM + disk), so storm heat cannot
/// admit more than that share of peer-owned copies per window. The generation
/// store resumes the admission generation above the persisted high-water mark
/// (R4, ADR-0017) so a restarted pod on the same node never re-vends a
/// generation the home already recorded — which the fold would drop as stale,
/// leaving this holder invisible.
fn build_admission_gate(
    cfg: &config::Config,
    cluster_cfg: &config::ClusterConfig,
) -> pacer_cache::admission::AdmissionGate {
    let cache_capacity = (cfg.cache.mem_capacity + cfg.cache.disk_capacity) as u64;
    pacer_cache::admission::AdmissionGate::new(
        cluster_cfg.local_admission_threshold,
        cluster_cfg.local_admission_window,
        cluster_cfg.local_copy_capacity_fraction,
        cache_capacity,
        std::time::Instant::now(),
    )
    .with_generation_store(cfg.cache.dir.join(ADMISSION_GENERATION_FILE))
}

/// Seed the ring from the configured membership source (ADR-0012): a static
/// peer list loads once, while a K8s EndpointSlice source spawns a watch task
/// that keeps the ring live. Returns the watch task (if any) for shutdown.
fn setup_membership(
    cluster_cfg: &config::ClusterConfig,
    ring: &pacer_ring::SharedRing,
    metrics: &metrics::Metrics,
) -> Option<MembershipTask> {
    match &cluster_cfg.membership {
        config::Membership::Static { peers } => {
            info!(peers = peers.len(), "static membership");
            metrics.ring_members.set(peers.len() as i64);
            ring.store(peers.clone());
            None
        }
        config::Membership::K8s { namespace, service } => {
            info!(%namespace, %service, "endpointslice membership watch");
            Some(tokio::spawn(watch_membership(
                ring.clone(),
                namespace.clone(),
                service.clone(),
                metrics.clone(),
            )))
        }
    }
}

/// Wire the cluster tier (ADR-0012): membership source → SharedRing, peer
/// gRPC server, and the ring-aware miss path on the proxy.
/// Build the tier chunk bodies live in, per `config.diskTier` (ADR-0033).
///
/// The store's `open` **scans every slot header** to rebuild its index, which is what
/// makes a restart keep the tier — so this is where that cost is paid, once, before the
/// daemon serves. It is small by design (4 KiB reads at ~82 µs, 256 in flight), and the
/// store logs what it found.
///
/// # Errors
///
/// Creating or opening the store's extent files, or a scan read failing. On the `foyer`
/// tier this cannot fail — there is nothing to open that `build_chunk_cache_with_metrics`
/// has not already opened.
async fn build_chunk_tier(
    cfg: &config::Config,
    cache: pacer_cache::ChunkCache,
) -> anyhow::Result<pacer_cache::tier::ChunkTier> {
    if cfg.disk_tier == pacer_cache::DiskTier::Foyer {
        return Ok(pacer_cache::tier::ChunkTier::foyer(cache, cfg.promotion));
    }
    // Its own subdirectory: foyer owns files directly under `cache.dir` and a shared
    // directory would make "whose file is this?" a question during recovery.
    let store = pacer_cache::store::ChunkStore::open(pacer_cache::store::StoreConfig {
        dir: cfg.cache.dir.join(CHUNK_STORE_SUBDIR),
        chunk_size: usize::try_from(cfg.chunk.chunk_size())
            .context("chunk size does not fit this platform's usize")?,
        capacity_bytes: cfg.cache.disk_capacity as u64,
        verify_body: cfg.verify_chunk_body,
        read_shape: cfg.store_read_shape,
        read_concurrency: cfg.store_read_concurrency,
    })
    .await
    .context("opening the ADR-0033 chunk store")?;
    Ok(pacer_cache::tier::ChunkTier::with_store(
        cache,
        store,
        cfg.promotion,
    ))
}

/// Subdirectory of the cache dir holding the ADR-0033 store's extent files, kept apart
/// from foyer's own files so recovery never has to guess which layer owns a file.
const CHUNK_STORE_SUBDIR: &str = "chunk-store";

/// `tier` and `backend` arrive as one pair rather than two arguments to stay
/// inside the argument budget once the shutdown signal joined the list; they are
/// already paired that way by [`build_peer_server`], which is where both end up.
#[allow(clippy::type_complexity)]
fn enable_cluster(
    cfg: &config::Config,
    cluster_cfg: &config::ClusterConfig,
    proxy: proxy::PacerProxy,
    (tier, backend): (pacer_cache::tier::ChunkTier, aws_sdk_s3::Client),
    metrics: &metrics::Metrics,
    #[cfg_attr(not(feature = "efa"), allow(unused_variables))]
    rdma_runtime: &tokio::runtime::Handle,
    shutdown: shutdown::ShutdownSignal,
) -> anyhow::Result<(proxy::PacerProxy, ClusterTasks)> {
    let ring = pacer_ring::SharedRing::default();
    let membership_task = setup_membership(cluster_cfg, &ring, metrics);
    // The same windows the peer server advertises, applied to what this node dials — so the
    // knob is symmetric and one value describes the node rather than one direction of it.
    // Here they govern what this node RECEIVES, i.e. `FetchBlob` bodies on the restore path.
    let grpc = pacer_transport::grpc::GrpcTransport::new(
        cluster_cfg.peer_port,
        cluster_cfg.node_name.clone(),
    )
    .with_h2_windows(cluster_cfg.h2_windows)
    .with_connections_per_peer(cluster_cfg.peer_connections);
    // This node's directory shard (ADR-0017): sharer sets for the chunk keys
    // the ring homes here. Soft state — starts empty on every daemon start,
    // repopulates from read-through admits and peer re-announcements.
    let directory = pacer_ring::directory::SharedDirectory::new(cluster_cfg.max_sharers_tracked);
    let admission = std::sync::Arc::new(build_admission_gate(cfg, cluster_cfg));
    // Wrap gRPC in the EFA-RDMA transport when the `efa` feature is built AND
    // this node's capability probe succeeds (ADR-0018); otherwise gRPC-only.
    #[cfg(feature = "efa")]
    let (transport, efa) =
        select_transport(grpc, cluster_cfg, arena_config(cfg, cluster_cfg), metrics);
    #[cfg(not(feature = "efa"))]
    let transport: std::sync::Arc<dyn pacer_transport::PeerTransport> = std::sync::Arc::new(grpc);
    // ADR-0017 soft-state healer: periodically re-assert the chunks this node
    // holds/admitted to their homes so a restarted home's directory reconverges
    // without waiting on organic re-reads. Spawned from clones captured before
    // the transport/directory/admission handles move into the cluster below;
    // gated to cluster mode by construction (this whole fn runs only with a
    // cluster config present).
    let reannounce_task = Some(spawn_reannouncer(
        cluster_cfg,
        &ring,
        &directory,
        &transport,
        &admission,
    ));
    // ADR-0028: one fill policy for the whole node. Built here because this is
    // where the slab exists, and handed to the proxy AND the peer server —
    // a node where only one of them uses the slab is the silent-no-op failure
    // `cachefill`'s module doc describes.
    #[cfg(feature = "efa")]
    let chunk_fill = {
        let slab = efa.as_ref().and_then(|e| e.cache_slab()).cloned();
        // The fourth filler — foyer's own decoder, on a disk-tier promotion —
        // reaches the slab through a process-wide seam instead of this value,
        // because it has no `self` to hold one. Same slab, installed together so
        // the two cannot disagree about whether this node has one.
        cachefill::install_promotion_frames(slab.clone(), metrics);
        cachefill::ChunkFill::new(slab)
    };
    #[cfg(not(feature = "efa"))]
    let chunk_fill = cachefill::ChunkFill::default();
    // Publish the slab's size before anything can be cached: it is resident from
    // startup, so any memory accounting that reads /metrics needs it from the
    // first scrape (see `ChunkFill::publish_slab_size`).
    chunk_fill.publish_slab_size(metrics);
    let cluster = proxy::Cluster {
        ring: ring.clone(),
        directory: directory.clone(),
        transport,
        local_node: cluster_cfg.node_name.clone(),
        channel_capacity: cluster_cfg.channel_capacity,
        replication_r: cluster_cfg.replication_r,
        admission,
        // ADR-0026's delivery path needs the concrete transport (it registers
        // client memory and offers it to holders), which the trait object cannot
        // express; `None` here just means deliveries land through the daemon.
        #[cfg(feature = "efa")]
        efa: efa.clone(),
    };
    let staging = scatter_staging(&cfg.scatter);
    let proxy = attach_scatter(
        proxy
            .with_chunk_fill(chunk_fill.clone())
            .with_promotion(cfg.promotion)
            .with_cluster(cluster.clone()),
        cluster,
        staging.clone(),
        cfg,
        (backend.clone(), tier.clone()),
        metrics,
    );
    let peer_server = build_peer_server(
        cfg,
        cluster_cfg,
        (tier, backend),
        (ring.clone(), directory),
        metrics,
        (proxy.filling(), chunk_fill),
        #[cfg(feature = "efa")]
        (efa.clone(), rdma_runtime),
    );
    let (peer_server, staging_reaper) = attach_staging(peer_server, staging, &cfg.scatter);
    let peer_task = spawn_peer_server(peer_server, cluster_cfg, shutdown)?;
    #[cfg(feature = "efa")]
    let efa_maintenance_task = efa.map(|efa| tokio::spawn(drive_efa_maintenance(efa, ring)));
    #[cfg(not(feature = "efa"))]
    let efa_maintenance_task: Option<tokio::task::JoinHandle<()>> = None;
    Ok((
        proxy,
        ClusterTasks {
            peer: Some(peer_task),
            membership: membership_task,
            efa_maintenance: efa_maintenance_task,
            reannounce: reannounce_task,
            reap: staging_reaper,
        },
    ))
}

/// Choose the peer transport (ADR-0018): wrap `grpc` in the EFA-RDMA transport
/// when this node's capability probe succeeds, else fall through to gRPC-only.
/// On the RDMA path, also wire the transport into `metrics` so /metrics can
/// publish its copy-timers (planning/15 B4). Returns the boxed transport plus
/// the concrete EFA handle (the caller still needs it for the handshake sweep
/// and the peer server). Takes no runtime handle: since planning/19 D5 each rail's
/// completion reaper owns a pinned thread and its own current-thread runtime, so
/// there is nothing left to place it on. `rdma_runtime` still exists — the peer
/// server spawns holder-side serves onto it (see `enable_cluster`).
#[cfg(feature = "efa")]
fn select_transport(
    grpc: pacer_transport::grpc::GrpcTransport,
    cluster_cfg: &config::ClusterConfig,
    arena_cfg: pacer_transport::efa::ArenaConfig,
    metrics: &metrics::Metrics,
) -> (
    std::sync::Arc<dyn pacer_transport::PeerTransport>,
    Option<std::sync::Arc<pacer_transport::efa::EfaRdmaTransport>>,
) {
    match build_efa_transport(grpc, cluster_cfg, arena_cfg) {
        Ok(efa) => {
            metrics.set_efa_transport(std::sync::Arc::clone(&efa));
            (std::sync::Arc::clone(&efa) as _, Some(efa))
        }
        Err(grpc) => (std::sync::Arc::new(grpc) as _, None),
    }
}

/// Resolve the RDMA-buffer knobs into the geometry the transport registers
/// (ADR-0024): a node-wide requester arena in bytes, the cache `chunk_size`
/// every range is cut to (so ranges hold exactly one chunk — the coupling the
/// ADR relies on), and the page size to map with. The single place the operator's
/// knobs meet the arena, so the arithmetic behind a run's pinned memory is
/// readable in one function rather than spread across the startup path.
#[cfg(feature = "efa")]
fn arena_config(
    cfg: &config::Config,
    cluster_cfg: &config::ClusterConfig,
) -> pacer_transport::efa::ArenaConfig {
    pacer_transport::efa::ArenaConfig {
        requester_bytes: if cluster_cfg.rdma_arena_bytes == 0 {
            pacer_transport::efa::DEFAULT_REQUESTER_ARENA_BYTES
        } else {
            cluster_cfg.rdma_arena_bytes
        },
        chunk_size: cfg.chunk.chunk_size() as usize,
        pages: pacer_transport::efa::ArenaPages::from_mib(cluster_cfg.rdma_arena_page_mib),
        rail_window: cluster_cfg.rdma_rail_window,
        slab_bytes: slab_bytes(cfg, cluster_cfg),
        // Zero, i.e. a frame is exactly one chunk. ADR-0033's store briefly read a slot's
        // header and body in ONE direct I/O and needed the chunk plus that header; the arm
        // measured that at +2.7 % (noise) while forcing a stride that misaligned every body
        // against the RAID0 stripe, so the headers moved into a per-extent region and the body
        // read is a whole chunk again. The knob stays — ADR-0028's amendment documents it, and
        // `0` reproduces the original geometry exactly.
        slab_frame_headroom: 0,
    }
}

/// ADR-0028's slab size, with the one misconfiguration that silently disables the
/// design called out at startup.
///
/// A frame is occupied by *everything* holding the chunk: foyer's resident set
/// plus every chunk in flight to a client or a peer. A slab sized at or below
/// `mem_capacity` therefore runs out of frames as soon as the cache fills, every
/// later fill quietly takes the heap instead, and the node ends up paying for
/// pinned memory while still staging every serve — with throughput that looks
/// exactly like the pre-slab numbers. That is a measurement trap, not just a
/// waste, so it warns loudly here and is counted at runtime
/// (`pacer_cache_slab_heap_fallbacks_total`).
///
/// Not an error: the operator may be deliberately testing a small slab, and
/// refusing to boot over a sizing choice would be worse than saying so.
#[cfg(feature = "efa")]
fn slab_bytes(cfg: &config::Config, cluster_cfg: &config::ClusterConfig) -> usize {
    let slab = cluster_cfg.cache_slab_bytes;
    let mem = cfg.cache.mem_capacity;
    if slab > 0 && slab <= mem {
        tracing::warn!(
            slab_bytes = slab,
            mem_capacity = mem,
            "PACER_CACHE_SLAB_BYTES is not larger than the cache's RAM capacity: once the cache \
             fills, in-flight chunks leave no free frame and fills fall back to the heap — the \
             ADR-0028 serve path then stages copies as before. Size the slab above \
             PACER_MEM_CAPACITY by the node's concurrent in-flight chunk count, and watch \
             pacer_cache_slab_heap_fallbacks_total"
        );
    }
    slab
}

/// How often every ring member is (re-)handshaken for its EFA endpoint
/// (ADR-0019 bidirectional exchange), and — on the same task — how often idle
/// client-edge records are expired. Membership is small and stable
/// (DaemonSet-sized), so a plain periodic sweep is simpler than reacting to
/// each ring change — a new member is picked up within one interval, and a
/// peer that already has a cached AH just gets a harmless repeat handshake
/// (`AhCache::get_or_insert` is a no-op on an existing entry).
///
/// It also sets how long a departed client's records outlive their TTL: up to one
/// interval past it, which is well inside "bounded" and far below the cost of a
/// second timer for the same `Arc`.
#[cfg(feature = "efa")]
const HANDSHAKE_SWEEP_INTERVAL: std::time::Duration = std::time::Duration::from_secs(30);

/// The EFA plane's periodic maintenance: peer handshakes, then client-edge expiry.
///
/// **Handshakes.** Handshake every current ring member so both directions of the
/// EFA endpoint exchange happen without waiting on a peer to fetch from or
/// handshake with us first (ADR-0018 finding 6: the AH must exist before EITHER
/// side's first WRITE). A peer that never responds simply keeps missing the RDMA
/// path on every retry.
///
/// **Client-edge expiry.** [`EfaRdmaTransport::sweep_expired_clients`] is the
/// periodic caller its own docs say it is owed. The client registries already
/// sweep themselves when a new endpoint is inserted, i.e. when room is needed, so
/// what is left uncovered is the opposite case: a node whose clients have all gone
/// home inserts nothing, and its last records stay resident until the next client
/// arrives. On the same task and the same cadence as the handshakes rather than a
/// task of its own — it is the same `Arc`, the same period, and one task means one
/// thing for the drain to cancel instead of two.
///
/// Runs for the daemon's lifetime and never returns, so it never shows "done" in
/// the main `select!`; the drain aborts it (see [`drain`]).
#[cfg(feature = "efa")]
async fn drive_efa_maintenance(
    efa: std::sync::Arc<pacer_transport::efa::EfaRdmaTransport>,
    ring: pacer_ring::SharedRing,
) {
    loop {
        for member in ring.load().members() {
            if let Err(e) = efa.initiate_handshake(member).await {
                tracing::debug!(peer = %member.name(), error = %e, "EFA handshake failed; will retry next sweep");
            }
        }
        let expired = efa.sweep_expired_clients().await;
        if expired > 0 {
            tracing::debug!(
                expired,
                "released idle client-edge records (no client has addressed them within the TTL)"
            );
        }
        tokio::time::sleep(HANDSHAKE_SWEEP_INTERVAL).await;
    }
}

/// Bring up the EFA plane (ADR-0018's capability probe gate): if this node
/// can create and activate an SRD queue pair, wrap `grpc` in an
/// [`pacer_transport::efa::EfaRdmaTransport`]; otherwise hand `grpc` straight
/// back so the caller falls through to gRPC-only, exactly as if the `efa`
/// feature had never been compiled in. Never panics/errors the daemon on a
/// failed probe — a node without EFA attached, or whose device plugin
/// didn't mount `/dev/infiniband`, is a normal fleet member, just gRPC-only.
#[cfg(feature = "efa")]
fn build_efa_transport(
    grpc: pacer_transport::grpc::GrpcTransport,
    cluster_cfg: &config::ClusterConfig,
    arena_cfg: pacer_transport::efa::ArenaConfig,
) -> Result<
    std::sync::Arc<pacer_transport::efa::EfaRdmaTransport>,
    pacer_transport::grpc::GrpcTransport,
> {
    // `bring_up_rails` IS the capability probe (ADR-0018/ADR-0021: a passing
    // SRD QP create+activate on an EFA device is the hardware-not-emulated
    // result) — rail 0 failing means gRPC-only, later rails degrade to the
    // ones below them (A5 multi-rail). The completion pumps it spawns land on
    // the dedicated RDMA runtime, not the main one.
    // Each rail's completion reaper now runs on its own pinned thread rather than
    // as a task on `rdma_runtime` (planning/19 D5 step 0), so bring-up takes a
    // placement policy instead of a runtime handle. `rdma_runtime` is still the
    // runtime the peer server spawns holder-side serves onto — see `peer.rs`.
    let rails = match pacer_transport::efa::EfaContext::bring_up_rails(
        cluster_cfg.efa_rails,
        cluster_cfg.efa_qps_per_rail,
        if cluster_cfg.rdma_affinity {
            pacer_transport::efa::RailPlacementPolicy::NumaLocal
        } else {
            pacer_transport::efa::RailPlacementPolicy::Unpinned
        },
    ) {
        Ok(rails) => rails
            .into_iter()
            .map(std::sync::Arc::new)
            .collect::<Vec<_>>(),
        Err(e) => {
            // `{e:#}` — the whole chain. The outer context alone ("opening the RDMA device
            // context") named the step and hid the errno, which is the only part that says
            // whether to fix the pod spec or the node (2026-08-24: EPERM, device cgroup).
            info!(error = %format!("{e:#}"), "EFA capability probe failed; this node speaks gRPC only");
            return Err(grpc);
        }
    };
    info!(
        node = %cluster_cfg.node_name,
        rails = rails.len(),
        "EFA plane up; RDMA WRITE data path enabled"
    );
    Ok(std::sync::Arc::new(
        pacer_transport::efa::EfaRdmaTransport::new(
            rails,
            grpc,
            cluster_cfg.node_name.clone(),
            arena_cfg,
        ),
    ))
}

/// How often the ring-size gauge samples the current member set.
const RING_GAUGE_SAMPLE_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);

/// Run the EndpointSlice watch; the gauge tracks epochs via a lightweight
/// sampler (the watch itself owns the ring).
async fn watch_membership(
    ring: pacer_ring::SharedRing,
    namespace: String,
    service: String,
    metrics: metrics::Metrics,
) -> anyhow::Result<()> {
    let sampler = {
        let ring = ring.clone();
        async move {
            loop {
                metrics.ring_members.set(ring.load().members().len() as i64);
                tokio::time::sleep(RING_GAUGE_SAMPLE_INTERVAL).await;
            }
        }
    };
    tokio::select! {
        r = pacer_ring::membership::watch_endpoint_slices(ring, namespace, service) => r,
        _ = sampler => unreachable!("sampler never returns"),
    }
}

/// Spawn the directory re-announce healer (ADR-0017) for this cluster node.
/// The `ring`/`directory`/`transport`/`admission` handles are cloned here,
/// before the originals move into the cluster, so the sweep observes the same
/// live state the proxy and peer server do.
fn spawn_reannouncer(
    cluster_cfg: &config::ClusterConfig,
    ring: &pacer_ring::SharedRing,
    directory: &pacer_ring::directory::SharedDirectory,
    transport: &std::sync::Arc<dyn pacer_transport::PeerTransport>,
    admission: &std::sync::Arc<pacer_cache::admission::AdmissionGate>,
) -> ReannounceTask {
    tokio::spawn(
        Reannouncer {
            ring: ring.clone(),
            directory: directory.clone(),
            transport: std::sync::Arc::clone(transport),
            admission: std::sync::Arc::clone(admission),
            local_node: cluster_cfg.node_name.clone(),
            interval: cluster_cfg.reannounce_interval,
        }
        .run(),
    )
}

/// Background healer that periodically re-announces the chunks this node holds
/// or has admitted to their directory homes (ADR-0017 soft state). The
/// directory has no persistence: a home that restarts loses its shard, and the
/// layer-1 admits (ADR-0016) and co-home fills that populated it stay invisible
/// until their holders re-assert them. This is the directory analogue of
/// [`drive_efa_maintenance`] — a plain periodic sweep, simpler than reacting to
/// each membership change, that converges a restarted home within one interval.
struct Reannouncer {
    /// Live ownership view: the home of a chunk key is its [`SharedRing::owner`].
    ring: pacer_ring::SharedRing,
    /// This node's own directory shard — the source for chunks it homes/co-homes
    /// and filled, and the sink for the idempotent self re-assert.
    directory: pacer_ring::directory::SharedDirectory,
    /// Control plane for announcing to a remote home (always gRPC, ADR-0017).
    transport: std::sync::Arc<dyn pacer_transport::PeerTransport>,
    /// Layer-1 admission record — the source for peer copies this node keeps,
    /// carrying the restart-monotonic generation (R4).
    admission: std::sync::Arc<pacer_cache::admission::AdmissionGate>,
    /// This node's stable name (matches `NodeId::name`).
    local_node: String,
    /// Sweep period (see `PACER_REANNOUNCE_INTERVAL_SECS`).
    interval: std::time::Duration,
}

/// The disposition of one chunk's re-announce in a sweep (tallied for the log).
enum ReannounceOutcome {
    /// Home is this node → idempotent local directory write, no RPC.
    Local,
    /// Home is a peer and the announce RPC succeeded.
    RemoteOk,
    /// Home is a peer and the announce RPC failed (retried next sweep).
    RemoteErr,
    /// The ring has no owner yet (membership not converged) — nothing to do.
    NotConverged,
}

impl Reannouncer {
    /// Run the sweep forever, once per `interval`. Never returns (so it never
    /// shows "done" in the main `select!`); a failed announce is logged and
    /// retried on the next tick (ADR-0017 soft state).
    async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        // Skip the immediate first tick: at startup both holding sources are
        // empty soft state, so the first useful sweep is one interval out.
        ticker.tick().await;
        loop {
            ticker.tick().await;
            self.sweep().await;
        }
    }

    /// One pass over this node's holdings, re-asserting each to its home and
    /// logging the tallied outcome.
    async fn sweep(&self) {
        let holdings = self.holdings();
        let total = holdings.len();
        let (mut local, mut remote_ok, mut remote_err) = (0usize, 0usize, 0usize);
        for (chunk_key, tier, generation) in holdings {
            match self.reannounce_one(&chunk_key, tier, generation).await {
                ReannounceOutcome::Local => local += 1,
                ReannounceOutcome::RemoteOk => remote_ok += 1,
                ReannounceOutcome::RemoteErr => remote_err += 1,
                ReannounceOutcome::NotConverged => {}
            }
        }
        info!(
            total,
            local, remote_ok, remote_err, "directory re-announce sweep complete"
        );
    }

    /// Re-assert one chunk to its home: a local idempotent write when this node
    /// is the home, else a fire-and-forget announce over the transport.
    async fn reannounce_one(
        &self,
        chunk_key: &str,
        tier: pacer_ring::directory::Tier,
        generation: u64,
    ) -> ReannounceOutcome {
        let Some(home) = self.ring.owner(chunk_key) else {
            return ReannounceOutcome::NotConverged;
        };
        if home.name() == self.local_node.as_str() {
            self.directory
                .admit(chunk_key, &self.local_node, tier, generation);
            return ReannounceOutcome::Local;
        }
        match self
            .transport
            .announce_admit(&home, chunk_key, &self.local_node, tier, generation)
            .await
        {
            Ok(()) => ReannounceOutcome::RemoteOk,
            Err(e) => {
                tracing::debug!(key = %chunk_key, home = %home.name(), error = %e,
                    "re-announce failed; will retry next sweep");
                ReannounceOutcome::RemoteErr
            }
        }
    }

    /// This node's holdings, merged and de-duplicated from its two soft-state
    /// sources: the directory shard (chunks it homes/co-homes and filled, with
    /// their tier hint) and the layer-1 admission record (peer copies, DRAM-tier
    /// hint, carrying the R4 restart-monotonic generation). Both are bounded, so
    /// the result is bounded. On the rare key present in both (a ring change
    /// flipped its home between the two reads) the admission generation wins —
    /// it is the persisted, restart-monotonic one.
    fn holdings(&self) -> Vec<(String, pacer_ring::directory::Tier, u64)> {
        use pacer_ring::directory::Tier;
        use std::collections::HashMap;
        let mut merged: HashMap<String, (Tier, u64)> = HashMap::new();
        for (key, tier, generation) in self.directory.holdings_of(&self.local_node) {
            merged.insert(key, (tier, generation));
        }
        for (key, generation) in self.admission.admitted_holdings() {
            merged.insert(key, (Tier::Dram, generation));
        }
        merged
            .into_iter()
            .map(|(key, (tier, generation))| (key, tier, generation))
            .collect()
    }
}

/// The S3 front end, with the placeholder auth every request is re-signed past
/// (ADR-0006: the client's signature is stripped, this node's identity replaces
/// it, so the credentials here are accepted-and-discarded rather than trusted).
///
/// No custom route: ADR-0030's pre-flight endpoint query is a request header on an
/// ordinary `GetObject` (`pacer_daemon::preflight`), so it inherits this auth, this
/// signing and this routing rather than needing any of its own.
fn build_s3_service(proxy: proxy::PacerProxy, cfg: &config::Config) -> s3s::service::S3Service {
    let mut b = S3ServiceBuilder::new(proxy);
    b.set_auth(auth::PlaceholderAuth::new(
        cfg.placeholder_access_key.clone(),
        cfg.placeholder_secret_key.clone(),
    ));
    b.build()
}

/// Start the peer gRPC server on this node's peer address.
///
/// `serve_with_shutdown` rather than `serve` (ADR-0036): the peer plane carries a
/// requester's chunk fetches, so a node that stops answering them mid-stream turns
/// one pod's rolling update into a fallback storm on every node reading from it.
/// tonic stops accepting on the signal and lets started RPCs finish, which is the
/// same contract the S3 listener drains under.
///
/// # Errors
///
/// An unparseable `peer_listen_addr`, which is a config error worth failing
/// startup for rather than serving on a default nobody configured.
fn spawn_peer_server(
    peer_server: peer::PacerPeer,
    cluster_cfg: &config::ClusterConfig,
    shutdown: shutdown::ShutdownSignal,
) -> anyhow::Result<PeerTask> {
    let peer_addr: std::net::SocketAddr = cluster_cfg.peer_listen_addr.parse()?;
    let windows = cluster_cfg.h2_windows;
    info!(
        %peer_addr,
        node = %cluster_cfg.node_name,
        h2_stream_window = ?windows.stream_bytes,
        h2_connection_window = ?windows.connection_bytes,
        // Dialled, not listened on — logged here because this is the line an arm greps to
        // confirm what the peer plane came up as, and a pool width that did not take is
        // indistinguishable in the metrics from a pool width that bought nothing.
        peer_connections = cluster_cfg.peer_connections,
        "peer gRPC listening"
    );
    // THE RECEIVING side of `StoreChunk`, so these are the windows that govern how fast a
    // coordinator may push a 16 MiB window into this node — see `pacer_transport::H2Windows`.
    // `None` leaves hyper's 1 MiB/1 MiB server defaults exactly as they were.
    let mut builder = tonic::transport::Server::builder();
    if let Some(bytes) = windows.stream_bytes {
        builder = builder.initial_stream_window_size(bytes);
    }
    if let Some(bytes) = windows.connection_bytes {
        builder = builder.initial_connection_window_size(bytes);
    }
    Ok(tokio::spawn(
        builder
            .add_service(peer_server.into_service())
            .serve_with_shutdown(peer_addr, async move { shutdown.wait().await }),
    ))
}

/// Assemble the peer server from the parts cluster mode has just built.
///
/// Extracted purely for length: [`enable_cluster`] is at its budget, and this call
/// is twenty lines of it. Arguments are paired by what they belong to — the two
/// storage handles, the two ring structures, the two fill-related ones, and on an
/// EFA build the plane and the runtime its serves run on — which is what keeps this
/// list inside the budget now that [`peer::PeerParts`] carries the sixteen values
/// the constructor used to take positionally.
fn build_peer_server(
    cfg: &config::Config,
    cluster_cfg: &config::ClusterConfig,
    (tier, backend): (pacer_cache::tier::ChunkTier, aws_sdk_s3::Client),
    (ring, directory): (
        pacer_ring::SharedRing,
        pacer_ring::directory::SharedDirectory,
    ),
    metrics: &metrics::Metrics,
    (filling, chunk_fill): (proxy::FillRegistry, cachefill::ChunkFill),
    #[cfg(feature = "efa")] (efa, rdma_runtime): (
        Option<Arc<pacer_transport::efa::EfaRdmaTransport>>,
        &tokio::runtime::Handle,
    ),
) -> peer::PacerPeer {
    peer::PacerPeer::new(peer::PeerParts {
        tier,
        backend,
        chunk: cfg.chunk,
        ring,
        directory,
        local_node: cluster_cfg.node_name.clone(),
        replication_r: cluster_cfg.replication_r,
        min_object_size: cfg.min_object_size,
        max_object_size: cfg.max_object_size,
        channel_capacity: cluster_cfg.channel_capacity,
        metrics: metrics.clone(),
        filling,
        fill_parallelism: cfg.fill_parallelism,
        fill: chunk_fill,
        #[cfg(feature = "efa")]
        efa,
        #[cfg(feature = "efa")]
        rdma_runtime: rdma_runtime.clone(),
    })
    // The same value the proxy gets: one node must not promote on one read path
    // and not the other.
    .with_promotion(cfg.promotion)
}

/// Give the proxy a scatter coordinator, when the scatter is on (ADR-0032).
///
/// Takes the **same** `staging` the peer server is handed, deliberately: one
/// node-wide budget has to cover both roles, because a coordinator's own windows
/// wait for the same Complete an owner's do and so occupy memory the same way. Two
/// budgets would each be sized as though it were the only one.
///
/// `backend` and `tier` arrive as a pair because they are only clones taken for
/// this construction — the originals go on to the peer server.
///
/// Also where the scatter's two bounds are wired into `/metrics`: this is the one place
/// that holds both the node-wide staging area and the coordinator that owns the window
/// slots, and the condition for wiring them ("the scatter is on here") is the condition
/// this function already branches on.
fn attach_scatter(
    proxy: proxy::PacerProxy,
    cluster: proxy::Cluster,
    staging: Option<Arc<StagingArea>>,
    cfg: &config::Config,
    (backend, tier): (aws_sdk_s3::Client, pacer_cache::tier::ChunkTier),
    metrics: &metrics::Metrics,
) -> proxy::PacerProxy {
    let Some(staging) = staging else {
        return proxy;
    };
    let coordinator = Arc::new(coordinate::ScatterCoordinator::new(
        backend,
        tier,
        cfg.chunk,
        cluster,
        &cfg.scatter,
        Arc::clone(&staging),
        metrics.clone(),
    ));
    metrics.set_scatter_bounds(staging, Arc::clone(coordinator.window_slots()));
    proxy.with_scatter(coordinator, cfg.scatter.min_object_bytes)
}

/// Give the peer server a staging area, and start the reaper that guards it, when
/// the scatter is on (ADR-0032).
///
/// Attaching neither is how a node answers `NotAccepting`, which is what makes
/// "the scatter is off here" distinguishable from "this node is busy".
///
/// Returned together because they are one decision: an accepting node needs both,
/// and a non-accepting node must have neither — a staging area with no reaper
/// would let one dead coordinator's budget wedge the node permanently.
fn attach_staging(
    peer_server: peer::PacerPeer,
    staging: Option<Arc<StagingArea>>,
    cfg: &pacer_daemon::scatter::ScatterConfig,
) -> (peer::PacerPeer, Option<ReapTask>) {
    match staging {
        Some(staging) => (
            peer_server.with_staging(Arc::clone(&staging)),
            Some(tokio::spawn(reap_staged(staging, cfg.staging_ttl))),
        ),
        None => (peer_server, None),
    }
}

/// The staging area this node offers peers for scattered writes, or `None` when
/// the scatter is off (ADR-0032).
///
/// `None` rather than a zero-budget area, deliberately: the peer handler reads its
/// absence as `NotAccepting`, which a coordinator treats as permanent and stops
/// retrying. A zero-budget area would refuse every offer as though the node were
/// merely busy, and the coordinator would keep coming back to a node that will
/// never accept.
fn scatter_staging(cfg: &pacer_daemon::scatter::ScatterConfig) -> Option<Arc<StagingArea>> {
    if !cfg.enabled {
        return None;
    }
    info!(
        staging_bytes = cfg.staging_bytes,
        ttl_secs = cfg.staging_ttl.as_secs(),
        windows_in_flight = cfg.windows_in_flight,
        min_object_bytes = cfg.min_object_bytes,
        "write scatter enabled (ADR-0032)"
    );
    Some(Arc::new(StagingArea::new(
        usize::try_from(cfg.staging_bytes).unwrap_or(usize::MAX),
        cfg.staging_ttl,
    )))
}

/// Drop staged chunks whose coordinator never came back to commit or discard them
/// (ADR-0032 § 4).
///
/// Sweeps at a fraction of the TTL rather than at the TTL itself, so an abandoned
/// chunk's budget comes back within about one sweep of expiring instead of taking
/// up to two TTLs. Without this loop one crashed coordinator's reservations would
/// refuse every later write on this node until the daemon restarted — a dead
/// writer would be indistinguishable from a permanently saturated peer.
async fn reap_staged(staging: Arc<StagingArea>, ttl: std::time::Duration) {
    /// Sweeps per TTL. Four keeps the worst-case overshoot to a quarter of the
    /// TTL while leaving the loop essentially free: one map scan over at most
    /// `budget ÷ chunk_size` entries.
    const SWEEPS_PER_TTL: u32 = 4;
    let mut ticker = tokio::time::interval(ttl / SWEEPS_PER_TTL);
    loop {
        ticker.tick().await;
        let reaped = staging.reap_at(std::time::Instant::now());
        if !reaped.is_empty() {
            warn!(
                count = reaped.len(),
                staged_bytes = staging.staged_bytes(),
                "reaped staged chunks whose upload never committed or aborted; \
                 a coordinator most likely died mid-write"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::store_without_slab_is_fatal;

    /// The whole truth table of the ADR-0038 startup guard, in one test.
    ///
    /// `store` + no slab is the ONLY fatal cell, and it is fatal because the store's read is
    /// `O_DIRECT` into a page-aligned frame: without one it silently reads BUFFERED at roughly
    /// half the rate, through the page cache the tier exists to bypass, and nothing but
    /// `pacer_cache_slab_heap_fallbacks_total` would say so.
    #[test]
    fn only_the_store_without_a_slab_is_refused() {
        assert!(
            store_without_slab_is_fatal(false, false).is_ok(),
            "foyer, no slab: the shipped default on a node with no hugepages"
        );
        assert!(
            store_without_slab_is_fatal(false, true).is_ok(),
            "foyer with a slab: the slab is ADR-0028's RAM tier, not the disk tier"
        );
        assert!(
            store_without_slab_is_fatal(true, true).is_ok(),
            "store with a slab: the configuration ADR-0038 defaults on"
        );
        assert!(
            store_without_slab_is_fatal(true, false).is_err(),
            "store with NO slab: every read buffered, and refused"
        );
    }

    /// The refusal has to be ACTIONABLE, because it stops a node from starting. Both escapes
    /// are named — give it a slab, or choose the tier that is measured without one — and so is
    /// the reason, since "diskTier=store is invalid" would send a reader to the wrong knob.
    #[test]
    fn the_refusal_names_both_escapes_and_the_reason() {
        let error = store_without_slab_is_fatal(true, false).expect_err("must refuse");
        let text = error.to_string();
        for needle in [
            "efa.hugepages",         // escape 1: give the node a slab
            "config.diskTier=foyer", // escape 2: the measured alternative
            "BUFFERED",              // the reason, not just the verdict
            "slab_heap_fallbacks",   // where it would otherwise have shown up
        ] {
            assert!(
                text.contains(needle),
                "the refusal must mention {needle}: {text}"
            );
        }
    }
}
