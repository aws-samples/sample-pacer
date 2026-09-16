//! The daemon's budgets under exhaustion, and its drain under load (ADR-0032,
//! ADR-0036).
//!
//! Every bound in the daemon has unit tests for its arithmetic. What had none was the
//! daemon *reaching* one with a client on the other end: the ADR-0032 staging budget
//! with more concurrent PUT bodies than it admits, foyer's memory tier with a working
//! set several times its capacity, the ADR-0036 connection cap under a real SDK
//! client, and the ADR-0036 drain with several bodies mid-flight.
//!
//! # Shape
//!
//! One daemon on a loopback port, assembled the way `tests/daemon/listener.rs` assembles it
//! (placeholder auth → `PacerProxy` → foyer → an `s3s-fs` backend, served through
//! [`pacer_daemon::listen::serve_s3_on`]) and parameterised by the bound under test
//! ([`Setup`]). Two additions over that file, both needed to make a budget reachable
//! or observable:
//!
//! * **foyer's own metrics are registered into the daemon's Prometheus registry**, as
//!   `main` does. Without that the memory tier's residency has no series at all, and
//!   the ceiling under test could only be argued, not read.
//! * **the ADR-0032 scatter can be switched on for a single-node ring**, which is
//!   what makes the staging budget reachable without a second daemon: every window's
//!   home is then this node, so every window takes `coordinate::upload_here` — the
//!   coordinator's *own* staging attempt against the same node-wide budget an owner
//!   would consult (see [`enable_scatter`]).
//!
//! # What each budget does when it is full — decided by the code, not by this file
//!
//! * **Staging** (`staging::StagingArea::try_stage_at`) **refuses, never waits**, and
//!   the refusal is not client-visible: `coordinate::upload_here` uploads the window
//!   anyway and only declines to *cache* it (`DonePart::staged = false`, counted as
//!   `pacer_scatter_uncached_windows_total`). So the assertion is that the PUT
//!   succeeds, every byte lands, and exhaustion shows up as uncached windows with
//!   staged bytes never above the published ceiling.
//! * **The memory tier** evicts per shard down to `capacity − weight` *before* each
//!   insert (foyer 0.22 `RawCacheShard::emplace`), demoting to the disk tier. So the
//!   assertion is that residency never exceeds the configured capacity, that
//!   evictions and RAM misses are visible, and that every GET is still byte-correct.
//! * **The connection cap** (`listen::acquire_permit`) makes a client **wait**: the
//!   permit is taken before `accept`, so a node at its cap stops draining the kernel
//!   backlog. So the assertion is that concurrent GETs past the cap all *succeed*.
//! * **The drain** (`listen::serve_s3_on`'s `GracefulShutdown`, bounded by `main`'s
//!   drain deadline) finishes in-flight requests and refuses new connections, and at
//!   the deadline gives up on whatever is left.
//!
//! # Not duplicated here
//!
//! `tests/daemon/listener.rs` covers the cap with raw idle sockets, the two socket deadlines
//! and a single-request drain; `tests/daemon/scatter/` covers reject-fast across a
//! five-node fleet with a one-window budget. This file is the exhaustion-side,
//! many-clients-at-once half of both.

// Tests are linear scenarios; splitting them to satisfy a line count would hurt
// readability (CLAUDE.md: the size limits target production code). Same allowance,
// for the same reason, as `tests/daemon/scatter/`.
#![allow(clippy::too_many_lines)]

use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use pacer_backend::BackendType;
use pacer_cache::chunk::ChunkConfig;
use pacer_cache::tier::ChunkTier;
use pacer_daemon::coordinate::ScatterCoordinator;
use pacer_daemon::listen::ListenLimits;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::proxy::{Cluster, PacerProxy};
use pacer_daemon::scatter::ScatterConfig;
use pacer_daemon::staging::StagingArea;
use pacer_ring::{NodeId, SharedRing};
use pacer_transport::grpc::GrpcTransport;

use crate::common::{
    self, body_for_key, daemon_core, metric_value, CacheSpec, Daemon, DaemonSpec, BUCKET,
    CHANNEL_CAPACITY, LOCAL_ADMISSION_RATIO, LOCAL_ADMISSION_THRESHOLD, LOCAL_ADMISSION_WINDOW,
    PATIENCE, TEST_BUDGET,
};

// ---------------------------------------------------------------- shared fixtures

/// Read-path cache floor (ADR-0002), below every object here, so nothing is bypassed
/// as "too small to cache" — the subject is exhaustion, not admission.
const MIN_OBJECT_SIZE: u64 = 4 << 10;

/// foyer's in-memory shard count (`foyer_memory::RawCacheConfig::shards`, whose
/// default is 8 in foyer 0.22).
///
/// Load-bearing for every memory-tier size below. The capacity is split evenly across
/// shards and **each shard evicts against its own share**, so a capacity under
/// `shards × entry_size` cannot be honoured at all: a shard whose share is smaller
/// than one entry evicts everything it holds (`evict(capacity − weight)` with a
/// saturating subtraction) and then keeps the entry it just inserted anyway, leaving
/// total residency at up to `shards × entry_size` however small the number in the
/// config was. So the smallest *meaningful* memory tier for a given chunk size is one
/// chunk per shard, and that is what [`READ_MEM_CAPACITY`] is.
const MEM_TIER_SHARDS: u64 = 8;

/// A roomy memory tier for the arms whose subject is not the memory tier.
/// `MEM_TIER_SHARDS` × the largest chunk any arm uses, several times over.
const ROOMY_MEM_CAPACITY: usize = 128 << 20;

/// A roomy disk tier for the same arms. Sparse on every filesystem the suite runs on.
const ROOMY_DISK_CAPACITY: usize = 128 << 20;

/// Disk block size. A block is foyer's eviction unit **and** the largest cacheable
/// entry, so it must exceed every chunk size below.
const BLOCK_SIZE: usize = 16 << 20;

// ------------------------------------------------------------------- the harness

/// The daemon this file brings up, and the bound under test.
///
/// A struct with a documented [`Default`] rather than a builder: each test overrides
/// the one or two fields its budget is about (`Setup { .., ..Setup::default() }`), so
/// the diff between two arms is exactly the knob that differs. It sits *in front of*
/// [`DaemonSpec`] rather than replacing it because the two knobs no other arm has —
/// the socket bounds and the staging budget — are what this file is about, and
/// flattening them into the shared spec would put a scatter coordinator in every
/// arm's bring-up.
#[derive(Debug, Clone)]
struct Setup {
    /// The ADR-0036 socket bounds.
    limits: ListenLimits,
    /// Chunk size — the cache's grid, and the scatter's window and S3 part size.
    chunk_size: u64,
    /// foyer memory tier capacity, in bytes. See [`MEM_TIER_SHARDS`] before shrinking.
    mem_capacity: usize,
    /// foyer disk tier capacity, in bytes.
    disk_capacity: usize,
    /// `Some(budget)` turns the ADR-0032 scatter on with that node-wide staging
    /// ceiling; `None` leaves the write path on ADR-0007's proxy-and-invalidate.
    staging_bytes: Option<u64>,
}

impl Default for Setup {
    fn default() -> Self {
        Self {
            limits: ListenLimits::default(),
            chunk_size: READ_CHUNK_SIZE,
            mem_capacity: ROOMY_MEM_CAPACITY,
            disk_capacity: ROOMY_DISK_CAPACITY,
            staging_bytes: None,
        }
    }
}

/// Bring up the daemon stack under `setup` and serve it on a loopback port.
///
/// `foyer_metrics` is on for every arm here, not just the memory-tier one: this file
/// reads its assertions off the exposition (`Daemon::scrape`) rather than off the
/// metric structs, which is the half of the instrument a paid arm actually sees, and
/// foyer's own series only exist in it when its registry is wired.
async fn daemon(setup: Setup) -> Daemon {
    let mut core = daemon_core(DaemonSpec {
        min_object_size: MIN_OBJECT_SIZE,
        max_object_size: None,
        chunk_size: setup.chunk_size,
        cache: CacheSpec {
            mem_capacity: setup.mem_capacity,
            disk_capacity: setup.disk_capacity,
            block_size: BLOCK_SIZE,
        },
        foyer_metrics: true,
        ..DaemonSpec::default()
    })
    .await;
    if let Some(budget) = setup.staging_bytes {
        // The coordinator's backend client is the oracle's twin — same service, same
        // daemon identity — so there is no third client to build.
        let (backend, tier, metrics) = (
            core.backend.clone(),
            core.tier.clone(),
            core.metrics.clone(),
        );
        core.proxy = enable_scatter(
            core.proxy,
            backend,
            &tier,
            ChunkConfig::new(setup.chunk_size),
            &metrics,
            budget,
        );
    }
    core.served(setup.limits).await
}

/// A body whose bytes depend on the key, so a misplaced or duplicated chunk shows up
/// as wrong bytes rather than as bytes that happen to match.
fn body_of(key: &str, len: u64) -> Bytes {
    body_for_key(key, len)
}

// -------------------------------------------------- 1. the ADR-0032 staging budget

/// Window size for the write-path arm: `MIN_S3_PART_SIZE`, the smallest grid a
/// scatter may legally use, which keeps the test objects as small as the design
/// permits.
const WRITE_CHUNK_SIZE: u64 = pacer_daemon::scatter::MIN_S3_PART_SIZE;

/// Windows per scattered object.
const WRITE_WINDOWS: u64 = 4;

/// Length of each scattered object.
const WRITE_OBJECT_LEN: u64 = WRITE_CHUNK_SIZE * WRITE_WINDOWS;

/// Windows the staging budget admits at once. Two, so **one** object already overruns
/// it by two windows and the lower bound below holds however the concurrent PUTs
/// interleave.
const STAGED_WINDOWS: u64 = 2;

/// The node-wide staging ceiling under test.
const TINY_STAGING_BYTES: u64 = STAGED_WINDOWS * WRITE_CHUNK_SIZE;

/// Concurrent PUTs. Three objects of [`WRITE_WINDOWS`] windows want six times the
/// budget, and want it at the same time — which is the part no existing test does.
const CONCURRENT_PUTS: u64 = 3;

/// Smallest object this file scatters: two windows, so [`WRITE_OBJECT_LEN`] is well
/// clear of the floor.
const MIN_SCATTER_BYTES: u64 = 2 * WRITE_CHUNK_SIZE;

/// Windows a coordinator keeps in flight. Above [`WRITE_WINDOWS`] on purpose: the
/// window semaphore must not be the bound in this arm, or an assertion about the
/// staging budget would be measuring the wrong ceiling.
const WRITE_WINDOWS_IN_FLIGHT: usize = 8;

/// Staged-chunk TTL. Longer than the whole binary runs, so nothing is reaped and
/// budget that came back can only have been freed by a commit.
const STAGING_TTL: Duration = Duration::from_secs(900);

/// Cooldown after a transient refusal. Irrelevant here (with one node no offer ever
/// crosses the wire) but it has to be set; long enough to be provably not the
/// mechanism behind anything asserted.
const SATURATED_COOLDOWN: Duration = Duration::from_secs(300);

/// This node's name in the ring.
const LOCAL_NODE: &str = "node-a";

/// The peer gRPC address recorded for [`LOCAL_NODE`]. Never dialled: with one node in
/// the ring every window's home is this node, and `coordinate::upload_window`
/// short-circuits to `upload_here` before it touches the transport.
const PEER_ADDR_UNUSED: &str = "127.0.0.1:0";

/// Single-copy placement (ADR-0012): one home per chunk, so "the home" is unambiguous.
const REPLICATION_R: usize = 1;

/// Turn the ADR-0032 scatter on for a **single-node** ring.
///
/// One node is the whole trick. `ScatterPlan::build` homes every window on the only
/// member, so `coordinate::upload_window` never offers over the wire and every window
/// goes through `upload_here` — which consults the same node-wide [`StagingArea`] an
/// owner's `StoreChunk` handler would. That reaches the staging budget with no peer
/// plane, no gRPC server and no second daemon, and it exercises the branch a
/// five-node fleet reaches least: the coordinator's own staging attempt.
///
/// Both bounds are wired into the scrape (`Metrics::set_scatter_bounds`) exactly as
/// `main` wires them, with the same `Arc` the coordinator holds — a second
/// `StagingArea` would publish a budget nothing consults.
fn enable_scatter(
    proxy: PacerProxy,
    backend: aws_sdk_s3::Client,
    tier: &ChunkTier,
    chunk: ChunkConfig,
    metrics: &Metrics,
    budget_bytes: u64,
) -> PacerProxy {
    let cfg = ScatterConfig {
        enabled: true,
        staging_bytes: budget_bytes,
        staging_ttl: STAGING_TTL,
        windows_in_flight: WRITE_WINDOWS_IN_FLIGHT,
        min_object_bytes: MIN_SCATTER_BYTES,
        saturated_cooldown: SATURATED_COOLDOWN,
    };
    let ring = SharedRing::default();
    ring.store(vec![NodeId::new(LOCAL_NODE, PEER_ADDR_UNUSED)]);
    let cluster = Cluster {
        ring,
        directory: pacer_ring::directory::SharedDirectory::new(
            pacer_ring::directory::DEFAULT_MAX_SHARERS_TRACKED,
        ),
        transport: Arc::new(GrpcTransport::new(0, LOCAL_NODE)),
        local_node: LOCAL_NODE.to_owned(),
        channel_capacity: CHANNEL_CAPACITY,
        replication_r: REPLICATION_R,
        admission: Arc::new(pacer_cache::admission::AdmissionGate::new(
            LOCAL_ADMISSION_THRESHOLD,
            LOCAL_ADMISSION_WINDOW,
            LOCAL_ADMISSION_RATIO,
            u64::MAX,
            std::time::Instant::now(),
        )),
        #[cfg(feature = "efa")]
        efa: None,
    };
    let staging = Arc::new(StagingArea::new(
        usize::try_from(budget_bytes).unwrap(),
        STAGING_TTL,
    ));
    let coordinator = Arc::new(ScatterCoordinator::new(
        backend,
        tier.clone(),
        chunk,
        cluster.clone(),
        &cfg,
        Arc::clone(&staging),
        metrics.clone(),
    ));
    metrics.set_scatter_bounds(staging, Arc::clone(coordinator.window_slots()));
    proxy
        // ADR-0032 § 6: the scatter is a general-purpose-bucket feature, and enabling
        // it on Express is refused at startup.
        .with_backend_type(BackendType::Standard)
        .with_cluster(cluster)
        .with_scatter(coordinator, MIN_SCATTER_BYTES)
}

/// **The staging budget is exhausted by concurrent PUTs, and exhaustion costs warmth
/// rather than the write.**
///
/// `staging::StagingArea::try_stage_at` refuses instead of waiting (its module header
/// gives the three reasons: deadlock, load awareness, degradation), and
/// `coordinate::upload_here` treats that refusal as "upload it, do not cache it". So
/// the documented behaviour, asserted here, is:
///
/// * every PUT succeeds — a full budget is never a client-visible error;
/// * every byte is durable and correct, read back from the backend *and* through the
///   daemon, because a budget that corrupts under pressure would be worse than one
///   that refuses;
/// * staged bytes never exceed the published ceiling, read as the latched high-water
///   mark rather than a sample (`staging::StagingArea::staged_bytes_peak`: a residency
///   that peaks and drains between two scrapes is invisible to a gauge);
/// * the overrun is *attributable* — `pacer_scatter_uncached_windows_total` accounts
///   for every window nobody could stage.
///
/// What is new over `scatter.rs`'s `reject_fast_absorbs_the_windows_owners_refuse`:
/// that test drives one PUT at a time against *peers'* budgets, so it never contends
/// the local budget from several bodies at once, and it reads the metric structs
/// directly rather than the exposition a paid arm actually reads.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_puts_past_the_staging_budget_still_land_every_byte() {
    tokio::time::timeout(TEST_BUDGET, async {
        let mut d = daemon(Setup {
            chunk_size: WRITE_CHUNK_SIZE,
            staging_bytes: Some(TINY_STAGING_BYTES),
            ..Setup::default()
        })
        .await;
        let client = d.client.clone();

        let keys: Vec<String> = (0..CONCURRENT_PUTS)
            .map(|i| format!("scatter/budget-{i}.bin"))
            .collect();
        let payloads: Vec<Bytes> = keys.iter().map(|k| body_of(k, WRITE_OBJECT_LEN)).collect();
        let puts = keys.iter().zip(&payloads).map(|(key, payload)| {
            client
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(ByteStream::from(payload.clone()))
                .send()
        });
        for (i, result) in futures::future::join_all(puts)
            .await
            .into_iter()
            .enumerate()
        {
            result.unwrap_or_else(|e| {
                panic!(
                    "a PUT no budget can fully stage must still succeed (key {}): {e}",
                    keys[i]
                )
            });
        }

        let text = d.scrape();
        let scattered = metric_value(&text, "pacer_scatter_puts_total", &[]);
        assert_eq!(
            scattered,
            CONCURRENT_PUTS as f64,
            "every PUT must have taken the scatter path, or this arm never reached the \
             staging budget at all:\n{}",
            common::nonzero_lines(&text, "pacer_scatter")
        );
        let budget = metric_value(&text, "pacer_scatter_staging_budget_bytes", &[]);
        assert_eq!(
            budget, TINY_STAGING_BYTES as f64,
            "the published ceiling must be the configured one, or the peak below is \
             being judged against the wrong number"
        );
        let peak = metric_value(&text, "pacer_scatter_staged_bytes_peak", &[]);
        assert!(
            peak > 0.0,
            "staged bytes must have risen, or the budget was never exercised:\n{}",
            common::nonzero_lines(&text, "pacer_scatter")
        );
        assert!(
            peak <= budget,
            "staged bytes peaked at {peak} against a {budget}-byte ceiling: the budget \
             admitted more than it holds"
        );
        assert_eq!(
            metric_value(&text, "pacer_scatter_staged_bytes", &[]),
            0.0,
            "every upload committed or unwound, so nothing may still be staged"
        );

        // Every window was uploaded by this node (single-node ring), and the ones the
        // budget could not hold are counted rather than lost.
        let windows = CONCURRENT_PUTS * WRITE_WINDOWS;
        assert_eq!(
            metric_value(&text, "pacer_scatter_windows_total", &[("role", "local")]),
            windows as f64
        );
        let uncached = metric_value(&text, "pacer_scatter_uncached_windows_total", &[]);
        let least_uncached = CONCURRENT_PUTS * (WRITE_WINDOWS - STAGED_WINDOWS);
        assert!(
            uncached >= least_uncached as f64 && uncached <= windows as f64,
            "expected between {least_uncached} and {windows} windows to go uncached at a \
             {STAGED_WINDOWS}-window budget, got {uncached}:\n{}",
            common::nonzero_lines(&text, "pacer_scatter")
        );

        for (key, payload) in keys.iter().zip(&payloads) {
            assert_eq!(
                &d.read_backend(key).await,
                payload,
                "the backend's copy of {key} must be byte-exact after an over-budget write"
            );
            assert_eq!(
                &d.get(key).await,
                payload,
                "a read of {key} through the daemon must be byte-exact, whether its \
                 chunks were cached or not"
            );
        }

        d.shutdown_quietly().await;
    })
    .await
    .expect("the staging-budget arm must finish inside TEST_BUDGET");
}

// ------------------------------------------------- 2. the foyer memory tier's ceiling

/// Chunk size for the read-path arms. Small on purpose: the arm's subject is a working
/// set several times the memory tier, and 1 MiB chunks make that a few tens of MiB
/// instead of a gigabyte, which is what keeps this binary inside a minute.
const READ_CHUNK_SIZE: u64 = 1 << 20;

/// The memory tier under test: one chunk per foyer shard — the smallest capacity that
/// can honour a ceiling at all (see [`MEM_TIER_SHARDS`] for why a smaller number
/// cannot be honoured, and would make the assertion below fail for a reason that has
/// nothing to do with the daemon).
const READ_MEM_CAPACITY: usize = (MEM_TIER_SHARDS * READ_CHUNK_SIZE) as usize;

/// Distinct objects the memory-tier arm GETs, one chunk each — twice what the tier can
/// hold, so a second pass provably cannot be served from RAM alone.
const READ_OBJECTS: u64 = 2 * MEM_TIER_SHARDS;

/// The disk tier for that arm: room for the whole working set, so a chunk evicted from
/// RAM is *demoted* rather than dropped. Demotion is the behaviour under test; a disk
/// tier too small to accept it would turn every eviction into a backend re-read and
/// measure the backend instead.
const READ_DISK_CAPACITY: usize = 4 * BLOCK_SIZE;

/// foyer's in-memory op counter, whose children are the tier's own hit/miss/evict.
const FOYER_MEMORY_OPS: &str = "foyer_memory_op_total";

/// foyer's live in-memory residency gauge, in bytes.
const FOYER_MEMORY_USAGE: &str = "foyer_memory_usage";

/// The `name` label foyer stamps on every one of its series — the constant
/// `HybridCacheBuilder::with_name` is actually given, not a copy of it. It used to be
/// a hardcoded `"pacer"` here, which would have failed as "expected exactly one …
/// found 0" the first time the cache was renamed; reading the real constant makes a
/// rename move both sides at once.
const FOYER_CACHE_NAME: &str = pacer_cache::CACHE_NAME;

/// One of foyer's in-memory op counters.
fn foyer_op(text: &str, op: &str) -> f64 {
    metric_value(
        text,
        FOYER_MEMORY_OPS,
        &[("name", FOYER_CACHE_NAME), ("op", op)],
    )
}

/// **A working set twice the memory tier is served correctly, and the tier stays
/// inside its ceiling while doing it.**
///
/// foyer's memory tier evicts per shard down to `capacity − weight` *before* each
/// insert (`RawCacheShard::emplace`), demoting the evicted entry to the disk tier. So
/// the documented behaviour, asserted here:
///
/// * every GET on both passes returns the exact bytes — the correctness half, because
///   an eviction race that served a stale or short chunk would be a data bug wearing a
///   capacity bug's clothes;
/// * residency never exceeds the configured capacity, and is not zero (a ceiling
///   asserted against an unwired gauge would pass for the wrong reason);
/// * the tier is genuinely at its ceiling: it evicted, and the second pass still
///   misses in RAM. That second reading is deliberately foyer's *memory* miss and not
///   the daemon's `pacer_cache_misses_total`: the daemon counts an object-header miss
///   per GET, and a header demoted to the disk tier is still a hybrid-cache hit, so
///   the daemon-level counter cannot see a RAM eviction at all. Pinning both here is
///   what says which layer the exhaustion happened in.
#[tokio::test(flavor = "multi_thread")]
async fn a_working_set_past_the_memory_tier_is_still_served_correctly() {
    tokio::time::timeout(TEST_BUDGET, async {
        let mut d = daemon(Setup {
            mem_capacity: READ_MEM_CAPACITY,
            disk_capacity: READ_DISK_CAPACITY,
            ..Setup::default()
        })
        .await;

        let mut objects = Vec::with_capacity(usize::try_from(READ_OBJECTS).unwrap());
        for i in 0..READ_OBJECTS {
            let key = format!("tier/object-{i}.bin");
            let body = d.seed_chunks(&key, 1).await;
            objects.push((key, body));
        }

        let mut ram_misses = Vec::new();
        for pass in 1..=2 {
            for (key, body) in &objects {
                assert_eq!(
                    &d.get(key).await,
                    body,
                    "pass {pass} of {key} must be byte-exact past the tier's capacity"
                );
            }
            let text = d.scrape();
            let usage = metric_value(&text, FOYER_MEMORY_USAGE, &[("name", FOYER_CACHE_NAME)]);
            assert!(
                usage > 0.0,
                "pass {pass}: the memory tier reported no residency at all, so the \
                 ceiling below is not being tested — is foyer's registry wired?"
            );
            assert!(
                usage <= READ_MEM_CAPACITY as f64,
                "pass {pass}: the memory tier held {usage} bytes against a \
                 {READ_MEM_CAPACITY}-byte capacity"
            );
            ram_misses.push(foyer_op(&text, "miss"));
        }
        assert!(
            ram_misses[1] > ram_misses[0],
            "the second pass must still miss in RAM ({} then {}): a working set {}× the \
             tier cannot be resident, and a second pass that hit everything would mean \
             the capacity was not the one configured",
            ram_misses[0],
            ram_misses[1],
            READ_OBJECTS / MEM_TIER_SHARDS
        );

        let text = d.scrape();
        assert!(
            foyer_op(&text, "evict") > 0.0,
            "a working set {}× the tier must have evicted; without an eviction this arm \
             proves nothing about a ceiling",
            READ_OBJECTS / MEM_TIER_SHARDS
        );
        assert_eq!(
            metric_value(&text, "pacer_cache_misses_total", &[]),
            READ_OBJECTS as f64,
            "exactly one header miss per distinct object: the first GET of each learns \
             its length from the backend, and no later GET may add one"
        );
        assert!(
            metric_value(&text, "pacer_cache_hits_total", &[]) > 0.0,
            "the tier must have served something, or the misses above are the whole story"
        );

        d.shutdown_quietly().await;
    })
    .await
    .expect("the memory-tier arm must finish inside TEST_BUDGET");
}

// ------------------------------------------------------- 3. the ADR-0036 drain

/// Chunks per object in the drain arms. Big enough that the body is genuinely still
/// streaming when the flag goes up **and** that a response nobody reads cannot fit in
/// the socket's buffers — the negative arm depends on the second property, since a
/// response the kernel has already absorbed would let the drain finish and the deadline
/// would never be reached.
const DRAIN_CHUNKS: u64 = 8;

/// In-flight GETs the drain must carry to completion. More than one, which is what
/// `tests/daemon/listener.rs`'s single-request drain cannot say anything about: a drain that
/// awaited its connections one at a time, or that dropped all but the first watcher,
/// passes that test and fails this one.
const DRAIN_REQUESTS: usize = 4;

/// The drain deadline for the completing arm — the value `main` gets from
/// `PACER_SHUTDOWN_DRAIN_TIMEOUT`, scaled down to a test. Orders of magnitude above
/// the milliseconds four loopback bodies take, so a failure here is a drain that never
/// completes rather than a slow runner.
const DRAIN_DEADLINE: Duration = Duration::from_secs(10);

/// The drain deadline for the negative arm. Well below [`SLOW_READER_PAUSE`], so the
/// deadline is provably what ends the drain and not the request finishing.
const SHORT_DRAIN_DEADLINE: Duration = Duration::from_millis(300);

/// How long the slow reader stops mid-body. An order of magnitude above
/// [`SHORT_DRAIN_DEADLINE`], so the drain cannot outlast it by luck.
const SLOW_READER_PAUSE: Duration = Duration::from_secs(3);

/// **A drain finishes every in-flight body, refuses new connections, and reports
/// completion inside the deadline.**
///
/// `main`'s drain is a `tokio::time::timeout` around the listener's `JoinHandle`
/// (`main::drain`, step 1), which resolves once `hyper_util`'s `GracefulShutdown` has
/// let every *watched* connection finish. That handle is the only thing a drain
/// reports through, so it is what this arm asserts on — see the report note about
/// `main::drain` being binary-private and therefore uncallable from a test.
///
/// Four concurrent multi-chunk bodies, each proven mid-flight before the flag goes up.
/// The failure this catches and `listener.rs` cannot: a drain that serialises its
/// connections, or one that watches only the connection it happens to hold.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_finishes_every_in_flight_body_inside_its_deadline() {
    tokio::time::timeout(TEST_BUDGET, async {
        let mut d = daemon(Setup::default()).await;
        let client = d.client.clone();

        let mut in_flight = Vec::with_capacity(DRAIN_REQUESTS);
        for i in 0..DRAIN_REQUESTS {
            let key = format!("drain/in-flight-{i}.bin");
            let body = d.seed_chunks(&key, DRAIN_CHUNKS).await;
            let mut stream = client
                .get_object()
                .bucket(BUCKET)
                .key(&key)
                .send()
                .await
                .expect("GET must succeed")
                .body;
            let first = stream
                .try_next()
                .await
                .expect("first piece must arrive")
                .expect("body must not be empty");
            assert!(!first.is_empty());
            in_flight.push((key, body, stream, first));
        }
        assert_eq!(
            metric_value(&d.scrape(), "pacer_s3_connections_active", &[]),
            DRAIN_REQUESTS as f64,
            "all {DRAIN_REQUESTS} requests must be on their own live connection, or this \
             arm is draining fewer connections than it thinks"
        );

        // Every request is now genuinely mid-body.
        d.trigger_shutdown();

        for (key, body, mut stream, first) in in_flight {
            let mut received = first.to_vec();
            while let Some(piece) = stream
                .try_next()
                .await
                .expect("a draining server must not break an in-flight body")
            {
                received.extend_from_slice(&piece);
            }
            assert_eq!(
                Bytes::from(received),
                body,
                "the in-flight GET of {key} must complete with every byte"
            );
        }

        common::wait_until_connection_refused(d.addr()).await;
        d.shutdown_and_drain(DRAIN_DEADLINE).await;
    })
    .await
    .expect("the drain arm must finish inside TEST_BUDGET");
}

/// Read `stream` until it ends cleanly or breaks, discarding the bytes.
///
/// A severed body and a completed one are the same outcome to the caller — the point is
/// only that it *stops*. Extracted so the arm below does not nest a match inside a loop
/// inside an async block inside the test's own timeout (clippy `excessive_nesting`).
async fn read_until_it_stops(stream: &mut aws_sdk_s3::primitives::ByteStream) {
    while let Ok(Some(_)) = stream.try_next().await {}
}

/// **A drain deadline shorter than the request it is waiting for ends at the deadline,
/// and the daemon still shuts down cleanly.**
///
/// The other half of ADR-0036's bargain, and the one `shutdown::DEFAULT_DRAIN_TIMEOUT_SECS`
/// documents as deliberate: "what can legitimately exceed 20 s is a client streaming a
/// multi-GiB object at its own pace; that one is cut, and cutting it is correct". A pod
/// that waited for it would be SIGKILLed mid-drain by the kubelet, which is the failure
/// the drain exists to remove.
///
/// So: a client that stops reading mid-body holds the drain open, the deadline expires
/// while it does, and then the process-exit path (`main::drain` steps 2-3: abandon the
/// listener, close the cache) still completes — no panic, no hang. The slow request is
/// severed, which is the cost.
#[tokio::test(flavor = "multi_thread")]
async fn a_drain_deadline_cuts_a_slow_reader_and_still_exits_cleanly() {
    tokio::time::timeout(TEST_BUDGET, async {
        let mut d = daemon(Setup::default()).await;
        let client = d.client.clone();
        let key = "drain/slow-reader.bin";
        d.seed_chunks(key, DRAIN_CHUNKS).await;

        let mut stream = client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .expect("GET must succeed")
            .body;
        let first = stream
            .try_next()
            .await
            .expect("first piece must arrive")
            .expect("body must not be empty");
        assert!(!first.is_empty(), "the request must be mid-body");

        d.trigger_shutdown();
        // Taken rather than left to the `Daemon` guard: this arm's whole subject is what
        // the drain does at its deadline, so the handle has to be the test's.
        let mut task = d.take_listener();
        // The client reads nothing for longer than the deadline, so the response write
        // blocks on TCP backpressure and the watched connection cannot finish.
        let elapsed = tokio::time::Instant::now();
        assert!(
            tokio::time::timeout(SHORT_DRAIN_DEADLINE, &mut task)
                .await
                .is_err(),
            "the drain must still be waiting on the stalled request at its deadline, or \
             the deadline is not what ends it"
        );
        assert!(
            elapsed.elapsed() < SLOW_READER_PAUSE,
            "the deadline must expire well before the slow reader would have resumed"
        );

        // What `main` does next, in order: abandon the listener, then close the cache.
        // Both must complete — a drain that failed still has to exit 0, because a
        // non-zero exit during a rolling update is reported as a crash-looping pod.
        task.abort();
        assert!(
            tokio::time::timeout(PATIENCE, d.tier.cache().close())
                .await
                .expect("closing the cache must not hang after an abandoned drain")
                .is_ok(),
            "the cache must close cleanly even when the drain gave up"
        );
        assert!(
            tokio::time::timeout(PATIENCE, read_until_it_stops(&mut stream))
                .await
                .is_ok(),
            "the cut request must end rather than hang once the drain has been abandoned"
        );
    })
    .await
    .expect("the drain-deadline arm must finish inside TEST_BUDGET");
}

// --------------------------------------- 4. the ADR-0036 connection cap, real client

/// Connections the cap arm admits at once.
const CAP_MAX_CONNECTIONS: usize = 2;

/// Concurrent GETs a real SDK client issues against that cap — four times it, so its
/// pool must dial more sockets than the daemon will hold at once.
const CAP_CONCURRENT_GETS: usize = 4 * CAP_MAX_CONNECTIONS;

/// Header/keep-alive and no-progress deadlines for the cap arm, and **the reason this
/// arm needs short ones at all**.
///
/// A permit is released when its *connection* closes, not when its request finishes
/// (`listen::serve_s3_on`), and hyper's client keeps a served connection in its pool.
/// So with a pool larger than the cap, the connections that were served sit idle
/// holding every permit while the sockets still in the kernel backlog wait for one. The
/// two socket deadlines are what break that: either the header timeout (armed whenever
/// a connection starts reading a head, so it is the keep-alive reaper) or the
/// no-progress timeout closes the idle connection and returns its permit. This arm does
/// not care which fires, only that a permit comes back — so both are set to the same
/// value.
///
/// 500 ms: far above the sub-millisecond service time of a [`CAP_OBJECT_BYTES`] object
/// out of a temp dir, so it can never cut a request that is being served; and low
/// enough that the arm's ~4 rounds of reaping finish inside [`PATIENCE`].
///
/// **Production is not in this regime** — the cap defaults to 1024 against a measured
/// 512-socket peak (`listen::DEFAULT_MAX_CONNECTIONS`) — but a client that does exceed
/// the cap pays up to one header timeout per round, which is worth having pinned.
const CAP_SOCKET_TIMEOUT: Duration = Duration::from_millis(500);

/// Object size for the cap arm. Above [`MIN_OBJECT_SIZE`] so it is cached rather than
/// bypassed, and far below one chunk so every GET is a single quick response — the
/// subject is the accept loop, not the body.
const CAP_OBJECT_BYTES: u64 = 64 << 10;

/// **The connection cap backpressures a real client instead of failing it.**
///
/// `listen::acquire_permit` takes the permit *before* `accept`, so a node at its cap
/// stops draining the kernel backlog — "backpressure at the accept queue is the only
/// kind a client's TCP stack understands without being taught a new error". The
/// documented consequence, asserted here: the cost of the cap being too low is added
/// latency on a client's connect, **never an error**.
///
/// `tests/daemon/listener.rs` proves the cap with raw sockets that never send a request; this
/// proves it with an unmodified `aws-sdk-s3` client whose pool wants
/// [`CAP_CONCURRENT_GETS`] connections against a cap of [`CAP_MAX_CONNECTIONS`], which
/// is the only shape that also exercises the interaction with keep-alive (see
/// [`CAP_SOCKET_TIMEOUT`]).
#[tokio::test(flavor = "multi_thread")]
async fn the_connection_cap_backpressures_a_real_client() {
    tokio::time::timeout(TEST_BUDGET, async {
        let mut d = daemon(Setup {
            limits: ListenLimits {
                max_connections: CAP_MAX_CONNECTIONS,
                header_timeout: CAP_SOCKET_TIMEOUT,
                idle_timeout: CAP_SOCKET_TIMEOUT,
            },
            ..Setup::default()
        })
        .await;

        let mut expected = Vec::with_capacity(CAP_CONCURRENT_GETS);
        for i in 0..CAP_CONCURRENT_GETS {
            let key = format!("cap/object-{i}.bin");
            let body = d.seed(&key, CAP_OBJECT_BYTES).await;
            expected.push((key, body));
        }

        let gets = expected.iter().map(|(key, _)| async { d.get(key).await });
        for ((key, body), got) in expected.iter().zip(futures::future::join_all(gets).await) {
            assert_eq!(
                &got, body,
                "every GET past the cap must succeed with the right bytes, not fail: {key}"
            );
        }

        let text = d.scrape();
        assert!(
            metric_value(&text, "pacer_s3_connections_at_capacity_total", &[]) >= 1.0,
            "reaching the cap must be observable in /metrics; with {CAP_CONCURRENT_GETS} \
             concurrent GETs against a cap of {CAP_MAX_CONNECTIONS} it cannot not have \
             happened, so a zero here means the counter is not wired"
        );
        assert_eq!(
            metric_value(&text, "pacer_s3_accept_errors_total", &[]),
            0.0,
            "backpressure is not an accept failure; a non-zero count here would mean the \
             cap was enforced by refusing sockets rather than by not taking them"
        );

        d.shutdown_quietly().await;
    })
    .await
    .expect("the connection-cap arm must finish inside TEST_BUDGET");
}
