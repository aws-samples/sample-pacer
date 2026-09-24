//! Daemon-level Prometheus metrics. foyer's own metrics are registered into
//! the same registry via mixtrics (see main.rs), so /metrics exposes both.

use prometheus::{
    Encoder, Gauge, Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge,
    Opts, Registry, TextEncoder,
};

/// Clock ticks per second used to convert `/proc/self/stat` `utime`/`stime`
/// (reported in jiffies) into seconds. The Linux `times(2)`/`/proc` interface
/// reports process CPU accounting in `USER_HZ`, which is fixed at 100 on every
/// mainstream Linux ABI regardless of the kernel's internal `CONFIG_HZ`. We
/// hardcode it rather than link libc for a single `sysconf(_SC_CLK_TCK)` call;
/// A4 only needs CPU-per-GiB *ratios*, and both benchmark arms use the same
/// divisor, so even an exotic non-100 kernel would not skew the comparison.
#[cfg(target_os = "linux")]
const USER_HZ: f64 = 100.0;

/// Divisor converting the RDMA copy accumulators (kept as monotonic nanosecond
/// counters inside the transport, so the hot path only does a relaxed
/// `fetch_add`) into the seconds the Prometheus gauges publish.
#[cfg(feature = "efa")]
const NANOS_PER_SEC: f64 = 1_000_000_000.0;

/// Latency buckets (seconds) for the directory-RPC service-time histogram
/// (`pacer_dir_rpc_seconds`). A directory op is an in-memory
/// `RwLock`-guarded map lookup/insert on one shard ([`pacer_ring::directory`]),
/// so the interesting range is sub-microsecond to tens of microseconds; the
/// tail buckets exist only to catch lock contention under a storm. These are
/// the **per-lookup CPU-cost input** to the B4 Step-6 ADR-0020 projection
/// (planning/15): the histogram `_sum / _count` at N=16 is multiplied by the
/// ring-derived peak lookup rate on the hottest shard at N=1000 to decide
/// whether the one-sided-READ directory (ADR-0020) ever activates. Buckets
/// span 500 ns → 5 ms so that projection reads off real resolution, not a
/// saturated top bucket.
const DIR_RPC_LATENCY_BUCKETS: &[f64] = &[
    0.000_000_5,
    0.000_001,
    0.000_002_5,
    0.000_005,
    0.000_01,
    0.000_025,
    0.000_05,
    0.000_1,
    0.000_5,
    0.001,
    0.005,
];

/// Latency buckets (seconds) for the per-chunk delivery histogram
/// (`pacer_delivery_chunk_seconds`). One observation covers a chunk's whole
/// resolution — a cache lookup, then either a `memcpy` into the client's window
/// or a holder-driven WRITE into it — so the interesting range runs from a
/// hundred microseconds (a RAM hit into a warm window) to tens of milliseconds
/// (an NVMe read at shallow queue depth).
///
/// The tail buckets earn their place by separating two readings that a rate
/// alone cannot tell apart, and that the 32-rail C4 arm therefore could not
/// close (`bench/ladder/results/c4-dcp-hf-safetensors.md`): a 512 MiB span is 32
/// windows, and 32 windows resolving in 96 ms is *either* 3 ms each with no
/// overlap *or* ~96 ms each with full overlap. Only a latency distribution,
/// read against [`DeliveryMetrics::inflight_chunks_peak`], says which.
const DELIVERY_CHUNK_LATENCY_BUCKETS: &[f64] = &[
    0.000_1, 0.000_25, 0.000_5, 0.001, 0.002, 0.004, 0.008, 0.016, 0.032, 0.064, 0.128, 0.256,
    0.512,
];

/// Latency buckets (seconds) for the write-scatter phase histogram
/// ([`ScatterMetrics::phase_seconds`]).
///
/// Sized from Little's Law at the operating point
/// `bench/ladder/results/w1-write-ceilings.md` measured, because that is the only place
/// a bucket edge earns its resolution. That arm found
/// `pacer_scatter_windows_in_flight_peak` pinned at its limit at **both** 16 and 64
/// slots, which makes a window's slot-hold `slots × chunk_size ÷ per-node rate`:
/// 16 × 16 MiB ÷ 0.592 GiB/s ≈ **0.42 s** at the chart default, and
/// 64 × 16 MiB ÷ 0.878 GiB/s ≈ **1.14 s** with both ceilings lifted. So the decade
/// that decides ADR-0032 Phase 5 is 0.1–2 s, and it gets four interior edges rather
/// than one — a single edge there would put every window in one bucket and answer
/// nothing.
///
/// The bottom edges are not padding either. `served_stage` is a mutex plus a budget
/// compare, so it belongs in the microsecond floor, and a `served_stage` that starts
/// appearing at milliseconds is a lock convoy across `windowsInFlight × N` concurrent
/// offers — the one phase in this family whose *smallness* is the finding, which a
/// bucket set starting at 0.1 s could not show. The top edge is a stalled S3 call,
/// which has to be distinguishable from a merely slow one instead of saturating
/// `+Inf` beside it.
const SCATTER_PHASE_LATENCY_BUCKETS: &[f64] = &[
    0.000_1, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.0, 4.0, 8.0, 16.0,
];

/// Label separating the phases of a scattered window's life
/// ([`ScatterMetrics::phase_seconds`]). Named once so the metric and its five
/// observation sites cannot drift onto two spellings of one dimension — the same
/// reason [`DELIVERY_STAGE_LABEL`] exists.
const SCATTER_PHASE_LABEL: &str = "phase";

/// `phase` value for the coordinator's wait for a window slot — `WindowSlots::acquire`
/// inside `ScatterCoordinator::dispatch`. **The queueing term.**
///
/// The only phase observed on the body reader's own task, which is what makes it the
/// client's cost rather than the daemon's: while this is running `run_pipeline` is not
/// calling `body.next()`, so it is exactly the time the client spends backpressured
/// through TCP (`ebe07c07`).
pub const SCATTER_PHASE_PERMIT_WAIT: &str = "permit_wait";
/// `phase` value for a `StoreChunk` RPC that came back `Uploaded`: the window's bytes
/// out over the wire, the owner's whole handler, and the answer back.
///
/// **Not separable here, on purpose.** The RPC covers the owner's own `UploadPart`, so
/// a coordinator-side timer cannot say which of the two the time went to. That is what
/// [`SCATTER_PHASE_SERVED_STAGE`] and [`SCATTER_PHASE_SERVED_UPLOAD`] are for, and why
/// this constant's doc says so rather than leaving a reader to assume "remote" means
/// "network".
pub const SCATTER_PHASE_OWNER_RPC: &str = "owner_rpc";
/// `phase` value for a `StoreChunk` RPC that came back `Refused` — **the same wire
/// transfer with no S3 inside it.**
///
/// An owner refuses before it uploads anything (`PacerPeer::store_chunk` gates on
/// `try_stage` first), but by then it has already received the window, so this is the
/// coordinator→owner hop measured on its own. On any arm that refused at all it is a
/// direct read of the term ADR-0032 Phase 5 would replace, needing no subtraction and
/// no cross-node arithmetic.
pub const SCATTER_PHASE_OWNER_REFUSED: &str = "owner_refused";
/// `phase` value for a `StoreChunk` RPC that failed as a transport error. Separate from
/// [`SCATTER_PHASE_OWNER_RPC`] because its duration is a timeout, and folding one
/// timeout into the mean of a few hundred successful offers would move it by more than
/// the quantity being measured.
pub const SCATTER_PHASE_OWNER_FAILED: &str = "owner_failed";
/// `phase` value for a window the coordinator uploaded itself — `upload_here`: its own
/// `try_stage` plus its own `UploadPart`.
///
/// Deliberately covers the same two steps as
/// `served_stage + served_upload`, so the two are directly comparable and
/// `owner_rpc − local_upload` is a **single-node** estimate of the wire term (it assumes
/// only that an owner's `UploadPart` costs what this node's does, which on a homogeneous
/// fleet writing one bucket it should).
pub const SCATTER_PHASE_LOCAL_UPLOAD: &str = "local_upload";
/// `phase` value for `CompleteMultipartUpload` — **once per PUT, not per window.** The
/// durability point, serialised after every window, so it is a floor on a PUT's latency
/// that no amount of window parallelism removes.
pub const SCATTER_PHASE_COMPLETE: &str = "complete";
/// `phase` value for the staging reservation this node makes when it takes **somebody
/// else's** window (`PacerPeer::store_chunk`). Observed on the owner, not the
/// coordinator.
pub const SCATTER_PHASE_SERVED_STAGE: &str = "served_stage";
/// `phase` value for the `UploadPart` this node issues for **somebody else's** window.
/// Observed on the owner.
///
/// **The phase that decides ADR-0032 Phase 5.** If this accounts for nearly all of
/// [`SCATTER_PHASE_OWNER_RPC`], the remote leg is S3's per-part throughput and replacing
/// gRPC `StoreChunk` with a one-sided RDMA WRITE buys nothing; if it accounts for little
/// of it, the hop is the lever.
pub const SCATTER_PHASE_SERVED_UPLOAD: &str = "served_upload";

/// Label separating the stages of one chunk's delivery
/// ([`DeliveryMetrics::stage_seconds`]). Named once so the metric and both
/// observation sites cannot drift onto two spellings of the same dimension.
const DELIVERY_STAGE_LABEL: &str = "stage";
/// `stage` value for the chunk-entry read — foyer's RAM tier, or its disk tier
/// plus the entry codec's checksum and decode.
pub const DELIVERY_STAGE_CACHE_READ: &str = "cache_read";
/// `stage` value for moving a chunk's bytes into the client's window. Absent on
/// the `peer_rdma` path, where the holder's NIC writes the window instead, and
/// absent on a **token** target, which is written by [`DELIVERY_STAGE_RDMA_WRITE`]
/// rather than copied.
pub const DELIVERY_STAGE_COPY: &str = "copy";
/// `stage` value for the one-sided WRITE that places a chunk in a token target's
/// window — the token path's counterpart to [`DELIVERY_STAGE_COPY`].
///
/// **Why this exists.** Until 2026-09-12 the token path observed neither `copy`
/// (there is no memcpy) nor anything else, so `chunk_seconds` minus `cache_read`
/// was a 141–158 ms residual that `vllm-prelayout-where-the-load-goes.md` could
/// only name "the placement" and attribute by subtraction — 84–96 % of a
/// pre-layout 70B load, unmeasured. This and [`DELIVERY_STAGE_DIGEST`] close that
/// partition, so the token path sums like the mapped one does.
pub const DELIVERY_STAGE_RDMA_WRITE: &str = "rdma_write";
/// `stage` value for the source-side CRC32 over a delivered chunk, on the
/// blocking pool.
///
/// Observed **only when the token asked for a checksum**. A zero count beside a
/// non-zero chunk count is that gate working, not a gap: a digest the client
/// discards is a full pass over every delivered byte (131.4 GiB on a 70B load)
/// on the same pool the chunk store's `pread`s use.
pub const DELIVERY_STAGE_DIGEST: &str = "digest";

/// Times a delivery stage and records it **on drop**, so every exit path is
/// charged.
///
/// A stage that ends in `?` is the normal case here, not an edge one:
/// `Proxy::copy_window` returns early on a join failure and again on a refused
/// device copy. An `.observe()` written before each `return` would have to be
/// repeated at each of them, and the failure mode of forgetting one is a stage
/// that appears to get *faster* under exactly the conditions worth measuring.
/// A guard cannot be forgotten.
pub struct ScopedStage {
    /// The `stage`-labelled child, resolved once so the drop does no lookup.
    histogram: Histogram,
    /// When the stage began — set at construction, which is deliberately before
    /// any queueing the stage has to wait through.
    started: std::time::Instant,
}

impl ScopedStage {
    /// Start timing `stage`, recording into `vec` when the guard drops.
    #[must_use]
    pub fn new(vec: &HistogramVec, stage: &str) -> Self {
        Self {
            histogram: vec.with_label_values(&[stage]),
            started: std::time::Instant::now(),
        }
    }
}

impl Drop for ScopedStage {
    fn drop(&mut self) {
        self.histogram.observe(self.started.elapsed().as_secs_f64());
    }
}

/// Handles to every daemon counter, cheap to clone into request paths.
#[derive(Clone)]
pub struct Metrics {
    registry: Registry,
    /// S3 operations handled, labeled by operation name.
    pub ops_total: IntCounterVec,
    /// GETs served from the local cache.
    pub cache_hits: IntCounter,
    /// Cacheable GETs that missed the local cache.
    pub cache_misses: IntCounter,
    /// GETs that bypassed the cache per policy.
    pub cache_bypass: IntCounter,
    /// GETs carrying `If-Match` that the ETag check allowed onto the cache path
    /// (ADR-0039).
    ///
    /// Exists because a client whose *every* GET is conditional — Mountpoint-for-S3 is
    /// one — looks identical in `cache_hits`/`cache_bypass` whether ADR-0039 engaged or
    /// the request was turned away: both end in a bypass today and both end in a hit
    /// tomorrow. This counter is the only series that says the conditional itself was
    /// honoured, which is what the Mountpoint arm's unexplained 0 % hit rate needed.
    pub conditional_get_served: IntCounter,
    /// Whole-object cache fills that completed.
    pub fills_completed: IntCounter,
    /// Cache fills abandoned (client disconnect or short read).
    pub fills_aborted: IntCounter,
    /// Bytes served from the local cache.
    pub bytes_from_cache: IntCounter,
    /// Bytes written into the local cache.
    pub bytes_filled: IntCounter,
    /// How the retrying backend chunk read fared — see [`BackendReadMetrics`].
    pub backend_read: BackendReadMetrics,
    // ---- cluster tier (Phase 2) ----
    /// Misses resolved by fetching from the owning peer.
    pub peer_fetches: IntCounter,
    /// Peer fetch failed → direct backend GET without filling (ADR-0012).
    pub peer_fallbacks: IntCounter,
    /// Bytes received from peers (requester side).
    pub bytes_from_peers: IntCounter,
    /// FetchBlob requests served from this node's cache or read-through.
    pub peer_serves: IntCounter,
    /// Subset of `peer_serves` delivered via a one-sided RDMA WRITE rather than
    /// gRPC streaming (ADR-0018). The RDMA-served fraction (`peer_serves_rdma /
    /// peer_serves`) is the A4 benchmark's correctness gate: ≈1.0 confirms the
    /// EFA path carried the load, 0 confirms a clean gRPC baseline with no
    /// silent fallback.
    pub peer_serves_rdma: IntCounter,
    /// FetchBlob requests answered NOT_FOUND (no read-through applicable).
    pub peer_misses: IntCounter,
    /// FetchBlob misses this node owned and read through to the backend.
    pub peer_readthroughs: IntCounter,
    /// Bytes sent to peers (server side).
    pub bytes_to_peers: IntCounter,
    /// Peer-owned chunks kept locally after passing the frequency gate
    /// (ADR-0016 layer 1 requester-local admission).
    pub local_admits: IntCounter,
    /// Fills currently claimed through a `FillGuard` (private to `proxy.rs`) — the
    /// occupancy of the node-wide fill registry, on every path that claims it:
    /// `maybe_admit_local`, `maybe_fill`, ADR-0040's leading read
    /// (`FillCtx::fetch_owned`) and the peer server's read-through.
    ///
    /// Note what ADR-0040 changed about the *shape* of this gauge without changing
    /// its definition: a leader now holds its claim across the whole backend read,
    /// so a cold multi-chunk read keeps `fill_parallelism` claims up for the
    /// duration of the reads rather than only for the inserts that follow them. A
    /// step change here at that ADR is the mechanism engaging, not a leak.
    pub fill_inflight: IntGauge,
    /// Guarded fills (see [`Self::fill_inflight`]) whose `FillGuard` dropped
    /// before calling `complete()` — i.e. the fill's future was cut short
    /// rather than finishing on its own.
    ///
    /// The case this exists to catch: `chunked_body`'s `stream::buffered`
    /// pipeline drops an in-flight chunk resolution's future outright when the
    /// client disconnects mid-stream, which used to leave the key in `filling`
    /// forever — silently, with no metric, so that chunk could never be filled
    /// again until the daemon restarted. Every increment here is exactly that
    /// event, now visible.
    pub fill_abandoned: IntCounter,
    /// Reads served from another request's in-flight fill (ADR-0040) — see
    /// [`FillCoalesceMetrics`].
    pub fill_coalesce: FillCoalesceMetrics,
    /// Client-memory delivery (ADR-0026, planning/19 Track C) — see
    /// [`DeliveryMetrics`].
    pub delivery: DeliveryMetrics,
    /// Write scatter (ADR-0032) — see [`ScatterMetrics`].
    pub scatter: ScatterMetrics,
    /// S3 listener bounds (ADR-0036) — see [`ListenerMetrics`].
    pub listener: ListenerMetrics,
    /// ADR-0028's cache slab — see [`SlabMetrics`]. Registered on every build so
    /// the scrape surface does not change with the feature set; the series simply
    /// stay at 0 where there is no slab to fill.
    pub slab: SlabMetrics,
    /// ADR-0033's chunk store — see [`ChunkStoreMetrics`]. Registered on every build,
    /// like the slab, so the scrape surface does not change with `config.diskTier`; on
    /// the `foyer` tier every series stays 0, which is the truth rather than a gap.
    pub chunk_store: ChunkStoreMetrics,
    /// The cache directory's backing block devices — see [`DeviceIoMetrics`]. The only
    /// series here that evidences a byte reaching flash rather than the page cache.
    pub device_io: DeviceIoMetrics,
    /// Directory-shard RPC service time (seconds), labeled `op` =
    /// `lookup`/`admit`/`evict`. Times only the in-lock directory op
    /// ([`pacer_ring::directory`]), not the surrounding tonic/gRPC frame, so it
    /// isolates the metadata-shard CPU that the B4 Step-6 projection scales to
    /// N=1000 (planning/15, ADR-0020). The histogram's `_count` child series
    /// doubles as the per-shard RPC counter — under a restore storm, every
    /// miss (`lookup`) and every layer-1 admit (`admit`) for the hottest chunk
    /// converges on that chunk's single home, so the `op="lookup"` rate on the
    /// busiest node is the directory hotspot detector.
    pub dir_rpc_seconds: HistogramVec,
    /// Current ring size (kept as a counter-pair-free gauge via set).
    pub ring_members: IntGauge,
    /// Cumulative process CPU time in seconds (`utime + stime` from
    /// `/proc/self/stat`), refreshed on each scrape. Named to match the
    /// node_exporter convention so the A4 harness can derive utilization as
    /// `Δcpu_seconds / (Δwall × cores)` — CPU-per-GiB is RDMA's headline win
    /// (holder CPU off the one-sided-WRITE data path). Stays at 0 on non-Linux,
    /// where the handle is only retained for registration, never refreshed.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    process_cpu_seconds: Gauge,
    /// Resident set size in bytes (`VmRSS` from `/proc/self/status`), refreshed on
    /// each scrape. Named to match the node_exporter/client_golang convention.
    ///
    /// The daemon exported CPU but NOT memory, which made an OOM kill invisible
    /// twice over: the container is killed by the kernel, so nothing reaches the
    /// log, and there was no memory series to alert on or to plot afterwards. That
    /// is not hypothetical — a 2026-08-21 restore OOMKilled the daemon (exit 137)
    /// and the only evidence was the exit code, with the last log line being a
    /// routine directory sweep.
    ///
    /// What RSS is good for: the daemon's memory is NOT all foyer-accounted — an
    /// idle daemon with an empty cache already holds ~8 GiB resident (the registered
    /// RDMA arena, pinned at startup). Plotted against `foyer_memory_usage` it
    /// separates "the cache tier grew" from "something outside the cache grew",
    /// which is the first question worth asking.
    ///
    /// ⚠ **What RSS is NOT.** This comment used to claim "RSS is the right series
    /// for this because it is what the cgroup limit acts on". **That is false, and
    /// it mattered.** The limit acts on `memory.current`, which *includes page
    /// cache*; `VmRSS` excludes the page cache that buffered `read()`/`pread()`
    /// creates. foyer's disk tier does buffered I/O, so on a large-memory node the
    /// "NVMe tier" largely IS page cache — 72.1 GiB of `Cached` for one 131 GiB
    /// checkpoint, against `read_bytes: 0` on the process
    /// (`bench/ladder/results/c4-fanout-depth.md`). A daemon can therefore hold this
    /// gauge flat to the byte while its cgroup marches to the limit and the kernel
    /// kills it.
    ///
    /// So this gauge cannot answer "are we about to be OOMKilled", and neither can
    /// `bench/ladder/memwatch.sh`, which subtracts two process-level series. The
    /// series that can is [`CgroupGauges`], beside it — read that one for
    /// proximity-to-death and this one for what the process itself owns.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    process_resident_memory_bytes: Gauge,
    /// Established TCP connections from this daemon to the backend's HTTPS port, and how
    /// many DISTINCT remote addresses they reach. See [`parse_established_peers`] for the
    /// question these two answer together and why neither answers it alone.
    ///
    /// Gauges rather than counters because they are an occupancy, and refreshed at scrape
    /// like [`Self::process_cpu_seconds`] so a `/metrics` bracket taken around an arm
    /// carries them without the driver having to run anything inside the pod.
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    backend_connections: Gauge,
    /// Distinct remote addresses among [`Self::backend_connections`].
    #[cfg_attr(not(target_os = "linux"), allow(dead_code))]
    backend_peer_endpoints: Gauge,
    /// The cgroup's own accounting — **the numbers the kernel kills on**, which the
    /// gauge above is not. See [`CgroupGauges`].
    cgroup: CgroupGauges,
    /// The C allocator's own accounting, which is what makes the RSS gauge above
    /// *actionable* rather than merely alarming. See [`MallocGauges`].
    malloc: MallocGauges,
    /// What this daemon is *configured* to hold, term by term — the third leg of the
    /// memory story beside the cgroup's measurement and the allocator's. See
    /// [`MemoryBudgetGauges`].
    memory_budget: MemoryBudgetGauges,
    /// The RDMA serve-path gauges and the transport they scrape from — present
    /// only in an `efa` build (grouped so the feature-gated surface is one
    /// field, not one per gauge). See [`RdmaMetrics`].
    #[cfg(feature = "efa")]
    rdma: RdmaMetrics,
}

/// Backend chunk-read counters (`pacer_backend::retry`): did a transient S3 fault
/// cost the client anything?
///
/// The pair that matters is `retries` vs `failures`. Both come from the same
/// event — a chunk read that did not work first time — and the difference is
/// whether PACER absorbed it. Before the retry existed there was only the second
/// case and it was invisible: one severed body truncated a whole client GET, whose
/// `200` and `Content-Length` had already been sent, leaving nothing behind but a
/// `warn!` line. Counted separately rather than as one "read errors" series
/// because their meanings for an operator are opposite — a `retries` rate is the
/// backend being flaky, a `failures` rate is a client being served short.
#[derive(Clone)]
pub struct BackendReadMetrics {
    /// Chunk reads re-issued after a transient failure. Counts *retried attempts*,
    /// not reads, so one read that succeeded on its third try adds 2.
    pub retries: IntCounter,
    /// Chunk reads that gave up, labeled `outcome` = `exhausted` (every attempt
    /// hit a retryable fault) or `permanent` (a fault retrying cannot fix, e.g.
    /// `AccessDenied`). A missing key is not counted — a 404 is an answer, not a
    /// failure.
    ///
    /// **This is the alertable one**: each increment is a client GET that failed
    /// or was truncated, and the label says whether to look at the backend's
    /// health (`exhausted`) or at this daemon's credentials and request shape
    /// (`permanent`).
    pub failures: IntCounterVec,
}

/// ADR-0040's single flight, as an operator sees it: what it saved, and how often
/// it tried and could not.
///
/// Three series rather than one because the question "did it engage?" and the
/// question "was it worth it?" have different answers, and before this the read
/// path had neither: duplicated backend GETs were invisible, inferable only from
/// the gap between `pacer_cache_misses_total` and `pacer_fills_completed_total`,
/// which also folds in every read that was never admitted.
#[derive(Clone)]
pub struct FillCoalesceMetrics {
    /// Chunk reads answered from a fill another request already had in flight.
    /// Each one is a backend ranged GET this node did **not** issue.
    pub served: IntCounter,
    /// Bytes those reads returned — i.e. backend traffic avoided.
    ///
    /// **The headline number**, and the reason [`Self::served`] is not enough on
    /// its own: chunks differ in size (ADR-0015 clamps the last one to the object),
    /// so a count cannot be turned into bytes by multiplying by `chunkSize`.
    pub bytes: IntCounter,
    /// Requests parked on another request's fill **right now**.
    ///
    /// The only one of these four readable *during* an incident rather than after it.
    /// The counters say a fan-in was absorbed; this says one is being absorbed — which is
    /// the difference between "the single flight is working" and "a stuck leader is
    /// holding N requests", the one failure mode ADR-0040's coupling introduces. Pinned
    /// here with [`Self::served`] flat is that incident.
    pub waiters: IntGauge,
    /// Reads that waited on a leader and got nothing — the leader's own read failed,
    /// or its client disconnected and took the future with it — and so fell back to
    /// fetching for themselves.
    ///
    /// Not an error series: a fallback is the pre-ADR-0040 path, so every increment
    /// is a request that lost the optimisation and nothing else. A *rate* here
    /// tracking `pacer_backend_read_failures_total` is the backend being unhealthy;
    /// a rate here without that is clients disconnecting mid-read.
    pub fallbacks: IntCounter,
}

/// Allocator gauges (`mallinfo2`, see [`crate::memstats`]): of the resident bytes
/// that are NOT in foyer, how many does the process still own and how many has
/// the allocator merely kept?
///
/// The pair that matters is `in_use` vs `free_retained`, and the reason it is a
/// pair is planning/17's open defect: a requester grew 15.27 GiB outside foyer on
/// a 131 GiB restore and eventually OOMKilled, and RSS cannot distinguish a
/// retention bug in our code (fix: find the owner) from glibc keeping freed
/// chunk-sized blocks on a per-thread arena's free list (fix: `MALLOC_ARENA_MAX`
/// in the environment, no rebuild). Both curves look identical in RSS; these two
/// series tell them apart in a single scrape.
///
/// Grouped rather than flattened into [`Metrics`] for the same reason as its
/// neighbours: one mechanism, one field, and a caller reads all four or none.
#[derive(Clone)]
struct MallocGauges {
    /// [`crate::memstats::MallocStats::heap_bytes`].
    heap: Gauge,
    /// [`crate::memstats::MallocStats::mmapped_bytes`].
    mmapped: Gauge,
    /// [`crate::memstats::MallocStats::in_use_bytes`] — live, ours, cache included.
    in_use: Gauge,
    /// [`crate::memstats::MallocStats::free_retained_bytes`] — freed by us, kept
    /// by the allocator.
    free_retained: Gauge,
}

/// The memory budget the daemon was *configured* with ([`crate::memory_budget`]):
/// **what did this pod promise to hold, and does the limit cover it?**
///
/// The pair that matters is `pacer_memory_budget_total_bytes` against
/// `pacer_cgroup_memory_max_bytes`, and it is the one comparison neither
/// [`CgroupGauges`] nor [`MallocGauges`] can make: those measure, this *declares*. A
/// dashboard plotting the three together separates "the configuration never fitted"
/// from "the configuration fitted and something grew" — which is exactly the
/// distinction the recorded OOM kills could not be told apart on, and which decides
/// whether the fix is a chart value or a code path.
///
/// Static for the process's life, published once at startup, and deliberately so: the
/// interesting quantity is the promise, not a sample of it.
#[derive(Clone)]
struct MemoryBudgetGauges {
    /// One series per [`crate::memory_budget::Term`], labelled `term` (its stable
    /// label) and `counted` (whether the chart's `pacer.memoryLimit` accounts for it,
    /// and therefore whether it is in the total below). Two fixed label dimensions
    /// over a closed enum, so the cardinality is a constant — the discipline
    /// `pacer_rdma_rail` keeps for the per-rail family.
    ///
    /// `sum(pacer_memory_budget_bytes{counted="true"})` is the total; summing the
    /// whole family instead double-counts nothing but adds the advisory terms, which
    /// is a different (and also useful) question.
    terms: prometheus::GaugeVec,
    /// The enforced sum — the number the startup check compared against
    /// `memory.max`. Its own series rather than a `term="total"` label so a naive
    /// `sum()` over the family above cannot double-count it.
    total: Gauge,
}

/// The cgroup's memory accounting ([`crate::cgroup`]): **how close is this
/// container to the limit that ends it?**
///
/// The pair that matters is `current` vs `max`, and the reason this struct exists
/// beside [`MallocGauges`] at all is that neither those nor
/// [`Metrics::process_resident_memory_bytes`] can answer that question. All three
/// process-level series exclude page cache; the limit does not. foyer's buffered
/// disk tier makes page cache the *largest* term on a large-memory node (72.1 GiB
/// for a 131 GiB checkpoint), so a daemon can be flat in every other series here
/// and still be killed — which is what makes planning/17's two FLAT memgap arms
/// inconclusive rather than clearing.
///
/// Deliberately raw quantities and no verdict, exactly like [`MallocGauges`]: the
/// arithmetic that matters (`current / max`, and whether `file` or `anon` is
/// driving it) needs the time axis, and that lives in the sampler.
///
/// Every gauge is registered on every platform, so the scrape surface does not vary
/// with the target, and left unset where the field is absent — see
/// [`Metrics::refresh_cgroup`] for why "unset" rather than "0".
#[derive(Clone)]
struct CgroupGauges {
    /// [`crate::cgroup::CgroupMemory::current_bytes`] — the quantity the limit is
    /// compared against.
    current: Gauge,
    /// [`crate::cgroup::CgroupMemory::max_bytes`] — the limit itself, or infinity
    /// when the cgroup is unlimited (rendered `inf`; see
    /// `an_unlimited_limit_encodes_as_infinity_not_zero`).
    max: Gauge,
    /// [`crate::cgroup::CgroupMemory::file_bytes`] — page cache, the term RSS cannot
    /// see.
    file: Gauge,
    /// [`crate::cgroup::CgroupMemory::file_dirty_bytes`] — page cache that cannot be
    /// reclaimed until writeback finishes.
    file_dirty: Gauge,
    /// [`crate::cgroup::CgroupMemory::file_writeback_bytes`].
    file_writeback: Gauge,
    /// [`crate::cgroup::CgroupMemory::anon_bytes`] — the part of `current` that
    /// behaves like the process-level series.
    anon: Gauge,
    /// [`crate::cgroup::CgroupMemory::oom_events`] — reclaim failures under the
    /// limit. **The alertable one**: it advances while the daemon is still alive.
    oom: Gauge,
    /// [`crate::cgroup::CgroupMemory::oom_kill_events`] — kills, with the scope
    /// caveat documented on that field.
    oom_kill: Gauge,
}

/// S3 listener bounds (ADR-0036): is the connection cap binding, and is `accept`
/// failing?
///
/// The cap backpressures at the accept queue, which means a client at the ceiling
/// sees a slow connect and nothing else — no status code, no log on its side. So
/// the ceiling is invisible to everyone unless the daemon says so, and these three
/// series are the only place it does.
///
/// Read them together. `connections_active` near [`crate::listen::ListenLimits`]'s
/// `max_connections` with `connections_at_capacity_total` advancing is a cap that
/// binds — either raise `PACER_S3_MAX_CONNECTIONS` or find the client that is
/// leaking sockets. `connections_at_capacity_total` flat while `connections_active`
/// sits high is a fleet operating as designed. `accept_errors_total` advancing is
/// descriptor or memory pressure on the node, which the cap cannot fix.
#[derive(Clone)]
pub struct ListenerMetrics {
    /// Connections the S3 listener currently holds open. A gauge, not a counter:
    /// the question is always "how close to the cap right now".
    pub connections_active: IntGauge,
    /// Times the accept loop found no free permit and had to wait for a
    /// connection to close. One increment is one moment at the ceiling, not one
    /// rejected client — nothing is rejected, so a rejection counter would stay
    /// at zero while the node throttled every connect.
    pub connections_at_capacity: IntCounter,
    /// `accept` calls that failed transiently (`EMFILE`, `ENOBUFS`,
    /// `ECONNABORTED`, …) and were retried after a backoff. Before ADR-0036 the
    /// first of these ended the accept loop and took the S3 endpoint down while
    /// the pod stayed Ready, so this series is the one that would have named that
    /// failure.
    pub accept_errors: IntCounter,
}

/// Cache-slab counters (ADR-0028): is the cache's RAM tier actually IN registered
/// memory, and is the slab big enough to keep it there?
///
/// The pair that matters is `stores` vs `heap_fallbacks`. ADR-0028's whole payoff
/// is the serve path posting a WRITE out of the cache, which only happens for
/// chunks that got a frame; a `heap_fallbacks` rate that is not ~0 means the slab
/// is undersized for the node's in-flight chunk count and the pinned memory is
/// being wasted on a design that has quietly stopped applying. That failure is
/// silent in every other metric — throughput just looks like the pre-slab
/// numbers — which is why these two are counted separately rather than as one
/// "fills" counter with a ratio to infer.
#[derive(Clone)]
pub struct SlabMetrics {
    /// Chunks stored INTO a slab frame — i.e. cached in registered memory, and
    /// servable by a holder with no staging copy.
    pub stores: IntCounter,
    /// Chunks that wanted a frame and got the heap instead (no free frame). See
    /// the type doc: anything but ~0 is a sizing bug, not a transient.
    pub heap_fallbacks: IntCounter,
    /// Frames currently holding a resident chunk. Refreshed on each store; with
    /// the slab's frame count this is cache occupancy in the units admission
    /// works in, which a byte-denominated capacity gauge cannot show.
    pub frames_in_use: Gauge,
    /// The slab's REGISTERED SIZE in bytes — the whole mapping, not the occupied
    /// part — or 0 when no slab is configured. Published once at startup, because
    /// that is when it becomes true: the slab is mapped, registered and therefore
    /// **resident** before a single chunk is cached.
    ///
    /// It exists because memory accounting is otherwise wrong the moment ADR-0028
    /// is switched on. `process_resident_memory_bytes − foyer_memory_usage` was a
    /// valid "how much memory is outside the cache" only while cached bytes sat on
    /// the heap: with a slab, RSS carries the entire mapping from boot while
    /// `foyer_memory_usage` counts just the entries in it, so that difference
    /// *shrinks* as the cache warms and can hide real growth behind it. Subtract
    /// `max(foyer_memory_usage, pacer_cache_slab_bytes)`, never the sum — occupied
    /// frames are counted by BOTH series (see `bench/ladder/memwatch.sh`).
    pub bytes: Gauge,
}

/// The cache directory's backing block devices, straight out of `/proc/diskstats`.
///
/// **The only series in this registry that is not the daemon's opinion of itself.** Every
/// tier rate here — foyer's and the chunk store's alike — is computed from the daemon's own
/// atomics, which count bytes the tier *served*, not bytes a device *moved*. The two differ
/// by the whole page cache, and on a node with 2 TiB of RAM against a working set that fits
/// it, they differ silently and in the flattering direction: the store's writes are buffered
/// and nothing drops those pages, so a read straight after a seed is a memory read reported
/// as a disk rate. `results/c4-foyer-readpath.md` is that failure with numbers (foyer claimed
/// 428.45 GB read while `md127` moved 0.58 GiB).
///
/// So the honesty check is `store_read_bytes` over a delta of these: about 1.0 means the tier
/// read the device, and ~0 means it read RAM. It is the ratio
/// `clients/python/pacer_nvme_report.py` calls `dev_over_fio` for the fio probe, applied to
/// the daemon — and it is the check `bench/ladder/c5-safetensors.sh` and `bench/ladder/README.md`
/// have both demanded in writing since before anything could answer them.
///
/// Gauges rather than counters, for the same reason as [`ChunkStoreMetrics`]: the value is
/// read from outside the process at scrape time, so there is nothing to increment and nothing
/// that can drift from the kernel's own view.
#[derive(Clone)]
pub struct DeviceIoMetrics {
    /// The devices backing the cache directory, resolved once at startup by
    /// [`crate::diskstats::backing_devices`]. Empty when the chain could not be resolved
    /// (no procfs, an overlay cache dir) — in which case both gauges below stay absent
    /// rather than reporting a device nobody chose.
    ///
    /// `Arc<OnceLock<_>>` for exactly the reason [`ChunkStoreMetrics::store`] is: [`Metrics`]
    /// is cloned into every request path, and a bare cell would leave clones taken before
    /// [`Metrics::set_cache_dir`] permanently empty depending on startup order.
    devices: std::sync::Arc<std::sync::OnceLock<Vec<String>>>,
    /// Cumulative bytes read from each backing device since boot, labelled `device`.
    ///
    /// **Sum the family freely**: only array MEMBERS are published, never the array beside
    /// them, so there is no double count to remember (see [`crate::diskstats::resolve`]).
    pub read_bytes: prometheus::GaugeVec,
    /// Cumulative bytes written to each backing device since boot, labelled `device`.
    /// What says whether a seed reached flash or is still sitting in dirty pages.
    pub written_bytes: prometheus::GaugeVec,
}

/// The ADR-0033 chunk-store series, and the reason each one exists.
///
/// All **gauges**, refreshed from the store's atomics at scrape time rather than
/// incremented at the call site. That is deliberate: the store lives in `pacer-cache`,
/// which has no Prometheus registry (the same constraint that makes
/// `frames::FrameSource` a trait), and a counter incremented from there would need
/// either a metrics dependency in that crate or a callback per read on the hot path.
/// Reading nine atomics once per scrape costs nothing and cannot drift from the store's
/// own view — there is only one source of truth.
///
/// The cost of that choice, stated so nobody reads these as counters: they are
/// monotonically-increasing gauges, so `rate()` works but `increase()` over a daemon
/// restart will show the reset as a drop. Every prior tier metric here has the same
/// shape.
#[derive(Clone)]
pub struct ChunkStoreMetrics {
    /// Set once the daemon has a store to read. Absent on `diskTier=foyer`, where every
    /// gauge below stays 0 — which is the truth, not a missing metric.
    ///
    /// `Arc<OnceLock<_>>`, not a bare `OnceLock`: [`Metrics`] is `Clone` and is cloned
    /// into every request path, and cloning a `OnceLock` copies its *current* state — so
    /// a bare cell would leave every clone taken before [`Metrics::set_chunk_store`]
    /// permanently empty, and the /metrics handler's clone would report zeros forever
    /// depending on startup order. Sharing one cell removes the ordering requirement
    /// instead of documenting it.
    store: std::sync::Arc<std::sync::OnceLock<pacer_cache::store::ChunkStore>>,
    /// Reads served from the tier.
    pub hits: Gauge,
    /// Reads with no slot for the key.
    pub misses: Gauge,
    /// Body bytes served. **The numerator of gate 33.5's per-node read rate** — and not
    /// `hits × chunk_size`, because a short last chunk is a hit for far fewer bytes.
    pub read_bytes: Gauge,
    /// Body bytes written.
    pub written_bytes: Gauge,
    /// **Gate 33.4.** Mean seconds per hit, the like-for-like comparison against foyer's
    /// 40–75 ms (`foyer_storage_disk_io_duration`'s sum ÷ count) and against the device's
    /// own 1.335 ms for the same 16 MiB.
    pub read_seconds_mean: Gauge,
    /// Longest single read, in seconds. A mean plus a max is what atomics can honestly
    /// carry; this is what shows a stall the mean hides.
    pub read_seconds_max: Gauge,
    /// **What a hit actually COSTS** — mean seconds of service time, measured inside the
    /// blocking task, so it excludes the wait for a blocking thread. This is the number to
    /// compare against the device's own 1.335 ms for a 16 MiB read, and the one ADR-0033
    /// gate 33.4 should have been written against; `read_seconds_mean` beside it is
    /// queue-inclusive and answers a different question.
    pub service_seconds_mean: Gauge,
    /// Longest single read's service time, in seconds.
    pub service_seconds_max: Gauge,
    /// Mean seconds a hit spent waiting for a blocking thread — `read` minus `service`.
    /// The quantity that made 26-31 ms per hit look like a cost when it was Little's law.
    pub queue_seconds_mean: Gauge,
    /// **Summed** service seconds over every hit — the numerator the `_mean` above collapses.
    ///
    /// Exposed because a mean over *all hits since startup* cannot be attributed to an
    /// interval, and gate 33.4 is a statement about one arm. Divided by `Δhits` over the same
    /// two scrapes this gives that arm's own per-hit cost; the mean beside it cannot, and read
    /// as if it could it flatters the gate — the C5 arm's verification pass issues tens of GiB
    /// of ranged GETs at low concurrency *after* the timed load, and every one of those fast
    /// reads pulls the lifetime mean down
    /// (`bench/ladder/results/c5-dcp-tier-accounting.md`).
    pub service_seconds_total: Gauge,
    /// Summed queue-inclusive seconds over every hit, for the same reason as
    /// [`ChunkStoreMetrics::service_seconds_total`] — and so that `Δread − Δservice` gives an
    /// interval's queueing rather than a difference of two lifetime means.
    pub read_seconds_total: Gauge,
    /// Chunks written, and writes skipped because the key was already held.
    pub writes: Gauge,
    /// Writes that were a no-op because chunks are immutable.
    pub write_dedups: Gauge,
    /// Slots reused, evicting the least-recently-used chunk.
    pub evictions: Gauge,
    /// **The one to alert on.** A slot whose header named a different key than the index
    /// did — always a bug or a torn write, and the read was refused rather than served.
    /// Any non-zero value here means the check ADR-0033 added in place of foyer's
    /// unconditional checksum has caught something, and it should be zero forever.
    pub key_mismatches: Gauge,
    /// Body CRC failures. Only ever non-zero with `verifyChunkBody` on.
    pub crc_mismatches: Gauge,
    /// Slots whose header was present but impossible.
    pub corrupt_slots: Gauge,
    /// I/O errors on a read or write.
    pub io_errors: Gauge,
    /// Chunks held, and slots the tier has — cache occupancy in the unit the tier
    /// actually allocates in, which a byte capacity cannot show.
    pub slots_used: Gauge,
    /// Total slots.
    pub slots_capacity: Gauge,
    /// **Gate 33.7.** Seconds the startup scan took, and how many chunks it recovered.
    /// Together they say whether an in-memory index over a multi-TB tier is practical,
    /// and whether a restart started warm.
    pub scan_seconds: Gauge,
    /// Chunks the scan recovered from slot headers.
    pub scan_recovered: Gauge,
}

/// The write-scatter series (ADR-0032).
///
/// Between them these answer the one question the design cannot answer from first
/// principles: **is the scatter engaging, and if not, why not?** On the measured
/// customer shape the honest expectation is that `declined{reason="..."}` and
/// `windows{role="local"}` dominate while `windows{role="owner"}` stays small — so
/// these have to distinguish "working as designed" from "silently doing nothing",
/// which a single engaged/not counter could not.
#[derive(Clone)]
pub struct ScatterMetrics {
    /// PUTs that took the scatter path.
    pub scattered: IntCounter,
    /// PUTs that did not, by reason — `too_small`, `overwrite`, `client_checksum`,
    /// `no_content_length`, `too_many_parts`, and the two misconfiguration labels.
    /// Every one of these fell through to ADR-0007's path, which is never wrong,
    /// only colder.
    pub declined: IntCounterVec,
    /// Windows uploaded, by `role`: `owner` when a chunk's home took it, `local`
    /// when this node did. The ratio *is* the scatter's effectiveness — all `local`
    /// means reject-fast degraded the write to a plain parallel upload with a local
    /// populate, which is correct but buys nothing.
    pub windows: IntCounterVec,
    /// Windows uploaded but not staged anywhere, because a staging budget was full.
    /// Durable in S3, absent from every cache — each one is a later miss, and a
    /// rising rate here means the budget is the binding constraint.
    pub uncached_windows: IntCounter,
    /// Distinct nodes that took at least one window, summed per PUT. Divided by
    /// [`Self::scattered`] it gives the mean fan-out, which the design predicts
    /// approaches the fleet size; a mean near 1 means the scatter is not spreading.
    pub owners_engaged: IntCounter,
    /// Offers an owner refused, by `reason`. Expected to be *large* on a balanced
    /// all-ranks save — that is reject-fast working, not a fault.
    pub refusals: IntCounterVec,
    /// **Where a coordinator's window slot actually goes**, by `phase` — the
    /// measurement `bench/ladder/results/w1-write-ceilings.md` closed on and could not
    /// take (§ *What this arm could not settle*, item 3).
    ///
    /// That arm established the coordinator as the write path's wall:
    /// `windows_in_flight_peak` pinned at `_limit` at client concurrency 1, on five
    /// nodes, at both `wif=16` and `wif=64`, and lifting the ceiling 4× bought 1.48×
    /// without un-pinning it. What nothing could say was *what occupies a slot*. A slot
    /// is held for one window's whole upload, and until this series existed that life
    /// was one undivided number, so an arm could not tell an expensive network hop from
    /// an expensive `UploadPart` inside the peer — and those two readings point at
    /// opposite decisions about ADR-0032 Phase 5.
    ///
    /// The phases, and which side of the wire observes each:
    ///
    /// | phase | observed on | covers |
    /// |---|---|---|
    /// | [`SCATTER_PHASE_PERMIT_WAIT`] | coordinator | the wait for a slot — the queueing term, and the client's backpressure |
    /// | [`SCATTER_PHASE_OWNER_RPC`] | coordinator | a `StoreChunk` that was taken: wire + the owner's *whole* handler |
    /// | [`SCATTER_PHASE_OWNER_REFUSED`] | coordinator | a `StoreChunk` that was refused: the same wire, **no S3 in it** |
    /// | [`SCATTER_PHASE_OWNER_FAILED`] | coordinator | a `StoreChunk` that errored (a timeout, kept out of the mean above) |
    /// | [`SCATTER_PHASE_LOCAL_UPLOAD`] | coordinator | `upload_here`: its own stage + its own `UploadPart` |
    /// | [`SCATTER_PHASE_COMPLETE`] | coordinator | `CompleteMultipartUpload`, **once per PUT** |
    /// | [`SCATTER_PHASE_SERVED_STAGE`] | **owner** | `try_stage` for somebody else's window |
    /// | [`SCATTER_PHASE_SERVED_UPLOAD`] | **owner** | the `UploadPart` for somebody else's window |
    ///
    /// # Why the owner reports on its own side rather than over the wire
    ///
    /// The `StoreChunk` RPC covers the owner's own `UploadPart`, so the coordinator's
    /// timer physically cannot split network hop from peer-side S3. Two ways to fix
    /// that: have the owner return its split in `StoreChunkResponse`, or have the owner
    /// publish its own series. **The second is what this does**, for one decisive
    /// reason: an owner running a build without the field reports nothing, so
    /// `owner_rpc − 0` would attribute the *entire* RPC to the wire — precisely the
    /// "the network is slow" misreading that would send someone to build RDMA for
    /// nothing. A missing `served_*` series is instead visibly a gap, which a reader
    /// cannot mistake for a measurement. The cost is that the subtraction crosses
    /// nodes; see the harness note below.
    ///
    /// # Turning these into "the wall is X"
    ///
    /// Three *independent* estimates of the wire term, which is what makes the
    /// conclusion robust rather than resting on one arithmetic:
    ///
    /// 1. `owner_refused` alone — the hop with no S3 in it. Only available on an arm
    ///    that refused, which is exactly the arms `refused{budget_exhausted}` names.
    /// 2. `owner_rpc − (served_stage + served_upload)`, summed over the fleet. Exact
    ///    populations on a single-writer arm (every remote window the one coordinator
    ///    sent is a window some other node served) and symmetric on a balanced one;
    ///    approximate on an asymmetric arm, which is the caveat to state.
    /// 3. `owner_rpc − local_upload` — single-node, assuming an owner's `UploadPart`
    ///    costs what this node's does.
    ///
    /// If those agree and are small, the remote leg is S3's per-part throughput and
    /// Phase 5's RDMA WRITE buys that fraction and no more. If they agree and are
    /// large, the hop is the lever.
    ///
    /// # Cost
    ///
    /// Two `Instant::now()` reads and one `observe` per phase — a vDSO clock read plus
    /// a 12-edge bucket scan and three relaxed atomics, so ~200 ns. At most six
    /// observations attach to one window, against a slot-hold Little's Law puts at
    /// ~1.14 s: about 10⁻⁶ of the quantity being measured, and ~330 observations per
    /// second per node at the rate that arm ran.
    pub phase_seconds: HistogramVec,
    /// How close this node came to the scatter's two *bounds* — see
    /// [`ScatterBoundGauges`]. Private because nothing outside this module sets them:
    /// they are refreshed at scrape from the staging area and the coordinator's own
    /// slots, which are the single sources of truth.
    bounds: ScatterBoundGauges,
}

impl ScatterMetrics {
    /// Record `elapsed` against one `phase` of [`Self::phase_seconds`].
    ///
    /// `phase` must be one of the `SCATTER_PHASE_*` constants. A helper rather than
    /// five open-coded `with_label_values` calls so the label *arity* is stated once:
    /// this family has exactly one label, and a call with the wrong number of values
    /// would panic at the observation site instead of failing to compile.
    ///
    /// Timed by the caller, not by a guard, because two of the phases choose their
    /// label from the *outcome* of the await they are timing — a refused offer and an
    /// accepted one are different phases — and a drop guard cannot pick a label after
    /// the fact.
    pub fn observe_phase(&self, phase: &str, elapsed: std::time::Duration) {
        self.phase_seconds
            .with_label_values(&[phase])
            .observe(elapsed.as_secs_f64());
    }
}

/// The scatter's two bounds, and how close a node actually came to each (ADR-0032 § 4,
/// `planning/24-write-path.md`).
///
/// **Why these exist at all.** Every other series in [`ScatterMetrics`] is a monotonic
/// `_total`, so `refusals{reason="budget_exhausted"}` says refusals *happened* and
/// nothing said whether either bound was at its ceiling when they did — which is exactly
/// the discriminator an arm needs, leaving its central prediction unfalsifiable on
/// hardware:
///
/// * **staged bytes vs the budget.** At the budget, staging is *residency*-bound (a
///   window is held until its object's Complete, and Complete waits for every window),
///   so no coordinator-ordering fix moves it and only ADR-0032 Phase 5's reservation, a
///   per-part commit, or staging to disk can. Well below it, with refusals still
///   present, the cause is something else — a racing upload, the reaper, ownership skew.
/// * **windows in flight vs `windowsInFlight`.** The direct check that `ebe07c07`'s byte
///   bound holds on a real daemon: the peak must never exceed the limit, and how close
///   it gets is how much of the chart's coordinator headroom is actually used.
///
/// **Both peaks are latched at the event, not sampled at the scrape** — in
/// `StagingArea::try_stage_at` and `WindowSlots::acquire` respectively — and only
/// *published* here. Prometheus scrapes every 15-60 s and this repo's Grafana has a 60 s
/// rate floor, while both quantities fill and drain inside one PUT, so a gauge sampled at
/// scrape reports whatever one instant held and misses the maximum, which is the whole
/// quantity of interest (the same argument as
/// [`DeliveryMetrics::inflight_chunks_peak`]).
///
/// Each limit is published beside its peak so a dashboard and the ladder can compute
/// headroom instead of requiring the reader to know the chart value. Every gauge is
/// registered unconditionally and reads 0 until the scatter is wired, which on a
/// `scatter.enabled=false` node is the truth rather than a gap.
#[derive(Clone)]
struct ScatterBoundGauges {
    /// [`crate::staging::StagingArea::staged_bytes`] — live residency, the plottable
    /// one.
    staged_bytes: IntGauge,
    /// [`crate::staging::StagingArea::staged_bytes_peak`] — **the discriminator**.
    staged_bytes_peak: IntGauge,
    /// [`crate::staging::StagingArea::budget_bytes`] — the ceiling the two above are
    /// judged against.
    staging_budget_bytes: IntGauge,
    /// [`crate::coordinate::WindowSlots::in_flight()`] — slots held right now.
    windows_in_flight: IntGauge,
    /// [`crate::coordinate::WindowSlots::peak()`] — the widest the pipeline ever got.
    windows_in_flight_peak: IntGauge,
    /// [`crate::coordinate::WindowSlots::limit()`] — `scatter.windowsInFlight`.
    windows_in_flight_limit: IntGauge,
    /// The staging area the first three are read from, wired after construction (the
    /// daemon builds it later, and only when the scatter is on). The same
    /// `Arc`-shared-`OnceLock` shape [`ChunkStoreMetrics`] uses for its store: one cell,
    /// visible through every [`Metrics`] clone, so no startup ordering is required.
    staging: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<crate::staging::StagingArea>>>,
    /// The coordinator's slots the last three are read from, wired the same way.
    windows: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<crate::coordinate::WindowSlots>>>,
}

/// Client-memory delivery counters (ADR-0026, planning/19 Track C C1).
///
/// Grouped rather than flattened into [`Metrics`] — the same shape the `efa`-only
/// RDMA gauges use: the delivery path is one feature with one on/off knob, and
/// keeping its series together is what makes "was this run delivering at all?" a
/// single field to look at. (No doc link to that struct on purpose: it is both
/// private and feature-gated, so linking it breaks the default doc build.)
#[derive(Clone)]
pub struct DeliveryMetrics {
    /// GETs answered by delivering into a client's own memory (header-only 200).
    /// Against `pacer_ops_total{op="get_object"}` this is the share of reads
    /// taking the accelerated path at all — the denominator for every claim C1
    /// makes.
    pub requests: IntCounter,
    /// Bytes delivered into client memory. They never crossed the HTTP body, so
    /// they are absent from every socket-level byte count — which is the point.
    ///
    /// ⚠ **Advances once per REQUEST**, when a span's last window lands, so
    /// `rate()` over it measures completions bunching rather than bandwidth: a
    /// 1-second sample holding 12 finished spans reported 53.6 GiB/s on a single
    /// ~11 GiB/s rail (2026-08-24). For bandwidth use [`Self::chunk_bytes`].
    pub bytes: IntCounter,
    /// Bytes delivered, counted **as each chunk lands**, by the same `source` label
    /// as [`Self::chunks`] — the honest bandwidth series, and the reason this exists
    /// beside `bytes` rather than replacing it.
    ///
    /// `bytes` answers "how much did this request deliver" and moves in span-sized
    /// steps; this answers "how fast is the delivery plane moving right now", which is
    /// the only one a dashboard can plot. Deriving it from `chunks` was the workaround
    /// (`rate(chunks) x chunk_size`), and it silently lies whenever an edge window is
    /// short or `PACER_CHUNK_SIZE` is not what the reader of the graph assumed — so a
    /// grafana panel could not show delivery bandwidth at all without knowing the
    /// daemon's config (2026-08-25).
    pub chunk_bytes: IntCounterVec,
    /// Delivered chunks by `source` (`local`/`peer_rdma`/`peer_stream`/
    /// `backend`). `peer_rdma` is the only source that touches no bytes on this
    /// node; a rising `peer_stream` share means holders are declining to WRITE
    /// (no cached AH, capacity) — a fallback to investigate, not a failure.
    pub chunks: IntCounterVec,
    /// Per-chunk resolution latency (seconds) by the same `source` label as
    /// [`Self::chunks`]: from taking the chunk's key to its bytes standing in the
    /// client's window. Excludes the optional read-back digest, which is a
    /// verification cost and not part of delivering.
    ///
    /// `_sum / _count` per source is the per-chunk cost the C4 arms had to derive
    /// from the wall clock (`chunk_size ÷ rate`), which only equals the real
    /// latency when nothing overlaps — the assumption that made the arm's disk
    /// reading a hypothesis instead of a measurement. Read beside
    /// [`Self::inflight_chunks_peak`]: latency × peak ÷ span gives the overlap
    /// that actually happened.
    pub chunk_seconds: HistogramVec,
    /// [`Self::chunk_seconds`] split into the stages a chunk actually spends it
    /// in, by `stage` — **the number the C4 read-path ladder could not get**.
    ///
    /// On a local hit `deliver_window` is `cache_read` and then whichever
    /// placement the target needs — `copy` for a mapped target, `rdma_write`
    /// (plus `digest`, if one was asked for) for a token one — so the stages sum
    /// to `chunk_seconds` on **both** paths and each is separately attributable.
    /// Without them the total is unpartitioned, and the 2026-08-26 arm had to
    /// reason about a 97 ms chunk from foyer's own storage-hit histogram — which
    /// is per *storage* hit, not per delivered chunk, so it says nothing about a
    /// chunk served from the RAM tier and cannot be subtracted from this total.
    /// The ~47 ms that arithmetic left over was invisible for that reason alone
    /// (`bench/ladder/results/c4-foyer-readpath.md`).
    ///
    /// * `cache_read` — the chunk-entry read: foyer's RAM tier, or its disk tier
    ///   plus the unconditional XxHash64 and the decode into a slab frame.
    /// * `copy` — moving those bytes into the client's window, on the blocking
    ///   pool. **Not observed on the `peer_rdma` path**, where a holder's NIC
    ///   writes the window and this node touches no bytes, so a `copy` count
    ///   below the chunk count is that path working rather than a gap. Nor on a
    ///   token target, which is not copied at all.
    /// * `rdma_write` — the one-sided WRITE into a token target's window,
    ///   including the wait for its completion.
    /// * `digest` — the source-side CRC32, **only when the token asked for one**.
    pub stage_seconds: HistogramVec,
    /// Chunk resolutions in flight right now, summed across every delivered
    /// request. The instantaneous width of the delivery fan-out.
    pub inflight_chunks: IntGauge,
    /// High-water mark of [`Self::inflight_chunks`] since the process started —
    /// the one number that says whether `delivery.parallelism` was ever reached.
    ///
    /// A peak is the right shape here rather than a scrape-time sample, because
    /// the fan-out of one request opens and closes inside a single span and a
    /// 15 s scrape interval will land between them: the 2026-08-25 arm had 322
    /// spans in 31 s, so any gauge sampled at scrape reports whatever one instant
    /// happened to hold. It never decreases, so it is read once at the end of a
    /// run, not `rate()`d.
    ///
    /// The update is `get`-compare-`set` rather than a fetch-max, so two
    /// resolutions starting at once can lose one increment. That costs at most a
    /// count of 1 against a bound in the tens or hundreds, and buys keeping this
    /// on prometheus' own atomics instead of a second counter beside them.
    pub inflight_chunks_peak: IntGauge,
    /// Targets refused, by `reason` (`malformed`/`unusable`/`quota`). Only
    /// `quota` degraded to a body-delivered read; the other two were answered
    /// with a 4xx, so a rising `malformed` rate is a client-side bug.
    pub rejects: IntCounterVec,
    /// Individual **windows** a client-registered target could not take, by `reason` —
    /// `TokenDecline::label`, plus `no_rdma_plane`.
    ///
    /// Distinct from [`Self::rejects`], which counts whole *requests* refused up front. This
    /// counts a WRITE declining mid-request, which before it existed was visible only as a
    /// `debug!` line and one narrow counter
    /// (`pacer_delivery_unknown_peer_declines_total`, covering one of five reasons). The
    /// reason is the whole value of the series: `not_announceable` and
    /// `writer_not_installed` are the client not keeping up and are re-attempted per window,
    /// while `no_healthy_rail`, `not_addressable` and `body_exceeds_staging` are properties
    /// of this node, the descriptor or the configuration and degrade the request at once.
    ///
    /// ⚠ **An increment is not a failed read.** A declined window is either re-attempted or
    /// degrades the request to an ordinary body, which is slower and never wrong. Alert on a
    /// *rate*, and read the label before concluding anything: a run with declines and no
    /// change in `pacer_delivery_requests_total` is the retry absorbing them.
    pub declines: IntCounterVec,
    /// Windows whose RDMA WRITE into client-registered memory **failed**, by `reason` —
    /// the completion status, from `pacer_transport::efa::write_failure_reason`.
    ///
    /// **The counterpart of [`Self::declines`], and the gap it closes is total.** A decline
    /// is benign and was already counted; a *failure* answers the client's GET with an HTTP
    /// 500, and until this existed it moved nothing at all — not this, not
    /// `pacer_delivery_requests_total` (which only counts deliveries that completed), not
    /// `pacer_rdma_cq_errors_total` (which counts the completion pump dying, and the pump is
    /// alive: it delivered the failure). Measured on a p6-b200, 4 occurrences in ~22 fresh
    /// client processes: 500s with every series flat, discoverable only by reading the
    /// daemon's log. No alert anyone would plausibly write could see it.
    ///
    /// ⚠ **Counts WORK REQUESTS, not failed reads, and the two differ by a lot.** One
    /// failing window fails its whole request, and `run_delivery` then drops the rest of the
    /// pipeline — but the windows already posted complete too, and when the queue pair has
    /// entered the error state they all complete `work_request_flushed`. The single observed
    /// incident increments this by **four** (work requests 1863/1864/1865/1868) for **one**
    /// failed GET. So alert on a rate being non-zero, never on its magnitude, and do not
    /// read it as a request count.
    ///
    /// **Read the label before concluding anything, because one value is not a cause.**
    /// `work_request_flushed` means the queue pair was already in the error state when the
    /// NIC reached this work request; it is a *consequence*, and the status that caused the
    /// transition is a different one. If that primary status is not here beside the flushes,
    /// it was discarded before anything could receive it — which
    /// `pacer_rdma_unobserved_completion_failures_total` counts, and the daemon's log names.
    pub write_failures: IntCounterVec,
    /// Pre-flight endpoint queries answered, by `outcome` (ADR-0030's pre-flight exchange —
    /// see [`crate::preflight`]).
    ///
    /// **This is the series that catches the design's own silent failure.** A shim that stops
    /// priming — an older loader, a botocore change that breaks its signing, an exception
    /// swallowed by the deliberate fallback — leaves everything *working*, just back on the
    /// announce-and-retry path with the race it has. Nothing fails, so nobody looks. This flat
    /// at zero while `pacer_delivery_requests_total` climbs is exactly that regression, and it
    /// is visible on the daemon, where an operator already looks.
    ///
    /// Outcomes: `served` (endpoints named), `no_plane` (answerable, but this node has no RDMA
    /// plane, so there is nothing to prime) and `malformed` (the marker's value did not parse —
    /// a 4xx, and a client-side bug). There is deliberately no `disabled` outcome: with delivery
    /// off the marker is ignored exactly as a target descriptor is, so nothing is counted and the
    /// GET is served normally.
    pub preflight: IntCounterVec,
    /// Holder nodes named across every pre-flight answer.
    ///
    /// Divided by `preflight_total{outcome="served"}` this is the average holder count one
    /// object's read is predicted to face — which is the number that sizes the client's
    /// address-handle bound (`pacer_client::handles::DEFAULT_MAX_HANDLES`). A rising mean
    /// against a fixed bound is the warning that a client will start evicting handles, which is
    /// the one failure mode of that cache that is not benign.
    pub preflight_nodes: IntCounter,
    /// Registrations actually performed. Divided by `requests` this is the fraction
    /// of delivered reads that needed an rkey at all — **0 on a node serving its own
    /// cached chunks**, which is what makes the lazy registration visible rather than
    /// merely claimed. A value that tracks `requests` on a single-node arm means the
    /// laziness regressed.
    pub registrations: IntCounter,
    /// Cumulative seconds spent registering client memory (`ibv_reg_mr`).
    /// ADR-0026 point 7 makes registration per-request, so this cost is on the
    /// request path by design — publishing it is what keeps it measured instead
    /// of assumed: divide by `requests` for the per-GET tax.
    pub register_seconds: prometheus::Counter,
    /// Client bytes currently mapped and pinned. Refreshed at scrape from the
    /// same quota the admission decision consults, so a value pinned at
    /// `PACER_DELIVERY_PINNED_BYTES_MAX` explains a rising
    /// `rejects{reason="quota"}` directly.
    pinned_bytes: Gauge,
    /// The quota `pinned_bytes` reads from, wired after construction (the proxy
    /// owns it). Same `OnceLock`-shared-through-clones shape the RDMA transport
    /// handle uses: one cell, visible through every [`Metrics`] clone.
    quota: std::sync::Arc<std::sync::OnceLock<std::sync::Arc<crate::delivery::DeliveryQuota>>>,
}

impl DeliveryMetrics {
    /// Count one chunk resolution as in flight for as long as the returned guard
    /// lives, and raise [`Self::inflight_chunks_peak`] if this is the widest the
    /// fan-out has been.
    #[must_use]
    pub fn chunk_in_flight(&self) -> InflightChunk {
        let now = self.inflight_chunks.get() + 1;
        self.inflight_chunks.set(now);
        if now > self.inflight_chunks_peak.get() {
            self.inflight_chunks_peak.set(now);
        }
        InflightChunk {
            current: self.inflight_chunks.clone(),
        }
    }
}

/// One chunk resolution's place in [`DeliveryMetrics::inflight_chunks`], released
/// on drop.
///
/// RAII rather than a decrement at the end of the resolution because a window can
/// leave that code by `?` (a backend fetch with no bytes to serve) as readily as by
/// returning, and one leaked increment does not wash out — it biases the gauge, and
/// with it the peak, for the rest of the process's life.
pub struct InflightChunk {
    current: IntGauge,
}

impl Drop for InflightChunk {
    fn drop(&mut self) {
        self.current.dec();
    }
}

/// The RDMA-plane gauges (planning/15 B4, planning/16 §5) plus the handle they
/// scrape. Split into its own struct so [`Metrics`] carries a single
/// `#[cfg(feature = "efa")]` field rather than one per gauge — the plane is
/// all-or-nothing, so grouping it keeps the feature seam in one place.
///
/// `Clone` mirrors [`Metrics`]'s own derive: each field is a cheap handle
/// clone (`Gauge` and the `Arc`-backed `OnceLock` all share their state), so a
/// cloned [`Metrics`] observes the same gauges and the same wired transport.
#[cfg(feature = "efa")]
#[derive(Clone)]
struct RdmaMetrics {
    /// Cumulative wall-clock seconds spent in the holder-side RDMA bounce copy
    /// (cached `Bytes` → registered WRITE-source buffer). Refreshed on each
    /// scrape from the transport's nanosecond accumulator. This copy runs on
    /// the serve path only; dividing it by `bytes_to_peers` gives the CPU-per-
    /// GiB the zero-copy work (planning/15) is trying to remove.
    holder_copy_seconds: Gauge,
    /// Cumulative wall-clock seconds spent in the requester-side copy-out.
    /// LEGACY since the requester zero-copy path (planning/16 §5): the RDMA-served
    /// path now hands the S3 client a `Bytes` that owns its registered slot
    /// (`Bytes::from_owner`) instead of memcpying, so this reads ~0 — retained
    /// only as a fallback-attribution hook (see the transport's `requester_copy_nanos`).
    requester_copy_seconds: Gauge,
    /// Requester-side RDMA buffer-pool slots currently leased out. With the
    /// zero-copy path a slot stays leased until the served `Bytes` is drained by
    /// the S3 client, so a persistently high value is the operator's slot-starvation
    /// signal (planning/16 §5): fetches queue on the pool rather than starve, but
    /// latency rises. Refreshed from the transport on each scrape.
    requester_slots_in_use: Gauge,
    /// EFA rails the transport brought up at startup (A5 multi-rail): 1 on
    /// r8gd, up to 32 on p5.48xlarge (capped by `PACER_EFA_RAILS`). Static
    /// after startup; the rung harness asserts it to prove the multi-rail
    /// plane actually engaged rather than silently running one rail.
    rails: Gauge,
    /// Cumulative seconds spent building+submitting the holder's RDMA WRITE
    /// send batch (the synchronous, non-yielding span inside `post_write`).
    /// This IS CPU time: the span never crosses an `.await`, so — like
    /// `holder_copy_seconds` — dividing it by `bytes_to_peers` yields a valid
    /// serve-path CPU-per-GiB contribution. Refreshed from the transport's
    /// nanosecond accumulator on each scrape.
    post_batch_seconds: Gauge,
    /// Cumulative seconds the holder spent *awaiting* WRITE completions. This
    /// is WALL-CLOCK scheduler-wait time, NOT CPU: the await yields the task
    /// while the WRITE crosses the wire and the completion pump reaps it, so
    /// the elapsed time is on-wire round-trip plus scheduler resume latency,
    /// during which the holder burns ~no CPU. Never add this to a CPU/GiB
    /// ratio (that re-creates the retracted whole-process-CPU confound,
    /// planning/15); its use is the inverse — elevated wait with
    /// `post_batch_seconds` flat flags a busy shared runtime delaying the
    /// pump's wakeup (planning/16 contention question). Refreshed from the
    /// transport's nanosecond accumulator on each scrape.
    write_completion_wait_seconds: Gauge,
    /// Cumulative completion-pump deaths (A1). 0 in steady state; becomes ≥ 1
    /// if the CQ→tokio drain loop ever exits on a terminal CQ/fd error, at
    /// which point the transport flips RDMA capability off and every fetch
    /// serves via gRPC directly (no per-op completion-timeout tax). A nonzero,
    /// non-increasing value is the operator's "RDMA silently degraded to gRPC
    /// on this node" alarm — pair it with `peer_serves_rdma` going flat.
    /// Refreshed from the transport's counter on each scrape.
    cq_errors_total: Gauge,
    /// Cumulative **failed** work completions that no waiter received — the completion
    /// pump's blind spot, and the other half of what makes a burst of flushed work requests
    /// uninterpretable.
    ///
    /// A queue pair entering the error state flushes every work request posted on it, so the
    /// flushes are consequences and exactly one non-flush status caused the transition. That
    /// one is delivered to its own waiter like any other — unless that waiter had already
    /// gone (a completion timeout handed its buffer over, or its request was abandoned and
    /// dropped the receiver), in which case the pump discarded the status in silence. Which
    /// is entirely possible for the causing work request: once one window has failed a
    /// delivery, every other window of that request is abandoned.
    ///
    /// So read this **beside** `pacer_delivery_write_failures_total`: non-zero says the cause
    /// was swallowed and is now in the log; zero, with flushes present, says the transition
    /// produced no completion this process can observe at all — which is a finding, and the
    /// point at which the announce/handle path needs its own instrumentation.
    ///
    /// Not to be confused with [`Self::cq_errors_total`], which counts the pump *dying*.
    /// Here the pump is alive and working: it delivered the flushes.
    unobserved_completion_failures_total: Gauge,
    /// Cumulative WRITE source buffers handed to a completion pump because their
    /// serve stopped waiting first (ADR-0028's timed-out-completion safety item).
    ///
    /// 0 in steady state. Any non-zero value means a serve blew its 5 s software
    /// deadline while the work request was still outstanding — which before this
    /// mechanism returned the chunk's cache frame to the free list while the NIC
    /// may still have been DMA-reading it, i.e. shipped a *different* chunk's
    /// bytes to the requester with no digest on that path to catch it. So this is
    /// simultaneously the safety mechanism's activity counter and a rail-latency
    /// alarm: chase a rising rate, but do not read it as corruption.
    write_sources_orphaned_total: Gauge,
    /// Delivery WRITEs re-posted because the client had not yet installed this writer, and
    /// deliveries that gave up and fell back to a body. Retries with no declines is the
    /// healthy shape — the race is being absorbed; declines mean clients are not answering.
    unknown_peer_retries_total: Gauge,
    unknown_peer_declines_total: Gauge,
    /// Source buffers a pump still holds, i.e. WRITEs whose completion has not
    /// arrived at all. Bounded by posted-but-unreaped WRITEs (serve admission ×
    /// `PACER_RDMA_RAIL_WINDOW`), so it cannot exhaust memory; a value that
    /// stays elevated instead of returning to 0 means completions have stopped
    /// arriving, which `cq_errors_total` describes more directly.
    write_sources_held: Gauge,
    /// Per-rail live counters, labelled `rail` (and `numa` — the node that rail's
    /// arenas and reaper sit on, ADR-0025). Four series: WRITEs in flight, WRITEs
    /// completed, bytes WRITTEN, and requester ranges leased.
    ///
    /// Why labelled rather than aggregate: every rail-level question D4 raised
    /// could only be answered from EFA *hardware* counters, which show bytes but
    /// not queueing. `rail_writes_in_flight` pinned at `PACER_RDMA_RAIL_WINDOW`
    /// means the window binds; well below it means something upstream does. And a
    /// rail carrying a disproportionate share of `rail_write_bytes` is the
    /// spread problem, which no aggregate can show. Gauges (not counters) even for
    /// the cumulative series, matching how the rest of this struct is refreshed
    /// from the transport's own accumulators at scrape.
    rail: prometheus::GaugeVec,
    /// The bounded client-edge maps (R4) — see [`ClientEdgeMetrics`].
    client_edge: ClientEdgeMetrics,
    /// The RDMA transport, if the `efa` plane came up. Held so [`Metrics::encode`]
    /// can pull its copy-timer accumulators at scrape time — the same
    /// pull-on-scrape shape as [`Metrics::process_cpu_seconds`]. A [`OnceLock`] so
    /// a single [`Metrics`] clone can be wired after construction (the transport
    /// is built later, inside the cluster path) and the value is visible through
    /// every existing clone (all clones share the one `Arc`-backed cell).
    transport:
        std::sync::Arc<std::sync::OnceLock<std::sync::Arc<pacer_transport::efa::EfaRdmaTransport>>>,
}

/// Create an [`IntCounter`] and register it in one step.
fn counter(registry: &Registry, name: &str, help: &str) -> anyhow::Result<IntCounter> {
    let c = IntCounter::new(name, help)?;
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

/// Create a [`Gauge`] and register it in one step.
fn gauge(registry: &Registry, name: &str, help: &str) -> anyhow::Result<Gauge> {
    let g = Gauge::new(name, help)?;
    registry.register(Box::new(g.clone()))?;
    Ok(g)
}

/// Create a float [`prometheus::Counter`] and register it in one step — for
/// cumulative *seconds*, where an integer counter would quantize away everything
/// interesting.
fn float_counter(
    registry: &Registry,
    name: &str,
    help: &str,
) -> anyhow::Result<prometheus::Counter> {
    let c = prometheus::Counter::new(name, help)?;
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

/// Create a labelled [`IntCounterVec`] and register it in one step.
fn int_counter_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> anyhow::Result<IntCounterVec> {
    let c = IntCounterVec::new(Opts::new(name, help), labels)?;
    registry.register(Box::new(c.clone()))?;
    Ok(c)
}

/// Register the ADR-0016 layer-1 admission series: [`Metrics::local_admits`]
/// plus the fill-guard pair, [`Metrics::fill_inflight`] and
/// [`Metrics::fill_abandoned`] (see [`crate::proxy::FillGuard`]) — one
/// requester-local admission decision and the guard that dedupes the fill it
/// triggers, so grouped rather than left as three separate call sites. Split
/// out of [`Metrics::new`] for the same reason as [`register_scatter_metrics`]:
/// that constructor is at its function-length budget.
fn register_fill_guard_metrics(
    registry: &Registry,
) -> anyhow::Result<(IntCounter, IntGauge, IntCounter)> {
    let local_admits = counter(
        registry,
        "pacer_local_admits_total",
        "Peer-owned chunks admitted locally after the frequency gate (ADR-0016 layer 1)",
    )?;
    let inflight = int_gauge(
        registry,
        "pacer_fill_inflight",
        "Fills currently claimed via FillGuard — every path that claims the node-wide dedup set: the proxy's maybe_admit_local/maybe_fill and the peer server's read-through, which used to claim it directly and go uncounted",
    )?;
    let abandoned = counter(
        registry,
        "pacer_fill_abandoned_total",
        "Guarded fills whose future was dropped before completing (e.g. a cancelled client GET) rather than finishing on its own — invisible before FillGuard existed",
    )?;
    Ok((local_admits, inflight, abandoned))
}

/// Register ADR-0040's single-flight series (see [`FillCoalesceMetrics`]). Split out
/// of [`Metrics::new`] for the same reason as [`register_fill_guard_metrics`]: that
/// constructor is at its function-length budget.
fn register_fill_coalesce_metrics(registry: &Registry) -> anyhow::Result<FillCoalesceMetrics> {
    Ok(FillCoalesceMetrics {
        served: counter(
            registry,
            "pacer_fill_coalesced_total",
            "Chunk reads served from a fill another request already had in flight — each one a backend ranged GET this node did not issue (ADR-0040)",
        )?,
        bytes: counter(
            registry,
            "pacer_fill_coalesced_bytes_total",
            "Bytes served from another request's in-flight fill, i.e. backend traffic avoided (ADR-0040)",
        )?,
        waiters: int_gauge(
            registry,
            "pacer_fill_waiters",
            "Requests parked on another request's in-flight fill right now — pinned with pacer_fill_coalesced_total flat means a stuck leader is holding them (ADR-0040)",
        )?,
        fallbacks: counter(
            registry,
            "pacer_fill_coalesce_fallbacks_total",
            "Reads that waited on a leading fill, were handed nothing (its read failed or its client disconnected), and fetched for themselves — the pre-ADR-0040 path, not an error",
        )?,
    })
}

/// Register the backend chunk-read series (`pacer_backend::retry`). Split out of
/// [`Metrics::new`] for the same reason as [`register_scatter_metrics`]: that
/// constructor is at its function-length budget.
/// The cache read/fill path's plain counters, on the way to [`Metrics`]'s flat fields.
///
/// Private and immediately destructured, so it is a grouping for one function's benefit and
/// not a change to the public shape — every reader still says `metrics.cache_hits`.
struct CachePathCounters {
    hits: IntCounter,
    misses: IntCounter,
    bypass: IntCounter,
    conditional_get_served: IntCounter,
    fills_completed: IntCounter,
    fills_aborted: IntCounter,
    bytes_from_cache: IntCounter,
    bytes_filled: IntCounter,
}

/// Register the eight counters of [`CachePathCounters`].
///
/// # Errors
///
/// A duplicate registration in `registry`.
fn register_cache_path_counters(registry: &Registry) -> anyhow::Result<CachePathCounters> {
    let c = |name: &str, help: &str| counter(registry, name, help);
    Ok(CachePathCounters {
        hits: c("pacer_cache_hits_total", "GETs served from cache")?,
        misses: c("pacer_cache_misses_total", "Cacheable GETs that missed")?,
        bypass: c(
            "pacer_cache_bypass_total",
            "GETs that bypassed the cache (policy)",
        )?,
        conditional_get_served: c(
            "pacer_conditional_get_served_total",
            "If-Match GETs whose ETag matched, so the cache path was taken (ADR-0039)",
        )?,
        fills_completed: c("pacer_fills_completed_total", "Whole-object cache fills")?,
        fills_aborted: c(
            "pacer_fills_aborted_total",
            "Cache fills abandoned (client disconnect or short read)",
        )?,
        bytes_from_cache: c("pacer_bytes_from_cache_total", "Bytes served from cache")?,
        bytes_filled: c("pacer_bytes_filled_total", "Bytes written into the cache")?,
    })
}

/// The peer tier's plain counters — both sides of it — on the way to [`Metrics`]'s flat
/// fields.
///
/// Private and immediately destructured, for the same reason as
/// [`CachePathCounters`]: a grouping for one function's benefit, not a change to the
/// public shape. Every reader still says `metrics.peer_fetches`.
struct PeerPathCounters {
    fetches: IntCounter,
    fallbacks: IntCounter,
    bytes_from_peers: IntCounter,
    serves: IntCounter,
    serves_rdma: IntCounter,
    misses: IntCounter,
    readthroughs: IntCounter,
    bytes_to_peers: IntCounter,
}

/// Register the eight counters of [`PeerPathCounters`].
///
/// Extracted from [`Metrics::new`] for the reason the sibling registrars name: that
/// constructor is at clippy's 80-line budget, and these eight were 32 of its lines.
///
/// # Errors
///
/// A duplicate registration in `registry`.
fn register_peer_path_counters(registry: &Registry) -> anyhow::Result<PeerPathCounters> {
    let c = |name: &str, help: &str| counter(registry, name, help);
    Ok(PeerPathCounters {
        fetches: c(
            "pacer_peer_fetches_total",
            "Misses resolved by fetching from the owning peer",
        )?,
        fallbacks: c(
            "pacer_peer_fallbacks_total",
            "Peer fetches that failed over to a direct backend GET",
        )?,
        bytes_from_peers: c(
            "pacer_bytes_from_peers_total",
            "Bytes received from peers (requester side)",
        )?,
        serves: c(
            "pacer_peer_serves_total",
            "Peer FetchBlob requests served (cache hit or read-through)",
        )?,
        serves_rdma: c(
            "pacer_peer_serves_rdma_total",
            "Peer FetchBlob requests served via a one-sided RDMA WRITE (ADR-0018)",
        )?,
        misses: c(
            "pacer_peer_misses_total",
            "Peer FetchBlob requests answered NOT_FOUND",
        )?,
        readthroughs: c(
            "pacer_peer_readthroughs_total",
            "Owned misses read through to the backend on behalf of a peer",
        )?,
        bytes_to_peers: c(
            "pacer_bytes_to_peers_total",
            "Bytes sent to peers (server side)",
        )?,
    })
}

fn register_backend_read_metrics(registry: &Registry) -> anyhow::Result<BackendReadMetrics> {
    Ok(BackendReadMetrics {
        retries: counter(
            registry,
            "pacer_backend_read_retries_total",
            "Backend chunk reads re-issued after a transient failure (retried attempts, not reads)",
        )?,
        failures: int_counter_vec(
            registry,
            "pacer_backend_read_failures_total",
            "Backend chunk reads that gave up, by outcome — each one failed or truncated a client GET",
            &["outcome"],
        )?,
    })
}

/// Register the write-scatter series (ADR-0032). Split out of [`Metrics::new`] for
/// the same reason as [`register_delivery_metrics`].
fn register_scatter_metrics(registry: &Registry) -> anyhow::Result<ScatterMetrics> {
    Ok(ScatterMetrics {
        scattered: counter(
            registry,
            "pacer_scatter_puts_total",
            "PUTs decomposed onto the chunk grid and uploaded by the chunks' homes (ADR-0032)",
        )?,
        declined: int_counter_vec(
            registry,
            "pacer_scatter_declined_total",
            "PUTs that did not scatter, by reason — all fell through to the proxy-and-invalidate path",
            &["reason"],
        )?,
        windows: int_counter_vec(
            registry,
            "pacer_scatter_windows_total",
            "Windows uploaded, by role: owner (a chunk's home took it) or local (this node did)",
            &["role"],
        )?,
        uncached_windows: counter(
            registry,
            "pacer_scatter_uncached_windows_total",
            "Windows uploaded but staged nowhere because a budget was full — durable, uncached, a later miss",
        )?,
        owners_engaged: counter(
            registry,
            "pacer_scatter_owners_engaged_total",
            "Distinct owners per scattered PUT, summed — divide by pacer_scatter_puts_total for mean fan-out",
        )?,
        refusals: int_counter_vec(
            registry,
            "pacer_scatter_refusals_total",
            "Offers an owner refused, by reason — large on a balanced save is reject-fast working, not a fault",
            &["reason"],
        )?,
        phase_seconds: histogram_vec(
            registry,
            "pacer_scatter_phase_seconds",
            "Where a coordinator's window slot goes, by phase. permit_wait=the wait for a slot (the client's backpressure); owner_rpc=a StoreChunk that was taken, covering the wire AND the owner's own UploadPart, so it is NOT a network measurement; owner_refused=the same wire with no S3 in it; owner_failed=a transport error; local_upload=this node's own stage+UploadPart; complete=CompleteMultipartUpload, ONCE PER PUT. Plus two observed on the OWNER: served_stage and served_upload. Wire = owner_rpc - (served_stage + served_upload); if served_upload is nearly all of owner_rpc the remote leg is S3 and ADR-0032 Phase 5 buys nothing",
            &[SCATTER_PHASE_LABEL],
            SCATTER_PHASE_LATENCY_BUCKETS,
        )?,
        bounds: register_scatter_bound_gauges(registry)?,
    })
}

/// Register the scatter's bound gauges ([`ScatterBoundGauges`]). Split out of
/// [`register_scatter_metrics`] for the same reason that was split out of
/// [`Metrics::new`]: both are at their function-length budget.
///
/// # Errors
///
/// A metric failing to register (a duplicate name).
fn register_scatter_bound_gauges(registry: &Registry) -> anyhow::Result<ScatterBoundGauges> {
    // Local alias, the same trick `register_chunk_store_metrics` uses: it keeps each
    // field on one line despite the long metric names.
    let g = |name: &str, help: &str| int_gauge(registry, name, help);
    Ok(ScatterBoundGauges {
        staged_bytes: g("pacer_scatter_staged_bytes", "Bytes this node holds staged for scattered writes right now — uploaded to S3, not yet cache-visible, held until their upload's Complete (ADR-0032 § 4)")?,
        staged_bytes_peak: g("pacer_scatter_staged_bytes_peak", "High-water mark of pacer_scatter_staged_bytes. READ IT AGAINST pacer_scatter_staging_budget_bytes: at the budget, refused{budget_exhausted} is residency-driven and only a reservation (ADR-0032 Phase 5), a per-part commit or staging to disk moves it; well below the budget with refusals present means a different cause entirely")?,
        staging_budget_bytes: g("pacer_scatter_staging_budget_bytes", "This node's staging ceiling (scatter.stagingBytes) — published so headroom is computable without knowing the chart value")?,
        windows_in_flight: g("pacer_scatter_windows_in_flight", "Window slots the scatter coordinator holds right now; times chunkSize, the client bytes it is buffering")?,
        windows_in_flight_peak: g("pacer_scatter_windows_in_flight_peak", "High-water mark of pacer_scatter_windows_in_flight — the on-hardware check that the coordinator's byte bound holds. It must never exceed pacer_scatter_windows_in_flight_limit")?,
        windows_in_flight_limit: g("pacer_scatter_windows_in_flight_limit", "The coordinator's slot ceiling (scatter.windowsInFlight), published beside the peak so headroom is computable without knowing the chart value")?,
        staging: std::sync::Arc::new(std::sync::OnceLock::new()),
        windows: std::sync::Arc::new(std::sync::OnceLock::new()),
    })
}

/// Register the client-memory delivery series (ADR-0026). Split out of
/// [`Metrics::new`] for the same reason as [`register_rdma_gauges`]: that
/// constructor is at its function-length budget.
fn register_delivery_metrics(registry: &Registry) -> anyhow::Result<DeliveryMetrics> {
    let (preflight, preflight_nodes) = register_preflight_metrics(registry)?;
    let (declines, write_failures) = register_window_outcome_metrics(registry)?;
    Ok(DeliveryMetrics {
        preflight,
        preflight_nodes,
        declines,
        write_failures,
        requests: counter(
            registry,
            "pacer_delivery_requests_total",
            "GETs answered by delivering into client-supplied memory (ADR-0026)",
        )?,
        bytes: counter(
            registry,
            "pacer_delivery_bytes_total",
            "Bytes delivered into client-supplied memory, per REQUEST (see pacer_delivery_chunk_bytes_total for bandwidth)",
        )?,
        chunk_bytes: int_counter_vec(
            registry,
            "pacer_delivery_chunk_bytes_total",
            "Bytes delivered into client memory, counted as each chunk lands, by source — the bandwidth series",
            &["source"],
        )?,
        chunks: int_counter_vec(
            registry,
            "pacer_delivery_chunks_total",
            "Chunks delivered into client memory, by source (peer_rdma touches no bytes on this node)",
            &["source"],
        )?,
        chunk_seconds: histogram_vec(
            registry,
            "pacer_delivery_chunk_seconds",
            "Per-chunk delivery latency (cache read plus the copy or the holder's WRITE), by source",
            &["source"],
            DELIVERY_CHUNK_LATENCY_BUCKETS,
        )?,
        stage_seconds: histogram_vec(
            registry,
            "pacer_delivery_stage_seconds",
            "Per-chunk delivery latency split by stage: cache_read, then copy into the client's window",
            &[DELIVERY_STAGE_LABEL],
            DELIVERY_CHUNK_LATENCY_BUCKETS,
        )?,
        inflight_chunks: int_gauge(
            registry,
            "pacer_delivery_inflight_chunks",
            "Chunk resolutions in flight right now — the instantaneous delivery fan-out",
        )?,
        inflight_chunks_peak: int_gauge(
            registry,
            "pacer_delivery_inflight_chunks_peak",
            "High-water mark of pacer_delivery_inflight_chunks — whether delivery.parallelism was ever reached",
        )?,
        rejects: int_counter_vec(
            registry,
            "pacer_delivery_rejects_total",
            "Delivery targets refused, by reason (only quota degrades to a body-delivered read)",
            &["reason"],
        )?,
        registrations: counter(
            registry,
            "pacer_delivery_registrations_total",
            "Client-memory registrations performed (0 when every chunk was served locally)",
        )?,
        register_seconds: float_counter(
            registry,
            "pacer_delivery_register_seconds_total",
            "Time spent registering client memory for RDMA (per-request by ADR-0026 point 7)",
        )?,
        pinned_bytes: gauge(
            registry,
            "pacer_delivery_pinned_bytes",
            "Client bytes currently mapped and pinned for delivery",
        )?,
        quota: std::sync::Arc::new(std::sync::OnceLock::new()),
    })
}

/// Register the two ways a single delivery *window* can end badly. Split out of
/// [`register_delivery_metrics`] for the same reason as [`register_preflight_metrics`] — it is
/// at its function-length budget — and these two belong together because they partition one
/// question between them: a window that the client's memory could not take **benignly**
/// (`declines`, re-attempted or degraded to a body, never wrong) versus one whose WRITE
/// **failed** (`write_failures`, an HTTP 500).
///
/// Two series and not one labelled series, deliberately. A dashboard panel or an alert wants
/// one of these and never both: a decline rate is a performance signal and a failure rate is
/// an availability one, and folding them together would put "the request took the slow path"
/// and "the request returned 500" behind the same number, where any threshold is wrong for
/// one of them.
fn register_window_outcome_metrics(
    registry: &Registry,
) -> anyhow::Result<(IntCounterVec, IntCounterVec)> {
    let declines = int_counter_vec(
        registry,
        "pacer_delivery_declines_total",
        "Windows a client-registered target could not take, by reason. Transient reasons (not_announceable, writer_not_installed) are re-attempted per window; the rest degrade the request to a body, which is slower and never wrong",
        &["reason"],
    )?;
    let write_failures = int_counter_vec(
        registry,
        "pacer_delivery_write_failures_total",
        "Windows whose RDMA WRITE into client-registered memory FAILED, by completion status — each one answered a GET with a 500, and before this series existed nothing moved at all. Counts WORK REQUESTS, not reads: a queue pair in the error state flushes every request posted on it, so one failed GET incremented this by four (reason=work_request_flushed). Alert on non-zero, not on magnitude. A flush is a CONSEQUENCE — if no other status appears beside it, the status that caused it was discarded before any waiter saw it, which pacer_rdma_unobserved_completion_failures_total counts and the daemon log names",
        &["reason"],
    )?;
    Ok((declines, write_failures))
}

/// Register the pre-flight endpoint-exchange series (ADR-0030 point 2's refinement). Split out
/// of [`register_delivery_metrics`] for the same reason that was split out of [`Metrics::new`]:
/// it is at its function-length budget, and these two series belong together — one counts the
/// answers, the other the holder fan-out those answers named.
fn register_preflight_metrics(registry: &Registry) -> anyhow::Result<(IntCounterVec, IntCounter)> {
    let answers = int_counter_vec(
        registry,
        "pacer_delivery_preflight_total",
        "Pre-flight endpoint queries answered, by outcome (served/no_plane/malformed). Flat at zero while pacer_delivery_requests_total climbs means no client is priming its address handles, so every delivery is back on the announce-and-retry race",
        &["outcome"],
    )?;
    let nodes = counter(
        registry,
        "pacer_delivery_preflight_nodes_total",
        "Holder nodes named across every pre-flight answer; divided by the served count it is the holder fan-out a client must build address handles for",
    )?;
    Ok((answers, nodes))
}

/// Register the allocator series ([`MallocGauges`]). Split out of [`Metrics::new`]
/// for the same reason as its neighbours: that constructor is at its length budget.
///
/// Registered on every platform, including those where [`crate::memstats::sample`]
/// returns `None`. A gauge that is present and never set reads 0, which is exactly
/// how the CPU gauge above already behaves off Linux, and keeping the series list
/// platform-independent means a dashboard is not built per target.
fn register_malloc_gauges(registry: &Registry) -> anyhow::Result<MallocGauges> {
    Ok(MallocGauges {
        heap: gauge(
            registry,
            "pacer_malloc_heap_bytes",
            "Allocator capacity in sbrk-grown arenas, summed over arenas (mallinfo2 arena)",
        )?,
        mmapped: gauge(
            registry,
            "pacer_malloc_mmapped_bytes",
            "Allocator capacity obtained directly by mmap, released on free (mallinfo2 hblkhd)",
        )?,
        in_use: gauge(
            registry,
            "pacer_malloc_in_use_bytes",
            "Bytes allocated and not yet freed — includes foyer's memory tier (mallinfo2 uordblks)",
        )?,
        free_retained: gauge(
            registry,
            "pacer_malloc_free_retained_bytes",
            "Bytes freed by the daemon but still held by the allocator (mallinfo2 fordblks)",
        )?,
    })
}

/// Register the configured-budget series ([`MemoryBudgetGauges`]). Split out of
/// [`Metrics::new`] for the same reason as its neighbours: that constructor is at its
/// length budget.
///
/// # Errors
///
/// A metric failing to register (a duplicate name).
fn register_memory_budget_gauges(registry: &Registry) -> anyhow::Result<MemoryBudgetGauges> {
    Ok(MemoryBudgetGauges {
        terms: gauge_vec(
            registry,
            "pacer_memory_budget_bytes",
            "Bytes this daemon is CONFIGURED to hold, by term (the sum deploy/helm/pacer/templates/_helpers.tpl's pacer.memoryLimit derives). counted=\"false\" terms are reported but NOT enforced, because the chart does not add them either",
            &["term", "counted"],
        )?,
        total: gauge(
            registry,
            "pacer_memory_budget_total_bytes",
            "The enforced sum of the terms above — what the startup check compared against memory.max. Plot it against pacer_cgroup_memory_max_bytes: a configuration that never fitted looks nothing like one that fitted and then grew",
        )?,
    })
}

/// Register the cgroup series ([`CgroupGauges`]). Split out of [`Metrics::new`] for
/// the same reason as its neighbours: that constructor is at its length budget.
///
/// `pacer_cgroup_*` rather than the `container_memory_*` names cAdvisor already
/// publishes for the same cgroup: those come from kubelet on its own schedule, and a
/// series with one name and two producers is how a dashboard ends up plotting
/// whichever arrived last. Naming them ours also makes it explicit that they are
/// scraped from *inside* the container, which is why they exist on a bench cluster
/// where cAdvisor's may not be collected at all.
fn register_cgroup_gauges(registry: &Registry) -> anyhow::Result<CgroupGauges> {
    Ok(CgroupGauges {
        current: gauge(
            registry,
            "pacer_cgroup_memory_current_bytes",
            "Memory charged to this container's cgroup INCLUDING page cache (memory.current) — the quantity the limit is compared against, unlike process_resident_memory_bytes",
        )?,
        max: gauge(
            registry,
            "pacer_cgroup_memory_max_bytes",
            "This cgroup's memory limit (memory.max); infinity when unlimited. Plot current/max for proximity to an OOM kill",
        )?,
        file: gauge(
            registry,
            "pacer_cgroup_memory_file_bytes",
            "Page cache charged to this cgroup (memory.stat file) — the term VmRSS EXCLUDES and foyer's buffered disk tier fills",
        )?,
        file_dirty: gauge(
            registry,
            "pacer_cgroup_memory_file_dirty_bytes",
            "Page cache written but not yet flushed (memory.stat file_dirty) — unreclaimable until writeback completes",
        )?,
        file_writeback: gauge(
            registry,
            "pacer_cgroup_memory_file_writeback_bytes",
            "Page cache currently being written back (memory.stat file_writeback)",
        )?,
        anon: gauge(
            registry,
            "pacer_cgroup_memory_anon_bytes",
            "Anonymous memory charged to this cgroup (memory.stat anon) — the part of current that tracks the process-level series",
        )?,
        oom: gauge(
            registry,
            "pacer_cgroup_memory_oom_total",
            "ALERT ON THIS. Times an allocation failed under this cgroup's limit after reclaim (memory.events oom) — counted BEFORE any kill, and readable by a daemon still alive",
        )?,
        oom_kill: gauge(
            registry,
            "pacer_cgroup_memory_oom_kill_total",
            "Tasks the OOM killer terminated in this cgroup (memory.events oom_kill). Resets with the container's cgroup, so it evidences a kill that did NOT end the container",
        )?,
    })
}

/// Register ADR-0028's cache-slab series. Split out of [`Metrics::new`] for the
/// same reason as its neighbours: that constructor is at its length budget.
/// Register the ADR-0033 chunk-store gauges.
///
/// All gauges rather than counters, for the reason in [`ChunkStoreMetrics`]: the store
/// owns the atomics and this refreshes from them at scrape time, so there is exactly one
/// source of truth and nothing to drift.
///
/// # Errors
///
/// A metric failing to register (a duplicate name).
fn register_chunk_store_metrics(registry: &Registry) -> anyhow::Result<ChunkStoreMetrics> {
    // Local alias, the same trick `Metrics::new` uses for counters: it keeps each field on
    // one line despite the long metric names, which is what holds this function inside the
    // workspace's 80-line budget without splitting a single struct literal in half.
    let g = |name: &str, help: &str| gauge(registry, name, help);
    Ok(ChunkStoreMetrics {
        store: std::sync::Arc::new(std::sync::OnceLock::new()),
        hits: g("pacer_chunk_store_hits_total", "Chunk reads served from the ADR-0033 store (0 on diskTier=foyer)")?,
        misses: g("pacer_chunk_store_misses_total", "Chunk reads the ADR-0033 store did not hold")?,
        read_bytes: g("pacer_chunk_store_read_bytes_total", "Body bytes served from the ADR-0033 store — the numerator of its read rate (NOT hits x chunkSize: a short last chunk is a hit for fewer bytes)")?,
        written_bytes: g("pacer_chunk_store_written_bytes_total", "Body bytes written into the ADR-0033 store")?,
        read_seconds_mean: g("pacer_chunk_store_read_seconds_mean", "Mean seconds per chunk-store hit — compare against foyer_storage_op_duration's mean (40-75 ms per 16 MiB) and the device's own 1.335 ms")?,
        read_seconds_max: g("pacer_chunk_store_read_seconds_max", "Longest single chunk-store read, in seconds — what a mean hides")?,
        service_seconds_mean: g("pacer_chunk_store_service_seconds_mean", "Mean SERVICE seconds per chunk-store hit — the header read, key compare, pread and optional CRC, measured inside the blocking task so it EXCLUDES queueing. Compare this against the device's 1.335 ms for 16 MiB, not read_seconds_mean")?,
        service_seconds_total: g("pacer_chunk_store_service_seconds_total", "Summed SERVICE seconds over every chunk-store hit. Divide a delta of this by a delta of hits for ONE ARM's per-hit cost — the _mean beside it is a lifetime mean and cannot be attributed to an interval")?,
        read_seconds_total: g("pacer_chunk_store_read_seconds_total", "Summed queue-inclusive seconds over every chunk-store hit; a delta of this minus a delta of service_seconds_total is that interval's queueing")?,
        service_seconds_max: g("pacer_chunk_store_service_seconds_max", "Longest single chunk-store read's service time, in seconds")?,
        queue_seconds_mean: g("pacer_chunk_store_queue_seconds_mean", "Mean seconds a chunk-store hit waited for a blocking thread (read minus service) — the share of a hit that is Little's law rather than cost")?,
        writes: g("pacer_chunk_store_writes_total", "Chunks written into the ADR-0033 store")?,
        write_dedups: g("pacer_chunk_store_write_dedups_total", "Writes skipped because the key was already held (chunks are immutable)")?,
        evictions: g("pacer_chunk_store_evictions_total", "Slots reused, evicting the least-recently-used chunk")?,
        key_mismatches: g("pacer_chunk_store_key_mismatches_total", "ALERT ON THIS. A slot whose header named a different key than the index did; the read was refused rather than served. Should be 0 forever — it is the check ADR-0033 added in place of foyer's unconditional checksum")?,
        crc_mismatches: g("pacer_chunk_store_crc_mismatches_total", "Chunk bodies that failed their CRC32 (only ever non-zero with verifyChunkBody on)")?,
        corrupt_slots: g("pacer_chunk_store_corrupt_slots_total", "Slots whose header was present but impossible")?,
        io_errors: g("pacer_chunk_store_io_errors_total", "I/O errors on a chunk-store read or write")?,
        slots_used: g("pacer_chunk_store_slots_used", "Slots currently holding a chunk — occupancy in the unit the tier allocates in")?,
        slots_capacity: g("pacer_chunk_store_slots_capacity", "Slots the ADR-0033 store has, occupied or not")?,
        scan_seconds: g("pacer_chunk_store_scan_seconds", "Seconds the startup slot-header scan took (it is what makes a restart keep the tier)")?,
        scan_recovered: g("pacer_chunk_store_scan_recovered", "Chunks the startup scan recovered from slot headers — how warm this restart started")?,
    })
}
/// Register the backing-device series. Split out of [`Metrics::new`] for the same reason as
/// its neighbours: that constructor is at its length budget.
///
/// # Errors
///
/// A metric failing to register (a duplicate name).
fn register_device_io_metrics(registry: &Registry) -> anyhow::Result<DeviceIoMetrics> {
    Ok(DeviceIoMetrics {
        devices: std::sync::Arc::new(std::sync::OnceLock::new()),
        read_bytes: gauge_vec(
            registry,
            "pacer_node_device_read_bytes_total",
            "Bytes READ from the block devices backing the cache directory, from /proc/diskstats. THE ONLY series here that evidences a byte left a device: divide a delta of pacer_chunk_store_read_bytes_total (or foyer's) by a delta of this — about 1.0 means the tier read flash, ~0 means it read the page cache and the rate is a RAM rate. Array members only, so the family is safe to sum",
            &["device"],
        )?,
        written_bytes: gauge_vec(
            registry,
            "pacer_node_device_written_bytes_total",
            "Bytes WRITTEN to the block devices backing the cache directory, from /proc/diskstats. The store's writes are buffered, so a seed whose delta here is ~0 is still in dirty pages and the read that follows it cannot be a device measurement",
            &["device"],
        )?,
    })
}

/// Register the S3 listener series (ADR-0036). Split out of [`Metrics::new`] for
/// the same reason as the others: that constructor is at its length budget.
fn register_listener_metrics(registry: &Registry) -> anyhow::Result<ListenerMetrics> {
    Ok(ListenerMetrics {
        connections_active: int_gauge(
            registry,
            "pacer_s3_connections_active",
            "S3 connections currently held open (compare with PACER_S3_MAX_CONNECTIONS)",
        )?,
        connections_at_capacity: counter(
            registry,
            "pacer_s3_connections_at_capacity_total",
            "Times the accept loop waited for a free connection permit (the cap is binding)",
        )?,
        accept_errors: counter(
            registry,
            "pacer_s3_accept_errors_total",
            "Transient accept() failures on the S3 listener, retried after a backoff",
        )?,
    })
}

fn register_slab_metrics(registry: &Registry) -> anyhow::Result<SlabMetrics> {
    Ok(SlabMetrics {
        stores: counter(
            registry,
            "pacer_cache_slab_stores_total",
            "Chunks cached IN a registered slab frame (servable with no staging copy, ADR-0028)",
        )?,
        heap_fallbacks: counter(
            registry,
            "pacer_cache_slab_heap_fallbacks_total",
            "Chunks that found no free slab frame and were cached on the heap (a sizing signal)",
        )?,
        frames_in_use: gauge(
            registry,
            "pacer_cache_slab_frames_in_use",
            "Slab frames currently holding a resident cached chunk",
        )?,
        bytes: gauge(
            registry,
            "pacer_cache_slab_bytes",
            "Registered size of the ADR-0028 cache slab (resident from startup; 0 = no slab). Subtract max(this, foyer_memory_usage) to get memory outside the cache — never the sum",
        )?,
    })
}

/// The three bounded client-edge maps' size, and what they have shed (R4).
///
/// Its own struct, registered by its own function, for the same reason
/// [`register_rdma_gauges`] is split out of [`Metrics::new`]: both are at the
/// function-length lint's budget.
/// `Clone` because [`RdmaMetrics`] is: a `Metrics` is cloned per request handler, and every
/// clone must publish into the same registered series (each member here is itself a handle to
/// one registered metric, so cloning shares rather than copies).
#[cfg(feature = "efa")]
#[derive(Clone)]
struct ClientEdgeMetrics {
    /// Client address handles held, summed over rails.
    ah_entries: Gauge,
    /// Endpoints whose first contact is tracked, summed over rails.
    ready_entries: Gauge,
    /// Endpoints this node has announced itself to (node-wide, not per rail).
    announced_entries: Gauge,
    /// Cumulative evictions across all three maps, labelled `reason`.
    evictions_total: prometheus::GaugeVec,
}

/// Register the client-edge series and pre-create one eviction series per reason, so a scrape
/// shows a zero rather than a missing metric before the first eviction ever happens.
///
/// Gauges — including for the cumulative `_total` series — for the same reason as every other
/// member of [`RdmaMetrics`]: the transport owns the counters on its hot path and this layer
/// samples them at scrape, which a `prometheus::Counter` cannot express (it has no `set`).
#[cfg(feature = "efa")]
fn register_client_edge_gauges(registry: &Registry) -> anyhow::Result<ClientEdgeMetrics> {
    let evictions_total = gauge_vec(
        registry,
        "pacer_efa_client_evictions_total",
        "Client-edge records evicted, summed over the AH cache, the first-contact gate and the announce set. reason=cap is the ALARM (the endpoint set outgrew PACER_EFA_MAX_CLIENTS, so an endpoint still being written to can lose its record mid-request); reason=ttl is the idle reaper working; reason=explicit is UNKNOWN_PEER evidence retiring a stale record",
        &["reason"],
    )?;
    for reason in pacer_transport::client_registry::EvictionReason::all() {
        evictions_total
            .with_label_values(&[reason.label()])
            .set(0.0);
    }
    Ok(ClientEdgeMetrics {
        ah_entries: gauge(
            registry,
            "pacer_efa_client_ah_entries",
            "Client address handles held across rails, bounded by PACER_EFA_MAX_CLIENTS per rail (ADR-0030 client edge)",
        )?,
        ready_entries: gauge(
            registry,
            "pacer_efa_client_ready_entries",
            "Client endpoints whose first contact this node is tracking, across rails — same bound",
        )?,
        announced_entries: gauge(
            registry,
            "pacer_efa_announced_entries",
            "Client endpoints this node has announced itself to (ADR-0030 point 2) — node-wide, same bound",
        )?,
        evictions_total,
    })
}

/// Register the RDMA serve-path gauges (planning/15 B4, planning/16 §5) and
/// bundle them with an as-yet-unwired transport handle. Split out of
/// [`Metrics::new`] to keep that constructor under the function-length lint.
#[cfg(feature = "efa")]
fn register_rdma_gauges(registry: &Registry) -> anyhow::Result<RdmaMetrics> {
    Ok(RdmaMetrics {
        holder_copy_seconds: gauge(
            registry,
            "pacer_rdma_holder_copy_seconds_total",
            "Holder-side RDMA bounce-copy time (cached bytes → registered WRITE buffer)",
        )?,
        requester_copy_seconds: gauge(
            registry,
            "pacer_rdma_requester_copy_seconds_total",
            "Requester-side RDMA copy-out time (legacy; ~0 since the zero-copy serve path)",
        )?,
        requester_slots_in_use: gauge(
            registry,
            "pacer_rdma_requester_slots_in_use",
            "Requester-side RDMA pool slots currently leased (slot-starvation signal)",
        )?,
        rails: gauge(
            registry,
            "pacer_rdma_rails",
            "EFA rails brought up at startup (A5 multi-rail)",
        )?,
        post_batch_seconds: gauge(
            registry,
            "pacer_rdma_post_batch_seconds_total",
            "Holder-side RDMA WRITE batch build+submit time (CPU-bound serve-path span)",
        )?,
        write_completion_wait_seconds: gauge(
            registry,
            "pacer_rdma_write_completion_wait_seconds_total",
            "Holder-side wait for RDMA WRITE completions (WALL-CLOCK scheduler wait, NOT CPU; do not use in a CPU/GiB ratio)",
        )?,
        cq_errors_total: gauge(
            registry,
            "pacer_rdma_cq_errors_total",
            "Completion-pump deaths (≥1 means RDMA flipped off to gRPC on this node — ADR-0003 fallback)",
        )?,
        unobserved_completion_failures_total: gauge(
            registry,
            "pacer_rdma_unobserved_completion_failures_total",
            "FAILED work completions no waiter received, so their status reached no caller and no other series. NOT a pump death (see pacer_rdma_cq_errors_total) — the pump is alive and delivered them nowhere. Read beside pacer_delivery_write_failures_total{reason=\"work_request_flushed\"}: a queue pair in the error state flushes everything posted on it, so the flushes are consequences and exactly one non-flush status caused the transition. Non-zero here means that cause was swallowed and is in the daemon's log; ZERO, with flushes present, means the transition produced no observable completion at all",
        )?,
        unknown_peer_retries_total: gauge(
            registry,
            "pacer_delivery_unknown_peer_retries_total",
            "Delivery WRITEs re-posted after completing UNKNOWN_PEER: the client had not built an address handle for this writer yet (ADR-0030 point 2). Absorbed, not failed",
        )?,
        unknown_peer_declines_total: gauge(
            registry,
            "pacer_delivery_unknown_peer_declines_total",
            "Deliveries that gave up after retrying UNKNOWN_PEER and served the body instead — a client whose announce pump is starved or dead",
        )?,
        write_sources_orphaned_total: gauge(
            registry,
            "pacer_rdma_write_sources_orphaned_total",
            "WRITE source buffers handed to the completion pump because the serve's deadline elapsed first (ADR-0028: a timed-out completion must not free the frame)",
        )?,
        write_sources_held: gauge(
            registry,
            "pacer_rdma_write_sources_held",
            "Source buffers a completion pump still holds, awaiting a completion that has not arrived",
        )?,
        rail: gauge_vec(
            registry,
            "pacer_rdma_rail",
            // No design-record citation in a metric HELP string: this text is
            // scraped and rendered by every Prometheus and Grafana that reads the
            // daemon, none of which can open `planning/19`. The record still owns
            // the *why* — it is cited in the module comment, where a reader who
            // can follow it is.
            "Per-rail RDMA state: metric=writes_in_flight|writes_total|write_bytes_total|ranges_in_use|staging_in_use|staging_ranges|ah_evictions, labelled by rail and its NUMA node",
            &["rail", "numa", "metric"],
        )?,
        client_edge: register_client_edge_gauges(registry)?,
        transport: std::sync::Arc::new(std::sync::OnceLock::new()),
    })
}

/// Create a labelled [`prometheus::GaugeVec`] and register it.
///
/// Two callers, both for the same reason: a label set keeps a family of related
/// quantities from becoming one struct member each — 32 rails × 4 quantities for
/// `pacer_rdma_rail`, one per budget term for `pacer_memory_budget_bytes`. Both label
/// sets are closed, which is what keeps the cardinality a constant.
fn gauge_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
) -> anyhow::Result<prometheus::GaugeVec> {
    let g = prometheus::GaugeVec::new(prometheus::Opts::new(name, help), labels)?;
    registry.register(Box::new(g.clone()))?;
    Ok(g)
}

/// Create a labeled [`HistogramVec`] with fixed buckets and register it.
fn histogram_vec(
    registry: &Registry,
    name: &str,
    help: &str,
    labels: &[&str],
    buckets: &[f64],
) -> anyhow::Result<HistogramVec> {
    let h = HistogramVec::new(
        HistogramOpts::new(name, help).buckets(buckets.to_vec()),
        labels,
    )?;
    registry.register(Box::new(h.clone()))?;
    Ok(h)
}

/// Create an [`IntGauge`] and register it in one step.
fn int_gauge(registry: &Registry, name: &str, help: &str) -> anyhow::Result<IntGauge> {
    let g = IntGauge::new(name, help)?;
    registry.register(Box::new(g.clone()))?;
    Ok(g)
}

/// Build and register the metrics that need bespoke construction (a labeled
/// counter vec, two gauges, and the directory-RPC histogram), returned in
/// declaration order. Split out of [`Metrics::new`] to keep it under the
/// function-length lint; the plain [`IntCounter`]s stay inline via [`counter`].
#[allow(clippy::type_complexity)]
fn register_non_counters(
    registry: &Registry,
) -> anyhow::Result<(
    IntCounterVec,
    IntGauge,
    Gauge,
    Gauge,
    Gauge,
    Gauge,
    HistogramVec,
)> {
    let ops_total = IntCounterVec::new(
        Opts::new("pacer_ops_total", "S3 operations handled, by operation"),
        &["op"],
    )?;
    registry.register(Box::new(ops_total.clone()))?;
    let ring_members = IntGauge::new("pacer_ring_members", "Nodes in the current ring epoch")?;
    registry.register(Box::new(ring_members.clone()))?;
    let process_cpu_seconds = Gauge::new(
        "process_cpu_seconds_total",
        "Total user + system CPU time spent by the process, in seconds",
    )?;
    registry.register(Box::new(process_cpu_seconds.clone()))?;
    let process_resident_memory_bytes = Gauge::new(
        "process_resident_memory_bytes",
        "Resident set size of the process, in bytes",
    )?;
    registry.register(Box::new(process_resident_memory_bytes.clone()))?;
    let backend_connections = gauge(
        registry,
        "pacer_backend_connections",
        "Established TCP connections from this daemon to the backend's HTTPS port",
    )?;
    let backend_peer_endpoints = gauge(
        registry,
        "pacer_backend_peer_endpoints",
        "Distinct remote addresses among pacer_backend_connections — few of them means the ceiling may be per destination IP",
    )?;
    let dir_rpc_seconds = histogram_vec(
        registry,
        "pacer_dir_rpc_seconds",
        "Directory-shard RPC service time (in-lock op only), by op",
        &["op"],
        DIR_RPC_LATENCY_BUCKETS,
    )?;
    Ok((
        ops_total,
        ring_members,
        process_cpu_seconds,
        process_resident_memory_bytes,
        backend_connections,
        backend_peer_endpoints,
        dir_rpc_seconds,
    ))
}

impl Metrics {
    /// Create and register every counter in a fresh registry.
    ///
    /// # Errors
    ///
    /// Only on duplicate metric names within the registry — a programming
    /// error surfaced at startup, not a runtime condition.
    pub fn new() -> anyhow::Result<Self> {
        let registry = Registry::new();
        // The metrics that need bespoke construction (labeled vec, gauge,
        // histogram) — the plain IntCounters below all go through `counter`.
        let (
            ops_total,
            ring_members,
            process_cpu_seconds,
            process_resident_memory_bytes,
            backend_connections,
            backend_peer_endpoints,
            dir_rpc_seconds,
        ) = register_non_counters(&registry)?;
        #[cfg(feature = "efa")]
        let rdma = register_rdma_gauges(&registry)?;
        let (local_admits, fill_inflight, fill_abandoned) = register_fill_guard_metrics(&registry)?;
        // Both groups are flattened into the fields below rather than held as sub-structs:
        // they are read by name all over the tree and by three test suites, so grouping
        // them in the public type would be a rename with no benefit. Split out of this
        // function for the same reason as `register_scatter_metrics` — the line budget
        // (clippy.toml's `too-many-lines-threshold = 80`), which each of them crossed in
        // turn as a counter was added.
        let cache_path = register_cache_path_counters(&registry)?;
        let peer_path = register_peer_path_counters(&registry)?;
        Ok(Self {
            ops_total,
            ring_members,
            process_cpu_seconds,
            process_resident_memory_bytes,
            backend_connections,
            backend_peer_endpoints,
            cgroup: register_cgroup_gauges(&registry)?,
            malloc: register_malloc_gauges(&registry)?,
            memory_budget: register_memory_budget_gauges(&registry)?,
            dir_rpc_seconds,
            cache_hits: cache_path.hits,
            cache_misses: cache_path.misses,
            cache_bypass: cache_path.bypass,
            conditional_get_served: cache_path.conditional_get_served,
            fills_completed: cache_path.fills_completed,
            fills_aborted: cache_path.fills_aborted,
            bytes_from_cache: cache_path.bytes_from_cache,
            bytes_filled: cache_path.bytes_filled,
            backend_read: register_backend_read_metrics(&registry)?,
            peer_fetches: peer_path.fetches,
            peer_fallbacks: peer_path.fallbacks,
            bytes_from_peers: peer_path.bytes_from_peers,
            peer_serves: peer_path.serves,
            peer_serves_rdma: peer_path.serves_rdma,
            peer_misses: peer_path.misses,
            peer_readthroughs: peer_path.readthroughs,
            bytes_to_peers: peer_path.bytes_to_peers,
            local_admits,
            fill_inflight,
            fill_abandoned,
            fill_coalesce: register_fill_coalesce_metrics(&registry)?,
            delivery: register_delivery_metrics(&registry)?,
            scatter: register_scatter_metrics(&registry)?,
            listener: register_listener_metrics(&registry)?,
            slab: register_slab_metrics(&registry)?,
            chunk_store: register_chunk_store_metrics(&registry)?,
            device_io: register_device_io_metrics(&registry)?,
            #[cfg(feature = "efa")]
            rdma,
            registry,
        })
    }

    /// Publish the configured memory budget, term by term (quality item R3).
    ///
    /// Call once at startup, right after the configuration is known and before
    /// anything can be served — the values are static for the process's life, so
    /// there is nothing to refresh on a scrape and no atomics to read. Every term is
    /// published, including the zeroes: a series that appears and disappears with a
    /// feature flag is one a dashboard and an alert both have to special-case.
    pub fn set_memory_budget(&self, budget: &crate::memory_budget::MemoryBudget) {
        #[allow(clippy::cast_precision_loss)]
        for (term, bytes) in budget.terms() {
            let counted = if term.counted() { "true" } else { "false" };
            self.memory_budget
                .terms
                .with_label_values(&[term.label(), counted])
                .set(bytes as f64);
        }
        #[allow(clippy::cast_precision_loss)]
        self.memory_budget.total.set(budget.total() as f64);
    }

    /// Wire the delivery quota so [`Self::encode`] can publish how much client
    /// memory is pinned. Idempotent; call once, right after the proxy's quota is
    /// built.
    pub fn set_delivery_quota(&self, quota: std::sync::Arc<crate::delivery::DeliveryQuota>) {
        let _ = self.delivery.quota.set(quota);
    }

    /// Refresh the pinned-client-bytes gauge. No-op until
    /// [`Self::set_delivery_quota`] has wired a quota (a daemon with delivery
    /// disabled stays at 0, which is the truth).
    fn refresh_delivery(&self) {
        if let Some(quota) = self.delivery.quota.get() {
            self.delivery.pinned_bytes.set(quota.in_use() as f64);
        }
    }

    /// Wire this node's staging area and its coordinator's window slots, so the
    /// scatter's two bounds have something to read — the `pacer_scatter_staged_bytes*`
    /// and `pacer_scatter_windows_in_flight*` series.
    ///
    /// Call once at startup, only when `scatter.enabled` — and with the **same**
    /// `staging` the peer server is handed, since one node-wide budget covers both
    /// roles. Without it every bound gauge reads 0, which is exactly right for a node
    /// that does not scatter. Idempotent; a second call is ignored.
    pub fn set_scatter_bounds(
        &self,
        staging: std::sync::Arc<crate::staging::StagingArea>,
        windows: std::sync::Arc<crate::coordinate::WindowSlots>,
    ) {
        let _ = self.scatter.bounds.staging.set(staging);
        let _ = self.scatter.bounds.windows.set(windows);
    }

    /// Publish the scatter's bounds. No-op until [`Self::set_scatter_bounds`] has wired
    /// them.
    ///
    /// **The two peaks are read, not computed here** — they are latched where the bytes
    /// and the slots are taken, so a residency that peaked and drained between two
    /// scrapes is still reported. This only copies them out, and samples the two live
    /// quantities beside them; the ceilings are static, but are set on every scrape
    /// rather than once at startup so a wiring that happened after the first scrape
    /// cannot leave them at 0.
    #[allow(clippy::cast_possible_wrap)]
    fn refresh_scatter(&self) {
        let b = &self.scatter.bounds;
        if let Some(staging) = b.staging.get() {
            b.staged_bytes.set(staging.staged_bytes() as i64);
            b.staged_bytes_peak.set(staging.staged_bytes_peak() as i64);
            b.staging_budget_bytes.set(staging.budget_bytes() as i64);
        }
        if let Some(windows) = b.windows.get() {
            b.windows_in_flight.set(windows.in_flight() as i64);
            b.windows_in_flight_peak.set(windows.peak() as i64);
            b.windows_in_flight_limit.set(windows.limit() as i64);
        }
    }

    /// Wire the ADR-0033 chunk store so its gauges have something to read.
    ///
    /// Call once at startup, only on `diskTier=store`. Without it every chunk-store
    /// series reads 0, which is exactly right for a daemon whose chunks are in foyer.
    pub fn set_chunk_store(&self, store: pacer_cache::store::ChunkStore) {
        let _ = self.chunk_store.store.set(store);
    }

    /// Resolve which block devices back `cache_dir`, so [`Self::encode`] can publish their
    /// counters. Idempotent; call once at startup.
    ///
    /// Wired from the cache directory rather than from the chunk store on purpose: the
    /// question "did this rate come off a device" is the same question on `diskTier=foyer`,
    /// where `bench/ladder/README.md` first asked it about `md127` and nothing could answer.
    /// Resolving costs three procfs reads, once.
    ///
    /// A directory whose devices cannot be resolved simply publishes no series at all, rather
    /// than publishing a zero: absent means "unknown", where a 0 would say "the device served
    /// nothing" and condemn a tier that was fine. See [`DeviceIoMetrics`].
    ///
    /// (Named as the type and not as its `devices` field on purpose — the field is private, and
    /// `cargo doc` runs at `-D warnings` with `rustdoc::private_intra_doc_links` denied, so a
    /// link to it fails the build even though clippy and every test pass.)
    pub fn set_cache_dir(&self, cache_dir: &std::path::Path) {
        let devices = crate::diskstats::backing_devices(cache_dir);
        if devices.is_empty() {
            tracing::warn!(
                dir = %cache_dir.display(),
                "cannot resolve the cache directory's backing block devices; \
                 pacer_node_device_*_bytes_total will be absent, so a disk-tier rate \
                 measured on this node has NO evidence that it came off a device"
            );
        } else {
            tracing::info!(
                dir = %cache_dir.display(),
                devices = %devices.join(","),
                "backing block devices resolved for the /proc/diskstats honesty check"
            );
        }
        let _ = self.device_io.devices.set(devices);
    }

    /// Publish the backing devices' cumulative counters. No-op until
    /// [`Self::set_cache_dir`] has resolved a device set.
    ///
    /// One `/proc/diskstats` read per scrape, which is a few kilobytes of kernel-formatted
    /// text — the same order as the `/proc/self/*` reads the process gauges already do.
    fn refresh_device_io(&self) {
        let Some(devices) = self.device_io.devices.get() else {
            return;
        };
        if devices.is_empty() {
            return;
        }
        self.publish_device_counters(&crate::diskstats::sample(devices));
    }

    /// Set both gauges for every device in `counters`.
    ///
    /// Split from [`Self::refresh_device_io`] so the label shape and the units are testable
    /// without a `/proc/diskstats` to read — the same seam
    /// [`crate::diskstats::counters`] is split at, for the same reason.
    #[allow(clippy::cast_precision_loss)]
    fn publish_device_counters(&self, counters: &[crate::diskstats::DeviceCounters]) {
        for one in counters {
            let labels = &[one.device.as_str()];
            self.device_io
                .read_bytes
                .with_label_values(labels)
                .set(one.read_bytes as f64);
            self.device_io
                .written_bytes
                .with_label_values(labels)
                .set(one.written_bytes as f64);
        }
    }

    /// Copy the store's atomics into the gauges. No-op until
    /// [`Self::set_chunk_store`] has wired one.
    ///
    /// Nine loads and two derived values per scrape. Cheap enough to do unconditionally,
    /// and doing it here rather than at the call site is what keeps the store free of a
    /// metrics dependency — see [`ChunkStoreMetrics`].
    #[allow(clippy::cast_precision_loss)]
    fn refresh_chunk_store(&self) {
        let Some(store) = self.chunk_store.store.get() else {
            return;
        };
        let stats = store.stats();
        let load =
            |c: &std::sync::atomic::AtomicU64| c.load(std::sync::atomic::Ordering::Relaxed) as f64;
        let m = &self.chunk_store;
        m.hits.set(load(&stats.hits));
        m.misses.set(load(&stats.misses));
        m.read_bytes.set(load(&stats.read_bytes));
        m.written_bytes.set(load(&stats.written_bytes));
        m.writes.set(load(&stats.writes));
        m.write_dedups.set(load(&stats.write_dedups));
        m.evictions.set(load(&stats.evictions));
        m.key_mismatches.set(load(&stats.key_mismatches));
        m.crc_mismatches.set(load(&stats.crc_mismatches));
        m.corrupt_slots.set(load(&stats.corrupt_slots));
        m.io_errors.set(load(&stats.io_errors));
        m.scan_recovered.set(load(&stats.scan_recovered));
        /// Nanoseconds per second, for turning the store's integer nanos into the
        /// seconds Prometheus convention wants.
        const NANOS_PER_SEC: f64 = 1e9;
        m.read_seconds_mean
            .set(stats.mean_read_nanos().unwrap_or(0) as f64 / NANOS_PER_SEC);
        m.read_seconds_max
            .set(load(&stats.read_nanos_max) / NANOS_PER_SEC);
        m.service_seconds_mean
            .set(stats.mean_service_nanos().unwrap_or(0) as f64 / NANOS_PER_SEC);
        m.service_seconds_max
            .set(load(&stats.service_nanos_max) / NANOS_PER_SEC);
        m.queue_seconds_mean
            .set(stats.mean_queue_nanos().unwrap_or(0) as f64 / NANOS_PER_SEC);
        // The sums the three means above are derived from, so a scrape pair can attribute a
        // per-hit cost to the interval between them instead of to the process's whole life.
        m.service_seconds_total
            .set(load(&stats.service_nanos_total) / NANOS_PER_SEC);
        m.read_seconds_total
            .set(load(&stats.read_nanos_total) / NANOS_PER_SEC);
        m.scan_seconds.set(load(&stats.scan_nanos) / NANOS_PER_SEC);
        m.slots_used.set(store.len() as f64);
        m.slots_capacity.set(store.capacity_slots() as f64);
    }

    /// The registry foyer's mixtrics metrics are also registered into.
    pub fn registry(&self) -> &Registry {
        &self.registry
    }

    /// Handle to the directory-RPC timer for one `op` (`lookup`/`admit`/
    /// `evict`). Start it immediately before the in-lock directory call and
    /// let the returned guard drop right after, so the observed span is the
    /// shard op alone — the B4 Step-6 CPU-cost input (planning/15).
    pub fn dir_rpc_timer(&self, op: &str) -> Histogram {
        self.dir_rpc_seconds.with_label_values(&[op])
    }

    /// Refresh the process CPU-seconds gauge from `/proc/self/stat`. No-op on
    /// non-Linux (the gauge stays at its initial 0) and on any read/parse
    /// failure — a missing CPU sample must never break a metrics scrape.
    fn refresh_process_cpu(&self) {
        #[cfg(target_os = "linux")]
        if let Some(secs) = read_proc_cpu_seconds() {
            self.process_cpu_seconds.set(secs);
        }
    }

    /// Refresh the RSS gauge from `/proc/self/status`. Same shape and same
    /// failure policy as [`Self::refresh_process_cpu`]: a missing sample leaves
    /// the previous value and must never break a scrape, because losing the
    /// memory series is exactly the situation this metric exists to prevent.
    fn refresh_process_memory(&self) {
        #[cfg(target_os = "linux")]
        if let Some(bytes) = read_proc_resident_bytes() {
            self.process_resident_memory_bytes.set(bytes);
        }
    }

    /// Refresh the backend-socket gauges from `/proc/net/tcp{,6}`. Same failure policy as
    /// the two above: an unreadable `/proc` leaves the previous values and never breaks a
    /// scrape.
    ///
    /// Both files are read and their counts summed, because a dual-stack node may reach the
    /// backend over either and a count from one alone would understate SILENTLY — which is
    /// the failure mode these gauges exist to remove, not to reproduce.
    fn refresh_backend_sockets(&self) {
        #[cfg(target_os = "linux")]
        {
            /// The backend's port. S3 is HTTPS-only in every deployment this daemon serves
            /// (ADR-0006 re-signs and forwards to a real S3 endpoint), so this is a constant
            /// rather than parsed out of `backend.endpoint` — which in a test or local run
            /// is some other port, and deriving it would make these gauges silently zero on
            /// exactly the fleets they are for.
            const BACKEND_PORT: u16 = 443;

            let v4 = std::fs::read_to_string("/proc/net/tcp").unwrap_or_default();
            let v6 = std::fs::read_to_string("/proc/net/tcp6").unwrap_or_default();
            let (connections, endpoints) =
                parse_established_peers(&[v4.as_str(), v6.as_str()], BACKEND_PORT);
            self.backend_connections.set(connections as f64);
            self.backend_peer_endpoints.set(endpoints as f64);
        }
    }

    /// Refresh the cgroup gauges from this container's own `sysfs` files. Same shape
    /// and same failure policy as its neighbours: no cgroup, or a sample that cannot
    /// be taken, leaves the previous values rather than breaking the scrape.
    ///
    /// **An absent field is left unset rather than zeroed.** That distinction is the
    /// whole reason each field is an `Option`: a 0 in
    /// `pacer_cgroup_memory_file_bytes` asserts "this container holds no page
    /// cache", which is exactly the false conclusion the old instrument reached by
    /// accident. An unset gauge reads 0 too — but it reads 0 *always*, on every
    /// scrape from boot, which is recognisable as "not measured here" in a way that
    /// a value that dropped to 0 is not.
    ///
    /// An unlimited cgroup publishes `+Inf` for the limit, not 0: `max − current`
    /// then reads as unbounded headroom, where 0 would read as a container already
    /// over its limit.
    #[allow(clippy::cast_precision_loss)]
    fn refresh_cgroup(&self) {
        let Some(s) = crate::cgroup::sample() else {
            return;
        };
        let set = |gauge: &Gauge, value: Option<u64>| {
            if let Some(v) = value {
                gauge.set(v as f64);
            }
        };
        let c = &self.cgroup;
        set(&c.current, s.current_bytes);
        set(&c.file, s.file_bytes);
        set(&c.file_dirty, s.file_dirty_bytes);
        set(&c.file_writeback, s.file_writeback_bytes);
        set(&c.anon, s.anon_bytes);
        set(&c.oom, s.oom_events);
        set(&c.oom_kill, s.oom_kill_events);
        // The limit is the one field whose absence has a MEANING (unlimited) rather
        // than being a gap, so it does not go through `set`.
        c.max.set(s.max_bytes.map_or(f64::INFINITY, |m| m as f64));
    }

    /// Refresh the allocator gauges from `mallinfo2`. Same failure policy as its
    /// neighbours: a platform without the call (or a sample that cannot be taken)
    /// leaves the previous values rather than breaking the scrape.
    ///
    /// Sampled here, on the scrape, and never on the data path — `mallinfo2` takes
    /// each arena's lock in turn, which is cheap once every scrape interval and
    /// would be a contention source per request.
    fn refresh_malloc(&self) {
        if let Some(s) = crate::memstats::sample() {
            self.malloc.heap.set(s.heap_bytes as f64);
            self.malloc.mmapped.set(s.mmapped_bytes as f64);
            self.malloc.in_use.set(s.in_use_bytes as f64);
            self.malloc.free_retained.set(s.free_retained_bytes as f64);
        }
    }

    /// Wire the RDMA transport so [`Self::encode`] can publish its copy-timer
    /// accumulators. Idempotent; a second call (or a build without the RDMA
    /// plane) is ignored. Call once, right after the transport is built.
    #[cfg(feature = "efa")]
    pub fn set_efa_transport(&self, efa: std::sync::Arc<pacer_transport::efa::EfaRdmaTransport>) {
        let _ = self.rdma.transport.set(efa);
    }

    /// Refresh the RDMA gauges from the transport's live accumulators. No-op
    /// until [`Self::set_efa_transport`] has wired a transport (gRPC-only nodes
    /// stay at 0).
    /// Refresh the client-edge series from the transport's three bounded maps (R4).
    ///
    /// Its own method rather than more lines in [`Self::refresh_rdma_copy`], which is at the
    /// function-length lint's budget. Lock-free on the transport side: each registry keeps its
    /// counters outside its own mutex so a scrape cannot queue behind a delivery.
    #[cfg(feature = "efa")]
    fn refresh_client_edge(&self, efa: &pacer_transport::efa::EfaRdmaTransport) {
        let stats = efa.client_edge_stats();
        let edge = &self.rdma.client_edge;
        edge.ah_entries.set(stats.client_ah_entries as f64);
        edge.ready_entries.set(stats.client_ready_entries as f64);
        edge.announced_entries.set(stats.announced_entries as f64);
        for reason in pacer_transport::client_registry::EvictionReason::all() {
            edge.evictions_total
                .with_label_values(&[reason.label()])
                .set(stats.evicted(reason) as f64);
        }
    }

    #[cfg(feature = "efa")]
    fn refresh_rdma_copy(&self) {
        if let Some(efa) = self.rdma.transport.get() {
            self.refresh_client_edge(efa);
            self.rdma
                .holder_copy_seconds
                .set(efa.holder_copy_nanos() as f64 / NANOS_PER_SEC);
            self.rdma
                .requester_copy_seconds
                .set(efa.requester_copy_nanos() as f64 / NANOS_PER_SEC);
            self.rdma
                .requester_slots_in_use
                .set(efa.requester_slots_in_use() as f64);
            self.rdma.rails.set(efa.rail_count() as f64);
            self.rdma
                .post_batch_seconds
                .set(efa.post_batch_nanos() as f64 / NANOS_PER_SEC);
            self.rdma
                .write_completion_wait_seconds
                .set(efa.write_completion_wait_nanos() as f64 / NANOS_PER_SEC);
            // A raw count, not a nanosecond accumulator — no NANOS_PER_SEC.
            self.rdma.cq_errors_total.set(efa.cq_error_count() as f64);
            self.rdma
                .unobserved_completion_failures_total
                .set(efa.unobserved_completion_failures() as f64);
            let (retries, declines) = efa.client_write_unknown_peer();
            self.rdma.unknown_peer_retries_total.set(retries as f64);
            self.rdma.unknown_peer_declines_total.set(declines as f64);
            let (orphaned, held) = efa.orphaned_write_sources();
            self.rdma.write_sources_orphaned_total.set(orphaned as f64);
            self.rdma.write_sources_held.set(held as f64);
            // Per-rail series (planning/19 D5.1). Sampled here rather than
            // incremented at the source for the same reason as every gauge above:
            // the transport owns cheap atomics on its hot path and the scrape pays
            // the labelled-metric cost, so a 32-rail node adds no per-WRITE work.
            for s in efa.rail_stats() {
                let rail = s.rail.to_string();
                let numa = s
                    .numa_node
                    .map_or_else(|| "unknown".to_string(), |n| n.to_string());
                let set = |metric: &str, v: f64| {
                    self.rdma
                        .rail
                        .with_label_values(&[&rail, &numa, metric])
                        .set(v);
                };
                set("writes_in_flight", s.writes_in_flight as f64);
                set("writes_total", s.writes_total as f64);
                set("write_bytes_total", s.write_bytes_total as f64);
                set("ranges_in_use", s.ranges_in_use as f64);
                // The pool a DELIVERY leases from, and its size. `ranges_in_use` above is the
                // requester arena, which a client-memory WRITE never touches — so occupancy of
                // the staging pool that every un-slabbed WRITE does touch had no series at all.
                set("staging_in_use", s.staging_in_use as f64);
                set("staging_ranges", s.staging_ranges as f64);
                // Per-rail rather than aggregate because the failure this makes visible is
                // *shape*, not volume: one rail's AH evicted after a slow serve is by design,
                // while all 32 evicted in one scrape is the collapse that took the
                // RDMA-served fraction to 0.313 (planning/19 § Track D item 1). A total
                // cannot distinguish those, which is why the incident was diagnosed from a
                // log line repeated 2675 times instead of from a metric.
                set("ah_evictions", s.ah_evictions as f64);
            }
        }
    }

    /// Render the registry in Prometheus text exposition format.
    pub fn encode(&self) -> String {
        self.refresh_process_cpu();
        self.refresh_process_memory();
        self.refresh_backend_sockets();
        self.refresh_cgroup();
        self.refresh_malloc();
        self.refresh_delivery();
        self.refresh_scatter();
        self.refresh_chunk_store();
        self.refresh_device_io();
        #[cfg(feature = "efa")]
        self.refresh_rdma_copy();
        let mut buf = Vec::new();
        let encoder = TextEncoder::new();
        if let Err(e) = encoder.encode(&self.registry.gather(), &mut buf) {
            return format!("# encode error: {e}\n");
        }
        String::from_utf8(buf).unwrap_or_default()
    }
}

/// Read cumulative process CPU time (`utime + stime`) in seconds from
/// `/proc/self/stat`. Returns `None` on any read or parse failure.
///
/// The two fields sit at positions 14 and 15 (1-indexed) of the stat line, but
/// field 2 (`comm`) is a parenthesized, space-permitting command name, so we
/// split *after* the closing `)` to keep the remaining fields positionally
/// stable.
#[cfg(target_os = "linux")]
fn read_proc_cpu_seconds() -> Option<f64> {
    /// Offset of `utime` among the whitespace-separated fields that follow the
    /// `comm` field's closing `)` (state=0, ppid=1, …, utime=11, stime=12).
    const UTIME_AFTER_COMM: usize = 11;

    let stat = std::fs::read_to_string("/proc/self/stat").ok()?;
    let after_comm = stat.rsplit_once(')')?.1;
    let mut fields = after_comm.split_whitespace();
    let utime: u64 = fields.nth(UTIME_AFTER_COMM)?.parse().ok()?;
    let stime: u64 = fields.next()?.parse().ok()?;
    Some((utime + stime) as f64 / USER_HZ)
}

/// Established TCP connections to `port`, and how many DISTINCT remote addresses they
/// reach, over the text of `/proc/net/tcp` and/or `/proc/net/tcp6`.
///
/// # Why this exists, and why it is two numbers rather than one
///
/// The save path lands on ~9.4 Gbps per node whatever its shape, and no documented network
/// cap fits: the nodes are 40 Gbps, the backend is reached through a **gateway** VPC endpoint
/// (no NAT, no interface-endpoint ENI, no internet gateway), and the concurrency that looks
/// like a cap is Little's Law's output — 3.5x the bytes in flight moved the rate 0.14 %
/// (`bench/ladder/results/w1-peer-flows-and-part-size.md`). Two readings survive, and **the
/// pair below is what separates them**:
///
/// * **few distinct endpoints** — the ceiling may be per destination IP, and spreading
///   connections over more S3 front-ends is the lever. That is what the CRT client
///   (`aws-c-s3`) does that this SDK does not, so this is the reading that would justify
///   adopting it.
/// * **many endpoints, aggregate still pinned** — not addressable by any client change, and
///   the next place to look is node CPU (the READ path's ceiling was exactly that, 84 % of
///   it kernel socket work).
///
/// Neither number decides it alone: a busy node with few endpoints is consistent with both.
///
/// # Why `/proc/net/tcp` and not `ss -tin`
///
/// The runtime image is `amazonlinux-minimal` assembled COPY-only (no `RUN`, so kaniko never
/// emulates), so it carries no `iproute2` — the bench harness already works around that same
/// image having no `curl` by scraping through a separate pod. `/proc/net/*` needs no tooling
/// at all, and reading it *here* rather than from outside also means the sample is taken in
/// the right network namespace by construction (the daemon is not `hostNetwork`).
///
/// What that gives up is per-connection `tcp_info` rates. They are derivable — an arm knows
/// its own byte total and wall clock — while the endpoint count is derivable from nothing.
///
/// # The format
///
/// One connection per line after a header, with `rem_address` as `HEX_ADDR:HEX_PORT` and
/// `st` the state. `01` is `TCP_ESTABLISHED`, and only established rows count: a socket in
/// `TIME_WAIT` names an endpoint this node has stopped using, and counting it would inflate
/// both numbers exactly while a burst drains.
///
/// The v4 file's address hex is byte-swapped per 32-bit word, and this function deliberately
/// never decodes it: two rows share an endpoint exactly when their `rem_address` strings are
/// equal, so the raw token is a sound identity and one that cannot be decoded wrongly. The
/// cost is that the *value* is not human-readable — which is why the report beside it prints
/// a COUNT and not a list.
// The only caller is `refresh_backend_sockets`'s Linux-only body, but this stays free of
// `#[cfg(target_os = "linux")]`: it is pure text-to-numbers, and the suite that pins it has
// to run on the machine a developer edits it on. Same idiom as the gauge fields it feeds.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_established_peers(proc_net_tcp: &[&str], port: u16) -> (u64, u64) {
    /// `st` value for `TCP_ESTABLISHED` in `/proc/net/tcp`.
    const ST_ESTABLISHED: &str = "01";
    /// Field index of `rem_address` on a row (`sl`=0, `local_address`=1).
    const REM_ADDRESS_FIELD: usize = 2;
    /// Field index of `st`.
    const STATE_FIELD: usize = 3;

    let want_port = format!("{port:04X}");
    let mut endpoints = std::collections::BTreeSet::new();
    let mut connections = 0u64;
    for text in proc_net_tcp {
        for line in text.lines().skip(1) {
            let fields: Vec<&str> = line.split_whitespace().collect();
            let (Some(rem), Some(state)) = (fields.get(REM_ADDRESS_FIELD), fields.get(STATE_FIELD))
            else {
                continue;
            };
            if *state != ST_ESTABLISHED {
                continue;
            }
            let Some((addr, port_hex)) = rem.rsplit_once(':') else {
                continue;
            };
            if !port_hex.eq_ignore_ascii_case(&want_port) {
                continue;
            }
            connections += 1;
            endpoints.insert(addr.to_owned());
        }
    }
    (connections, endpoints.len() as u64)
}

/// Read resident set size in bytes from `/proc/self/status`. Returns `None` on any
/// read or parse failure.
///
/// Reads `VmRSS` from `status` rather than field 24 of `stat`: `stat` reports RSS
/// in pages, which would need the page size, while `status` states its own unit.
#[cfg(target_os = "linux")]
fn read_proc_resident_bytes() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_vm_rss_bytes(&status)
}

/// Extract `VmRSS` from `/proc/*/status` contents, in bytes.
///
/// Split from the read so the unit and field assumptions are testable without a
/// live `/proc`. Compiled on every platform for that reason, even though its only
/// non-test caller is Linux-gated — hence the `dead_code` allowance elsewhere,
/// matching how the process gauges above are handled.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
fn parse_vm_rss_bytes(status: &str) -> Option<f64> {
    /// `/proc/*/status` reports `VmRSS` in kibibytes, always — the line carries a
    /// literal `kB` suffix that means kiB despite the spelling.
    const VM_RSS_UNIT_BYTES: f64 = 1024.0;

    let line = status.lines().find(|l| l.starts_with("VmRSS:"))?;
    let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib * VM_RSS_UNIT_BYTES)
}

#[cfg(test)]
mod tests {
    use super::parse_established_peers;
    use super::parse_vm_rss_bytes;
    use super::Encoder;
    use super::Metrics;

    /// Before a store is wired, every chunk-store series must be present and 0 — present
    /// so a dashboard does not have to branch on `config.diskTier`, and 0 because that is
    /// the truth for a daemon whose chunks are in foyer. An absent series would read as
    /// "unknown" to Prometheus, which is a different and worse thing.
    #[test]
    fn chunk_store_series_exist_and_read_zero_without_a_store() {
        let metrics = Metrics::new().expect("registry");
        let text = metrics.encode();
        for series in [
            "pacer_chunk_store_hits_total",
            "pacer_chunk_store_read_bytes_total",
            "pacer_chunk_store_read_seconds_mean",
            "pacer_chunk_store_key_mismatches_total",
            "pacer_chunk_store_slots_capacity",
            "pacer_chunk_store_scan_seconds",
        ] {
            assert!(text.contains(series), "{series} must be registered");
            assert!(
                text.contains(&format!("{series} 0")),
                "{series} must read 0 with no store wired"
            );
        }
    }

    /// The device series carry BYTES under a `device` label, and reads are not writes.
    ///
    /// Asserted through the encoder because the label name and the unit are what a harness
    /// parses: `pacer_daemon_metrics.py` keys these by `device` and divides the tier's bytes
    /// by them, so a renamed label or a sector-vs-byte slip would silently produce an
    /// honesty ratio 512x off — which is exactly the direction that turns a page-cache read
    /// into a plausible-looking device read.
    #[test]
    fn the_device_series_publish_bytes_per_device_and_keep_reads_apart_from_writes() {
        /// Distinct, unmistakable byte counts, so a swapped field cannot pass.
        const READ: u64 = 8 << 20;
        const WRITTEN: u64 = 1 << 20;
        let metrics = Metrics::new().expect("registry");
        metrics.publish_device_counters(&[crate::diskstats::DeviceCounters {
            device: "nvme1n1".to_owned(),
            read_bytes: READ,
            written_bytes: WRITTEN,
        }]);
        let text = metrics.encode();
        assert!(
            text.contains(&format!(
                "pacer_node_device_read_bytes_total{{device=\"nvme1n1\"}} {READ}"
            )),
            "the read gauge must carry bytes under a device label; got:\n{text}"
        );
        assert!(
            text.contains(&format!(
                "pacer_node_device_written_bytes_total{{device=\"nvme1n1\"}} {WRITTEN}"
            )),
            "the write gauge must be separate from the read gauge; got:\n{text}"
        );
    }

    /// A daemon that cannot name its backing devices must publish NOTHING here, not a zero.
    ///
    /// The difference is the whole point of the series: a 0 says "the device served no
    /// bytes", which would make the honesty ratio 0 and condemn a tier that was in fact
    /// fine. An absent series says "unknown", which is the truth and is what stops a
    /// harness from computing a ratio at all.
    #[test]
    fn an_unresolvable_cache_dir_publishes_no_device_series_rather_than_zero() {
        let metrics = Metrics::new().expect("registry");
        let text = metrics.encode();
        assert!(
            !text.contains("pacer_node_device_read_bytes_total{"),
            "no device may be published before one is resolved; got:\n{text}"
        );
    }

    /// With a store wired, the scrape must reflect it — and specifically the two
    /// quantities the ADR-0033 gates are read from: bytes served (33.5's numerator) and
    /// the mean cost of a hit (33.4). A refresh that silently did nothing would leave
    /// both at 0 and make a passing arm indistinguishable from a broken one.
    #[tokio::test(flavor = "multi_thread")]
    async fn wiring_a_store_makes_its_counters_visible_to_the_scrape() {
        /// Body bytes written and then read back, so both byte gauges are non-trivial.
        const BODY: usize = 8 << 10;
        let dir = tempfile::tempdir().unwrap();
        let store = pacer_cache::store::ChunkStore::open(pacer_cache::store::StoreConfig {
            dir: dir.path().to_path_buf(),
            chunk_size: 64 << 10,
            capacity_bytes: 4 * ((4 << 10) + (64 << 10)),
            verify_body: false,
            read_shape: pacer_cache::store::ReadShape::TwoRead,
            // Unlimited: this asserts the counters are wired, and a semaphore would only add
            // a way for the test to deadlock without testing anything it is about.
            read_concurrency: 0,
        })
        .await
        .unwrap();
        let chunk = pacer_cache::chunk::CachedChunk::new(bytes::Bytes::from(vec![7u8; BODY]));
        store.put("b/k#100:0", &chunk).await.unwrap();
        store.get("b/k#100:0").await.unwrap().unwrap();

        let metrics = Metrics::new().expect("registry");
        metrics.set_chunk_store(store);
        let text = metrics.encode();
        assert!(text.contains("pacer_chunk_store_hits_total 1"));
        assert!(text.contains("pacer_chunk_store_writes_total 1"));
        assert!(text.contains(&format!("pacer_chunk_store_read_bytes_total {BODY}")));
        assert!(text.contains(&format!("pacer_chunk_store_written_bytes_total {BODY}")));
        assert!(text.contains("pacer_chunk_store_slots_used 1"));
        assert!(text.contains("pacer_chunk_store_slots_capacity 4"));
        assert!(text.contains("pacer_chunk_store_key_mismatches_total 0"));
        // The mean must be positive: a hit was recorded, so a 0 here means the nanos
        // never reached the gauge.
        let mean = text
            .lines()
            .find_map(|l| l.strip_prefix("pacer_chunk_store_read_seconds_mean "))
            .expect("mean series present")
            .parse::<f64>()
            .expect("mean parses");
        assert!(
            mean > 0.0,
            "a recorded hit must yield a positive mean, got {mean}"
        );

        // **The SUMS, which is what makes a per-hit cost attributable to an interval.** A mean
        // over all hits since startup cannot be: the C5 arm reads tens of GiB back through the
        // daemon after its clock stops, and every one of those low-concurrency reads pulls the
        // lifetime mean down. `Δsum / Δhits` over one arm's own scrape pair is the quantity
        // gate 33.4 is about. With exactly one hit recorded here the sum must equal the mean,
        // which is the cheapest possible check that they are derived from the same nanos.
        let value_of = |series: &str| -> f64 {
            text.lines()
                .find_map(|l| l.strip_prefix(&format!("{series} ")))
                .unwrap_or_else(|| panic!("{series} must be registered"))
                .parse()
                .expect("series parses")
        };
        let read_total = value_of("pacer_chunk_store_read_seconds_total");
        let service_total = value_of("pacer_chunk_store_service_seconds_total");
        assert!(service_total > 0.0, "one hit did service work");
        assert!(
            (read_total - mean).abs() < f64::EPSILON,
            "with one hit the summed read seconds ({read_total}) must equal the mean ({mean}) — \
             if they differ the sum and the mean are not the same nanos"
        );
        assert!(
            service_total <= read_total,
            "service ({service_total}) is measured inside the interval read ({read_total}) \
             covers, so it cannot exceed it"
        );
    }

    /// A failed delivery WRITE must be countable, under its completion status, and apart
    /// from a benign decline.
    ///
    /// **What this does NOT do is exercise the RDMA path.** Reaching
    /// `PacerProxy::write_window` needs the `efa` plane and a client-registered window, i.e.
    /// hardware — the same limit `tests/daemon/delivery.rs` states for `peer_rdma`. Asserting the
    /// increment there would need a fake transport this workspace does not have, so what is
    /// pinned here is the half that is real without hardware and that was the actual defect:
    /// the series exists, is registered, spells its label the way the runbook will, and is
    /// not the same series as `declines`.
    #[test]
    fn a_failed_delivery_write_is_countable_and_apart_from_a_decline() {
        /// The completion status the p6-b200 incident reported — four work requests
        /// (1863/1864/1865/1868) for ONE failed GET.
        const FLUSHED: &str = "work_request_flushed";
        /// Those four, because the help text tells an operator not to read this as a request
        /// count and that instruction is only correct if it is in fact not one.
        const WORK_REQUESTS: usize = 4;

        /// SAMPLE lines for a series, not a `contains` over the whole scrape. These two
        /// families' help text names other series (that is the whole point of the help
        /// text), and `# HELP` lines are part of the encoding — so a bare substring search
        /// matches this metric's own documentation and reports data that is not there. It
        /// did, on the first run of this test.
        fn samples<'a>(text: &'a str, series: &str) -> Vec<&'a str> {
            text.lines().filter(|l| l.starts_with(series)).collect()
        }

        let metrics = Metrics::new().expect("registry");
        // An `IntCounterVec` with no children is dropped by `gather()`, exactly as the
        // histogram family below is, so an idle node publishes no sample of this family.
        // Registration failure is still caught: it would make `Metrics::new` return `Err`.
        let idle = metrics.encode();
        assert!(samples(&idle, "pacer_delivery_write_failures_total").is_empty());

        for _ in 0..WORK_REQUESTS {
            metrics
                .delivery
                .write_failures
                .with_label_values(&[FLUSHED])
                .inc();
        }
        let text = metrics.encode();
        assert_eq!(
            samples(&text, "pacer_delivery_write_failures_total"),
            vec![format!(
                "pacer_delivery_write_failures_total{{reason=\"{FLUSHED}\"}} {WORK_REQUESTS}"
            )],
            "the failure must reach the scrape under its status, and under no other"
        );
        // The two must not be one series: a decline is a slower-but-correct read and a
        // failure is a 500, so no threshold is right for both.
        assert!(
            samples(&text, "pacer_delivery_declines_total").is_empty(),
            "a failure must not increment the benign-decline series"
        );
        // And the property that made the incident invisible, stated as an assertion: a
        // delivery that fails completes no request, so the request counter cannot stand in
        // for it.
        assert_eq!(metrics.delivery.requests.get(), 0);
    }

    /// The pump's unobserved-failure gauge must be present and 0 on a node whose RDMA plane
    /// never came up — a `Gauge` (unlike a `*Vec`) publishes from registration, so absence
    /// here would mean it was never registered and would read as "unknown" forever.
    #[cfg(feature = "efa")]
    #[test]
    fn the_unobserved_completion_failure_gauge_is_registered_and_reads_zero() {
        let text = Metrics::new().expect("registry").encode();
        assert!(
            text.contains("pacer_rdma_unobserved_completion_failures_total 0"),
            "the gauge must publish 0 before any transport is wired; scrape said: {:?}",
            text.lines()
                .find(|l| l.contains("unobserved_completion_failures"))
        );
    }

    /// Every series the scatter's two bounds publish, so one list names the contract.
    const SCATTER_BOUND_SERIES: [&str; 6] = [
        "pacer_scatter_staged_bytes",
        "pacer_scatter_staged_bytes_peak",
        "pacer_scatter_staging_budget_bytes",
        "pacer_scatter_windows_in_flight",
        "pacer_scatter_windows_in_flight_peak",
        "pacer_scatter_windows_in_flight_limit",
    ];

    /// A node that does not scatter must still publish all six, at 0 — present so a
    /// dashboard and the ladder do not branch on `scatter.enabled`, and 0 because that is
    /// the truth for a node with no staging area. An absent series would read as
    /// "unknown" to Prometheus, which is a different and worse thing.
    #[test]
    fn scatter_bound_series_exist_and_read_zero_without_a_scatter() {
        let text = Metrics::new().expect("registry").encode();
        for series in SCATTER_BOUND_SERIES {
            assert!(text.contains(series), "{series} must be registered");
            assert!(
                text.contains(&format!("{series} 0")),
                "{series} must read 0 on a node with no scatter wired"
            );
        }
    }

    /// With the bounds wired, the scrape must carry each PEAK and each CEILING — the two
    /// halves the decision they exist for needs: a peak alone is unjudgeable, and a
    /// ceiling alone says nothing about what happened.
    ///
    /// Deliberately staged-then-committed and acquired-then-released, so every LIVE
    /// quantity is back to 0 at scrape time. That is the shape a scrape actually finds
    /// between two PUTs, and a peak that behaved like a sample would report 0 here — the
    /// failure that would quietly void the arm.
    #[tokio::test]
    async fn wiring_the_scatter_bounds_publishes_its_peaks_and_its_ceilings() {
        /// One staged body.
        const BODY: usize = 8 << 10;
        /// A staging budget with room for two bodies, so the peak is a fraction of it
        /// rather than exactly it.
        const BUDGET: usize = 2 * BODY;
        /// Slots the test coordinator's pipeline is given.
        const SLOTS: usize = 4;
        /// How long a staged chunk may sit — longer than the test, so nothing reaps.
        const TTL: std::time::Duration = std::time::Duration::from_secs(900);

        let staging = std::sync::Arc::new(crate::staging::StagingArea::new(BUDGET, TTL));
        staging.try_stage("b/k#100:0", "up-1", bytes::Bytes::from(vec![7u8; BODY]));
        staging.commit("up-1");
        let windows = std::sync::Arc::new(crate::coordinate::WindowSlots::new(SLOTS));
        drop(windows.acquire().await.expect("a free slot"));

        let metrics = Metrics::new().expect("registry");
        metrics.set_scatter_bounds(
            std::sync::Arc::clone(&staging),
            std::sync::Arc::clone(&windows),
        );
        let text = metrics.encode();
        for (series, want) in [
            ("pacer_scatter_staged_bytes", 0),
            ("pacer_scatter_staged_bytes_peak", BODY),
            ("pacer_scatter_staging_budget_bytes", BUDGET),
            ("pacer_scatter_windows_in_flight", 0),
            ("pacer_scatter_windows_in_flight_peak", 1),
            ("pacer_scatter_windows_in_flight_limit", SLOTS),
        ] {
            assert!(
                text.contains(&format!("{series} {want}")),
                "{series} must read {want}; scrape said: {:?}",
                text.lines().find(|l| l.starts_with(series))
            );
        }
    }

    /// Every `phase` of [`super::ScatterMetrics::phase_seconds`], in the order a window
    /// meets them. One list so the metric, the observation sites and `bench/ladder`'s
    /// report cannot drift onto different phase sets.
    const SCATTER_PHASES: [&str; 8] = [
        super::SCATTER_PHASE_PERMIT_WAIT,
        super::SCATTER_PHASE_OWNER_RPC,
        super::SCATTER_PHASE_OWNER_REFUSED,
        super::SCATTER_PHASE_OWNER_FAILED,
        super::SCATTER_PHASE_LOCAL_UPLOAD,
        super::SCATTER_PHASE_COMPLETE,
        super::SCATTER_PHASE_SERVED_STAGE,
        super::SCATTER_PHASE_SERVED_UPLOAD,
    ];

    /// Cumulative `(le, count)` pairs for one phase, parsed out of the exposition.
    /// `+Inf` parses as an infinite bound, which is what makes "did the sample fall in a
    /// FINITE bucket" checkable below.
    fn phase_buckets(text: &str, phase: &str) -> Vec<(f64, u64)> {
        let prefix = format!("pacer_scatter_phase_seconds_bucket{{phase=\"{phase}\",le=\"");
        let mut out: Vec<(f64, u64)> = text
            .lines()
            .filter_map(|l| l.strip_prefix(&prefix))
            .filter_map(|rest| {
                let (le, count) = rest.split_once("\"} ")?;
                Some((le.parse().ok()?, count.trim().parse().ok()?))
            })
            .collect();
        out.sort_by(|a, b| a.0.total_cmp(&b.0));
        out
    }

    /// **A phase with no observations must be ABSENT from the scrape, not zero — and
    /// before the first window the whole family is absent, `# HELP` line included.**
    ///
    /// This is the opposite convention from the six ceiling gauges beside it, which are
    /// registered unconditionally and read 0, and the difference is not an oversight in
    /// either direction. For a gauge or a counter, 0 and absent mean the same thing. For
    /// a *mean* they do not: `owner_refused` at 0 ms reads as "the wire costs nothing",
    /// while `owner_refused` absent reads as "nothing refused on this arm" — which is
    /// the truth on a fleet with staging headroom, and the opposite conclusion about
    /// ADR-0032 Phase 5. So the daemon must not invent a child series, and
    /// `bench/ladder/scatter.sh` prints `-` rather than `0.0ms` for the absent case.
    ///
    /// A `HistogramVec` with no children is dropped by `Registry::gather` entirely, so
    /// an idle or scatter-off node publishes not one line of this family. That is what
    /// the harness has to tolerate, hence the assertion on the family name and not just
    /// on the children. Registration failure is caught anyway: it would make
    /// `Metrics::new` return `Err` and every test in this module `expect`s it.
    #[test]
    fn a_scatter_phase_is_absent_until_something_is_observed_into_it() {
        let metrics = Metrics::new().expect("registry");
        let idle = metrics.encode();
        assert!(
            !idle.contains("pacer_scatter_phase_seconds"),
            "an unobserved histogram family is dropped by gather(); the harness relies \
             on that reading as `-`. Scrape said: {:?}",
            idle.lines()
                .filter(|l| l.contains("phase_seconds"))
                .collect::<Vec<_>>()
        );

        // One phase observed: that one appears, the other seven still must not.
        metrics.scatter.observe_phase(
            super::SCATTER_PHASE_PERMIT_WAIT,
            std::time::Duration::from_millis(1),
        );
        let text = metrics.encode();
        assert!(text.contains("pacer_scatter_phase_seconds"));
        for phase in SCATTER_PHASES {
            if phase == super::SCATTER_PHASE_PERMIT_WAIT {
                continue;
            }
            assert!(
                !text.contains(&format!(
                    "pacer_scatter_phase_seconds_count{{phase=\"{phase}\"}}"
                )),
                "{phase} has no observations, so it must not publish a child series — a \
                 mean of 0 ms reads as 'this phase is free', which is a different and \
                 wrong claim"
            );
        }
    }

    /// Every `stage` of [`super::DeliveryMetrics::stage_seconds`]. One list for the same
    /// reason [`SCATTER_PHASES`] is one: the metric, the two observation sites in
    /// `proxy::place` and `bench/ladder`'s partition table must not drift onto different
    /// stage sets. A chunk meets `cache_read` and then exactly one placement.
    const DELIVERY_STAGES: [&str; 4] = [
        super::DELIVERY_STAGE_CACHE_READ,
        super::DELIVERY_STAGE_COPY,
        super::DELIVERY_STAGE_RDMA_WRITE,
        super::DELIVERY_STAGE_DIGEST,
    ];

    /// **The four stages are distinct spellings, and an unobserved one stays ABSENT.**
    ///
    /// Both halves are load-bearing for the same reason, and it is not hypothetical. A
    /// pre-layout load asks for no checksum, so `digest` must publish nothing — and that
    /// absence is how `vllm_chunk_partition` proves the gate in `FillCtx::digest_sent`
    /// fired rather than that the CRC was merely fast. If `digest` collided with `copy`
    /// (both are "the thing that touches the bytes", and a copy-paste between the two
    /// observation sites is one keystroke) the partition would silently fold a 131.4 GiB
    /// CRC pass into a stage that is documented as never firing on this path — which is
    /// exactly the residual that made 84–96 % of a 70B load unattributable until
    /// 2026-09-12.
    #[test]
    fn a_delivery_stage_is_absent_until_observed_and_never_shares_a_label() {
        let mut seen = std::collections::BTreeSet::new();
        for stage in DELIVERY_STAGES {
            assert!(
                seen.insert(stage),
                "two delivery stages share the label {stage:?}"
            );
        }

        let metrics = Metrics::new().expect("registry");
        assert!(
            !metrics.encode().contains("pacer_delivery_stage_seconds"),
            "an unobserved family is dropped by gather(); the partition table reads that as `-`"
        );

        metrics
            .delivery
            .stage_seconds
            .with_label_values(&[super::DELIVERY_STAGE_RDMA_WRITE])
            .observe(0.25);
        let text = metrics.encode();
        assert!(text.contains("pacer_delivery_stage_seconds_sum{stage=\"rdma_write\"} 0.25"));
        for stage in DELIVERY_STAGES {
            if stage == super::DELIVERY_STAGE_RDMA_WRITE {
                continue;
            }
            assert!(
                !text.contains(&format!(
                    "pacer_delivery_stage_seconds_count{{stage=\"{stage}\"}}"
                )),
                "{stage} has no observations, so it must not publish a child series: a mean \
                 of 0 s reads as 'this stage is free', and for `digest` that is the opposite \
                 of the conclusion the absence supports"
            );
        }
    }

    /// Every budget term publishes a child series — **including the zeroes** — and the
    /// `counted="true"` subset sums to the total the startup check enforced.
    ///
    /// The zeroes are the point: a series that appears only when its feature is on is one
    /// a dashboard panel and a `PrometheusRule` both have to special-case, and the panel
    /// that matters (`pacer_memory_budget_total_bytes` against
    /// `pacer_cgroup_memory_max_bytes`) is written once for every install. The sum is
    /// asserted through the exposition rather than through `MemoryBudget::total` so the
    /// `counted` label is proved to partition the family — that label is what makes
    /// `sum(pacer_memory_budget_bytes{counted="true"})` a correct query.
    #[test]
    fn every_budget_term_publishes_a_series_and_counted_partitions_them() {
        use crate::memory_budget::Term;

        let metrics = Metrics::new().expect("registry");
        // A distinct value per term, so a mislabelled `with_label_values` cannot pass by
        // coincidence: term i gets (i + 1) bytes.
        let budget =
            crate::memory_budget::MemoryBudget::from_terms_for_test(|term| term as u64 + 1);
        metrics.set_memory_budget(&budget);
        let text = metrics.encode();
        let mut counted_sum = 0_u64;
        for term in Term::ALL {
            let counted = if term.counted() { "true" } else { "false" };
            let line = format!(
                "pacer_memory_budget_bytes{{counted=\"{counted}\",term=\"{}\"}} {}",
                term.label(),
                term as u64 + 1
            );
            assert!(text.contains(&line), "missing or mislabelled: {line}");
            if term.counted() {
                counted_sum += term as u64 + 1;
            }
        }
        assert!(
            text.contains(&format!("pacer_memory_budget_total_bytes {counted_sum}")),
            "the total must be the counted subset's sum, not the whole family's"
        );
    }

    /// An observation must land in **its own** phase's `_sum` and `_count`, in seconds,
    /// and must not create any other phase.
    ///
    /// The failure this catches is a mislabelled observation site: eight phases, one
    /// label, and a copy-paste that charges `served_upload` to `owner_rpc` would make
    /// the wire term come out at exactly zero — a confident answer to the question this
    /// family exists for, and the wrong one.
    #[test]
    fn an_observation_lands_in_its_own_phase_and_creates_no_other() {
        /// A distinctive elapsed time, chosen so the `_sum` can be recognised in the
        /// exposition rather than merely being non-zero.
        const ELAPSED: std::time::Duration = std::time::Duration::from_millis(1234);

        let metrics = Metrics::new().expect("registry");
        metrics
            .scatter
            .observe_phase(super::SCATTER_PHASE_OWNER_RPC, ELAPSED);
        let text = metrics.encode();
        assert!(
            text.contains("pacer_scatter_phase_seconds_count{phase=\"owner_rpc\"} 1"),
            "one observation, one sample; scrape said {:?}",
            text.lines()
                .filter(|l| l.contains("phase_seconds_count"))
                .collect::<Vec<_>>()
        );
        assert!(
            text.contains("pacer_scatter_phase_seconds_sum{phase=\"owner_rpc\"} 1.234"),
            "the sum must be SECONDS, not millis or nanos; scrape said {:?}",
            text.lines().find(|l| l.contains("phase_seconds_sum"))
        );
        for phase in SCATTER_PHASES {
            if phase == super::SCATTER_PHASE_OWNER_RPC {
                continue;
            }
            assert!(
                phase_buckets(&text, phase).is_empty(),
                "{phase} was never observed and must stay absent"
            );
        }
    }

    /// **The buckets must separate the two operating points the write path is known to
    /// run at**, which is the only thing that makes this family's resolution earn its
    /// cardinality.
    ///
    /// `bench/ladder/results/w1-write-ceilings.md` found the coordinator pinned at its
    /// slot limit at both `wif=16` and `wif=64`, so Little's Law puts a window's
    /// slot-hold at `slots × 16 MiB ÷ rate`: ~0.42 s at the chart default and ~1.14 s
    /// with both ceilings lifted. Those are 2.7× apart, and a bucket set with no edge
    /// between them would report a 48 % throughput change as no change at all — the
    /// exact failure mode of a histogram whose edges were picked without an operating
    /// point. Stated as "there exists an edge with a cumulative count of exactly 1", so
    /// it holds however the edges are respaced, and fails the moment they stop
    /// straddling.
    #[test]
    fn the_phase_buckets_separate_the_two_measured_operating_points() {
        /// Slot-hold at the chart default: 16 × 16 MiB ÷ 0.592 GiB/s.
        const AT_DEFAULT_CEILINGS: f64 = 0.42;
        /// Slot-hold with both ceilings lifted: 64 × 16 MiB ÷ 0.878 GiB/s.
        const AT_LIFTED_CEILINGS: f64 = 1.14;

        let metrics = Metrics::new().expect("registry");
        for secs in [AT_DEFAULT_CEILINGS, AT_LIFTED_CEILINGS] {
            metrics.scatter.observe_phase(
                super::SCATTER_PHASE_OWNER_RPC,
                std::time::Duration::from_secs_f64(secs),
            );
        }
        let buckets = phase_buckets(&metrics.encode(), super::SCATTER_PHASE_OWNER_RPC);
        assert!(!buckets.is_empty(), "the phase must publish buckets");
        assert!(
            buckets.iter().any(|(_, count)| *count == 1),
            "no bucket edge lies between {AT_DEFAULT_CEILINGS}s and {AT_LIFTED_CEILINGS}s, \
             so the two measured operating points are indistinguishable: {buckets:?}"
        );
        // And neither may fall past the last finite edge, which would put both in `+Inf`
        // and leave the tail unbounded exactly where a stalled S3 call lives.
        let (last_finite, count) = *buckets
            .iter()
            .rfind(|(le, _)| le.is_finite())
            .expect("a finite top edge");
        assert_eq!(
            count, 2,
            "both samples must land under the top finite edge ({last_finite}s); \
             anything above it is unresolvable"
        );
    }

    /// The cgroup series must be present on every platform, whether or not this
    /// machine has a cgroup to read — a dashboard and an alert are written once, not
    /// per target, and the whole reason this family exists is that the OOM question
    /// had no series at all to point at.
    ///
    /// Values are deliberately NOT asserted: the CI runner, the build pod and a
    /// developer's laptop are three different cgroup situations, and a test that
    /// pinned numbers would be asserting on its own container rather than on this
    /// code.
    #[test]
    fn cgroup_series_are_registered_on_every_platform() {
        let text = Metrics::new().expect("registry").encode();
        for series in [
            "pacer_cgroup_memory_current_bytes",
            "pacer_cgroup_memory_max_bytes",
            "pacer_cgroup_memory_file_bytes",
            "pacer_cgroup_memory_file_dirty_bytes",
            "pacer_cgroup_memory_file_writeback_bytes",
            "pacer_cgroup_memory_anon_bytes",
            "pacer_cgroup_memory_oom_total",
            "pacer_cgroup_memory_oom_kill_total",
        ] {
            assert!(text.contains(series), "{series} must be registered");
        }
    }

    /// A scrape must survive being taken twice in a row, which is the shape a real
    /// Prometheus produces and the one that would catch a refresh that panics on its
    /// second pass (a `OnceLock` misuse, or a file handle held across calls).
    #[test]
    fn scraping_twice_is_safe() {
        let metrics = Metrics::new().expect("registry");
        let first = metrics.encode();
        let second = metrics.encode();
        assert!(first.contains("pacer_cgroup_memory_current_bytes"));
        assert!(second.contains("pacer_cgroup_memory_current_bytes"));
    }

    /// An unlimited cgroup publishes infinity rather than 0, because 0 would make
    /// `max − current` read as "already over the limit" on every unlimited pod in the
    /// fleet. This pins how that actually reaches the wire.
    ///
    /// **It renders as `inf`, not the exposition format's `+Inf`** — the `prometheus`
    /// crate formats gauges with Rust's `f64` `Display`, which spells infinity that
    /// way. Prometheus parses it regardless (its text parser is Go's `ParseFloat`,
    /// which accepts `inf` case-insensitively), so this is safe, but it is not what
    /// the spec's grammar shows and a future reader would otherwise assume the
    /// encoder was doing something wrong. Asserted through the encoder rather than at
    /// the `set`, because the encoder is the only place a non-finite gauge could
    /// break.
    #[test]
    fn an_unlimited_limit_encodes_as_infinity_not_zero() {
        let registry = prometheus::Registry::new();
        let g = super::gauge(&registry, "pacer_cgroup_memory_max_bytes", "help").expect("gauge");
        g.set(f64::INFINITY);
        let mut buf = Vec::new();
        prometheus::TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .expect("encode");
        let text = String::from_utf8(buf).expect("utf8");
        let value = text
            .lines()
            .find_map(|l| l.strip_prefix("pacer_cgroup_memory_max_bytes "))
            .expect("the limit series is present");
        assert_eq!(value, "inf", "unlimited rendered as {value:?}");
        // The property that actually matters, stated independently of the spelling:
        // whatever it renders as, it must parse back to something no finite
        // `current` can exceed.
        let parsed: f64 = value.parse().expect("the rendered limit parses as a float");
        assert!(
            parsed.is_infinite() && parsed.is_sign_positive(),
            "an unlimited cgroup must not publish a finite limit: {parsed}"
        );
    }

    /// The `kB` suffix is kibibytes, so the observed idle-daemon value must come
    /// back as the ~8 GiB the registered RDMA arena actually occupies — a 1000
    /// multiplier would understate it by 2.4%, which is exactly the sort of quiet
    /// error a memory alert would then be wrong about.
    #[test]
    fn vm_rss_is_kibibytes() {
        // Verbatim shape of the real file, including the fields around VmRSS.
        let status =
            "Name:\tpacer-daemon\nVmHWM:\t 8413628 kB\nVmRSS:\t 8413628 kB\nThreads:\t97\n";
        let bytes = parse_vm_rss_bytes(status).expect("VmRSS present");
        assert_eq!(bytes, 8_413_628.0 * 1024.0);
        assert!((bytes / (1024.0 * 1024.0 * 1024.0) - 8.02).abs() < 0.01);
    }

    #[test]
    fn vm_rss_absent_or_malformed_is_none() {
        // A kernel without VmRSS (or a kthread) must not panic the scrape.
        assert!(parse_vm_rss_bytes("Name:\tx\nThreads:\t1\n").is_none());
        // A present-but-unparseable value is equally a no-sample, not a zero:
        // reporting 0 would look like a daemon that freed all its memory.
        assert!(parse_vm_rss_bytes("VmRSS:\t notanumber kB\n").is_none());
        assert!(parse_vm_rss_bytes("VmRSS:\n").is_none());
    }

    /// A real `/proc/net/tcp`: real header, real column order, so the field offsets are
    /// exercised rather than assumed. `1D02A8C0` and `1E02A8C0` are two endpoints;
    /// `01BB` is 443 and `0050` is 80; `st` 01 is ESTABLISHED and 06 is TIME_WAIT.
    const PROC_NET_TCP: &str = "\
  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode
   0: 0100007F:2382 1D02A8C0:01BB 01 00000000:00000000 00:00000000 00000000  1000        0 12345
   1: 0100007F:2383 1D02A8C0:01BB 01 00000000:00000000 00:00000000 00000000  1000        0 12346
   2: 0100007F:2384 1E02A8C0:01BB 01 00000000:00000000 00:00000000 00000000  1000        0 12347
   3: 0100007F:2385 1D02A8C0:01BB 06 00000000:00000000 00:00000000 00000000  1000        0 12348
   4: 0100007F:2386 1D02A8C0:0050 01 00000000:00000000 00:00000000 00000000  1000        0 12349
";

    #[test]
    fn established_peers_counts_connections_and_distinct_endpoints() {
        // Three ESTABLISHED on :443 over TWO endpoints, which is the whole reason both
        // numbers are reported: connections sharing few IPs is the shape that says the
        // ceiling may be per destination IP, and one number cannot show it.
        assert_eq!(parse_established_peers(&[PROC_NET_TCP], 443), (3, 2));
    }

    #[test]
    fn a_socket_that_is_not_established_is_not_counted() {
        // Row 3 reaches the same endpoint as rows 0/1 but is TIME_WAIT. Counting it would
        // name an endpoint this node has stopped using and inflate both numbers exactly
        // while a burst drains — i.e. worst at the moment the gauges are read.
        let (connections, _) = parse_established_peers(&[PROC_NET_TCP], 443);
        assert_eq!(connections, 3, "TIME_WAIT must not be counted");
    }

    #[test]
    fn another_port_is_a_different_question() {
        // Row 4 is ESTABLISHED to :80. These gauges are about the BACKEND, so a plaintext
        // socket or a peer-plane connection must not land in them.
        assert_eq!(parse_established_peers(&[PROC_NET_TCP], 80), (1, 1));
        assert_eq!(parse_established_peers(&[PROC_NET_TCP], 9000), (0, 0));
    }

    #[test]
    fn both_address_families_are_summed() {
        // A dual-stack node may reach the backend over either, and a count from one file
        // alone would understate silently — the failure these gauges exist to remove.
        let v6 = "\
  sl  local_address                         remote_address                        st
   0: 00000000000000000000000001000000:2387 20010DB8000000000000000000000001:01BB 01
";
        assert_eq!(parse_established_peers(&[PROC_NET_TCP, v6], 443), (4, 3));
    }

    #[test]
    fn junk_and_emptiness_yield_zero_rather_than_panicking() {
        // `refresh_backend_sockets` passes `unwrap_or_default()`, so the empty string is a
        // real input on any node where /proc is not readable — and a scrape must never fail
        // for it.
        assert_eq!(parse_established_peers(&[""], 443), (0, 0));
        assert_eq!(parse_established_peers(&["header only\n"], 443), (0, 0));
        assert_eq!(parse_established_peers(&["h\nshort line\n"], 443), (0, 0));
    }
}
