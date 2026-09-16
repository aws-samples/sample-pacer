//! EFA-RDMA peer transport (ADR-0003, ADR-0008, ADR-0018/0019/0021).
//!
//! Holder-driven one-sided RDMA WRITE into requester-supplied pre-registered
//! buffers, hardware-validated by the A0 spike (planning/09; `spike/efa/`):
//! the requester leases a range from its rail's [`RailBuffers`], offers it on
//! the `FetchBlob` gRPC call, and the holder WRITEs the cached body straight
//! into it — the fetch RPC's response doubles as the done signal (ADR-0018
//! point 3), so there is no second completion message. Falls back to
//! [`crate::grpc::GrpcTransport`]'s plain streaming path on ANY error
//! (ADR-0003) — capability probe failure, no cached AH for this peer, a body
//! too large for the leased range, or a WRITE that fails on the wire.
//!
//! Both sides need a *registered* local buffer for the WRITE's local SGE
//! (RDMA cannot source from arbitrary heap memory): the requester's is its
//! leased arena range (the WRITE's destination); the holder stages the cached
//! chunk into its OWN leased range first (the WRITE's source) — the same
//! bounce-buffer shape ADR-0018 describes for the NVMe tier, used here
//! because a cached chunk is a refcounted `Bytes`, not already an MR.
//!
//! Scoping note: the cache unit is a chunk (ADR-0015, B-track — the holder
//! WRITEs one whole cached chunk per fetch). ADR-0020's one-sided-READ
//! directory and read-through-over-RDMA remain later tracks — see `arena.rs`'s
//! module doc.

mod address;
mod affinity;
mod announce;
mod arena;
mod buffers;
mod client_write;
mod completion;
mod context;
mod slab;
mod target;
mod write;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use bytes::Bytes;
use ibverbs::{LocalMemorySlice, ProtectionDomain, QueuePairEndpoint};
use pacer_proto::v1::{BlobMeta, FetchBlobRequest, RdmaBuffer, RdmaCapabilities};
use pacer_ring::directory::{SharerSet, Tier};
use pacer_ring::NodeId;
use tracing::{debug, info, warn};

use crate::grpc::{call_fetch_blob, range_fields, read_first_chunk, GrpcTransport};
use crate::{BlobStream, ByteRange, PeerTransport, TransportError};

// `AhRef` and `DEFAULT_QPS_PER_RAIL` are re-exported because they are already
// reachable API — `AhRef` is what two of `AhCache`'s public methods return, and
// the const is the documented clamp floor of `EfaContext::bring_up_rails` — and a
// `pub` item its own crate cannot name is a worse contract than one it can.
pub use address::{AhCache, AhRef};
// The announcer is the writer's half of ADR-0030 point 2 — one SEND that installs this
// node in a client's address vector so the client's NIC can ACK its WRITEs. Re-exported
// rather than kept private because its caller is the delivery path, which lives in the
// daemon crate; the wire format it posts is `crate::announce`.
pub use announce::{Announcer, ANNOUNCE_TIMEOUT};
// The client-side WRITE path (ADR-0030): `write_into_token` is an inherent method on the
// transport, so only the cache type needs naming from outside — its bound and its counters are
// what the daemon publishes as `pacer_efa_client_*` (see [`ClientEdgeStats`]).
pub use client_write::{write_failure_reason, ClientAhCache, TokenDecline, TokenWrite};
// `ranges_for`/`range_bytes_for_chunk` are re-exported for the same reason as
// `AhRef` above: they ARE the arena's documented sizing arithmetic (ADR-0024 —
// `ranges = budget ÷ chunk`), the geometry a `HostArena` is built from, and the
// rule an operator sizing `PACER_RDMA_ARENA_BYTES` is applying by hand.
pub use arena::{
    range_bytes_for_chunk, ranges_for, ArenaLease, ArenaPages, HostArena, WrittenRange,
};
pub use buffers::{LeasedRange, OwnedRdmaLease, RdmaBuffers, RdmaLease};
// ADR-0028's slab: the cache's RAM tier IS registered memory, so a holder posts
// a WRITE straight out of the cache instead of staging a copy into an arena.
pub use context::{EfaContext, DEFAULT_QPS_PER_RAIL};
pub use slab::{CacheFrame, CacheSlab, FrameWriter};
// ADR-0026's client-memory delivery: the daemon registers a window of a
// client's own segment and holders WRITE cached chunks into it, so the
// requester's arena and the whole HTTP/TCP leg leave the data path. Both types
// are what `pacer_daemon::proxy`'s delivery path names, so they are API.
// `TokenDestination` is ADR-0030's remote half: under a client token the holder writes
// straight into the reading client, so the requester hands it one chunk's destination
// instead of registering anything itself.
pub use target::{ChunkDelivery, ClientTarget, TokenDestination};

/// Startup log line for the placement + window geometry, mirrored into the
/// per-rail metrics ([`RailStats`]) so a run's configuration is visible from both
/// the log and the scrape.
pub use affinity::RailPlacement;

/// The [`RdmaBuffers`] backend every rail leases from — the ONE place the choice
/// of where RDMA buffers live is made (planning/19 § the shared seam). Today
/// ADR-0024's hugepage-backed host-memory arena (track D), which replaced
/// ADR-0018's fixed slot pool; ADR-0022's HBM tier (track H) lands by pointing
/// this alias at its own implementor, because everything below is written
/// against the trait rather than the concrete type. Construction is the only
/// site that names it (a backend's constructor takes backend-specific
/// arguments, so it is deliberately not part of the trait) — see
/// [`EfaRdmaTransport::new`].
pub type RailBuffers = HostArena;

/// Default requester-side arena: the registered bytes a node pins to receive
/// peers' WRITEs, node-wide, split across rails
/// (`PACER_RDMA_ARENA_BYTES` overrides it).
///
/// 4 GiB is exactly what the fixed pool this replaced pinned on the requester
/// side (64 slots × 64 MiB), chosen so the default commits **no new memory** and
/// the chart's `efa.pinnedPoolReservation` needs no change: the win comes from
/// the reshape, not from more RAM — at the 16 MiB default chunk the same 4 GiB
/// is 256 chunk-sized ranges instead of 64 over-provisioned slots (ADR-0024
/// point 2's ~4×).
///
/// It is deliberately BELOW what saturation needs, because the sizing is
/// hold-time arithmetic an operator must do against their own client drain:
/// `ranges = throughput × hold_time ÷ chunk_size`. At the measured ~58 GiB/s
/// transport ceiling and the 645 ms drain of planning/15 rung 1 that is ~2340
/// ranges ≈ **37 GiB** — 1.8 % of a p5.48xlarge's 2 TiB, and what the track-D
/// exit run sets. Raising it must move `efa.pinnedPoolReservation` (and, with
/// hugepages, the node reservation) in lockstep: an arena that cannot be
/// registered is a startup failure by design, not a silent degrade.
pub const DEFAULT_REQUESTER_ARENA_BYTES: usize = 4 << 30;

/// Where a rail's registered memory and its completion reaper are placed
/// (planning/19 D5 step 0) — the operator-visible half of `affinity.rs`.
///
/// This exists as a policy rather than a hardcoded behaviour for one reason: it
/// makes the experiment possible. The ~58 GiB/s bar in planning/18 was measured
/// with NUMA-local buffers and pinned reapers; D4 measured 15.7 GiB/s without
/// either. One image that can run both arms is what turns "probably the placement"
/// into a measurement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RailPlacementPolicy {
    /// Pin each rail's reaper to a distinct CPU on that rail's own NIC NUMA node,
    /// and register that rail's arenas from the same node. The default, and the
    /// discipline the transport-only bench used. Degrades to [`Self::Unpinned`]
    /// per rail wherever sysfs cannot be read.
    NumaLocal,
    /// Place nothing: reapers land wherever the scheduler puts them and arenas
    /// wherever the startup thread happened to run. Reproduces the pre-D5
    /// behaviour exactly — the control arm, and the escape hatch if pinning ever
    /// misbehaves on a host we have not seen (`PACER_RDMA_AFFINITY=0`).
    Unpinned,
}

/// Holder-side arena depth, in ranges, node-wide across rails — how many cached
/// bodies this node can have staged for outbound WRITEs at once.
///
/// A count rather than a byte budget because the serve-admission gate
/// (`pacer_daemon::peer::HOLDER_SERVE_SLOTS`) must not exceed it, and that
/// invariant is checked at compile time where `chunk_size` is not yet known —
/// expressing the arena in bytes would silently break it for any chunk size
/// above the assumed one. The consequence to keep in view: holder pinned memory
/// is `this × chunk_size` (4 GiB at the 16 MiB default — again exactly what the
/// old 64 × 64 MiB holder pool pinned), so raising `chunk_size` raises it
/// proportionally.
///
/// 256 comes from the wire-bound arithmetic, not from the requester's: a holder
/// lease is transient (stage, WRITE, await completion, drop), so at the measured
/// ~58 GiB/s ceiling and ~10 ms in-flight per 16 MiB chunk only ~37 ranges are
/// needed. 256 keeps ~7× headroom for scheduler and completion-pump jitter and
/// matches ADR-0018's chunk-plane reference count.
///
/// ## The client-delivery path leases from here too, and 8 rails' worth is not the wall
///
/// Divided across rails this is **8 ranges — 128 MiB in flight — per rail on a 32-rail p5**,
/// and ADR-0030's delivery leases from it for every WRITE whose source is not an ADR-0028
/// slab frame. A 70B / 8-GPU arm pins every busy rail at `staging_in_use == 8` while only
/// 2-6 WRITEs are on its wire, which looks exactly like a supply bound
/// (`pacer_rdma_rail{metric="staging_in_use"}` exists to show it).
///
/// **It was measured, and it is not one.** 2026-08-25, alternating 8/rail against 64/rail
/// (`bench/ladder/results/c5-multigpu-remeasure.md`), 131.4 GiB into 8 H100s:
///
/// | ranges | delivery GiB/s | mean WRITE hold |
/// |---|---|---|
/// | 256 (8/rail) | 22.0, 19.6, 15.0, 17.5 | ~41 ms |
/// | 2048 (64/rail) | 12.3, 17.0, 20.3, 20.1, 16.9 | ~25 ms |
///
/// Eight times the pool **halves the hold time and does not move the rate** — the run-to-run
/// spread (15-22 GiB/s at a FIXED setting) is larger than any effect it has. So a chunk waits
/// somewhere else, and this number should be raised only alongside a measurement that says
/// what it bought. Keep it at 256: the pinned memory is real (`× chunk_size`, and
/// `efa.pinnedPoolReservation` must cover it), and the saturation signal turned out to be a
/// symptom rather than the cause.
///
/// **The cause was the staging COPY, not the pool it copies into**
/// (`bench/ladder/results/c5-slab-zerocopy-source.md`): mapping ADR-0028's slab so a WRITE
/// reads the cache in place took the same arm 13.7 → **52.7 GiB/s** and lifted in-flight WRITEs
/// from ~29 to 142.7, because a 16 MiB `copy_from_slice` blocks the task driving a request's
/// whole window set. With a slab, nothing leases from here at all — which is the shape this
/// number should be sized against once a slab is the default.
pub const HOLDER_ARENA_RANGES: usize = 256;

/// [`PeerTransport`] over EFA-RDMA, falling back to a wrapped
/// [`GrpcTransport`] on any error. Built once at daemon startup behind the
/// `efa` feature, after [`EfaContext::probe`] confirms the hardware supports
/// it. Also the daemon's entry point for the *holder* side
/// ([`EfaRdmaTransport::serve_via_write`]), since both directions share the
/// same context, AH cache, and per-rail arenas.
pub struct EfaRdmaTransport {
    /// A5 multi-rail: one [`Rail`] per EFA device brought up at startup, in
    /// device order. Rails pair BY INDEX across nodes (requester rail i is
    /// addressed by holder rail i; usable count per peer = however many rails
    /// both sides handshook) because an rkey is only valid at the protection
    /// domain that issued it — the fetch pins `(rail, rkey, addr)` together
    /// and the holder must honor the pin. `Arc` so the A2 proactive
    /// re-handshake — the eviction path spawns it, not awaits it — can share
    /// the rails from its own `'static` task without an `Arc<Self>` (which
    /// the `&self`, trait-bound fetch/serve methods can't hand out).
    rails: Arc<Vec<Rail>>,
    /// This node's announce (ADR-0030 point 2), built on the first client delivery.
    ///
    /// Lazy rather than built in `new`: it registers a small message on every rail's PD,
    /// and a daemon that never serves a client delivery should pay neither the
    /// registrations nor the pinned bytes.
    announcer: tokio::sync::OnceCell<Announcer>,
    /// Round-robin cursor for the requester's rail choice
    /// ([`Self::try_fetch_via_rdma`]): concurrency across chunks is what
    /// aggregates rails (a single fetch rides ONE rail — no intra-chunk
    /// striping in v1), so an even spread of concurrent fetches is the whole
    /// load-balancing story.
    next_rail: AtomicU64,
    /// Round-robin cursor for the **client's** rail choice on a delivery
    /// ([`Self::write_into_token`]). Separate from [`Self::next_rail`] because
    /// they balance different NICs: that one spreads which of OUR rails posts,
    /// this one spreads which of the CLIENT's rails receives — and a node with a
    /// single healthy rail must still stripe a window that names several, which
    /// deriving the destination from the source could not do.
    next_client_rail: AtomicU64,
    /// WRITEs re-posted because the client had not installed this writer yet, and
    /// deliveries that gave up after [`client_write::UNKNOWN_PEER_ATTEMPTS`] and
    /// fell back to a body.
    ///
    /// Counted because the retry is otherwise INVISIBLE: it absorbs a race whose
    /// only previous symptom was a 500, so "it passed" and "it never fired" look
    /// identical without these. A non-zero retries count with zero declines is the
    /// healthy shape; declines climbing means clients are not answering announces
    /// (a starved pump thread, or one that died).
    unknown_peer_retries: AtomicU64,
    unknown_peer_declines: AtomicU64,
    /// The gRPC transport every fetch's control plane rides on (ADR-0019) and
    /// the fallback body path when RDMA isn't used. `Arc` for the same reason
    /// as `ah_cache`: the spawned re-handshake dials over it.
    grpc: Arc<GrpcTransport>,
    /// This node's identity, carried as `FetchBlobRequest.requester_node_id`
    /// so a holder can look up the AH it cached for THIS node.
    local_node: String,
    /// ADR-0028's cache slab, when configured: registered memory the cache's
    /// RAM tier lives in, shared with the daemon's fill path (which stores
    /// chunks into it) and consulted by [`Self::serve_via_write`] (which posts
    /// out of it). `None` — the default until the serve path is measured on
    /// hardware — means every serve stages a copy, exactly as before.
    cache_slab: Option<CacheSlab>,
    /// Cumulative nanoseconds spent in the holder-side stage copy
    /// ([`Self::serve_via_write`], `mod.rs` bounce copy): cached `Bytes` →
    /// leased arena range, the source the WRITE reads from. Measured, not
    /// estimated: B4 (planning/15) needs the *real* share the two RDMA copies
    /// take of serve CPU on-hardware to decide whether a zero-copy path is
    /// worth building. Nanoseconds, monotonic, surfaced as a counter by the
    /// daemon at scrape (a synchronous `copy_from_slice` span is CPU-bound, so
    /// wall ≈ CPU for it). See [`Self::requester_copy_nanos`].
    holder_copy_nanos: AtomicU64,
    /// Cumulative nanoseconds spent in the requester-side copy-out. LEGACY as
    /// of the requester zero-copy path (planning/16 §5): [`blob_from_written_lease`]
    /// no longer copies — it hands the S3 client a `Bytes` that owns the lease
    /// (`bytes::Bytes::from_owner`), so this counter stays at 0 on the
    /// RDMA-served path and its metric (`pacer_rdma_requester_copy_seconds_total`)
    /// reads ~0. Retained (rather than removed) so the gauge stays stable across
    /// the change and to leave a hook if a future fallback ever needs to attribute
    /// a requester-side copy again. Nanoseconds, monotonic. See
    /// [`Self::holder_copy_nanos`].
    requester_copy_nanos: AtomicU64,
    /// Cumulative nanoseconds spent building and submitting the RDMA WRITE's
    /// send batch on the holder side ([`Self::serve_via_write`] →
    /// [`write::post_write`]'s `ctx.post` closure). This is genuinely
    /// CPU-bound, server-role-only work: it covers only the synchronous
    /// batch-build+submit span (which `EfaContext::post` guarantees never
    /// crosses an `.await`), NOT the QP-mutex acquisition that precedes it nor
    /// the completion wait that follows. Because that span does not yield,
    /// wall ≈ CPU for it, so — like [`Self::holder_copy_nanos`] — it is a
    /// valid input to a serve-path CPU/GiB number, immune to client-role
    /// contamination (planning/15 serve-path CPU accounting). Nanoseconds,
    /// monotonic, surfaced as a counter by the daemon at scrape.
    post_batch_nanos: AtomicU64,
    /// Cumulative nanoseconds the holder spent *waiting* for a posted WRITE's
    /// completion ([`Self::serve_via_write`] → [`write::await_write_completion`],
    /// awaiting a `oneshot::Receiver` the completion pump wakes).
    ///
    /// This is WALL-CLOCK, NOT CPU time — the await genuinely yields the task
    /// to the tokio scheduler while the WRITE traverses the wire and the pump
    /// reaps its completion. The elapsed time therefore includes both the
    /// on-wire round-trip AND however long the scheduler took to resume this
    /// task after the pump woke it. Do NOT fold this into any CPU/GiB ratio:
    /// the holder burns ~no CPU during this span, so attributing it as serve
    /// CPU would re-create exactly the whole-process-CPU confound retracted in
    /// planning/15 (a number that moves with unrelated runtime load, not with
    /// real serve work). Its legitimate use is the opposite reading: elevated
    /// wait here — with `post_batch_nanos` flat — is the direct signal that a
    /// busy shared runtime (e.g. the S3 proxy's tasks) is delaying the
    /// completion pump's wakeup, the contention question planning/16 raised.
    /// Nanoseconds, monotonic, surfaced as a counter by the daemon at scrape.
    write_completion_wait_nanos: AtomicU64,
}

/// What one rail is doing right now, for the `pacer_rdma_rail_*` metrics
/// (planning/19 D5). Per-rail visibility was the gap D4 could only fill from EFA
/// *hardware* counters: the daemon itself could not say how many WRITEs a rail had
/// outstanding, so per-rail queueing was invisible — which is also why the
/// in-flight window below cannot be tuned without this.
#[derive(Clone, Copy, Debug)]
pub struct RailStats {
    /// Rail index, i.e. its position in device order and its label in the metrics.
    pub rail: usize,
    /// NUMA node this rail's NIC reports, and therefore where its arenas and
    /// reaper live (ADR-0025). `None` when the topology was unreadable.
    pub numa_node: Option<u32>,
    /// WRITEs posted on this rail and not yet completed — the quantity the window
    /// bounds, and the one to watch when sweeping it.
    pub writes_in_flight: u64,
    /// Cumulative WRITEs this rail has completed.
    pub writes_total: u64,
    /// Cumulative payload bytes this rail has WRITTEN. Compare across rails to see
    /// spread; compare against the EFA hardware `tx_bytes` to see overhead.
    pub write_bytes_total: u64,
    /// Requester-side arena ranges leased on this rail (ADR-0024).
    pub ranges_in_use: usize,
    /// HOLDER-side arena ranges leased on this rail: the staging pool a WRITE copies
    /// into when its source is not already registered (no ADR-0028 slab frame).
    ///
    /// Separate from [`Self::ranges_in_use`] because they are different pools with
    /// different sizing rules, and because this is the one the *delivery* path draws
    /// from — a client-memory WRITE leases here, never from the requester arena. It
    /// is a supply that a 32-rail node divides [`HOLDER_ARENA_RANGES`] by, so
    /// `staging_in_use == staging_ranges` is the shape that says the source side is
    /// what a delivery is queueing on, and the difference between "the fabric is the
    /// wall" and "our staging pool is".
    pub staging_in_use: usize,
    /// Ranges this rail's holder arena has in total — the denominator for
    /// [`Self::staging_in_use`], so a dashboard can plot occupancy without hard-coding
    /// the split (`HOLDER_ARENA_RANGES / rails`).
    pub staging_ranges: usize,
    /// Cumulative peer address handles evicted from this rail's cache.
    ///
    /// **Read this across rails, not as a total.** One error evicting one rail is the
    /// designed behaviour; the same count arriving on all 32 rails in the same scrape is the
    /// collapse `EvictionScope` exists to prevent, and only the per-rail split can tell those
    /// apart. A rail whose count climbs while its `writes_total` stays flat has lost RDMA to
    /// a peer and is waiting on the handshake sweep.
    pub ah_evictions: u64,
}

/// What the three bounded client-edge maps hold, and what they have shed (R4).
///
/// Three gauges and one three-way counter, because the two questions an operator has are
/// different: *how big is the client population* (per map, since the AH cache and the readiness
/// gate are per rail while the announce record is node-wide, and a skew between them is
/// diagnostic) and *is the shared cap big enough* (one number — the caps are one number, so a
/// per-map split of the eviction count would invite tuning three things that cannot be tuned
/// separately).
///
/// Read the eviction reasons against each other, not in isolation:
/// `cap` climbing is the **alarm** — the endpoint set outgrew
/// [`crate::client_registry::DEFAULT_MAX_CLIENTS`], so an endpoint still being written to can
/// lose its record mid-request; `ttl` climbing is the mechanism working, i.e. clients coming and
/// going; `explicit` climbing tracks `UNKNOWN_PEER` evidence and should move with
/// `pacer_delivery_unknown_peer_retries_total`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientEdgeStats {
    /// Client address handles held, summed over rails (`ClientAhCache`).
    pub client_ah_entries: usize,
    /// Endpoints whose first contact is being tracked, summed over rails (`ClientReady`).
    pub client_ready_entries: usize,
    /// Endpoints this node has announced itself to ([`Announcer`]) — node-wide, not per rail.
    pub announced_entries: usize,
    /// Cumulative capacity evictions across all three maps.
    pub evicted_cap: u64,
    /// Cumulative idle-TTL evictions across all three maps.
    pub evicted_ttl: u64,
    /// Cumulative removals by name across all three maps.
    pub evicted_explicit: u64,
}

impl ClientEdgeStats {
    /// Add one registry's eviction counts into this total.
    fn fold_evictions(&mut self, stats: crate::client_registry::ClientRegistryStats) {
        self.evicted_cap += stats.evicted_cap;
        self.evicted_ttl += stats.evicted_ttl;
        self.evicted_explicit += stats.evicted_explicit;
    }

    /// The cumulative count for one reason, so a caller can drive a labelled counter from
    /// [`crate::client_registry::EvictionReason::all`] rather than naming three fields.
    #[must_use]
    pub fn evicted(&self, reason: crate::client_registry::EvictionReason) -> u64 {
        use crate::client_registry::EvictionReason;
        match reason {
            EvictionReason::Cap => self.evicted_cap,
            EvictionReason::Ttl => self.evicted_ttl,
            EvictionReason::Explicit => self.evicted_explicit,
        }
    }
}

/// One EFA rail: a device's context plus everything protection-domain-scoped
/// — both buffer arenas (MRs bind to the rail's PD) and the per-peer AH cache
/// (AHs are created from the rail's PD and address the PEER'S same-index
/// rail; see [`EfaRdmaTransport::rails`] for the pairing rule).
struct Rail {
    ctx: Arc<EfaContext>,
    /// Requester-side: leased as the WRITE's destination, offered to peers
    /// with this rail's index pinned in the `RdmaBuffer`.
    requester_arena: RailBuffers,
    /// Holder-side: leased to stage a cached body before WRITEing out of it
    /// (RDMA needs a registered local source; cached `Bytes` isn't one).
    holder_arena: RailBuffers,
    /// AHs for each peer's same-index rail endpoint.
    ah_cache: AhCache,
    /// Bounds WRITEs posted-but-not-completed ON THIS RAIL, when the operator
    /// sets one (`PACER_RDMA_RAIL_WINDOW`); `None` = unbounded, the pre-D5.1
    /// behaviour and still the default.
    ///
    /// The last premise the transport-only bench has that the daemon lacked: it
    /// keeps a fixed in-flight window per rail and swept it as a first-class
    /// variable, finding the *shape* matters (planning/18: window 1 beat window 64
    /// at 32 rails into host memory, while H2's HBM run reached line rate at 64).
    /// Unbounded, a rail's depth is whatever `HOLDER_SERVE_SLOTS` × the
    /// requester's round-robin happens to deliver — an emergent number nobody
    /// chose, which is exactly the class of undeclared cap track D exists to
    /// remove. A `Semaphore` because exhaustion must be backpressure (the serve
    /// waits), never an error that pushes a fetch onto the gRPC fallback.
    write_window: Option<Arc<Semaphore>>,
    /// Address handles for **client** endpoints on this rail's PD (ADR-0030), bounded by
    /// capacity and idle TTL (`crate::client_registry`).
    ///
    /// Separate from `ah_cache` because a delivery client is not a ring member: it never
    /// handshakes, has no `node_id`, and is identified by the endpoint in its own token.
    /// Same PD-scoping reason as every other AH — hence per rail, not per transport.
    client_ah: client_write::ClientAhCache,
    /// Which client endpoints on this rail have already accepted a WRITE (ADR-0030).
    ///
    /// The gate that keeps the un-installed-writer race off the per-chunk path: exactly one
    /// chunk per endpoint pays the retry ladder, and every other chunk to that endpoint waits
    /// for its verdict instead of discovering the same not-ready window on its own. Without it,
    /// a checkpoint's first batch had thousands of chunks each burning their own ladder — and
    /// each retry re-stages a 16 MiB body, so the waste was in copies, not just in waiting.
    client_ready: client_write::ClientReady,
    /// WRITEs posted on this rail and not yet completed — [`RailStats`].
    writes_in_flight: AtomicU64,
    /// Cumulative completed WRITEs on this rail.
    writes_total: AtomicU64,
    /// Cumulative payload bytes WRITTEN on this rail.
    write_bytes_total: AtomicU64,
    /// Cumulative peer AHs evicted from THIS rail's cache — [`RailStats`].
    ///
    /// Per rail rather than per transport because that is the granularity the decision now
    /// has (see [`EvictionScope`]), and an aggregate could not show the failure this counter
    /// exists to make visible: 32 rails losing their AH at the same instant, from one error.
    ah_evictions: AtomicU64,
}

/// How much of the AH cache one RDMA error justifies throwing away.
///
/// **This type is the fix for a measured collapse.** Every transport error used to evict the
/// peer's address handle on *every* rail. One requester-side fetch error therefore emptied a
/// 32-rail cache: the RDMA-served fraction fell to **0.313**, the requester logged `no usable
/// RDMA rail for peer (not negotiated)` **2675 times in one 30-second bucket**, and the holder
/// was blameless throughout (0 warnings, `cq_errors_total 0`, `peer_fallbacks_total 0`) — the
/// cache stayed empty until a 30 s handshake sweep relearned it. planning/19 § Track D item 1
/// names the fix: distinguish "peer is gone" from "this serve was slow".
///
/// The distinction is *evidence*, not severity. An AH is stale only if the endpoint it
/// addresses is gone, and almost nothing that goes wrong on a WRITE is evidence of that.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EvictionScope {
    /// Nothing is stale. A protocol answer, not a transport fault (ADR-0003).
    None,
    /// Only the rail the operation used. A slow completion, a congested rail or a dead pump
    /// says something about **that queue pair**, and nothing whatever about the other 31 —
    /// so at most one rail's worth of capacity is given up, and the handshake sweep restores
    /// it while every other rail keeps carrying RDMA.
    OneRail,
    /// Every rail. Reserved for evidence that the *peer* is gone rather than slow: it could
    /// not be dialed at all, or its NIC answered that it holds no queue pair for us. A
    /// restart gives a peer new SRD queue pairs on every rail at once, which is the case A2
    /// was written for and the only one that justifies this.
    AllRails,
}

/// Which AHs a requester-side fetch error justifies evicting.
///
/// A free function over the error *variant* so the rule is testable with no fabric, no peer
/// and no ibverbs — which matters because the behaviour it encodes was previously only
/// observable by running a 32-rail node and reading a Prometheus counter afterwards.
fn requester_eviction_scope(err: &TransportError) -> EvictionScope {
    match err {
        // Protocol answers: the peer replied, so its endpoint is demonstrably fine.
        TransportError::NotCached | TransportError::RangeNotSatisfiable => EvictionScope::None,
        // The peer could not be dialed or the RPC died in transit. Its gRPC server is not
        // answering, which is what a restarting pod looks like — and a restart invalidates
        // every rail's endpoint together.
        TransportError::PeerUnavailable(_) => EvictionScope::AllRails,
        // Everything else: a completion that timed out, a decode failure, a holder-side
        // error. The fetch reached the peer, so the peer exists; at most the rail this
        // attempt used is suspect. **This is the variant that used to evict all 32.**
        TransportError::Other(_) => EvictionScope::OneRail,
    }
}

/// Which AHs a holder-side WRITE completion failure justifies evicting.
///
/// Structural, on the typed completion status the pump preserves — never a string match, for
/// the same reason `client_write::is_unknown_peer` is not one.
///
/// `UNKNOWN_PEER` is the one status that *is* evidence about the peer: the requester's NIC
/// says it holds no queue pair for us, which after a pod restart is true on every rail. A
/// timeout is the opposite kind of news — the WRITE may still be in flight, and
/// `write_sources_orphaned_total` exists precisely because it often is — so it evicts at most
/// the rail it was posted on.
fn holder_eviction_scope(err: &anyhow::Error) -> EvictionScope {
    if client_write::is_unknown_peer(err) {
        EvictionScope::AllRails
    } else {
        EvictionScope::OneRail
    }
}

impl Rail {
    /// Whether this rail can carry RDMA to `peer` right now: its completion
    /// pump is alive (A1) and a handshake taught it the peer's same-index
    /// rail endpoint.
    async fn usable_for(&self, peer: &str) -> bool {
        self.ctx.rdma_healthy.load(Ordering::Relaxed) && self.ah_cache.has(peer).await
    }

    /// Wait for a slot in this rail's in-flight window, if it has one. The
    /// returned permit is held for post + completion and released on drop, so the
    /// window bounds exactly "WRITEs on the wire", not "serves admitted".
    async fn enter_window(&self) -> Option<OwnedSemaphorePermit> {
        match &self.write_window {
            Some(sem) => Some(
                Arc::clone(sem)
                    .acquire_owned()
                    .await
                    .expect("a rail's write window is never closed"),
            ),
            None => None,
        }
    }

    /// This rail's live counters (see [`RailStats`]).
    fn stats(&self, rail: usize) -> RailStats {
        RailStats {
            rail,
            numa_node: self.ctx.placement().numa_node,
            writes_in_flight: self.writes_in_flight.load(Ordering::Relaxed),
            writes_total: self.writes_total.load(Ordering::Relaxed),
            write_bytes_total: self.write_bytes_total.load(Ordering::Relaxed),
            ranges_in_use: self.requester_arena.slots_in_use(),
            staging_in_use: self.holder_arena.slots_in_use(),
            staging_ranges: self.holder_arena.ranges(),
            ah_evictions: self.ah_evictions.load(Ordering::Relaxed),
        }
    }
}

/// What the operator's RDMA-buffer knobs resolve to, as the transport needs
/// them (ADR-0024): a node-wide requester budget in bytes, the cache
/// `chunk_size` every range is cut to, and the page size to map with. Grouped
/// into a struct rather than three positional arguments because all three are
/// `usize`-ish and a transposed pair would be a silently mis-sized arena.
#[derive(Clone, Copy, Debug)]
pub struct ArenaConfig {
    /// Requester-side registered bytes for the whole node, split across rails
    /// — `PACER_RDMA_ARENA_BYTES`, defaulting to
    /// [`DEFAULT_REQUESTER_ARENA_BYTES`]. The holder side is not a knob: it is
    /// [`HOLDER_ARENA_RANGES`] ranges, for the reason documented there.
    pub requester_bytes: usize,
    /// The cache chunk size (ADR-0015, `PACER_CHUNK_SIZE`): a range holds
    /// exactly one chunk, so this is what makes the arena's ranges the right
    /// size instead of the old fixed 64 MiB slot.
    pub chunk_size: usize,
    /// Pages to map both arenas with. Base pages need no node reservation; an
    /// explicit size that cannot be mapped degrades to base pages with a
    /// warning (see [`HostArena::new`]).
    pub pages: ArenaPages,
    /// Bytes to map as the ADR-0028 cache slab — the cache's RAM tier, made
    /// RDMA-postable in place. `0` disables it and holders stage their WRITEs as
    /// before, which is the default until the serve path has been measured on
    /// hardware.
    ///
    /// This is a HARD reservation, not a budget: it is mapped and pinned at
    /// startup, once per rail, so it must be covered by the pod's memory limit
    /// and (for hugepages) the node's reservation. ADR-0028's gate measured the
    /// cost — ~8-14 s for 96 GiB × 32 rails on hugepages.
    pub slab_bytes: usize,
    /// Extra bytes per slab frame, beyond one chunk.
    ///
    /// A frame holds whatever the cache reads in one I/O, and **today that is exactly one
    /// chunk, so the daemon passes `0`.** It was briefly `SLOT_HEADER_BYTES`: ADR-0033's store
    /// read a slot's 4 KiB header and its body together, which removed a dependent round trip
    /// per chunk and measured at +2.7 % — noise — while forcing a stride that misaligned every
    /// body against the RAID0 stripe. The headers now live in a per-extent region and the body
    /// read is a whole chunk again (`bench/ladder/results/c5-dcp-store-single-read.md`).
    ///
    /// The knob stays because the question recurs — a format that reads anything alongside a
    /// body needs it — and because it is the seam that keeps this crate from having to know
    /// about `pacer-cache`'s on-disk layout: the header size is that crate's contract
    /// (`slot::SLOT_HEADER_BYTES`), and this one must not depend on it.
    pub slab_frame_headroom: usize,
    /// WRITEs a single rail may have on the wire at once (`PACER_RDMA_RAIL_WINDOW`);
    /// `0` = unbounded, the default and the pre-D5.1 behaviour.
    ///
    /// Lives here rather than as its own argument because it is the same kind of
    /// operator-set transport geometry as the rest of this struct, and because a
    /// window is only meaningful alongside the buffer supply it draws from — see
    /// `Rail::write_window` (in this module) for why the knob exists at all.
    pub rail_window: usize,
}

impl EfaRdmaTransport {
    /// Build the transport around an already-brought-up [`EfaContext`]
    /// (construction is fallible and belongs in the daemon's startup path,
    /// gated on [`EfaContext::probe`] — this constructor never fails).
    ///
    /// # Panics
    ///
    /// If `ctxs` is empty, or if either [`HostArena::new`] call fails to map or
    /// register its arena (out of `RLIMIT_MEMLOCK`, or a pod memory limit that
    /// does not cover the pinned bytes) — a startup-time condition the daemon
    /// treats as fatal for the RDMA plane rather than silently degrading, per
    /// ADR-0018's "a permissive bundle...is a startup failure, not a slow path"
    /// precedent and ADR-0024's refusal to fall back to a smaller arena.
    pub fn new(
        ctxs: Vec<Arc<EfaContext>>,
        grpc: GrpcTransport,
        local_node: impl Into<String>,
        arena_cfg: ArenaConfig,
    ) -> Self {
        assert!(
            !ctxs.is_empty(),
            "EfaRdmaTransport requires at least one rail"
        );
        let (rails, cache_slab) = build_rails(ctxs, arena_cfg);
        Self {
            rails: Arc::new(rails),
            cache_slab,
            next_rail: AtomicU64::new(0),
            next_client_rail: AtomicU64::new(0),
            unknown_peer_retries: AtomicU64::new(0),
            unknown_peer_declines: AtomicU64::new(0),
            announcer: tokio::sync::OnceCell::new(),
            grpc: Arc::new(grpc),
            local_node: local_node.into(),
            holder_copy_nanos: AtomicU64::new(0),
            requester_copy_nanos: AtomicU64::new(0),
            post_batch_nanos: AtomicU64::new(0),
            write_completion_wait_nanos: AtomicU64::new(0),
        }
    }

    /// ADR-0028's cache slab, if one was built. The daemon's cache-fill path
    /// takes this to store chunks *in* registered memory; a `None` here (no
    /// `slab_bytes`, or a budget too small to be useful) means the daemon should
    /// keep allocating cached chunks on the heap.
    #[must_use]
    pub fn cache_slab(&self) -> Option<&CacheSlab> {
        self.cache_slab.as_ref()
    }

    /// Cumulative nanoseconds spent in the holder-side stage copy so far
    /// (see [`Self::holder_copy_nanos`]). Read by the daemon's metrics layer
    /// at scrape to expose `pacer_rdma_holder_copy_seconds_total`.
    #[must_use]
    pub fn holder_copy_nanos(&self) -> u64 {
        self.holder_copy_nanos.load(Ordering::Relaxed)
    }

    /// Cumulative nanoseconds spent in the requester-side copy-out so far
    /// (see [`Self::requester_copy_nanos`]). Read by the daemon's metrics
    /// layer at scrape to expose `pacer_rdma_requester_copy_seconds_total`.
    #[must_use]
    pub fn requester_copy_nanos(&self) -> u64 {
        self.requester_copy_nanos.load(Ordering::Relaxed)
    }

    /// Cumulative nanoseconds spent building+submitting the holder's WRITE
    /// send batch so far (see [`Self::post_batch_nanos`] the field). CPU-bound
    /// serve-path work. Read by the daemon's metrics layer at scrape to expose
    /// `pacer_rdma_post_batch_seconds_total`.
    #[must_use]
    pub fn post_batch_nanos(&self) -> u64 {
        self.post_batch_nanos.load(Ordering::Relaxed)
    }

    /// Cumulative nanoseconds the holder spent awaiting WRITE completions so
    /// far (see [`Self::write_completion_wait_nanos`] the field). WALL-CLOCK
    /// scheduler-wait time, NOT CPU — read the field doc before using it. Read
    /// by the daemon's metrics layer at scrape to expose
    /// `pacer_rdma_write_completion_wait_seconds_total`.
    #[must_use]
    pub fn write_completion_wait_nanos(&self) -> u64 {
        self.write_completion_wait_nanos.load(Ordering::Relaxed)
    }

    /// Requester-side arena ranges currently leased out (summed over rails). On
    /// the zero-copy path a range stays leased for as long as the served `Bytes`
    /// is alive (until the S3 client drains it), so this is the operator's view
    /// of arena pressure / range-starvation risk (planning/16 §5). Pinned at
    /// capacity now means the arena is genuinely too small for the offered load
    /// — raise `PACER_RDMA_ARENA_BYTES` — rather than the 64-slot artifact
    /// ADR-0024 removed. Read by the daemon's metrics layer at scrape to expose
    /// `pacer_rdma_requester_slots_in_use` (the gauge name is kept: it is an
    /// external contract, and a range IS the leasable unit it always counted).
    #[must_use]
    pub fn requester_slots_in_use(&self) -> usize {
        self.rails
            .iter()
            .map(|r| r.requester_arena.slots_in_use())
            .sum()
    }

    /// Rails brought up at startup (devices found, capped by the operator's
    /// `efa_rails` knob). Surfaced by the daemon at scrape as
    /// `pacer_rdma_rails`.
    #[must_use]
    pub fn rail_count(&self) -> usize {
        self.rails.len()
    }

    /// WRITE source buffers the completion pumps have taken ownership of because
    /// their serve gave up first: `(cumulative, still held)`, summed over rails.
    ///
    /// **The pair is the safety item's only external evidence** (ADR-0028 § "a
    /// completion that times out must not free the frame"). Before this
    /// mechanism a timed-out serve returned its cache frame to the free list
    /// while the NIC might still be DMA-reading it — silent corruption with no
    /// counter to see it by. Now: the cumulative count says how often a serve
    /// blew `context::COMPLETION_TIMEOUT` (0 in steady state; a rising rate is a
    /// rail-latency problem worth chasing on its own — that deadline is only
    /// OURS, never proof the transfer ended), and the held count says
    /// how many buffers are pinned awaiting a completion that has not arrived. A
    /// held count that grows without bound means completions have stopped
    /// arriving entirely, which `cq_error_count`/`rdma_healthy` describe better;
    /// it cannot itself exhaust memory, being bounded by posted-but-unreaped
    /// WRITEs (serve admission × `PACER_RDMA_RAIL_WINDOW`).
    #[must_use]
    pub fn orphaned_write_sources(&self) -> (u64, usize) {
        self.rails
            .iter()
            .map(|r| r.ctx.pump().orphan_counts())
            .fold((0, 0), |(t, h), (rt, rh)| (t + rt, h + rh))
    }

    /// `(retries, declines)` for WRITEs whose target had not installed this writer — see the
    /// fields' own note on why both are worth a series.
    #[must_use]
    pub fn client_write_unknown_peer(&self) -> (u64, u64) {
        (
            self.unknown_peer_retries.load(Ordering::Relaxed),
            self.unknown_peer_declines.load(Ordering::Relaxed),
        )
    }

    /// What the three bounded client-edge maps hold and have shed (see [`ClientEdgeStats`]).
    ///
    /// Read by the daemon's metrics layer at scrape. Lock-free: every registry keeps its counters
    /// in an `Arc` outside its own mutex precisely so a scrape can never queue behind a delivery
    /// (`crate::client_registry::ClientRegistryCounters`).
    #[must_use]
    pub fn client_edge_stats(&self) -> ClientEdgeStats {
        let mut stats = ClientEdgeStats::default();
        for rail in self.rails.iter() {
            let handles = rail.client_ah.stats();
            stats.client_ah_entries += handles.entries;
            stats.fold_evictions(handles);
            let ready = rail.client_ready.stats();
            stats.client_ready_entries += ready.entries;
            stats.fold_evictions(ready);
        }
        if let Some(announcer) = self.announcer.get() {
            let announced = announcer.stats();
            stats.announced_entries = announced.entries;
            stats.fold_evictions(announced);
        }
        stats
    }

    /// Drop every client-edge record that has gone unaddressed for longer than the idle TTL,
    /// returning how many went.
    ///
    /// The registries also sweep themselves whenever a new endpoint is inserted, which is when
    /// room is actually needed — so this exists for the other case: a node whose clients have all
    /// gone home inserts nothing, and its last records stay resident (bounded, but resident) until
    /// the next client arrives. Called by the daemon's EFA maintenance sweep
    /// (`drive_efa_maintenance`, once per `HANDSHAKE_SWEEP_INTERVAL`), so a departed client's
    /// records outlive their TTL by at most one interval.
    pub async fn sweep_expired_clients(&self) -> usize {
        let mut gone = 0;
        for rail in self.rails.iter() {
            gone += rail.client_ah.sweep_expired().await;
            gone += rail.client_ready.sweep_expired();
        }
        if let Some(announcer) = self.announcer.get() {
            gone += announcer.sweep_expired();
        }
        gone
    }

    /// Per-rail live counters, in rail order (see [`RailStats`]). Read by the
    /// daemon's metrics layer at scrape to expose the `pacer_rdma_rail_*` family.
    ///
    /// Without this, per-rail behaviour is only visible through EFA *hardware*
    /// counters — which is how D4 had to prove rail spread, and which cannot show
    /// queueing. Reading these across rails answers three questions the aggregate
    /// hides: is one rail carrying a disproportionate share, is a rail's in-flight
    /// depth pinned at its window, and do the daemon's byte totals agree with the
    /// NIC's.
    #[must_use]
    pub fn rail_stats(&self) -> Vec<RailStats> {
        self.rails
            .iter()
            .enumerate()
            .map(|(i, rail)| rail.stats(i))
            .collect()
    }

    /// Cumulative completion-pump deaths observed so far (A1): 0 in steady
    /// state, becomes ≥ 1 if the CQ→tokio drain loop ever exits on a terminal
    /// CQ/fd error — at which point RDMA capability is flipped off and every
    /// fetch/serve uses gRPC directly (see `completion.rs`'s `drain_loop`). Read
    /// by the daemon's metrics layer at scrape to expose
    /// `pacer_rdma_cq_errors_total`. Reads through [`EfaContext`] because the
    /// pump (which owns the counter) is built before this transport.
    #[must_use]
    pub fn cq_error_count(&self) -> u64 {
        self.rails
            .iter()
            .map(|r| r.ctx.cq_errors.load(Ordering::Relaxed))
            .sum()
    }

    /// Cumulative **failed** work completions no waiter received, summed over rails.
    ///
    /// The pump's blind spot, made countable: when a queue pair enters the error state every
    /// work request still posted on it completes `WorkRequestFlushed`, and those flushes are
    /// reported to their waiters — but the single non-flush completion that *transitioned*
    /// the queue pair is discarded without trace if its own waiter had already gone. A
    /// non-zero value here beside a spike of
    /// `pacer_delivery_write_failures_total{reason="work_request_flushed"}` says the cause is
    /// in the daemon's log rather than in any series; a zero one says the transition produced
    /// no completion this process can see, which is a stronger and more useful finding.
    ///
    /// Distinct from `pacer_rdma_cq_errors_total`, which counts the pump *dying*. The pump is
    /// alive and working in this case — it delivered the flushes.
    #[must_use]
    pub fn unobserved_completion_failures(&self) -> u64 {
        self.rails
            .iter()
            .map(|r| r.ctx.pump().unobserved_failure_count())
            .sum()
    }

    /// This node's rail-0 SRD endpoint — the single-rail compatibility view;
    /// the handshake advertises every rail via [`Self::endpoint_proto`].
    pub fn local_endpoint(&self) -> QueuePairEndpoint {
        self.rails[0].ctx.local_endpoint
    }

    /// Record a peer's endpoint from its handshake, inserting the AH now
    /// rather than lazily — ADR-0018 finding 6 requires the AH to exist
    /// *before* either side's first WRITE, and the handshake is the only
    /// point both sides are guaranteed to exchange endpoints before any
    /// fetch happens. Takes the wire-format `EfaEndpoint` proto message
    /// directly (rather than the decoded `ibverbs::QueuePairEndpoint`) so
    /// callers outside this crate — the daemon's `peer.rs`, on both the
    /// server (`Handshake` RPC) and client (`initiate_handshake`) sides —
    /// never need `ibverbs` as a direct dependency.
    ///
    /// # Errors
    ///
    /// The endpoint bytes being malformed (wrong length or an unrecognized
    /// flags byte — see [`QueuePairEndpoint::from_bytes`]), or
    /// [`AhCache::get_or_insert`]'s errors (a malformed GID, or the peer
    /// being EFA-unreachable — different AZ, no placement group). The
    /// caller should log and continue in gRPC-only mode for this peer
    /// rather than fail the handshake over it.
    pub async fn learn_peer(
        &self,
        peer_node_id: &str,
        endpoint: &pacer_proto::v1::EfaEndpoint,
    ) -> anyhow::Result<()> {
        learn_peer_rails(&self.rails, peer_node_id, endpoint).await
    }

    /// Client-initiated counterpart to [`Self::learn_peer`]/the daemon's
    /// server-side `learn_efa_peer` (`pacer_daemon::peer`): dial `peer`,
    /// carry this node's own endpoint + capabilities on the handshake, and
    /// learn whatever endpoint comes back. Called periodically for every
    /// ring member (see the daemon's membership-driven handshake loop) so
    /// both directions of ADR-0019's bidirectional exchange happen without
    /// waiting for a peer to happen to call us first.
    ///
    /// A peer that never responds with an `efa_endpoint` (non-EFA, or its
    /// own probe failed) is simply not learned — subsequent fetches to it
    /// fall through to gRPC via [`AhCache::has`] returning false, with no
    /// special-casing needed here.
    ///
    /// # Errors
    ///
    /// [`TransportError`] if the peer cannot be dialed or the RPC fails; a
    /// malformed returned endpoint, or the AH not being insertable, if it
    /// responded (see `handshake_and_learn`). Both are non-fatal for the
    /// caller — log and retry next cycle.
    pub async fn initiate_handshake(&self, peer: &NodeId) -> anyhow::Result<()> {
        handshake_and_learn(&self.grpc, &self.rails, self.endpoint_proto(), peer).await
    }

    /// Every local rail's SRD endpoint, wire-encoded as the proto message
    /// [`Self::learn_peer`]/handshakes carry. Field 1 stays the rail-0
    /// endpoint so single-rail peers predating A5 interoperate; multi-rail
    /// peers read `rail_endpoints` (which includes rail 0 again, in order).
    pub fn endpoint_proto(&self) -> pacer_proto::v1::EfaEndpoint {
        pacer_proto::v1::EfaEndpoint {
            queue_pair_endpoint: self.local_endpoint().to_bytes().to_vec(),
            rail_endpoints: self
                .rails
                .iter()
                .map(|r| r.ctx.local_endpoint.to_bytes().to_vec())
                .collect(),
        }
    }

    /// This node's own rails as [`crate::announce::AnnouncedRail`] entries — the endpoints a
    /// client must hold address handles for before **this** node writes into its window.
    ///
    /// Exactly what [`Announcer`] puts on the wire (both go through one `announced_rails`
    /// helper), which is the point: a client primed from these addresses and a client taught
    /// by an announce install handles for the same GIDs by construction. Read-only — it posts
    /// nothing and builds nothing — so it is safe on a control-plane query path.
    ///
    /// # Errors
    ///
    /// A rail whose endpoint carries no GID, which EFA always provides; that is a bring-up
    /// failure, and it is surfaced rather than silently yielding a short list, because a
    /// pre-flight that omits a rail primes a client for fewer writers than will arrive.
    pub fn local_announced_rails(&self) -> anyhow::Result<Vec<crate::announce::AnnouncedRail>> {
        let contexts: Vec<&EfaContext> = self.rails.iter().map(|r| r.ctx.as_ref()).collect();
        announce::announced_rails(&contexts)
    }

    /// The rails of `peer_node_id` that the capability handshake has already negotiated, as
    /// [`crate::announce::AnnouncedRail`] entries.
    ///
    /// Sourced from the per-rail [`AhCache`] rather than from a fresh handshake, for two
    /// reasons. It is the **only** place a peer's per-rail endpoint is held on this node
    /// (ADR-0019's exchange populates it and nothing else does), so reading it cannot
    /// disagree with what a holder would actually write from; and it makes this query
    /// non-blocking — no RPC, so a pre-flight stays off the network exactly as the read
    /// path's own source selection does.
    ///
    /// An empty result means "not negotiated yet", which is a *legitimate* answer and not an
    /// error: the handshake sweep is periodic (30 s), a peer may be gRPC-only, and a client
    /// that is not primed for that holder is exactly the case announce still covers. The rail
    /// index carried on each entry is the **peer's** rail: the by-index pairing rule means
    /// local rail *i* holds peer rail *i*'s endpoint.
    pub async fn peer_announced_rails(
        &self,
        peer_node_id: &str,
    ) -> Vec<crate::announce::AnnouncedRail> {
        let mut rails = Vec::with_capacity(self.rails.len());
        for (index, rail) in self.rails.iter().enumerate() {
            let Some(endpoint) = rail.ah_cache.endpoint_of(peer_node_id).await else {
                continue;
            };
            // No GID means nothing addressable; `AhCache::get_or_insert` would have refused
            // the insert, so this is unreachable in practice and skipping is the safe answer.
            let Some(gid) = endpoint.gid else { continue };
            rails.push(crate::announce::AnnouncedRail {
                gid: <[u8; 16]>::from(gid),
                qpn: endpoint.qp_num,
                // Rail count is bounded by the device count, far below u16::MAX.
                rail: index as u16,
            });
        }
        rails
    }

    /// Holder side: stage `body` into a leased range of this rail's holder
    /// arena and WRITE it into the requester's offered buffer, called from the
    /// daemon's `FetchBlob` handler (`pacer_daemon::peer::PacerPeer` — that
    /// crate depends on this one, so the link cannot go the other way) whenever
    /// a request carries `rdma_buffer`. Returns `Ok(true)` (WRITE landed, set
    /// `BlobMeta.served_via_rdma`) or `Ok(false)` (a benign reason to fall
    /// back to streaming this response instead — no cached AH for the
    /// requester, or the body doesn't fit the offered range). An `Err` means
    /// the WRITE was attempted and failed on the wire; the caller should
    /// still fall back to streaming (ADR-0003: any error means fall back),
    /// just log it as a real fault rather than an expected miss.
    ///
    /// # Errors
    ///
    /// The one-sided WRITE completing with a failed status (see
    /// `write::await_write_completion`).
    pub async fn serve_via_write(
        &self,
        requester_node_id: &str,
        buffer: &RdmaBuffer,
        body: &Bytes,
    ) -> anyhow::Result<bool> {
        // The requester pinned the rail when it leased the buffer: its rkey is
        // only valid at the PD of that rail, and the WRITE must be addressed
        // to that rail's endpoint (posted from OUR same-index rail — the
        // pairing rule). A rail index we don't have (requester has more rails,
        // or a stale peer) is a benign fallback, not an error.
        let Some(rail) = self.rails.get(buffer.rail as usize) else {
            return Ok(false);
        };
        if !rail.ctx.rdma_healthy.load(Ordering::Relaxed) {
            // This rail's completion pump has died (A1) — a posted WRITE's
            // completion would never be reaped, so awaiting it would burn a
            // full COMPLETION_TIMEOUT before failing. Skip RDMA and let the
            // caller stream this response instead (a benign fallback,
            // `Ok(false)`), exactly as the no-cached-AH guard below does.
            return Ok(false);
        }
        // Capacity, checked on BOTH ends. The requester's offered range is the
        // obvious one; our own staging range matters too now that a range is
        // `chunk_size` rather than a fixed 64 MiB slot (ADR-0024) — the stage
        // copy below writes `body.len()` bytes into it, so a body that does not
        // fit would panic in a serve handler instead of falling back. It cannot
        // happen in a consistent cluster (a chunk key encodes the `chunk_size`
        // it was cut at, so a holder only serves bodies bounded by the same
        // chunk size the requester leased for), which is exactly why this reads
        // as a benign fallback rather than an error: it fires only if the two
        // ends' geometry has already disagreed.
        if body.len() as u64 > buffer.len || body.len() > rail.holder_arena.slot_bytes() {
            return Ok(false);
        }
        let Ok(ah_ref) = rail
            .ah_cache
            .get_or_insert_cached_only(requester_node_id)
            .await
        else {
            // No AH on file for this requester: either it never handshook
            // with RDMA, or this holder hasn't learned it yet. Either way,
            // posting a WRITE without a return-path AH would fail with
            // UNKNOWN_PEER (ADR-0018 finding 6) — a benign fallback, not an
            // error worth propagating.
            return Ok(false);
        };
        let (ah, peer_qp_num) = ah_ref.handle_and_qp_num();
        // Enter this rail's in-flight window BEFORE staging: a serve that will
        // queue on the wire should not first pin a staging range and hold a cached
        // body resident while it waits (the same ordering reason the daemon's
        // serve-admission gate sits before its cache read — planning/15 B4).
        // `None` when no window is configured, which is the default.
        let _window = rail.enter_window().await;
        // ADR-0028: if the cached body already lives in the slab, it is already
        // registered on THIS rail and the WRITE sources it in place — no staging
        // range, no memcpy. Either way the SGE's backing memory must outlive the
        // NIC's reads of it (the SGE is a plain `ibv_sge` and borrows nothing),
        // so `source` owns whichever it is and is handed to
        // `await_write_completion` to release at the one moment that is provably
        // safe. For an in-place serve that owner is a `Bytes` clone — a refcount
        // bump that keeps the frame out of the slab's free list even if the
        // caller's own handle goes away.
        let source: write::SourceGuard;
        let local: LocalMemorySlice = match self
            .cache_slab
            .as_ref()
            .and_then(|slab| slab.local_slice(buffer.rail as usize, body))
        {
            Some(in_place) => {
                source = Box::new(body.clone());
                in_place
            }
            None => {
                let mut lease = rail.holder_arena.lease().await;
                let copy_start = Instant::now();
                lease.with_bytes_mut(|dst| dst[..body.len()].copy_from_slice(body));
                self.holder_copy_nanos
                    .fetch_add(copy_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
                let slice = lease.local_slice(0..body.len());
                source = Box::new(lease);
                slice
            }
        };
        let remote = ibverbs::RemoteMemorySlice {
            addr: buffer.addr,
            len: buffer.len as usize,
            rkey: buffer.rkey,
        };
        // Post while `ah_ref` (the AhCache lock guard) is held — the AH must
        // stay alive for the synchronous post — but drop it immediately
        // after: awaiting the WRITE's wire round-trip while still holding
        // the cache's lock would serialize every concurrent holder WRITE in
        // the process behind it, one lock for however many peers are being
        // served at once. Invisible when a holder only ever serves one
        // requester at a time (rungs 1-2 of the saturation ladder,
        // planning/15), but a real ~2x throughput loss once >1 requester
        // hits the same holder concurrently (rung 3) — found on-hardware.
        let posted =
            write::post_write(&rail.ctx, ah, peer_qp_num, context::QKEY, local, remote).await;
        // Invariant R7 — dropping the AH lease mid-flight is safe (1cf61f4).
        // What `drop(ah_ref)` actually releases is the AhCache *mutex guard*,
        // NOT the `AddressHandle`: the handle stays owned by its `CachedPeer`
        // entry in the cache, so the WRITE just posted still has a live AH.
        // The handle is only destroyed (its `ibv_destroy_ah` on `Drop`) when
        // that entry is replaced (`get_or_insert` on a changed endpoint) or
        // evicted (A2) — both of which must take this same mutex, so NEITHER
        // can run during the synchronous post above (which holds the guard).
        // After the guard is released, an evict/rebuild *could* destroy the
        // handle while this WRITE is still in flight on the wire; that is
        // still safe because `ibv_post_send` captured the address vector into
        // the WQE/hardware at submit time and the transmit path does not
        // dereference the user's AH object again. And even in the worst case
        // where a provider did, a torn-down AH mid-flight surfaces as a WRITE
        // *completion error* — reaped below, mapped to a gRPC fallback
        // (ADR-0003: any error means fall back) — never a misdirect or
        // corruption, so correctness holds regardless. Validated on-hardware
        // by the fan-in-7 holder-restart re-probe (planning/15).
        drop(ah_ref);
        let posted = posted?;
        self.post_batch_nanos
            .fetch_add(posted.post_batch_nanos(), Ordering::Relaxed);
        // From here until the completion is reaped this WRITE is ON THE WIRE, which
        // is exactly the quantity `RailStats::writes_in_flight` reports and the
        // window bounds. Incremented after a successful post (a failed post never
        // reached the wire) and decremented on every exit path below.
        rail.writes_in_flight.fetch_add(1, Ordering::Relaxed);
        // Wall-clock, not CPU: this await yields to the scheduler for the
        // wire round-trip + pump wakeup — see `write_completion_wait_nanos`.
        let wait_start = Instant::now();
        // `source` goes with it: the WRITE's local buffer is only safe to release
        // once the completion is reaped, and on the paths where no completion
        // arrives it must outlive this call entirely — ownership moves to the
        // completion pump (see `write::await_write_completion`). That is the
        // whole reason this is not a `drop(staged_lease)` after the await.
        let completion = write::await_write_completion(posted, source).await;
        self.write_completion_wait_nanos
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        rail.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
        if let Err(e) = completion {
            // A WRITE that completed `UNKNOWN_PEER` is the surest live sign the requester's
            // cached AH went stale — a pod restart gave it a new SRD `qp_num`, so this AH now
            // addresses a dead QP. Evict it (A2) so subsequent serves to this requester fall
            // back to gRPC cleanly instead of re-posting doomed WRITEs for up to the 30 s
            // handshake sweep. No proactive re-handshake from here: the serve path holds only
            // the requester's node *name*, not its dialable pod IP (`FetchBlobRequest`
            // carries no address, and the transport has no ring to resolve one), so it cannot
            // dial back — the ≤30 s sweep (`main.rs`) and the requester's own handshake
            // relearn the new endpoint.
            //
            // **But `UNKNOWN_PEER` is not the only way to get here.** A completion that timed
            // out is the commonest, and it is evidence of a SLOW serve rather than a departed
            // peer — the WRITE may well still be in flight, which is why
            // `write_sources_orphaned_total` exists. Evicting 32 rails for it is what emptied
            // the AH cache (planning/19 § Track D item 1), so the scope now follows the
            // completion status.
            let scope = holder_eviction_scope(&e);
            self.evict_stale_ah(requester_node_id, scope, buffer.rail as usize)
                .await;
            return Err(e);
        }
        // Only successful WRITEs count as bytes this rail moved — a failed one is
        // served over gRPC instead, and attributing its bytes here would make the
        // per-rail totals disagree with the EFA hardware counters they exist to be
        // compared against.
        rail.writes_total.fetch_add(1, Ordering::Relaxed);
        rail.write_bytes_total
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(true)
    }

    /// A2: drop the now-stale cached AH for `peer_node_id` after a WRITE/fetch error, over
    /// the rails that [`EvictionScope`] says the error is actually evidence about.
    ///
    /// Awaited (not spawned) so no concurrent fetch/serve re-attempts a doomed WRITE against
    /// a stale AH before it is gone; once removed, [`AhCache::has`] /
    /// [`AhCache::get_or_insert_cached_only`] report the peer as un-negotiated on that rail
    /// and RDMA is skipped cleanly there until a handshake rebuilds a fresh AH.
    ///
    /// **`OneRail` is the important case and it is the common one.** This used to evict every
    /// rail unconditionally, on the reasoning that the stale-AH *cause* is a peer restart. The
    /// reasoning is sound and the scope was not: a restart is one of several ways to get an
    /// error here, and for all the others — a completion that timed out, a congested rail, a
    /// rail's own pump dying — evicting 32 rails discards 31 working paths on the strength of
    /// evidence about one. That is what emptied a 32-rail cache from a single fetch error.
    async fn evict_stale_ah(&self, peer_node_id: &str, scope: EvictionScope, rail_idx: usize) {
        let rails: &[usize] = match scope {
            EvictionScope::None => return,
            EvictionScope::OneRail => &[rail_idx],
            // Borrowed from a scratch vec rather than iterating twice: `AllRails` is rare
            // (a peer restart), so allocating on it costs nothing worth avoiding.
            EvictionScope::AllRails => &(0..self.rails.len()).collect::<Vec<_>>(),
        };
        let mut evicted = 0_usize;
        for &i in rails {
            // A rail index from a caller that picked it out of `self.rails`, so this cannot
            // miss — but a panic on the serve path is not worth the assertion.
            if let Some(rail) = self.rails.get(i) {
                if rail.ah_cache.evict(peer_node_id).await {
                    rail.ah_evictions.fetch_add(1, Ordering::Relaxed);
                    evicted += 1;
                }
            }
        }
        if evicted > 0 {
            debug!(
                peer = peer_node_id,
                ?scope,
                evicted,
                rails = self.rails.len(),
                "evicted stale AHs after an RDMA error (A2)"
            );
        }
    }

    /// A2: kick off a proactive re-handshake with `peer` so RDMA re-engages
    /// without waiting out the 30 s handshake sweep (`main.rs`'s
    /// `drive_efa_handshakes`). Spawned, never awaited, so it cannot block the
    /// fetch path that triggered it. Dials `peer.addr()`, so `peer` must be a
    /// fully dialable [`NodeId`] — the requester's `fetch_blob` has one; the
    /// holder's `serve_via_write` does not, which is why only the requester
    /// side calls this. A failure just logs: the sweep is the backstop.
    fn spawn_rehandshake(&self, peer: NodeId) {
        let grpc = Arc::clone(&self.grpc);
        let rails = Arc::clone(&self.rails);
        let endpoint_proto = self.endpoint_proto();
        tokio::spawn(async move {
            if let Err(e) = handshake_and_learn(&grpc, &rails, endpoint_proto, &peer).await {
                debug!(peer = %peer.name(), error = %e, "proactive re-handshake failed; the 30s sweep will retry (A2)");
            }
        });
    }
}

#[async_trait::async_trait]
impl PeerTransport for EfaRdmaTransport {
    async fn fetch_blob(
        &self,
        peer: &NodeId,
        cache_key: &str,
        range: Option<ByteRange>,
        no_fill: bool,
    ) -> Result<BlobStream, TransportError> {
        match self
            .try_fetch_via_rdma(peer, cache_key, range, no_fill)
            .await
        {
            Ok(stream) => Ok(stream),
            // NotCached/RangeNotSatisfiable are protocol answers, not
            // failures — never retried on a different transport (ADR-0003:
            // only transport-layer errors fall back).
            Err(e @ (TransportError::NotCached | TransportError::RangeNotSatisfiable)) => Err(e),
            Err(e) => {
                warn!(peer = %peer.name(), key = cache_key, error = %e, "RDMA fetch failed; falling back to gRPC streaming");
                self.grpc.fetch_blob(peer, cache_key, range, no_fill).await
            }
        }
    }

    /// Control-plane only, always gRPC (ADR-0019) — identical to
    /// [`GrpcTransport::invalidate`], reused directly rather than duplicated.
    async fn invalidate(&self, peer: &NodeId, cache_key: &str) -> Result<(), TransportError> {
        self.grpc.invalidate(peer, cache_key).await
    }

    /// Directory control-plane RPCs (ADR-0017) — always gRPC (ADR-0019),
    /// identical to [`GrpcTransport`]'s, reused directly. RDMA replaces only
    /// the blob data path (`fetch_blob`), never the directory edges.
    async fn announce_admit(
        &self,
        home: &NodeId,
        chunk_key: &str,
        node: &str,
        tier: Tier,
        generation: u64,
    ) -> Result<(), TransportError> {
        self.grpc
            .announce_admit(home, chunk_key, node, tier, generation)
            .await
    }

    async fn announce_evict(
        &self,
        home: &NodeId,
        chunk_key: &str,
        node: &str,
        generation: u64,
    ) -> Result<(), TransportError> {
        self.grpc
            .announce_evict(home, chunk_key, node, generation)
            .await
    }

    async fn lookup_sharers(
        &self,
        home: &NodeId,
        chunk_key: &str,
    ) -> Result<Option<SharerSet>, TransportError> {
        self.grpc.lookup_sharers(home, chunk_key).await
    }

    /// Write-scatter offer (ADR-0032 § 2) — still gRPC here.
    ///
    /// This is the one control-plane-shaped method on this trait that carries
    /// bulk data, so unlike the directory RPCs above it is *not* permanently
    /// gRPC: `planning/24` Phase 5 replaces it with ADR-0018's WRITE verb, the
    /// trigger inverted so the offering coordinator posts into an arena frame the
    /// owner hands back. Delegating for now keeps the correctness work
    /// (staging, commit, reject-fast, the abort paths) transport-independent, so
    /// swapping the leg later cannot change what any of it means.
    async fn store_chunk(
        &self,
        owner: &NodeId,
        offer: crate::StoreOffer<'_>,
    ) -> Result<crate::StoreOutcome, TransportError> {
        self.grpc.store_chunk(owner, offer).await
    }

    /// Commit is metadata only and stays gRPC forever, like the directory edges.
    async fn commit_upload(
        &self,
        owner: &NodeId,
        upload_id: &str,
        e_tag: &str,
    ) -> Result<u32, TransportError> {
        self.grpc.commit_upload(owner, upload_id, e_tag).await
    }

    /// Discard is metadata only and stays gRPC forever, like the directory edges.
    async fn discard_upload(&self, owner: &NodeId, upload_id: &str) -> Result<u32, TransportError> {
        self.grpc.discard_upload(owner, upload_id).await
    }
}

impl EfaRdmaTransport {
    /// The RDMA attempt: lease a buffer, offer it on the fetch RPC, and
    /// either read the WRITE-landed bytes out of the lease (RDMA served) or
    /// finish decoding the streamed body (`served_via_rdma == false` — the
    /// holder itself chose to fall back). Any error here is the caller's
    /// cue to retry over [`GrpcTransport`] from scratch.
    async fn try_fetch_via_rdma(
        &self,
        peer: &NodeId,
        cache_key: &str,
        range: Option<ByteRange>,
        no_fill: bool,
    ) -> Result<BlobStream, TransportError> {
        // Collect the rails that can carry RDMA to this peer right now: pump
        // alive (A1 — a dead rail degrades to the others, not to gRPC) AND a
        // handshake taught it the peer's same-index rail endpoint. Empty =
        // RDMA not negotiated / plane dead: skip straight to the gRPC path
        // rather than leasing a buffer for nothing.
        let mut usable = Vec::with_capacity(self.rails.len());
        for (i, rail) in self.rails.iter().enumerate() {
            if rail.usable_for(peer.name()).await {
                usable.push(i);
            }
        }
        if usable.is_empty() {
            return Err(TransportError::Other(anyhow::anyhow!(
                "no usable RDMA rail for peer {} (not negotiated, or pumps dead); using gRPC",
                peer.name()
            )));
        }
        // Round-robin over the usable rails — concurrency across fetches is
        // what aggregates rails (one fetch rides one rail).
        let pick = self.next_rail.fetch_add(1, Ordering::Relaxed) as usize % usable.len();
        let rail_idx = usable[pick];
        let rail = &self.rails[rail_idx];
        let lease = rail.requester_arena.lease_owned().await;
        let remote = lease.remote();
        let (range_start, range_end, suffix_len) = range_fields(range);
        let mut stream = match call_fetch_blob(
            &self.grpc,
            peer,
            FetchBlobRequest {
                cache_key: cache_key.to_owned(),
                range_start,
                range_end,
                suffix_len,
                no_fill,
                rdma_buffer: Some(RdmaBuffer {
                    addr: remote.addr,
                    rkey: remote.rkey,
                    len: remote.len as u64,
                    rail: rail_idx as u32,
                }),
                requester_node_id: Some(self.local_node.clone()),
                // A body-path fetch delivers into this node's own arena; a client's own
                // registered window is only ever offered by `fetch_chunk_into_token`.
                client_token: None,
            },
        )
        .await
        {
            Ok(stream) => stream,
            Err(e) => return Err(self.on_rdma_transport_error(peer, Some(rail_idx), e).await),
        };
        let (meta, first_data) = match read_first_chunk(&mut stream).await {
            Ok(chunk) => chunk,
            Err(e) => return Err(self.on_rdma_transport_error(peer, Some(rail_idx), e).await),
        };
        if !meta.served_via_rdma {
            // The holder fell back on its own (capacity, no cached AH for
            // us, its own WRITE error) — finish decoding what it streamed
            // instead, reusing the first chunk already read.
            return decode_stream_fallback(meta, first_data, stream).await;
        }
        Ok(blob_from_written_lease(lease, meta))
    }

    /// A2 requester-side counterpart to [`Self::serve_via_write`]'s eviction: an RDMA fetch
    /// that failed may have left this node holding a stale AH for the peer, so evict — over
    /// the rails [`requester_eviction_scope`] says the error is evidence about — and return
    /// `err` unchanged for [`PeerTransport::fetch_blob`] to retry over gRPC.
    ///
    /// `rail_idx` is the rail this fetch was attempted on, and it is the *whole* of what a
    /// non-`PeerUnavailable` error licenses touching. **`None` means no rail of ours was
    /// involved at all** — the client-token path (`fetch_chunk_into_token`) leases nothing and
    /// posts nothing, since the HOLDER writes — so a `OneRail` verdict has no rail to name and
    /// degrades to evicting nothing. Suppressing it there is the same discipline as the scopes
    /// below: evict on evidence about a rail, never on the absence of it.
    ///
    /// Three behaviours, where there used to be one:
    ///
    /// * A protocol answer ([`TransportError::NotCached`],
    ///   [`TransportError::RangeNotSatisfiable`]) evicts nothing — the peer answered, so its
    ///   endpoint is demonstrably fine (ADR-0003: only transport-layer errors imply a stale
    ///   endpoint).
    /// * [`TransportError::PeerUnavailable`] evicts **every** rail and kicks off a proactive
    ///   re-handshake (this side *does* have a dialable [`NodeId`], unlike the holder). The
    ///   peer's gRPC is not answering, which is what a restarting pod looks like, and a
    ///   restart invalidates every rail's endpoint at once. This is the case A2 was written
    ///   for.
    /// * Anything else evicts **only `rail_idx`**, and does *not* re-handshake. The fetch
    ///   reached the peer, so at most this rail's queue pair is suspect; 31 rails keep
    ///   carrying RDMA and the ≤30 s sweep restores the one. Not re-handshaking here is
    ///   deliberate: the measured incident produced 2675 of these errors in a 30-second
    ///   bucket, and a spawned handshake per error would have been a storm aimed at a peer
    ///   that was already struggling.
    async fn on_rdma_transport_error(
        &self,
        peer: &NodeId,
        rail_idx: Option<usize>,
        err: TransportError,
    ) -> TransportError {
        let scope = match (requester_eviction_scope(&err), rail_idx) {
            // A per-rail verdict with no rail behind it. See this function's doc.
            (EvictionScope::OneRail, None) => EvictionScope::None,
            (scope, _) => scope,
        };
        // The index is only read for `OneRail`, which the match above has just proved carries
        // a rail — so the fallback is unreachable rather than a default.
        self.evict_stale_ah(peer.name(), scope, rail_idx.unwrap_or(0))
            .await;
        if scope == EvictionScope::AllRails {
            self.spawn_rehandshake(peer.clone());
        }
        err
    }
}

/// Register one rail's two arenas **on a thread pinned to that rail's NUMA node**
/// (planning/19 D5 step 0).
///
/// Why a borrowed thread rather than pinning the caller: `build_rails` runs on a
/// tokio worker during daemon startup, and permanently narrowing a worker's
/// affinity would hobble the whole runtime. A scoped thread is pinned, does the
/// registration, and dies — and because `ibv_reg_mr` is what first-touches and
/// pins the pages, doing it on that thread is precisely what places them on the
/// NIC's own node. A one-sided WRITE DMA-*reads* its source from host memory and
/// the requester's NIC DMA-*writes* the landing range, so far-socket pages put
/// every byte across the inter-socket link — the mechanism `affinity`'s module doc
/// documents and D4 measured the shadow of.
///
/// Falls back to registering inline when the placement is unpinned or the thread
/// cannot be spawned: an unplaced arena is a throughput regression, a failed
/// startup is an outage.
///
/// # Panics
///
/// If either arena fails to map or register — the contract
/// [`EfaRdmaTransport::new`] documents.
fn register_rail_arenas(
    ctx: &EfaContext,
    requester_ranges: usize,
    holder_ranges: usize,
    range_bytes: usize,
    cfg: ArenaConfig,
) -> (RailBuffers, RailBuffers) {
    let build = || {
        let requester = RailBuffers::new(ctx.pd(), requester_ranges, range_bytes, cfg.pages)
            .expect("mapping and registering a requester-side RDMA arena at startup");
        let holder = RailBuffers::new(ctx.pd(), holder_ranges, range_bytes, cfg.pages)
            .expect("mapping and registering a holder-side RDMA arena at startup");
        (requester, holder)
    };
    let placement = ctx.placement();
    if !placement.is_pinned() {
        return build();
    }
    std::thread::scope(|scope| {
        let handle = std::thread::Builder::new()
            .name(format!("pacer-arena-{}", placement.cpu))
            .spawn_scoped(scope, || {
                affinity::pin_current_thread(placement, "arena registration");
                build()
            });
        match handle {
            Ok(joined) => joined
                .join()
                // A panic here is `HostArena::new`'s documented startup failure
                // (memlock, or a pod limit that does not cover the pinned bytes),
                // raised on the borrowed thread; re-panic so it stays fatal
                // instead of silently degrading to an unplaced arena.
                .unwrap_or_else(|e| std::panic::resume_unwind(e)),
            Err(e) => {
                warn!(error = %e, cpu = placement.cpu, "could not spawn a pinned thread to register this rail's arenas; registering unplaced");
                build()
            }
        }
    })
}

/// Register both arenas on every rail (ADR-0024) and assemble the [`Rail`]s.
///
/// The pinned-memory BUDGET is what divides across rails; the range COUNT it
/// buys does NOT shrink with rails the way ADR-0018's fixed 64 slots did — those
/// were divided, so p5's 32 rails got 2 slots each and the requester capped the
/// node at 2 concurrent fetches per rail (planning/19 D2). A node now keeps its
/// node-wide concurrency and merely spreads it, with a one-range floor per rail
/// so a high rail count can never leave a rail unable to lease at all.
///
/// Also builds ADR-0028's cache slab, when `cfg.slab_bytes` asks for one: it is
/// registered on every rail's PD, so it has to be built here, where the PDs are.
///
/// # Panics
///
/// If any arena fails to map or register — see [`EfaRdmaTransport::new`], whose
/// contract this implements. The slab is fatal on the same terms and for the
/// same reason (ADR-0028's last consequence: `mem_capacity` becomes a hard
/// reservation, so a slab that silently came up smaller is a silently smaller
/// cache) — but a budget too small to be *useful* is not an error, it just means
/// no slab.
fn build_rails(ctxs: Vec<Arc<EfaContext>>, cfg: ArenaConfig) -> (Vec<Rail>, Option<CacheSlab>) {
    let n = ctxs.len();
    let range_bytes = arena::range_bytes_for_chunk(cfg.chunk_size);
    let per_rail_requester = arena::ranges_for(cfg.requester_bytes / n, range_bytes);
    let per_rail_holder = (HOLDER_ARENA_RANGES / n).max(1);
    let rails: Vec<Rail> = ctxs
        .into_iter()
        .map(|ctx| {
            let (requester_arena, holder_arena) =
                register_rail_arenas(&ctx, per_rail_requester, per_rail_holder, range_bytes, cfg);
            Rail {
                ctx,
                requester_arena,
                holder_arena,
                ah_cache: AhCache::new(),
                client_ah: client_write::ClientAhCache::default(),
                client_ready: client_write::ClientReady::default(),
                write_window: (cfg.rail_window > 0)
                    .then(|| Arc::new(Semaphore::new(cfg.rail_window))),
                writes_in_flight: AtomicU64::new(0),
                writes_total: AtomicU64::new(0),
                write_bytes_total: AtomicU64::new(0),
                ah_evictions: AtomicU64::new(0),
            }
        })
        .collect();
    // One line carrying everything a run needs to be interpreted: the realized
    // page size (a hugepage request can degrade — see `HostArena::new`), the
    // per-rail range counts that ARE the node's concurrency, and the pinned
    // bytes the pod's memory limit must cover.
    // ADR-0028: one slab, registered on every rail's PD. Built AFTER the arenas
    // so a node whose memlock budget cannot cover both fails on the arenas (the
    // plane's correctness floor) rather than on the slab (an optimization).
    //
    // Deliberately NOT registered on a pinned thread the way the per-rail arenas
    // are (D5, planning/19): one mapping is shared by every rail, so there is no
    // single NUMA node to be local to. That is a real tension with D5's +65% —
    // and a thing to measure rather than assume, since D5's win came from the
    // requester arenas, which keep their placement.
    let slab = {
        let pds: Vec<&ProtectionDomain> = rails.iter().map(|r| r.ctx.pd()).collect();
        slab::CacheSlab::new(
            &pds,
            cfg.slab_bytes,
            // NOT `range_bytes_for_chunk` on its own: that sizes the requester/holder arena
            // ranges, which hold a fetched BODY and want no headroom. A slab frame holds what
            // the cache reads in one I/O — one chunk today, hence a headroom of 0.
            arena::range_bytes_for_chunk(cfg.chunk_size + cfg.slab_frame_headroom),
            cfg.pages,
        )
        .expect("mapping and registering the ADR-0028 cache slab at startup")
    };
    let pinned_bytes: usize = rails
        .iter()
        .map(|r| r.requester_arena.registered_bytes() + r.holder_arena.registered_bytes())
        .sum::<usize>()
        // The slab is ONE mapping, pinned once however many rails register it —
        // so it is added once, not per rail. Counting it per rail would overstate
        // what the pod's memory limit must cover 32-fold on a p5.
        + slab.as_ref().map_or(0, CacheSlab::registered_bytes);
    info!(
        rails = n,
        requester_ranges_per_rail = per_rail_requester,
        holder_ranges_per_rail = per_rail_holder,
        range_bytes,
        pages = rails[0].requester_arena.pages().label(),
        pinned_bytes,
        // How many rails got their arenas registered node-locally (planning/19
        // D5). A run with this at 0 is the pre-D5 configuration, whatever the
        // policy asked for, and must not be compared against a placed one.
        node_local_rails = rails
            .iter()
            .filter(|r| r.ctx.placement().is_pinned())
            .count(),
        // ADR-0028's slab, when configured: the frames ARE the cache's RAM tier,
        // so this count is the node's resident-chunk ceiling, and the pages are
        // what decide whether registering it cost seconds or minutes.
        slab_frames = slab.as_ref().map_or(0, CacheSlab::frames),
        slab_pages = slab.as_ref().map_or("none", |s| s.pages().label()),
        "RDMA arenas registered (ADR-0024)"
    );
    (rails, slab)
}

/// Build the [`BlobStream`] for the RDMA-served case with NO copy-out: the
/// bytes are already in `lease`'s registered buffer (the holder's WRITE landed
/// before it returned the gRPC response — ADR-0018 point 3's done-as-response
/// guarantee), so this hands the S3 layer a `Bytes` that *owns the lease*
/// ([`bytes::Bytes::from_owner`] over the lease's [`OwnedRdmaLease::Written`])
/// rather than memcpying the range into a fresh allocation. The registered range
/// stays leased for exactly as long as that `Bytes` (and every clone/slice)
/// lives, and returns to the arena on the last drop (planning/16 §5
/// recommendation 2 — the single largest avoidable serve-path CPU cost, 0.30
/// CPU-s/GiB at client rate). That long hold is precisely why ADR-0024 sizes the
/// arena by hold time instead of shortening it with a copy-out.
///
/// This retires the requester-side copy: `pacer_rdma_requester_copy_seconds_total`
/// now reads ~0 on the RDMA-served path (it is only touched by the legacy
/// fallback, which this path never uses). See [`EfaRdmaTransport::requester_copy_nanos`].
///
/// Retention hazard (documented for the reviewer): anything that *keeps* these
/// bytes beyond the client stream — the layer-1 cache admit in the daemon's
/// proxy — must copy them out of the range first, or a hot chunk would pin its
/// registered range indefinitely and starve the arena. The daemon's admit path
/// (`proxy.rs::maybe_admit_local`) does exactly that.
///
/// Generic over the lease shape rather than taking [`ArenaLease`] concretely:
/// this is the requester's half of the [`buffers`] seam, so the HBM backend
/// (ADR-0022, track H) reuses it unchanged.
fn blob_from_written_lease<L: OwnedRdmaLease>(lease: L, meta: BlobMeta) -> BlobStream {
    let written = lease.into_written(meta.total_len as usize);
    let body = Bytes::from_owner(written);
    BlobStream {
        len: meta.total_len,
        object_len: meta.object_len,
        body_start: meta.body_start,
        e_tag: meta.e_tag,
        content_type: meta.content_type,
        last_modified_epoch_secs: meta.last_modified_epoch_secs,
        chunks: Box::pin(futures::stream::iter([Ok(body)])),
    }
}

/// Finish assembling a [`BlobStream`] when the holder answered
/// `served_via_rdma = false` on a request that DID offer a buffer — same
/// shape as [`crate::grpc::decode_blob_stream`], but starting from a first
/// chunk already consumed by [`read_first_chunk`] rather than reading it
/// again.
async fn decode_stream_fallback(
    meta: BlobMeta,
    first_data: Bytes,
    stream: tonic::Streaming<pacer_proto::v1::BlobChunk>,
) -> Result<BlobStream, TransportError> {
    use futures::StreamExt;
    let head = futures::stream::iter([Ok(first_data)]);
    let tail = stream.map(|chunk| match chunk {
        Ok(c) => Ok(c.data),
        Err(s) => Err(TransportError::PeerUnavailable(s.to_string())),
    });
    Ok(BlobStream {
        len: meta.total_len,
        object_len: meta.object_len,
        body_start: meta.body_start,
        e_tag: meta.e_tag,
        content_type: meta.content_type,
        last_modified_epoch_secs: meta.last_modified_epoch_secs,
        chunks: Box::pin(head.chain(tail)),
    })
}

/// The RDMA capabilities this node advertises at handshake once its EFA
/// context is up (ADR-0018: WRITE data plane; READ is ADR-0020's directory
/// primitive, not yet built in A1; `send_recv` stays false — the two-sided
/// messaging optimization, ADR-0019, is deferred).
pub fn advertised_capabilities() -> RdmaCapabilities {
    RdmaCapabilities {
        rdma_read: false,
        rdma_write: true,
        send_recv: false,
    }
}

/// Shared body of [`EfaRdmaTransport::initiate_handshake`] and the A2 proactive
/// re-handshake ([`EfaRdmaTransport::spawn_rehandshake`]): dial `peer`, carry
/// this node's `endpoint_proto` + capabilities, and learn whatever endpoint
/// comes back into `ah_cache`. Free-standing — over borrowed handles rather
/// than `&self` — so the eviction path can `tokio::spawn` it from cloned
/// `Arc`s without an `Arc<Self>` (which the trait-bound `&self` fetch/serve
/// methods cannot produce). A peer that responds without an `efa_endpoint`
/// (non-EFA, or its own probe failed) is simply not learned.
///
/// # Errors
///
/// The peer failing to dial or rejecting the handshake RPC; a malformed
/// returned endpoint ([`decode_endpoint`]); or [`AhCache::get_or_insert`]
/// failing to build the AH. All are non-fatal for the caller — log and let the
/// next sweep retry.
async fn handshake_and_learn(
    grpc: &GrpcTransport,
    rails: &[Rail],
    endpoint_proto: pacer_proto::v1::EfaEndpoint,
    peer: &NodeId,
) -> anyhow::Result<()> {
    let resp = grpc
        .handshake(peer, advertised_capabilities(), Some(endpoint_proto))
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let Some(peer_endpoint) = resp.efa_endpoint else {
        return Ok(());
    };
    learn_peer_rails(rails, peer.name(), &peer_endpoint).await
}

/// Shared body of [`EfaRdmaTransport::learn_peer`] and [`handshake_and_learn`]:
/// build an AH on each local rail for the peer's SAME-INDEX rail endpoint, up
/// to `min(local rails, peer rails)` — the by-index pairing rule
/// ([`EfaRdmaTransport::rails`]). A single-rail peer (no `rail_endpoints` on
/// the wire) is a one-element list: only rail 0 pairs, exactly the pre-A5
/// behavior.
///
/// # Errors
///
/// A malformed endpoint at any rail, or an AH build failing (see
/// [`AhCache::get_or_insert`]) — rails already learned stay learned; the
/// caller logs and lets the next sweep retry the rest.
async fn learn_peer_rails(
    rails: &[Rail],
    peer_node_id: &str,
    endpoint: &pacer_proto::v1::EfaEndpoint,
) -> anyhow::Result<()> {
    let peer_rails = decode_rail_endpoints(endpoint)?;
    for (rail, qpe) in rails.iter().zip(peer_rails.iter()) {
        rail.ah_cache
            .get_or_insert(peer_node_id, qpe, rail.ctx.pd())
            .await?;
    }
    Ok(())
}

/// Decode the wire-format `EfaEndpoint` proto message into an
/// [`ibverbs::QueuePairEndpoint`], the one place this crate's `efa` module
/// crosses from "proto bytes" to "ibverbs type" — kept internal so no other
/// crate needs `ibverbs` as a direct dependency just to pass an endpoint
/// through a handshake.
///
/// # Errors
///
/// The byte slice's length not matching [`QueuePairEndpoint::WIRE_LEN`], or
/// the flags byte carrying bits this version doesn't recognize (see
/// [`QueuePairEndpoint::from_bytes`]).
fn decode_endpoint(bytes: &[u8]) -> anyhow::Result<QueuePairEndpoint> {
    let bytes: [u8; QueuePairEndpoint::WIRE_LEN] = bytes
        .try_into()
        .map_err(|_| anyhow::anyhow!("EfaEndpoint wire length mismatch"))?;
    QueuePairEndpoint::from_bytes(&bytes).map_err(|e| anyhow::anyhow!("decoding EfaEndpoint: {e}"))
}

/// Decode every rail endpoint a peer advertised, in rail order: the
/// `rail_endpoints` list when present (A5 multi-rail peers), else the lone
/// `queue_pair_endpoint` (single-rail peers predating the field) as a
/// one-element list.
///
/// # Errors
///
/// Any element failing [`decode_endpoint`].
fn decode_rail_endpoints(
    endpoint: &pacer_proto::v1::EfaEndpoint,
) -> anyhow::Result<Vec<QueuePairEndpoint>> {
    if endpoint.rail_endpoints.is_empty() {
        return Ok(vec![decode_endpoint(&endpoint.queue_pair_endpoint)?]);
    }
    endpoint
        .rail_endpoints
        .iter()
        .map(|b| decode_endpoint(b))
        .collect()
}

#[cfg(test)]
mod eviction_scope_tests {
    use super::{holder_eviction_scope, requester_eviction_scope, EvictionScope};
    use crate::TransportError;
    use ibverbs::{WcError, WcStatus};

    /// EFA's `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER`, spelled again here because
    /// `client_write`'s copy is private to it and this asserts the same hardware fact from
    /// the other side of the same decision.
    const UNKNOWN_PEER: u32 = 14;

    /// **The defect, as one assertion.** A fetch error that is not `PeerUnavailable` must not
    /// reach beyond the rail it happened on. This is the case that emptied a 32-rail AH cache
    /// and dropped the RDMA-served fraction to 0.313 while the holder was blameless
    /// (planning/19 § Track D item 1).
    #[test]
    fn a_slow_or_failed_serve_touches_only_its_own_rail() {
        let err = TransportError::Other(anyhow::anyhow!(
            "timed out after 5s awaiting WRITE completion"
        ));
        assert_eq!(requester_eviction_scope(&err), EvictionScope::OneRail);
    }

    /// A peer that cannot be dialed at all is the case A2 was written for: a restart hands it
    /// new SRD queue pairs on every rail at once, so every rail's AH really is stale.
    #[test]
    fn an_undialable_peer_invalidates_every_rail() {
        let err = TransportError::PeerUnavailable("connection refused".to_owned());
        assert_eq!(requester_eviction_scope(&err), EvictionScope::AllRails);
    }

    /// Protocol answers are not transport faults (ADR-0003): the peer replied, so its
    /// endpoint is demonstrably fine, and evicting for one would throw away a working path
    /// because a chunk happened not to be cached.
    #[test]
    fn a_protocol_answer_evicts_nothing() {
        for err in [
            TransportError::NotCached,
            TransportError::RangeNotSatisfiable,
        ] {
            assert_eq!(requester_eviction_scope(&err), EvictionScope::None, "{err}");
        }
    }

    /// On the holder side the pivot is the completion STATUS, not merely that there was an
    /// error: `UNKNOWN_PEER` says the requester's NIC holds no queue pair for us, which a
    /// restart makes true on every rail at once.
    #[test]
    fn an_unknown_peer_completion_invalidates_every_rail() {
        let err = anyhow::Error::new(WcError {
            status: WcStatus::RemoteOperationError,
            vendor_err: UNKNOWN_PEER,
        })
        .context("work request 42");
        assert_eq!(holder_eviction_scope(&err), EvictionScope::AllRails);
    }

    /// Every other holder-side failure is news about this rail. The timeout is the one that
    /// matters — it is the commonest, the WRITE may still be in flight (hence
    /// `write_sources_orphaned_total`), and it is exactly what a slow serve produces.
    #[test]
    fn a_timeout_or_other_status_touches_only_its_own_rail() {
        let timeout = anyhow::anyhow!("timed out after 5s awaiting WRITE completion");
        assert_eq!(holder_eviction_scope(&timeout), EvictionScope::OneRail);

        let torn_down = anyhow::anyhow!("completion pump dropped the waiter");
        assert_eq!(holder_eviction_scope(&torn_down), EvictionScope::OneRail);

        for (status, vendor) in [
            // Congestion exhausting the retry count says the rail is unhappy, not that the
            // peer has gone.
            (WcStatus::RetryExceeded, 0),
            (WcStatus::RemoteOperationError, 0),
            // The same vendor code under a different status must not read as the
            // un-installed-peer case either — `is_unknown_peer`'s contract, asserted here
            // from its caller's side.
            (WcStatus::RetryExceeded, UNKNOWN_PEER),
        ] {
            let err = anyhow::Error::new(WcError {
                status,
                vendor_err: vendor,
            });
            assert_eq!(
                holder_eviction_scope(&err),
                EvictionScope::OneRail,
                "{status:?}/{vendor}"
            );
        }
    }

    /// The property that makes this safe to reason about: exactly one requester error kind
    /// may empty the cache. A scan rather than a restatement, so that adding a
    /// `TransportError` variant without deciding its scope shows up here rather than in
    /// production.
    #[test]
    fn all_rails_is_reserved_for_the_peer_is_gone_evidence() {
        let all = [
            TransportError::NotCached,
            TransportError::RangeNotSatisfiable,
            TransportError::PeerUnavailable("x".to_owned()),
            TransportError::Other(anyhow::anyhow!("x")),
        ];
        let sweeping = all
            .iter()
            .filter(|e| requester_eviction_scope(e) == EvictionScope::AllRails)
            .count();
        assert_eq!(
            sweeping, 1,
            "exactly one requester error kind may evict every rail"
        );
    }
}
