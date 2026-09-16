//! Does the memory this daemon is *configured* to hold fit the cgroup limit the
//! kernel will kill it on? Asked once, at startup, before anything is allocated
//! (quality item **R3**).
//!
//! ## Why this exists in the daemon and not only in the chart
//!
//! The budget itself is not new: `pacer.memoryLimit` in
//! `deploy/helm/pacer/templates/_helpers.tpl` (**L690-769**) sums every term
//! `config.memCapacity`'s own accounting cannot see and emits the container limit.
//! That helper is the **spec** this module mirrors term for term, and each [`Term`]
//! below cites the helper and the line it comes from.
//!
//! ⚠ The **names** are what holds the two in lockstep — every [`Term::helper`] is
//! asserted to exist in that file by `helper_terms_are_all_present_in_the_chart`, so a
//! helper renamed or deleted fails the build. The **line numbers are a reading aid as
//! of this commit** and nothing checks them; trust the name, use the number to find it.
//!
//! What the chart cannot do is *check* its own arithmetic against the limit the kubelet
//! actually applied — it can only compare values with each other. It does that already,
//! and well: `pacer.validateMemoryBudget` fails the render when
//! `resources.limits.memory < config.memCapacity`, and
//! `pacer.validateArenaReservation` when `efa.pinnedPoolReservation` is below the two
//! arenas (both in `templates/_validate.tpl`).
//!
//! **This module exists for the gap the first of those documents as deliberate.** Its
//! own comment: *"Equality still renders, and that is a deliberate gap.
//! `memCapacity: 24GiB` against a 24Gi limit is exactly what cost a 256 GiB run"* — and
//! it renders because a chart cannot tell a deliberately tiny tier on a large limit from
//! an under-sized one. A **daemon** can, because by then the terms are resolved and the
//! limit is a file: comparing the whole sum against the *rendered* limit is a decision
//! neither key alone supports, and it is what closes incident 1.
//!
//! The failure it replaces leaves nothing behind: the kernel kills the container, so
//! **nothing reaches the log** (`c4-dcp-256gib.md`: the last line was a routine
//! directory sweep), and the client-side symptom names memory nowhere —
//! `IncompleteRead(… more expected)` on one arm, `EndpointConnectionError` on the next,
//! both from the same server-side cause (`c4-dcp-hf-safetensors.md`).
//!
//! ## What it catches, against the recorded incidents
//!
//! `docs/runbooks/daemon-oom.md` § 2 numbers eight. **Four are the daemon's own cgroup**
//! (1-4); 5-8 are a loader, a bench pod or a third-party client OOMing in *their* cgroup,
//! which nothing in this process can see. Of the four:
//!
//! | # | incident | configured | limit | caught? |
//! |---|---|---|---|---|
//! | 1 | `c4-dcp-256gib.md` | 24 GiB tier + 20 GiB arenas + 9 GiB delivery = **53 GiB** | **52 GiB** | **YES**, at any headroom |
//! | 2 | `c4-dcp-hf-safetensors.md` 70B rung A2 | ~53 GiB | 76 GiB | no — the *measured* delivery working set, of which `pacer.deliveryWorkingSetBytes` is only a floor |
//! | 3 | `w1-write-scatter.md` `bal-c8`, 3 of 5 daemons | ~37 GiB | 53 GiB | no — pre-`ebe07c07` the coordinator held up to 32 GiB against a 1 GiB declared bound |
//! | 4 | `memgap-arms.md` (**still open**) | 64 GiB tier + 8 GiB arenas = 72 GiB | 104 GiB | no — +15.27 GiB of allocator retention over one restore |
//!
//! And, from § 5's history outside the eight, planning/16 §4.5: 16 GiB tier + 8 GiB
//! pools = 24 GiB against a 32 GiB limit — no, PUT/stream buffers under 100-concurrent
//! load. (`pacer.validateArenaReservation` is what covers that one now, at render time.)
//!
//! So this is deliberately **not** a general OOM guard. It catches the one class a
//! configuration can be *proved* wrong for — a budget that already exceeds the limit
//! before a byte is served — and refuses in the one place where refusing is cheap. The
//! others died from terms that are **not bounds**: page cache (`memory-model.md` rule 3,
//! `pacer_cgroup_memory_file_bytes`), a measured working set, allocator retention, code
//! exceeding its own declared bound. See [`DEFAULT_HEADROOM_FRACTION`] for what the
//! headroom does and does not stand in for, and why it cannot be enlarged to cover them.
//!
//! Refusing to start is the deliberate choice: an OOM kill later loses the whole warm
//! cache and reads as a protocol bug at the client, while a refused start is one line
//! in `kubectl describe`. `PACER_MEMORY_CHECK=warn` downgrades it for a migration.

use crate::config::Config;

/// Fraction of the cgroup limit the check leaves unclaimed by the terms below.
///
/// **It is a stand-in for the two terms no configuration can express**, both named in
/// `docs/helm/memory-model.md`: the **page cache** foyer's
/// buffered disk tier instantiates (rule 3 — measured at 72.1 GiB for one 131 GiB
/// checkpoint, and it scales with bytes moved rather than with any value in the
/// chart), and the delivery path's **measured** working set, of which
/// `pacer.deliveryWorkingSetBytes` is explicitly only a floor.
///
/// **10 % is an upper bound, not a lower one, and the arithmetic that pins it is the
/// chart's own render.** `--set efa.enabled=true --set delivery.enabled=true` on the
/// shipped values renders a 21 GiB limit against 18 GiB of terms this module counts,
/// i.e. only 14.3 % of that limit is unaccounted — so any fraction above
/// `21/18 − 1 = 16.6 %` would refuse a stock render. Nor does a larger value buy the
/// recorded incidents: the only one this check catches is caught at a headroom of
/// **zero** (53 GiB configured against a 52 GiB limit), and the next-closest would
/// need **33 %** (24 of 32 GiB), which is past that ceiling. 10 % is therefore the
/// largest round fraction that keeps every shipped configuration startable.
///
/// ## What the fraction actually asks of an operator
///
/// `pacer.validateArenaReservation` now makes `efa.pinnedPoolReservation` ≥ the two
/// arenas, and the slab, delivery and scatter terms are the *same* numbers on both
/// sides, so nearly everything cancels and the whole check reduces to
///
/// ```text
/// resources.limits.memory − config.memCapacity  ≥  headroom × total
/// ```
///
/// — which is `docs/helm/memory-model.md` rule 1 (*"~2× memCapacity is the rule of thumb
/// this repo's overlays use"*) turned into an enforced inequality. Incident 1 is the
/// case where that difference was **zero**, and `pacer.validateMemoryBudget` cannot fail
/// it at render time for a reason it states: a chart cannot tell a deliberately tiny
/// tier from an under-sized limit.
///
/// **The boundary, measured over every values file used against this chart
/// (2026-09-08):** of 34 renders — the base, the shipped example, and every internal
/// benchmark and dev overlay — **every one passes**, most with 3-10× margin. Exactly one
/// render does not, and it is worth knowing as a shape rather than as a filename: a
/// 4 GiB RAM tier, an 8 GiB arena pair and an 8 GiB slab total 20 GiB against a 20 GiB
/// rendered limit, i.e. `base == tier`, leaving the tier no room to fill. That overlay is
/// never deployed alone — layered onto its base it gets a 24 GiB limit and 2 GiB of slack
/// — and it is the case `pacer.validateMemoryBudget` names as why equality still renders
/// rather than failing. Enabling the slab on a node whose limit is close to
/// `tier + arenas + slab` is the configuration to check by hand.
pub const DEFAULT_HEADROOM_FRACTION: f64 = 0.10;

/// Refused above this, because a headroom of 1.0 asks for twice the budget as slack
/// and anything beyond it cannot be a margin — it is a second budget nobody wrote
/// down. Exclusive: `1.0` itself is already absurd.
///
/// Public because it is the bound [`Headroom::new`]'s error names, so an operator reading
/// that message can read the reason for it too.
pub const MAX_HEADROOM_FRACTION: f64 = 1.0;

/// Where the EFA device plugin mounts the rails. Its presence is how this module
/// decides whether the RDMA arenas will be pinned at all: the chart mounts it exactly
/// when `efa.enabled`, which is exactly when `pacer.memoryLimit` adds
/// `efa.pinnedPoolReservation` (`_helpers.tpl:693-696`). Probing the device rather
/// than the build keeps the two sides in step — an `efa`-feature image deployed with
/// `efa.enabled=false` gets neither the mount, nor the arenas, nor the budget term.
const EFA_DEVICE_DIR: &str = "/dev/infiniband";

/// Requester-arena bytes when `cluster.rdmaArenaBytes` is left empty.
///
/// A mirror of `pacer_transport::efa::DEFAULT_REQUESTER_ARENA_BYTES` rather than a
/// use of it, so this module and its tests compile on a build without the `efa`
/// feature (where `pacer_transport::efa` does not exist). The assertion below fails
/// the `efa` build — which is what CI's `rust-efa` and every `--all-features` check
/// compile — if the two ever drift.
const DEFAULT_REQUESTER_ARENA_BYTES: u64 = 4 << 30;
#[cfg(feature = "efa")]
const _: () = assert!(
    DEFAULT_REQUESTER_ARENA_BYTES as usize == pacer_transport::efa::DEFAULT_REQUESTER_ARENA_BYTES
);

/// Ranges the holder arena is cut into, one `chunk_size` each — the term
/// `pacer.pinnedPoolReservation`'s own comment spells `HOLDER_ARENA_RANGES (256) x
/// config.chunkSize` (`_helpers.tpl:510-539`, and `values.yaml` under `efa:`).
/// Mirrored, and cross-checked against the transport, for the reason above.
const HOLDER_ARENA_RANGES: u64 = 256;
#[cfg(feature = "efa")]
const _: () = assert!(HOLDER_ARENA_RANGES as usize == pacer_transport::efa::HOLDER_ARENA_RANGES);

/// foyer's own in-flight DRAM→NVMe write budget when `submit-queue-threshold` is `0`.
/// Restated here for the same reason `pacer.submitQueueThreshold`
/// (`_helpers.tpl:182-231`) restates it: the number the daemon will actually run is
/// the one worth reporting, not the zero in the values file.
const FOYER_DEFAULT_SUBMIT_QUEUE_THRESHOLD: u64 = 16 << 20;

/// One named term of the daemon's configured memory footprint.
///
/// A closed enum, not a string, and that is a metrics decision as much as a typing
/// one: `pacer_memory_budget_bytes` is labelled by [`Term::label`], so the label set
/// is bounded by this declaration and a dashboard's series count cannot grow with
/// configuration (the cardinality discipline `metrics.rs` keeps for
/// `pacer_rdma_rail`).
///
/// Discriminants are explicit and dense because [`MemoryBudget`] indexes an array by
/// them; [`Term::ALL`] is asserted to agree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[repr(usize)]
pub enum Term {
    /// The cache's RAM tier (`PACER_MEM_CAPACITY`). Not one of the terms
    /// `pacer.memoryLimit` *adds* — it is what the operator's own
    /// `resources.limits.memory` base (`_helpers.tpl:691`) has to hold, which is
    /// rule 1 of `docs/helm/memory-model.md` and the invariant nothing checked
    /// before this module: `memCapacity: 24GiB` against a 24Gi base is how
    /// `c4-dcp-256gib.md` died.
    CacheMemTier = 0,
    /// Requester-side RDMA arena (ADR-0024), the pinned bytes this node offers peers
    /// to WRITE into. Half of `efa.pinnedPoolReservation` as
    /// `_helpers.tpl:693-696` adds it, and named as `requester = cluster.rdmaArenaBytes`
    /// by the hugepage guard's own decomposition.
    EfaRequesterArena = 1,
    /// Holder-side RDMA arena: `HOLDER_ARENA_RANGES × chunk_size`, the other half of
    /// `efa.pinnedPoolReservation` (`_helpers.tpl:693-696`). Not a knob — the ADR
    /// fixes the range count so a range holds exactly one chunk.
    EfaHolderArena = 2,
    /// ADR-0028's registered cache slab, counted **once** however many rails
    /// register it: mirrors `pacer.cacheSlabBytes` (`_helpers.tpl:144-181`), added at
    /// `_helpers.tpl:697-710`.
    CacheSlab = 3,
    /// Client pages ADR-0026's delivery path may pin node-wide. Mirrors
    /// `delivery.pinnedReservation` (`_helpers.tpl:711-712`) — read from the
    /// *ceiling* the daemon enforces (`delivery.pinnedBytesMax`,
    /// `pacer.deliveryPinnedBytesMax` at `_helpers.tpl:563-578`) rather than from the
    /// reservation, because `pacer.validateDelivery` (`_helpers.tpl:595-634`) already
    /// fails the render when the reservation is the smaller of the two.
    DeliveryPinned = 4,
    /// The delivery fan-out's own in-flight bodies, `delivery.parallelism ×
    /// chunk_size`. Mirrors `pacer.deliveryWorkingSetBytes`
    /// (`_helpers.tpl:635-651`), added at `_helpers.tpl:721`. **A floor, not the
    /// working set** — the term that OOMKilled `c4-dcp-hf-safetensors.md` attempt 2
    /// is the measured one, which no formula holds.
    DeliveryWorkingSet = 5,
    /// ADR-0032 bytes this node holds for *other* nodes between their `UploadPart`
    /// and the coordinator's commit. Mirrors `pacer.scatterStagingBytes`
    /// (`_helpers.tpl:311-334`), added at `_helpers.tpl:761`.
    ScatterStaging = 6,
    /// This node's own buffered coordinator windows, `windows_in_flight ×
    /// chunk_size`. Mirrors `pacer.scatterCoordinatorBytes`
    /// (`_helpers.tpl:386-452`) at its **bound**, deliberately not at the chart's
    /// `coordinatorConcurrency × coordinatorObjectBytes` headroom form: since
    /// `ebe07c07` the semaphore permit is taken before the window's bytes, so this is
    /// the real node-wide ceiling, and a check that refused a start over declared
    /// headroom would refuse configurations that provably fit.
    ScatterCoordinator = 7,
    /// foyer's flush (DRAM→NVMe) buffer pool, `flush_buffer_size` or `2 ×
    /// block_size`. **Advisory** — see [`Term::counted`]: `pacer.memoryLimit` does
    /// not add it, because `resources.limits.memory` is the budget for everything the
    /// cache's own accounting can see.
    FoyerFlushBuffers = 8,
    /// foyer's in-flight DRAM→NVMe write budget. **Advisory**: the chart derives it
    /// (`pacer.submitQueueThreshold`, `_helpers.tpl:182-231`) and emits it into the
    /// ConfigMap, but deliberately does not add it to the limit — so neither does
    /// [`MemoryBudget::total`], which would otherwise make the daemon stricter than
    /// the chart and refuse a stock render (2 GiB of it at the shipped `flushers: 32`).
    FoyerSubmitQueue = 9,
    /// A body read's ordered look-ahead, `fill_parallelism × chunk_size`.
    /// **Advisory**, and the same reasoning: `docs/helm/memory-model.md` rule 1 names
    /// "in-flight chunk fills" as one of the things the base limit is *for*.
    ChunkFillPipeline = 10,
    /// What the S3 listener's connection cap allows at one chunk-sized buffer per
    /// accepted connection: `listen.max_connections × chunk_size`
    /// (ADR-0036, `PACER_S3_MAX_CONNECTIONS`).
    ///
    /// **Advisory, and it is the largest unbudgeted term on a default install** — 1024 ×
    /// 16 MiB is 16 GiB against a 4 GiB limit, which is exactly why it cannot be counted:
    /// the chart adds nothing for it, so enforcing it would refuse every shipped
    /// configuration. Reported because it is a *ceiling nobody has to reach*, and the
    /// distinction between a ceiling and a bound is what this module publishes the
    /// `counted` label for.
    ///
    /// The per-connection allowance is `chunk_size` on the cap's own authority:
    /// `DEFAULT_MAX_CONNECTIONS` in `crate::listen` states that "every accepted
    /// connection may hold a chunk-sized buffer on the fill path, so the memory this
    /// number multiplies is megabytes, not kilobytes". Watch it against
    /// `pacer_cgroup_memory_current_bytes` on a fan-in arm rather than budgeting for it:
    /// no arm has ever driven all 1024 at once, and `pacer_s3_connections_at_capacity_total`
    /// is what says whether the cap binds at all.
    S3Connections = 11,
}

impl Term {
    /// Every term, in discriminant order.
    pub const ALL: [Self; Self::COUNT] = [
        Self::CacheMemTier,
        Self::EfaRequesterArena,
        Self::EfaHolderArena,
        Self::CacheSlab,
        Self::DeliveryPinned,
        Self::DeliveryWorkingSet,
        Self::ScatterStaging,
        Self::ScatterCoordinator,
        Self::FoyerFlushBuffers,
        Self::FoyerSubmitQueue,
        Self::ChunkFillPipeline,
        Self::S3Connections,
    ];

    /// How many terms there are — the width of [`MemoryBudget`]'s array and the
    /// bound on `pacer_memory_budget_bytes`' `term` label.
    pub const COUNT: usize = 12;

    /// The `term` label this appears under in `pacer_memory_budget_bytes`.
    ///
    /// Stable strings: a dashboard, a `PrometheusRule` and a results file all outlive
    /// the Rust identifier, so renaming a variant must not rename a series.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::CacheMemTier => "cache_mem_tier",
            Self::EfaRequesterArena => "efa_requester_arena",
            Self::EfaHolderArena => "efa_holder_arena",
            Self::CacheSlab => "cache_slab",
            Self::DeliveryPinned => "delivery_pinned",
            Self::DeliveryWorkingSet => "delivery_working_set",
            Self::ScatterStaging => "scatter_staging",
            Self::ScatterCoordinator => "scatter_coordinator",
            Self::FoyerFlushBuffers => "foyer_flush_buffers",
            Self::FoyerSubmitQueue => "foyer_submit_queue",
            Self::ChunkFillPipeline => "chunk_fill_pipeline",
            Self::S3Connections => "s3_connections",
        }
    }

    /// Whether this term is part of the sum [`check`] enforces — i.e. whether
    /// `pacer.memoryLimit` adds it (or, for [`Term::CacheMemTier`], requires the base
    /// it passes through to hold it).
    ///
    /// **The uncounted terms are reported, not enforced, and that asymmetry is the
    /// point.** Counting a term the chart does not would make the daemon stricter
    /// than the limit the chart rendered, and the failure mode of *that* is a whole
    /// fleet refusing to start on its own defaults. So they are published as gauges
    /// (labelled `counted="false"`) for a dashboard to add up, and left out of the
    /// verdict.
    #[must_use]
    pub const fn counted(self) -> bool {
        !matches!(
            self,
            Self::FoyerFlushBuffers
                | Self::FoyerSubmitQueue
                | Self::ChunkFillPipeline
                | Self::S3Connections
        )
    }

    /// The `_helpers.tpl` construct this term mirrors, or `None` for a term the
    /// chart does not compute at all.
    ///
    /// This is the lockstep hook: `helper_terms_are_all_present_in_the_chart` reads
    /// the template out of the repo and fails the build when a name here has no
    /// counterpart there, so a helper renamed or deleted on the chart side cannot
    /// leave a stale mirror in this module.
    #[must_use]
    pub const fn helper(self) -> Option<&'static str> {
        match self {
            Self::CacheMemTier => Some(".Values.config.memCapacity"),
            Self::EfaRequesterArena | Self::EfaHolderArena => {
                Some(".Values.efa.pinnedPoolReservation")
            }
            Self::CacheSlab => Some("pacer.cacheSlabBytes"),
            Self::DeliveryPinned => Some("pacer.deliveryPinnedBytesMax"),
            Self::DeliveryWorkingSet => Some("pacer.deliveryWorkingSetBytes"),
            Self::ScatterStaging => Some("pacer.scatterStagingBytes"),
            Self::ScatterCoordinator => Some("pacer.scatterCoordinatorBytes"),
            Self::FoyerSubmitQueue => Some("pacer.submitQueueThreshold"),
            // Inside `.Values.resources.limits.memory`: the chart neither derives nor
            // adds these, so there is nothing to hold in lockstep.
            Self::FoyerFlushBuffers | Self::ChunkFillPipeline | Self::S3Connections => None,
        }
    }
}

/// Whether the RDMA arenas (and, with them, ADR-0028's slab) will actually be pinned
/// by this process.
///
/// Passed in rather than probed inside [`pinned_budget`] so that function stays pure
/// and its expected numbers can be asserted without a device tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EfaArenas {
    /// The transport will map and register them, so their bytes are resident from
    /// startup — and `pacer.memoryLimit` will have added
    /// `efa.pinnedPoolReservation` to cover them.
    Mapped,
    /// No arenas: either the `efa` feature is absent, or this daemon is single-node,
    /// or the device was never mounted. All three mean the chart added nothing for
    /// them either, so counting them would refuse a correctly-sized pod.
    Absent,
}

impl EfaArenas {
    /// Decide from the build, the configuration and the node.
    ///
    /// Under-counting is the safe direction here and this errs that way on purpose:
    /// a missed term costs a possible OOM later (the status quo), while an invented
    /// one refuses a start that would have worked.
    #[must_use]
    pub fn detect(cfg: &Config) -> Self {
        let built = cfg!(feature = "efa");
        if built && cfg.cluster.is_some() && std::path::Path::new(EFA_DEVICE_DIR).exists() {
            Self::Mapped
        } else {
            Self::Absent
        }
    }
}

/// Every term of the configured footprint, by name, plus the two sums.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryBudget {
    /// Bytes per term, indexed by `Term as usize`.
    bytes: [u64; Term::COUNT],
}

impl MemoryBudget {
    /// This term's bytes.
    #[must_use]
    pub fn get(&self, term: Term) -> u64 {
        self.bytes[term as usize]
    }

    /// Every term with its bytes, in [`Term::ALL`] order — what the gauges and the
    /// refusal message both iterate.
    pub fn terms(&self) -> impl Iterator<Item = (Term, u64)> + '_ {
        Term::ALL.into_iter().map(|t| (t, self.get(t)))
    }

    /// The sum [`check`] compares against the cgroup limit: the terms
    /// `pacer.memoryLimit` accounts for, and no others (see [`Term::counted`]).
    #[must_use]
    pub fn total(&self) -> u64 {
        self.terms()
            .filter(|(t, _)| t.counted())
            .map(|(_, b)| b)
            .sum()
    }

    /// The reported-but-not-enforced terms, summed. Present so an operator reading a
    /// refusal (or a dashboard) sees the whole configured footprint rather than only
    /// the part that produced the verdict.
    #[must_use]
    pub fn advisory_total(&self) -> u64 {
        self.terms()
            .filter(|(t, _)| !t.counted())
            .map(|(_, b)| b)
            .sum()
    }

    /// Build a budget with a chosen value per term.
    ///
    /// Test-only, and `pub(crate)` for one caller: `metrics.rs` asserts that every term
    /// publishes its own labelled series, which needs each term to carry a *distinct*
    /// value — a budget resolved from a configuration has zeroes and repeats, so a
    /// mislabelled `with_label_values` would pass by coincidence.
    #[cfg(test)]
    pub(crate) fn from_terms_for_test(mut value: impl FnMut(Term) -> u64) -> Self {
        let mut bytes = [0_u64; Term::COUNT];
        for term in Term::ALL {
            bytes[term as usize] = value(term);
        }
        Self { bytes }
    }

    /// One line per term, for a log line or a refusal.
    fn render_terms(&self) -> String {
        self.terms()
            .map(|(term, bytes)| {
                let kind = if term.counted() {
                    "counted"
                } else {
                    "advisory"
                };
                format!("\n  {:<22} {bytes:>16} B  ({kind})", term.label())
            })
            .collect()
    }
}

/// Compute every term of the configured footprint. Pure over `(cfg, arenas)`.
///
/// Mirrors `pacer.memoryLimit` (`_helpers.tpl:690-769`) term for term; each
/// [`Term`]'s doc names the line it comes from. Terms whose feature is off are `0`
/// rather than absent, so the gauge set does not change shape with configuration.
#[must_use]
pub fn pinned_budget(cfg: &Config, arenas: EfaArenas) -> MemoryBudget {
    let chunk = cfg.chunk.chunk_size();
    let pinned = arenas == EfaArenas::Mapped;
    let cluster = cfg.cluster.as_ref();
    let mut bytes = [0_u64; Term::COUNT];
    let mut set = |term: Term, value: u64| bytes[term as usize] = value;

    set(Term::CacheMemTier, cfg.cache.mem_capacity as u64);
    if pinned {
        let arena = cluster.map_or(0, |c| c.rdma_arena_bytes as u64);
        let requester = if arena == 0 {
            DEFAULT_REQUESTER_ARENA_BYTES
        } else {
            arena
        };
        set(Term::EfaRequesterArena, requester);
        set(Term::EfaHolderArena, HOLDER_ARENA_RANGES * chunk);
        set(
            Term::CacheSlab,
            cluster.map_or(0, |c| c.cache_slab_bytes as u64),
        );
    }
    if cfg.delivery.enabled {
        set(Term::DeliveryPinned, cfg.delivery.pinned_bytes_max);
        set(
            Term::DeliveryWorkingSet,
            cfg.delivery.parallelism as u64 * chunk,
        );
    }
    if cfg.scatter.enabled {
        set(Term::ScatterStaging, cfg.scatter.staging_bytes);
        set(
            Term::ScatterCoordinator,
            cfg.scatter.windows_in_flight as u64 * chunk,
        );
    }
    set(
        Term::FoyerFlushBuffers,
        cfg.cache.flush_buffer_size_or_default() as u64,
    );
    let submit_queue = cfg.cache.tuning.submit_queue_threshold as u64;
    set(
        Term::FoyerSubmitQueue,
        if submit_queue == 0 {
            FOYER_DEFAULT_SUBMIT_QUEUE_THRESHOLD
        } else {
            submit_queue
        },
    );
    set(Term::ChunkFillPipeline, cfg.fill_parallelism as u64 * chunk);
    set(
        Term::S3Connections,
        cfg.listen.max_connections as u64 * chunk,
    );
    MemoryBudget { bytes }
}

/// The fraction of the cgroup limit left unclaimed, validated once on the way in.
///
/// A newtype rather than a bare `f64` so an unvalidated number cannot reach the
/// arithmetic: `NaN` would make every comparison false and silently disable the
/// check, which is the one failure mode a guard must not have.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Headroom(f64);

impl Headroom {
    /// [`DEFAULT_HEADROOM_FRACTION`].
    pub const DEFAULT: Self = Self(DEFAULT_HEADROOM_FRACTION);

    /// No margin at all: the terms must merely fit. What a control arm asks for, and
    /// what the one recorded incident this check catches is caught at.
    pub const NONE: Self = Self(0.0);

    /// Validate a fraction.
    ///
    /// # Errors
    ///
    /// A value that is not finite, is negative, or is at/above
    /// [`MAX_HEADROOM_FRACTION`].
    pub fn new(fraction: f64) -> anyhow::Result<Self> {
        anyhow::ensure!(
            fraction.is_finite() && (0.0..MAX_HEADROOM_FRACTION).contains(&fraction),
            "memory headroom fraction {fraction} is not in [0, {MAX_HEADROOM_FRACTION})"
        );
        Ok(Self(fraction))
    }

    /// The validated fraction.
    #[must_use]
    pub fn fraction(self) -> f64 {
        self.0
    }

    /// Bytes the budget needs the limit to have: `total × (1 + fraction)`, rounded
    /// **up**, so a fractional byte can never make an over-budget configuration pass.
    ///
    /// Saturating rather than wrapping: a `total` near `u64::MAX` is a nonsense
    /// configuration, and the honest answer for it is "more than any limit".
    #[must_use]
    #[allow(clippy::cast_precision_loss, clippy::cast_sign_loss)]
    pub fn required(self, total: u64) -> u64 {
        let required = (total as f64 * (1.0 + self.0)).ceil();
        if required >= u64::MAX as f64 {
            u64::MAX
        } else {
            required as u64
        }
    }
}

/// What a budget that does not fit should do.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum CheckMode {
    /// Refuse to start. The default, and the reason is asymmetric cost: a refused
    /// start is one line in `kubectl describe`, while the OOM kill it replaces takes
    /// the warm cache with it and surfaces at the client as a truncated body or a
    /// vanished endpoint — never as a memory problem.
    #[default]
    Enforce,
    /// Log the same message at `warn` and start anyway. For a migration, where an
    /// operator is knowingly running a limit this arithmetic has not caught up with.
    Warn,
}

impl std::str::FromStr for CheckMode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "enforce" => Ok(Self::Enforce),
            "warn" => Ok(Self::Warn),
            other => anyhow::bail!("unknown memory check mode {other:?} (expected enforce|warn)"),
        }
    }
}

/// The two knobs of the startup check (`PACER_MEMORY_CHECK`,
/// `PACER_MEMORY_HEADROOM_FRACTION`).
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct MemoryCheckConfig {
    /// Refuse or warn.
    pub mode: CheckMode,
    /// Margin left for the terms nobody can compute (see
    /// [`DEFAULT_HEADROOM_FRACTION`]).
    pub headroom: Headroom,
}

impl Default for MemoryCheckConfig {
    fn default() -> Self {
        Self {
            mode: CheckMode::default(),
            headroom: Headroom::DEFAULT,
        }
    }
}

/// A budget that fits, and by how much.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Report {
    /// [`MemoryBudget::total`].
    pub total_bytes: u64,
    /// What the limit had to be: [`Headroom::required`] of the total.
    pub required_bytes: u64,
    /// The limit read from the cgroup, or `None` when it is unlimited or unreadable
    /// — the two cases a container cannot tell apart from inside and that both mean
    /// "nothing to check against".
    pub cgroup_max_bytes: Option<u64>,
}

impl Report {
    /// Whether there was a limit to check at all. `false` is not a pass — it is the
    /// absence of a verdict, and the caller logs it as such.
    #[must_use]
    pub fn checked(&self) -> bool {
        self.cgroup_max_bytes.is_some()
    }
}

/// A configured footprint that does not fit this container's cgroup limit.
///
/// Carries the whole budget rather than a summary because the actionable part is
/// *which* term is large: an operator reading this has to change one number, and the
/// message names every candidate with the value the daemon resolved for it.
#[derive(Clone, Debug, thiserror::Error)]
#[error(
    "configured memory budget does not fit this container's cgroup limit: \
     {total_bytes} B of accounted terms need {required_bytes} B at a headroom of \
     {headroom_percent:.1} %, but memory.max is {cgroup_max_bytes} B. \
     Raise resources.limits.memory (pacer.memoryLimit adds the pinned terms on top of \
     it), or lower one of the terms below. Refusing to start is deliberate: an OOM \
     kill later loses the whole cache and reads at the client as a truncated body or \
     a vanished endpoint. PACER_MEMORY_CHECK=warn downgrades this to a warning.{terms}"
)]
pub struct BudgetExceeded {
    /// [`MemoryBudget::total`].
    pub total_bytes: u64,
    /// [`Headroom::required`] of the total.
    pub required_bytes: u64,
    /// The cgroup limit that is too small.
    pub cgroup_max_bytes: u64,
    /// The headroom in force, as a percentage, for the message.
    headroom_percent: f64,
    /// Every term, pre-rendered, so the message an operator reads names the value the
    /// daemon resolved for each one rather than the knob it came from.
    terms: String,
}

/// Does this budget fit `cgroup_max`?
///
/// `cgroup_max` is `None` for **both** an unlimited cgroup (`memory.max` = `max`) and
/// one that could not be read; from inside a container those are indistinguishable
/// and neither gives anything to compare against, so both are a pass with
/// [`Report::checked`] false and the caller warns.
///
/// # Errors
///
/// [`BudgetExceeded`] when `total × (1 + headroom)` is above the limit. Exactly at the
/// limit passes: the limit is what the terms are allowed to consume, and refusing a
/// configuration whose arithmetic lands on it would refuse a chart that computed it.
pub fn check(
    budget: &MemoryBudget,
    cgroup_max: Option<u64>,
    headroom: Headroom,
) -> Result<Report, BudgetExceeded> {
    let total_bytes = budget.total();
    let required_bytes = headroom.required(total_bytes);
    let report = Report {
        total_bytes,
        required_bytes,
        cgroup_max_bytes: cgroup_max,
    };
    let Some(max) = cgroup_max else {
        return Ok(report);
    };
    if required_bytes <= max {
        return Ok(report);
    }
    Err(BudgetExceeded {
        total_bytes,
        required_bytes,
        cgroup_max_bytes: max,
        headroom_percent: headroom.fraction() * 100.0,
        terms: budget.render_terms(),
    })
}

/// Read the cgroup limit, compute the budget, and apply `cfg.memory.mode`.
///
/// Called from `main` immediately after the configuration loads and **before any
/// allocation**: the whole value of refusing here is that the tier, the arenas and
/// the slab have not been mapped yet, so a refusal costs a pod start rather than a
/// partially-warmed node.
///
/// # Errors
///
/// [`BudgetExceeded`] under [`CheckMode::Enforce`]. Under [`CheckMode::Warn`] the same
/// condition logs and returns `Ok`.
pub fn enforce_at_startup(cfg: &Config) -> anyhow::Result<MemoryBudget> {
    let budget = pinned_budget(cfg, EfaArenas::detect(cfg));
    let cgroup_max = crate::cgroup::sample().and_then(|s| s.max_bytes);
    match check(&budget, cgroup_max, cfg.memory.headroom) {
        Ok(report) if report.checked() => {
            tracing::info!(
                total_bytes = report.total_bytes,
                required_bytes = report.required_bytes,
                cgroup_max_bytes = report.cgroup_max_bytes,
                advisory_bytes = budget.advisory_total(),
                headroom_fraction = cfg.memory.headroom.fraction(),
                "configured memory budget fits the cgroup limit"
            );
            Ok(budget)
        }
        Ok(report) => {
            tracing::warn!(
                total_bytes = report.total_bytes,
                advisory_bytes = budget.advisory_total(),
                "no cgroup memory limit to check the configured budget against \
                 (unlimited, or memory.max unreadable) — the startup check is inert on \
                 this node; watch pacer_cgroup_memory_current_bytes instead"
            );
            Ok(budget)
        }
        Err(e) if cfg.memory.mode == CheckMode::Warn => {
            tracing::warn!(error = %e, "PACER_MEMORY_CHECK=warn: starting anyway");
            Ok(budget)
        }
        Err(e) => Err(e.into()),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        check, pinned_budget, CheckMode, EfaArenas, Headroom, MemoryBudget, Term,
        DEFAULT_HEADROOM_FRACTION,
    };

    /// One GiB, so the expectations below read as the chart's own units.
    const GIB: u64 = 1 << 30;

    /// The **whole** `config.yaml` `helm template deploy/helm/pacer` renders on the
    /// shipped values, copied verbatim (comments stripped) from the ConfigMap.
    ///
    /// Verbatim rather than minimal on purpose: the point of these two fixtures is that
    /// the daemon's resolution of the chart's *actual* output is what the budget is
    /// asserted against, so anything trimmed is a term whose provenance stops being
    /// checkable. `deny_unknown_fields` on the file schema means a chart that grows a key
    /// the daemon does not know fails here rather than in a cluster.
    const CHART_DEFAULT_CONFIG: &str = r#"
listen-addr: "0.0.0.0:9000"
admin-addr: "0.0.0.0:9090"
log:
  format: "json"
cache:
  dir: "/var/cache/pacer"
  mem-capacity: "1GiB"
  disk-capacity: "100GiB"
  block-size: "1GiB"
  io-engine: "psync"
  uring:
    threads: 4
    io-depth: 256
  tuning:
    flushers: 32
    reclaimers: 8
    submit-queue-threshold: "2147483648"
runtime:
  worker-threads: 0
  rdma-worker-threads: 0
memory:
  check: "enforce"
policy:
  min-object-size: "4MiB"
backend:
  backend-type: "express"
  endpoint: ""
  force-path-style: false
cluster:
  peer-listen-addr: "0.0.0.0:9100"
  channel-capacity: 8
"#;

    /// The same, for `--set efa.enabled=true --set delivery.enabled=true`. The only
    /// difference the daemon sees is the `delivery:` block — every arena and ceiling is
    /// left empty by the chart, so the numbers below come from the daemon's own
    /// defaults, which is exactly the coupling this test exists to pin.
    const CHART_EFA_DELIVERY_CONFIG: &str = r#"
listen-addr: "0.0.0.0:9000"
admin-addr: "0.0.0.0:9090"
log:
  format: "json"
cache:
  dir: "/var/cache/pacer"
  mem-capacity: "1GiB"
  disk-capacity: "100GiB"
  block-size: "1GiB"
  io-engine: "psync"
  uring:
    threads: 4
    io-depth: 256
  tuning:
    flushers: 32
    reclaimers: 8
    submit-queue-threshold: "2147483648"
runtime:
  worker-threads: 0
  rdma-worker-threads: 0
memory:
  check: "enforce"
policy:
  min-object-size: "4MiB"
backend:
  backend-type: "express"
  endpoint: ""
  force-path-style: false
cluster:
  peer-listen-addr: "0.0.0.0:9100"
  channel-capacity: 8
delivery:
  enabled: true
  max-target-bytes: "4Gi"
"#;

    /// `limits.memory` the chart renders on the shipped values: `resources.limits.memory`
    /// passed through, because nothing pinned is on.
    const CHART_DEFAULT_LIMIT: u64 = 4 * GIB;

    /// `limits.memory` the chart renders for `--set efa.enabled=true --set
    /// delivery.enabled=true`: 4 (base) + 8 (`efa.pinnedPoolReservation`) + 8
    /// (`delivery.pinnedReservation`) + 1 (`pacer.deliveryWorkingSetBytes`) = 21 GiB.
    /// The literal is what `helm template` prints, so a chart change to any of those
    /// four moves this test rather than passing silently.
    const CHART_EFA_DELIVERY_LIMIT: u64 = 22_548_578_304;

    /// The env a DaemonSet pod actually has: `PACER_NODE_NAME` is what turns cluster
    /// mode on (it is per-pod, so it can never live in the ConfigMap), and a static peer
    /// list stands in for the EndpointSlice watch so resolution does not need a cluster.
    fn cluster_env() -> Vec<(&'static str, &'static str)> {
        vec![
            ("PACER_NODE_NAME", "node-a"),
            ("PACER_PEERS", "node-a=10.0.0.1:9100"),
        ]
    }

    fn budget_of(yaml: &str, arenas: EfaArenas) -> MemoryBudget {
        let cfg = crate::config::resolve_for_test(yaml, &cluster_env())
            .expect("the chart's own ConfigMap must resolve");
        pinned_budget(&cfg, arenas)
    }

    /// Every term of the shipped defaults, against the 4 GiB the chart renders.
    ///
    /// The interesting assertion is the last one: the RAM tier is the ONLY enforced term
    /// on a default install, and the two foyer terms beside it sum to more than it does
    /// — which is why they are advisory. Counting them would put 5 GiB against a 4 GiB
    /// limit and refuse the chart's own defaults.
    #[test]
    fn budget_mirrors_the_chart_default_render() {
        let b = budget_of(CHART_DEFAULT_CONFIG, EfaArenas::Absent);
        assert_eq!(b.get(Term::CacheMemTier), GIB, "config.memCapacity: 1GiB");
        for term in [
            Term::EfaRequesterArena,
            Term::EfaHolderArena,
            Term::CacheSlab,
            Term::DeliveryPinned,
            Term::DeliveryWorkingSet,
            Term::ScatterStaging,
            Term::ScatterCoordinator,
        ] {
            assert_eq!(b.get(term), 0, "{} is off by default", term.label());
        }
        // 2 x blockSize (1GiB), because config.flushBufferSize ships empty.
        assert_eq!(b.get(Term::FoyerFlushBuffers), 2 * GIB);
        // pacer.submitQueueThreshold: flushers 32 x chunkSize 16MiB x depth 4.
        assert_eq!(b.get(Term::FoyerSubmitQueue), 2 * GIB);
        // DEFAULT_FILL_PARALLELISM (8) x the 16 MiB default chunk.
        assert_eq!(b.get(Term::ChunkFillPipeline), 128 << 20);
        // The listener's cap (1024) at one chunk-sized buffer each. FOUR TIMES the whole
        // limit, which is the clearest case for why an advisory term must not be counted:
        // the chart adds nothing for it because no arm has ever driven the cap.
        assert_eq!(b.get(Term::S3Connections), 16 * GIB);
        assert_eq!(b.total(), GIB);
        assert_eq!(b.advisory_total(), 20 * GIB + (128 << 20));
        assert!(
            b.advisory_total() > CHART_DEFAULT_LIMIT,
            "the advisory terms exceed the shipped limit, so counting them would refuse \
             every default install — that is the invariant Term::counted encodes"
        );
        check(&b, Some(CHART_DEFAULT_LIMIT), Headroom::DEFAULT)
            .expect("the chart's own default render must start");
    }

    /// Every term of `--set efa.enabled=true --set delivery.enabled=true`, and the
    /// **headroom ceiling that render imposes**: 18 GiB of terms inside a 21 GiB limit
    /// leaves 14.3 %, so 10 % fits and 17 % would refuse a stock install. That is the
    /// whole argument for [`DEFAULT_HEADROOM_FRACTION`] being an upper bound, and it is
    /// asserted here rather than only written down.
    #[test]
    fn budget_mirrors_the_chart_efa_and_delivery_render() {
        let b = budget_of(CHART_EFA_DELIVERY_CONFIG, EfaArenas::Mapped);
        assert_eq!(b.get(Term::CacheMemTier), GIB);
        // cluster.rdmaArenaBytes ships empty -> the transport's 4 GiB default.
        assert_eq!(b.get(Term::EfaRequesterArena), 4 * GIB);
        // HOLDER_ARENA_RANGES (256) x the 16 MiB chunk. With the requester arena that is
        // efa.pinnedPoolReservation's 8Gi default, exactly.
        assert_eq!(b.get(Term::EfaHolderArena), 4 * GIB);
        assert_eq!(
            b.get(Term::EfaRequesterArena) + b.get(Term::EfaHolderArena),
            8 * GIB,
            "the two arenas must reproduce efa.pinnedPoolReservation"
        );
        // efa.hugepages is empty, so pacer.cacheSlabBytes derives nothing.
        assert_eq!(b.get(Term::CacheSlab), 0);
        // delivery.pinnedBytesMax ships empty -> DEFAULT_PINNED_BYTES_MAX (8 GiB), which
        // pacer.validateDelivery guarantees delivery.pinnedReservation covers.
        assert_eq!(b.get(Term::DeliveryPinned), 8 * GIB);
        // DEFAULT_DELIVERY_PARALLELISM (64) x the 16 MiB chunk.
        assert_eq!(b.get(Term::DeliveryWorkingSet), GIB);
        assert_eq!(b.total(), 18 * GIB);
        assert_eq!(CHART_EFA_DELIVERY_LIMIT, 21 * GIB);

        check(&b, Some(CHART_EFA_DELIVERY_LIMIT), Headroom::DEFAULT)
            .expect("10 % headroom must leave the chart's own efa+delivery render startable");
        let too_much = Headroom::new(0.17).expect("a valid fraction");
        check(&b, Some(CHART_EFA_DELIVERY_LIMIT), too_much)
            .expect_err("17 % headroom would refuse a stock render — the ceiling is 21/18-1");
        // The ceiling this render imposes, asserted against the shipped default. A `const`
        // block because both sides are constants and clippy (rightly) wants that stated:
        // this is a fact about the two numbers, checked when the crate compiles, not a
        // runtime condition.
        const { assert!(DEFAULT_HEADROOM_FRACTION < 21.0 / 18.0 - 1.0) };
    }

    /// The one recorded incident a configured-budget check can catch, replayed:
    /// `bench/ladder/results/c4-dcp-256gib.md` raised `config.memCapacity` to 24 GiB
    /// without raising the 24Gi base under it, and the ladder's own overlay adds a
    /// 16 GiB requester arena, a 4 GiB holder arena and the delivery pair — 53 GiB of
    /// terms against the 52 GiB `kubectl describe` reported. **Caught at zero headroom**,
    /// which is what makes it a proof rather than a margin.
    #[test]
    fn the_c4_dcp_256gib_configuration_is_refused_at_any_headroom() {
        const LADDER_CONFIG: &str = r#"
cache:
  mem-capacity: "24GiB"
  block-size: "1GiB"
cluster:
  rdma-arena-bytes: "16GiB"
delivery:
  enabled: true
"#;
        /// What `kubectl describe` reported for the pod that was OOMKilled (exit 137):
        /// 24Gi base + 20Gi `efa.pinnedPoolReservation` + 8Gi
        /// `delivery.pinnedReservation`.
        const RENDERED_LIMIT: u64 = 52 * GIB;
        let b = budget_of(LADDER_CONFIG, EfaArenas::Mapped);
        assert_eq!(
            b.total(),
            53 * GIB,
            "24 tier + 16 + 4 arenas + 8 + 1 delivery"
        );
        let e = check(&b, Some(RENDERED_LIMIT), Headroom::NONE)
            .expect_err("53 GiB of terms cannot fit a 52 GiB limit even with no margin");
        // The refusal has to name the term to change, not just the shortfall.
        let message = e.to_string();
        assert!(message.contains("cache_mem_tier"), "{message}");
        assert!(message.contains("efa_requester_arena"), "{message}");
        assert!(message.contains("advisory"), "{message}");
    }

    /// Exactly at the limit is a pass, one byte over is not, and an unlimited cgroup is
    /// neither — the three cases the verdict turns on.
    #[test]
    fn the_boundary_is_inclusive_and_unlimited_is_not_a_verdict() {
        let b = budget_of(CHART_DEFAULT_CONFIG, EfaArenas::Absent);
        let required = Headroom::DEFAULT.required(b.total());
        let at = check(&b, Some(required), Headroom::DEFAULT).expect("exactly at the limit fits");
        assert_eq!(at.required_bytes, required);
        assert!(at.checked());
        check(&b, Some(required - 1), Headroom::DEFAULT)
            .expect_err("one byte short of the requirement must refuse");
        let unlimited = check(&b, None, Headroom::DEFAULT).expect("no limit is not a failure");
        assert!(
            !unlimited.checked(),
            "an unlimited cgroup must report NO verdict, not a pass"
        );
    }

    /// Headroom 0 requires exactly the total, and the requirement rounds UP so a
    /// fractional byte cannot let an over-budget configuration through.
    #[test]
    fn headroom_zero_requires_the_bare_total_and_rounds_up() {
        assert_eq!(Headroom::NONE.required(53 * GIB), 53 * GIB);
        assert_eq!(Headroom::NONE.required(0), 0);
        // 3 x 1.10 = 3.3 -> 4, never 3.
        let ten = Headroom::new(0.10).expect("valid");
        assert_eq!(ten.required(3), 4);
        assert_eq!(ten.required(10 * GIB), 11 * GIB);
    }

    /// A fraction that cannot be a margin is refused at parse time, because the failure
    /// mode of accepting it is a check that silently never fires: `NaN` makes every
    /// comparison false.
    #[test]
    fn an_unusable_headroom_fraction_is_refused() {
        assert!(Headroom::new(f64::NAN).is_err());
        assert!(Headroom::new(f64::INFINITY).is_err());
        assert!(Headroom::new(-0.01).is_err());
        assert!(Headroom::new(1.0).is_err());
        assert!(Headroom::new(0.0).is_ok());
        assert!(Headroom::new(0.999).is_ok());
    }

    /// The mode is an operator's explicit choice, so an unrecognised value is an error
    /// rather than a default in either direction.
    #[test]
    fn check_mode_parses_only_what_it_documents() {
        assert_eq!("enforce".parse::<CheckMode>().unwrap(), CheckMode::Enforce);
        assert_eq!(" WARN ".parse::<CheckMode>().unwrap(), CheckMode::Warn);
        assert_eq!(CheckMode::default(), CheckMode::Enforce);
        assert!("off".parse::<CheckMode>().is_err());
        assert!("".parse::<CheckMode>().is_err());
    }

    /// **The lockstep test.** Every helper this module claims to mirror must exist in
    /// the chart, so a term added or renamed on one side alone fails the build instead
    /// of drifting — which is the failure this whole module exists to make impossible.
    ///
    /// Reads the template out of the repo (`CARGO_MANIFEST_DIR`) rather than embedding
    /// it: an embedded copy would be a third thing to keep in step.
    #[test]
    fn helper_terms_are_all_present_in_the_chart() {
        /// The chart's helper library, relative to this crate's manifest.
        const HELPERS: &str = "../../deploy/helm/pacer/templates/_helpers.tpl";
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(HELPERS);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        for term in Term::ALL {
            if let Some(helper) = term.helper() {
                assert!(
                    text.contains(helper),
                    "term {} mirrors `{helper}`, which no longer appears in {} — the chart \
                     and crates/pacer-daemon/src/memory_budget.rs have drifted",
                    term.label(),
                    path.display()
                );
            }
        }
        // The other direction, for the sum itself: the helper this module re-derives.
        assert!(
            text.contains("pacer.memoryLimit"),
            "the mirrored sum is gone"
        );
    }

    /// **The other lockstep test.** The chart alerts on the same inequality
    /// [`check`] enforces — `PacerDaemonMemoryBudgetNearLimit` in
    /// `templates/prometheusrule.yaml` — and to write that expression its
    /// `pacer.memoryHeadroomFraction` helper must resolve the same default this
    /// module applies when `config.memoryHeadroomFraction` is unset. So the chart
    /// holds a copy of [`DEFAULT_HEADROOM_FRACTION`], and this reads it back.
    ///
    /// Why it is worth a test: an alert computed from a *different* headroom is
    /// worse than no alert. Too small and it stays quiet over configurations this
    /// check will refuse to start; too large and it pages over ones that start
    /// perfectly well — and either way the number in the alert's own description
    /// tells the operator something untrue about what was enforced.
    #[test]
    fn the_chart_mirrors_the_default_headroom_fraction() {
        /// The chart's helper library, relative to this crate's manifest.
        const HELPERS: &str = "../../deploy/helm/pacer/templates/_helpers.tpl";
        /// The `define` line whose body carries the mirrored literal.
        const HELPER: &str = r#"define "pacer.memoryHeadroomFraction""#;
        /// What precedes the literal in that body: sprig's fallback for an unset value.
        const FALLBACK: &str = "default \"";
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join(HELPERS);
        let text = std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("reading {}: {e}", path.display()));
        let body = text
            .split_once(HELPER)
            .unwrap_or_else(|| {
                panic!(
                    "`{HELPER}` is gone from {} — the budget alert has no headroom to \
                     render, or it now hard-codes one",
                    path.display()
                )
            })
            .1;
        let mirrored = body
            .split_once(FALLBACK)
            .and_then(|(_, rest)| rest.split_once('"'))
            .map(|(literal, _)| literal)
            .unwrap_or_else(|| {
                panic!(
                    "`{HELPER}` in {} no longer falls back to a literal fraction",
                    path.display()
                )
            });
        let mirrored: f64 = mirrored.parse().unwrap_or_else(|e| {
            panic!("the chart's fallback headroom `{mirrored}` is not a number: {e}")
        });
        assert_eq!(
            mirrored, DEFAULT_HEADROOM_FRACTION,
            "the chart's fallback headroom and DEFAULT_HEADROOM_FRACTION have drifted, so \
             PacerDaemonMemoryBudgetNearLimit alerts on a threshold this module does not \
             enforce"
        );
    }

    /// A term in the enforced sum MUST name the helper that adds it, so a future term
    /// cannot join the verdict without a chart counterpart — the asymmetry that would
    /// otherwise make the daemon stricter than the limit the chart renders.
    #[test]
    fn every_counted_term_names_a_helper() {
        for term in Term::ALL {
            assert_eq!(
                term.counted(),
                term.helper().is_some() && term != Term::FoyerSubmitQueue,
                "{}: counted terms need a helper, and the only helper-backed term the \
                 chart deliberately excludes from the limit is the submit queue",
                term.label()
            );
        }
    }

    /// `MemoryBudget` indexes an array by discriminant, and the labels are a public
    /// metric contract — so the order must match and no two may collide.
    #[test]
    fn term_discriminants_and_labels_are_a_stable_contract() {
        for (i, term) in Term::ALL.into_iter().enumerate() {
            assert_eq!(term as usize, i, "{} is out of order", term.label());
        }
        let mut labels: Vec<&str> = Term::ALL.iter().map(|t| t.label()).collect();
        labels.sort_unstable();
        let count = labels.len();
        labels.dedup();
        assert_eq!(labels.len(), count, "two terms share a metric label");
        assert_eq!(count, Term::COUNT);
    }
}
