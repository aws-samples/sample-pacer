//! The **bounded** registry every client-edge map on this node is built from — one entry per
//! client endpoint, least-recently-used first out, plus an idle TTL.
//!
//! ## What a client-edge map is, and why three of them existed unbounded
//!
//! A delivery client (ADR-0026/ADR-0030) is not a ring member: it never handshakes, has no
//! `node_id`, and is named only by the endpoints in its own token ([`crate::token::TokenRail`]).
//! Three separate pieces of per-client state follow from that, and all three used to grow for
//! the process lifetime:
//!
//! 1. `efa::client_write::ClientAhCache` — one `ibv_ah` per endpoint, per rail, so
//!    `ibv_create_ah` (a firmware admin command) stays off the per-chunk path.
//! 2. `efa::client_write::ClientReady` — the first-contact gate: which endpoints on this rail
//!    have already accepted a WRITE, and the mutex one chunk holds while proving it.
//! 3. `efa::Announcer`'s set — which endpoints this node has already announced itself to
//!    (ADR-0030 point 2).
//!
//! They are three *views of one population*: the client endpoints this node has talked to,
//! keyed identically on [`ClientEndpoint`]. That is why they share ONE capacity
//! (`PACER_EFA_MAX_CLIENTS`, [`max_clients`]) and ONE idle TTL (`PACER_EFA_CLIENT_TTL`,
//! [`client_ttl`]) rather than three of each — three independently-set numbers would let the
//! three maps disagree about how many endpoints they can remember, and the interesting failures
//! on this path are all disagreements.
//!
//! ## Why eviction skew between them is safe
//!
//! Eviction is per map, so one can forget an endpoint the others still hold. Every such skew
//! resolves through machinery that already exists, because both directions are states the
//! path already handles:
//!
//! * AH evicted, still "announced" and "proven": the next delivery rebuilds the handle and
//!   posts. The *client* holds its own handle for us in its own (separately bounded) cache
//!   (`pacer_client::handles`), so the WRITE lands.
//! * "announced"/"proven" evicted, AH held: first contact re-runs — one announce SEND and one
//!   extra WRITE attempt, which is exactly what a cold endpoint pays.
//! * The client really has gone and something stale survived: the WRITE completes
//!   `UNKNOWN_PEER`, which drops the proof (`ClientReady::forget`) and the announce record
//!   (`Announcer::forget`) and re-announces — the evidence-based recovery the recycled-QPN
//!   incident of 2026-08-25 forced.
//!
//! So no bound on any one of these maps can produce a wrong answer; the worst case is paying
//! first contact again.
//!
//! ## Why this module is not behind the `efa` feature
//!
//! For the same reason [`crate::announce`] and [`crate::token`] are not: the policy — the key,
//! the capacity, the eviction order, the TTL, the counters — is where the bugs are, and none of
//! it needs a device. [`ClientRegistry`] is generic over the value so it compiles and is tested
//! in every build (`cargo test --workspace`, no `--features efa`); the `efa` build instantiates
//! it at `ClientRegistry<std::sync::Arc<ibverbs::AddressHandle>>` and two device-free types.
//!
//! This is the daemon-side mirror of `pacer_client::handles::HandleCache`, which bounds the
//! handles a *client* holds for *writers*. The two ends are deliberately symmetric, including
//! the shape of the arithmetic below.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::warn;

use crate::token::TokenRail;

/// Client ranks one node serves at the same time.
///
/// Eight — one loader process per GPU, which is every accelerator node this repo runs:
/// p5.48xlarge (8 × H100) and p6-b200 (8 × B200). A rank is the unit that opens windows, so
/// this is the outer multiplier on the endpoint population.
const RANKS_PER_NODE: usize = 8;

/// Windows one rank has open at the same time.
///
/// Four, from the widest shape measured: `bench/ladder/results/c5-multirail.md` ran a 4-GPU
/// loader holding 16 windows, i.e. four per rank. Each `pacer_client::Client::open` brings up
/// its OWN queue pairs, so windows multiply endpoints — this is not a per-rank constant that
/// can be folded away.
const WINDOWS_PER_RANK: usize = 4;

/// Rails one window can name, i.e. distinct `(gid, qpn)` pairs it publishes.
///
/// 32 — a p5.48xlarge's full complement, and the ceiling rather than the expectation: ADR-0030
/// point 5 makes PCIe affinity mandatory, so a window normally names the four rails on its
/// GPU's own switch (`WindowSpec::rail_count`). Sizing on 32 is what keeps a client that
/// registers on every rail from evicting its own earlier rails mid-request.
const RAILS_PER_WINDOW: usize = 32;

/// Allowance for endpoints a departed process left behind.
///
/// Two, i.e. one dead generation may coexist with the live one. A queue-pair number is
/// **recycled** — a fresh process on the same device is handed the same small integers (`1`,
/// `49153`) — so an endpoint that has gone is not distinguishable by its key, and its entry
/// survives until the idle TTL or the LRU takes it (see [`ClientEndpoint`]). Two generations is
/// what the TTL needs headroom for; more than two means the TTL is not running.
const RESTART_ALLOWANCE: usize = 2;

/// Default ceiling on client endpoints one registry remembers.
///
/// `8 ranks × 4 windows × 32 rails × 2 generations = 2048`, i.e. every rank on the node holding
/// every window it was measured holding, each registered on every rail a p5 has, with a whole
/// dead generation still resident. That is a worst case, not an expectation, and the margin over
/// anything measured is what makes it safe to evict at: the realistic set is the PCIe-affine
/// four rails per window, `8 × 4 × 4 = 128` endpoints, which this clears by 16×.
///
/// The cost of the ceiling being *reached* is 2048 `ibv_ah` device objects on the AH cache of
/// each rail. That is the same per-rail number the client end bounds itself by
/// (`pacer_client::handles::DEFAULT_MAX_HANDLES`), and it is a backstop rather than a working
/// number: nothing is created until an endpoint is actually addressed.
pub const DEFAULT_MAX_CLIENTS: usize =
    RANKS_PER_NODE * WINDOWS_PER_RANK * RAILS_PER_WINDOW * RESTART_ALLOWANCE;

/// Override for [`DEFAULT_MAX_CLIENTS`], for a node serving wider clients than a p5's 32 rails
/// or more ranks than it has GPUs.
///
/// Read here rather than declared in `pacer_daemon::config`'s `EnvVar` table for the reason
/// `PACER_CLIENT_MAX_ADDRESS_HANDLES` is read in `pacer_client::handles`: this is the *other
/// end of that same bound*, and the two have to be derivable from one piece of arithmetic
/// rather than from a daemon config the client cannot read. It is also a backstop with a
/// derived default, not a chart-driven capacity — nothing in `deploy/` needs to set it.
const MAX_CLIENTS_ENV: &str = "PACER_EFA_MAX_CLIENTS";

/// Floor for [`MAX_CLIENTS_ENV`]: one full window's rails must fit, or first contact with a
/// single 32-rail client would evict its own earlier rails while the same request is still
/// running — the one case that is guaranteed to break rather than merely thrash.
const MIN_MAX_CLIENTS: usize = RAILS_PER_WINDOW;

/// Ceiling for [`MAX_CLIENTS_ENV`]. Well above any plausible rank × window × rail product, and
/// low enough that a mistyped value cannot ask the device for a number of address handles that
/// would take minutes of firmware admin commands to build.
const MAX_MAX_CLIENTS: usize = 1 << 16;

/// How long an endpoint may sit unused before it is dropped — an **idle** timeout, refreshed
/// every time the endpoint is addressed, not a lifetime.
///
/// Five minutes. The floor it has to clear is the longest single delivery this repo has
/// measured, so a live client is never expired out from under a request in progress: the
/// full-Laguna restore storm moved 1094.88 GiB at 8.862 GiB/s, ~124 s
/// (`bench/ladder/results/`, planning/19). 300 s clears that by ~2.4× and is the same order as
/// the daemon's own soft-state healer (`DEFAULT_REANNOUNCE_INTERVAL_SECS`, also 300 s) and 10×
/// the 30 s handshake sweep, so it sheds a departed client's endpoints inside a few scrape
/// intervals rather than at process exit.
///
/// Expiry is never a correctness event, only a cost one: an expired endpoint pays first contact
/// again (one `ibv_create_ah`, one announce SEND, one extra WRITE attempt). That asymmetry is
/// why the default is chosen to be safely *long* and the floor below is nonetheless small.
pub const DEFAULT_CLIENT_TTL: Duration = Duration::from_secs(300);

/// Override for [`DEFAULT_CLIENT_TTL`], **in whole seconds** (`"300"`). Same reason as
/// [`MAX_CLIENTS_ENV`] for living here.
const CLIENT_TTL_ENV: &str = "PACER_EFA_CLIENT_TTL";

/// Floor for [`CLIENT_TTL_ENV`], in seconds. One, not the measured-restore length: a short TTL
/// costs re-established first contact and nothing else (see [`DEFAULT_CLIENT_TTL`]), so an
/// operator diagnosing eviction must be able to set one. Zero is excluded because it would
/// expire an entry before its own inserter could use it.
const MIN_CLIENT_TTL_SECS: u64 = 1;

/// Ceiling for [`CLIENT_TTL_ENV`], in seconds: one day. Beyond this the TTL has stopped being a
/// reaper and the capacity is the only bound left, which is the state this module exists to end.
const MAX_CLIENT_TTL_SECS: u64 = 24 * 60 * 60;

/// Compile-time invariants on the sizing above — build failures rather than tests, because each
/// is a property of the constants alone (the discipline `pacer_client::handles` applies to the
/// bound it mirrors).
const _: () = {
    // The cap must clear one full first-contact burst from the widest client, or a single
    // 32-rail window's rails could evict each other.
    assert!(
        DEFAULT_MAX_CLIENTS >= RAILS_PER_WINDOW,
        "the cap must hold every rail of one window"
    );
    // And it must clear the largest endpoint set the measured shapes produce (8 ranks × 4
    // windows × 4 affine rails = 128), or the shipped default would evict on a shape this repo
    // has already run.
    assert!(
        DEFAULT_MAX_CLIENTS > RANKS_PER_NODE * WINDOWS_PER_RANK * 4,
        "the default must clear the PCIe-affine endpoint set of a full node"
    );
    // The bounds have to admit the defaults, or an unset environment would be clamped.
    assert!(MIN_MAX_CLIENTS <= DEFAULT_MAX_CLIENTS && DEFAULT_MAX_CLIENTS <= MAX_MAX_CLIENTS);
    assert!(
        MIN_CLIENT_TTL_SECS <= DEFAULT_CLIENT_TTL.as_secs()
            && DEFAULT_CLIENT_TTL.as_secs() <= MAX_CLIENT_TTL_SECS
    );
};

/// A client endpoint: the GID of one of its rails and the queue-pair number its window is
/// reachable at. **The one identity every client-edge map keys on**, never a name the client
/// chose.
///
/// GID *and* QPN: a client that restarts reuses its GID (the device's) with a fresh `qp_num`, so
/// keying on the GID alone would treat a new process as already-known and its WRITEs would fail
/// `UNKNOWN_PEER` with nothing to explain why. The peer-side version of that mistake is A1
/// finding 11 — RDMA silently stopped engaging with any peer after it restarted.
///
/// ⚠ **The QPN is not enough either, and no key can be.** A queue-pair number is recycled: a
/// fresh process on the same device is handed the same small integers (`1`, `49153`), so an
/// endpoint a record calls known can be one that has never heard of us. Measured 2026-08-25 on a
/// 70B / 8-GPU arm — four window-opens into the run, `ensure_announced` short-circuited on a
/// record left by an earlier arm, no announce was ever sent, and every WRITE to that endpoint
/// completed `UNKNOWN_PEER` until the ladder gave up and degraded the whole GET to a body. Two
/// mechanisms answer it, and both are needed: membership is invalidated by **evidence**
/// (`Announcer::forget`, `ClientReady::forget`, called on exactly that completion status), and
/// by **idleness** ([`DEFAULT_CLIENT_TTL`]), which needs no evidence and so also covers the
/// endpoint nothing has written to since the process died.
///
/// ⚠ **Same GID, different QPN is NOT a restart.** Every `pacer_client::Client::open` brings up
/// its own queue pairs, so one rank with four windows publishes four distinct QPNs on the same
/// device GID *at the same time*. A registry that replaced same-GID entries would therefore
/// destroy a live multi-window client's handles — which is why the key is the pair and the
/// stale-generation problem is left to the TTL.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct ClientEndpoint {
    /// GID of the client rail, in wire byte order.
    pub gid: [u8; 16],
    /// Queue pair the client's window is reachable at. **`0` is legitimate** — EFA firmware
    /// assigns from a small set including it — so it is never a sentinel.
    pub qpn: u32,
}

impl ClientEndpoint {
    /// The endpoint named by a GID and a queue-pair number.
    #[must_use]
    pub fn new(gid: [u8; 16], qpn: u32) -> Self {
        Self { gid, qpn }
    }
}

impl From<&TokenRail> for ClientEndpoint {
    /// The endpoint a token rail names. The `rkey` is deliberately not part of the identity: it
    /// is a capability over one window, and one endpoint serves many.
    fn from(rail: &TokenRail) -> Self {
        Self::new(rail.gid, rail.qpn)
    }
}

/// Why an entry left a [`ClientRegistry`] — a **fixed** label set, so the eviction counter is
/// usable as a Prometheus dimension.
///
/// Bounded on purpose: the endpoint's GID and QPN are per-client-process and would let one
/// workload mint series without limit, exactly as `client_write::write_failure_reason`
/// documents for the failure labels. They stay in the log line beside the increment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvictionReason {
    /// The capacity was reached and this was the least recently used entry. **Read this as an
    /// alarm, not a tuning hint**: with the cap at or above the concurrent working set the
    /// least-recently-used entry is by construction not an endpoint with a WRITE in flight, so
    /// a non-zero count says the cap was smaller than the working set.
    Cap,
    /// The entry went unused for longer than the idle TTL. The healthy, expected reason: it is
    /// how a client that vanished without unregistering stops being remembered.
    Ttl,
    /// Removed by name — evidence said the record was stale (`Announcer::forget`), or the
    /// endpoint was retired deliberately.
    Explicit,
}

impl EvictionReason {
    /// The metric label for this reason. Stable: it is a Prometheus dimension, so renaming one
    /// silently re-partitions a dashboard's history.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Cap => "cap",
            Self::Ttl => "ttl",
            Self::Explicit => "explicit",
        }
    }

    /// Every reason, for a caller that has to pre-create one series per label so a scrape shows
    /// a zero rather than a missing metric.
    #[must_use]
    pub fn all() -> [Self; 3] {
        [Self::Cap, Self::Ttl, Self::Explicit]
    }
}

/// A [`ClientRegistry`]'s live counters, readable **without taking its lock**.
///
/// Separate from the registry, and behind an `Arc` the owner keeps outside its mutex, for one
/// reason: a metrics scrape must not be able to stall behind the data path, and it must not be
/// able to make a counter go backwards either. The alternative in use before this — a
/// `try_lock` that reports `0` when a delivery holds the lock — is acceptable for a gauge that
/// dips for microseconds and *not* acceptable for a cumulative eviction count, which a
/// Prometheus counter reset would read as a restart.
#[derive(Debug, Default)]
pub struct ClientRegistryCounters {
    /// Entries held right now. Maintained on every insert and removal, so it needs no scan.
    entries: AtomicUsize,
    /// Cumulative entries evicted because the capacity was reached.
    evicted_cap: AtomicU64,
    /// Cumulative entries evicted because they went idle past the TTL.
    evicted_ttl: AtomicU64,
    /// Cumulative entries removed by name.
    evicted_explicit: AtomicU64,
}

impl ClientRegistryCounters {
    /// A consistent-enough snapshot for a scrape: four relaxed loads, which can straddle one
    /// insert. That is the same tolerance every other gauge in this transport is read with, and
    /// the alternative is a lock on the scrape path.
    #[must_use]
    pub fn snapshot(&self) -> ClientRegistryStats {
        ClientRegistryStats {
            entries: self.entries.load(Ordering::Relaxed),
            evicted_cap: self.evicted_cap.load(Ordering::Relaxed),
            evicted_ttl: self.evicted_ttl.load(Ordering::Relaxed),
            evicted_explicit: self.evicted_explicit.load(Ordering::Relaxed),
        }
    }

    /// Count one eviction under `reason`.
    fn record(&self, reason: EvictionReason) {
        let counter = match reason {
            EvictionReason::Cap => &self.evicted_cap,
            EvictionReason::Ttl => &self.evicted_ttl,
            EvictionReason::Explicit => &self.evicted_explicit,
        };
        counter.fetch_add(1, Ordering::Relaxed);
    }
}

/// What one [`ClientRegistry`] holds and has shed, as the daemon's metrics layer publishes it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ClientRegistryStats {
    /// Entries held right now — the gauge a cap is sized against.
    pub entries: usize,
    /// Cumulative capacity evictions. Expected to stay 0; see [`EvictionReason::Cap`].
    pub evicted_cap: u64,
    /// Cumulative idle-TTL evictions. Expected to climb on a node serving short-lived jobs.
    pub evicted_ttl: u64,
    /// Cumulative removals by name.
    pub evicted_explicit: u64,
}

impl ClientRegistryStats {
    /// The cumulative count for one reason — so a caller can drive a labelled counter from
    /// [`EvictionReason::all`] instead of naming three fields.
    #[must_use]
    pub fn evicted(&self, reason: EvictionReason) -> u64 {
        match reason {
            EvictionReason::Cap => self.evicted_cap,
            EvictionReason::Ttl => self.evicted_ttl,
            EvictionReason::Explicit => self.evicted_explicit,
        }
    }
}

/// One entry: the value, and when the endpoint was last addressed.
struct Entry<V> {
    value: V,
    /// Refreshed on every insert and every lookup that is not explicitly a peek. It answers
    /// **both** questions this registry asks — which entry is least recently used, and which
    /// has gone idle — which is why there is no second monotonic tick beside it (unlike
    /// `pacer_client::handles::Entry`, whose map has no notion of age). `Instant` cannot go
    /// backwards; two entries tying to the nanosecond would make either an equally correct
    /// eviction victim.
    used: Instant,
}

/// Per-client state, bounded by a capacity with least-recently-used eviction **and** an idle
/// TTL.
///
/// Not internally locked, deliberately: its three owners already hold different kinds of mutex
/// (one `tokio::sync::Mutex`, because building an `ibv_ah` happens under it; two
/// `std::sync::Mutex`, because a hash lookup never awaits), and a second lock inside would
/// either duplicate theirs or force them all onto one flavour. Every mutating method therefore
/// takes `&mut self`.
///
/// # The eviction-safety invariant
///
/// **A value in this registry may be destroyed at any point after it is evicted, so a value
/// whose destruction is unsafe while hardware is using it must be held by reference-count, not
/// by this map.** The `efa` build's AH cache stores `Arc<ibverbs::AddressHandle>` for exactly
/// that reason: `AddressHandle::drop` is `ibv_destroy_ah`, and a poster clones the `Arc` and
/// hands the clone to the completion pump alongside the WRITE's source buffer, so the handle
/// outlives its work request on every path (completion reaped, deadline elapsed, waiter
/// dropped). Evicting therefore drops the *map's* reference only and can never destroy a handle
/// a work request is using. Two further defences make the case belt-and-braces: the victim is
/// the least recently *used* entry, and an endpoint with a WRITE in flight was used
/// microseconds ago; and the TTL only expires an endpoint nothing has addressed for
/// [`DEFAULT_CLIENT_TTL`].
pub struct ClientRegistry<V> {
    entries: HashMap<ClientEndpoint, Entry<V>>,
    /// Ceiling on [`Self::len`]. Never zero: [`Self::with_limits`] clamps it, because a
    /// registry of zero would evict every entry the instant it was inserted.
    capacity: usize,
    /// How long an entry may go unaddressed before it is dropped.
    ttl: Duration,
    /// Shared with the owner, which keeps a clone outside its mutex so a scrape needs no lock.
    counters: Arc<ClientRegistryCounters>,
    /// Whether the first capacity eviction has been warned about, so a registry permanently
    /// over its cap does not bury a delivery's own diagnostics under one line per endpoint.
    warned: bool,
}

impl<V> Default for ClientRegistry<V> {
    fn default() -> Self {
        Self::new()
    }
}

impl<V> ClientRegistry<V> {
    /// A registry bounded by [`max_clients`] and [`client_ttl`] — what every client-edge map
    /// gets.
    #[must_use]
    pub fn new() -> Self {
        Self::with_limits(max_clients(), client_ttl())
    }

    /// A registry with explicit limits, `capacity` clamped to at least one entry.
    ///
    /// The clamp is not defensive tidiness: a capacity of zero would evict every entry
    /// immediately after inserting it, so every WRITE would find no address handle and the node
    /// would look broken in a way that reads as a fabric fault. Public so a test can drive
    /// eviction at a capacity of 2 and a TTL of milliseconds rather than 2048 and 300 s.
    #[must_use]
    pub fn with_limits(capacity: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            capacity: capacity.max(1),
            ttl,
            counters: Arc::new(ClientRegistryCounters::default()),
            warned: false,
        }
    }

    /// The counters, for the owner to keep outside its mutex and hand to the metrics layer.
    #[must_use]
    pub fn counters(&self) -> Arc<ClientRegistryCounters> {
        Arc::clone(&self.counters)
    }

    /// The capacity this registry was built with.
    #[must_use]
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The idle TTL this registry was built with.
    #[must_use]
    pub fn ttl(&self) -> Duration {
        self.ttl
    }

    /// Entries held right now.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is held yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The value for `endpoint`, refreshing its recency and its idle deadline; `None` if it was
    /// never held or has gone idle past the TTL (in which case it is dropped here, which is what
    /// makes expiry observable without a background task).
    pub fn get(&mut self, endpoint: &ClientEndpoint) -> Option<&V> {
        self.touch(endpoint).map(|entry| &entry.value)
    }

    /// As [`Self::get`], mutably — for a value the caller updates in place.
    pub fn get_mut(&mut self, endpoint: &ClientEndpoint) -> Option<&mut V> {
        self.touch(endpoint).map(|entry| &mut entry.value)
    }

    /// The value for `endpoint` **without touching recency or the TTL deadline**.
    ///
    /// Read-only on purpose: it answers "do I hold this?", and a query that promoted its
    /// subject would make the eviction order depend on who asked rather than on who was
    /// written to. It still reports an expired entry as absent — but leaves it in place, since
    /// removing under `&self` is not possible and a peek must not be the thing that reaps.
    #[must_use]
    pub fn peek(&self, endpoint: &ClientEndpoint) -> Option<&V> {
        self.entries
            .get(endpoint)
            .filter(|entry| entry.used.elapsed() <= self.ttl)
            .map(|entry| &entry.value)
    }

    /// Whether `endpoint` is held and live, without touching recency ([`Self::peek`]).
    #[must_use]
    pub fn holds(&self, endpoint: &ClientEndpoint) -> bool {
        self.peek(endpoint).is_some()
    }

    /// Insert or replace `endpoint`'s value, making room first. Returns the value it displaced.
    ///
    /// A replacement is the recycled-queue-pair case: a fresh process handed the same `(gid,
    /// qpn)` gets a fresh value, and the stale one is returned so the caller can drop it where
    /// it chooses.
    pub fn insert(&mut self, endpoint: ClientEndpoint, value: V) -> Option<V> {
        // Sweep before capping: an expired entry is a free slot, and taking it costs a departed
        // client's record rather than a live client's.
        self.sweep_expired();
        if !self.entries.contains_key(&endpoint) {
            self.make_room();
        }
        let displaced = self.entries.insert(
            endpoint,
            Entry {
                value,
                used: Instant::now(),
            },
        );
        self.counters
            .entries
            .store(self.entries.len(), Ordering::Relaxed);
        displaced.map(|entry| entry.value)
    }

    /// The value for `endpoint`, building and inserting one if it is absent or expired.
    ///
    /// `build` runs at most once, and only when there is nothing live to return — the whole
    /// point on the AH path, where building is a firmware admin command. A `build` failure
    /// inserts nothing.
    ///
    /// # Errors
    ///
    /// Whatever `build` returns, unchanged: the caller decides whether an unbuildable endpoint
    /// is a decline or a failure.
    ///
    /// # Panics
    ///
    /// Never in practice: the entry is either just touched or just inserted, and `capacity >= 1`
    /// means an insert cannot evict what it inserted (`make_room` runs *before* it). A panic here
    /// would mean that invariant was broken, not that a client went away.
    pub fn get_or_try_insert_with<F, E>(
        &mut self,
        endpoint: ClientEndpoint,
        build: F,
    ) -> Result<&mut V, E>
    where
        F: FnOnce() -> Result<V, E>,
    {
        if self.touch(&endpoint).is_none() {
            self.insert(endpoint, build()?);
        }
        Ok(self
            .entries
            .get_mut(&endpoint)
            .map(|entry| &mut entry.value)
            .expect("ClientRegistry invariant: just inserted or just touched"))
    }

    /// The value for `endpoint`, inserting `V::default()` if it is absent or expired.
    ///
    /// The gate case: a first-contact mutex has nothing to fail at, so an infallible sibling of
    /// [`Self::get_or_try_insert_with`] keeps the caller from having to name an error type it
    /// cannot produce.
    ///
    /// # Panics
    ///
    /// Never in practice, for [`Self::get_or_try_insert_with`]'s reason.
    pub fn get_or_insert_default(&mut self, endpoint: ClientEndpoint) -> &mut V
    where
        V: Default,
    {
        if self.touch(&endpoint).is_none() {
            self.insert(endpoint, V::default());
        }
        self.entries
            .get_mut(&endpoint)
            .map(|entry| &mut entry.value)
            .expect("ClientRegistry invariant: just inserted or just touched")
    }

    /// Remove `endpoint` by name, counting an [`EvictionReason::Explicit`] eviction. Returns the
    /// value if one was held, so the caller can distinguish "the record was stale and is now
    /// gone" from "we never held one, and something else is wrong".
    pub fn remove(&mut self, endpoint: &ClientEndpoint) -> Option<V> {
        self.take(endpoint, EvictionReason::Explicit)
    }

    /// Drop every entry that has gone unaddressed for longer than the TTL. Returns how many
    /// went.
    ///
    /// Called from [`Self::insert`] — i.e. once per newly seen endpoint — because that is when
    /// room is needed, and a registry nothing is inserting into is not growing. It is public so
    /// a periodic caller can also reap a node whose clients have all gone home; without one, a
    /// departed client's last entries persist until the next insert, bounded but resident.
    pub fn sweep_expired(&mut self) -> usize {
        let ttl = self.ttl;
        let expired: Vec<ClientEndpoint> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.used.elapsed() > ttl)
            .map(|(endpoint, _)| *endpoint)
            .collect();
        for endpoint in &expired {
            self.take(endpoint, EvictionReason::Ttl);
        }
        expired.len()
    }

    /// Refresh `endpoint`'s recency and idle deadline, dropping it instead if it has expired.
    fn touch(&mut self, endpoint: &ClientEndpoint) -> Option<&mut Entry<V>> {
        let expired = self
            .entries
            .get(endpoint)
            .is_some_and(|entry| entry.used.elapsed() > self.ttl);
        if expired {
            self.take(endpoint, EvictionReason::Ttl);
            return None;
        }
        let entry = self.entries.get_mut(endpoint)?;
        entry.used = Instant::now();
        Some(entry)
    }

    /// Remove one entry, counting it under `reason` and keeping the entry gauge honest.
    fn take(&mut self, endpoint: &ClientEndpoint, reason: EvictionReason) -> Option<V> {
        let entry = self.entries.remove(endpoint)?;
        self.counters
            .entries
            .store(self.entries.len(), Ordering::Relaxed);
        self.counters.record(reason);
        Some(entry.value)
    }

    /// Evict least-recently-used entries until one more will fit.
    ///
    /// `O(len)` per eviction, deliberately — the same trade `pacer_client::handles::HandleCache`
    /// makes at the other end of this bound: eviction is the *abnormal* path (it means the cap
    /// was smaller than the concurrent working set), a scan of at most a few thousand map
    /// entries is nanoseconds, and it buys not carrying a second index that could disagree with
    /// the map about what is held. An intrusive order list would make the common path — a single
    /// hash lookup — more expensive to keep correct.
    fn make_room(&mut self) {
        let mut gone = 0_usize;
        while self.entries.len() >= self.capacity {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(endpoint, _)| *endpoint)
            else {
                // Unreachable: a non-empty map has a minimum, and `capacity >= 1` means an empty
                // map never enters the loop. Breaking rather than panicking keeps a delivery
                // degraded instead of aborting a daemon that was serving.
                break;
            };
            self.take(&victim, EvictionReason::Cap);
            gone += 1;
        }
        if gone > 0 && !self.warned {
            self.warned = true;
            warn!(
                capacity = self.capacity,
                ttl_secs = self.ttl.as_secs(),
                "evicting client-edge state at its capacity: the endpoint set outgrew the cap, \
                 so an endpoint that is still being written to can lose its record and pay first \
                 contact again mid-request. Raise {MAX_CLIENTS_ENV} above this node's ranks x \
                 windows x rails"
            );
        }
    }
}

/// The capacity every client-edge registry is built with, resolved once per process.
///
/// Cached for the reason `pacer_client::handles::max_handles` is: all three registries and all
/// 32 rails are sized from it, and two different answers within one process would mean two maps
/// of one population disagreeing about their own ceiling.
#[must_use]
pub fn max_clients() -> usize {
    static RESOLVED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| parse_max_clients(std::env::var(MAX_CLIENTS_ENV).ok().as_deref()))
}

/// The idle TTL every client-edge registry is built with, resolved once per process (same
/// reason as [`max_clients`]).
#[must_use]
pub fn client_ttl() -> Duration {
    static RESOLVED: std::sync::OnceLock<Duration> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| parse_client_ttl(std::env::var(CLIENT_TTL_ENV).ok().as_deref()))
}

/// The decision [`max_clients`] caches, as a pure function of the environment's value.
///
/// Separate so it is testable: `max_clients` resolves once per process, so a test driving it
/// through the environment could only ever assert one case, and which one would depend on test
/// ordering.
fn parse_max_clients(raw: Option<&str>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_MAX_CLIENTS;
    };
    match raw.trim().parse::<usize>() {
        Ok(n) if (MIN_MAX_CLIENTS..=MAX_MAX_CLIENTS).contains(&n) => n,
        // An out-of-range number and a non-number are the same case: the operator's intent
        // cannot be honoured, and a daemon that mistyped an env var should still serve.
        _ => {
            warn!(
                value = raw,
                default = DEFAULT_MAX_CLIENTS,
                min = MIN_MAX_CLIENTS,
                max = MAX_MAX_CLIENTS,
                "{MAX_CLIENTS_ENV} is not an endpoint count in range; using the default"
            );
            DEFAULT_MAX_CLIENTS
        }
    }
}

/// The decision [`client_ttl`] caches, as a pure function of the environment's value (same
/// reason for the split as [`parse_max_clients`]).
fn parse_client_ttl(raw: Option<&str>) -> Duration {
    let Some(raw) = raw else {
        return DEFAULT_CLIENT_TTL;
    };
    match raw.trim().parse::<u64>() {
        Ok(secs) if (MIN_CLIENT_TTL_SECS..=MAX_CLIENT_TTL_SECS).contains(&secs) => {
            Duration::from_secs(secs)
        }
        _ => {
            warn!(
                value = raw,
                default_secs = DEFAULT_CLIENT_TTL.as_secs(),
                min_secs = MIN_CLIENT_TTL_SECS,
                max_secs = MAX_CLIENT_TTL_SECS,
                "{CLIENT_TTL_ENV} is not a whole number of seconds in range; using the default"
            );
            DEFAULT_CLIENT_TTL
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        client_ttl, max_clients, parse_client_ttl, parse_max_clients, ClientEndpoint,
        ClientRegistry, EvictionReason, DEFAULT_CLIENT_TTL, DEFAULT_MAX_CLIENTS,
        MAX_CLIENT_TTL_SECS, MAX_MAX_CLIENTS, MIN_CLIENT_TTL_SECS, MIN_MAX_CLIENTS,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    /// A TTL long enough that nothing expires while a test runs, for the cases that are about
    /// the capacity alone. Not [`DEFAULT_CLIENT_TTL`], so a change to the shipped default cannot
    /// silently turn one of those into a TTL test.
    const NO_EXPIRY: Duration = Duration::from_secs(3600);

    /// An endpoint whose GID differs in more than its last byte, so a registry keying on a
    /// prefix cannot pass by accident.
    fn endpoint(i: u8) -> ClientEndpoint {
        let mut gid = [0u8; 16];
        for (j, b) in gid.iter_mut().enumerate() {
            *b = i.wrapping_mul(17).wrapping_add(j as u8);
        }
        ClientEndpoint::new(gid, 16_384 + u32::from(i))
    }

    /// A stand-in value that reports its own destruction, so eviction can be observed as the
    /// thing it really is on the `efa` build — `ibv_destroy_ah` — rather than only as a
    /// shrinking length.
    struct Tracked {
        destroyed: Arc<AtomicUsize>,
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.destroyed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// At the capacity the least-recently-used entry goes, its value is really destroyed, and the
    /// eviction is counted under `cap` — that counter being the alarm that the cap was smaller
    /// than the concurrent working set.
    #[test]
    fn insert_past_the_cap_evicts_the_least_recently_used() {
        let destroyed = Arc::new(AtomicUsize::new(0));
        let mut r: ClientRegistry<Tracked> = ClientRegistry::with_limits(2, NO_EXPIRY);
        let counters = r.counters();
        let value = || Tracked {
            destroyed: Arc::clone(&destroyed),
        };
        r.insert(endpoint(0), value());
        r.insert(endpoint(1), value());
        assert_eq!(r.len(), 2);
        assert_eq!(counters.snapshot().entries, 2);
        assert_eq!(destroyed.load(Ordering::Relaxed), 0);

        r.insert(endpoint(2), value());
        assert_eq!(r.len(), 2, "the cap is never exceeded");
        assert!(!r.holds(&endpoint(0)), "the least recently used went");
        assert!(r.holds(&endpoint(1)));
        assert!(r.holds(&endpoint(2)));
        assert_eq!(
            destroyed.load(Ordering::Relaxed),
            1,
            "eviction must DESTROY the value, which is the hazard the cap exists to make safe"
        );
        let stats = counters.snapshot();
        assert_eq!(stats.entries, 2);
        assert_eq!(stats.evicted(EvictionReason::Cap), 1);
        assert_eq!(stats.evicted(EvictionReason::Ttl), 0);
        assert_eq!(stats.evicted(EvictionReason::Explicit), 0);
    }

    /// A lookup refreshes recency, so an endpoint that is still being written to is never the
    /// victim — the property that makes capacity eviction safe for an in-flight WRITE.
    #[test]
    fn a_lookup_refreshes_recency() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(2, NO_EXPIRY);
        r.insert(endpoint(0), 0);
        r.insert(endpoint(1), 1);
        assert_eq!(r.get(&endpoint(0)).copied(), Some(0), "touches endpoint 0");
        r.insert(endpoint(2), 2);
        assert!(r.holds(&endpoint(0)), "the touched entry survived");
        assert!(!r.holds(&endpoint(1)), "the untouched one went instead");
    }

    /// And a peek must not reorder anything: if it promoted its subject, the eviction order
    /// would depend on who asked rather than on who was written to.
    #[test]
    fn a_peek_does_not_touch_the_order() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(2, NO_EXPIRY);
        r.insert(endpoint(0), 0);
        r.insert(endpoint(1), 1);
        assert_eq!(r.peek(&endpoint(0)).copied(), Some(0));
        assert!(r.holds(&endpoint(0)));
        r.insert(endpoint(2), 2);
        assert!(!r.holds(&endpoint(0)), "the query must not have saved it");
    }

    /// A prefetch far beyond the cap never exceeds it, holds the LAST `capacity` endpoints named,
    /// and accounts for every eviction.
    #[test]
    fn many_endpoints_stay_bounded() {
        const CAP: usize = 8;
        const NAMED: u8 = 64;
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(CAP, NO_EXPIRY);
        let counters = r.counters();
        for i in 0..NAMED {
            r.insert(endpoint(i), i);
        }
        assert_eq!(r.len(), CAP);
        assert_eq!(
            counters.snapshot().evicted(EvictionReason::Cap),
            u64::from(NAMED) - CAP as u64,
            "everything past the cap must be accounted for"
        );
        for i in (NAMED - CAP as u8)..NAMED {
            assert!(r.holds(&endpoint(i)), "endpoint {i} should have survived");
        }
        assert!(!r.holds(&endpoint(0)), "the first named must have gone");
    }

    /// An entry idle past the TTL is gone on the next access, counted under `ttl` — this is what
    /// drops a client that vanished without unregistering while the cap is nowhere near reached.
    #[test]
    fn an_idle_entry_expires_on_the_next_access() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, Duration::ZERO);
        let counters = r.counters();
        r.insert(endpoint(0), 7);
        assert_eq!(counters.snapshot().entries, 1);
        // A zero TTL means the entry is idle-expired the moment any time has passed, which is
        // the boundary case: the comparison is `>`, not `>=`, so a same-instant read keeps it.
        std::thread::sleep(Duration::from_millis(2));
        assert_eq!(r.get(&endpoint(0)), None, "an idle entry is not returned");
        assert_eq!(r.len(), 0, "and it is dropped, not merely hidden");
        let stats = counters.snapshot();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.evicted(EvictionReason::Ttl), 1);
        assert_eq!(
            stats.evicted(EvictionReason::Cap),
            0,
            "an idle expiry must not be filed as a capacity eviction"
        );
    }

    /// A sweep drops every idle entry at once and leaves the live ones, which is what makes the
    /// TTL a reaper rather than only a lookup filter.
    #[test]
    fn a_sweep_drops_the_idle_and_keeps_the_live() {
        let ttl = Duration::from_millis(30);
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, ttl);
        let counters = r.counters();
        r.insert(endpoint(0), 0);
        r.insert(endpoint(1), 1);
        std::thread::sleep(ttl * 2);
        // Endpoint 2 arrives after the others went idle; inserting it sweeps them, which is the
        // path that makes room for a new client out of departed ones rather than live ones.
        r.insert(endpoint(2), 2);
        assert_eq!(r.len(), 1, "only the newcomer is left");
        assert!(r.holds(&endpoint(2)));
        assert_eq!(counters.snapshot().evicted(EvictionReason::Ttl), 2);
        // And a sweep with nothing idle removes nothing.
        assert_eq!(r.sweep_expired(), 0);
    }

    /// Touching an entry restarts its idle clock: the TTL is an *idle* timeout, not a lifetime,
    /// or a client in the middle of a long restore would lose its own record.
    #[test]
    fn touching_an_entry_restarts_the_idle_clock() {
        let ttl = Duration::from_millis(60);
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, ttl);
        r.insert(endpoint(0), 0);
        for _ in 0..4 {
            std::thread::sleep(ttl / 3);
            assert!(
                r.get(&endpoint(0)).is_some(),
                "an endpoint addressed inside the TTL must never expire"
            );
        }
    }

    /// Explicit removal is its own reason, so a record dropped on evidence
    /// (`Announcer::forget` after an UNKNOWN_PEER completion) is never read as the cap being
    /// too small.
    #[test]
    fn explicit_removal_is_counted_separately() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, NO_EXPIRY);
        let counters = r.counters();
        r.insert(endpoint(0), 0);
        assert_eq!(r.remove(&endpoint(0)), Some(0));
        assert_eq!(
            r.remove(&endpoint(0)),
            None,
            "a second removal held nothing"
        );
        let stats = counters.snapshot();
        assert_eq!(stats.entries, 0);
        assert_eq!(stats.evicted(EvictionReason::Explicit), 1);
        assert_eq!(stats.evicted(EvictionReason::Cap), 0);
        assert_eq!(stats.evicted(EvictionReason::Ttl), 0);
    }

    /// The recycled-queue-pair case: a fresh process handed the same `(gid, qpn)` must REPLACE
    /// the dead one's value, and the stale value must be destroyed — not kept beside it, and not
    /// short-circuited into reuse. Measured hazard: 2026-08-25, a loader inherited `qpn=49153`
    /// and every WRITE to it completed UNKNOWN_PEER because a stale record was reused.
    #[test]
    fn re_registering_the_same_endpoint_replaces_its_value() {
        let destroyed = Arc::new(AtomicUsize::new(0));
        let mut r: ClientRegistry<Tracked> = ClientRegistry::with_limits(1024, NO_EXPIRY);
        let counters = r.counters();
        r.insert(
            endpoint(0),
            Tracked {
                destroyed: Arc::clone(&destroyed),
            },
        );
        let displaced = r.insert(
            endpoint(0),
            Tracked {
                destroyed: Arc::clone(&destroyed),
            },
        );
        assert!(displaced.is_some(), "the stale value is handed back");
        drop(displaced);
        assert_eq!(destroyed.load(Ordering::Relaxed), 1);
        assert_eq!(r.len(), 1, "a replacement is not a second entry");
        let stats = counters.snapshot();
        assert_eq!(stats.entries, 1);
        assert_eq!(
            stats.evicted(EvictionReason::Cap) + stats.evicted(EvictionReason::Ttl),
            0,
            "a replacement is not an eviction"
        );
    }

    /// ⚠ The inverse, and the trap: the SAME GID under a DIFFERENT queue-pair number is a live
    /// multi-window client, not a restart. Every `Client::open` brings up its own queue pairs,
    /// so a registry that replaced same-GID entries would destroy the handles of a rank that is
    /// mid-request. Both must be held.
    #[test]
    fn the_same_gid_under_a_new_queue_pair_is_a_second_endpoint() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, NO_EXPIRY);
        let first = endpoint(0);
        let second_window = ClientEndpoint::new(first.gid, first.qpn + 1);
        r.insert(first, 1);
        r.insert(second_window, 2);
        assert_eq!(r.len(), 2, "two windows of one rank are two endpoints");
        assert_eq!(r.peek(&first).copied(), Some(1));
        assert_eq!(r.peek(&second_window).copied(), Some(2));
    }

    /// The build closure runs once per endpoint and not at all for one already held — the whole
    /// point on the AH path, where building is a firmware admin command that would otherwise sit
    /// on the per-chunk path.
    #[test]
    fn a_built_value_is_built_once_and_a_failure_inserts_nothing() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, NO_EXPIRY);
        let built = AtomicUsize::new(0);
        for _ in 0..2 {
            r.get_or_try_insert_with(endpoint(0), || {
                built.fetch_add(1, Ordering::Relaxed);
                Ok::<u8, &str>(1)
            })
            .expect("an infallible build");
        }
        assert_eq!(
            built.load(Ordering::Relaxed),
            1,
            "the second call reused it"
        );

        let failed = r.get_or_try_insert_with(endpoint(1), || Err::<u8, &str>("no route"));
        assert_eq!(failed.err(), Some("no route"));
        assert!(!r.holds(&endpoint(1)), "a failed build caches nothing");
        assert_eq!(r.len(), 1);
    }

    /// An expired entry is rebuilt rather than returned, which is what makes the TTL cover the
    /// recycled-queue-pair case that no evidence reached: the record is stale, nothing wrote to
    /// it, and the next delivery gets a fresh value.
    #[test]
    fn an_expired_entry_is_rebuilt() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, Duration::ZERO);
        r.insert(endpoint(0), 1);
        std::thread::sleep(Duration::from_millis(2));
        let fresh = r
            .get_or_try_insert_with(endpoint(0), || Ok::<u8, &str>(2))
            .copied();
        assert_eq!(fresh, Ok(2), "the stale value must not be handed back");
        assert_eq!(r.len(), 1);
    }

    /// The infallible sibling behaves the same way, and is what the first-contact gate uses.
    #[test]
    fn a_default_value_is_inserted_once() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(1024, NO_EXPIRY);
        *r.get_or_insert_default(endpoint(0)) = 5;
        assert_eq!(*r.get_or_insert_default(endpoint(0)), 5, "not re-defaulted");
        assert_eq!(r.len(), 1);
    }

    /// A capacity of zero would destroy every value the instant it was inserted, so it is
    /// clamped rather than accepted — the same reasoning that clamps the client's handle bound.
    #[test]
    fn a_zero_capacity_is_clamped_to_one() {
        let mut r: ClientRegistry<u8> = ClientRegistry::with_limits(0, NO_EXPIRY);
        assert_eq!(r.capacity(), 1);
        r.insert(endpoint(0), 7);
        assert_eq!(r.len(), 1, "one entry must survive its own insertion");
    }

    /// The default gate: an unset environment resolves to the derived default, not to a clamp,
    /// and the process-wide resolution agrees with it.
    #[test]
    fn the_defaults_are_what_an_unset_environment_gets() {
        assert_eq!(parse_max_clients(None), DEFAULT_MAX_CLIENTS);
        assert_eq!(parse_client_ttl(None), DEFAULT_CLIENT_TTL);
        // The environment is not set in this suite, so the cached resolution is the default —
        // which also pins that `max_clients`/`client_ttl` read the vars this module documents.
        assert_eq!(max_clients(), DEFAULT_MAX_CLIENTS);
        assert_eq!(client_ttl(), DEFAULT_CLIENT_TTL);
        let default_registry: ClientRegistry<u8> = ClientRegistry::new();
        assert_eq!(
            default_registry.capacity(),
            DEFAULT_MAX_CLIENTS,
            "a default registry is the resolved cap, not an ad-hoc one"
        );
        assert_eq!(default_registry.ttl(), DEFAULT_CLIENT_TTL);
    }

    /// Both overrides are honoured in range and fall back outside it, including the values a
    /// YAML env entry produces (trailing newline) and the ones that would break rather than
    /// degrade (zero, negative, non-numeric).
    #[test]
    fn the_overrides_are_honoured_or_fall_back() {
        assert_eq!(parse_max_clients(Some("4096")), 4096);
        assert_eq!(parse_max_clients(Some(" 4096 \n")), 4096);
        assert_eq!(parse_max_clients(Some("32")), MIN_MAX_CLIENTS);
        assert_eq!(parse_max_clients(Some("65536")), MAX_MAX_CLIENTS);
        for bad in ["0", "1", "31", "-1", "wat", "", "  ", "65537", "2048.5"] {
            assert_eq!(
                parse_max_clients(Some(bad)),
                DEFAULT_MAX_CLIENTS,
                "{bad:?} should have fallen back"
            );
        }
        assert_eq!(parse_client_ttl(Some("60")), Duration::from_secs(60));
        assert_eq!(parse_client_ttl(Some(" 60 \n")), Duration::from_secs(60));
        assert_eq!(
            parse_client_ttl(Some("1")),
            Duration::from_secs(MIN_CLIENT_TTL_SECS)
        );
        assert_eq!(
            parse_client_ttl(Some("86400")),
            Duration::from_secs(MAX_CLIENT_TTL_SECS)
        );
        for bad in ["0", "-1", "wat", "", "  ", "86401", "300.5", "5m"] {
            assert_eq!(
                parse_client_ttl(Some(bad)),
                DEFAULT_CLIENT_TTL,
                "{bad:?} should have fallen back"
            );
        }
    }

    /// Eviction labels are a metric dimension, so they must be distinct, non-empty and complete
    /// — a duplicate would silently merge two reasons into one series.
    #[test]
    fn every_eviction_reason_has_its_own_label() {
        let mut labels: Vec<&str> = EvictionReason::all().iter().map(|r| r.label()).collect();
        assert_eq!(labels.len(), 3, "all() must name every variant");
        labels.sort_unstable();
        let count = labels.len();
        labels.dedup();
        assert_eq!(labels.len(), count, "duplicate eviction label: {labels:?}");
        assert!(labels.iter().all(|l| !l.is_empty()));
    }

    /// A token rail names an endpoint, and the rkey is deliberately not part of it: one endpoint
    /// serves many windows, so folding the capability into the identity would mint one address
    /// handle per window.
    #[test]
    fn a_token_rail_names_an_endpoint_without_its_rkey() {
        let rail = crate::token::TokenRail {
            gid: [0xfe; 16],
            qpn: 49_153,
            rkey: 7,
        };
        let other_window = crate::token::TokenRail { rkey: 9, ..rail };
        assert_eq!(
            ClientEndpoint::from(&rail),
            ClientEndpoint::from(&other_window)
        );
        assert_eq!(ClientEndpoint::from(&rail).qpn, 49_153);
    }
}
