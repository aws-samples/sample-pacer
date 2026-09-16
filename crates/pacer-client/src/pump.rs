//! The announce pump: the thread that makes this client *writable*.
//!
//! ADR-0030 point 2, from the client's side. A writer installs itself in the client's
//! address table with a two-sided SEND before its first WRITE, because a target holding no
//! address handle for the initiator refuses the WRITE with `UNKNOWN_PEER` and the bytes do
//! not land. So between an announce arriving and the WRITE that follows it, *something* in
//! this process has to reap a completion, decode the message and call `ibv_create_ah`.
//!
//! That something cannot be the loader's own thread: the loader is blocked inside a signed
//! GET, and the announce arrives while it waits. Hence a thread — the one piece of this
//! library that has to be running for a delivery to work at all, and the reason a client is
//! a live object rather than a function that renders a string.
//!
//! Two properties worth stating because their absence is silent:
//!
//! * **A receive must always be posted.** A SEND arriving at an empty receive queue does not
//!   fail; it *hangs the sender* (`bench/ladder/results/c2-announce-gate.md`). Each slot is
//!   re-posted the moment its completion is reaped — so the ring is a burst tolerance, and
//!   running out of it is a **delivery depth ceiling** (`endpoint::DEFAULT_RECV_SLOTS`).
//!   Because a drained ring shows up on the *writer* as a timed-out announce and produces no
//!   event here at all, this module publishes [`PumpState::peak_burst`] so the ceiling is
//!   attributable from the client's side instead of only inferable from the daemon's.
//! * **Address handles live in a bounded cache this thread does not own.** A handle dropped
//!   while a WRITE is in flight is the `UNKNOWN_PEER` case again, so the set was originally
//!   never evicted at all. It is now [`crate::handles::HandleCache`], **shared with the
//!   pre-flight priming path** — which is what makes "the pre-flight got there first" a
//!   number ([`PumpState::prearmed`]) rather than a hope — and bounded, because a client that
//!   prefetches holders can no longer be trusted to name only writers that really wrote. The
//!   bound is sized to the concurrent working set precisely so the least-recently-used entry
//!   is never a writer with a WRITE in flight; see that module's header for the invariant and
//!   for why its eviction counter is an alarm.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use anyhow::{Context, Result};
use ibverbs::{AccessFlags, AddressHandle, MemoryRegion, RecvRequest};
use pacer_transport::announce::ANNOUNCE_MAX_BYTES;
use tracing::{info, warn};

use crate::endpoint::{address_handle, Endpoint};
use crate::handles::HandleCache;

/// The handle cache one rail's pump shares with the pre-flight priming path.
///
/// A `std::sync::Mutex` rather than a `tokio` one: the pump is a plain thread and the priming
/// path is a synchronous C entry point, so neither has a runtime to yield to, and the critical
/// section is a hash lookup plus (on a miss) one `ibv_create_ah`.
pub type SharedHandles = Arc<Mutex<HandleCache<AddressHandle>>>;

/// How long the pump blocks on the completion channel before looking at the stop flag.
///
/// Short enough that `close()` returns promptly, long enough that an idle client costs no
/// measurable CPU — this is an event wait on an fd, not a poll loop.
const WAIT: Duration = Duration::from_millis(250);
/// Consecutive completion-queue failures after which the pump gives up.
///
/// Not zero-tolerance: a single failed completion is worth a warning and another turn. Not
/// unbounded either: a CQ in a terminal state would otherwise spin a core forever, and a
/// pump that has stopped answering announces must be *reportable* — `Client::healthy()`
/// reads the flag this sets, so a delivery that starts failing has a legible cause.
const MAX_CONSECUTIVE_ERRORS: usize = 8;

/// Shared, observable state of a running pump.
///
/// Read by the owning [`crate::Client`] (and, through the C ABI, by a loader's own gate: an
/// arm that claims RDMA delivery should assert that announces were answered).
pub struct PumpState {
    /// Set by `Client::close` to end the loop.
    stop: AtomicBool,
    /// Announces decoded since bring-up.
    announces: AtomicU64,
    /// Address handles created **by this pump** — one per announced rail whose GID this client
    /// did not already hold, so it exceeds `announces` for a multi-rail writer.
    ///
    /// Read beside [`Self::prearmed`]: together they say whether the pre-flight is working.
    /// Every increment here is a writer the pre-flight did NOT cover, i.e. the repair path
    /// earning its keep.
    handles: AtomicU64,
    /// Rails named by an arriving announce whose handle **was already held** — the pre-flight
    /// got there first.
    ///
    /// The number that makes ADR-0030's pre-flight visible rather than merely intended. Its
    /// failure mode is silent by nature: a shim that stops priming still works, just slower,
    /// so nothing breaks and nobody looks. `prearmed == 0` after a delivery, with `handles`
    /// climbing, is that regression — the client was taught by announces, which is the old
    /// behaviour and the race that comes with it.
    prearmed: AtomicU64,
    /// `false` once the loop has exited on a terminal error.
    healthy: AtomicBool,
    /// Most announce completions reaped in a single drain — the high-water mark of how much of
    /// the receive ring was consumed at once.
    ///
    /// The one observable that speaks to the depth ceiling. A drained ring cannot be detected
    /// directly: the writer whose SEND found no receive simply waits, times out on the daemon
    /// side and is declined `not_announceable`, and **nothing arrives here to count**. But a
    /// drain that reaps the whole ring is the state immediately before that, so this reaching
    /// the ring size means the burst either hit the ceiling or came within one announce of it.
    peak_burst: AtomicU64,
    /// Slots the ring holds, so [`Self::peak_burst`] can be read against its own bound
    /// without the caller knowing how the endpoint was configured.
    recv_slots: u64,
}

impl PumpState {
    /// Announces decoded since bring-up.
    pub fn announces(&self) -> u64 {
        self.announces.load(Ordering::Relaxed)
    }

    /// Address handles built for announced rails — writers the pre-flight did not cover.
    pub fn handles(&self) -> u64 {
        self.handles.load(Ordering::Relaxed)
    }

    /// Announced rails whose handle was already held, i.e. writers the pre-flight primed. See
    /// the field's own doc for why this is the number an operator watches.
    pub fn prearmed(&self) -> u64 {
        self.prearmed.load(Ordering::Relaxed)
    }

    /// Whether the pump is still reaping. `false` means announces are no longer being
    /// answered, so deliveries into this window will start failing on the writer's side.
    pub fn healthy(&self) -> bool {
        self.healthy.load(Ordering::Relaxed)
    }

    /// High-water mark of announces reaped in one drain. See [`PumpState::peak_burst`]'s field
    /// doc for why this, and not a dropped-SEND count, is what the client can honestly report.
    pub fn peak_burst(&self) -> u64 {
        self.peak_burst.load(Ordering::Relaxed)
    }

    /// Slots the receive ring holds — [`Self::peak_burst`]'s own ceiling.
    pub fn recv_slots(&self) -> u64 {
        self.recv_slots
    }

    /// Whether the ring was ever fully drained in one pass, i.e. whether the announce burst
    /// reached the depth ceiling.
    ///
    /// **This is the check a delivery arm should assert on.** `true` beside daemon-side
    /// `pacer_delivery_declines_total{reason="not_announceable"}` identifies the ceiling as
    /// the cause and names the fix (`PACER_CLIENT_ANNOUNCE_RECV_SLOTS`); `false` beside the
    /// same declines means the client was keeping up and the cause is elsewhere.
    pub fn ring_saturated(&self) -> bool {
        self.peak_burst() >= self.recv_slots
    }
}

/// A running pump: the thread plus the state it publishes.
pub struct Pump {
    state: Arc<PumpState>,
    thread: Option<JoinHandle<()>>,
}

impl Pump {
    /// Post the receive ring and start reaping.
    ///
    /// Takes the whole [`Endpoint`] by value: after bring-up nothing else needs the queues,
    /// and moving them here is what makes the shared state a pair of counters rather than a
    /// lock over hardware handles. The protection domain is `Arc`-backed inside the ibverbs
    /// crate, so the caller's window registration keeps its own reference.
    ///
    /// `handles` is this rail's cache, **created by the caller** rather than here, because the
    /// pre-flight priming path holds the same one: a handle built by either path has to be
    /// visible to the other or the pre-flight would prime a set the pump then rebuilds.
    ///
    /// # Errors
    ///
    /// Registering the announce inbox, or the first `ibv_post_recv`, failing. A failure here
    /// means the client cannot be announced to and therefore cannot be delivered into — it
    /// is not a degradation to accept quietly.
    pub fn spawn(endpoint: Endpoint, handles: SharedHandles) -> Result<Self> {
        let mut endpoint = endpoint;
        // The QP's own value, never a fresh read of the environment: the inbox is carved into
        // exactly this many slots and the QP was built to hold exactly this many receives.
        let slots = endpoint.recv_slots;
        let state = Arc::new(PumpState {
            stop: AtomicBool::new(false),
            announces: AtomicU64::new(0),
            handles: AtomicU64::new(0),
            prearmed: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
            peak_burst: AtomicU64::new(0),
            recv_slots: slots as u64,
        });
        // `allocate` (rather than this crate's `Window`) because the inbox wants exactly
        // what it gives: an owned, registered, byte-addressable buffer on base pages. It is
        // kilobytes, it is never the delivery target, and its page size is nobody's variable.
        let inbox = endpoint
            .pd
            .allocate(ANNOUNCE_MAX_BYTES * slots, AccessFlags::LOCAL_WRITE)
            .context("registering the announce inbox")?;
        for slot in 0..slots {
            post_slot(&mut endpoint, &inbox, slot)?;
        }
        info!(
            slots,
            "announce receives posted (a client with none HANGS its writer; the ring is the \
             delivery depth ceiling)"
        );
        let thread = std::thread::Builder::new()
            .name("pacer-announce".to_owned())
            .spawn({
                let state = Arc::clone(&state);
                move || run(endpoint, inbox, &state, &handles)
            })
            .context("spawning the announce pump thread")?;
        Ok(Self {
            state,
            thread: Some(thread),
        })
    }

    /// The state a caller may observe while the pump runs.
    pub fn state(&self) -> &Arc<PumpState> {
        &self.state
    }
}

impl Drop for Pump {
    fn drop(&mut self) {
        self.state.stop.store(true, Ordering::Relaxed);
        if let Some(thread) = self.thread.take() {
            // Joined rather than detached: the thread owns the queue pair and the inbox MR,
            // and letting it outlive this value would deregister memory it is still posting
            // receives into.
            if thread.join().is_err() {
                warn!("the announce pump thread panicked");
            }
        }
    }
}

/// Post receive `slot`, scattering into its own [`ANNOUNCE_MAX_BYTES`] region of `inbox`.
///
/// # Errors
///
/// `ibv_post_recv` failing.
fn post_slot(endpoint: &mut Endpoint, inbox: &MemoryRegion<Box<[u8]>>, slot: usize) -> Result<()> {
    let at = slot * ANNOUNCE_MAX_BYTES;
    let sges = [inbox.slice(at..at + ANNOUNCE_MAX_BYTES)];
    let mut recvs = [RecvRequest::new(slot as u64, &sges)];
    // SAFETY: `inbox` is registered on this QP's protection domain and outlives every
    // receive posted against it — both are owned by the pump for its whole life, and the
    // pump is joined before either is dropped.
    unsafe { endpoint.qp.post_recv(&mut recvs) }
        .with_context(|| format!("posting announce receive {slot}"))
}

/// The pump thread's body: run the loop, then tear the rail down **in order**.
///
/// The order is the whole reason this function exists rather than the loop taking ownership.
/// A Rust local (or parameter) is dropped last-declared-first, which put the inbox's
/// `ibv_dereg_mr` **before** the queue pair's destruction — and an MR that a live QP still
/// has receives posted against answers `EINVAL`. Measured on a p5 (2026-08-24): the panic
/// inside `MemoryRegion`'s own `Drop` unwound into `ibv_dealloc_pd` returning `EBUSY`,
/// panicked again mid-unwind and **aborted the process**, which for a checkpoint loader is a
/// crash *after* a successful load — the whole result lost at exit.
fn run(
    endpoint: Endpoint,
    inbox: MemoryRegion<Box<[u8]>>,
    state: &Arc<PumpState>,
    handles: &SharedHandles,
) {
    let mut endpoint = endpoint;
    pump(&mut endpoint, &inbox, state, handles);
    // Explicit, and not reorderable by accident: the queue pair goes first, so nothing can
    // reference the inbox by the time it is deregistered. The CQ, channel and protection
    // domain need no help — the QP holds an `Arc` of each, so they outlive it by refcount.
    drop(endpoint);
    drop(inbox);
}

/// The pump loop: arm, drain, wait, drain, until stopped.
///
/// Arm-then-drain rather than drain-then-arm: `req_notify` before the poll is what makes a
/// completion that lands between the two impossible to miss.
fn pump(
    endpoint: &mut Endpoint,
    inbox: &MemoryRegion<Box<[u8]>>,
    state: &Arc<PumpState>,
    handles: &SharedHandles,
) {
    let mut errors = 0usize;
    while !state.stop.load(Ordering::Relaxed) {
        if let Err(e) = turn(endpoint, inbox, state, handles) {
            errors += 1;
            warn!(error = %format!("{e:#}"), errors, "announce pump turn failed");
            if errors >= MAX_CONSECUTIVE_ERRORS {
                state.healthy.store(false, Ordering::Relaxed);
                warn!("announce pump giving up; deliveries into this window will now fail");
                return;
            }
            continue;
        }
        errors = 0;
    }
    info!(
        announces = state.announces(),
        handles = state.handles(),
        prearmed = state.prearmed(),
        peak_burst = state.peak_burst(),
        recv_slots = state.recv_slots(),
        saturated = state.ring_saturated(),
        "announce pump stopped"
    );
}

/// One turn of the loop: arm the channel, drain what is there, block briefly, drain again.
///
/// # Errors
///
/// Arming, polling or re-posting failing, or a completion carrying a failure status. A
/// timeout is not an error — it is the ordinary case of no announce having arrived.
fn turn(
    endpoint: &mut Endpoint,
    inbox: &MemoryRegion<Box<[u8]>>,
    state: &Arc<PumpState>,
    handles: &SharedHandles,
) -> Result<()> {
    endpoint.cq.req_notify(false).context("arming the CQ")?;
    drain(endpoint, inbox, state, handles)?;
    // `Ok(None)` is a timeout, which is what an idle client mostly does.
    if endpoint
        .channel
        .wait(Some(WAIT))
        .context("waiting on the completion channel")?
        .is_some()
    {
        drain(endpoint, inbox, state, handles)?;
    }
    Ok(())
}

/// Reap every completion currently on the queue.
///
/// # Errors
///
/// The poll failing, a completion carrying a failure status, or re-posting a drained receive
/// failing.
fn drain(
    endpoint: &mut Endpoint,
    inbox: &MemoryRegion<Box<[u8]>>,
    state: &Arc<PumpState>,
    handles: &SharedHandles,
) -> Result<()> {
    // Collected before anything is posted: the completions borrow the CQ, and re-posting
    // needs `&mut endpoint`.
    let mut reaped: Vec<(usize, usize)> = Vec::new();
    {
        let Some(mut completions) = endpoint.cq.poll().context("polling the CQ")? else {
            return Ok(());
        };
        while let Some(wc) = completions.next() {
            let (slot, len) = (wc.wr_id() as usize % endpoint.recv_slots, wc.len());
            wc.ok()
                .with_context(|| format!("announce receive {slot} completed with a failure"))?;
            reaped.push((slot, len));
        }
    }
    record_burst(state, reaped.len());
    for (slot, len) in reaped {
        let at = slot * ANNOUNCE_MAX_BYTES;
        // `len` is what the sender put on the wire, which is exactly what the decoder wants:
        // it demands an EXACT length, since a message longer than its own rail count means
        // the two ends disagree about the layout.
        install(
            &inbox.bytes()[at..at + len.min(ANNOUNCE_MAX_BYTES)],
            &endpoint.pd,
            state,
            handles,
        );
        post_slot(endpoint, inbox, slot)?;
    }
    Ok(())
}

/// Raise [`PumpState::peak_burst`] if this drain was the largest yet, and say so loudly the
/// first time the ring is emptied.
///
/// Warned once per pump rather than per drain: at the ceiling every subsequent burst saturates
/// too, and a per-drain warning would bury the delivery's own diagnostics under thousands of
/// copies of this one. The counter keeps the full picture for anything that wants to assert on
/// it.
fn record_burst(state: &Arc<PumpState>, reaped: usize) {
    let reaped = reaped as u64;
    if reaped == 0 {
        return;
    }
    let previous = state.peak_burst.fetch_max(reaped, Ordering::Relaxed);
    if reaped >= state.recv_slots && previous < state.recv_slots {
        warn!(
            burst = reaped,
            slots = state.recv_slots,
            "announce receive ring fully drained in one pass — a concurrent announce beyond \
             this would find no receive posted and its delivery would be declined. Raise \
             PACER_CLIENT_ANNOUNCE_RECV_SLOTS if the daemon reports \
             pacer_delivery_declines_total{{reason=\"not_announceable\"}}"
        );
    }
}

/// Decode one announce and install every rail it names into this rail's handle cache.
///
/// The decode, the dedup and the `ibv_create_ah` all live in
/// [`crate::handles::HandleCache::install_announce`] — **not here** — because the pre-flight
/// priming path does exactly the same thing from a different source of rails, and two loops
/// that must agree about the key and the bound is how they would stop agreeing.
///
/// Never fails the pump: a message that does not decode is logged and dropped by the cache,
/// and reported here by not counting an announce. A malformed or unknown-version announce
/// means some writer will not be able to reach this client, which is the writer's problem to
/// report — while a pump that died over it would break every *other* writer too.
///
/// A poisoned cache lock is recovered from rather than propagated, for the same reason: the
/// cache holds hardware handles that are perfectly valid whatever panicked, and refusing every
/// later announce over a poison flag would take a whole window's deliveries down.
fn install(
    message: &[u8],
    pd: &ibverbs::ProtectionDomain,
    state: &Arc<PumpState>,
    handles: &SharedHandles,
) {
    let mut cache = handles
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    let Some(installed) = cache.install_announce(message, |gid| address_handle(pd, gid)) else {
        return;
    };
    state.announces.fetch_add(1, Ordering::Relaxed);
    state
        .handles
        .fetch_add(installed.created as u64, Ordering::Relaxed);
    state
        .prearmed
        .fetch_add(installed.already_held as u64, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::{record_burst, PumpState};
    use std::sync::atomic::{AtomicBool, AtomicU64};
    use std::sync::Arc;

    /// A pump's observable state with `slots` receives, without a device. Only `record_burst`
    /// is under test here — the rest of the pump needs an EFA rail.
    fn state(slots: u64) -> Arc<PumpState> {
        Arc::new(PumpState {
            stop: AtomicBool::new(false),
            announces: AtomicU64::new(0),
            handles: AtomicU64::new(0),
            prearmed: AtomicU64::new(0),
            healthy: AtomicBool::new(true),
            peak_burst: AtomicU64::new(0),
            recv_slots: slots,
        })
    }

    /// The high-water mark must keep the largest burst, not the latest — a ring that filled
    /// once during first contact and then idled is exactly the case worth reporting, and a
    /// last-value gauge would erase it by the time anyone looked.
    #[test]
    fn the_peak_is_a_high_water_mark() {
        let s = state(64);
        record_burst(&s, 5);
        record_burst(&s, 31);
        record_burst(&s, 2);
        assert_eq!(s.peak_burst(), 31);
        assert!(!s.ring_saturated(), "31 of 64 is not saturation");
    }

    /// Draining the whole ring is the reportable state: one more concurrent announce would
    /// have found no receive posted, and that announce's delivery would be declined.
    #[test]
    fn a_full_drain_is_reported_as_saturation() {
        let s = state(8);
        record_burst(&s, 8);
        assert_eq!(s.peak_burst(), 8);
        assert!(s.ring_saturated());
    }

    /// An idle pump reaps nothing, and must not read as saturated — `0 >= 0` would be true for
    /// a ring of zero, so the empty case is checked explicitly rather than left to arithmetic.
    #[test]
    fn an_idle_pump_is_not_saturated() {
        let s = state(64);
        record_burst(&s, 0);
        assert_eq!(s.peak_burst(), 0);
        assert!(!s.ring_saturated());
    }

    /// Recorded from concurrent drains without losing the maximum: `fetch_max` rather than a
    /// load-compare-store, so two rails reporting at once cannot drop the larger.
    #[test]
    fn concurrent_drains_keep_the_largest() {
        let s = state(256);
        let threads: Vec<_> = (1..=16_u64)
            .map(|n| {
                let s = Arc::clone(&s);
                std::thread::spawn(move || record_burst(&s, n as usize * 10))
            })
            .collect();
        for t in threads {
            t.join().expect("recording a burst cannot panic");
        }
        assert_eq!(s.peak_burst(), 160);
    }
}
