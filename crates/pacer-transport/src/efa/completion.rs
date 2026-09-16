//! CQ → tokio bridge: one dedicated, NUMA-pinned OS thread per rail drains that
//! rail's completion queue via a `tokio::io::unix::AsyncFd`-wrapped
//! completion-channel descriptor (arm-then-drain, no busy-poll core — the spike's
//! S7 finding, planning/09) and dispatches each completion to whichever
//! `fetch`/`serve` task is awaiting that work request's id.
//!
//! The spike's `await_completion` blocked one thread on one lockstep
//! WRITE-then-READ; the daemon needs many fetches in flight at once
//! (`fetch_parallelism`, ADR-0018 default 64), so this generalizes it to a
//! wr_id → waiter map multiplexed over the one CQ [`super::context::EfaContext`]
//! owns.
//!
//! **Why a thread per rail and not a task on a shared runtime** (planning/19 D5
//! step 0). Every in-flight WRITE is blocked on its rail's reaper being scheduled.
//! The transport-only bench that set the ~58 GiB/s bar gave each rail its own
//! reaper thread pinned to a CPU on that rail's NUMA node, and measured what
//! happens otherwise: "several rail threads pack onto shared cores and their
//! wakeup latency starves the in-flight window ... per-rail rate *collapses* as
//! rails are added". The daemon previously spawned all 32 reapers as tasks on one
//! shared multi-threaded runtime, and D4 measured that signature — 344 ms
//! completion waits against 1.6 µs posts, with 0.76 of 192 cores busy. A dedicated
//! thread with its own current-thread runtime gives each reaper an fd reactor
//! nothing else shares, and `affinity.rs` pins it to its rail's node.
//!
//! This strictly strengthens the isolation the previous design reached for (keeping
//! completion wakeups off the S3 proxy's worker threads): a reaper can no longer be
//! queued behind *any* other task, proxy or peer.

use anyhow::Result;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use ibverbs::{CompletionChannel, CompletionQueue, WcError, WcStatus};
use tokio::io::unix::AsyncFd;
use tokio::sync::oneshot;
use tracing::{error, warn};

use super::affinity::{self, RailPlacement};

/// What a completed work request resolves its waiter with: `Ok(bytes moved)`
/// mirrors the spike's per-op checks (S4/S5 done, S6 verified), `Err` carries
/// the work-completion status the caller maps to a `TransportError::Other`
/// fallback (ADR-0003: any error means fall back, never fail the client).
pub(super) type CompletionResult = Result<()>;

/// A WRITE's local source buffer, kept alive by the pump on behalf of a waiter
/// that gave up while the work request may still be outstanding in the NIC
/// (ADR-0028 § "a completion that times out must not free the frame").
///
/// Type-erased because the pump has no use for the value beyond its `Drop`, and
/// the two kinds the serve path posts from are unrelated: an ADR-0028 cache
/// frame (held by a `bytes::Bytes` clone) and an ADR-0024 staging lease. Both
/// release their memory — to the slab's free list, or to the arena — exactly
/// when this box drops, which is the whole mechanism.
pub(super) type SourceGuard = Box<dyn Send>;

/// The label for a failure that carried no work completion at all — the post was
/// rejected before reaching the wire, the completion never arrived inside
/// [`super::context::COMPLETION_TIMEOUT`], or the pump dropped the waiter.
///
/// Deliberately ONE bucket for those three. They are distinguishable only by the text of
/// an `anyhow` message, and matching on that text is exactly what the typed `WcError`
/// exists to avoid (see [`super::client_write::is_unknown_peer`]) — a label derived from a
/// message string silently re-partitions itself the next time someone rewords the message.
/// The log line beside the increment carries the full error, so nothing is lost that an
/// operator reading a spike cannot recover.
pub(super) const NO_COMPLETION_STATUS: &str = "no_completion_status";

/// The label for a status the pinned `ibverbs` revision did not have when this was written.
///
/// `WcStatus` is `#[non_exhaustive]`, so a wildcard arm is compulsory — but it gets its
/// **own** bucket rather than being folded into a named status or into
/// [`NO_COMPLETION_STATUS`]. A status nobody has classified yet is precisely the one worth
/// seeing as new; merging it into a bucket that already carries traffic would hide its first
/// appearance behind an existing rate. Its presence in a scrape is the signal to extend
/// [`status_label`].
pub(super) const UNCLASSIFIED_STATUS: &str = "unclassified_status";

/// A stable, bounded metric label for a work completion's status.
///
/// **Bounded by the enum**, which is the property that makes it safe as a Prometheus label:
/// `WcStatus` has a fixed set of variants, so this can produce at most that many series
/// (plus [`NO_COMPLETION_STATUS`] and [`UNCLASSIFIED_STATUS`]). The vendor error code is
/// deliberately NOT part of it — it is a device firmware value with no enumeration anywhere
/// in this workspace, so labelling by it would let one misbehaving NIC mint unbounded
/// series. It stays in the log line, where the full error is printed.
///
/// Spelled out variant by variant rather than grouped, because the whole purpose of the
/// label is to separate a `WorkRequestFlushed` consequence from whichever status caused the
/// queue pair to enter the error state — any grouping risks putting those two together.
pub(super) fn status_label(status: WcStatus) -> &'static str {
    match status {
        WcStatus::Success => "success",
        WcStatus::LocalLengthError => "local_length_error",
        WcStatus::LocalQpOperationError => "local_qp_operation_error",
        WcStatus::LocalEecOperationError => "local_eec_operation_error",
        WcStatus::LocalProtectionError => "local_protection_error",
        WcStatus::WorkRequestFlushed => "work_request_flushed",
        WcStatus::MemoryWindowBindError => "memory_window_bind_error",
        WcStatus::BadResponse => "bad_response",
        WcStatus::LocalAccessError => "local_access_error",
        WcStatus::RemoteInvalidRequest => "remote_invalid_request",
        WcStatus::RemoteAccessError => "remote_access_error",
        WcStatus::RemoteOperationError => "remote_operation_error",
        WcStatus::RetryExceeded => "retry_exceeded",
        WcStatus::RnrRetryExceeded => "rnr_retry_exceeded",
        WcStatus::LocalRddViolation => "local_rdd_violation",
        WcStatus::RemoteInvalidRdRequest => "remote_invalid_rd_request",
        WcStatus::RemoteAborted => "remote_aborted",
        WcStatus::InvalidEecn => "invalid_eecn",
        WcStatus::InvalidEecState => "invalid_eec_state",
        WcStatus::Fatal => "fatal",
        WcStatus::ResponseTimeout => "response_timeout",
        WcStatus::GeneralError => "general_error",
        WcStatus::TagMatchingError => "tag_matching_error",
        WcStatus::TagMatchingRendezvousIncomplete => "tag_matching_rendezvous_incomplete",
        // Compulsory: the upstream enum is `#[non_exhaustive]`. See the const.
        _ => UNCLASSIFIED_STATUS,
    }
}

/// The status label for an error that may or may not carry a work completion, i.e. the
/// classification a *caller* of the WRITE path can make from what reached it.
///
/// `None` of the `WcError` downcast means the failure happened before or instead of a
/// completion — see [`NO_COMPLETION_STATUS`].
pub(super) fn failure_label(error: &anyhow::Error) -> &'static str {
    error
        .downcast_ref::<WcError>()
        .map_or(NO_COMPLETION_STATUS, |wc| status_label(wc.status))
}

/// The pump's shared state: who is waiting, and whose source buffers it is
/// holding on their behalf.
///
/// One `Mutex` over both maps rather than one each, because the correctness
/// argument for [`CompletionPump::orphan_if_unreaped`] is precisely that
/// "remove the waiter" and "insert the orphan" happen without the reaper
/// interleaving between them. Two locks could not express that.
struct PumpState {
    /// Waiters keyed by the work-request id they posted.
    waiters: HashMap<u64, oneshot::Sender<CompletionResult>>,
    /// Source buffers whose waiter abandoned them while their WQE may still be
    /// live, keyed by the same id. The reaper drops one when its completion
    /// finally arrives; whatever is left drops with this map, i.e. when the
    /// context — and so the QP — is gone, which is itself proof no further DMA
    /// can occur.
    ///
    /// Needs no reclaim policy of its own: an entry exists only for a
    /// posted-but-unreaped WRITE, a quantity already bounded by serve admission
    /// and `PACER_RDMA_RAIL_WINDOW`.
    orphans: HashMap<u64, SourceGuard>,
}

/// Shared pump state. A `std::sync::Mutex` (not `tokio::sync::Mutex`) because
/// every access is a quick insert/remove, never held across an `.await`.
type Shared = Arc<Mutex<PumpState>>;

/// One handle per [`super::context::EfaContext`]; cloning shares the same
/// background pump, waiter map and orphaned-source map.
#[derive(Clone)]
pub struct CompletionPump {
    state: Shared,
    /// Cumulative sources this pump has taken ownership of (see
    /// [`SourceGuard`]). Surfaced as a counter by the daemon: any non-zero value
    /// means WRITEs are outliving their deadline, which is the condition that
    /// used to recycle a frame under a live DMA.
    orphaned_total: Arc<AtomicU64>,
    /// Cumulative **failed** completions no waiter ever received — the pump's blind spot,
    /// and the reason a burst of flushed work requests can have no cause anywhere in the
    /// daemon's output.
    ///
    /// A successful completion nobody wanted is unremarkable (a serve that timed out and
    /// moved on). A *failed* one is not: when a queue pair enters the error state every
    /// work request still posted on it completes `WorkRequestFlushed`, so the flushes are
    /// the consequence and the single non-flush status that preceded them is the cause. If
    /// that one completion happened to belong to a waiter that had already gone —
    /// [`CompletionPump::orphan_if_unreaped`] took it on a timeout, or its request was
    /// abandoned and dropped the receiver — [`dispatch_one`] discarded it in silence, and
    /// the operator saw only flushes.
    ///
    /// Observed 2026-09-05 on a p6-b200: four flushed work requests (1863/1864/1865/1868)
    /// reported a 500 each, with **1866 and 1867 absent from that list** — either already
    /// completed before the transition, or the swallowed cause. Nothing in the process
    /// could tell those two readings apart, which is what this counter (and the `warn!`
    /// beside it) exists to end.
    unobserved_failures: Arc<AtomicU64>,
}

impl CompletionPump {
    /// Start this rail's reaper on its own OS thread, pinned per `placement`, and
    /// return a handle callers register waiters through. `cq`/`channel` are the
    /// same pair [`super::context::EfaContext::bring_up`] built the queue pair on.
    ///
    /// The thread owns a **current-thread** tokio runtime so the completion
    /// channel's fd registers with an IO reactor nothing else shares — see the
    /// module doc for why that is the difference between the bar planning/18
    /// measured and the number D4 measured. The `AsyncFd` is built *inside* the
    /// loop, on that runtime, so this function is infallible; a failure to
    /// register surfaces from the drain loop (logged, pump exits) exactly like any
    /// later fd error.
    ///
    /// `rail` only labels the thread (`pacer-cq-<rail>`), which is what makes the
    /// placement checkable from outside the process (`ps -To comm,psr`).
    ///
    /// `cq_errors`/`rdma_healthy` are the shared handles the drain loop uses to
    /// signal its own death (A1): a terminal CQ/fd error bumps `cq_errors` (the
    /// operator's pump-death counter) and stores `false` into `rdma_healthy`,
    /// which the fetch/serve paths check up front so a dead pump degrades to
    /// gRPC at zero per-op cost rather than every WRITE waiting out
    /// [`super::context::COMPLETION_TIMEOUT`]. They live on
    /// [`super::context::EfaContext`] (built before this pump, which the
    /// context owns) and are cloned in here.
    ///
    /// The thread runs for the process's lifetime, like the context it belongs to:
    /// the loop returns only on a terminal error, and a rail is never torn down
    /// while the daemon serves. A failure to spawn the thread is logged and leaves
    /// this rail with no reaper — the same terminal state as a pump death, which
    /// `rdma_healthy` already covers.
    pub fn spawn(
        cq: CompletionQueue,
        channel: CompletionChannel,
        rail: usize,
        placement: RailPlacement,
        cq_errors: Arc<AtomicU64>,
        rdma_healthy: Arc<AtomicBool>,
    ) -> Self {
        let state: Shared = Arc::new(Mutex::new(PumpState {
            waiters: HashMap::new(),
            orphans: HashMap::new(),
        }));
        let pump = Self {
            state: Arc::clone(&state),
            orphaned_total: Arc::new(AtomicU64::new(0)),
            unobserved_failures: Arc::new(AtomicU64::new(0)),
        };
        // Cloned into the drain loop so a completion nobody is waiting for is still
        // counted; the pump handle the caller keeps reads the same atomic.
        let unobserved = Arc::clone(&pump.unobserved_failures);
        // Cloned before the closure takes ownership so the spawn-failure path
        // below can still record the death.
        let (errors, healthy) = (Arc::clone(&cq_errors), Arc::clone(&rdma_healthy));
        let spawned = std::thread::Builder::new()
            .name(format!("pacer-cq-{rail}"))
            .spawn(move || {
                affinity::pin_current_thread(placement, "completion reaper");
                // A current-thread runtime, not a handle to a shared one: this
                // reaper must never queue behind another task (module doc).
                match tokio::runtime::Builder::new_current_thread()
                    .enable_io()
                    .build()
                {
                    // Only one arm runs, so both may consume the same handles.
                    Ok(rt) => rt.block_on(drain_loop(
                        cq,
                        channel,
                        state,
                        cq_errors,
                        rdma_healthy,
                        unobserved,
                    )),
                    Err(e) => {
                        error!(error = %e, rail, "building the reaper's runtime failed; this rail has no completion pump");
                        mark_dead(&cq_errors, &rdma_healthy);
                    }
                }
            });
        if let Err(e) = spawned {
            error!(error = %e, rail, "spawning the completion reaper thread failed; this rail has no completion pump");
            // The state the drain loop would have recorded, set here because there
            // is no loop to record it: RDMA off, so fetches use gRPC at once
            // instead of each waiting out COMPLETION_TIMEOUT.
            mark_dead(&errors, &healthy);
        }
        pump
    }

    /// Register interest in `wr_id`'s completion before posting the work
    /// request that carries it — the caller must register-then-post, never
    /// the reverse, or a fast completion could arrive with no waiter to
    /// deliver it to.
    pub(super) fn register(&self, wr_id: u64) -> oneshot::Receiver<CompletionResult> {
        let (tx, rx) = oneshot::channel();
        self.state.lock().unwrap().waiters.insert(wr_id, tx);
        rx
    }

    /// Drop a stale registration (the post that would have completed it
    /// never went out, e.g. because building the send batch failed) so it
    /// cannot be confused for a live one that later reuses the same id.
    ///
    /// Only for ids whose work request never reached the wire — there is no
    /// buffer to protect, because the NIC never saw one. A waiter giving up on a
    /// WRITE that *was* posted must use [`Self::orphan_if_unreaped`] instead.
    pub(super) fn cancel(&self, wr_id: u64) {
        self.state.lock().unwrap().waiters.remove(&wr_id);
    }

    /// Hand the pump ownership of `wr_id`'s source buffer because our deadline
    /// elapsed — unless its completion has already been reaped, in which case
    /// the NIC is provably done with those bytes and the caller may free them.
    /// Returns whether the pump took it.
    ///
    /// **The waiter's presence is the test.** The reaper removes a waiter in the
    /// same critical section it delivers the completion in, and `cancel` is only
    /// ever called for a WRITE that never reached the wire — so an id still in
    /// `waiters` here means no completion has been dispatched for it, and its
    /// WQE may still be outstanding. Doing both map operations under one lock is
    /// what closes the race where a completion lands between the timeout firing
    /// and this call: either we win and the pump owns the buffer until the
    /// (already-queued) CQE arrives, or the reaper won and there is nothing left
    /// to protect.
    ///
    /// # Panics
    ///
    /// If the state mutex is poisoned — see [`Self::register`]'s critical
    /// section, which cannot panic.
    pub(super) fn orphan_if_unreaped(&self, wr_id: u64, source: SourceGuard) -> bool {
        let mut state = self.state.lock().unwrap();
        if state.waiters.remove(&wr_id).is_none() {
            // Reaped already. Release the lock BEFORE `source` drops at the end
            // of this scope: an ADR-0028 frame's drop takes the slab's free-list
            // lock, and holding two unrelated locks is how a future change grows
            // a cycle.
            drop(state);
            return false;
        }
        state.orphans.insert(wr_id, source);
        drop(state);
        self.orphaned_total.fetch_add(1, Ordering::Relaxed);
        true
    }

    /// Hand the pump ownership unconditionally, for a caller that knows its
    /// completion was never dispatched to it — its waiter's sender was dropped
    /// without a send, so no CQE can ever resolve it and the WQE's fate is
    /// unknowable from here. The bytes must stay put; the entry drops with the
    /// context (QP destroyed ⇒ no further DMA) or if a CQE for the id still
    /// arrives.
    ///
    /// # Panics
    ///
    /// If the state mutex is poisoned — see [`Self::register`].
    pub(super) fn orphan(&self, wr_id: u64, source: SourceGuard) {
        self.state.lock().unwrap().orphans.insert(wr_id, source);
        self.orphaned_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Cumulative source buffers this pump has taken ownership of, and how many
    /// it still holds. Both are read at scrape by the daemon's metrics layer;
    /// the second staying elevated means completions are not arriving at all,
    /// which is a rail-health signal rather than a memory one.
    ///
    /// # Panics
    ///
    /// If the state mutex is poisoned — see [`Self::register`].
    pub(super) fn orphan_counts(&self) -> (u64, usize) {
        (
            self.orphaned_total.load(Ordering::Relaxed),
            self.state.lock().unwrap().orphans.len(),
        )
    }

    /// Cumulative failed completions this pump delivered to nobody — see the field.
    /// Read at scrape by the daemon's metrics layer.
    pub(super) fn unobserved_failure_count(&self) -> u64 {
        self.unobserved_failures.load(Ordering::Relaxed)
    }

    /// A pump with no reaper behind it: the waiter/orphan bookkeeping, detached
    /// from the completion queue that would normally drive it.
    ///
    /// Test-only, and the reason the orphan protocol is testable at all — the
    /// live path needs an EFA device, so without this the hand-over race could
    /// only ever be argued for, never asserted. Reaping is simulated by calling
    /// [`dispatch_one`], the same function the real reaper calls.
    #[cfg(test)]
    fn detached() -> Self {
        Self {
            state: Arc::new(Mutex::new(PumpState {
                waiters: HashMap::new(),
                orphans: HashMap::new(),
            })),
            orphaned_total: Arc::new(AtomicU64::new(0)),
            unobserved_failures: Arc::new(AtomicU64::new(0)),
        }
    }
}

/// Arm, drain, wait, repeat — for the lifetime of the context — then, when the
/// pump dies, flip RDMA capability off (A1). The inner [`pump_drain`] runs
/// until a terminal CQ/fd error (which means the whole EFA context is no longer
/// usable); this wrapper then records the death exactly once.
///
/// Why flip capability off rather than restart the pump: before A1, a dead pump
/// left every subsequent WRITE to wait out [`super::context::COMPLETION_TIMEOUT`]
/// (5 s) before its completion-never-arrives error fell back to gRPC — a
/// permanent, silent 5-s-per-op tax. Storing `false` into `rdma_healthy` makes
/// the fetch/serve paths skip RDMA up front and use gRPC directly, preserving
/// correctness (ADR-0003: any error means fall back) at zero per-op cost.
/// Rebuilding the CQ/QP and respawning the pump is heavy and out of scope for
/// A1 — a full pump/CQ *restart* is a deliberate future item; capability-off is
/// the correct, cheap stopgap. Callers already awaiting a completion still see
/// their oneshot sender drop (a `RecvError`), which the transport maps to a
/// fallback like any other transport error.
/// Flip this rail's RDMA capability off and bump the operator's pump-death
/// counter — the one place that pairing is written, so a pump that dies, one that
/// fails to build its runtime, and one whose thread never spawned all leave the
/// same observable state.
fn mark_dead(cq_errors: &Arc<AtomicU64>, rdma_healthy: &Arc<AtomicBool>) {
    cq_errors.fetch_add(1, Ordering::Relaxed);
    rdma_healthy.store(false, Ordering::Relaxed);
}

async fn drain_loop(
    cq: CompletionQueue,
    channel: CompletionChannel,
    state: Shared,
    cq_errors: Arc<AtomicU64>,
    rdma_healthy: Arc<AtomicBool>,
    unobserved_failures: Arc<AtomicU64>,
) {
    pump_drain(cq, channel, &state, &unobserved_failures).await;
    // pump_drain returns only on a terminal error, which it has already logged
    // with the specific cause. Record the death (operator's pump-death
    // counter) and disable the RDMA plane so no further WRITE pays the
    // completion-timeout tax — see this fn's doc for why off, not restart.
    mark_dead(&cq_errors, &rdma_healthy);
    error!("RDMA completion pump exited; capability flipped off, fetches now use gRPC directly (a pump/CQ restart is a future item)");
}

/// The arm-then-drain loop itself. Returns only when the completion channel's
/// fd, the CQ notify/poll, or the fd registration hits a terminal error —
/// logging the specific cause before returning so [`drain_loop`]'s death record
/// stays a single terse line.
///
/// Wraps `channel` in the `AsyncFd` here (rather than taking one pre-built) so
/// the fd registers with the runtime this task runs on — see
/// [`CompletionPump::spawn`]. Failing to register is terminal, the same as any
/// later fd error: logged, and the pump exits.
async fn pump_drain(
    cq: CompletionQueue,
    channel: CompletionChannel,
    state: &Shared,
    unobserved_failures: &AtomicU64,
) {
    let async_fd = match AsyncFd::new(channel) {
        Ok(fd) => fd,
        Err(e) => {
            error!(error = %e, "registering completion channel with the RDMA runtime failed; completion pump exiting");
            return;
        }
    };
    loop {
        // Arm before draining: a completion that lands between an earlier
        // drain and this arm is still caught by the drain below, closing the
        // race the crate's own docs call out (arm-then-drain).
        if let Err(e) = cq.req_notify(false) {
            error!(error = %e, "arming CQ notifications failed; completion pump exiting");
            return;
        }
        if let Err(e) = drain_and_dispatch(&cq, state, unobserved_failures) {
            error!(error = %e, "draining CQ failed; completion pump exiting");
            return;
        }
        let mut guard = match async_fd.readable().await {
            Ok(g) => g,
            Err(e) => {
                error!(error = %e, "completion channel fd errored; completion pump exiting");
                return;
            }
        };
        // Consumes one channel notification (auto-acked by the crate); the
        // next loop iteration's drain picks up whatever completion caused it.
        if let Err(e) = guard.get_inner().get_event() {
            error!(error = %e, "consuming completion-channel event failed; completion pump exiting");
            return;
        }
        guard.clear_ready();
    }
}

/// Poll every completion currently queued and deliver each to its waiter (if
/// any is still registered — a cancelled or already-timed-out fetch has none,
/// and that is fine, the completion is just dropped).
///
/// A completion is also the **only** proof that the NIC has finished reading a
/// WRITE's source buffer, so reaping one releases any source this pump was
/// holding for that id ([`CompletionPump::orphan_if_unreaped`]). Released
/// *after* the lock: freeing an ADR-0028 frame takes the slab's free-list lock,
/// and the reaper must not hold two.
///
/// **A failed completion nobody received is logged and counted here**, and nowhere else
/// could do it: past this function the failure has no representation at all. See
/// [`CompletionPump::unobserved_failures`] for why that case is the one that matters — it
/// is the candidate primary cause behind a burst of `WorkRequestFlushed` work requests,
/// which are its consequence. The line carries no rail field because this runs on the
/// per-rail reaper thread, which is named `pacer-cq-<rail>`; a subscriber configured with
/// thread names attributes it without this having to thread one through six frames.
fn drain_and_dispatch(
    cq: &CompletionQueue,
    state: &Shared,
    unobserved_failures: &AtomicU64,
) -> Result<()> {
    while let Some(mut completions) = cq.poll()? {
        // Sources whose WRITE just completed, dropped once this batch is done.
        // Almost always empty — an orphan only exists where a serve already blew
        // its deadline.
        let mut released: Vec<SourceGuard> = Vec::new();
        while let Some(wc) = completions.next() {
            let wr_id = wc.wr_id();
            // `Error::new` + `context`, not `anyhow!("… {e}")`: the typed `WcError` has to
            // survive the trip to whoever awaited this work request, because the *status* is
            // what decides whether a failure is fatal or a race. A delivery WRITE completing
            // `REM_OP_ERR`/vendor 14 means the target had not installed this writer yet
            // (ADR-0030 point 2) — retryable — while every other status is a real fault.
            // Formatting it into a string here made both look identical, and the delivery path
            // answered a customer's GET with a 500 for the retryable one (measured 2026-08-24,
            // `bench/ladder/results/c5-multirail.md`).
            let result = wc
                .ok()
                .map_err(|e| anyhow::Error::new(e).context(format!("work request {wr_id}")));
            // Classified BEFORE dispatch, because dispatch moves `result` into the waiter's
            // channel and the status is not recoverable afterwards.
            let failure = result.as_ref().err().map(failure_label);
            let dispatched = dispatch_one(state, wr_id, result);
            released.extend(dispatched.source);
            if let Some(status) = failure.filter(|_| !dispatched.observed) {
                unobserved_failures.fetch_add(1, Ordering::Relaxed);
                warn!(
                    wr_id,
                    status,
                    "work request FAILED with no waiter left to receive it; this is the \
                     status that a burst of flushed work requests would otherwise hide"
                );
            }
        }
        drop(released);
    }
    Ok(())
}

/// What [`dispatch_one`] did with one completion.
///
/// A struct rather than the bare `Option<SourceGuard>` it used to return, because the
/// caller now needs the second fact too: whether *anyone* received the completion. Only
/// `dispatch_one` can know that — it holds the lock that removes the waiter — and a failure
/// nobody received is the one this pump used to discard in silence.
struct Dispatched {
    /// The source buffer the pump was holding for this work request, if it was holding one,
    /// for the caller to drop outside the lock.
    source: Option<SourceGuard>,
    /// Whether the completion reached a waiter that was still listening. False covers both
    /// shapes of "nobody is home": no registration left at all (a timeout handed the
    /// buffer over, or the post never went out and was cancelled), and a registration whose
    /// receiver has since been dropped (its request was abandoned mid-flight — which is
    /// exactly what happens to the *other* windows of a delivery once one of them has
    /// failed the request).
    observed: bool,
}

/// Deliver one completion: wake its waiter if one is still registered, and yield
/// any source buffer the pump was holding for it so the caller can drop it
/// outside the lock (freeing an ADR-0028 frame takes the slab's free-list lock,
/// and the reaper must not hold two).
///
/// Split from [`drain_and_dispatch`] because it is the half of the orphan
/// protocol that runs on the reaper's side, and the only way to exercise the
/// hand-over race without a real completion queue.
fn dispatch_one(state: &Shared, wr_id: u64, result: CompletionResult) -> Dispatched {
    let mut guard = state.lock().unwrap();
    let source = guard.orphans.remove(&wr_id);
    let waiter = guard.waiters.remove(&wr_id);
    drop(guard);
    // A dropped receiver is still not this pump's problem to *fix* — the send failing is
    // the normal shape of a timed-out serve — but it is now reported, because for a FAILED
    // completion it means the status died here.
    let observed = waiter.is_some_and(|tx| tx.send(result).is_ok());
    Dispatched { source, observed }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Stands in for a WRITE's source buffer: flips a flag when dropped, which is
    /// exactly the event that would return an ADR-0028 frame to the slab's free
    /// list (or a staging range to the arena) and let the next fill overwrite
    /// bytes the NIC may still be reading.
    struct Tracked(Arc<AtomicBool>);

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.0.store(true, Ordering::Relaxed);
        }
    }

    fn tracked() -> (SourceGuard, Arc<AtomicBool>) {
        let flag = Arc::new(AtomicBool::new(false));
        (Box::new(Tracked(Arc::clone(&flag))), flag)
    }

    /// The safety item itself: a serve whose deadline elapses while its
    /// completion has NOT been reaped must not release its source buffer — the
    /// pump takes it, and holds it until the completion actually arrives.
    #[test]
    fn a_deadline_that_elapses_first_hands_the_source_to_the_pump() {
        const WR: u64 = 7;
        let pump = CompletionPump::detached();
        let _rx = pump.register(WR);
        let (source, freed) = tracked();

        assert!(
            pump.orphan_if_unreaped(WR, source),
            "the waiter was still registered, so the pump must take the buffer"
        );
        assert!(
            !freed.load(Ordering::Relaxed),
            "the buffer must NOT be freed: the NIC may still be DMA-reading it"
        );
        assert_eq!(pump.orphan_counts(), (1, 1));

        // The completion finally arrives. NOW the bytes are provably untouched.
        let released = dispatch_one(&pump.state, WR, Ok(()));
        assert!(
            released.source.is_some(),
            "the reap must yield the held buffer"
        );
        drop(released.source);
        assert!(
            freed.load(Ordering::Relaxed),
            "reaping must free the buffer"
        );
        assert_eq!(pump.orphan_counts(), (1, 0));
    }

    /// The race the single lock exists to close: the completion lands between the
    /// deadline firing and the hand-over. The waiter is gone, so the NIC is done
    /// and the buffer must be released immediately — orphaning it here would pin
    /// a frame forever, since no further completion will ever arrive for that id.
    #[test]
    fn a_completion_that_races_in_leaves_nothing_to_protect() {
        const WR: u64 = 11;
        let pump = CompletionPump::detached();
        let rx = pump.register(WR);
        // The reaper wins: it dispatches into a receiver the serve already
        // abandoned, which is what a timed-out serve leaves behind.
        drop(rx);
        assert!(dispatch_one(&pump.state, WR, Ok(())).source.is_none());

        let (source, freed) = tracked();
        assert!(
            !pump.orphan_if_unreaped(WR, source),
            "the completion was already reaped, so the pump must decline"
        );
        assert!(
            freed.load(Ordering::Relaxed),
            "a declined hand-over must free the buffer, not leak the frame"
        );
        assert_eq!(pump.orphan_counts(), (0, 0));
    }

    /// A waiter lost without a completion (`Ok(Err(recv))`) knows its CQE was
    /// never dispatched, so its source is kept unconditionally — the id is not in
    /// `waiters` any more, which is precisely why the conditional path would get
    /// this one wrong.
    #[test]
    fn a_lost_waiter_keeps_its_source_unconditionally() {
        const WR: u64 = 13;
        let pump = CompletionPump::detached();
        let (source, freed) = tracked();

        pump.orphan(WR, source);
        assert!(!freed.load(Ordering::Relaxed));
        assert_eq!(pump.orphan_counts(), (1, 1));
    }

    /// Held sources drop when the pump's state does — the context torn down, its
    /// QP destroyed, which is itself proof no further DMA can occur. Without this
    /// a rail that stops completing would pin frames for the process's lifetime.
    #[test]
    fn dropping_the_pump_releases_everything_it_holds() {
        let (source, freed) = tracked();
        {
            let pump = CompletionPump::detached();
            pump.orphan(17, source);
            assert!(!freed.load(Ordering::Relaxed));
        }
        assert!(
            freed.load(Ordering::Relaxed),
            "tearing the pump down must release held buffers"
        );
    }

    /// The three shapes of "who received this completion", because the whole
    /// unobserved-failure signal hangs off this one boolean: a failure reported as
    /// `observed` is discarded exactly as it was before, and a success reported as
    /// unobserved would cry wolf on every timed-out serve.
    ///
    /// The middle case is the one that was invisible and is NOT rare: a registration whose
    /// receiver has been dropped. It is what every other window of a delivery looks like
    /// once one window has failed the request and `run_delivery` dropped the pipeline — so
    /// if a queue pair goes down mid-request, the completion carrying the *reason* can
    /// easily be one of those.
    #[test]
    fn only_a_completion_a_live_waiter_took_counts_as_observed() {
        let pump = CompletionPump::detached();

        // A waiter that is still listening: observed, and it gets the result.
        let rx = pump.register(1);
        assert!(dispatch_one(&pump.state, 1, Ok(())).observed);
        assert!(rx.blocking_recv().is_ok(), "the waiter must receive it");

        // Registered, then abandoned. `send` fails, so nothing received the status.
        let rx = pump.register(2);
        drop(rx);
        assert!(
            !dispatch_one(&pump.state, 2, Err(anyhow::anyhow!("boom"))).observed,
            "a dropped receiver received nothing, however the send is written"
        );

        // Never registered at all — a timeout already handed the buffer over, or the post
        // was cancelled before it reached the wire.
        assert!(
            !dispatch_one(&pump.state, 3, Err(anyhow::anyhow!("boom"))).observed,
            "no waiter means no observer"
        );
    }

    /// The label a failed completion is counted under, asserted on the status that produced
    /// the incident this counter exists for.
    ///
    /// `WorkRequestFlushed` must be its own label rather than sharing one with the status
    /// that transitioned the queue pair: a flush is a *consequence*, arriving once per work
    /// request that happened to be posted, so a series that merges it with the primary
    /// error measures queue depth at the moment of failure instead of failures.
    #[test]
    fn a_flushed_work_request_is_labelled_apart_from_its_cause() {
        let flushed = anyhow::Error::new(WcError {
            status: WcStatus::WorkRequestFlushed,
            // The vendor error the p6-b200 incident carried. Not part of the label — see
            // `status_label` — so this value must make no difference to the answer.
            vendor_err: 1,
        })
        .context("work request 1863");
        assert_eq!(failure_label(&flushed), "work_request_flushed");

        // A plausible primary cause, and the point of the whole exercise: it must NOT land
        // in the same bucket as the flushes it caused.
        let primary = anyhow::Error::new(WcError {
            status: WcStatus::LocalProtectionError,
            vendor_err: 0,
        });
        assert_ne!(failure_label(&primary), failure_label(&flushed));

        // An error with no completion behind it at all: a rejected post, a completion that
        // never arrived, a dropped waiter. One honest bucket, not a parse of the message.
        assert_eq!(
            failure_label(&anyhow::anyhow!(
                "timed out after 5s awaiting WRITE completion"
            )),
            NO_COMPLETION_STATUS
        );
    }

    /// Every status maps to a distinct, non-empty label. A duplicate would silently merge
    /// two statuses into one series — and this family's entire job is to tell a flush apart
    /// from the thing that caused it, so a collision here would defeat the metric while
    /// leaving it looking healthy.
    #[test]
    fn every_completion_status_has_its_own_label() {
        let all = [
            WcStatus::Success,
            WcStatus::LocalLengthError,
            WcStatus::LocalQpOperationError,
            WcStatus::LocalEecOperationError,
            WcStatus::LocalProtectionError,
            WcStatus::WorkRequestFlushed,
            WcStatus::MemoryWindowBindError,
            WcStatus::BadResponse,
            WcStatus::LocalAccessError,
            WcStatus::RemoteInvalidRequest,
            WcStatus::RemoteAccessError,
            WcStatus::RemoteOperationError,
            WcStatus::RetryExceeded,
            WcStatus::RnrRetryExceeded,
            WcStatus::LocalRddViolation,
            WcStatus::RemoteInvalidRdRequest,
            WcStatus::RemoteAborted,
            WcStatus::InvalidEecn,
            WcStatus::InvalidEecState,
            WcStatus::Fatal,
            WcStatus::ResponseTimeout,
            WcStatus::GeneralError,
            WcStatus::TagMatchingError,
            WcStatus::TagMatchingRendezvousIncomplete,
        ];
        let mut labels: Vec<&str> = all.iter().copied().map(status_label).collect();
        assert!(
            labels.iter().all(|l| !l.is_empty()),
            "an empty label is not a usable metric dimension"
        );
        // None may collide with the no-completion bucket, which is not a status at all.
        assert!(!labels.contains(&NO_COMPLETION_STATUS));
        // And none may fall through the compulsory `#[non_exhaustive]` wildcard: that arm is
        // for a status a FUTURE ibverbs adds, so a known status reaching it means an arm was
        // deleted and its traffic silently merged into the catch-all.
        assert!(
            !labels.contains(&UNCLASSIFIED_STATUS),
            "a known status fell through to the wildcard arm"
        );
        labels.sort_unstable();
        let count = labels.len();
        labels.dedup();
        assert_eq!(labels.len(), count, "duplicate status label: {labels:?}");
    }

    /// `cancel` is for a WRITE that never reached the wire, so it must not be
    /// confusable with the orphan path: it removes the waiter and there is no
    /// buffer to keep. Asserted because a future edit that routed a real
    /// post-failure through `orphan` would silently pin a frame per failed post.
    #[test]
    fn cancelling_an_unposted_write_leaves_no_orphan() {
        const WR: u64 = 19;
        let pump = CompletionPump::detached();
        let _rx = pump.register(WR);
        pump.cancel(WR);
        assert_eq!(pump.orphan_counts(), (0, 0));
        assert!(pump.state.lock().unwrap().waiters.is_empty());
    }
}
