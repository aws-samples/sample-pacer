//! The holder-driven one-sided RDMA WRITE itself (ADR-0018 points 2-3): post
//! the WRITE into the requester's buffer, then await this node's OWN
//! send-completion as the done signal — SRD is reliable, so local
//! send-completion implies the bytes already landed in the requester's
//! registered memory. Mirrors `spike/efa/src/rdma.rs::post_write`, generalized
//! from the spike's one fixed `WR_WRITE` id to a counter so many fetches can
//! be in flight at once.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use anyhow::Result;
use ibverbs::{AddressHandle, LocalMemorySlice, RemoteMemorySlice};
use tokio::sync::oneshot;

use super::completion::CompletionPump;
pub(super) use super::completion::SourceGuard;
use super::context::{EfaContext, COMPLETION_TIMEOUT};

/// Distinguishes work requests from every other kind this context might ever
/// post (today, only WRITE — READ is ADR-0020's directory lookup, not yet
/// built). Starts above 0 so a default/uninitialized id is never mistaken
/// for a real one.
static NEXT_WR_ID: AtomicU64 = AtomicU64::new(1);

/// The next work-request id. `pub(super)` because [`super::announce`] posts SENDs on the
/// same queue pairs and the completion pump keys waiters by id alone, so both kinds of
/// work request must draw from ONE id space or a SEND could resolve a WRITE's waiter.
pub(super) fn next_wr_id() -> u64 {
    NEXT_WR_ID.fetch_add(1, Ordering::Relaxed)
}

/// A WRITE successfully posted to the wire; its completion can be awaited
/// via [`await_write_completion`] without any reference to the `AddressHandle`
/// that addressed it — the AH is only needed for the synchronous post.
pub(super) struct PostedWrite {
    rx: oneshot::Receiver<anyhow::Result<()>>,
    /// The completion pump this WRITE registered its waiter with, plus the
    /// `wr_id` that keys it, so [`await_write_completion`] can remove the
    /// waiter on the timeout path (A3). Without this, a WRITE whose completion
    /// never arrives — exactly the timeout case — would leak its
    /// `(wr_id → Sender)` entry in the pump's map forever: `post_write` only
    /// cancels on the *post-failure* path, never on a timeout. Cloning the
    /// handle is cheap (it shares the one `Arc`-backed waiter map — see
    /// [`CompletionPump`]).
    pump: CompletionPump,
    wr_id: u64,
    /// Nanoseconds spent building and submitting the send batch — the
    /// synchronous, non-yielding span inside [`post_write`]'s `ctx.post`
    /// closure only (the QP-mutex acquisition, which awaits, is deliberately
    /// excluded). This span never crosses an `.await`, so wall ≈ CPU for it
    /// and it is a valid holder-side CPU-cost input, unlike
    /// [`super::EfaRdmaTransport::write_completion_wait_nanos`]. Surfaced by
    /// the caller into the transport's accumulator; see
    /// [`super::EfaRdmaTransport::post_batch_nanos`].
    post_batch_nanos: u64,
}

impl PostedWrite {
    /// Nanoseconds the synchronous post batch took (see the field doc): the
    /// holder-side CPU-bound span the caller folds into the transport's
    /// `post_batch_nanos` accumulator.
    pub(super) fn post_batch_nanos(&self) -> u64 {
        self.post_batch_nanos
    }
}

/// Post a one-sided RDMA WRITE of `local` into `remote` on the peer addressed
/// by `ah`/`peer_qp_num`. The `ah` borrow ends when this call returns — the
/// posting itself (`ctx.post`'s closure) is synchronous, only the outer QP
/// lock acquisition is async — so it is split from [`await_write_completion`]
/// specifically so the caller can drop anything borrowed to build `ah` (the
/// holder's `AhCache` lock guard) before awaiting the WRITE's full wire
/// round-trip instead of holding it across that whole await — see
/// [`super::EfaRdmaTransport::serve_via_write`]'s call site and its doc
/// comment for why that distinction is load-bearing.
///
/// # Errors
///
/// The synchronous post itself failing (e.g. send-queue full).
pub(super) async fn post_write(
    ctx: &EfaContext,
    ah: &AddressHandle,
    peer_qp_num: u32,
    peer_qkey: u32,
    local: LocalMemorySlice,
    remote: RemoteMemorySlice,
) -> Result<PostedWrite> {
    let wr_id = next_wr_id();
    let pump = ctx.pump();
    let rx = pump.register(wr_id);
    // Time the batch build+submit *inside* the closure: it runs after the QP
    // mutex is acquired and is fully synchronous (never yields — see
    // `EfaContext::post`'s doc), so this span is genuinely CPU-bound. Timing
    // the whole `ctx.post(...).await` instead would fold in the lock
    // acquisition, a real `.await`, turning a CPU measurement into a
    // wall-clock one (the confound `write_completion_wait_nanos` isolates).
    let (posted, post_batch_nanos) = ctx
        .post(|qp| {
            let batch_start = Instant::now();
            let mut batch = qp.start_send();
            batch
                .to(ah, peer_qp_num, peer_qkey)
                .signaled()
                .write(wr_id, &[local], remote);
            // SAFETY: `local`'s backing MR (a leased arena range, see
            // `super::arena::ArenaLease`) stays registered and untouched by the
            // caller until the completion this returns resolves — the lease
            // is held by the caller across the whole post+await sequence.
            // The `unsafe` is inherent to the RDMA verbs FFI (ibv_post_send has
            // no safe wrapper); the invariant is justified above, not a bug. The
            // marker must stay DIRECTLY above the code and carry the full rule
            // id — semgrep ignores it otherwise.
            // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
            let submitted = unsafe { batch.submit() };
            (submitted, batch_start.elapsed().as_nanos() as u64)
        })
        .await;
    if let Err(e) = posted {
        pump.cancel(wr_id);
        return Err(e.into());
    }
    Ok(PostedWrite {
        rx,
        pump: pump.clone(),
        wr_id,
        post_batch_nanos,
    })
}

/// Block until *this node's own* send-completion for a WRITE posted by
/// [`post_write`] is reaped — the ADR-0018 point 3 done signal. Returns once
/// the bytes are guaranteed visible in the peer's memory; the caller (the
/// holder, inside `FetchBlob`) then simply returns its gRPC response as the
/// fetch's completion, no separate message needed. Takes no reference to the
/// AH: by this point the WRITE is already on the wire, so nothing from
/// [`super::address::AhCache`] needs to stay borrowed — the caller has already
/// dropped its AH lease guard (invariant R7, documented at the `drop(ah_ref)`
/// site in [`super::EfaRdmaTransport::serve_via_write`]).
///
/// `source` is the local buffer the WRITE reads from — an ADR-0028 cache frame
/// or an ADR-0024 staging lease, type-erased. It is **consumed here rather than
/// by the caller** because a completion is the only proof the NIC has stopped
/// reading it, and the two failure paths below return *without* that proof:
///
/// * [`COMPLETION_TIMEOUT`] elapsing is only **our** deadline. The device
///   produces a completion for every posted WQE while the QP lives, so a
///   timeout is not evidence the transfer is over — it is evidence we stopped
///   waiting. Freeing the buffer here would let the next fill overwrite bytes
///   the NIC is still DMA-reading, sending a *different chunk's* bytes to the
///   requester with nothing on the RDMA path to detect it (ADR-0028 § "a
///   completion that times out must not free the frame").
/// * The waiter being dropped without a completion means no CQE can ever
///   resolve it here.
///
/// On both, ownership transfers to the completion pump, which releases the
/// buffer when the CQE actually arrives or when the QP is destroyed — the one
/// event that *is* proof no further DMA can occur. On the success path the
/// buffer drops at the end of this function, i.e. after the completion is
/// reaped, which is where the caller used to drop it.
///
/// # Errors
///
/// The work request completing with a failed status (most notably
/// `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER` if the AH-before-WRITE
/// ordering — ADR-0018 finding 6 — was violated), the completion pump
/// dropping the waiter (context torn down) before a completion arrives, or
/// [`COMPLETION_TIMEOUT`] elapsing.
pub(super) async fn await_write_completion(posted: PostedWrite, source: SourceGuard) -> Result<()> {
    let PostedWrite {
        rx, pump, wr_id, ..
    } = posted;
    match tokio::time::timeout(COMPLETION_TIMEOUT, rx).await {
        // Timed out. Hand the source to the pump if the completion has not been
        // reaped yet, which in the same operation removes our `(wr_id → Sender)`
        // waiter — otherwise it would leak for the process's lifetime (A3), the
        // job `cancel` used to do here. A completion that raced in just before
        // this call took the waiter with it, so `orphan_if_unreaped` declines,
        // and `source` drops normally: the NIC is done.
        Err(_elapsed) => {
            let orphaned = pump.orphan_if_unreaped(wr_id, source);
            Err(anyhow::anyhow!(
                "timed out after {COMPLETION_TIMEOUT:?} awaiting WRITE completion \
                 (source buffer {} the completion pump)",
                if orphaned {
                    "handed to"
                } else {
                    "released, completion raced in past"
                }
            ))
        }
        // The pump dropped the sender without sending: the EFA context was torn
        // down (or the pump exited — A1). Structurally near-unreachable, since
        // `PostedWrite` holds a pump handle and so keeps the waiter map alive —
        // but if it happens we know the completion was NOT dispatched to us
        // (a dispatch sends), so the source is orphaned unconditionally.
        Ok(Err(_recv)) => {
            pump.orphan(wr_id, source);
            Err(anyhow::anyhow!(
                "completion pump dropped the waiter (EFA context torn down); \
                 source buffer handed to the pump"
            ))
        }
        // The pump delivered the completion and removed the waiter itself, so
        // the NIC is done: `source` drops as this scope ends.
        Ok(Ok(result)) => result,
    }
}
