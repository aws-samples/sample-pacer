//! The RDMA buffer seam (planning/19 task S1): the lease surface every RDMA
//! buffer backend exposes, so *where the bytes live* can change without the
//! transport changing.
//!
//! [`HostArena`](super::HostArena) — ADR-0024's hugepage-backed registered
//! arena (planning/19 track D) — is the implementor today, having replaced
//! ADR-0018's fixed pool of 64 MiB slots without a single call site changing:
//! the payoff the seam was built for. Two more are planned behind the same
//! surface: that arena over GPU HBM (ADR-0022's HBM half, track H) and an HBM
//! arena filled from NVMe (track N). Each is a separate implementation in its
//! own file. Without the trait, each of them instead rewrites the one concrete
//! buffer module and they conflict on every commit — which is why planning/19
//! makes this the one ordering constraint between the tracks.
//!
//! What the trait deliberately does NOT cover is **construction**: a host arena
//! takes a range count, a range size and a page policy; an HBM arena takes a
//! CUDA allocation and a dmabuf fd. So the backend is chosen exactly once, where
//! the rails are built ([`super::RailBuffers`]), and every other use in this
//! crate goes through the trait.
//!
//! The surface is the one ADR-0024 point 6 freezes: `lease`/`lease_owned`
//! handing out a borrow-scoped or `'static` lease, `remote()` for the wire
//! descriptor, `local_slice()` for the postable SGE, `with_bytes*()` for local
//! access, and `slots_in_use()` for the
//! `pacer_rdma_requester_slots_in_use` gauge. "Slot" is retained as the name of
//! a leasable unit even though ADR-0024's arena calls it a *range*: keeping the
//! names is what keeps call sites and the gauge untouched across the swap.
//!
//! Not object-safe, by choice — [`RdmaLease::with_bytes_mut`] and
//! [`RdmaLease::local_slice`] are generic, and [`RdmaBuffers::lease`] returns an
//! associated type. Selection is a build-time choice of one concrete backend
//! ([`super::RailBuffers`]), so the data path stays monomorphic and unboxed; a
//! *runtime* choice (H1's "HBM if the driver exposes dmabuf, host arena
//! otherwise") is a delegating enum that implements this trait, not a vtable.

use std::future::Future;
use std::ops::RangeBounds;

use ibverbs::{AccessFlags, LocalMemorySlice, RemoteMemorySlice};

/// The EFA-valid access flags every registration in this module's family uses:
/// local write (to receive a peer's WRITE when this node is the requester) plus
/// remote read/write (so a peer's one-sided ops can target it).
///
/// Deliberately NOT [`AccessFlags::PERMISSIVE`] — that bundle includes
/// `REMOTE_ATOMIC` and `ibv_reg_mr` fails `EOPNOTSUPP` registering it on EFA
/// (no hardware atomics, spike finding 7 / planning/04 §2). The same reduced set
/// `spike/efa/src/regbuf.rs` validated cross-node.
///
/// Lives here, on the seam, rather than in one backend: the flags are part of
/// the [`RdmaBuffers`] contract (see its invariants), and every registration —
/// the host arena (ADR-0024), a client's target memory (ADR-0026), the HBM tier
/// to come (ADR-0022) — must use the identical set or fail on hardware in a way
/// no test on a non-EFA host can catch.
pub(crate) fn efa_access_flags() -> AccessFlags {
    AccessFlags::LOCAL_WRITE | AccessFlags::REMOTE_READ | AccessFlags::REMOTE_WRITE
}

/// A supply of pre-registered RDMA buffers on ONE rail's protection domain,
/// leased per fetch and released on drop.
///
/// `Send + Sync` because a rail is shared across the daemon's tasks (the
/// transport lives behind an `Arc`, and the requester and holder paths lease
/// concurrently); the lease futures are `Send` because they are awaited inside
/// `#[async_trait]`-boxed transport methods and inside the `tokio::spawn`ed
/// holder serve.
///
/// # Invariants an implementor must uphold
///
/// - **Registered once, never per-lease** (ADR-0018 point 1, restated by
///   ADR-0024 point 1). Leasing must not call `ibv_reg_mr`: registration is a
///   startup cost, and paying it on the data path is the thing both ADRs exist
///   to prevent.
/// - **EFA-valid access flags**: `LOCAL_WRITE | REMOTE_READ | REMOTE_WRITE`,
///   never [`AccessFlags::PERMISSIVE`](ibverbs::AccessFlags::PERMISSIVE) — that
///   bundle includes `REMOTE_ATOMIC` and registration fails `EOPNOTSUPP` on EFA,
///   which has no hardware atomics (spike finding 7, planning/04 §2).
/// - **A lease is exclusive** for its whole lifetime: two live leases must never
///   hand out overlapping bytes, because a peer WRITEs into the range named by
///   [`LeasedRange::remote`] with no further coordination — an overlap is silent
///   data corruption, not a contended lock.
/// - **Leasing waits; it never fails.** Exhaustion is backpressure, not an
///   error: callers are already bounded by `fetch_parallelism` ≤ capacity
///   (ADR-0018), so a full backend queues the lease rather than pushing the
///   caller onto the gRPC fallback.
pub trait RdmaBuffers: Send + Sync {
    /// The borrow-scoped lease [`RdmaBuffers::lease`] hands out — used where the
    /// buffer is filled, posted, and released inside one function (the holder's
    /// serve path).
    type Lease<'a>: RdmaLease
    where
        Self: 'a;

    /// The `'static` lease [`RdmaBuffers::lease_owned`] hands out — used where
    /// the buffer must outlive the call that leased it (the requester's
    /// zero-copy path hands it to the S3 client inside a `bytes::Bytes`).
    type OwnedLease: OwnedRdmaLease;

    /// Capacity of one leasable buffer: the ceiling on what a single fetch can
    /// offer a peer. A body larger than this is the `RdmaBuffer.len` capacity
    /// miss the proto documents — the holder streams it over `BlobChunk`
    /// instead, which is a fallback, not an error.
    fn slot_bytes(&self) -> usize;

    /// Buffers currently leased out, for the
    /// `pacer_rdma_requester_slots_in_use` gauge. A value pinned at capacity is
    /// the operator's signal that hold time (not the wire) is the binding
    /// constraint — exactly the observation ADR-0024 is a response to.
    fn slots_in_use(&self) -> usize;

    /// Lease a buffer for the duration of the `&self` borrow, waiting if every
    /// buffer is in flight.
    ///
    /// Returns a future rather than being an `async fn` so the `Send` bound is
    /// part of the contract: callers await it inside `Send` futures.
    fn lease(&self) -> impl Future<Output = Self::Lease<'_>> + Send;

    /// Lease a buffer as an owned, `'static` handle whose lifetime is decoupled
    /// from this backend's borrow — the requester's zero-copy path, where the
    /// buffer stays leased until the S3 client has drained the `Bytes` built
    /// from it (planning/16 §5).
    ///
    /// Same `Send` reasoning as [`RdmaBuffers::lease`].
    fn lease_owned(&self) -> impl Future<Output = Self::OwnedLease> + Send;
}

/// What every leased buffer exposes regardless of lease shape: the descriptor a
/// peer needs in order to WRITE into it.
pub trait LeasedRange {
    /// The remote descriptor `(addr, rkey, len)` to offer a peer — the
    /// `RdmaBuffer` a `FetchBlob` request carries (ADR-0018 point 1). The rkey
    /// is only valid at the protection domain that issued it, so a descriptor is
    /// meaningful only paired with the rail it was leased from — the by-index
    /// pairing rule documented on [`super::EfaRdmaTransport`]'s `rails` field.
    fn remote(&self) -> RemoteMemorySlice;
}

/// A buffer leased for the duration of a borrow: filled, posted, and released
/// within one function. The holder's stage-then-WRITE path.
///
/// `Send` because the holder holds its lease across the `.await` on the WRITE's
/// completion.
pub trait RdmaLease: LeasedRange + Send {
    /// Run `f` over the buffer's local bytes, mutably — the holder staging a
    /// WRITE payload into the registered source.
    fn with_bytes_mut<R>(&mut self, f: impl FnOnce(&mut [u8]) -> R) -> R;

    /// Read-only counterpart of [`RdmaLease::with_bytes_mut`]: read back what a
    /// peer WROTE into this buffer. Retained for the copy-out shape the
    /// requester used before the zero-copy path (planning/16 §5) replaced it
    /// with [`OwnedRdmaLease::into_written`].
    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R;

    /// A postable local scatter/gather entry over `bounds` of this buffer — the
    /// WRITE's local source on the holder side.
    ///
    /// # Panics
    ///
    /// Implementors panic if `bounds` is empty or falls outside the buffer (see
    /// [`ibverbs::MemoryRegion::slice`]): a caller computing an SGE outside the
    /// range it leased is a bug in the caller, not a runtime condition to fall
    /// back on.
    fn local_slice(&self, bounds: impl RangeBounds<usize>) -> LocalMemorySlice;
}

/// A buffer leased as a `'static` handle, so the bytes a peer WROTE into it can
/// be handed onward without a copy-out and the buffer returns to its backend
/// only when the last reader drops it.
///
/// `Send + 'static` are what `bytes::Bytes::from_owner` requires of the owner it
/// takes, via [`OwnedRdmaLease::Written`].
pub trait OwnedRdmaLease: LeasedRange + Send + 'static {
    /// The owning view of the WRITE-landed bytes that
    /// [`OwnedRdmaLease::into_written`] produces. Its bounds are exactly
    /// `bytes::Bytes::from_owner`'s, which is its only purpose: it lets the
    /// requester hand the S3 layer a `Bytes` backed directly by registered
    /// memory.
    type Written: AsRef<[u8]> + Send + 'static;

    /// Consume this lease into an owning view of its first `len` bytes — the
    /// body the holder's WRITE landed.
    ///
    /// The returned value keeps the lease alive, so the buffer stays out of the
    /// backend for as long as anything reads those bytes. Anything that
    /// *retains* them beyond the client stream (the layer-1 admit in the
    /// daemon's `proxy.rs::maybe_admit_local`) must copy them out first, or one
    /// cached chunk pins one registered buffer forever.
    ///
    /// # Panics
    ///
    /// Implementors panic if `len` exceeds the leased capacity. The caller
    /// validates the body against the offered buffer before the WRITE is posted
    /// ([`super::EfaRdmaTransport::serve_via_write`]), so reaching here with a
    /// too-large `len` means the wire descriptor and the buffer have already
    /// disagreed.
    fn into_written(self, len: usize) -> Self::Written;
}
