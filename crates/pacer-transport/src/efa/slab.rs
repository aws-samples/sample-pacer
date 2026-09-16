//! ADR-0028: the cache's RAM tier IS the registered arena.
//!
//! A holder can only source a one-sided WRITE from a registered MR, so today it
//! copies the cached chunk into a staging range first — `holder_copy`, 5.32 ms
//! per serve (planning/19 § D4). This module removes that copy by making the
//! cached bytes themselves live in registered memory: one hugepage mapping,
//! carved into `chunk_size` frames, registered on **every rail's** protection
//! domain, handed to the cache as `bytes::Bytes::from_owner(CacheFrame)`.
//!
//! Two properties of the existing design are what make it work, and both were
//! already true (ADR-0028 § Context):
//!
//! 1. **We choose where a cached chunk's bytes live**, not foyer — it stores the
//!    `Bytes` we hand it, and the requester path already hands the S3 layer a
//!    `Bytes::from_owner` over registered memory ([`super::WrittenRange`]).
//! 2. **Every cache entry is exactly one chunk** (ADR-0015), so a slab of fixed
//!    `chunk_size` frames has no fragmentation problem *at all* — the property
//!    that usually sinks a slab design is absent by construction.
//!
//! # How a serve finds the frame
//!
//! The serve path holds a `Bytes` and needs the SGE for it. It does not need to
//! know that the `Bytes` is frame-backed: **the data pointer is the identity**.
//! [`CacheSlab::local_slice`] takes the body's pointer, checks it against the
//! mapping's bounds and returns a slice of that rail's MR at the matching
//! offset — no downcast of the `Bytes` owner, no side table, and a body from
//! anywhere else (a disk-tier promotion, an `ObjectHeader`, a test fixture)
//! simply returns `None` and falls back to the staging copy. That fallback is
//! why this can land before it has ever run on hardware: the slab is an
//! optimization on a path that still works without it.
//!
//! # Why a frame cannot be recycled under the NIC
//!
//! A frame returns to the free list when the last `Bytes` clone built from it
//! drops, so the question is who holds a clone until the NIC is finished. On the
//! happy path [`super::EfaRdmaTransport::serve_via_write`] borrows the body for
//! the whole call, including the await on the WRITE's completion; eviction
//! cannot shorten that, since dropping foyer's copy only decrements a refcount.
//!
//! That argument is **not sufficient on its own**, because two paths return
//! while the work request may still be outstanding in the NIC: the software
//! completion deadline elapsing, and the pump dropping the waiter. There the
//! serve's borrow ends but the DMA has not, so the frame is handed to the
//! completion pump instead and released when the CQE actually arrives (or when
//! the QP dies, which is proof no further DMA can occur) — see
//! [`super::write::await_write_completion`]. Without that transfer the next fill
//! could overwrite bytes mid-DMA and ship a *different chunk* to the requester,
//! undetectably: there is no per-chunk digest on the RDMA path.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use bytes::Bytes;
use ibverbs::{LocalMemorySlice, MemoryRegion, ProtectionDomain};
use tracing::info;

use super::arena::{map_arena, ArenaPages, Mapping};
use super::buffers::efa_access_flags;

/// Frames a slab must afford before it is worth building. A slab that cannot
/// hold at least this many chunks would make admission a coin toss — every fill
/// evicting a frame another serve is still reading — so the daemon is better off
/// with the staging copy and its whole `mem_capacity` usable as ordinary heap.
/// Four is the smallest count that lets a fill, a serve and an evict overlap
/// without immediately starving each other.
const MIN_USEFUL_FRAMES: usize = 4;

/// One slab: the mapping, one registration **per rail**, and the free list over
/// its frames. Behind an `Arc` so a [`CacheFrame`] can be `'static` — it lives
/// inside a `Bytes` the cache owns for as long as the entry is resident.
struct SlabInner {
    /// One MR per rail, indexed by rail number. An MR is protection-domain
    /// scoped, so a frame any rail may serve from is registered once per rail;
    /// ADR-0028's gate measured what that costs (~8-14 s for 96 GiB × 32 rails
    /// on hugepages, versus ~96-269 s on base pages — which is why hugepages
    /// are a precondition of this design and not an optimization).
    ///
    /// Declared before `mapping` so every registration is torn down before the
    /// pages it pins are unmapped.
    mrs: Vec<MemoryRegion<()>>,
    mapping: Mapping,
    frame_bytes: usize,
    frames: usize,
    /// Indices not currently holding a cached chunk, as a LIFO stack: the most
    /// recently freed frame is the next one filled, so a burst reuses hot pages.
    /// A `std::sync::Mutex` because the critical section is a `push`/`pop` that
    /// never awaits.
    ///
    /// There is deliberately **no semaphore** here, unlike [`super::HostArena`]:
    /// a lease waits for a range, but a fill that cannot claim a frame is simply
    /// **not admitted** (ADR-0028 point 3). Waiting would be worse than not
    /// caching — it would stall a client read behind an eviction.
    free_frames: Mutex<Vec<u32>>,
}

// SAFETY: the only non-`Send`/`Sync` member is `Mapping`'s raw pointer, and the
// mapping is owned exclusively by this `SlabInner` from construction to drop.
// Frame bytes are written only through the `FrameWriter` that claimed the frame,
// which is the sole handle naming it until `seal` consumes it; after sealing the
// frame is immutable for as long as any `Bytes` built from it lives, and its
// index is not in the free list, so no two threads ever write the same bytes.
// The free list is itself `Send + Sync`.
unsafe impl Send for SlabInner {}
unsafe impl Sync for SlabInner {}

/// A hugepage-backed slab of `chunk_size` frames, registered on every rail, that
/// **is** the cache's RAM tier (ADR-0028). Cloneable: the daemon's fill path and
/// the transport's serve path hold the same slab.
#[derive(Clone)]
pub struct CacheSlab {
    inner: Arc<SlabInner>,
}

impl CacheSlab {
    /// Map `slab_bytes` worth of `frame_bytes` frames and register the whole
    /// mapping on each PD in `pds` (index = rail number).
    ///
    /// Returns `Ok(None)` when the budget affords too few frames to be worth
    /// pinning (see `MIN_USEFUL_FRAMES`), or when `pds` is empty — both mean "run
    /// without a slab", which is a supported configuration and not a failure.
    ///
    /// # Errors
    ///
    /// The mapping failing (the slab does not fit in the pod's memory) or any
    /// `ibv_reg_mr` failing — out of `RLIMIT_MEMLOCK`, or `ENOMEM` pinning more
    /// than the host allows. Fatal by ADR-0024's precedent and ADR-0028's last
    /// consequence: `mem_capacity` becomes a hard reservation, so a slab that
    /// silently came up smaller would be a silently smaller cache.
    ///
    /// # Panics
    ///
    /// If `frame_bytes` is zero — callers derive it from the configured
    /// `chunk_size`, which the daemon validates at startup.
    pub fn new(
        pds: &[&ProtectionDomain],
        slab_bytes: usize,
        frame_bytes: usize,
        pages: ArenaPages,
    ) -> Result<Option<Self>> {
        assert!(frame_bytes > 0, "a slab frame cannot be zero bytes");
        let frames = useful_frames(slab_bytes, frame_bytes).unwrap_or(0);
        if pds.is_empty() || frames == 0 {
            info!(
                slab_bytes,
                frame_bytes,
                frames,
                rails = pds.len(),
                "not building a cache slab: the budget affords fewer than {MIN_USEFUL_FRAMES} \
                 frames (or no rail has a protection domain) — the cache stays on the heap and \
                 holders keep staging their WRITEs"
            );
            return Ok(None);
        }
        let mapping = map_arena(frames * frame_bytes, pages).context("mapping the cache slab")?;
        let mut mrs = Vec::with_capacity(pds.len());
        for (rail, pd) in pds.iter().enumerate() {
            // SAFETY: `mapping` owns a valid, writable mapping of exactly
            // `mapping.len` bytes and is moved into the `SlabInner` below, whose
            // field order keeps it alive until after every MR here is
            // deregistered — `register_from_raw`'s requirement.
            let mr = unsafe { pd.register_from_raw(mapping.ptr, mapping.len, efa_access_flags()) }
                .with_context(|| {
                    format!(
                        "registering the {}-byte cache slab on rail {rail} of {}",
                        mapping.len,
                        pds.len(),
                    )
                })?;
            mrs.push(mr);
        }
        info!(
            frames,
            frame_bytes,
            rails = mrs.len(),
            pages = mapping.pages.label(),
            registered_bytes = mapping.len,
            "cache slab registered on every rail (ADR-0028): cached chunks are RDMA-postable \
             in place, so holders serve without staging a copy"
        );
        Ok(Some(Self {
            inner: Arc::new(SlabInner {
                mrs,
                mapping,
                frame_bytes,
                frames,
                free_frames: Mutex::new((0..frames as u32).rev().collect()),
            }),
        }))
    }

    /// Copy `src` into a free frame and return it as a `Bytes` that owns the
    /// frame — the value the cache stores, and the one a holder can WRITE from
    /// without staging.
    ///
    /// `None` means **not admitted**: either every frame is holding a resident
    /// chunk (ADR-0028 point 3 — the cache's own eviction is what frees frames,
    /// so the two accounts cannot drift) or `src` is larger than a frame, which
    /// in a consistent cluster means a body that is not a chunk.
    ///
    /// # Panics
    ///
    /// If the free-list mutex is poisoned, i.e. a previous holder panicked inside
    /// a `push`/`pop`. Nothing in those critical sections can panic, so this is
    /// unreachable — and it must stay a panic rather than a silent `None`, since
    /// a poisoned list means the frame accounting is already untrustworthy.
    #[must_use]
    pub fn store(&self, src: &[u8]) -> Option<Bytes> {
        let mut frame = self.claim(src.len())?;
        frame.as_mut_slice().copy_from_slice(src);
        Some(frame.seal())
    }

    /// Claim a free frame of exactly `len` bytes, to be filled by the caller and
    /// then sealed into the `Bytes` the cache stores.
    ///
    /// This exists for callers that produce the bytes *into* a buffer rather than
    /// from one — the disk-tier promotion decode, which reads a chunk out of
    /// foyer's region buffer with `Read::read_exact` (ADR-0028 § "a promoted
    /// chunk is NOT frame-backed"). Going through [`CacheSlab::store`] there
    /// would mean decoding into a `Vec` and copying again; filling a claimed
    /// frame directly costs the same single copy the decoder already pays.
    ///
    /// `None` means **not admitted**, on the same two conditions as
    /// [`CacheSlab::store`]: no free frame, or `len` exceeding a frame.
    ///
    /// # Panics
    ///
    /// If the free-list mutex is poisoned — see [`CacheSlab::store`].
    #[must_use]
    pub fn claim(&self, len: usize) -> Option<FrameWriter> {
        if len > self.inner.frame_bytes {
            return None;
        }
        let index = self
            .inner
            .free_frames
            .lock()
            .expect("the slab free list is only ever locked for a push/pop")
            .pop()?;
        Some(FrameWriter {
            frame: CacheFrame {
                slab: Arc::clone(&self.inner),
                index,
                len,
            },
        })
    }

    /// The postable SGE for `body` if — and only if — those bytes live in this
    /// slab: the pointer-identity lookup the serve path uses (see the module
    /// doc). `None` for a body from anywhere else, or for a rail this slab has
    /// no registration on, and the caller then stages a copy as before.
    #[must_use]
    pub fn local_slice(&self, rail: usize, body: &[u8]) -> Option<LocalMemorySlice> {
        let offset = offset_within(
            self.inner.mapping.ptr as usize,
            self.inner.mapping.len,
            body.as_ptr() as usize,
            body.len(),
        )?;
        let mr = self.inner.mrs.get(rail)?;
        Some(mr.slice(offset..offset + body.len()))
    }

    /// Frames currently holding a resident chunk. With [`CacheSlab::frames`] this
    /// is the cache's occupancy **in the units admission actually works in**,
    /// which the byte-denominated `mem_capacity` gauge cannot show.
    ///
    /// # Panics
    ///
    /// If the free-list mutex is poisoned — see [`CacheSlab::store`].
    #[must_use]
    pub fn frames_in_use(&self) -> usize {
        self.inner.frames
            - self
                .inner
                .free_frames
                .lock()
                .expect("the slab free list is only ever locked for a push/pop")
                .len()
    }

    /// Total frames — the hard ceiling on resident chunks.
    #[must_use]
    pub fn frames(&self) -> usize {
        self.inner.frames
    }

    /// Bytes pinned by this slab, once per rail. `registered_bytes() × rails` is
    /// what the pod's memory limit must cover, and the number that made
    /// ADR-0028 contingent on a measurement.
    #[must_use]
    pub fn registered_bytes(&self) -> usize {
        self.inner.mapping.len
    }

    /// Which pages the slab actually got, after any fallback. Read this — never
    /// the request — when interpreting a serve-path measurement: on base pages
    /// the registration cost is 20-30× worse (ADR-0028's gate table).
    #[must_use]
    pub fn pages(&self) -> ArenaPages {
        self.inner.mapping.pages
    }
}

impl SlabInner {
    /// Byte offset of frame `index` within the mapping.
    fn offset_of(&self, index: u32) -> usize {
        index as usize * self.frame_bytes
    }
}

/// A claimed-but-unfilled frame ([`CacheSlab::claim`]): exclusive mutable access
/// to its bytes until [`FrameWriter::seal`] turns it into the shareable `Bytes`
/// the cache stores.
///
/// Two states, one type, and the type system enforces the transition: a
/// `FrameWriter` is the only way to write a frame, and sealing consumes it, so no
/// handle capable of mutation survives into the shared phase — which is the
/// premise `SlabInner`'s `Send`/`Sync` justification rests on. Dropping one
/// without sealing returns the frame to the slab (an abandoned decode leaks
/// nothing).
pub struct FrameWriter {
    frame: CacheFrame,
}

impl FrameWriter {
    /// The frame's bytes, to be filled completely — the length was fixed at
    /// claim time, and [`FrameWriter::seal`] publishes all of it.
    ///
    /// Uninitialized on first claim and stale bytes of a previous chunk on any
    /// reuse, so a caller that writes less than the whole slice publishes
    /// garbage. Callers are decoders with an exact byte count
    /// (`Read::read_exact`), which is why this is a plain `&mut [u8]` rather than
    /// a cursor that could track partial fills.
    #[must_use]
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: the index came out of the free list and this is the only
        // handle naming it (no `Bytes` exists for it yet, and `seal` consumes
        // `self` to create one), so no other thread can read or write these
        // bytes; the offset and length lie within the mapping, which the frame's
        // `Arc` keeps alive.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.frame
                    .slab
                    .mapping
                    .ptr
                    .add(self.frame.slab.offset_of(self.frame.index)),
                self.frame.len,
            )
        }
    }

    /// Publish the filled frame as the refcounted `Bytes` the cache stores and a
    /// holder can WRITE from in place. The frame returns to the slab when the
    /// last clone of it drops.
    #[must_use]
    pub fn seal(self) -> Bytes {
        Bytes::from_owner(self.frame)
    }
}

/// One frame, owned by the `Bytes` the cache stores. Dropping it — i.e. dropping
/// the last clone of that `Bytes` — returns the frame to the slab.
pub struct CacheFrame {
    slab: Arc<SlabInner>,
    index: u32,
    len: usize,
}

impl AsRef<[u8]> for CacheFrame {
    fn as_ref(&self) -> &[u8] {
        // SAFETY: the frame's index is exclusively this handle's until it drops
        // (it is not in the free list), so nothing can write these bytes while
        // this reference lives; the offset and length are within the mapping,
        // which the `Arc` keeps alive at least as long as `self`.
        unsafe {
            std::slice::from_raw_parts(
                self.slab.mapping.ptr.add(self.slab.offset_of(self.index)),
                self.len,
            )
        }
    }
}

impl Drop for CacheFrame {
    fn drop(&mut self) {
        self.slab
            .free_frames
            .lock()
            .expect("the slab free list is only ever locked for a push/pop")
            .push(self.index);
    }
}

/// Frames a `slab_bytes` budget affords at `frame_bytes` each, or `None` if that
/// is too few to be worth pinning ([`MIN_USEFUL_FRAMES`]).
///
/// Separate from [`CacheSlab::new`] so the decision is testable without a device:
/// the interesting cases are all arithmetic (a budget smaller than one frame, a
/// budget that rounds down below the floor), and getting them wrong means either
/// pinning memory for a slab that cannot work or refusing one that would.
fn useful_frames(slab_bytes: usize, frame_bytes: usize) -> Option<usize> {
    let frames = slab_bytes / frame_bytes;
    (frames >= MIN_USEFUL_FRAMES).then_some(frames)
}

/// Offset of a `[ptr, ptr + len)` body within a `[base, base + mapped)` mapping,
/// or `None` if it does not lie wholly inside it.
///
/// Split out from [`CacheSlab::local_slice`] because it is the whole correctness
/// argument for pointer identity and the one part testable without a device: an
/// address that merely *looks* plausible must not produce an SGE, or a holder
/// would post arbitrary process memory to a peer. Takes `usize` rather than
/// pointers so the arithmetic cannot wrap or be UB on a provenance-free compare.
fn offset_within(base: usize, mapped: usize, ptr: usize, len: usize) -> Option<usize> {
    // A zero-length body has no meaningful SGE, and every real body has bytes.
    if len == 0 {
        return None;
    }
    let offset = ptr.checked_sub(base)?;
    // `offset + len` cannot wrap: both are `<= mapped` after this check, and
    // `mapped` is a real mapping length.
    (mapped.checked_sub(offset)? >= len).then_some(offset)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The bounds check, which is what stops a stray pointer becoming an SGE
    /// over unrelated process memory.
    #[test]
    fn only_bodies_wholly_inside_the_mapping_resolve() {
        const BASE: usize = 0x1_0000;
        const MAPPED: usize = 4096;
        // Exactly at the start, in the middle, and flush against the end.
        assert_eq!(offset_within(BASE, MAPPED, BASE, 16), Some(0));
        assert_eq!(offset_within(BASE, MAPPED, BASE + 100, 16), Some(100));
        assert_eq!(offset_within(BASE, MAPPED, BASE + 4080, 16), Some(4080));
        // The whole mapping as one body.
        assert_eq!(offset_within(BASE, MAPPED, BASE, MAPPED), Some(0));
        // Below the mapping, above it, and straddling its end by one byte.
        assert_eq!(offset_within(BASE, MAPPED, BASE - 1, 16), None);
        assert_eq!(offset_within(BASE, MAPPED, BASE + MAPPED, 16), None);
        assert_eq!(offset_within(BASE, MAPPED, BASE + 4081, 16), None);
        // A length that would wrap if the arithmetic were unchecked.
        assert_eq!(offset_within(BASE, MAPPED, BASE, usize::MAX), None);
        // An empty body: no SGE.
        assert_eq!(offset_within(BASE, MAPPED, BASE, 0), None);
    }

    /// The budget-to-frames decision, including the two ways a slab is refused.
    #[test]
    fn a_slab_is_only_built_when_the_budget_affords_useful_frames() {
        const FRAME: usize = 16 << 20;
        // Exactly the floor, and comfortably above it.
        assert_eq!(useful_frames(FRAME * MIN_USEFUL_FRAMES, FRAME), Some(4));
        assert_eq!(useful_frames(96 << 30, FRAME), Some(6144));
        // One frame short of the floor, a budget smaller than a single frame, and
        // nothing at all — the `0` default, which must not build a slab.
        assert_eq!(useful_frames(FRAME * (MIN_USEFUL_FRAMES - 1), FRAME), None);
        assert_eq!(useful_frames(FRAME - 1, FRAME), None);
        assert_eq!(useful_frames(0, FRAME), None);
        // A budget that is not a whole multiple rounds DOWN: a partial frame at
        // the end would be a frame that cannot hold a chunk.
        assert_eq!(useful_frames(FRAME * 5 + 1, FRAME), Some(5));
    }

    /// A pointer from a *different* allocation must not resolve just because its
    /// numeric value happens to be plausible — the reason `local_slice` checks
    /// bounds instead of trusting the caller to only pass frame-backed bytes.
    #[test]
    fn an_unrelated_allocation_does_not_resolve() {
        let other = [0u8; 64];
        let base = other.as_ptr() as usize + 4096;
        assert_eq!(offset_within(base, 4096, other.as_ptr() as usize, 64), None);
    }
}
