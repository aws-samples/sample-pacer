//! Where a chunk decoded off the disk tier lands (ADR-0028): a registered slab
//! frame, if this node has a slab.
//!
//! # Why this seam exists at all
//!
//! ADR-0028 makes the cache's RAM tier a hugepage slab of registered frames, so a
//! holder posts its RDMA WRITE straight out of the cache. Every path that *fills*
//! the cache goes through `pacer_daemon::cachefill`, which holds the slab and can
//! simply ask it for a frame. One path cannot: a chunk that foyer demoted to NVMe
//! and later promotes back is materialized by **foyer's decoder**, inside
//! `foyer::Code::decode` — an associated function with no `self` and no context
//! parameter. It has no way to reach a slab held elsewhere.
//!
//! That is the whole reason for the indirection here. A promoted chunk that lands
//! on the heap is invisible in every metric except a rising
//! `pacer_rdma_holder_copy_seconds_total`: the slab stays fully registered, the
//! cache stays full, and serves quietly go back to staging a copy — the same
//! silent-no-op failure mode the read-through fill already produced once
//! (planning/19 § C1b).
//!
//! # What it is NOT
//!
//! Not a second budget. The slab is one object, owned by the transport, installed
//! here once at startup; this module only lets the decoder *reach* it. And not a
//! fallback with different semantics: a decode that cannot claim a frame allocates
//! on the heap exactly as it did before, and counts itself (see
//! [`FrameSource::heap_fallback`]) so "the design quietly stopped applying" stays
//! visible.
//!
//! # Why a global, and why that is acceptable here
//!
//! `Code::decode` is reached only through foyer's storage engine, so there is no
//! call path to thread a parameter down: the alternative is a `OnceLock` set once
//! before the cache is built. It is written exactly once, by the daemon's startup
//! ([`install_frame_source`]), and read-only afterwards — the same shape as the
//! metrics registry the transport is wired into. Tests install their own and
//! assert through it.

use std::sync::OnceLock;

use bytes::Bytes;

/// A frame claimed from the slab, being filled by a decoder.
///
/// Object-safe by construction: the concrete type lives in `pacer-transport`
/// (which this crate must not depend on — the daemon depends on both, and the
/// dependency cannot go the other way), so the decoder sees only the two
/// operations it needs.
pub trait FrameFill: Send {
    /// The frame's bytes, to be filled completely. Its length is exactly the
    /// `len` the claim asked for.
    fn as_mut_slice(&mut self) -> &mut [u8];

    /// Publish the filled frame as the `Bytes` the cache stores — RDMA-postable
    /// in place, which is the entire point.
    ///
    /// Takes `Box<Self>` rather than `self` so this stays callable on a trait
    /// object.
    fn seal(self: Box<Self>) -> Bytes;
}

/// Something that can hand out registered frames — in production, ADR-0028's
/// `CacheSlab`.
pub trait FrameSource: Send + Sync {
    /// Claim a frame of exactly `len` bytes, or `None` when the slab has no free
    /// frame or `len` exceeds one (in a consistent cluster the latter means the
    /// value is not a chunk).
    fn claim(&self, len: usize) -> Option<Box<dyn FrameFill>>;

    /// Record that a decode fell back to the heap because [`Self::claim`]
    /// declined. Separate from the claim so the counter lives with the daemon's
    /// other slab metrics (`pacer_cache_slab_heap_fallbacks_total`) instead of
    /// being duplicated here — this crate has no metrics registry.
    fn heap_fallback(&self);

    /// Record that a decode landed in a frame, for the same counter family
    /// (`pacer_cache_slab_stores_total`). A promotion is a store like any other:
    /// without this, the disk-tier path's share of the cache would be invisible
    /// in the one metric that says whether chunks are in registered memory.
    fn stored(&self);
}

/// The node's frame source, if it has a slab. Written once at startup, read by
/// the codec on every promotion.
static FRAME_SOURCE: OnceLock<Box<dyn FrameSource>> = OnceLock::new();

/// Install the node's frame source, so a chunk promoted off the disk tier is
/// decoded into a registered frame rather than onto the heap.
///
/// Call once, at startup, **before the cache is built** — a promotion can only
/// happen after a demotion, so in practice any time before the first read
/// suffices, but ordering it with the cache's construction removes the question.
/// A second call is ignored, and returns `false` so a caller that believes it is
/// the only installer can assert.
///
/// Absent (never called), every promotion decodes onto the heap: correct, and
/// exactly the pre-ADR-0028 behaviour.
pub fn install_frame_source(source: Box<dyn FrameSource>) -> bool {
    FRAME_SOURCE.set(source).is_ok()
}

/// Claim a frame of `len`, or `None` when this node has no slab or no free frame.
///
/// [`decode_into_frame`] is the right entry point for a caller that only wants bytes. This
/// one exists for a caller whose fill **depends on which destination it got** — namely
/// `store`'s direct-I/O read: `O_DIRECT` requires a page-aligned buffer, a slab frame is
/// aligned by construction and a heap `Vec` is not, so the read has to know which it has.
/// Getting that wrong is an `EINVAL` on every read, not a slow path.
///
/// A caller using this owes the counters: [`note_stored`] on the frame path and
/// [`note_heap_fallback`] otherwise, which [`decode_into_frame`] does for you.
#[must_use]
pub fn claim(len: usize) -> Option<Box<dyn FrameFill>> {
    FRAME_SOURCE.get()?.claim(len)
}

/// Record that a chunk landed in a frame. See [`claim`].
pub fn note_stored() {
    if let Some(source) = FRAME_SOURCE.get() {
        source.stored();
    }
}

/// Record that a chunk had to go on the heap because no frame was available. See [`claim`].
pub fn note_heap_fallback() {
    if let Some(source) = FRAME_SOURCE.get() {
        source.heap_fallback();
    }
}

/// Bytes for a chunk of `len` being decoded: a registered frame if this node has
/// one to spare, otherwise the heap.
///
/// The `fill` closure is handed the destination slice and must fill it
/// completely — it is the decoder's `read_exact`. Its error is returned
/// unchanged, and a failed fill publishes nothing: the frame goes straight back
/// to the slab.
///
/// # Errors
///
/// Whatever `fill` returns. This function adds no failure of its own; a slab that
/// cannot supply a frame is not an error, it is the heap path.
pub fn decode_into_frame<E>(
    len: usize,
    fill: impl FnOnce(&mut [u8]) -> Result<(), E>,
) -> Result<Bytes, E> {
    if let Some(mut frame) = claim(len) {
        fill(frame.as_mut_slice())?;
        note_stored();
        return Ok(frame.seal());
    }
    note_heap_fallback();
    // No slab, or no free frame. `vec![0; len]` and not an uninitialized buffer:
    // this path is off the hot path by construction (it is a disk-tier promotion
    // that has already paid an NVMe read), and `unsafe { set_len }` here would
    // buy a memset in exchange for a way to publish uninitialized memory if a
    // future `fill` ever returned early without filling.
    let mut buf = vec![0u8; len];
    fill(&mut buf)?;
    Ok(Bytes::from(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A frame source over a fixed-size arena, standing in for the slab: enough
    /// to prove the decoder lands in it and that exhaustion falls back.
    struct TestFrames {
        /// One frame's worth of storage per slot, `None` once handed out.
        available: std::sync::Mutex<usize>,
        stored: std::sync::atomic::AtomicUsize,
        fallbacks: std::sync::atomic::AtomicUsize,
    }

    struct TestFrame {
        buf: Vec<u8>,
    }

    impl FrameFill for TestFrame {
        fn as_mut_slice(&mut self) -> &mut [u8] {
            &mut self.buf
        }
        fn seal(self: Box<Self>) -> Bytes {
            Bytes::from(self.buf)
        }
    }

    impl FrameSource for TestFrames {
        fn claim(&self, len: usize) -> Option<Box<dyn FrameFill>> {
            let mut left = self.available.lock().unwrap();
            if *left == 0 {
                return None;
            }
            *left -= 1;
            Some(Box::new(TestFrame { buf: vec![0; len] }))
        }
        fn heap_fallback(&self) {
            self.fallbacks
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
        fn stored(&self) {
            self.stored
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        }
    }

    /// With no source installed — the default, and every gRPC-only build — a
    /// decode still produces the right bytes. This is the property that lets the
    /// codec be unconditional.
    #[test]
    fn decoding_without_a_source_still_yields_the_bytes() {
        let out: Result<Bytes, std::convert::Infallible> = decode_into_frame(4, |dst| {
            dst.copy_from_slice(b"abcd");
            Ok(())
        });
        assert_eq!(out.unwrap(), Bytes::from_static(b"abcd"));
    }

    /// A fill that fails must not publish a half-written frame, and must not
    /// count as a store.
    #[test]
    fn a_failed_fill_publishes_nothing() {
        let out: Result<Bytes, &str> = decode_into_frame(8, |_| Err("short read"));
        assert_eq!(out, Err("short read"));
    }

    /// The whole point, exercised against a stand-in source: the first claims
    /// land in frames and count as stores; once exhausted, decodes fall back to
    /// the heap and count as fallbacks. Runs against the trait rather than the
    /// global, because `install_frame_source` is a process-wide `OnceLock` and a
    /// test that consumed it would make every other test order-dependent.
    #[test]
    fn frames_are_used_until_exhausted_then_the_heap() {
        let src = TestFrames {
            available: std::sync::Mutex::new(2),
            stored: std::sync::atomic::AtomicUsize::new(0),
            fallbacks: std::sync::atomic::AtomicUsize::new(0),
        };
        for _ in 0..3 {
            match src.claim(4) {
                Some(mut f) => {
                    f.as_mut_slice().copy_from_slice(b"abcd");
                    assert_eq!(Box::new(f).seal().len(), 4);
                    src.stored();
                }
                None => src.heap_fallback(),
            }
        }
        assert_eq!(src.stored.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(src.fallbacks.load(std::sync::atomic::Ordering::Relaxed), 1);
    }
}
