//! Where a cached chunk's bytes live (ADR-0028), for every path that fills the
//! cache.
//!
//! This module exists because getting it right in *most* places is the same as
//! getting it wrong. ADR-0028's payoff is that a holder posts its RDMA WRITE
//! straight out of the cache, which only happens for chunks whose bytes are in a
//! registered slab frame — so a fill path that quietly allocates on the heap
//! silently opts its chunks out of the whole design. That is exactly what
//! happened on first measurement: the proxy's two fill paths used the slab, the
//! peer server's read-through fill did not, and a holder with a cold cache
//! therefore staged a copy for every serve while `pacer_cache_slab_stores_total`
//! sat at 0 with a fully registered slab (planning/19 § C1b).
//!
//! So the policy lives here once, and the three fill sites call it:
//! [`crate::proxy`]'s owner fill and layer-1 admit, and [`crate::peer`]'s
//! read-through fill.
//!
//! There is a **fourth** filler that cannot call it, and this module also owns the
//! adapter that reaches it: a chunk foyer demoted to NVMe and later promotes is
//! materialized inside foyer's decoder, which has no `self` to carry a slab. See
//! `install_promotion_frames` below and `pacer_cache::frames`. (No doc link, for the
//! same reason as the one on `DeliveryMetrics` in `metrics.rs`: the item is
//! feature-gated, so a link breaks the default doc build.)

use bytes::Bytes;

use crate::metrics::Metrics;

/// The cache's RAM tier when this node has an ADR-0028 slab: cloneable (an `Arc`
/// inside), so every fill path can hold one.
///
/// Absent on a gRPC-only build — there is no registered memory to be in, so the
/// type is uninhabited and [`ChunkFill::cached_bytes`] is a refcount bump.
#[cfg(feature = "efa")]
type Slab = pacer_transport::efa::CacheSlab;

/// Decides where a freshly-fetched chunk's bytes are put before the cache takes
/// them. One value, constructed at startup, shared by every fill path.
#[derive(Clone, Default)]
pub struct ChunkFill {
    /// `None` on a node with no slab (the default, and every gRPC-only build):
    /// chunks go on the heap and holders stage their WRITEs, exactly as before
    /// ADR-0028.
    #[cfg(feature = "efa")]
    slab: Option<Slab>,
}

impl ChunkFill {
    /// A fill policy backed by `slab`, or by the heap when it is `None`.
    ///
    /// Takes an owned `Option` rather than a borrow because this is built once at
    /// startup from the transport's slab and then cloned into each fill path.
    #[cfg(feature = "efa")]
    #[must_use]
    pub fn new(slab: Option<Slab>) -> Self {
        Self { slab }
    }

    /// Publish the slab's registered size to `pacer_cache_slab_bytes` (0 without a
    /// slab). Call once at startup, right after construction.
    ///
    /// Startup rather than per-store, because the quantity is true from startup:
    /// the mapping is registered and resident before any chunk is cached. A gauge
    /// updated only on the first store would read 0 on a node whose slab is
    /// configured but idle, and anything subtracting it would then attribute the
    /// whole slab to a leak — the precise failure this metric exists to prevent.
    pub fn publish_slab_size(&self, metrics: &Metrics) {
        #[cfg(feature = "efa")]
        let bytes = self.slab.as_ref().map_or(0, Slab::registered_bytes);
        #[cfg(not(feature = "efa"))]
        let bytes = 0_usize;
        #[allow(clippy::cast_precision_loss)]
        metrics.slab.bytes.set(bytes as f64);
    }

    /// Bytes safe to hand the cache, in the right place.
    ///
    /// With a slab: copies into a frame, so the chunk is RDMA-postable in place
    /// and a holder serving it skips the staging copy (`holder_copy`, measured at
    /// 0.348 CPU-s/GiB before this and exactly 0.000 after — planning/19 § C1b).
    /// A frame that cannot be claimed falls back to the heap and counts
    /// `pacer_cache_slab_heap_fallbacks_total`; see the module doc for why that
    /// counter is the one to watch rather than throughput.
    ///
    /// Without a slab: [`retained_copy`], whose own doc explains why the RDMA
    /// build cannot simply clone.
    #[must_use]
    pub fn cached_bytes(&self, data: &Bytes, metrics: &Metrics) -> Bytes {
        #[cfg(feature = "efa")]
        if let Some(slab) = &self.slab {
            return match slab.store(data) {
                Some(framed) => {
                    metrics.slab.stores.inc();
                    metrics.slab.frames_in_use.set(slab.frames_in_use() as f64);
                    framed
                }
                None => {
                    metrics.slab.heap_fallbacks.inc();
                    retained_copy(data)
                }
            };
        }
        // Referenced on the gRPC-only build, where the slab field does not exist.
        let _ = metrics;
        retained_copy(data)
    }
}

/// Make chunks promoted off the disk tier land in slab frames too, by installing
/// the process-wide frame source `pacer_cache`'s codec consults.
///
/// Call once at startup, alongside [`ChunkFill::new`] and from the same slab. A
/// no-op without one — and returns whether it installed, so a second call (which
/// would mean two slabs, i.e. a bug) is visible rather than silent.
///
/// **Why this is not just another fill site.** The other three hold a `ChunkFill`
/// and ask it for bytes. foyer's decoder cannot: `foyer::Code::decode` is an
/// associated function reached only from inside the storage engine. A promotion
/// that lands on the heap is invisible in every metric except a
/// `holder_copy_seconds` that stopped being zero — the same shape as the
/// read-through fill's silent opt-out (module doc), which is why this exists at
/// all rather than being left to a later "promotions are rare" argument.
#[cfg(feature = "efa")]
pub fn install_promotion_frames(slab: Option<Slab>, metrics: &Metrics) -> bool {
    let Some(slab) = slab else { return false };
    pacer_cache::frames::install_frame_source(Box::new(PromotionFrames {
        slab,
        metrics: metrics.clone(),
    }))
}

/// Adapter from ADR-0028's slab to the seam `pacer_cache`'s codec decodes
/// through. Lives here rather than in either crate because it is the one place
/// that legitimately knows both: `pacer-cache` must not depend on the transport
/// (the daemon depends on both, and the graph cannot invert), and the transport
/// has no metrics registry.
#[cfg(feature = "efa")]
struct PromotionFrames {
    slab: Slab,
    /// Cloned, not borrowed: this value is installed for the process's lifetime,
    /// and a `Metrics` clone shares the same registered counters.
    metrics: Metrics,
}

#[cfg(feature = "efa")]
impl pacer_cache::frames::FrameSource for PromotionFrames {
    fn claim(&self, len: usize) -> Option<Box<dyn pacer_cache::frames::FrameFill>> {
        Some(Box::new(PromotionFrame(self.slab.claim(len)?)))
    }

    fn heap_fallback(&self) {
        self.metrics.slab.heap_fallbacks.inc();
    }

    fn stored(&self) {
        self.metrics.slab.stores.inc();
        self.metrics
            .slab
            .frames_in_use
            .set(self.slab.frames_in_use() as f64);
    }
}

/// One claimed frame, seen through the seam's two operations.
#[cfg(feature = "efa")]
struct PromotionFrame(pacer_transport::efa::FrameWriter);

#[cfg(feature = "efa")]
impl pacer_cache::frames::FrameFill for PromotionFrame {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        self.0.as_mut_slice()
    }

    fn seal(self: Box<Self>) -> Bytes {
        self.0.seal()
    }
}

/// Detach `data` into an owned allocation safe to retain in the cache
/// indefinitely.
///
/// On a build with the RDMA plane (`efa`), a peer-fetched `Bytes` may own a
/// requester arena range (the zero-copy fetch path, planning/16 §5); a plain
/// `clone()` only bumps its refcount, so caching it would pin that scarce range
/// for the chunk's entire cache lifetime and starve the arena. Copying into a
/// fresh, unregistered `Bytes` breaks that ownership so the range is released the
/// moment the client stream drops its handle. Admission is rare (frequency-gated
/// and byte-budgeted, ADR-0016), so this copy is off the throughput hot path.
///
/// On a gRPC-only build no arena-backed `Bytes` exists, so this stays a refcount
/// bump — the fallback path is byte-for-byte unchanged.
#[cfg(feature = "efa")]
#[must_use]
pub fn retained_copy(data: &Bytes) -> Bytes {
    Bytes::copy_from_slice(data)
}

/// gRPC-only counterpart of the `efa` [`retained_copy`]: no arena-backed bytes
/// exist, so retaining a refcounted clone is correct and copy-free.
#[cfg(not(feature = "efa"))]
#[must_use]
pub fn retained_copy(data: &Bytes) -> Bytes {
    data.clone()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The no-slab path must still detach on an RDMA build, since the source may
    /// own an arena range.
    #[test]
    #[cfg(feature = "efa")]
    fn retained_copy_detaches_from_the_source_allocation() {
        let src = Bytes::from_static(b"chunk bytes");
        let retained = retained_copy(&src);
        assert_eq!(retained, src);
        assert!(!std::ptr::eq(retained.as_ptr(), src.as_ptr()));
    }

    /// On the gRPC-only build the same call is a refcount bump: nothing it could
    /// be holding is scarce.
    #[test]
    #[cfg(not(feature = "efa"))]
    fn retained_copy_is_a_refcount_bump_without_the_rdma_plane() {
        let src = Bytes::from_static(b"chunk bytes");
        let retained = retained_copy(&src);
        assert!(std::ptr::eq(retained.as_ptr(), src.as_ptr()));
    }

    /// A default `ChunkFill` (no slab) returns usable bytes on either build —
    /// the property every fill path depends on when ADR-0028 is off.
    #[test]
    fn a_slabless_fill_still_yields_the_same_bytes() {
        let metrics = Metrics::new().expect("metrics registry");
        let src = Bytes::from_static(b"chunk bytes");
        assert_eq!(ChunkFill::default().cached_bytes(&src, &metrics), src);
    }
}
