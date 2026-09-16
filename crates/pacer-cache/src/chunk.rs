//! Chunk addressing (ADR-0015): the cluster cache unit is a fixed-size, aligned
//! slice of an object, not the whole object. This module is the pure addressing
//! layer — key derivation and covering-chunk math — with no cache or I/O
//! dependency, so it unit-tests in isolation and the rest of B1 builds on it.
//!
//! Two invariants from ADR-0015 live here:
//!
//! - **The chunk size is part of the key** (`"{bucket}/{key}#{size}:{index}"`).
//!   Changing `chunk_size` changes every chunk key, so old and new entries never
//!   alias — they orphan (and LRU-evict), never corrupt. There is no separate
//!   epoch counter; key-embedding is the whole mechanism.
//! - **Placement is per chunk** (ADR-0014 contract, unchanged): the rendezvous
//!   hash scores a *chunk key*, so a hot large object spreads across many homes.
//!   That happens in `pacer-ring`; this module only produces the keys it scores.

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Default chunk size, 16 MiB (ADR-0015). Near foyer's happy range and S3
/// Express's sweet spot; the central Phase 3 tunable, finalized by the
/// restore-storm benchmark. Config-overridable (ADR-0013); pinned per cluster.
pub const DEFAULT_CHUNK_SIZE: u64 = 16 << 20;

/// Per-object metadata, cached once per object alongside chunk 0's home
/// (ADR-0015). It carries everything needed to (a) compute the covering chunk
/// set for a ranged read and (b) reproduce the S3 GET response headers, without
/// holding any object bytes. Keyed by the plain object key (`"{bucket}/{key}"`),
/// distinct from chunk keys (which carry `#{size}:{index}`), so the two never
/// collide in the cache.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectHeader {
    /// Total object length in bytes — the Content-Range denominator and the
    /// input to [`ChunkConfig::chunk_count`]/[`ChunkConfig::chunk_bounds`].
    pub object_len: u64,
    /// Backend ETag, replayed on hits.
    pub e_tag: Option<String>,
    /// Backend Content-Type, replayed on hits.
    pub content_type: Option<String>,
    /// Seconds since epoch, from the backend's Last-Modified.
    pub last_modified_epoch_secs: Option<i64>,
}

impl ObjectHeader {
    /// Build a header from the fields every backend GET/HEAD and every peer
    /// [`BlobMeta`](../../pacer_transport/struct.BlobStream.html) response already
    /// carries. Both the proxy (backend response) and the peer read-through path
    /// construct headers, so the field mapping lives here, once.
    pub fn new(
        object_len: u64,
        e_tag: Option<String>,
        content_type: Option<String>,
        last_modified_epoch_secs: Option<i64>,
    ) -> Self {
        Self {
            object_len,
            e_tag,
            content_type,
            last_modified_epoch_secs,
        }
    }
}

/// One cached chunk's bytes, and optionally the ETag of the object version they
/// came from (ADR-0015 cache unit; ADR-0032 § 7 for the ETag).
///
/// Per-object metadata still lives once in the [`ObjectHeader`] — the ETag here
/// is not a copy of it for convenience, it is a **version witness**. A reader
/// takes the header first (it needs `object_len` to compute the covering set), so
/// it knows which object version it is assembling; a chunk witnessed by a
/// different ETag cannot belong to that assembly. Without it, two writers racing
/// one key could leave a cache holding chunk 0 from one and chunk 1 from the
/// other — an object matching neither.
///
/// `None` on every chunk the read path fills: ADR-0007's world had one version
/// per key by construction, and those entries keep their pre-ADR-0032 on-disk
/// encoding byte for byte. Set by the write path, which does not learn the ETag
/// until `CompleteMultipartUpload`.
///
/// **Not validated in v1.** Immutable checkpoint keys (ADR-0015: a new name per
/// version) mean mixed-version assembly cannot arise for the target workload.
/// Carrying the witness unchecked is what makes turning the check on later a flag
/// flip instead of a cache-format migration.
///
/// The last chunk of an object may be shorter than `chunk_size` (see
/// [`ChunkConfig::chunk_bounds`]).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CachedChunk {
    /// The chunk's bytes, `[chunk_bounds.start, chunk_bounds.end)` of the object.
    pub body: Bytes,
    /// ETag of the object version these bytes belong to, or `None` for a chunk
    /// the read path filled. See the type docs for why it is unchecked in v1.
    pub e_tag: Option<String>,
}

impl CachedChunk {
    /// A chunk holding `body` with no version witness — the read-fill path.
    ///
    /// Entries built this way encode exactly as they did before ADR-0032, which
    /// is what lets a cache directory survive the rollout that adds the witness.
    pub fn new(body: Bytes) -> Self {
        Self { body, e_tag: None }
    }

    /// A chunk holding `body`, witnessed by the ETag of the object version it was
    /// written as — the ADR-0032 write path, after Complete named that version.
    pub fn versioned(body: Bytes, e_tag: String) -> Self {
        Self {
            body,
            e_tag: Some(e_tag),
        }
    }

    /// The sub-slice of this chunk that intersects the object byte range
    /// `[req_start, req_end)`, given the chunk spans `chunk_start..` in the
    /// object. Used to trim the first and last covering chunks to exactly the
    /// requested bytes (ADR-0015: whole chunks fill, but a sub-chunk range
    /// returns only its bytes). Middle chunks pass `req_start <= chunk_start` and
    /// `req_end >= chunk_start + body.len()`, yielding the whole body.
    ///
    /// Returns an empty `Bytes` if the ranges do not intersect (caller should
    /// not include such a chunk, but this stays total rather than panicking).
    pub fn slice_for(&self, chunk_start: u64, req_start: u64, req_end: u64) -> Bytes {
        let chunk_end = chunk_start + self.body.len() as u64;
        let lo = req_start.max(chunk_start);
        let hi = req_end.min(chunk_end);
        if lo >= hi {
            return Bytes::new();
        }
        // Offsets within this chunk's body.
        let from = (lo - chunk_start) as usize;
        let to = (hi - chunk_start) as usize;
        self.body.slice(from..to)
    }
}

/// How an object is split into cache chunks. The size is a wire-visible part of
/// every chunk key (see module docs), so it is fixed per cluster and never mixed
/// across a rolling update — changing it is a cache-flush event (orphan, never
/// corrupt).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkConfig {
    /// Bytes per chunk. Every chunk but the last is exactly this size; the last
    /// holds the remainder.
    chunk_size: u64,
}

impl Default for ChunkConfig {
    fn default() -> Self {
        Self {
            chunk_size: DEFAULT_CHUNK_SIZE,
        }
    }
}

impl ChunkConfig {
    /// A config with the given chunk size.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is zero — a zero-sized chunk has no valid index
    /// math and is a config error, not a runtime condition.
    pub fn new(chunk_size: u64) -> Self {
        assert!(chunk_size > 0, "chunk_size must be positive");
        Self { chunk_size }
    }

    /// Bytes per chunk.
    pub fn chunk_size(&self) -> u64 {
        self.chunk_size
    }

    /// The cache key for chunk `index` of object `object_key` (which is already
    /// `"{bucket}/{key}"`, see [`crate::object_key`]). The size is embedded so a
    /// changed `chunk_size` yields disjoint keys (ADR-0015).
    ///
    /// Format: `"{object_key}#{chunk_size}:{index}"`. The `#` cannot appear in
    /// the `bucket/key` prefix produced by [`crate::object_key`] for a
    /// well-formed request, so the split back out is unambiguous.
    pub fn chunk_key(&self, object_key: &str, index: u64) -> String {
        format!("{object_key}#{}:{index}", self.chunk_size)
    }

    /// The half-open index range `[first, last)` of chunks that cover byte range
    /// `[start, end)` of an object. `end` is exclusive. An empty byte range
    /// (`start >= end`) yields an empty chunk range.
    ///
    /// A sub-chunk byte range still pulls whole chunks (ADR-0015): up to ~2
    /// chunks for a range straddling a boundary. The ADR-0011 cost invariant
    /// holds — only covering chunks fill, never the whole object.
    pub fn covering(&self, start: u64, end: u64) -> std::ops::Range<u64> {
        if start >= end {
            let first = start / self.chunk_size;
            return first..first;
        }
        let first = start / self.chunk_size;
        // end is exclusive: the last covered byte is end-1, in chunk (end-1)/sz.
        let last = (end - 1) / self.chunk_size + 1;
        first..last
    }

    /// Number of chunks an object of `object_len` bytes occupies. Zero-length
    /// objects occupy zero chunks (they are below `min_object_size` anyway and
    /// never chunked in practice).
    pub fn chunk_count(&self, object_len: u64) -> u64 {
        object_len.div_ceil(self.chunk_size)
    }

    /// The byte range `[start, end)` chunk `index` spans within an object of
    /// `object_len` bytes. The last chunk is clamped to `object_len`, so it may
    /// be shorter than `chunk_size`. Returns `None` if `index` is past the end.
    pub fn chunk_bounds(&self, index: u64, object_len: u64) -> Option<std::ops::Range<u64>> {
        let start = index * self.chunk_size;
        if start >= object_len {
            // The one exception: a zero-length object has chunk 0 spanning 0..0.
            if object_len == 0 && index == 0 {
                return Some(0..0);
            }
            return None;
        }
        let end = (start + self.chunk_size).min(object_len);
        Some(start..end)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A small size makes the boundary math legible in tests.
    const SZ: u64 = 100;

    fn cfg() -> ChunkConfig {
        ChunkConfig::new(SZ)
    }

    #[test]
    fn key_embeds_size_so_a_resize_orphans() {
        let a = ChunkConfig::new(SZ).chunk_key("b/k", 3);
        let b = ChunkConfig::new(SZ * 2).chunk_key("b/k", 3);
        assert_ne!(a, b, "different chunk sizes must yield disjoint keys");
        assert_eq!(a, "b/k#100:3");
    }

    #[test]
    fn covering_whole_and_partial() {
        let c = cfg();
        // Whole first chunk exactly.
        assert_eq!(c.covering(0, 100), 0..1);
        // Sub-chunk range inside chunk 0.
        assert_eq!(c.covering(10, 50), 0..1);
        // Range straddling the 0/1 boundary pulls both.
        assert_eq!(c.covering(50, 150), 0..2);
        // Range aligned to a boundary: [100,200) is exactly chunk 1.
        assert_eq!(c.covering(100, 200), 1..2);
        // One byte into chunk 2.
        assert_eq!(c.covering(50, 201), 0..3);
    }

    #[test]
    fn covering_empty_range_is_empty() {
        let c = cfg();
        assert!(c.covering(100, 100).is_empty());
        assert!(c.covering(200, 100).is_empty());
    }

    #[test]
    fn chunk_count_rounds_up() {
        let c = cfg();
        assert_eq!(c.chunk_count(0), 0);
        assert_eq!(c.chunk_count(1), 1);
        assert_eq!(c.chunk_count(100), 1);
        assert_eq!(c.chunk_count(101), 2);
        assert_eq!(c.chunk_count(250), 3);
    }

    #[test]
    fn chunk_bounds_clamps_last_chunk() {
        let c = cfg();
        assert_eq!(c.chunk_bounds(0, 250), Some(0..100));
        assert_eq!(c.chunk_bounds(1, 250), Some(100..200));
        // Last chunk is short (only 50 bytes).
        assert_eq!(c.chunk_bounds(2, 250), Some(200..250));
        // Past the end.
        assert_eq!(c.chunk_bounds(3, 250), None);
    }

    #[test]
    fn covering_then_bounds_reconstructs_the_range() {
        let c = cfg();
        let (start, end, len) = (37, 219, 300);
        let chunks = c.covering(start, end);
        // The covering chunks' spans must together include [start, end).
        let first = c.chunk_bounds(chunks.start, len).unwrap();
        let last = c.chunk_bounds(chunks.end - 1, len).unwrap();
        assert!(first.start <= start);
        assert!(last.end >= end);
    }

    #[test]
    fn header_drives_the_chunk_set_for_a_whole_object() {
        let c = cfg();
        let header = ObjectHeader {
            object_len: 250,
            e_tag: Some("abc".into()),
            content_type: None,
            last_modified_epoch_secs: Some(42),
        };
        // A whole-object read covers exactly chunk_count chunks, each with valid
        // bounds; the spans tile [0, object_len) with no gap.
        let n = c.chunk_count(header.object_len);
        assert_eq!(n, 3);
        let mut next = 0;
        for i in 0..n {
            let b = c.chunk_bounds(i, header.object_len).unwrap();
            assert_eq!(b.start, next, "chunk {i} must start where the last ended");
            next = b.end;
        }
        assert_eq!(next, header.object_len, "chunks must tile the whole object");
    }

    #[test]
    fn slice_for_trims_first_last_and_passes_middle() {
        // A chunk spanning object bytes [100, 200) (100 bytes, values = index).
        let body = Bytes::from((100u8..200u8).collect::<Vec<u8>>());
        let chunk = CachedChunk::new(body);
        // Whole chunk (middle-chunk case: request covers it fully).
        assert_eq!(chunk.slice_for(100, 0, 1000).len(), 100);
        // First-chunk trim: request starts mid-chunk at 150 → [150,200) = 50 bytes.
        let s = chunk.slice_for(100, 150, 1000);
        assert_eq!(s.len(), 50);
        assert_eq!(s[0], 150);
        // Last-chunk trim: request ends mid-chunk at 130 → [100,130) = 30 bytes.
        let s = chunk.slice_for(100, 0, 130);
        assert_eq!(s.len(), 30);
        assert_eq!(s[29], 129);
        // Both ends inside this chunk → [150,180) = 30 bytes.
        let s = chunk.slice_for(100, 150, 180);
        assert_eq!(s.len(), 30);
        assert_eq!((s[0], s[29]), (150, 179));
        // Non-intersecting → empty (total, no panic).
        assert!(chunk.slice_for(100, 0, 100).is_empty());
        assert!(chunk.slice_for(100, 200, 300).is_empty());
    }

    #[test]
    fn header_new_maps_fields() {
        let h = ObjectHeader::new(
            4096,
            Some("etag".into()),
            Some("text/plain".into()),
            Some(7),
        );
        assert_eq!(h.object_len, 4096);
        assert_eq!(h.e_tag.as_deref(), Some("etag"));
        assert_eq!(h.content_type.as_deref(), Some("text/plain"));
        assert_eq!(h.last_modified_epoch_secs, Some(7));
    }

    #[test]
    fn header_clone_eq() {
        // The Serialize/Deserialize derives are exercised end-to-end by foyer in
        // the integration tests; here just pin Clone + PartialEq (used by the
        // read path to compare/replay headers) without pulling a codec dep.
        let header = ObjectHeader {
            object_len: 1 << 30,
            e_tag: Some("\"deadbeef\"".into()),
            content_type: Some("application/octet-stream".into()),
            last_modified_epoch_secs: Some(1_700_000_000),
        };
        assert_eq!(header.clone(), header);
    }
}

/// Property tests for [`ChunkConfig`]'s covering-chunk math (T5): the function
/// `chunked_body` in `pacer-daemon`'s `proxy.rs` calls straight through to
/// [`ChunkConfig::covering`], so property-testing it here (already `pub`, and
/// already unit-tested above) covers that call site with no dependency on, or
/// edit to, `proxy.rs`.
#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    /// Default proptest case count for this module — see the identical const
    /// in `pacer-cache::proptests` for why it is documentation, not an override:
    /// leaving `ProptestConfig` untouched keeps `PROPTEST_CASES` in effect.
    #[allow(dead_code)]
    const PROPTEST_CASES: u32 = 256;

    /// Chunk sizes generated below, bounded well above 0 (the one value
    /// [`ChunkConfig::new`] rejects) and well below the point where
    /// `index * chunk_size` in [`ChunkConfig::chunk_bounds`] could overflow
    /// `u64` for the indices [`MAX_LEN`] below can produce.
    const MAX_CHUNK_SIZE: u64 = 1 << 24; // 16 MiB (the default) × 1

    /// Upper bound for object lengths / byte offsets generated below — large
    /// enough to span thousands of chunks at the smallest [`MAX_CHUNK_SIZE`],
    /// far below where chunk-index arithmetic could overflow.
    const MAX_LEN: u64 = 1 << 34; // 16 GiB

    proptest! {
        /// `covering` never panics, for any chunk size the type can hold and any
        /// (possibly inverted) `start`/`end` pair — including `start > end`,
        /// which [`ChunkConfig::covering`] documents as yielding an empty range
        /// rather than rejecting.
        #[test]
        fn covering_never_panics(
            chunk_size in 1..=MAX_CHUNK_SIZE,
            start in 0..=MAX_LEN,
            end in 0..=MAX_LEN,
        ) {
            let _ = ChunkConfig::new(chunk_size).covering(start, end);
        }

        /// The covering set is empty exactly when the requested range is empty
        /// or inverted (`start >= end`) — never for a non-empty range, since a
        /// non-empty byte range always intersects at least one chunk.
        #[test]
        fn covering_emptiness_matches_range_emptiness(
            chunk_size in 1..=MAX_CHUNK_SIZE,
            start in 0..=MAX_LEN,
            end in 0..=MAX_LEN,
        ) {
            let covering = ChunkConfig::new(chunk_size).covering(start, end);
            prop_assert_eq!(covering.is_empty(), start >= end);
        }

        /// For a non-empty `[start, end)`, the covering chunk indices are
        /// contiguous and strictly increasing (guaranteed by construction, since
        /// `covering` returns a `Range<u64>` — this pins that it is never
        /// returned reversed or as a single degenerate point for a real span),
        /// and the chunks they name TILE with no gap or overlap and together
        /// span at least `[start, end)`: the first chunk starts at or before
        /// `start`, consecutive chunks in the set abut exactly, and the last
        /// chunk ends at or after `end`.
        #[test]
        fn covering_tiles_the_requested_range(
            chunk_size in 1..=MAX_CHUNK_SIZE,
            start in 0..=MAX_LEN,
            extra in 0..=MAX_LEN,
        ) {
            let end = start + 1 + extra; // always non-empty: end > start
            let cfg = ChunkConfig::new(chunk_size);
            let covering = cfg.covering(start, end);
            prop_assert!(covering.start < covering.end, "non-empty range must yield a non-empty, strictly increasing chunk-index set");

            // `object_len = end` is the smallest object this range could belong
            // to, so every covering index is guaranteed in-bounds.
            let object_len = end;
            let first_bounds = cfg.chunk_bounds(covering.start, object_len).unwrap();
            prop_assert!(first_bounds.start <= start, "the first covering chunk must start at or before the requested start");

            let mut next_start = first_bounds.start;
            for idx in covering.clone() {
                let bounds = cfg.chunk_bounds(idx, object_len).unwrap();
                prop_assert_eq!(bounds.start, next_start, "covering chunks must tile with no gap or overlap");
                next_start = bounds.end;
            }
            prop_assert!(next_start >= end, "the last covering chunk must end at or after the requested end");
        }

        /// `chunk_count` never panics, and its chunks — by [`chunk_bounds`] —
        /// tile `[0, object_len)` exactly (no gap, no overlap, no leftover
        /// byte), for any chunk size and object length the type can hold.
        #[test]
        fn chunk_count_tiles_the_whole_object(chunk_size in 1..=MAX_CHUNK_SIZE, object_len in 0..=MAX_LEN) {
            let cfg = ChunkConfig::new(chunk_size);
            let n = cfg.chunk_count(object_len);
            let mut next = 0u64;
            for idx in 0..n {
                let bounds = cfg.chunk_bounds(idx, object_len).unwrap();
                prop_assert_eq!(bounds.start, next);
                next = bounds.end;
            }
            prop_assert_eq!(next, object_len);
            // One past the last chunk is out of range — except `object_len ==
            // 0`, where `chunk_bounds` documents chunk 0 as the one index that
            // is always `Some(0..0)`, regardless of `chunk_count` (which is 0
            // there, since a zero-length object occupies no chunks).
            if object_len > 0 {
                prop_assert_eq!(cfg.chunk_bounds(n, object_len), None);
            }
        }
    }
}
