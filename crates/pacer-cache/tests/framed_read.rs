//! The chunk store's **primary** read path: the body read lands in a slab frame, and the
//! served bytes are that frame at the body's true length.
//!
//! # Why this is an integration test and not a unit test
//!
//! `frames::install_frame_source` is a process-wide `OnceLock` by design — `foyer::Code::decode`
//! is an associated function with no `self`, so the codec has no other way to reach a slab. That
//! makes it untestable from inside the crate's unit tests: installing a source there would
//! silently change which branch *every other* store test takes, and `cargo test` runs them in
//! one process on many threads, so the result would be order-dependent.
//!
//! An integration test is its own binary, so the global starts unset and only this file's
//! expectations apply. Without this, the branch that production actually runs is exercised
//! nowhere but a paid hardware arm — the unit tests all fall through to the heap path, because
//! no source is installed there.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pacer_cache::chunk::CachedChunk;
use pacer_cache::frames::{FrameFill, FrameSource};
use pacer_cache::slot::SLOT_HEADER_BYTES;
use pacer_cache::store::{ChunkStore, StoreConfig};

/// Body size of the tier's chunks. Small enough to keep the test quick, and a page multiple so
/// the store's alignment guard accepts it.
const CHUNK: usize = 64 << 10;
/// Slots in the test tier.
const SLOTS: u64 = 8;

/// What the test observes about the source it installed. Behind an `Arc` because
/// `install_frame_source` takes ownership of the source, and without a handle to these the
/// test cannot tell the framed branch from the heap one — which is the whole point.
#[derive(Default)]
struct Counters {
    stored: AtomicUsize,
    fallbacks: AtomicUsize,
}

/// A stand-in slab: frames exactly one chunk long, which is the geometry the transport maps
/// now that slot headers live in their own region and no longer share the body's read
/// (`ArenaConfig::slab_frame_headroom` is back to 0).
struct TestSlab {
    free: Mutex<usize>,
    counters: Arc<Counters>,
}

/// Alignment a frame must have for the store to read into it **at all**.
///
/// `ChunkStore::read_body` reads a claimed frame through the extent's `O_DIRECT` descriptor
/// (`store.rs`, and its own `None` arm says why a heap `Vec` cannot be used there), and
/// `O_DIRECT` demands a block-aligned buffer address: an unaligned one fails the read with
/// `EINVAL`, not silently. 4 KiB is the block size on every filesystem this runs on, and the
/// real slab is a hugepage mapping, so production satisfies it trivially.
const FRAME_ALIGNMENT: usize = 4 << 10;

/// One frame, page-aligned inside a larger allocation.
///
/// ⚠ This used to be a bare `vec![0u8; len]`, on the reasoning that the test was about the
/// store's *slicing* rather than about `O_DIRECT`. **That made the test a coin flip on Linux**:
/// a `Vec`'s guaranteed alignment is its element's, so the read succeeded only when the
/// allocator happened to hand back a block-aligned pointer — which it usually does for a 64 KiB
/// request routed through `mmap`, and did not on 2026-09-12, when the same commit that passed
/// on one runner failed `EINVAL` on another after the test count changed. It cannot fail on
/// macOS at all (no `O_DIRECT`), so the laptop inner loop never sees it.
///
/// Over-allocate and slice rather than allocate aligned: `Bytes::from(Vec)` frees with the
/// `Vec`'s own layout, so handing it a buffer allocated under a different alignment would be
/// undefined behaviour. A `Bytes::slice` of the whole allocation keeps it alive and hands back
/// exactly the aligned region.
struct TestFrame {
    buf: Vec<u8>,
    /// Offset of the aligned region inside `buf`.
    at: usize,
    /// The claimed length, i.e. what the store will read.
    len: usize,
}

impl TestFrame {
    /// A frame of `len` bytes whose first byte is [`FRAME_ALIGNMENT`]-aligned.
    fn aligned(len: usize) -> Self {
        let buf = vec![0u8; len + FRAME_ALIGNMENT];
        let at = buf.as_ptr().align_offset(FRAME_ALIGNMENT);
        assert!(
            at < FRAME_ALIGNMENT,
            "a {FRAME_ALIGNMENT}-byte alignment must be reachable within the padding"
        );
        Self { buf, at, len }
    }
}

impl FrameFill for TestFrame {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.buf[self.at..self.at + self.len]
    }
    fn seal(self: Box<Self>) -> Bytes {
        let Self { buf, at, len } = *self;
        Bytes::from(buf).slice(at..at + len)
    }
}

impl FrameSource for TestSlab {
    fn claim(&self, len: usize) -> Option<Box<dyn FrameFill>> {
        // The real slab refuses a claim larger than one frame; mirror that, because the store
        // asking for more than a frame holds is precisely the bug this would hide. A body read
        // never asks for more than a chunk — if it ever asks for `chunk + header` again, the
        // headers have crept back in front of the bodies and the alignment went with them.
        if len > CHUNK {
            return None;
        }
        let mut free = self.free.lock().expect("test slab lock");
        if *free == 0 {
            return None;
        }
        *free -= 1;
        Some(Box::new(TestFrame::aligned(len)))
    }
    fn heap_fallback(&self) {
        self.counters.fallbacks.fetch_add(1, Ordering::Relaxed);
    }
    fn stored(&self) {
        self.counters.stored.fetch_add(1, Ordering::Relaxed);
    }
}

/// **The primary path, end to end.** With a frame source installed, a read must take the
/// framed branch and return the body's true bytes at its true length, for a full chunk and for
/// the short last chunk an object ends with.
///
/// The `stored` counter is the proof it took that branch: the heap branch would leave it at
/// zero and the assertions on the bytes would still pass, which is exactly how this path could
/// rot unnoticed.
#[tokio::test(flavor = "multi_thread")]
async fn a_framed_read_serves_the_body_at_its_true_length() {
    let dir = tempfile::tempdir().unwrap();
    let store = ChunkStore::open(StoreConfig {
        dir: dir.path().to_path_buf(),
        chunk_size: CHUNK,
        capacity_bytes: SLOTS * (SLOT_HEADER_BYTES as u64 + CHUNK as u64),
        verify_body: true,
        read_shape: pacer_cache::store::ReadShape::TwoRead,
        read_concurrency: 0,
    })
    .await
    .unwrap();

    let counters = Arc::new(Counters::default());
    assert!(
        pacer_cache::frames::install_frame_source(Box::new(TestSlab {
            free: Mutex::new(4),
            counters: Arc::clone(&counters),
        })),
        "this binary must be the only installer"
    );

    // A full chunk, and a short one — the case whose length is not a page multiple, so the
    // read rounds up and the served slice must still be exact.
    for (nth, len) in [CHUNK, 1234usize].into_iter().enumerate() {
        let key = format!("bucket/obj#100:{nth}");
        // Cast: a small loop index.
        #[allow(clippy::cast_possible_truncation)]
        let tag = (nth as u8) + 0xA0;
        store
            .put(&key, &CachedChunk::new(Bytes::from(vec![tag; len])))
            .await
            .unwrap();
        let got = store
            .get(&key)
            .await
            .unwrap()
            .unwrap_or_else(|| panic!("len {len} must read back"));
        assert_eq!(got.body.len(), len, "len {len} must be its true length");
        assert!(
            got.body.iter().all(|&b| b == tag),
            "len {len} must be its own bytes — not the header, not padding, not a neighbour's"
        );
    }

    // THE DISCRIMINATING ASSERTION. Everything above passes on the heap branch too, so
    // without this the test would happily pass with the framed path bypassed entirely.
    assert_eq!(
        counters.stored.load(Ordering::Relaxed),
        2,
        "both reads must have come through a FRAME — otherwise this test proves nothing about \
         the path it exists to cover"
    );
    assert_eq!(
        counters.fallbacks.load(Ordering::Relaxed),
        0,
        "four frames were offered for two reads, so nothing should have fallen back"
    );

    // And with `verify_body` on, a body read from the wrong offset — the header region rather
    // than the body region, say — would have failed its CRC and come back as a miss rather
    // than as wrong bytes.
    let stats = store.stats();
    assert_eq!(stats.hits.load(Ordering::Relaxed), 2);
    assert_eq!(stats.crc_mismatches.load(Ordering::Relaxed), 0);
    assert_eq!(stats.key_mismatches.load(Ordering::Relaxed), 0);
}
