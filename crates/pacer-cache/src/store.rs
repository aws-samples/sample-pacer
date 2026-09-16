//! The chunk store: the disk tier for chunk bodies (ADR-0033).
//!
//! **One DMA per hit, from the drive straight into an ADR-0028 slab frame.** `O_DIRECT`, so
//! no page cache; a registered hugepage frame, so no heap staging buffer; a per-thread
//! aligned scratch for the slot header, so nothing is allocated on the serve path at all.
//! No entry codec and no checksum pass on the default path either — the three things that
//! made foyer charge **40–75 ms** for a 16 MiB entry the device serves in **1.335 ms**
//! (`bench/ladder/results/nvme-device-truth.md`).
//!
//! The buffered version of this path measured **26.9–34.9 ms** of service time per 16 MiB —
//! 20–26× the device — and the width sweep showed that cost, not concurrency, is the wall
//! (`c5-dcp-store-width-sweep.md`). Removing the page-cache copy is the response.
//!
//! # Where the headers are, and what that is NOT for
//!
//! Slot headers are **collected into a region at the front of each extent**, not laid one in
//! front of each body. That costs a second (4 KiB, ~82 µs) read per hit and buys a startup
//! scan that reads 4096 headers in one 16 MiB I/O instead of 4096 scattered 4 KiB ones.
//!
//! **It is not a throughput change, and the arithmetic that says it should have been is
//! wrong.** Interleaved headers made the stride `chunk_size + 4096`, so nearly every body read
//! began part-way into a 512 KiB stripe chunk and spanned 33 chunks instead of 32 — which looks
//! like one member serving five chunk-units where the others serve four, and is not, because the
//! head and tail chunks are partial and every member still serves exactly 2 MiB. An fio sweep
//! across a whole stripe moved throughput **0.046 %**
//! (`bench/ladder/results/nvme-stripe-offset.md`). Do not re-derive the straggler; it was
//! measured and it is not there.
//!
//! # Why this can be so much simpler than a cache
//!
//! Every simplification here is a consequence of ADR-0015 rather than a new constraint:
//! chunks are **fixed-size**, so the tier is an array of equal slots and there is no
//! allocator; they are **immutable** and named by a key that embeds the chunk size, so a
//! slot is written once and a resize orphans rather than aliases; and eviction is
//! therefore just slot reuse. What is left is an index, a free list, an LRU order, and
//! two syscalls.
//!
//! # The failure mode this must not open
//!
//! Dropping foyer's unconditional XxHash64 gives up a media check. It must not also give
//! up the check on **our own** bookkeeping: an index that names a slot holding another
//! key's bytes would serve them. So the slot header carries the key and every read
//! compares it ([`slot::SlotHeader`]), a mismatch is a miss with its own counter, and
//! that is why the header is read even when the body CRC is not verified.
//!
//! # Concurrency, and why it is CAPPED rather than maximised
//!
//! The index is one `Mutex` held only for lookups and slot bookkeeping — never across a
//! syscall. The reads themselves are `pread` on `&File` from `spawn_blocking`, so they proceed
//! in parallel — but **no more than [`DEFAULT_READ_CONCURRENCY`] of them at a time**, and that
//! ceiling is the one thing here that raises throughput rather than bounding it.
//!
//! The array saturates at ~4 concurrent 16 MiB reads, because `max_hw_sectors_kb` is 128 and
//! one `pread` is therefore already ~128 device requests fanned across the stripe. Past a
//! shallow knee, width is queueing: measured at 48.7 GiB/s at depth 16 against 43.1 at 48, with
//! service time linear in depth throughout (`bench/ladder/nvme-probe.sh store`). Nothing above
//! this module bounds the total — `fill_parallelism` is per client GET and
//! `delivery.parallelism` is per delivery request, so N concurrent requests multiply — and a
//! real arm was measured at ~59 in flight. **That, and not the read path, is where the 40–43.8
//! ms per-hit service times in `results/c5-dcp-store-odirect.md` came from.**
//!
//! # On-disk layout
//!
//! ```text
//! extent file = [ header region: SLOTS_PER_EXTENT × 4 KiB ][ body region: slots × chunk_size ]
//!
//!   header(slot) at  slot × 4 KiB                              — page-aligned
//!   body(slot)   at  HEADER_REGION_BYTES + slot × chunk_size    — stripe-aligned
//! ```
//!
//! A slot still *costs* `4 KiB + chunk_size` ([`StoreConfig::slot_bytes`]) because the header
//! region is one page per slot, which is why collecting them changed the addressing and left
//! the capacity arithmetic untouched.

use std::collections::HashMap;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;

use crate::chunk::CachedChunk;
use crate::frames;
use crate::slot::{self, SlotHeader, SlotState, SLOT_HEADER_BYTES};

/// Slots per extent file. The fd budget bounds *extents*, not slots — which is the
/// constraint foyer's one-fd-per-block layout hit at ~1 TiB. At a 16 MiB chunk this is a
/// 64 GiB file, so a 28 TB tier is ~437 fds.
const SLOTS_PER_EXTENT: usize = 4 << 10;

/// Bytes of slot headers at the front of every extent: one page per slot the extent can
/// hold, collected into one contiguous region ahead of all the bodies.
///
/// **Why they are collected: the startup scan.** The region is contiguous, so one 16 MiB read
/// recovers a whole extent's 4096 headers where interleaved headers needed 4096 scattered 4 KiB
/// reads and a `SCAN_CONCURRENCY`-wide fan-out to hide them. It also makes a body read exactly
/// `chunk_size`, so a slab frame is exactly a chunk (`slab_frame_headroom` is back to 0).
///
/// **Why NOT: stripe alignment.** Collecting the headers does incidentally align every body
/// offset to [`STRIPE_BYTES`], and that was the original stated reason — a misaligned 16 MiB
/// read spans 33 stripe chunks instead of 32, which was argued to leave one member serving five
/// chunk-units where the others serve four. **Measured at 0.046 % across a full-stripe fio
/// sweep** (`bench/ladder/results/nvme-stripe-offset.md`), because the head and tail chunks are
/// *partial*: `(512K − r) + 3 × 512K + r` is 2 MiB, the same 2 MiB every other member serves.
/// The alignment is kept because it is free, not because it buys anything.
const HEADER_REGION_BYTES: usize = SLOTS_PER_EXTENT * SLOT_HEADER_BYTES;

/// Full-stripe width of the instance-store RAID0 the tier sits on: 8 members × a 512 KiB
/// chunk, read from sysfs on a p5.48xlarge (`bench/ladder/results/nvme-device-truth.md`
/// § device inventory — `md127 raid0, chunk_bytes 524288, raid_disks 8`).
///
/// Referenced only by the layout's alignment invariant and never used to address anything, and
/// on the evidence it is **documentation rather than a lever** — see [`HEADER_REGION_BYTES`].
/// It is a property of the node in any case, so it could not be enforced: a different array
/// shape makes the alignment merely irrelevant, never wrong.
const STRIPE_BYTES: usize = 4 << 20;

/// The alignment [`HEADER_REGION_BYTES`] happens to give the bodies, checked at compile time
/// so a change to either constant that quietly loses it is visible.
///
/// A *cheap* invariant to hold, not a load-bearing one: the sweep that measured alignment at
/// 0.046 % is the reason this is an assertion about the layout's tidiness rather than about its
/// performance.
const _: () = assert!(
    HEADER_REGION_BYTES.is_multiple_of(STRIPE_BYTES),
    "the header region should be a whole number of RAID0 stripes, so that a body offset is one \
     too — free here, and the layout is easier to reason about with it than without"
);

/// Alignment O_DIRECT requires of every buffer, offset and length.
///
/// The requirement is the device's *logical* block size — 512 or 4096 on NVMe. One page
/// satisfies both, is what [`SLOT_HEADER_BYTES`] already is, and is what the slot layout was
/// built around so both a header entry and a body start page-aligned.
const DIRECT_IO_ALIGN: usize = 4 << 10;

/// Byte offset of slot `within`'s header entry, inside its extent.
const fn header_offset(within: u64) -> u64 {
    within * SLOT_HEADER_BYTES as u64
}

/// Byte offset of slot `within`'s body, inside its extent — after the whole header region.
const fn body_offset(within: u64, chunk_size: u64) -> u64 {
    HEADER_REGION_BYTES as u64 + within * chunk_size
}

/// Slots extent `index` holds: a full extent, or whatever is left over for the last one.
///
/// The header region is [`HEADER_REGION_BYTES`] in **every** extent, full or partial, so a
/// body offset is the same arithmetic everywhere and stays stripe-aligned in the last extent
/// too. Only the body region shrinks, which is what keeps [`reserve`] from claiming space for
/// slots the tier does not have.
fn slots_in_extent(slot_count: usize, index: usize) -> usize {
    slot_count
        .saturating_sub(index * SLOTS_PER_EXTENT)
        .min(SLOTS_PER_EXTENT)
}

/// Filename of extent `index` within the store directory.
fn extent_name(index: usize) -> String {
    format!("chunks-{index:05}.slots")
}

/// Chunk reads in flight, node-wide, when nothing overrides it.
///
/// **The measured knee, and it is a CEILING that raises throughput rather than a limit that
/// costs some.** `bench/ladder/nvme-probe.sh store` on one p5, 256 GiB per point, two reps:
///
/// ```text
/// depth        8      16      24      48
/// GiB/s     47.9    48.3    46.5    43.2      (rep 2: 47.9, 48.3, 47.2)
/// svc ms    2.58    5.14    8.03   17.17
/// ```
///
/// 16 is +11.9 % over the 48 an unbounded daemon reaches, and service is *linear* in depth
/// throughout — the extra width is queueing and nothing else, because one 16 MiB `pread` is
/// already ~128 device requests (`max_hw_sectors_kb=128`) and the array saturates at ~4.
///
/// It is also why every earlier write-up reported 40-43.8 ms of per-hit service against a
/// device that does 16 MiB in 1.3: those arms ran ~59 reads in flight, far past this knee.
pub const DEFAULT_READ_CONCURRENCY: usize = 16;

/// Round `n` up to a multiple of [`DIRECT_IO_ALIGN`].
const fn align_up(n: usize) -> usize {
    n.next_multiple_of(DIRECT_IO_ALIGN)
}

/// A page-aligned buffer for a direct read whose destination is NOT a slab frame.
///
/// Two callers, both of them reading slot headers: the serve path's per-thread scratch
/// (`HEADER_SCRATCH`, one page) and the startup scan's whole-region buffer
/// ([`HEADER_REGION_BYTES`]). A `Vec` cannot serve either — `O_DIRECT` demands a
/// page-aligned buffer and `Vec`'s alignment is its element's — and the serve path must
/// allocate nothing per read, which is what the thread-local is for.
struct AlignedBuf {
    ptr: std::ptr::NonNull<u8>,
    len: usize,
}

// SAFETY: the allocation is owned solely by this value and only reached through `&mut self`,
// so moving it between threads moves the whole buffer.
unsafe impl Send for AlignedBuf {}

impl AlignedBuf {
    /// The layout for `len` page-aligned bytes.
    fn layout(len: usize) -> std::alloc::Layout {
        std::alloc::Layout::from_size_align(len, DIRECT_IO_ALIGN)
            .expect("a page-multiple, page-aligned layout is always valid")
    }

    /// Allocate `len` zeroed, page-aligned bytes.
    ///
    /// # Panics
    ///
    /// On allocation failure, like any other allocation in this crate.
    fn new(len: usize) -> Self {
        let layout = Self::layout(len);
        // SAFETY: `layout` is non-zero-sized and validly aligned.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        Self {
            ptr: std::ptr::NonNull::new(ptr)
                .unwrap_or_else(|| std::alloc::handle_alloc_error(layout)),
            len,
        }
    }

    /// The buffer's bytes.
    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: a live allocation of exactly `self.len` bytes that this value owns, and
        // `&mut self` proves no other reference exists.
        unsafe { std::slice::from_raw_parts_mut(self.ptr.as_ptr(), self.len) }
    }
}

impl Drop for AlignedBuf {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `alloc_zeroed` with exactly this layout.
        unsafe { std::alloc::dealloc(self.ptr.as_ptr(), Self::layout(self.len)) }
    }
}

thread_local! {
    /// Per-blocking-thread scratch for the header read, so the serve path allocates nothing.
    static HEADER_SCRATCH: std::cell::RefCell<AlignedBuf> =
        std::cell::RefCell::new(AlignedBuf::new(SLOT_HEADER_BYTES));
}

/// One extent file, opened twice.
///
/// **Reads are O_DIRECT and writes are not**, which is the whole point: a read must DMA from
/// the drive into a registered frame with no page-cache copy, while a write is once per chunk
/// and its source buffer has whatever alignment the fill path gave it. Two descriptors is the
/// cheapest way to have both without forcing an aligned staging copy onto every write.
struct Extent {
    /// O_DIRECT on Linux; the same handle as `writer` elsewhere, since no other platform
    /// this runs on has an equivalent. A non-Linux build therefore reads through the page
    /// cache — correct, just not what the design is for.
    reader: std::fs::File,
    /// Buffered, for `write_slot`.
    writer: std::fs::File,
}

/// Where a chunk's bytes are, and the LRU links that decide when they stop being there.
///
/// `prev`/`next` are slot indices rather than pointers: the slot count is fixed at open
/// time, so an intrusive list over a `Vec` gives O(1) touch and eviction with no
/// allocation per access and no `unsafe`.
#[derive(Debug, Clone)]
struct Slot {
    /// The key occupying this slot, or `None` when it is free. Shared with the index map
    /// so a key is stored once, not twice — at ~1.75 M slots the duplicate would be
    /// hundreds of MiB.
    key: Option<Arc<str>>,
    /// Body length, which may be short (an object's last chunk).
    body_len: u32,
    /// CRC32 of the body as written, for the opt-in verify.
    body_crc: u32,
    /// Previous slot in LRU order (towards least-recently-used).
    prev: Option<u32>,
    /// Next slot in LRU order (towards most-recently-used).
    next: Option<u32>,
}

impl Slot {
    /// A free slot with no links.
    const fn empty() -> Self {
        Self {
            key: None,
            body_len: 0,
            body_crc: 0,
            prev: None,
            next: None,
        }
    }
}

/// Counters the daemon publishes. Owned here because this crate has no metrics registry
/// (the same reason `frames::FrameSource` is a trait) — the daemon reads these and feeds
/// its own Prometheus gauges.
#[derive(Debug, Default)]
pub struct StoreStats {
    /// Reads that found a slot and served it.
    pub hits: AtomicU64,
    /// Reads with no slot for the key.
    pub misses: AtomicU64,
    /// Chunks written.
    pub writes: AtomicU64,
    /// Writes that found the key already present and did nothing (chunks are immutable,
    /// so a re-fill is a touch).
    pub write_dedups: AtomicU64,
    /// Slots reused, evicting the least-recently-used chunk.
    pub evictions: AtomicU64,
    /// **The one to alert on.** A slot whose header named a different key than the index
    /// did. Always a bug in this module or a torn write; the read is refused.
    pub key_mismatches: AtomicU64,
    /// Body CRC mismatches, only possible when `verify_body` is on.
    pub crc_mismatches: AtomicU64,
    /// Slots whose header was present but impossible (see [`SlotState::Corrupt`]).
    pub corrupt_slots: AtomicU64,
    /// I/O errors on a read or write.
    pub io_errors: AtomicU64,
    /// Body bytes served from the tier. The numerator of the per-node read rate
    /// ADR-0033 gate 33.5 is about; `hits` alone cannot be one, because a short last
    /// chunk is a hit for far fewer bytes than a full one.
    pub read_bytes: AtomicU64,
    /// Body bytes written into the tier.
    pub written_bytes: AtomicU64,
    /// Wall time inside the read path, summed over hits, in nanoseconds.
    ///
    /// With `hits` this gives the **mean cost of a hit**, which is exactly the quantity
    /// foyer's 40–75 ms was (`foyer_storage_disk_io_duration`'s sum ÷ count), so the two
    /// are comparable without a histogram. It spans the header read, the key compare, the
    /// body `pread` and the optional CRC — i.e. everything the caller waits for, not just
    /// the syscall, because a cost moved out of the syscall and into this module would
    /// otherwise disappear from the measurement.
    pub read_nanos_total: AtomicU64,
    /// Longest single read seen, in nanoseconds. Not a p99 — a mean plus a max is what
    /// atomics can honestly carry, and the max is what shows a stall the mean hides.
    pub read_nanos_max: AtomicU64,
    /// **SERVICE time**: nanoseconds spent doing the actual work of a read, summed over
    /// hits — the header read, the key compare, the body `pread`, and the CRC when it is
    /// on. Measured *inside* the blocking task, so it excludes the wait for a blocking
    /// thread.
    ///
    /// ⚠ **It is a LOADED service time, not a per-read cost.** Excluding the wait for a
    /// thread does not exclude contention *within* the read. Measured on hardware
    /// (`c5-dcp-store-width-sweep.md`): **3.80 ms** at 60 hits with little concurrency,
    /// **26.9-34.9 ms** for the same 16 MiB under a real arm. Read it as "what a read costs
    /// at this concurrency" and always beside the queue figure — never as a property of the
    /// read alone.
    ///
    /// This exists because [`StoreStats::read_nanos_total`] cannot answer "what does a hit
    /// cost". That one is timed from before the `spawn_blocking` hop and is therefore
    /// **queue-inclusive**: on the first hardware arm it read 26–31 ms per hit at roughly
    /// 74 concurrent reads, which is Little's law rather than a per-read cost, and ADR-0033
    /// gate 33.4 compared it against a 3 ms threshold that was never that quantity.
    /// `service` minus `total` is the queueing, and the two together are what make either
    /// number interpretable.
    pub service_nanos_total: AtomicU64,
    /// Longest single read's service time, in nanoseconds.
    pub service_nanos_max: AtomicU64,
    /// How long the startup scan took, in nanoseconds. ADR-0033 gate 33.7 asks for this
    /// number, and it is the one figure that decides whether an in-memory index over a
    /// multi-TB tier is practical.
    pub scan_nanos: AtomicU64,
    /// Chunks the startup scan recovered from slot headers — i.e. how warm a restart
    /// started. Gate 33.7's other half.
    pub scan_recovered: AtomicU64,
}

impl StoreStats {
    /// Bump a counter. A method so call sites read as intent rather than as an ordering.
    fn inc(counter: &AtomicU64) {
        counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Add to a counter.
    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// Record one completed read: its byte count and its duration.
    ///
    /// The max is a compare-and-swap loop rather than a `fetch_max` on the hot path for
    /// no reason — `fetch_max` is exactly right here and is what this uses.
    fn observe_read(&self, bytes: u64, elapsed: std::time::Duration) {
        // Saturating: a duration long enough to overflow u64 nanoseconds is 584 years, so
        // this is a formality, but a silent wrap would corrupt the mean rather than one
        // sample.
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        Self::add(&self.read_bytes, bytes);
        Self::add(&self.read_nanos_total, nanos);
        self.read_nanos_max.fetch_max(nanos, Ordering::Relaxed);
    }

    /// Record one read's SERVICE time — the work, without the wait for a blocking thread.
    ///
    /// Separate from [`StoreStats::observe_read`] because they are measured at different
    /// points on purpose: this one inside the blocking task, that one around the whole
    /// `spawn_blocking` await.
    fn observe_service(&self, elapsed: std::time::Duration) {
        let nanos = u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX);
        Self::add(&self.service_nanos_total, nanos);
        self.service_nanos_max.fetch_max(nanos, Ordering::Relaxed);
    }

    /// Mean nanoseconds per hit, or `None` before the first one.
    ///
    /// **Queue-inclusive** — see [`StoreStats::read_nanos_total`]. This is the like-for-like
    /// comparison against foyer's own per-hit mean, which is queue-inclusive too; it is
    /// *not* the cost of a read. For that use [`StoreStats::mean_service_nanos`].
    #[must_use]
    pub fn mean_read_nanos(&self) -> Option<u64> {
        let hits = self.hits.load(Ordering::Relaxed);
        (hits > 0).then(|| self.read_nanos_total.load(Ordering::Relaxed) / hits)
    }

    /// Mean SERVICE nanoseconds per hit — what a read actually costs — or `None` before
    /// the first one.
    ///
    /// The number to compare against the device's own 1.335 ms for a 16 MiB read
    /// (`bench/ladder/results/nvme-device-truth.md`), and the one ADR-0033 gate 33.4 should
    /// have been written against.
    #[must_use]
    pub fn mean_service_nanos(&self) -> Option<u64> {
        let hits = self.hits.load(Ordering::Relaxed);
        (hits > 0).then(|| self.service_nanos_total.load(Ordering::Relaxed) / hits)
    }

    /// Mean nanoseconds a hit spent WAITING for a blocking thread, or `None` before the
    /// first hit.
    ///
    /// Saturating: the two clocks start and stop at different points, so on a fast read the
    /// service sample can round above the total. That is measurement noise, not negative
    /// queueing, and it must not wrap into a huge number.
    #[must_use]
    pub fn mean_queue_nanos(&self) -> Option<u64> {
        let total = self.mean_read_nanos()?;
        Some(total.saturating_sub(self.mean_service_nanos()?))
    }
}

/// How the store is laid out and how carefully it reads.
#[derive(Debug, Clone)]
pub struct StoreConfig {
    /// Directory for the extent files. Shares the cache dir with foyer's header tier;
    /// the filenames do not collide.
    pub dir: PathBuf,
    /// Bytes of chunk body one slot holds — the cluster's chunk size (ADR-0015). A slot
    /// consumes this plus [`SLOT_HEADER_BYTES`].
    pub chunk_size: usize,
    /// Total bytes for the tier. Rounded **down** to a whole number of slots, and to at
    /// least one, so a capacity below one slot is a one-slot tier rather than an error.
    pub capacity_bytes: u64,
    /// Verify the body CRC on every read. Off by default: a full pass over 16 MiB is
    /// ~1.1 ms against a 1.335 ms device read, i.e. a ~45 % throughput tax to re-check
    /// what NVMe end-to-end protection covers and the client's delivery digest checks
    /// again (ADR-0033 § Integrity). The CRC is *written* either way, so this is a flag
    /// flip and not a migration.
    pub verify_body: bool,
    /// Whether a hit's two reads are issued one after the other or at the same time.
    pub read_shape: ReadShape,
    /// Chunk reads allowed in flight at once, node-wide. `0` = unlimited.
    ///
    /// **A ceiling, not a target, and the direction is the opposite of the intuition.** The
    /// array saturates at ~4 concurrent 16 MiB reads — one `pread` is already ~128 device
    /// requests, because `max_hw_sectors_kb` is 128 — so past a shallow knee extra width buys
    /// nothing and costs queueing. Measured on one p5 with
    /// `bench/ladder/nvme-probe.sh store`:
    ///
    /// ```text
    /// depth   4      8     16     24     32     48     64
    /// GiB/s  42.3   47.4  48.7   48.2   45.7   43.1   43.4
    /// svc ms  1.5    2.6   5.1    7.7   10.8   16.9   22.3
    /// ```
    ///
    /// Service time is linear in depth — pure queueing — while throughput peaks at 16 and
    /// then *falls*. Nothing above this module bounds the total: `fill_parallelism` is per
    /// GET and `delivery.parallelism` is per request, so N concurrent requests multiply. A
    /// real arm was measured at ~59 in flight, which is on the far side of the knee, and that
    /// is where the 40-43.8 ms service times every earlier write-up reported came from — the
    /// reads were queued behind each other, not slow.
    pub read_concurrency: usize,
}

/// How a hit's header read and body read are ordered against each other.
///
/// # Why this is a knob and not a decision
///
/// It is the one live hypothesis for the gap between this tier and the device that has not
/// been cleanly tested. fio, on the same node at this tier's exact shape — 48 concurrent
/// 16 MiB `O_DIRECT` psync reads — reaches **43.208 GiB/s at 17.3 ms per read** where the
/// store manages **19.926 GiB/s at 43.8 ms**, and sharing 4 inodes instead of 48 costs fio
/// only 4.1 % (`bench/ladder/results/c5-dcp-store-single-read.md`). So the ~2.1× is in this
/// code, not the device, the array, the io engine, the concurrency or the inode count.
///
/// **The arm that appeared to refute the header read was confounded.** It merged the two
/// reads into one of `chunk_size + SLOT_HEADER_BYTES`, which is 16 MiB + 4 KiB and therefore
/// **not** a multiple of the 512 KiB RAID0 stripe: every read grew a 33rd-drive tail whose
/// straggler that arm's own write-up names as the likely reason service got *worse* by 8 %.
/// Removing a dependent round trip and adding a stripe-misaligned tail are opposite effects
/// measured as one number, so the +2.7 % it reported cannot be read as "the round trip was
/// free". [`ReadShape::Overlap`] separates them: it removes the dependency and leaves every
/// offset and length exactly where [`ReadShape::TwoRead`] has them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ReadShape {
    /// Read the 4 KiB header, check the key, then read the body — the shape every measurement
    /// through 2026-09-10 used, and the default so a config that says nothing keeps it.
    ///
    /// The body read cannot be issued until the header returns. That costs ~82 µs against an
    /// idle device and an unknown amount against a saturated one, which is the whole question.
    #[default]
    TwoRead,
    /// Issue both reads at once, then check the key before returning anything.
    ///
    /// The body's offset is pure arithmetic on the slot index (`body_offset`, private), so
    /// nothing in
    /// the header is needed to *start* the read — only to decide whether to keep it. The body
    /// is read at the full `chunk_size` rather than `align_up(body_len)` because `body_len` is
    /// the one thing that does arrive with the header; that also makes every read exactly a
    /// whole number of stripes, where `TwoRead` shortens the final chunk's.
    ///
    /// **No unverified byte is ever served.** The key compare and the CRC both still happen,
    /// on the same disk-resident header, before the bytes leave this module — the only change
    /// is that a read that will be thrown away has already been issued when we find out. That
    /// costs a wasted 16 MiB read on index/slot skew, which is a bug path with its own counter
    /// ([`StoreStats::key_mismatches`]), not a routine one.
    Overlap,
}

impl std::str::FromStr for ReadShape {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "two-read" | "" => Ok(Self::TwoRead),
            "overlap" => Ok(Self::Overlap),
            other => anyhow::bail!("unknown store read shape {other:?} (two-read|overlap)"),
        }
    }
}

impl StoreConfig {
    /// Bytes one slot occupies on disk: its header entry plus its body.
    ///
    /// **Not a stride.** A slot's two ranges are far apart — the header entry is in the
    /// extent's `HEADER_REGION_BYTES` prefix and the body is out in the body region — but it
    /// still *costs* exactly this much, because the header region is one page per slot. That
    /// is why collecting the headers changed the addressing and left the capacity arithmetic
    /// below completely alone.
    #[must_use]
    pub const fn slot_bytes(&self) -> u64 {
        SLOT_HEADER_BYTES as u64 + self.chunk_size as u64
    }

    /// Slots this capacity affords — at least one, so a misconfigured tiny tier degrades
    /// to a tiny cache rather than refusing to open.
    #[must_use]
    pub fn slot_count(&self) -> usize {
        let slots = self.capacity_bytes / self.slot_bytes();
        usize::try_from(slots.max(1)).unwrap_or(usize::MAX)
    }
}

/// The mutable half, behind one lock. Never held across a syscall.
struct Index {
    /// Key to slot index.
    map: HashMap<Arc<str>, u32>,
    /// Every slot, indexed by slot number.
    slots: Vec<Slot>,
    /// Slots never yet written, popped before anything is evicted.
    free: Vec<u32>,
    /// Least-recently-used end of the occupied list.
    lru_head: Option<u32>,
    /// Most-recently-used end.
    lru_tail: Option<u32>,
}

/// The chunk store. Cheap to clone into every read path (`Arc` inside).
#[derive(Clone)]
pub struct ChunkStore {
    inner: Arc<Inner>,
}

impl std::fmt::Debug for ChunkStore {
    /// Occupancy and geometry, never the index: a `Debug` that printed a million keys is one
    /// nobody can use, and these three numbers are what a log line wants.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChunkStore")
            .field("dir", &self.inner.cfg.dir)
            .field("held", &self.len())
            .field("slots", &self.capacity_slots())
            .finish()
    }
}

/// Shared state behind [`ChunkStore`]'s `Arc`.
struct Inner {
    cfg: StoreConfig,
    /// One open file per extent, indexed by extent number.
    extents: Vec<Extent>,
    index: Mutex<Index>,
    stats: StoreStats,
    /// Permits for reads in flight, or `None` when [`StoreConfig::read_concurrency`] is 0.
    ///
    /// Node-wide, and the only place the total is bounded — every caller above this module
    /// bounds its own fan-out and none of them bound their sum.
    read_slots: Option<Arc<tokio::sync::Semaphore>>,
}

impl ChunkStore {
    /// Open (creating if absent) the store in `cfg.dir` and rebuild the index by scanning
    /// slot headers.
    ///
    /// The scan is what makes a restart keep the tier, and collecting the headers is what
    /// makes it nearly free: one 16 MiB read per extent recovers 4096 of them, so there is no
    /// snapshot file that could disagree with the slots.
    ///
    /// # Errors
    ///
    /// Directory creation, extent open/size, or a scan read failing. A *corrupt slot* is
    /// not an error: it is counted and treated as free, because refusing to start over
    /// one bad slot would take a node out for something the tier can simply re-fill.
    pub async fn open(cfg: StoreConfig) -> anyhow::Result<Self> {
        tokio::fs::create_dir_all(&cfg.dir).await?;
        let slot_count = cfg.slot_count();
        let extents = open_extents(&cfg, slot_count).await?;
        let read_slots = (cfg.read_concurrency > 0)
            .then(|| Arc::new(tokio::sync::Semaphore::new(cfg.read_concurrency)));
        let inner = Arc::new(Inner {
            cfg,
            extents,
            read_slots,
            index: Mutex::new(Index {
                map: HashMap::new(),
                slots: vec![Slot::empty(); slot_count],
                free: Vec::new(),
                lru_head: None,
                lru_tail: None,
            }),
            stats: StoreStats::default(),
        });
        let store = Self { inner };
        store.scan().await?;
        Ok(store)
    }

    /// The counters, for the daemon's metrics.
    #[must_use]
    pub fn stats(&self) -> &StoreStats {
        &self.inner.stats
    }

    /// Chunks currently held.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().map.len()
    }

    /// Whether the store holds nothing.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Slots the tier has, occupied or not.
    #[must_use]
    pub fn capacity_slots(&self) -> usize {
        self.lock().slots.len()
    }

    /// Snapshot of every key the tier currently holds.
    ///
    /// Exists for a caller that addresses chunks by something OTHER than the key and has to
    /// rebuild its own mapping after a restart — [`crate::store_engine`], where foyer
    /// addresses the disk tier by hash. A snapshot rather than an iterator because the index
    /// lock must not be held across the caller's work.
    ///
    /// O(n) in held chunks and allocates a `Vec` of `Arc<str>` clones, so it is a startup
    /// operation, not something to call per request.
    #[must_use]
    pub fn keys(&self) -> Vec<Arc<str>> {
        self.lock().map.keys().cloned().collect()
    }

    /// Read `key`'s chunk, or `None` if this node's tier does not hold it.
    ///
    /// The bytes land in a registered slab frame when one is available
    /// ([`frames::decode_into_frame`]), so a holder posts its RDMA WRITE straight out of
    /// the cache — ADR-0028's payoff, now reached with a single DMA and no copy after it.
    ///
    /// A slot whose header names a different key is a **miss**, counted in
    /// [`StoreStats::key_mismatches`]. So is a CRC mismatch when `verify_body` is on. In
    /// both cases the slot is dropped from the index, so the next read re-fills rather
    /// than re-failing.
    ///
    /// # Errors
    ///
    /// An I/O error on the read. A missing key, a skewed slot and a bad CRC are all
    /// `Ok(None)` — the caller's fallback (peer, then backend) is the same for all three,
    /// and making damage an error would turn a re-fillable chunk into a failed request.
    pub async fn get(&self, key: &str) -> anyhow::Result<Option<CachedChunk>> {
        let Some(slot) = self.touch(key) else {
            StoreStats::inc(&self.inner.stats.misses);
            return Ok(None);
        };
        let inner = Arc::clone(&self.inner);
        let key = key.to_owned();
        // spawn_blocking, not the reactor: this is a blocking pread, and the old path
        // putting its checksum and decode on the main runtime is half of what ADR-0033
        // is correcting.
        //
        // Timed from HERE, so the measurement includes the blocking-pool hop. That hop is
        // a real cost the caller waits for, and timing only inside `read_slot` would hide
        // it — which matters because a saturated blocking pool is one of the few ways this
        // design could disappoint without any syscall getting slower.
        let started = std::time::Instant::now();
        // BEFORE the clock's other half: waiting for a slot is queueing, not service, and
        // that is the distinction that makes the two numbers worth having. Held for the whole
        // hit — both reads of an `Overlap` pair are one chunk and one slot.
        let _slot = match &self.inner.read_slots {
            Some(slots) => Some(
                Arc::clone(slots)
                    .acquire_owned()
                    .await
                    .map_err(|_| anyhow::anyhow!("the chunk store's read semaphore was closed"))?,
            ),
            None => None,
        };
        let read = match self.inner.cfg.read_shape {
            ReadShape::TwoRead => {
                tokio::task::spawn_blocking(move || inner.read_slot(&key, slot)).await?
            }
            ReadShape::Overlap => self.read_overlapped(&key, slot).await,
        };
        match read {
            Ok(Some(chunk)) => {
                StoreStats::inc(&self.inner.stats.hits);
                self.inner
                    .stats
                    .observe_read(chunk.body.len() as u64, started.elapsed());
                Ok(Some(chunk))
            }
            Ok(None) => {
                self.forget(slot);
                Ok(None)
            }
            Err(err) => {
                StoreStats::inc(&self.inner.stats.io_errors);
                Err(err)
            }
        }
    }

    /// A hit with both reads in flight at once — [`ReadShape::Overlap`]'s whole body.
    ///
    /// Two `spawn_blocking` tasks rather than one, because the point is for the 4 KiB header
    /// and the 16 MiB body to be queued at the block layer **together** instead of the second
    /// waiting on the first. That costs one extra trip through the blocking pool, which the
    /// queue term reports separately and which is the price the arm is measuring against.
    ///
    /// # Errors
    ///
    /// An I/O error on either read, or either blocking task panicking.
    async fn read_overlapped(&self, key: &str, slot: u32) -> anyhow::Result<Option<CachedChunk>> {
        let header_task = {
            let inner = Arc::clone(&self.inner);
            tokio::task::spawn_blocking(move || {
                let at = std::time::Instant::now();
                inner.read_header(slot).map(|h| (h, at.elapsed()))
            })
        };
        // The full `chunk_size`, not `align_up(body_len)`: `body_len` is exactly what has not
        // arrived yet. It is also the better-aligned read — a whole number of RAID0 stripes on
        // every chunk, where the dependent shape shortens the final one.
        let read_len = self.inner.cfg.chunk_size;
        let body_task = {
            let inner = Arc::clone(&self.inner);
            tokio::task::spawn_blocking(move || {
                let at = std::time::Instant::now();
                inner.read_body(slot, read_len).map(|b| (b, at.elapsed()))
            })
        };
        // BOTH are awaited even once the header has disqualified the slot. The body read is
        // already in flight into a claimed frame, and dropping its `JoinHandle` would neither
        // cancel it (a blocking task cannot be cancelled) nor release the frame — it would
        // just stop anyone waiting for the write to finish, which is how a frame gets recycled
        // under an in-progress read.
        let (header, body) = (header_task.await?, body_task.await?);
        let (header, header_took) = header?;
        let (body, body_took) = body?;
        let Some(header) = header else {
            return Ok(None);
        };
        let Some(len) = self.inner.accept(key, slot, &header) else {
            return Ok(None);
        };
        let Some(chunk) = self.inner.verified(key, slot, &header, body, len) else {
            return Ok(None);
        };
        // Service is the SLOWER of the two, not their sum, and that is what makes this number
        // comparable with the dependent shape's — where the two are serial and the sum IS the
        // wall. Reporting a sum here would credit the overlap to itself. Recorded only on the
        // success path, for the same reason `read_slot` does: a refused slot did not do the
        // work a service time is meant to describe.
        self.inner.stats.observe_service(header_took.max(body_took));
        Ok(Some(chunk))
    }

    /// Write `chunk` under `key`, evicting the least-recently-used chunk if the tier is
    /// full.
    ///
    /// Write-through, and that costs nothing on this shape: foyer wrote a chunk on
    /// demotion, and a one-pass checkpoint sweep demotes every chunk, so the byte count
    /// is the same — while the submit queue that **silently dropped** those demotions
    /// (`storage_queue_channel_overflow`, 1463–4229 observed) is gone entirely.
    ///
    /// A key already present is a no-op: chunks are immutable, so a re-fill can only be
    /// the same bytes. Counted in [`StoreStats::write_dedups`].
    ///
    /// # Errors
    ///
    /// A body larger than the slot, a key too long for the header, or an I/O error.
    pub async fn put(&self, key: &str, chunk: &CachedChunk) -> anyhow::Result<()> {
        let body_len = chunk.body.len();
        if body_len > self.inner.cfg.chunk_size {
            anyhow::bail!(
                "chunk of {body_len} bytes exceeds the tier's {} byte slot",
                self.inner.cfg.chunk_size
            );
        }
        let key: Arc<str> = Arc::from(key);
        // Cast is bounded by the chunk_size check above; chunk sizes are far below 4 GiB.
        #[allow(clippy::cast_possible_truncation)]
        let header = SlotHeader {
            key: key.to_string(),
            body_len: body_len as u32,
            body_crc: slot::body_crc(&chunk.body),
        };
        let Some(slot) = self.claim(&key, &header) else {
            StoreStats::inc(&self.inner.stats.write_dedups);
            return Ok(());
        };
        let inner = Arc::clone(&self.inner);
        let body = chunk.body.clone();
        let written =
            tokio::task::spawn_blocking(move || inner.write_slot(slot, &header, &body)).await?;
        match written {
            Ok(()) => {
                StoreStats::inc(&self.inner.stats.writes);
                StoreStats::add(&self.inner.stats.written_bytes, body_len as u64);
                Ok(())
            }
            Err(err) => {
                // The slot holds nothing usable now, so take the key back out rather than
                // leave an index entry pointing at a half-written slot.
                self.forget(slot);
                StoreStats::inc(&self.inner.stats.io_errors);
                Err(err)
            }
        }
    }

    /// Drop `key` from the tier, freeing its slot.
    ///
    /// This is ADR-0007's invalidation, not an eviction: a write to a key must not leave
    /// a stale body behind that a later read could serve. Returns whether the key was
    /// held, so a caller can tell a real invalidation from a no-op.
    ///
    /// The slot's bytes are **not** erased — the header is what makes a slot readable,
    /// and the next write to the slot overwrites it. A read cannot reach the old bytes in
    /// the meantime because the index no longer names them, and if one somehow did, the
    /// header's key check would refuse it.
    pub fn remove(&self, key: &str) -> bool {
        let mut index = self.lock();
        let Some(slot) = index.map.get(key).copied() else {
            return false;
        };
        if index.evict(slot) {
            index.free.push(slot);
        }
        true
    }

    /// Lock the index. A helper so the `expect` message lives in one place: the lock is
    /// only ever held for bookkeeping, so a poisoned mutex means a panic *inside* that
    /// bookkeeping and is not recoverable.
    fn lock(&self) -> std::sync::MutexGuard<'_, Index> {
        self.inner
            .index
            .lock()
            .expect("the chunk store index lock is never held across a panic-capable call")
    }

    /// Look `key` up and mark it most-recently-used.
    fn touch(&self, key: &str) -> Option<u32> {
        let mut index = self.lock();
        let slot = *index.map.get(key)?;
        // Linked by construction: it was in the map, and a slot is in the map exactly
        // while it is in the LRU list.
        index.unlink_linked(slot);
        index.push_back(slot);
        Some(slot)
    }

    /// Reserve a slot for `key`, or `None` when the key is already held.
    ///
    /// Takes a free slot first and evicts the least-recently-used only when there is
    /// none. The index is updated **before** the write, so two concurrent `put`s of the
    /// same key cannot both claim a slot.
    fn claim(&self, key: &Arc<str>, header: &SlotHeader) -> Option<u32> {
        let mut index = self.lock();
        if index.map.contains_key(key.as_ref()) {
            return None;
        }
        let slot = match index.free.pop() {
            Some(slot) => slot,
            None => {
                let victim = index.lru_head?;
                index.evict(victim);
                StoreStats::inc(&self.inner.stats.evictions);
                victim
            }
        };
        index.occupy(slot, Arc::clone(key), header.body_len, header.body_crc);
        Some(slot)
    }

    /// Drop `slot` from the index and return it to the free list.
    ///
    /// Used both when a read finds damage and when a write fails: either way the slot
    /// holds nothing servable, and leaving the index pointing at it would make every
    /// later read of that key fail identically instead of re-filling.
    fn forget(&self, slot: u32) {
        let mut index = self.lock();
        // Push only if it really held a key: a slot on the free list twice would be
        // claimed by two writers at once.
        if index.evict(slot) {
            index.free.push(slot);
        }
    }
}

impl Index {
    /// Remove `slot` from the LRU order, leaving its key in place.
    ///
    /// **Only valid for a slot that is actually linked**, i.e. one whose `key` is
    /// `Some`. An unlinked slot has `prev`/`next` both `None`, which is
    /// indistinguishable from being the sole element of the list — so calling this on a
    /// free slot would set `lru_head`/`lru_tail` to `None` and lose every other slot.
    /// [`Index::evict`] is the guarded entry point; this is private for that reason.
    fn unlink_linked(&mut self, slot: u32) {
        let (prev, next) = {
            let s = &self.slots[slot as usize];
            (s.prev, s.next)
        };
        match prev {
            Some(p) => self.slots[p as usize].next = next,
            None => self.lru_head = next,
        }
        match next {
            Some(n) => self.slots[n as usize].prev = prev,
            None => self.lru_tail = prev,
        }
        let s = &mut self.slots[slot as usize];
        s.prev = None;
        s.next = None;
    }

    /// Append `slot` at the most-recently-used end.
    fn push_back(&mut self, slot: u32) {
        let old_tail = self.lru_tail.replace(slot);
        self.slots[slot as usize].prev = old_tail;
        self.slots[slot as usize].next = None;
        match old_tail {
            Some(t) => self.slots[t as usize].next = Some(slot),
            None => self.lru_head = Some(slot),
        }
    }

    /// Give `slot` to `key`, most-recently-used.
    fn occupy(&mut self, slot: u32, key: Arc<str>, body_len: u32, body_crc: u32) {
        let s = &mut self.slots[slot as usize];
        s.key = Some(Arc::clone(&key));
        s.body_len = body_len;
        s.body_crc = body_crc;
        self.map.insert(key, slot);
        self.push_back(slot);
    }

    /// Free `slot`, removing its key from the map, and report whether it had held one.
    ///
    /// Does **not** push onto the free list: [`ChunkStore::claim`] reuses the slot
    /// immediately, while [`ChunkStore::forget`] does push — and uses the return value to
    /// push **exactly once**, since a slot on the free list twice would be handed to two
    /// concurrent writers.
    fn evict(&mut self, slot: u32) -> bool {
        let Some(key) = self.slots[slot as usize].key.take() else {
            return false;
        };
        self.unlink_linked(slot);
        self.map.remove(key.as_ref());
        true
    }
}

/// Where a slot's two byte ranges are: its header entry, and its body.
///
/// A struct rather than a tuple because the two offsets are both `u64` and mixing them up
/// would read a body as a header (or worse, serve a header page as bytes) — a mistake a
/// name prevents and a position does not.
struct SlotAt<'a> {
    /// The extent file both ranges live in.
    extent: &'a Extent,
    /// Offset of the 4 KiB header entry, inside the extent's header region.
    header_at: u64,
    /// Offset of the body, after the whole header region.
    body_at: u64,
}

impl Inner {
    /// Which extent holds `slot`, and where its header and body are within it.
    fn locate(&self, slot: u32) -> SlotAt<'_> {
        let slot = slot as usize;
        let extent = slot / SLOTS_PER_EXTENT;
        let within = (slot % SLOTS_PER_EXTENT) as u64;
        SlotAt {
            extent: &self.extents[extent],
            header_at: header_offset(within),
            body_at: body_offset(within, self.cfg.chunk_size as u64),
        }
    }

    /// The whole read: the header entry, the key check, then **one** `pread` of the body
    /// straight into a slab frame.
    ///
    /// `Ok(None)` means the slot does not hold `key`'s bytes (skew, damage, or a bad
    /// CRC) — the caller drops the slot and falls back.
    ///
    /// # Errors
    ///
    /// An I/O error on either read.
    fn read_slot(&self, key: &str, slot: u32) -> anyhow::Result<Option<CachedChunk>> {
        // SERVICE time starts here, inside the blocking task: everything before this point
        // is the wait for a blocking thread, which `observe_read`'s clock already covers.
        // Recorded only on the success path — a miss or a refused slot did not do the work
        // a service time is meant to describe, and averaging them in would flatter it.
        let started = std::time::Instant::now();
        // TWO reads, and deliberately so. Reading the header and the body in one I/O was
        // measured at **+2.7 %** — inside that arm's ±20 % spread, with service time slightly
        // *worse* (`bench/ladder/results/c5-dcp-store-single-read.md`), so the dependent round
        // trip it removed was never the wall: the header is a 4 KiB read at ~82 µs and total
        // queueing at the knee is 0.6 ms. Two reads costs that 82 µs and buys the contiguous
        // header region the scan reads in one I/O; on throughput the two shapes are the same
        // within noise, and the stripe alignment that was supposed to break the tie measured
        // 0.046 % (see [`HEADER_REGION_BYTES`]).
        //
        // Always through the O_DIRECT descriptor and the per-thread aligned scratch: a header
        // entry is one page at a page-aligned offset, so this read needs no frame and never
        // has to fall back.
        let Some(header) = self.read_header(slot)? else {
            return Ok(None);
        };
        let Some(len) = self.accept(key, slot, &header) else {
            return Ok(None);
        };
        // O_DIRECT needs a block-aligned LENGTH, and an object's last chunk is short by
        // design (ADR-0015 clamps it). Read the rounded length — still inside the slot,
        // which reserves `chunk_size` whatever the body is — then publish only `body_len`.
        let body = self.read_body(slot, align_up(len).min(self.cfg.chunk_size))?;
        let Some(chunk) = self.verified(key, slot, &header, body, len) else {
            return Ok(None);
        };
        self.stats.observe_service(started.elapsed());
        Ok(Some(chunk))
    }

    /// One slot's 4 KiB header entry, or `None` when the slot is free or corrupt.
    ///
    /// # Errors
    ///
    /// An I/O error on the header read.
    fn read_header(&self, slot: u32) -> anyhow::Result<Option<SlotHeader>> {
        let at = self.locate(slot);
        let header = HEADER_SCRATCH.with(|scratch| {
            let mut scratch = scratch.borrow_mut();
            let page = scratch.as_mut_slice();
            at.extent.reader.read_exact_at(page, at.header_at)?;
            anyhow::Ok(SlotHeader::parse(page, self.cfg.chunk_size))
        })?;
        match header {
            Ok(header) => Ok(Some(header)),
            Err(SlotState::Free) => Ok(None),
            Err(SlotState::Corrupt | SlotState::KeyNotUtf8) => {
                StoreStats::inc(&self.stats.corrupt_slots);
                Ok(None)
            }
        }
    }

    /// `read_len` bytes of `slot`'s body, into a slab frame when one can be claimed.
    ///
    /// # Errors
    ///
    /// An I/O error on the body read.
    fn read_body(&self, slot: u32, read_len: usize) -> anyhow::Result<Bytes> {
        let at = self.locate(slot);
        match frames::claim(read_len) {
            // ADR-0028's payoff: the DMA lands in registered memory and the holder posts its
            // WRITE straight out of it, with no copy after the transfer.
            Some(mut frame) => {
                at.extent
                    .reader
                    .read_exact_at(frame.as_mut_slice(), at.body_at)?;
                frames::note_stored();
                Ok(frame.seal())
            }
            // No frame: a heap `Vec`'s alignment is its element's, so this path cannot use
            // the direct descriptor and reads through the buffered one instead. It happens
            // only where there is no slab — watch `slab_heap_fallbacks_total`.
            None => {
                frames::note_heap_fallback();
                let mut buf = vec![0u8; read_len];
                at.extent.writer.read_exact_at(&mut buf, at.body_at)?;
                Ok(Bytes::from(buf))
            }
        }
    }

    /// The body length to read for `key`, or `None` when this slot is not `key`'s.
    ///
    /// THE check that replaces foyer's XxHash64 for the failure this code can cause. It still
    /// reads the key off DISK rather than from the index, which would only be checking the
    /// index against itself.
    fn accept(&self, key: &str, slot: u32, header: &SlotHeader) -> Option<usize> {
        if header.key != key {
            StoreStats::inc(&self.stats.key_mismatches);
            tracing::error!(
                asked = key,
                found = %header.key,
                slot,
                "chunk slot holds another key's bytes — index/slot skew, refusing to serve"
            );
            return None;
        }
        Some(header.body_len as usize)
    }

    /// `body` trimmed to `len` and CRC-checked, or `None` when it fails its CRC.
    ///
    /// Slicing a sealed frame keeps the same allocation, so the trim costs a refcount and not
    /// a copy — which is what lets the overlapped shape read a whole `chunk_size` and publish
    /// a short final chunk for free.
    fn verified(
        &self,
        key: &str,
        slot: u32,
        header: &SlotHeader,
        body: Bytes,
        len: usize,
    ) -> Option<CachedChunk> {
        let body = body.slice(..len);
        if self.cfg.verify_body && slot::body_crc(&body) != header.body_crc {
            StoreStats::inc(&self.stats.crc_mismatches);
            tracing::error!(key, slot, "chunk body failed its CRC — refusing to serve");
            return None;
        }
        Some(CachedChunk::new(body))
    }

    /// Write the header page and the body. One `pwrite` each; the header goes **last** so
    /// a torn write leaves a slot whose magic is stale rather than one that claims bytes
    /// it does not have.
    ///
    /// # Errors
    ///
    /// A key too long for the header page, or an I/O error.
    fn write_slot(&self, slot: u32, header: &SlotHeader, body: &Bytes) -> anyhow::Result<()> {
        let at = self.locate(slot);
        let mut header_page = vec![0u8; SLOT_HEADER_BYTES];
        header.write_to(&mut header_page)?;
        at.extent.writer.write_all_at(body, at.body_at)?;
        // The body must be ON THE DRIVE before the header names it, because the reader is
        // O_DIRECT and would otherwise be able to reach a slot whose bytes exist only in the
        // page cache. Mixing buffered writes with direct reads is exactly the combination
        // that works until it does not, so the ordering is made explicit rather than left to
        // the kernel's page-cache invalidation.
        at.extent.writer.sync_data()?;
        at.extent.writer.write_all_at(&header_page, at.header_at)?;
        // NO second sync. The header is what makes a slot readable, so losing it to a crash
        // loses the chunk — which for a cache is a re-fetch, not damage. The sync above is
        // not durability, it is ORDERING: it is what stops the header naming bytes the
        // O_DIRECT reader cannot see. One fsync per chunk written, on a path that runs once
        // per chunk against a read path that runs many times.
        Ok(())
    }

    /// Read extent `index`'s whole header region and parse its first `slots` entries, in
    /// slot order.
    ///
    /// **One I/O for 4096 headers.** This is the second payoff of collecting the headers: the
    /// region is contiguous, so a whole extent's worth arrives in a single 16 MiB read instead
    /// of 4096 scattered 4 KiB ones, and the scan needs no concurrency to be fast. Direct, and
    /// for a second reason beyond consistency with the read path: a scan of a multi-TB tier
    /// would otherwise pull every slot header into the page cache at startup, which is
    /// eviction pressure for data read exactly once.
    ///
    /// # Errors
    ///
    /// An I/O error reading the region. A slot whose header is damage is counted and reported
    /// as `None`, exactly as a free slot is — refusing to start over one bad slot would take a
    /// node out for something the tier can re-fill.
    fn scan_extent(&self, index: usize, slots: usize) -> anyhow::Result<Vec<Option<SlotHeader>>> {
        let mut region = AlignedBuf::new(HEADER_REGION_BYTES);
        self.extents[index]
            .reader
            .read_exact_at(region.as_mut_slice(), 0)?;
        let bytes = region.as_mut_slice();
        let mut headers = Vec::with_capacity(slots);
        for within in 0..slots {
            let at = within * SLOT_HEADER_BYTES;
            let entry = &bytes[at..at + SLOT_HEADER_BYTES];
            headers.push(match SlotHeader::parse(entry, self.cfg.chunk_size) {
                Ok(header) => Some(header),
                Err(SlotState::Free) => None,
                Err(SlotState::Corrupt | SlotState::KeyNotUtf8) => {
                    StoreStats::inc(&self.stats.corrupt_slots);
                    None
                }
            });
        }
        Ok(headers)
    }
}

/// Open (creating and sizing if needed) every extent file the tier's `slot_count` needs.
///
/// A never-written slot needs no initialization pass: its header entry reads zeros — from a
/// hole or from an unwritten extent, both of which read the same — and
/// [`SlotHeader::parse`] reports zeros as [`SlotState::Free`]. That is what lets the free
/// list be rebuilt from the slots themselves with no on-disk bookkeeping.
///
/// # Errors
///
/// A chunk size the direct-I/O alignment cannot accept, or open/size/reserve failing.
async fn open_extents(cfg: &StoreConfig, slot_count: usize) -> anyhow::Result<Vec<Extent>> {
    // THE INVARIANT THE READ PATH DEPENDS ON, checked once here rather than as an EINVAL on
    // every read: a body lives at `HEADER_REGION_BYTES + slot * chunk_size`, and the header
    // region is a page multiple, so every body offset is page-aligned exactly when
    // `chunk_size` is. A chunk size that is not a multiple of the page would make O_DIRECT
    // refuse every read, and the failure would look like an I/O error rather than a
    // configuration mistake.
    if !cfg.chunk_size.is_multiple_of(DIRECT_IO_ALIGN) {
        anyhow::bail!(
            "chunk size {} is not a multiple of the {DIRECT_IO_ALIGN}-byte direct-I/O \
             alignment, so no slot body would be aligned and every direct read would fail",
            cfg.chunk_size
        );
    }
    // NO stripe-alignment check here, deliberately. An earlier cut warned when `chunk_size` did
    // not divide by STRIPE_BYTES, on the belief that a partial stripe costs throughput; the fio
    // sweep measured that at 0.046 % (see [`HEADER_REGION_BYTES`]), so the warning would be a
    // false alarm pointing an operator at a non-problem.
    let extent_count = slot_count.div_ceil(SLOTS_PER_EXTENT);
    let mut extents = Vec::with_capacity(extent_count);
    for index in 0..extent_count {
        let path: PathBuf = cfg.dir.join(extent_name(index));
        // Sized to the slots this extent actually holds, so the last one is short rather than
        // a full 64 GiB — which matters now that the space is really allocated.
        let bodies = slots_in_extent(slot_count, index) as u64 * cfg.chunk_size as u64;
        extents.push(open_extent(&path, HEADER_REGION_BYTES as u64 + bodies).await?);
    }
    Ok(extents)
}

/// Open one extent at `path`, sized to `bytes`: a direct-I/O reader and a buffered writer.
///
/// # Errors
///
/// Open, size or reserve failing. Notably `open` with `O_DIRECT` fails `EINVAL` on a
/// filesystem that does not support it — tmpfs, for one, which is worth knowing because a test
/// that points the store at `/dev/shm` would fail here rather than anywhere informative.
async fn open_extent(path: &Path, bytes: u64) -> anyhow::Result<Extent> {
    let path = path.to_path_buf();
    let extent = tokio::task::spawn_blocking(move || -> anyhow::Result<Extent> {
        let writer = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        // Only on a fresh or resized extent, so a restart over a warm tier stays as cheap as
        // it is today (gate 33.7 measures that path).
        if writer.metadata()?.len() != bytes {
            writer.set_len(bytes)?;
            reserve(&writer, bytes)?;
        }
        let reader = open_direct_reader(&path)?;
        Ok(Extent { reader, writer })
    })
    .await??;
    Ok(extent)
}

/// Allocate `bytes` of real space for the extent, up front.
///
/// **Aligned file offsets only buy an aligned device read if the file's extents are
/// contiguous.** A sparse file filled by concurrent writes at scattered slot offsets gets
/// whatever extents the allocator happens to hand out, and a body split across two of them is
/// two separate device ranges however carefully its offset was computed — which would leave
/// [`HEADER_REGION_BYTES`]'s alignment arithmetic true on paper and false on the drive. One
/// allocation at open removes that variable.
///
/// It also removes a failure mode: a tier that was merely *sized* can still fail to write
/// mid-run when the filesystem fills, which reads as the store being broken rather than the
/// disk being full.
///
/// # Errors
///
/// `fallocate` failing for any reason other than the filesystem not supporting it, in which
/// case the extent falls back to sparse and says so.
#[cfg(target_os = "linux")]
fn reserve(file: &std::fs::File, bytes: u64) -> anyhow::Result<()> {
    use std::os::unix::io::AsRawFd;
    // `fallocate(2)` directly, NOT `posix_fallocate(3)`: glibc emulates the latter by
    // *writing zeros* when the filesystem has no allocate operation, which on a 200 GiB tier
    // is minutes of device writes at startup where an error would have been the right answer.
    let len = i64::try_from(bytes)?;
    // SAFETY: `file` is an open, writable descriptor for the duration of the call, and mode,
    // offset and length are plain scalars.
    if unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len) } == 0 {
        return Ok(());
    }
    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        Some(libc::EOPNOTSUPP | libc::ENOSYS) => {
            tracing::warn!(
                error = %err,
                bytes,
                "the cache filesystem cannot preallocate, so extents stay sparse — bodies may \
                 be fragmented and a body read may span more device ranges than its offset \
                 suggests"
            );
            Ok(file.set_len(bytes)?)
        }
        _ => Err(err.into()),
    }
}

/// Non-Linux fallback: size the file and leave it sparse.
///
/// macOS has no `fallocate`, and this build is for tests — see [`open_direct_reader`] for the
/// same reason stated about `O_DIRECT`.
///
/// # Errors
///
/// `set_len` failing.
#[cfg(not(target_os = "linux"))]
fn reserve(file: &std::fs::File, bytes: u64) -> anyhow::Result<()> {
    Ok(file.set_len(bytes)?)
}

/// Open `path` read-only with `O_DIRECT`, so a read DMAs into the caller's buffer with no
/// page-cache copy.
///
/// # Errors
///
/// `open` failing — `EINVAL` when the filesystem does not support direct I/O.
#[cfg(target_os = "linux")]
fn open_direct_reader(path: &Path) -> anyhow::Result<std::fs::File> {
    use std::os::unix::fs::OpenOptionsExt;
    Ok(std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECT)
        .open(path)?)
}

/// Non-Linux fallback: an ordinary read-only handle.
///
/// macOS has no `O_DIRECT` (its nearest equivalent, `F_NOCACHE`, is advisory and is set after
/// open), and no other platform this runs on has one either. So a non-Linux build reads
/// through the page cache: correct, and not what the design is for — which matters mainly
/// because it means **the direct path cannot be unit-tested on a Mac** and has to be
/// exercised in the build pod or on a node.
///
/// # Errors
///
/// `open` failing.
#[cfg(not(target_os = "linux"))]
fn open_direct_reader(path: &Path) -> anyhow::Result<std::fs::File> {
    Ok(std::fs::OpenOptions::new().read(true).open(path)?)
}

impl ChunkStore {
    /// Rebuild the index by reading every extent's header region.
    ///
    /// **One I/O per extent**, not one per slot: the header entries are contiguous
    /// ([`HEADER_REGION_BYTES`]), so 4096 of them arrive in a single 16 MiB read and the whole
    /// scan is a handful of sequential reads rather than a `SCAN_CONCURRENCY`-wide fan-out of
    /// 4 KiB ones. Extents are read one at a time; if a multi-TB tier ever makes that the
    /// slow part, extent-level concurrency is the lever, and it is a much smaller one than the
    /// per-slot width it replaced.
    ///
    /// Slots are inserted in slot order, so LRU order after a restart is slot order rather
    /// than the true access history — an acceptable loss, and the alternative (a persisted
    /// access order) is exactly the on-disk bookkeeping this design avoids.
    ///
    /// # Errors
    ///
    /// An I/O error reading a header region, or a scan task panicking. A corrupt header is
    /// counted and skipped.
    async fn scan(&self) -> anyhow::Result<()> {
        let slots = self.capacity_slots();
        let started = std::time::Instant::now();
        let mut found = 0_usize;
        for index in 0..self.inner.extents.len() {
            let inner = Arc::clone(&self.inner);
            let count = slots_in_extent(slots, index);
            let headers =
                tokio::task::spawn_blocking(move || inner.scan_extent(index, count)).await??;
            // The lock is taken once per extent rather than once per slot: this is startup,
            // nothing else holds it, and 4096 `occupy` calls under one guard is strictly less
            // work than 4096 acquisitions.
            let mut idx = self.lock();
            for (within, header) in headers.into_iter().enumerate() {
                // Cast: slot indices are bounded by `slot_count`, which `open` built as a
                // usize small enough to index a Vec; u32 covers a 68 PiB tier at 16 MiB.
                #[allow(clippy::cast_possible_truncation)]
                let slot = (index * SLOTS_PER_EXTENT + within) as u32;
                match header {
                    Some(header) => {
                        idx.occupy(
                            slot,
                            Arc::from(header.key.as_str()),
                            header.body_len,
                            header.body_crc,
                        );
                        found += 1;
                    }
                    None => idx.free.push(slot),
                }
            }
        }
        let elapsed = started.elapsed();
        self.inner.stats.scan_nanos.store(
            u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
        self.inner
            .stats
            .scan_recovered
            .store(found as u64, Ordering::Relaxed);
        tracing::info!(
            slots,
            found,
            scan_secs = elapsed.as_secs_f64(),
            corrupt = self.inner.stats.corrupt_slots.load(Ordering::Relaxed),
            dir = %self.inner.cfg.dir.display(),
            "chunk store index rebuilt from slot headers"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small enough to keep tests fast, larger than the header page so short and full
    /// bodies are both exercised.
    const CHUNK: usize = 64 << 10;

    /// A store of exactly `slots` slots in a fresh temp dir, on the default read shape.
    async fn store_of(slots: u64, dir: &Path) -> ChunkStore {
        store_shaped(slots, dir, ReadShape::TwoRead).await
    }

    /// The same, with the read shape named — so a property can be asserted on both without
    /// each test having to spell a whole `StoreConfig`.
    async fn store_shaped(slots: u64, dir: &Path, read_shape: ReadShape) -> ChunkStore {
        store_limited(slots, dir, read_shape, 0).await
    }

    /// And with the read ceiling named too, for the one test that is about the ceiling.
    ///
    /// `0` everywhere else — unlimited — so no other test can deadlock on a permit, and so
    /// each of them exercises the same code path it did before the ceiling existed.
    async fn store_limited(
        slots: u64,
        dir: &Path,
        read_shape: ReadShape,
        read_concurrency: usize,
    ) -> ChunkStore {
        let cfg = StoreConfig {
            dir: dir.to_path_buf(),
            chunk_size: CHUNK,
            capacity_bytes: slots * (SLOT_HEADER_BYTES as u64 + CHUNK as u64),
            verify_body: true,
            read_shape,
            read_concurrency,
        };
        ChunkStore::open(cfg).await.unwrap()
    }

    fn chunk_of(byte: u8, len: usize) -> CachedChunk {
        CachedChunk::new(Bytes::from(vec![byte; len]))
    }

    /// Gate 33.1, base case: what goes in comes out, at both a full and a short body.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_written_chunk_reads_back_byte_exact() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(8, dir.path()).await;

        store
            .put("b/k#100:0", &chunk_of(0xab, CHUNK))
            .await
            .unwrap();
        // A short last chunk (ADR-0015 chunk_bounds clamps it).
        store.put("b/k#100:1", &chunk_of(0xcd, 1234)).await.unwrap();

        let full = store.get("b/k#100:0").await.unwrap().unwrap();
        assert_eq!(full.body.len(), CHUNK);
        assert!(full.body.iter().all(|&b| b == 0xab));
        let short = store.get("b/k#100:1").await.unwrap().unwrap();
        assert_eq!(short.body.len(), 1234);
        assert!(short.body.iter().all(|&b| b == 0xcd));
        assert_eq!(store.len(), 2);
        assert_eq!(store.stats().hits.load(Ordering::Relaxed), 2);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn an_absent_key_is_a_counted_miss() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(4, dir.path()).await;
        assert!(store.get("b/k#100:0").await.unwrap().is_none());
        assert_eq!(store.stats().misses.load(Ordering::Relaxed), 1);
        assert_eq!(store.stats().hits.load(Ordering::Relaxed), 0);
    }

    /// Gate 33.1, slot reuse: a full tier must evict the least-recently-used chunk and
    /// the survivors must still be byte-exact. The `get` in the middle is what makes
    /// this an LRU test rather than a FIFO one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_full_tier_evicts_the_least_recently_used() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(2, dir.path()).await;

        store.put("k#1:0", &chunk_of(1, 16)).await.unwrap();
        store.put("k#1:1", &chunk_of(2, 16)).await.unwrap();
        // Touch slot 0, making key 1 the least-recently-used.
        assert!(store.get("k#1:0").await.unwrap().is_some());
        store.put("k#1:2", &chunk_of(3, 16)).await.unwrap();

        assert_eq!(store.capacity_slots(), 2);
        assert_eq!(store.len(), 2);
        assert!(
            store.get("k#1:1").await.unwrap().is_none(),
            "the least-recently-used chunk must be the one evicted"
        );
        let kept = store.get("k#1:0").await.unwrap().unwrap();
        assert!(kept.body.iter().all(|&b| b == 1), "survivor stays exact");
        let fresh = store.get("k#1:2").await.unwrap().unwrap();
        assert!(fresh.body.iter().all(|&b| b == 3), "the new chunk is right");
        assert_eq!(store.stats().evictions.load(Ordering::Relaxed), 1);
    }

    /// **Gate 33.2.** A slot holding another key's bytes must be refused, not served.
    /// Planted by writing one key and then pointing the index at its slot from another —
    /// which is exactly the shape an eviction race or a bad scan would produce, and the
    /// reason the key lives in the slot header at all.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_slot_holding_another_key_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(4, dir.path()).await;
        store.put("real#1:0", &chunk_of(9, 32)).await.unwrap();
        let slot = *store.lock().map.get("real#1:0").unwrap();

        // Skew the index: claim the same slot under a different key.
        {
            let mut index = store.lock();
            let key: Arc<str> = Arc::from("impostor#1:0");
            index.map.insert(Arc::clone(&key), slot);
            index.slots[slot as usize].key = Some(key);
        }

        assert!(
            store.get("impostor#1:0").await.unwrap().is_none(),
            "the slot names another key, so this must not serve bytes"
        );
        assert_eq!(store.stats().key_mismatches.load(Ordering::Relaxed), 1);
        // And the damaged entry is dropped, so a re-fill can succeed.
        assert!(!store.lock().map.contains_key("impostor#1:0"));
    }

    /// A corrupted body must be caught when `verify_body` is on. Corrupts the body
    /// through the file so the header (and its CRC) is untouched — the only way to test
    /// the CRC rather than the key check.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_corrupted_body_fails_its_crc_when_verifying() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(4, dir.path()).await;
        store.put("k#1:0", &chunk_of(5, 64)).await.unwrap();

        let slot = *store.lock().map.get("k#1:0").unwrap();
        {
            let at = store.inner.locate(slot);
            // Through the WRITER, and synced: the reader is O_DIRECT, so corruption left
            // only in the page cache would not be visible to the read under test.
            at.extent.writer.write_all_at(&[0xff], at.body_at).unwrap();
            at.extent.writer.sync_data().unwrap();
        }
        assert!(store.get("k#1:0").await.unwrap().is_none());
        assert_eq!(store.stats().crc_mismatches.load(Ordering::Relaxed), 1);
    }

    /// The same corruption is SERVED when verification is off — which is the default, so
    /// this pins the documented trade rather than leaving it implied.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_unverified_store_serves_a_corrupted_body() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig {
            dir: dir.path().to_path_buf(),
            chunk_size: CHUNK,
            capacity_bytes: 4 * (SLOT_HEADER_BYTES as u64 + CHUNK as u64),
            verify_body: false,
            read_shape: ReadShape::TwoRead,
            read_concurrency: 0,
        };
        let store = ChunkStore::open(cfg).await.unwrap();
        store.put("k#1:0", &chunk_of(5, 64)).await.unwrap();
        let slot = *store.lock().map.get("k#1:0").unwrap();
        {
            let at = store.inner.locate(slot);
            // Through the WRITER, and synced: the reader is O_DIRECT, so corruption left
            // only in the page cache would not be visible to the read under test.
            at.extent.writer.write_all_at(&[0xff], at.body_at).unwrap();
            at.extent.writer.sync_data().unwrap();
        }
        let served = store.get("k#1:0").await.unwrap().unwrap();
        assert_eq!(served.body[0], 0xff, "no body check on the default path");
        assert_eq!(store.stats().crc_mismatches.load(Ordering::Relaxed), 0);
    }

    /// **Gate 33.7's offline half.** A store reopened over the same directory must find
    /// its chunks by scanning slot headers, with no snapshot file involved.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_reopened_store_rebuilds_its_index_from_the_slots() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = store_of(8, dir.path()).await;
            store.put("b/k#100:0", &chunk_of(7, 4096)).await.unwrap();
            store.put("b/k#100:1", &chunk_of(8, 99)).await.unwrap();
        }
        let reopened = store_of(8, dir.path()).await;
        assert_eq!(reopened.len(), 2, "the scan must find both chunks");
        let a = reopened.get("b/k#100:0").await.unwrap().unwrap();
        assert_eq!(a.body.len(), 4096);
        assert!(a.body.iter().all(|&b| b == 7));
        let b = reopened.get("b/k#100:1").await.unwrap().unwrap();
        assert_eq!(b.body.len(), 99);
        assert!(b.body.iter().all(|&b| b == 8));
    }

    /// Re-filling a held key must not consume a second slot — chunks are immutable, so a
    /// duplicate write is a touch. Without this a re-read storm would evict the tier.
    #[tokio::test(flavor = "multi_thread")]
    async fn writing_a_held_key_consumes_no_slot() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(2, dir.path()).await;
        store.put("k#1:0", &chunk_of(1, 16)).await.unwrap();
        for _ in 0..5 {
            store.put("k#1:0", &chunk_of(1, 16)).await.unwrap();
        }
        assert_eq!(store.len(), 1);
        assert_eq!(store.stats().write_dedups.load(Ordering::Relaxed), 5);
        assert_eq!(store.stats().evictions.load(Ordering::Relaxed), 0);
    }

    /// A body larger than a slot is refused rather than truncated or allowed to spill
    /// into the next slot.
    #[tokio::test(flavor = "multi_thread")]
    async fn an_oversized_chunk_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(2, dir.path()).await;
        assert!(store.put("k#1:0", &chunk_of(1, CHUNK + 1)).await.is_err());
        assert!(store.is_empty());
    }

    /// Slots must span extents correctly: with more slots than one extent holds, a chunk
    /// in a later extent has to read back from the right file at the right offset.
    #[tokio::test(flavor = "multi_thread")]
    async fn slots_address_correctly_across_extents() {
        let dir = tempfile::tempdir().unwrap();
        // One more slot than an extent holds, so the tier is two files.
        let slots = SLOTS_PER_EXTENT as u64 + 1;
        let store = store_of(slots, dir.path()).await;
        assert_eq!(store.inner.extents.len(), 2);
        // Fill every slot, so the last one necessarily lands in the second extent.
        for i in 0..slots {
            // Cast: `i` is bounded by a small test slot count.
            #[allow(clippy::cast_possible_truncation)]
            let byte = (i % 251) as u8;
            store
                .put(&format!("k#1:{i}"), &chunk_of(byte, 64))
                .await
                .unwrap();
        }
        assert_eq!(store.len(), slots as usize);
        for i in 0..slots {
            #[allow(clippy::cast_possible_truncation)]
            let byte = (i % 251) as u8;
            let got = store.get(&format!("k#1:{i}")).await.unwrap().unwrap();
            assert!(
                got.body.iter().all(|&b| b == byte),
                "slot {i} must read back its own bytes, not a neighbour's"
            );
        }
    }

    /// The accounting gates 33.4/33.5/33.7 are read from must add up: bytes served, a
    /// mean per-hit cost, and what the scan recovered. Byte counts specifically — a short
    /// last chunk is one hit for far fewer bytes, so `hits × chunk_size` would overstate
    /// the rate, which is the number the gate compares against foyer.
    #[tokio::test(flavor = "multi_thread")]
    async fn read_accounting_counts_bytes_and_a_mean_cost() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = store_of(8, dir.path()).await;
            assert_eq!(store.stats().mean_read_nanos(), None, "no hits yet");
            store.put("k#1:0", &chunk_of(1, 4096)).await.unwrap();
            store.put("k#1:1", &chunk_of(2, 100)).await.unwrap();
            assert_eq!(store.stats().written_bytes.load(Ordering::Relaxed), 4196);

            store.get("k#1:0").await.unwrap().unwrap();
            store.get("k#1:1").await.unwrap().unwrap();
            // A miss must not be charged as a read.
            assert!(store.get("k#1:absent").await.unwrap().is_none());
            let stats = store.stats();
            assert_eq!(stats.read_bytes.load(Ordering::Relaxed), 4196);
            assert_eq!(stats.hits.load(Ordering::Relaxed), 2);
            assert!(
                stats.mean_read_nanos().is_some_and(|n| n > 0),
                "two hits must yield a positive mean"
            );
            assert!(stats.read_nanos_max.load(Ordering::Relaxed) > 0);
        }
        // Gate 33.7's numbers come from a REOPEN, so they are asserted on a fresh store
        // over the same directory rather than on the one that did the writing.
        let reopened = store_of(8, dir.path()).await;
        let stats = reopened.stats();
        assert_eq!(stats.scan_recovered.load(Ordering::Relaxed), 2);
        assert!(
            stats.scan_nanos.load(Ordering::Relaxed) > 0,
            "the scan must report how long it took — gate 33.7 asks for it"
        );
    }

    /// **Service time and total time must be separable**, because ADR-0033 gate 33.4 was
    /// written against the wrong one. `read_nanos` is timed around the whole
    /// `spawn_blocking` await and so includes the wait for a blocking thread; `service`
    /// is timed inside it and is what a read costs. On the first hardware arm the former
    /// read 26-31 ms at ~74 concurrent reads, which is queueing, not cost.
    ///
    /// Pinned here: both are positive, service never exceeds total by more than rounding,
    /// and a MISS is charged to neither — averaging misses into a service time would
    /// flatter it.
    #[tokio::test(flavor = "multi_thread")]
    async fn service_time_is_separate_from_queueing_and_misses_are_charged_to_neither() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(8, dir.path()).await;
        store.put("k#1:0", &chunk_of(1, 4096)).await.unwrap();

        let stats = store.stats();
        assert_eq!(stats.mean_service_nanos(), None, "no hits yet");
        assert!(store.get("k#1:absent").await.unwrap().is_none());
        assert_eq!(
            stats.service_nanos_total.load(Ordering::Relaxed),
            0,
            "a miss did no work, so it must not be charged a service time"
        );

        store.get("k#1:0").await.unwrap().unwrap();
        let total = stats.mean_read_nanos().expect("one hit");
        let service = stats.mean_service_nanos().expect("one hit");
        assert!(
            service > 0,
            "the read did real work, so service must be positive"
        );
        assert!(total > 0);
        // The two clocks start and stop at different points, so on a very fast read they
        // can round to the same value; service must never be MEANINGFULLY larger.
        assert!(
            service <= total.saturating_add(total / 2) + 1_000,
            "service {service} ns cannot exceed total {total} ns — the clocks are nested"
        );
        // And the derived queueing must not wrap when they round equal.
        assert!(stats.mean_queue_nanos().is_some());
        assert!(stats.service_nanos_max.load(Ordering::Relaxed) > 0);
    }

    /// **The case O_DIRECT rejects, and the reason the read rounds its length.** An object's
    /// last chunk is short and arbitrary (ADR-0015 clamps it), while direct I/O demands a
    /// block-aligned length — so the read asks for the rounded size and publishes only
    /// `body_len`. A body of 1 byte, of one under a block, of exactly a block, of one over,
    /// and a full chunk all have to come back at their true length with their true bytes.
    ///
    /// ⚠ On macOS this passes through the buffered fallback, so it is NOT proof the direct
    /// path works — it proves the rounding arithmetic. The direct path needs Linux.
    #[tokio::test(flavor = "multi_thread")]
    async fn short_and_block_exact_bodies_round_trip_at_their_true_length() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(8, dir.path()).await;
        for (nth, len) in [1usize, 4095, 4096, 4097, CHUNK].into_iter().enumerate() {
            let key = format!("k#1:{nth}");
            // Cast: the loop's byte tag is small by construction.
            #[allow(clippy::cast_possible_truncation)]
            let tag = (nth as u8) + 1;
            store.put(&key, &chunk_of(tag, len)).await.unwrap();
            let got = store
                .get(&key)
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("len {len} must read back"));
            assert_eq!(got.body.len(), len, "len {len} must not be rounded UP");
            assert!(
                got.body.iter().all(|&b| b == tag),
                "len {len} must hold its own bytes, not a neighbour's or padding"
            );
        }
    }

    /// A chunk size that is not a multiple of the direct-I/O block must be refused at OPEN.
    ///
    /// Every slot body sits at `slot * (SLOT_HEADER_BYTES + chunk_size) + SLOT_HEADER_BYTES`,
    /// so a misaligned chunk size misaligns every body offset and O_DIRECT refuses every
    /// read — as an `EINVAL` that reads like an I/O error rather than the configuration
    /// mistake it is.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_misaligned_chunk_size_is_refused_at_open() {
        let dir = tempfile::tempdir().unwrap();
        let err = ChunkStore::open(StoreConfig {
            dir: dir.path().to_path_buf(),
            chunk_size: CHUNK + 1,
            capacity_bytes: 8 * (SLOT_HEADER_BYTES as u64 + CHUNK as u64),
            verify_body: false,
            read_shape: ReadShape::TwoRead,
            read_concurrency: 0,
        })
        .await
        .expect_err("a misaligned chunk size must not open");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("direct-I/O alignment"),
            "the error must name the real cause, got: {msg}"
        );
    }

    /// **Regression test for the intrusive list.** `forget` on a slot that is already
    /// free must be a no-op: an early version unlinked unconditionally, and since an
    /// unlinked slot has `prev`/`next` both `None` — indistinguishable from being the
    /// list's sole element — it set `lru_head` to `None` and orphaned every other slot,
    /// after which nothing could ever be evicted. It also pushed the slot onto the free
    /// list a second time, so two writers could claim it at once.
    #[tokio::test(flavor = "multi_thread")]
    async fn forgetting_a_free_slot_neither_breaks_the_lru_nor_double_frees() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_of(3, dir.path()).await;
        store.put("k#1:0", &chunk_of(1, 16)).await.unwrap();
        store.put("k#1:1", &chunk_of(2, 16)).await.unwrap();
        let free_before = store.lock().free.len();

        // Slot 2 was never occupied, so this is the double-forget case.
        let untouched = *store.lock().free.last().unwrap();
        store.forget(untouched);
        store.forget(untouched);
        assert_eq!(
            store.lock().free.len(),
            free_before,
            "forgetting a free slot must not lengthen the free list"
        );

        // The LRU must still be intact: filling past capacity has to evict the oldest,
        // which is impossible if lru_head was lost.
        store.put("k#1:2", &chunk_of(3, 16)).await.unwrap();
        store.put("k#1:3", &chunk_of(4, 16)).await.unwrap();
        assert_eq!(store.len(), 3, "the tier holds its three slots");
        assert!(
            store.get("k#1:0").await.unwrap().is_none(),
            "the oldest chunk must still be evictable"
        );
        assert!(store.get("k#1:3").await.unwrap().is_some());
        assert_eq!(store.stats().evictions.load(Ordering::Relaxed), 1);
    }

    /// **The layout's offset arithmetic**, pinned at a production chunk size in every extent
    /// including the last: header entries page-aligned, bodies immediately after a header region
    /// that neither overlaps them nor leaves a gap.
    ///
    /// The stripe-alignment assertion is here because it is *free* and it documents the geometry,
    /// **not** because it buys throughput — that was measured at 0.046 % and is not a lever
    /// (`bench/ladder/results/nvme-stripe-offset.md`). What this test really guards is the
    /// off-by-one: a header region one entry short would put body 0 on top of the last header.
    ///
    /// Pure arithmetic on purpose: asserting it through an open store would mean creating
    /// (and now *allocating*) a 64 GiB extent per extent under test.
    #[test]
    fn every_body_offset_is_stripe_aligned_at_a_production_chunk_size() {
        const PROD_CHUNK: u64 = 16 << 20;
        assert!(
            PROD_CHUNK.is_multiple_of(STRIPE_BYTES as u64),
            "the premise: a production chunk is a whole number of stripes"
        );
        // First and last slot of the first extent, and the first of the second — the three
        // places an off-by-one in the region size or the extent rollover would show.
        for within in [
            0,
            1,
            7,
            SLOTS_PER_EXTENT as u64 - 1,
            SLOTS_PER_EXTENT as u64,
        ] {
            let at = body_offset(within, PROD_CHUNK);
            assert!(
                at.is_multiple_of(STRIPE_BYTES as u64),
                "body of slot {within} at {at} is not stripe-aligned"
            );
        }
        // And the header entries stay page-aligned, which is what O_DIRECT needs of them.
        for within in [0, 1, SLOTS_PER_EXTENT as u64 - 1] {
            assert!(header_offset(within).is_multiple_of(DIRECT_IO_ALIGN as u64));
        }
        // The header region must not overlap the first body, and the first body must sit
        // immediately after it — a gap would waste a stripe, an overlap would serve a header
        // page as chunk bytes.
        assert_eq!(
            header_offset(SLOTS_PER_EXTENT as u64),
            HEADER_REGION_BYTES as u64
        );
        assert_eq!(body_offset(0, PROD_CHUNK), HEADER_REGION_BYTES as u64);
    }

    /// A partial last extent must be sized to the slots it holds, not to a full one — the
    /// difference is 64 GiB of really-allocated space per extent now that `reserve` claims it.
    #[test]
    fn the_last_extent_holds_only_the_slots_that_are_left() {
        assert_eq!(slots_in_extent(SLOTS_PER_EXTENT, 0), SLOTS_PER_EXTENT);
        assert_eq!(slots_in_extent(SLOTS_PER_EXTENT + 1, 1), 1);
        assert_eq!(slots_in_extent(3 * SLOTS_PER_EXTENT + 508, 3), 508);
        // Never past the end: an index beyond the tier's extents yields nothing rather than
        // underflowing into a colossal allocation.
        assert_eq!(slots_in_extent(8, 1), 0);
    }

    /// **The property that makes [`ReadShape`] a tuning knob rather than a second code
    /// path**: both shapes serve byte-identical bodies, at a full body and at a short one.
    ///
    /// The short body is the case the overlapped shape is most likely to get wrong, because it
    /// reads a whole `chunk_size` and trims afterwards rather than reading `body_len` rounded
    /// up — so a bug here would serve a chunk padded with whatever the slot's tail held.
    #[tokio::test(flavor = "multi_thread")]
    async fn both_read_shapes_serve_identical_bodies() {
        for shape in [ReadShape::TwoRead, ReadShape::Overlap] {
            let dir = tempfile::tempdir().unwrap();
            let store = store_shaped(4, dir.path(), shape).await;
            store.put("k#1:0", &chunk_of(0x5a, CHUNK)).await.unwrap();
            store.put("k#1:1", &chunk_of(0x3c, 17)).await.unwrap();
            let full = store
                .get("k#1:0")
                .await
                .unwrap()
                .expect("{shape:?} full body");
            assert_eq!(full.body.len(), CHUNK, "{shape:?} full length");
            assert!(full.body.iter().all(|&b| b == 0x5a), "{shape:?} full bytes");
            let short = store
                .get("k#1:1")
                .await
                .unwrap()
                .unwrap_or_else(|| panic!("{shape:?} lost the short body"));
            assert_eq!(
                short.body.len(),
                17,
                "{shape:?} short length — not the slot"
            );
            assert!(
                short.body.iter().all(|&b| b == 0x3c),
                "{shape:?} short bytes"
            );
        }
    }

    /// A slot holding another key's bytes must be refused on BOTH shapes. The overlapped one
    /// has already read the body by the time it finds out, so this pins that the wasted read
    /// is not also a served one — the failure the key check exists to prevent.
    #[tokio::test(flavor = "multi_thread")]
    async fn both_read_shapes_refuse_a_skewed_slot() {
        for shape in [ReadShape::TwoRead, ReadShape::Overlap] {
            let dir = tempfile::tempdir().unwrap();
            let store = store_shaped(4, dir.path(), shape).await;
            store.put("real#1:0", &chunk_of(7, CHUNK)).await.unwrap();
            let slot = *store.lock().map.get("real#1:0").unwrap();
            // Point a second key at the first key's slot, which is exactly the index/slot
            // skew the on-disk key compare is there to catch.
            store.lock().map.insert(Arc::from("liar#1:0"), slot);
            assert!(
                store.get("liar#1:0").await.unwrap().is_none(),
                "{shape:?} served a slot whose header names another key"
            );
            assert_eq!(
                store.stats().key_mismatches.load(Ordering::Relaxed),
                1,
                "{shape:?} must count the mismatch"
            );
        }
    }

    /// The read ceiling must actually bound reads in flight, and must not lose any.
    ///
    /// Asserted on the counter rather than on a rate, because a rate needs a device and this
    /// runs on a laptop: the property that matters here is that a ceiling of 1 serialises 8
    /// concurrent gets and still serves all 8 correct bodies. That the *number* 16 is the
    /// right ceiling is a hardware measurement, and it lives with
    /// [`DEFAULT_READ_CONCURRENCY`].
    #[tokio::test(flavor = "multi_thread")]
    async fn a_read_ceiling_serialises_without_losing_a_read() {
        let dir = tempfile::tempdir().unwrap();
        let store = store_limited(8, dir.path(), ReadShape::TwoRead, 1).await;
        for n in 0..8u8 {
            let key = format!("k#1:{n}");
            store.put(&key, &chunk_of(n, CHUNK)).await.unwrap();
        }
        let mut tasks = Vec::new();
        for n in 0..8u8 {
            let store = store.clone();
            tasks.push(tokio::spawn(async move {
                let got = store
                    .get(&format!("k#1:{n}"))
                    .await
                    .unwrap()
                    .expect("a hit");
                assert!(
                    got.body.iter().all(|&b| b == n),
                    "chunk {n} came back wrong"
                );
            }));
        }
        for task in tasks {
            task.await.unwrap();
        }
        assert_eq!(store.stats().hits.load(Ordering::Relaxed), 8);
        assert_eq!(store.stats().misses.load(Ordering::Relaxed), 0);
        assert_eq!(store.stats().io_errors.load(Ordering::Relaxed), 0);
    }

    /// A capacity below one slot must still open, as a one-slot tier — a misconfigured
    /// tier degrades rather than taking the node out.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_tiny_capacity_yields_a_one_slot_tier() {
        let dir = tempfile::tempdir().unwrap();
        let cfg = StoreConfig {
            dir: dir.path().to_path_buf(),
            chunk_size: CHUNK,
            capacity_bytes: 1,
            verify_body: false,
            read_shape: ReadShape::TwoRead,
            read_concurrency: 0,
        };
        let store = ChunkStore::open(cfg).await.unwrap();
        assert_eq!(store.capacity_slots(), 1);
        store.put("k#1:0", &chunk_of(1, 16)).await.unwrap();
        store.put("k#1:1", &chunk_of(2, 16)).await.unwrap();
        assert_eq!(store.len(), 1, "one slot holds one chunk");
    }
}
