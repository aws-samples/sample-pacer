//! ADR-0024's registered arena: ONE memory region per rail, registered once at
//! startup and carved into chunk-sized leasable ranges — the host-memory
//! implementor of `buffers.rs`'s [`RdmaBuffers`] seam (planning/19 track D).
//!
//! Why this shape rather than ADR-0018's fixed pool of 64 MiB slots. Requester
//! throughput is `ranges × range_bytes ÷ hold_time`, and on the zero-copy serve
//! path (planning/16 §5) a landed range stays leased until the S3 client has
//! drained the `Bytes` built from it — measured at 30–70× the wire time. Hold
//! time is therefore not ours to shrink without re-introducing the copy-out that
//! path exists to remove, and `range_bytes` is fixed by the cache unit
//! (ADR-0015's `chunk_size`). That leaves the range COUNT as the only term, and
//! the fixed pool made it the one thing it could not be: 64 slots, *divided*
//! across rails, each over-provisioned to 64 MiB so three quarters of every
//! pinned byte was unusable. Registering one arena instead of N slots turns the
//! same pinned bytes into ~4× the concurrent transfers, and lets an operator buy
//! more concurrency with memory alone (`PACER_RDMA_ARENA_BYTES`) — the daemon
//! moved 14.8 GiB/s where the transport delivers ~58 (planning/18).
//!
//! Sizing is arithmetic, not a magic number:
//! `ranges = target_throughput × hold_time ÷ chunk_size`. At the measured
//! ~58 GiB/s ceiling and a 645 ms client drain that is ~2340 ranges ≈ 37 GiB —
//! 1.8 % of a p5.48xlarge's 2 TiB. See [`super::DEFAULT_REQUESTER_ARENA_BYTES`]
//! for what the built-in default commits and why it is deliberately smaller.
//!
//! **Hugepages here are NOT a throughput claim.** planning/18 measured page size
//! as irrelevant to bandwidth (RESULT 5); explicit pages exist so that one MR
//! spanning tens of GiB stays cheap to register and small in the NIC's
//! translation footprint. They are also the fragile part in Kubernetes: a pod
//! that does not *request* `hugepages-<size>` gets `ENOMEM` from
//! `mmap(MAP_HUGETLB)`, so the requested size is a knob
//! ([`ArenaPages`]) the chart derives from its own hugepages request, and a
//! failed hugepage mapping degrades to base pages with a loud warning
//! (ADR-0024 point 3: base pages remain a working fallback). What must NEVER
//! degrade is the arena's SIZE — a silently smaller arena would reproduce
//! exactly the invisible concurrency cap this module removes — so a mapping or
//! registration failure at the requested length is a startup error.
//!
//! Exclusivity, the invariant the seam's docs call out as silent-corruption
//! territory, holds by construction: a range index is handed to exactly one live
//! lease (the free list pops it and `ArenaLease`'s `Drop` pushes it back), so two
//! leases can never name overlapping bytes for a peer to WRITE into.

use std::io;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use ibverbs::{LocalMemorySlice, MemoryRegion, ProtectionDomain, RemoteMemorySlice};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tracing::warn;

use super::buffers::{efa_access_flags, LeasedRange, OwnedRdmaLease, RdmaBuffers, RdmaLease};

/// Ordinary page size on every platform this daemon runs on (x86-64 and
/// aarch64 both use 4 KiB base pages). Ranges are rounded up to it so no two
/// ranges ever share a page: a WRITE landing in one range then cannot dirty a
/// page a neighbouring lease is reading, and every range's SGE is
/// page-aligned.
const BASE_PAGE_BYTES: usize = 4 << 10;

/// Which backing pages an arena got. Requested by the operator (the chart
/// derives it from its own `hugepages` request) and reported after
/// construction, because the request can degrade to [`ArenaPages::Base`] —
/// a result read without knowing the pages it ran on would attribute the
/// fallback to something else.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ArenaPages {
    /// Ordinary 4 KiB pages: no `MAP_HUGETLB`, no node reservation needed, and
    /// the fallback whenever an explicit size is unavailable.
    Base,
    /// 2 MiB explicit hugepages — 512× fewer translations for the NIC/IOMMU to
    /// cache than [`ArenaPages::Base`], and what the chart's `efa.hugepages`
    /// request reserves.
    Huge2Mi,
    /// 1 GiB explicit hugepages — 262144× fewer. Only reachable on nodes that
    /// reserve that size at boot (the planning/18 EFA nodepool did); the
    /// chart requests 2 MiB pages only, so this needs a direct knob override.
    Huge1Gi,
}

impl ArenaPages {
    /// Page size in bytes. A mapping's length must be a multiple of it.
    #[must_use]
    pub fn bytes(self) -> usize {
        match self {
            ArenaPages::Base => BASE_PAGE_BYTES,
            ArenaPages::Huge2Mi => 2 << 20,
            ArenaPages::Huge1Gi => 1 << 30,
        }
    }

    /// Short label for the startup log line and metrics/report text.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            ArenaPages::Base => "4KiB",
            ArenaPages::Huge2Mi => "2MiB",
            ArenaPages::Huge1Gi => "1GiB",
        }
    }

    /// Parse the size an operator asked for, in MiB (`PACER_RDMA_ARENA_PAGE_MIB`):
    /// `2` = 2 MiB, `1024` = 1 GiB, anything else — including the `0` default —
    /// = base pages. Unrecognized values fall back rather than failing startup:
    /// page size is an optimization, and refusing to boot over it would trade a
    /// working RDMA plane for a typo.
    #[must_use]
    pub fn from_mib(mib: usize) -> Self {
        match mib {
            2 => ArenaPages::Huge2Mi,
            1024 => ArenaPages::Huge1Gi,
            _ => ArenaPages::Base,
        }
    }

    /// `log2(page size)`, which is how `mmap` encodes an explicit hugepage size
    /// (`MAP_HUGE_SHIFT`); `None` for base pages, which take no encoding.
    fn huge_shift(self) -> Option<i32> {
        match self {
            ArenaPages::Base => None,
            // 2 MiB = 2^21, 1 GiB = 2^30.
            ArenaPages::Huge2Mi => Some(21),
            ArenaPages::Huge1Gi => Some(30),
        }
    }
}

/// How many `range_bytes`-sized ranges an `arena_bytes` budget affords.
///
/// At least one, always: a node with many rails and a small budget still gets a
/// working (if shallow) arena per rail rather than a zero-range one that would
/// deadlock every lease. That floor is the only way the realized pinned memory
/// can exceed the budget, and only by at most one range per rail.
#[must_use]
pub fn ranges_for(arena_bytes: usize, range_bytes: usize) -> usize {
    (arena_bytes / range_bytes.max(1)).max(1)
}

/// The leasable range size for a cache `chunk_size`: the chunk, rounded up to a
/// base page.
///
/// No further headroom is needed, and the rounding is the "+ headroom" ADR-0024
/// point 2 asks for: a chunk key encodes the `chunk_size` it was cut at
/// (ADR-0015), so a holder can only ever serve a body for a key the requester
/// asked for at ITS chunk size — the served body is bounded by `chunk_size` by
/// construction, not by convention. Bodies larger than a range (a mismatched
/// cluster, or a future non-chunk entry) are the `RdmaBuffer.len` capacity miss
/// the proto documents: the holder streams them instead.
#[must_use]
pub fn range_bytes_for_chunk(chunk_size: usize) -> usize {
    chunk_size.next_multiple_of(BASE_PAGE_BYTES)
}

/// An anonymous private mapping this value exclusively owns, unmapped on drop.
/// Separate from the MR so declaration order can guarantee the required
/// teardown order (deregister, then unmap — deregistering after the pages are
/// gone is undefined).
///
/// Shared with ADR-0028's [`super::CacheSlab`], which maps the same way and owes
/// the same field ordering — it just holds one MR per rail instead of one.
pub(super) struct Mapping {
    pub(super) ptr: *mut u8,
    pub(super) len: usize,
    pub(super) pages: ArenaPages,
}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` are exactly what `map_pages` returned and this is
        // their sole owner. The `ArenaInner` field order guarantees the MR over
        // these pages has already been deregistered.
        unsafe {
            libc::munmap(self.ptr.cast(), self.len);
        }
    }
}

/// One rail's arena: the mapping, its single registration, and the free list
/// over its ranges. Behind an `Arc` so an [`ArenaLease`] can be `'static` — the
/// requester's zero-copy path hands its lease to the S3 client inside a
/// `bytes::Bytes` that outlives the fetch call.
struct ArenaInner {
    /// Registered over the WHOLE mapping, once, at startup — the rule ADR-0018
    /// point 1 and ADR-0024 point 1 both turn on. Declared before `mapping` so
    /// it deregisters before those pages are unmapped.
    mr: MemoryRegion<()>,
    mapping: Mapping,
    range_bytes: usize,
    ranges: usize,
    /// Indices not currently leased, as a LIFO stack: the most recently freed
    /// range is the next one handed out, so a burst reuses the same hot pages
    /// instead of walking the arena. A `std::sync::Mutex` (not tokio's) because
    /// the critical section is a `push`/`pop` that never awaits.
    free_ranges: Mutex<Vec<u32>>,
    /// Permits == free ranges. Leasing waits here when every range is in
    /// flight (the seam's "leasing waits; it never fails" contract) and
    /// `ranges - available_permits()` is the
    /// `pacer_rdma_requester_slots_in_use` gauge.
    free: Arc<Semaphore>,
}

// SAFETY: the only non-`Send`/`Sync` member is `Mapping`'s raw pointer, and the
// mapping is owned exclusively by this `ArenaInner` from construction to drop.
// Every path that dereferences it does so through a range index a live lease
// holds exclusively, so no two threads ever alias the same bytes; the free list
// and the semaphore are themselves `Send + Sync`. The transport shares one
// arena per rail across its tasks, which is what these impls are for.
unsafe impl Send for ArenaInner {}
unsafe impl Sync for ArenaInner {}

impl ArenaInner {
    /// Byte offset of range `index` within the mapping.
    fn offset_of(&self, index: u32) -> usize {
        index as usize * self.range_bytes
    }

    /// Take a free range index. The semaphore permit the caller already holds
    /// guarantees one exists.
    ///
    /// # Panics
    ///
    /// If the free list is empty while a permit is held — that would mean a
    /// lease returned its permit without returning its index, i.e. the
    /// exclusivity invariant is already broken and continuing could hand two
    /// leases the same bytes.
    fn claim_range(&self) -> u32 {
        let popped = self
            .free_ranges
            .lock()
            .expect("arena free list is only ever locked for a push/pop")
            .pop();
        popped.expect("a semaphore permit guarantees a free range")
    }
}

/// A hugepage-backed registered arena, leased per fetch as chunk-sized ranges
/// (ADR-0024). One per rail per direction — the transport builds a requester
/// arena (a peer's WRITE destination) and a holder arena (the staging source
/// for its own WRITEs) on each rail's protection domain.
pub struct HostArena {
    inner: Arc<ArenaInner>,
}

impl HostArena {
    /// Map `ranges × range_bytes` bytes with the requested page size, register
    /// the whole mapping on `pd` once, and hand back the arena with every range
    /// free.
    ///
    /// `pages` is a REQUEST: an explicit-hugepage mapping that fails (the pod
    /// did not request the `hugepages-<size>` resource, or the node's
    /// reservation is exhausted) warns and retries at the SAME length on base
    /// pages, because page size is an optimization and the arena's size is not
    /// (ADR-0024 point 3). Read [`HostArena::pages`] — never the request — when
    /// interpreting a measurement.
    ///
    /// # Errors
    ///
    /// The base-page mapping failing (the arena does not fit in the pod's
    /// memory), or `ibv_reg_mr` failing — out of `RLIMIT_MEMLOCK`, or `ENOMEM`
    /// registering more than the device/host can pin. Both are startup-fatal
    /// for the RDMA plane by design: degrading to a smaller arena would restore
    /// the invisible concurrency cap this arena exists to remove.
    ///
    /// # Panics
    ///
    /// If `ranges` or `range_bytes` is zero — callers derive both from
    /// [`ranges_for`]/[`range_bytes_for_chunk`], which never return zero, so a
    /// zero here is a caller bug, not a runtime condition.
    pub fn new(
        pd: &ProtectionDomain,
        ranges: usize,
        range_bytes: usize,
        pages: ArenaPages,
    ) -> Result<Self> {
        assert!(ranges > 0 && range_bytes > 0, "an arena needs ≥ 1 range");
        let mapping = map_arena(ranges * range_bytes, pages)?;
        // SAFETY: `mapping` owns a valid, writable mapping of exactly
        // `mapping.len` bytes and is moved into the `ArenaInner` below, whose
        // field order keeps it alive until after this MR is deregistered —
        // `register_from_raw`'s requirement.
        let mr = unsafe { pd.register_from_raw(mapping.ptr, mapping.len, efa_access_flags()) }
            .with_context(|| {
                format!(
                    "registering a {}-byte RDMA arena ({} pages, {ranges} × {range_bytes} B ranges)",
                    mapping.len,
                    mapping.pages.label(),
                )
            })?;
        Ok(Self {
            inner: Arc::new(ArenaInner {
                mr,
                mapping,
                range_bytes,
                ranges,
                free_ranges: Mutex::new((0..ranges as u32).rev().collect()),
                free: Arc::new(Semaphore::new(ranges)),
            }),
        })
    }

    /// Leasable ranges in this arena — the concurrent-transfer capacity one
    /// rail contributes, and the denominator of its `slots_in_use` gauge.
    #[must_use]
    pub fn ranges(&self) -> usize {
        self.inner.ranges
    }

    /// Which pages this arena actually got, after any fallback (see
    /// [`HostArena::new`]).
    #[must_use]
    pub fn pages(&self) -> ArenaPages {
        self.inner.mapping.pages
    }

    /// Bytes pinned by this arena's registration — the mapping length, which is
    /// the requested `ranges × range_bytes` rounded up to a whole page. This is
    /// the number the pod's memory limit and the chart's
    /// `efa.pinnedPoolReservation` must cover.
    #[must_use]
    pub fn registered_bytes(&self) -> usize {
        self.inner.mapping.len
    }

    /// Wait for a free range and take it. The one lease path both trait methods
    /// use: an arena lease is always `'static` (it owns an `Arc` of the arena
    /// and an owned permit), which the borrow-scoped holder path can use
    /// unchanged.
    async fn claim(&self) -> ArenaLease {
        let permit = Arc::clone(&self.inner.free)
            .acquire_owned()
            .await
            .expect("the arena semaphore is never closed");
        let index = self.inner.claim_range();
        ArenaLease {
            arena: Arc::clone(&self.inner),
            index,
            _permit: permit,
        }
    }
}

impl RdmaBuffers for HostArena {
    type Lease<'a> = ArenaLease;
    type OwnedLease = ArenaLease;

    /// One range: the ceiling on a single fetch, which is `chunk_size` rounded
    /// up to a page (see [`range_bytes_for_chunk`]).
    fn slot_bytes(&self) -> usize {
        self.inner.range_bytes
    }

    /// Ranges currently leased out. Unlike the fixed pool this replaced, a
    /// value pinned at capacity is now a genuine signal to raise
    /// `PACER_RDMA_ARENA_BYTES` (or to look at client drain rate) rather than
    /// an artifact of a 64-slot ceiling nobody chose.
    fn slots_in_use(&self) -> usize {
        self.inner.ranges - self.inner.free.available_permits()
    }

    /// # Panics
    ///
    /// Never in practice: the arena's semaphore is closed only by `close()`,
    /// which nothing calls — it lives exactly as long as the arena.
    async fn lease(&self) -> ArenaLease {
        self.claim().await
    }

    /// # Panics
    ///
    /// Never in practice — same reasoning as [`RdmaBuffers::lease`].
    async fn lease_owned(&self) -> ArenaLease {
        self.claim().await
    }
}

/// One exclusively-held range of an arena. `'static` (it owns an `Arc` of the
/// arena plus an owned permit) so the requester can move it into a
/// `bytes::Bytes` handed to the S3 client; the range returns to the arena when
/// the lease — and therefore the last `Bytes` built from it — drops.
pub struct ArenaLease {
    arena: Arc<ArenaInner>,
    index: u32,
    _permit: OwnedSemaphorePermit,
}

impl ArenaLease {
    /// This range's byte offset within the arena's mapping.
    fn offset(&self) -> usize {
        self.arena.offset_of(self.index)
    }

    /// The range's bytes. Sound only because the index is exclusively this
    /// lease's for its whole lifetime (see the module doc).
    fn bytes(&self) -> &[u8] {
        // SAFETY: `offset() + range_bytes` is within the mapping (the index
        // came from the arena's own free list, which only ever holds
        // `0..ranges`), the pages stay mapped for as long as the `Arc` this
        // lease holds is alive, and no other lease can name these bytes.
        unsafe {
            std::slice::from_raw_parts(
                self.arena.mapping.ptr.add(self.offset()),
                self.arena.range_bytes,
            )
        }
    }
}

impl Drop for ArenaLease {
    fn drop(&mut self) {
        // Return the index BEFORE `_permit` drops (this body runs first): a
        // permit released while its index is still missing would let the next
        // lease find an empty free list.
        self.arena
            .free_ranges
            .lock()
            .expect("arena free list is only ever locked for a push/pop")
            .push(self.index);
    }
}

impl LeasedRange for ArenaLease {
    /// The `(addr, rkey, len)` for THIS range — the arena's rkey with the
    /// range's own address and length, so a peer's WRITE cannot reach past the
    /// range it was offered even though the whole arena shares one
    /// registration.
    fn remote(&self) -> RemoteMemorySlice {
        self.arena
            .mr
            .remote()
            .slice(self.offset()..self.offset() + self.arena.range_bytes)
    }
}

impl RdmaLease for ArenaLease {
    fn with_bytes_mut<R>(&mut self, f: impl FnOnce(&mut [u8]) -> R) -> R {
        // SAFETY: same exclusivity as `ArenaLease::bytes`, and `&mut self`
        // rules out any other live reference to these bytes through this lease.
        let bytes = unsafe {
            std::slice::from_raw_parts_mut(
                self.arena.mapping.ptr.add(self.offset()),
                self.arena.range_bytes,
            )
        };
        f(bytes)
    }

    fn with_bytes<R>(&self, f: impl FnOnce(&[u8]) -> R) -> R {
        f(self.bytes())
    }

    /// A postable SGE over `bounds` OF THIS RANGE (not of the arena): bounds
    /// are relative to the range, so callers compute offsets in body space and
    /// never see the arena's layout.
    ///
    /// # Panics
    ///
    /// If `bounds` is empty or reaches outside this range (see
    /// [`ibverbs::LocalMemorySlice::slice`]) — a caller posting outside the
    /// range it leased is a caller bug, and on an arena it would be a WRITE
    /// into a neighbouring lease's bytes rather than a contained one.
    fn local_slice(&self, bounds: impl RangeBounds<usize>) -> LocalMemorySlice {
        self.arena
            .mr
            .slice(self.offset()..self.offset() + self.arena.range_bytes)
            .slice(bounds)
    }
}

impl OwnedRdmaLease for ArenaLease {
    type Written = WrittenRange;

    /// # Panics
    ///
    /// If `len` exceeds the range size — the caller validates the body against
    /// the offered `RdmaBuffer` before the WRITE is posted
    /// ([`super::EfaRdmaTransport::serve_via_write`]), so reaching here means
    /// the wire descriptor and the arena already disagree.
    fn into_written(self, len: usize) -> WrittenRange {
        assert!(
            len <= self.arena.range_bytes,
            "written length {len} exceeds arena range size {}",
            self.arena.range_bytes
        );
        WrittenRange { lease: self, len }
    }
}

/// An owning view of the first `len` WRITE-landed bytes of a leased range: what
/// lets the requester hand the S3 layer a `bytes::Bytes::from_owner` backed
/// directly by registered memory, with no copy-out (planning/16 §5). The range
/// is pinned for exactly as long as that `Bytes` and every clone of it lives —
/// which is why anything *retaining* the bytes past the client stream (the
/// daemon's layer-1 admit) copies them out first.
pub struct WrittenRange {
    lease: ArenaLease,
    len: usize,
}

impl AsRef<[u8]> for WrittenRange {
    fn as_ref(&self) -> &[u8] {
        &self.lease.bytes()[..self.len]
    }
}

/// Map `bytes` (rounded up to a whole page) with the requested page size,
/// falling back to base pages at the same length if an explicit size is
/// unavailable.
///
/// # Errors
///
/// The base-page mapping failing — i.e. the arena does not fit, which is fatal
/// rather than degradable (see [`HostArena::new`]).
pub(super) fn map_arena(bytes: usize, want: ArenaPages) -> Result<Mapping> {
    let want_len = bytes.next_multiple_of(want.bytes());
    match map_pages(want_len, want) {
        Ok(ptr) => Ok(Mapping {
            ptr,
            len: want_len,
            pages: want,
        }),
        Err(e) if want != ArenaPages::Base => {
            warn!(
                error = %e, bytes = want_len, requested = want.label(),
                "explicit-hugepage mmap for the RDMA arena failed (pod missing the \
                 hugepages-* resource request, or the node reservation is exhausted); \
                 falling back to 4 KiB pages — the arena is the requested SIZE, only \
                 its translation footprint is worse"
            );
            let base_len = bytes.next_multiple_of(ArenaPages::Base.bytes());
            Ok(Mapping {
                ptr: map_pages(base_len, ArenaPages::Base)
                    .context("mmap of the RDMA arena on base pages")?,
                len: base_len,
                pages: ArenaPages::Base,
            })
        }
        Err(e) => Err(anyhow::Error::new(e)).context("mmap of the RDMA arena on base pages"),
    }
}

/// `mmap` an anonymous private region of `len` bytes with an explicit page size.
///
/// `MAP_POPULATE` on the hugepage path is deliberate: without it, a mapping the
/// hugepage pool cannot actually back fails with **SIGBUS at first touch** —
/// mid-run and un-catchable — whereas pre-faulting turns the same shortfall into
/// an `ENOMEM` from `mmap` that `map_arena` degrades cleanly. It also front-loads
/// the fault cost to startup, which is where an arena's cost belongs.
fn map_pages(len: usize, pages: ArenaPages) -> io::Result<*mut u8> {
    let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS;
    if let Some(shift) = pages.huge_shift() {
        // The size goes in the flags word: MAP_HUGETLB alone silently takes the
        // kernel default (2 MiB), which would make a 1 GiB request a 2 MiB one.
        flags |= libc::MAP_HUGETLB | libc::MAP_POPULATE | (shift << libc::MAP_HUGE_SHIFT);
    }
    // SAFETY: a null hint lets the kernel choose the address, `len > 0` is
    // guaranteed by `HostArena::new`'s assert, and the result is checked
    // against MAP_FAILED before it is used as a pointer.
    let p = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        )
    };
    if p == libc::MAP_FAILED {
        return Err(io::Error::last_os_error());
    }
    Ok(p.cast())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The sizing arithmetic ADR-0024 documents: a budget divided by the chunk
    /// size, and never zero ranges (which would deadlock every lease).
    #[test]
    fn ranges_for_divides_the_budget_and_floors_at_one() {
        assert_eq!(ranges_for(4 << 30, 16 << 20), 256);
        // The ADR's p5 sizing: ~2340 ranges at 58 GiB/s × 645 ms ÷ 16 MiB.
        assert_eq!(ranges_for(37 << 30, 16 << 20), 2368);
        // A budget smaller than one range still yields a usable arena.
        assert_eq!(ranges_for(1 << 20, 16 << 20), 1);
        assert_eq!(ranges_for(0, 16 << 20), 1);
    }

    /// Ranges are chunk-sized, page-rounded — never the fixed 64 MiB slot that
    /// wasted three quarters of every pinned byte.
    #[test]
    fn range_bytes_round_the_chunk_up_to_a_page() {
        assert_eq!(range_bytes_for_chunk(16 << 20), 16 << 20);
        assert_eq!(range_bytes_for_chunk(4 << 20), 4 << 20);
        // An awkward chunk size still leaves every range page-aligned.
        assert_eq!(
            range_bytes_for_chunk((5 << 20) + 1),
            (5 << 20) + BASE_PAGE_BYTES
        );
    }

    /// Page requests map to the sizes `mmap` can encode, and anything else —
    /// including the `0` default — is base pages rather than a boot failure.
    #[test]
    fn page_requests_parse_from_mib() {
        assert_eq!(ArenaPages::from_mib(0), ArenaPages::Base);
        assert_eq!(ArenaPages::from_mib(2), ArenaPages::Huge2Mi);
        assert_eq!(ArenaPages::from_mib(1024), ArenaPages::Huge1Gi);
        assert_eq!(ArenaPages::from_mib(7), ArenaPages::Base);
        assert_eq!(ArenaPages::Huge2Mi.bytes(), 2 << 20);
        assert_eq!(ArenaPages::Huge1Gi.huge_shift(), Some(30));
        assert_eq!(ArenaPages::Base.huge_shift(), None);
    }
}
