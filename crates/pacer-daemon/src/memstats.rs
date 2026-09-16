//! Allocator-level accounting: *why* the resident set grew, not just that it did
//! (planning/17 "OPEN DEFECT — the daemon OOMs on the requester side").
//!
//! `process_resident_memory_bytes` closed the first gap — a restore OOMKilled the
//! daemon and there was no memory series at all to plot afterwards. It cannot
//! close the second one. That run measured the requester holding **15.27 GiB
//! outside foyer, monotonically**, while the holder stayed flat at the RDMA arena
//! floor under the same byte volume, and RSS alone cannot say whether those bytes
//! are:
//!
//! - **live** — something in this process still owns them (a chunk held past its
//!   response, a fill task that never finished, an unbounded queue). That is a
//!   defect in our code and the fix is a code change; or
//! - **free but retained** — we dropped them and the allocator kept the pages.
//!   That is ordinary glibc behaviour on a 192-core node: `M_ARENA_MAX` defaults
//!   to `8 × ncores`, and the dynamic mmap threshold climbs to 32 MiB once
//!   chunk-sized blocks are freed, after which a freed 16 MiB chunk lands on a
//!   free list instead of going back to the kernel. The fix is then
//!   *configuration* — glibc reads `MALLOC_ARENA_MAX` and `MALLOC_TRIM_THRESHOLD_`
//!   from the environment, so it needs no code change and no rebuild.
//!
//! The two have opposite fixes, produce an identical RSS curve, and the OOM is
//! not reproducible on demand (planning/17: it needs an accumulated floor, so "I
//! ran it and it didn't OOM" proves nothing). `mallinfo2` separates them in ONE
//! scrape — `uordblks` is live, `fordblks` is retained — which is the entire
//! reason this module exists. It deliberately exports raw allocator quantities
//! and no verdict: the arithmetic against `foyer_memory_usage` needs the time
//! axis, and that lives in the sampler (`bench/ladder/memwatch.sh`).

/// One `mallinfo2` sample, in bytes, with the four fields that answer the
/// live-versus-retained question. The other seven `mallinfo2` fields (block
/// counts and the unmaintained `usmblks`/`smblks` pair) are dropped on purpose:
/// a series nobody can act on still costs a scrape and a dashboard row.
///
/// **Live bytes are `in_use + mmapped`, not `in_use` alone.** `uordblks` counts
/// only chunks carved out of the arenas; a block the allocator served by `mmap`
/// is counted in `hblkhd` and appears in NEITHER `uordblks` nor `fordblks`. The
/// unit test measured it: an 8 MiB live `Vec` produced `mmapped_bytes: 8_392_704`
/// with `in_use_bytes` unmoved at 89_104. That is not a footnote here — 16 MiB
/// chunks are far above the mmap threshold, so reading `uordblks` as "live" would
/// have missed the requester's growth in precisely the allocation shape this
/// module exists to attribute.
///
/// The three regimes it distinguishes, which is the point:
/// - `mmapped` grows ⇒ live blocks, served by `mmap`, released on free.
/// - `in_use` grows ⇒ live blocks out of the arenas.
/// - `free_retained` grows ⇒ freed by us, kept by the allocator. This is the
///   regime the dynamic mmap threshold walks into: once it climbs past
///   `chunk_size` (up to 32 MiB), chunk-sized allocations stop being `mmap`ed and
///   a free stops being a release.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MallocStats {
    /// `arena`: total bytes in `sbrk`-grown heaps, summed over every arena.
    /// Grows with thread count on glibc (one arena per contending thread, up to
    /// `M_ARENA_MAX`) and never shrinks below its high-water mark, so this is
    /// the term that makes a many-core node's floor higher than a small node's.
    pub heap_bytes: u64,
    /// `hblkhd`: bytes in blocks the allocator obtained directly by `mmap`
    /// (anything over the mmap threshold). These *are* returned to the kernel on
    /// free, which is why the threshold's dynamic growth matters: once it passes
    /// `chunk_size`, chunk-sized allocations move out of this term and into
    /// [`Self::heap_bytes`], where a free is no longer a release.
    pub mmapped_bytes: u64,
    /// `uordblks`: allocated, not yet freed, and **carved out of the arenas** —
    /// `mmap`-served blocks are NOT here, they are in [`Self::mmapped_bytes`], so
    /// live memory is the sum of the two. Includes foyer's in-memory tier, so
    /// subtract `foyer_memory_usage` before reading it as "ours outside the
    /// cache". Monotonic growth in the sum is a retention defect in our code.
    pub in_use_bytes: u64,
    /// `fordblks`: bytes freed by us but still held by the allocator. Growth here
    /// with flat [`Self::in_use_bytes`] is allocator retention — the
    /// environment-only fix, not a code bug.
    pub free_retained_bytes: u64,
}

/// Sample the C allocator's own accounting. `None` where it does not exist:
/// `mallinfo2` is glibc ≥ 2.33 (the daemon's AL2023 base ships 2.34), so a musl
/// or macOS build gets no series rather than a wrong one.
///
/// Called once per metrics scrape. `mallinfo2` takes each arena's lock in turn,
/// which is why it is not called from anywhere on the data path.
#[cfg(all(target_os = "linux", target_env = "gnu"))]
pub fn sample() -> Option<MallocStats> {
    /// Widen a `size_t` field without a lossy cast. Infallible on the 64-bit
    /// targets the daemon builds for; saturating keeps a hypothetical 32-bit
    /// build lint-clean instead of silently wrapping.
    fn bytes(v: libc::size_t) -> u64 {
        u64::try_from(v).unwrap_or(u64::MAX)
    }

    // SAFETY: `mallinfo2` takes no arguments and returns a by-value struct of
    // integers. It has no failure mode and no pointer in its result, so there is
    // nothing to validate and nothing whose lifetime we depend on.
    let info = unsafe { libc::mallinfo2() };
    Some(MallocStats {
        heap_bytes: bytes(info.arena),
        mmapped_bytes: bytes(info.hblkhd),
        in_use_bytes: bytes(info.uordblks),
        free_retained_bytes: bytes(info.fordblks),
    })
}

/// Sample the C allocator's own accounting — the non-glibc build, which has no
/// `mallinfo2` to call. See the glibc variant for what the fields mean.
#[cfg(not(all(target_os = "linux", target_env = "gnu")))]
pub fn sample() -> Option<MallocStats> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// On glibc the sample must be internally consistent: a live heap allocation
    /// shows up in `in_use_bytes`, a live mmapped one in `mmapped_bytes`, and
    /// live + retained cannot exceed the capacity the allocator obtained to hold
    /// them. This is the field-meaning assertion — what catches a future `libc`
    /// release reordering the struct, which would otherwise publish plausible
    /// nonsense — so it has to model the fields' real meanings, including which
    /// allocations each one can see.
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    #[test]
    fn glibc_sample_is_self_consistent() {
        // TWO live allocations, because `uordblks` and `hblkhd` count different
        // things and a big allocation is invisible to the first of them.
        //
        // glibc routes any request above `M_MMAP_THRESHOLD` (128 KiB by default,
        // and dynamically raised) straight to `mmap`, where it is accounted in
        // `hblkhd` — `mmapped_bytes` — and NOT in `uordblks`. So `small` is what
        // proves `in_use_bytes` counts live heap bytes, while `big` is only
        // required to show up in the total. Asserting a multi-MiB allocation
        // against `in_use_bytes` alone fails on every glibc host (observed:
        // in_use 162480 with mmapped 8392704 for an 8 MiB `held`), which is what
        // it did on CI.
        // `black_box` is load-bearing, not decoration: in a RELEASE build LLVM
        // deletes an allocation whose contents are never observed, so without it
        // this test asserts on allocations that were never made. Observed on CI as
        // `mmapped_bytes: 0` for an 8 MiB vector — the debug `rust` job passed while
        // `build-binary` (release) failed on the same source.
        let small = std::hint::black_box(vec![0_u8; 32 << 10]);
        let big = std::hint::black_box(vec![0_u8; 8 << 20]);
        let s = sample().expect("glibc target must produce a sample");
        assert!(
            s.in_use_bytes >= small.len() as u64,
            "live heap bytes not counted in uordblks: {s:?}"
        );
        assert!(
            s.in_use_bytes + s.mmapped_bytes >= (small.len() + big.len()) as u64,
            "live bytes counted in neither uordblks nor hblkhd: {s:?}"
        );
        // `heap_bytes` is arena capacity, and the arenas hold exactly the in-use
        // and free-retained chunks — `mmapped` sits outside it, which is the
        // asymmetry the sum above exists to respect.
        assert!(
            s.in_use_bytes + s.free_retained_bytes <= s.heap_bytes,
            "arena accounting exceeds arena capacity: {s:?}"
        );
        // Keep both alive PAST the sample: dropping earlier would let the
        // allocator return the pages before `mallinfo2` ran.
        std::hint::black_box((&small, &big));
        drop(small);
        drop(big);
    }

    /// Off glibc there is no `mallinfo2`, and the contract is an absent series
    /// rather than zeros — zeros would plot as "nothing is allocated", which is a
    /// wrong answer where `None` is an honest one.
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    #[test]
    fn non_glibc_reports_nothing() {
        assert_eq!(sample(), None);
    }
}
