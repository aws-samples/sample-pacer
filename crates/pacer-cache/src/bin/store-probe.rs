//! Measure the ADR-0033 chunk store's read path on its own, without a model load.
//!
//! # Why this exists
//!
//! Every figure this tier has ever been judged on came out of a full 70B vLLM or DCP arm:
//! provision a p5, warm 131 GiB, load a model, read the rate off a delta. That costs tens of
//! minutes and a GPU node per data point, and it is why **five** candidate explanations for the
//! gap to the device survived four separate arms — and why one of them was "refuted" by a
//! measurement that changed two things at once (`results/c5-dcp-store-single-read.md` merged the
//! two reads *and* made them stripe-misaligned, then reported the sum as +2.7 %).
//!
//! This binary reads the same store through the same [`ChunkStore::get`] and reports the same
//! service/queue decomposition, in seconds, with one variable moving at a time. The target it is
//! aimed at is fio's on the identical shape: **43.208 GiB/s at 17.3 ms per read for 48 concurrent
//! 16 MiB `O_DIRECT` reads**, where the store measured 19.926 at 43.8 ms.
//!
//! # What it found, on its first run (2026-09-11, one p5.48xlarge)
//!
//! **The read path was never the wall.** At depth 48 it reads **43.06 GiB/s at 16.9 ms**
//! against fio's 43.58 at 17.2 on the same node and mount — 98.8 % of the device — and the
//! three differences from fio that had been left open are all nulls:
//!
//! | variable | control | treatment | verdict |
//! |---|---|---|---|
//! | `--shape` | two-read 43.055 | overlap 43.029 | **null** — the dependent header read is free |
//! | `--numa` | default 43.055 | interleave 43.075 | **null**, and fio agrees (bind:0 43.541 vs spread 43.579) |
//! | `--pages` | 2m 43.055 | base 44.140 | **null** (base marginally ahead, inside noise) |
//!
//! So the 40–43.8 ms per-hit service times in `c5-dcp-store-odirect.md` and
//! `c5-dcp-store-single-read.md`, and the 19.9–25 GiB/s they came with, are **not** properties
//! of this read path. They are what it does when it is asked for too much at once:
//!
//! | depth | 4 | 8 | 16 | 24 | 32 | 48 | 64 |
//! |---|---|---|---|---|---|---|---|
//! | GiB/s | 42.3 | 47.4 | **48.7** | 48.2 | 45.7 | 43.1 | 43.4 |
//! | svc ms | 1.5 | 2.6 | 5.1 | 7.7 | 10.8 | 16.9 | 22.3 |
//!
//! Service is *linear* in depth — pure queueing — and throughput peaks at 16 and then falls.
//! That is [`pacer_cache::store::DEFAULT_READ_CONCURRENCY`], and `--read-concurrency` is the
//! flag that measures it: `--depth 64 --read-concurrency 16` against a bare `--depth 64` asks
//! whether a ceiling recovers the knee when the caller overshoots it, which is the question
//! that matters because **the daemon does not choose its own depth** (`fill_parallelism` is per
//! GET, `delivery.parallelism` per request, and N requests multiply).
//!
//! The three refuted knobs are kept rather than deleted: each one costs a flag and answers "did
//! you check?" in seconds, and two of them had already been *argued* both ways in this repo.
//!
//! # Usage
//!
//! ```text
//! store-probe --dir /mnt/k8s-disks/0/pacer-probe --slots 512 --depth 48 --reads 4096 \
//!             --shape two-read --numa default --pages 2m
//! ```
//!
//! It warms the tier itself on first run and reuses it afterwards (`--warm-only` to stop after
//! filling). Reads are `O_DIRECT`, so a second run over the same directory is not reading a page
//! cache — but the `device / probe` line is printed anyway, because that is the check that caught
//! every earlier tier number being a memcpy out of RAM.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use pacer_cache::chunk::CachedChunk;
use pacer_cache::frames::{FrameFill, FrameSource};
use pacer_cache::store::{ChunkStore, ReadShape, StoreConfig};

/// Bytes per sector in `/proc/diskstats`, which reports sectors and not bytes. Fixed at 512 by
/// the kernel's interface regardless of the device's own logical block size.
const DISKSTATS_SECTOR_BYTES: u64 = 512;

/// Chunk size the probe defaults to: the cluster's own (ADR-0015), so a number it produces is
/// comparable with every arm in `bench/ladder/results`.
const DEFAULT_CHUNK_MIB: usize = 16;

/// Slots to warm by default. 512 × 16 MiB = 8 GiB — enough that the read loop is not revisiting
/// a handful of slots, small enough that warming is under a minute.
const DEFAULT_SLOTS: usize = 512;

/// Concurrent readers by default: the concurrency the store was measured at when fio reached
/// 43.208 GiB/s on the same node, so the two are directly comparable.
const DEFAULT_DEPTH: usize = 48;

/// Reads per run by default. At 16 MiB that is 64 GiB, which takes a few seconds at device rate
/// and is long enough that a per-read mean is not dominated by the first few.
const DEFAULT_READS: usize = 4096;

/// Flags this platform adds to the frame mapping beyond `MAP_PRIVATE | MAP_ANONYMOUS`.
///
/// `MAP_POPULATE` front-loads the faults so the read loop is not timing them, and matches how
/// the daemon maps its slab. It and `MAP_HUGETLB` are Linux-only spellings, so they are behind a
/// shim rather than referenced directly — otherwise this file would break
/// `cargo check -p pacer-cache`, which is the documented inner loop on a Mac.
#[cfg(target_os = "linux")]
const MAP_POPULATE_FLAG: i32 = libc::MAP_POPULATE;
/// Zero off Linux, where the probe cannot do its job anyway (see [`bind_pages`]).
#[cfg(not(target_os = "linux"))]
const MAP_POPULATE_FLAG: i32 = 0;

/// `MAP_HUGETLB`, or zero where it does not exist. See [`MAP_POPULATE_FLAG`].
#[cfg(target_os = "linux")]
const MAP_HUGETLB_FLAG: i32 = libc::MAP_HUGETLB;
/// Zero off Linux.
#[cfg(not(target_os = "linux"))]
const MAP_HUGETLB_FLAG: i32 = 0;

/// Bit position `mmap` reads the explicit page size from, or zero where it does not exist.
#[cfg(target_os = "linux")]
const MAP_HUGE_SHIFT_BITS: i32 = libc::MAP_HUGE_SHIFT;
/// Zero off Linux.
#[cfg(not(target_os = "linux"))]
const MAP_HUGE_SHIFT_BITS: i32 = 0;

/// Apply a NUMA policy to an existing mapping.
///
/// # Errors
///
/// `mbind` failing, or being asked for on a platform that has no such call — an error and not a
/// warning, because an arm that asked to interleave and silently did not would report the
/// control's number under the treatment's name.
#[cfg(target_os = "linux")]
fn bind_pages(ptr: *mut libc::c_void, len: usize, policy: i32, mask: u64) -> anyhow::Result<()> {
    // AFTER the populate, with MPOL_MF_MOVE: `MAP_POPULATE` has already faulted every page in
    // under the default policy, so a policy set without asking for a move would apply to nothing
    // and the arm would quietly measure the default.
    // SAFETY: `ptr`/`len` name a live mapping and `mask` is one live `u64`.
    let rc = unsafe {
        libc::syscall(
            libc::SYS_mbind,
            ptr,
            len,
            policy,
            std::ptr::from_ref(&mask),
            NODE_MASK_BITS,
            MPOL_MF_MOVE,
        )
    };
    if rc != 0 {
        return Err(std::io::Error::last_os_error().into());
    }
    Ok(())
}

/// Off Linux there is no `mbind`, so asking for a placement is refused rather than ignored.
///
/// # Errors
///
/// Always.
#[cfg(not(target_os = "linux"))]
fn bind_pages(
    _ptr: *mut libc::c_void,
    _len: usize,
    _policy: i32,
    _mask: u64,
) -> anyhow::Result<()> {
    anyhow::bail!("NUMA placement needs mbind(2), which only Linux has")
}

/// Page-size shift for 2 MiB explicit hugepages, as `mmap` encodes it in its flags word.
const HUGE_SHIFT_2M: i32 = 21;

/// Page-size shift for 1 GiB explicit hugepages.
const HUGE_SHIFT_1G: i32 = 30;

/// `mbind` policy: interleave the mapping's pages over the given node mask.
const MPOL_INTERLEAVE: i32 = 3;

/// `mbind` policy: allocate the mapping's pages only from the given node mask.
const MPOL_BIND: i32 = 2;

/// `mbind` flag: move pages that are already faulted in, not just future faults. Paired with
/// `MPOL_MF_MOVE` so a mapping created with `MAP_POPULATE` is actually re-placed rather than
/// silently keeping the policy the populate used.
#[cfg(target_os = "linux")]
const MPOL_MF_MOVE: u64 = 1 << 1;

/// Bits in the node mask word `mbind` is handed. One `u64` covers 64 NUMA nodes, which is every
/// machine this will run on by a wide margin.
const NODE_MASK_BITS: u64 = 64;

/// Where the probe's frames come from, which is the variable `--numa` and `--pages` move.
struct Placement {
    /// Explicit page size as an `mmap` flag shift, or `None` for base pages.
    huge_shift: Option<i32>,
    /// `mbind` policy and node mask to apply after mapping, or `None` for the kernel default
    /// (first touch, i.e. the mapping thread's node — the daemon's slab today).
    policy: Option<(i32, u64)>,
    /// What to print so a result names its own placement rather than leaving it to the reader.
    label: String,
}

/// A frame pool over one mapping, standing in for ADR-0028's slab.
///
/// Deliberately the same *shape* as the real one — a fixed set of `frame_bytes` frames over a
/// single mapping, handed out by a free list — because the point is to reproduce the daemon's
/// destination memory, not to build a better one.
struct MappedFrames {
    inner: Arc<Mapping>,
    counters: Arc<FrameCounters>,
}

/// Frames handed out versus decodes that had to use the heap.
///
/// Held behind an `Arc` because [`pacer_cache::frames::install_frame_source`] takes ownership of
/// the source, and a fallback count nobody can read afterwards is exactly the silent regression
/// ADR-0028 warns about: the slab stays registered, the reads still work, and they quietly go
/// through the buffered descriptor instead.
#[derive(Default)]
struct FrameCounters {
    stored: AtomicU64,
    fallbacks: AtomicU64,
}

/// The mapping and its free list, `Arc`-held so a frame can outlive the claim call.
struct Mapping {
    ptr: *mut u8,
    len: usize,
    frame_bytes: usize,
    free: Mutex<Vec<usize>>,
}

// SAFETY: `ptr` is a private anonymous mapping owned by this value for its whole life. Frames
// are disjoint ranges of it and the free list guarantees at most one holder each, so no two
// threads ever reach the same bytes.
unsafe impl Send for Mapping {}
unsafe impl Sync for Mapping {}

impl Drop for Mapping {
    fn drop(&mut self) {
        // SAFETY: `ptr`/`len` are exactly what `mmap` returned for this mapping.
        unsafe { libc::munmap(self.ptr.cast(), self.len) };
    }
}

/// One claimed frame, which returns itself to the free list when the last `Bytes` clone over it
/// drops — the same lifetime rule the real slab has, and the reason a frame cannot be recycled
/// under an in-flight read.
struct MappedFrame {
    mapping: Arc<Mapping>,
    index: usize,
    len: usize,
}

impl MappedFrame {
    /// The frame's bytes.
    fn as_slice(&self) -> &[u8] {
        // SAFETY: the free list handed out `index` exclusively and `len <= frame_bytes`, so this
        // range is inside the mapping and reachable by no one else.
        unsafe {
            std::slice::from_raw_parts(
                self.mapping.ptr.add(self.index * self.mapping.frame_bytes),
                self.len,
            )
        }
    }
}

impl AsRef<[u8]> for MappedFrame {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Drop for MappedFrame {
    fn drop(&mut self) {
        self.mapping
            .free
            .lock()
            .expect("the probe's free list is only locked for a push/pop")
            .push(self.index);
    }
}

impl FrameFill for MappedFrame {
    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: as `as_slice`, and `&mut self` proves this is the only reference.
        unsafe {
            std::slice::from_raw_parts_mut(
                self.mapping.ptr.add(self.index * self.mapping.frame_bytes),
                self.len,
            )
        }
    }

    fn seal(self: Box<Self>) -> Bytes {
        Bytes::from_owner(*self)
    }
}

impl FrameSource for MappedFrames {
    fn claim(&self, len: usize) -> Option<Box<dyn FrameFill>> {
        if len > self.inner.frame_bytes {
            return None;
        }
        let index = self
            .inner
            .free
            .lock()
            .expect("the probe's free list is only locked for a push/pop")
            .pop()?;
        Some(Box::new(MappedFrame {
            mapping: Arc::clone(&self.inner),
            index,
            len,
        }))
    }

    fn heap_fallback(&self) {
        self.counters.fallbacks.fetch_add(1, Ordering::Relaxed);
    }

    fn stored(&self) {
        self.counters.stored.fetch_add(1, Ordering::Relaxed);
    }
}

/// Map `frames × frame_bytes` bytes under `placement`.
///
/// # Errors
///
/// `mmap` failing, or `mbind` failing when a policy was asked for — the latter is an error and
/// not a warning on purpose: an arm that asked to interleave and silently did not would report
/// the control's number as the treatment's, which is the exact failure this probe exists to stop
/// happening again.
fn map_frames(frames: usize, frame_bytes: usize, placement: &Placement) -> anyhow::Result<Mapping> {
    let len = frames
        .checked_mul(frame_bytes)
        .ok_or_else(|| anyhow::anyhow!("{frames} frames of {frame_bytes} bytes overflows"))?;
    let mut flags = libc::MAP_PRIVATE | libc::MAP_ANONYMOUS | MAP_POPULATE_FLAG;
    if let Some(shift) = placement.huge_shift {
        flags |= MAP_HUGETLB_FLAG | (shift << MAP_HUGE_SHIFT_BITS);
    }
    // SAFETY: a fresh anonymous mapping; no existing address is being replaced.
    let ptr = unsafe {
        libc::mmap(
            std::ptr::null_mut(),
            len,
            libc::PROT_READ | libc::PROT_WRITE,
            flags,
            -1,
            0,
        )
    };
    if ptr == libc::MAP_FAILED {
        let err = std::io::Error::last_os_error();
        anyhow::bail!(
            "mmap of {len} bytes for the frame pool failed: {err}. On the hugepage placements \
             this is usually the pod not requesting the hugepages-* resource, or the node's \
             reservation being exhausted — run with --pages base to separate the two"
        );
    }
    if let Some((policy, mask)) = placement.policy {
        if let Err(err) = bind_pages(ptr, len, policy, mask) {
            // SAFETY: undoing the mapping this function just made.
            unsafe { libc::munmap(ptr, len) };
            anyhow::bail!(
                "applying {} over node mask {mask:#x} failed: {err}. Refusing rather than \
                 measuring the default placement under a treatment's name",
                placement.label
            );
        }
    }
    Ok(Mapping {
        ptr: ptr.cast(),
        len,
        frame_bytes,
        free: Mutex::new((0..frames).rev().collect()),
    })
}

/// Sectors read per device, from `/proc/diskstats`.
///
/// Field 3 is the device name and field 6 is sectors read (the kernel's documented layout). Not
/// namespaced, so a pod sees the host's counters — which is what makes this usable as the
/// honesty check at all.
///
/// # Errors
///
/// None today: a missing `/proc/diskstats` is reported as an empty map, so the cross-check reads
/// 0.000 and says it did not measure rather than failing an otherwise good run. The `Result` is
/// kept because a parse that starts wanting to be strict belongs here and not at the call site.
fn diskstats() -> anyhow::Result<HashMap<String, u64>> {
    /// Zero-based index of the device name among the whitespace-separated fields.
    const NAME_FIELD: usize = 2;
    /// Zero-based index of "sectors read".
    const SECTORS_READ_FIELD: usize = 5;
    // Absent rather than an error where there is no procfs: the honesty check then reports 0.000
    // and says so, which is a truthful "not measured" instead of failing a run that is otherwise
    // fine.
    let Ok(text) = std::fs::read_to_string("/proc/diskstats") else {
        return Ok(HashMap::new());
    };
    let mut out = HashMap::new();
    for line in text.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        let (Some(name), Some(sectors)) = (
            fields.get(NAME_FIELD),
            fields.get(SECTORS_READ_FIELD).and_then(|s| s.parse().ok()),
        ) else {
            continue;
        };
        // The ARRAY and its PARTITIONS are excluded, because the kernel counts a read against
        // both the member and its md device: summing everything reported `device / probe` of
        // exactly 2.000 on the first run of this probe, which reads as "twice as many bytes as
        // we asked for" and is really the same bytes counted twice.
        //
        // Leaf devices only, therefore. Every one that moved is still printed individually, so
        // the reader sees WHICH drives served rather than trusting a name rule — that matters
        // because the EBS root is an `nvme*` too, and it is `nvme0n1` on the p5 against
        // `nvme2n1` on the c8gd (`nvme-device-truth.md`).
        let is_array = name.starts_with("md");
        let is_partition = name.rsplit_once('p').is_some_and(|(head, tail)| {
            head.contains('n') && !tail.is_empty() && tail.chars().all(char::is_numeric)
        });
        if is_array || is_partition {
            continue;
        }
        out.insert((*name).to_owned(), sectors);
    }
    Ok(out)
}

/// What the probe was asked to do.
struct Args {
    dir: std::path::PathBuf,
    chunk_bytes: usize,
    slots: usize,
    depth: usize,
    reads: usize,
    shape: ReadShape,
    placement: Placement,
    warm_only: bool,
    read_concurrency: usize,
}

/// Parse `--flag value` pairs, or die naming the flag.
///
/// Hand-rolled rather than `clap`: this crate does not depend on it, and a probe that needs a new
/// dependency in the daemon's own cache crate to exist is a worse trade than twenty lines here.
///
/// # Errors
///
/// An unknown flag, a missing value, or a value that does not parse — all of them loudly, because
/// a probe that ignored a typo'd `--shape` would report the default under the treatment's name.
fn parse_args() -> anyhow::Result<Args> {
    let mut dir = None;
    let mut chunk_mib = DEFAULT_CHUNK_MIB;
    let mut slots = DEFAULT_SLOTS;
    let mut depth = DEFAULT_DEPTH;
    let mut reads = DEFAULT_READS;
    let mut shape = ReadShape::TwoRead;
    let mut numa = "default".to_owned();
    let mut pages = "base".to_owned();
    let mut warm_only = false;
    // 0 = unlimited, which is what every arm before the ceiling existed measured. The probe
    // does NOT default to the built-in ceiling: `--depth 64 --read-concurrency 16` against
    // `--depth 64` alone is the whole experiment, and a default would make the control
    // unreachable without knowing to ask for it.
    let mut read_concurrency = 0;
    let mut argv = std::env::args().skip(1);
    while let Some(flag) = argv.next() {
        let mut value = || {
            argv.next()
                .ok_or_else(|| anyhow::anyhow!("{flag} needs a value"))
        };
        match flag.as_str() {
            "--dir" => dir = Some(std::path::PathBuf::from(value()?)),
            "--chunk-mib" => chunk_mib = value()?.parse()?,
            "--slots" => slots = value()?.parse()?,
            "--depth" => depth = value()?.parse()?,
            "--reads" => reads = value()?.parse()?,
            "--shape" => shape = value()?.parse()?,
            "--numa" => numa = value()?,
            "--pages" => pages = value()?,
            "--read-concurrency" => read_concurrency = value()?.parse()?,
            "--warm-only" => warm_only = true,
            other => anyhow::bail!(
                "unknown flag {other:?}; \
                 --dir --chunk-mib --slots --depth --reads --shape --numa --pages \
                 --read-concurrency --warm-only"
            ),
        }
    }
    Ok(Args {
        dir: dir.ok_or_else(|| anyhow::anyhow!("--dir is required"))?,
        chunk_bytes: chunk_mib << 20,
        slots,
        depth,
        reads,
        shape,
        placement: placement_of(&numa, &pages)?,
        warm_only,
        read_concurrency,
    })
}

/// Turn the `--numa`/`--pages` spellings into a [`Placement`].
///
/// # Errors
///
/// An unrecognised spelling, rather than a default — see [`parse_args`].
fn placement_of(numa: &str, pages: &str) -> anyhow::Result<Placement> {
    let huge_shift = match pages {
        "base" => None,
        "2m" => Some(HUGE_SHIFT_2M),
        "1g" => Some(HUGE_SHIFT_1G),
        other => anyhow::bail!("unknown --pages {other:?} (base|2m|1g)"),
    };
    let policy = match numa {
        // The daemon's slab today: no `mbind` at all, so MAP_POPULATE places every page on
        // whichever node this thread is running on.
        "default" => None,
        "interleave" => Some((MPOL_INTERLEAVE, u64::MAX)),
        node if node.starts_with("bind:") => {
            let n: u32 = node["bind:".len()..].parse()?;
            anyhow::ensure!(
                u64::from(n) < NODE_MASK_BITS,
                "--numa bind:{n} is outside the {NODE_MASK_BITS}-node mask this probe builds"
            );
            Some((MPOL_BIND, 1u64 << n))
        }
        other => anyhow::bail!("unknown --numa {other:?} (default|interleave|bind:N)"),
    };
    Ok(Placement {
        huge_shift,
        policy,
        label: format!("numa={numa} pages={pages}"),
    })
}

/// The key for warm chunk `n`. Shaped like a real chunk key (ADR-0015 embeds the chunk size), so
/// the store's own parsing and the header's key compare do the same work they do in production.
fn key_of(n: usize, chunk_bytes: usize) -> String {
    format!("probe/slab#{chunk_bytes}:{n}")
}

/// Fill every slot, so the read loop measures reads and not misses.
///
/// # Errors
///
/// An I/O error from the store's write path.
async fn warm(store: &ChunkStore, args: &Args) -> anyhow::Result<()> {
    let body = CachedChunk::new(Bytes::from(vec![0xa5u8; args.chunk_bytes]));
    for n in 0..args.slots {
        store.put(&key_of(n, args.chunk_bytes), &body).await?;
    }
    Ok(())
}

/// Run `args.reads` gets with `args.depth` in flight, returning the wall clock it took.
///
/// `depth` tasks each running a sequential loop, rather than a buffered stream: that is exactly
/// fio's `numjobs` model, so "48 concurrent" means the same thing in both tools. A stream with a
/// buffer would let the in-flight count sag whenever a completion is slow to be polled, and the
/// number this probe exists to compare against is fio's.
///
/// # Errors
///
/// An I/O error from the store, a task panicking, or any key missing — a miss means the tier was
/// not warm and every rate computed from it would be a fiction.
async fn read_loop(store: &ChunkStore, args: &Args) -> anyhow::Result<std::time::Duration> {
    let started = std::time::Instant::now();
    let mut tasks = Vec::with_capacity(args.depth);
    for task in 0..args.depth {
        let store = store.clone();
        let (slots, chunk_bytes) = (args.slots, args.chunk_bytes);
        // Reads are spread over the whole warm set and each task starts at a different slot, so
        // the tasks are not walking the same offsets in lockstep.
        let mine = args.reads.div_ceil(args.depth);
        tasks.push(tokio::spawn(async move {
            for i in 0..mine {
                let n = (task + i * 7) % slots;
                let key = key_of(n, chunk_bytes);
                let got = store.get(&key).await?;
                anyhow::ensure!(got.is_some(), "{key} missed — the tier is not warm");
            }
            anyhow::Ok(())
        }));
    }
    for task in tasks {
        task.await??;
    }
    Ok(started.elapsed())
}

/// `bytes` as GiB. One place, so the several rates below cannot disagree about the divisor.
///
/// The precision loss is deliberate and report-only: at these magnitudes an `f64` is exact to
/// the byte anyway, and nothing downstream consumes the printed value.
#[allow(clippy::cast_precision_loss)]
fn gib(bytes: u64) -> f64 {
    bytes as f64 / (1u64 << 30) as f64
}

/// Nanoseconds as milliseconds, for the per-read decomposition.
#[allow(clippy::cast_precision_loss)]
fn ms(nanos: Option<u64>) -> f64 {
    nanos.unwrap_or(0) as f64 / 1e6
}

/// Print the run's verdict: the rate, the per-read decomposition, and the device cross-check.
fn report(
    args: &Args,
    store: &ChunkStore,
    took: std::time::Duration,
    devices: &[(String, u64)],
    frames: &FrameCounters,
) {
    let stats = store.stats();
    let hits = stats.hits.load(Ordering::Relaxed);
    let bytes = hits * args.chunk_bytes as u64;
    let seconds = took.as_secs_f64();
    println!("shape:                  {:?}", args.shape);
    println!("placement:              {}", args.placement.label);
    println!("depth:                  {}", args.depth);
    // Printed even when unlimited, and as the word: a result whose ceiling is implicit is one
    // nobody can compare against a control that had none.
    println!(
        "read ceiling:           {}",
        if args.read_concurrency == 0 {
            "unlimited".to_owned()
        } else {
            args.read_concurrency.to_string()
        }
    );
    println!("hits:                   {hits}");
    println!(
        "bytes read:             {bytes} ({:.2} GiB) in {seconds:.3} s",
        gib(bytes)
    );
    println!(
        "tier read rate:         {:.3} GiB/s   <- fio does 43.208 at this shape",
        gib(bytes) / seconds
    );
    println!(
        "per-read SERVICE mean:  {:.3} ms   <- fio's is 17.3 at depth 48",
        ms(stats.mean_service_nanos())
    );
    println!(
        "per-read queue mean:    {:.3} ms   (waiting for a blocking thread, NOT cost)",
        ms(stats.mean_queue_nanos())
    );
    println!(
        "per-read total mean:    {:.3} ms",
        ms(stats.mean_read_nanos())
    );
    println!(
        "key mismatches:         {}   <- MUST BE 0",
        stats.key_mismatches.load(Ordering::Relaxed)
    );
    println!(
        "io errors:              {}",
        stats.io_errors.load(Ordering::Relaxed)
    );
    // Both, together: a fallback count of 0 only means something beside a non-zero store count.
    // An arm where every read fell back read through the BUFFERED descriptor and measured the
    // page cache, which is the failure that made every tier number before O_DIRECT a fiction.
    println!(
        "frames / heap fallbacks: {} / {}   <- fallbacks MUST BE 0",
        frames.stored.load(Ordering::Relaxed),
        frames.fallbacks.load(Ordering::Relaxed)
    );
    // The check that caught every earlier tier figure being a page-cache memcpy: device bytes
    // over the bytes the probe asked for should be ~1.0. Well below means something served this
    // out of RAM and the rate above is not a device rate.
    let device: u64 = devices.iter().map(|(_, delta)| delta).sum();
    let ratio = if bytes > 0 {
        gib(device) / gib(bytes)
    } else {
        0.0
    };
    println!("device / probe:         {ratio:.3}   <- ~1.0 or the reads did not reach a drive");
    for (name, delta) in devices {
        if *delta > 0 {
            println!("  {name:<12} {:.2} GiB", gib(*delta));
        }
    }
}

/// Warm if needed, read, report.
///
/// # Errors
///
/// Anything the store, the mapping or the arguments raise; all of them are fatal, because a
/// probe that continued past one would publish a number under the wrong label.
fn main() -> anyhow::Result<()> {
    let args = parse_args()?;
    std::fs::create_dir_all(&args.dir)?;
    // One frame per concurrent reader plus a little slack, so frame exhaustion is not silently
    // the thing being measured — `heap_fallbacks` below is the proof it was not.
    let frames = args.depth + args.depth / 2 + 1;
    let mapping = Arc::new(map_frames(frames, args.chunk_bytes, &args.placement)?);
    // Kept behind an `Arc` on this side of `install_frame_source`, which takes the source itself.
    let counters = Arc::new(FrameCounters::default());
    anyhow::ensure!(
        pacer_cache::frames::install_frame_source(Box::new(MappedFrames {
            inner: mapping,
            counters: Arc::clone(&counters),
        })),
        "a frame source was already installed — this binary installs exactly one"
    );

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        let store = ChunkStore::open(StoreConfig {
            dir: args.dir.clone(),
            chunk_size: args.chunk_bytes,
            capacity_bytes: args.slots as u64 * (4096 + args.chunk_bytes as u64),
            verify_body: false,
            read_shape: args.shape,
            read_concurrency: args.read_concurrency,
        })
        .await?;
        if store.len() < args.slots {
            println!("warming {} slot(s)...", args.slots - store.len());
            warm(&store, &args).await?;
        }
        if args.warm_only {
            println!("warm: {} slot(s) held", store.len());
            return anyhow::Ok(());
        }
        let before = diskstats()?;
        let took = read_loop(&store, &args).await?;
        let after = diskstats()?;
        let devices: Vec<(String, u64)> = after
            .iter()
            .map(|(name, sectors)| {
                let was = before.get(name).copied().unwrap_or(0);
                (
                    name.clone(),
                    sectors.saturating_sub(was) * DISKSTATS_SECTOR_BYTES,
                )
            })
            .collect();
        report(&args, &store, took, &devices, &counters);
        anyhow::Ok(())
    })
}
