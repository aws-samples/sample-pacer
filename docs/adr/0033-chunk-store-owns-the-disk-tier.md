# ADR-0033: The chunk store owns the disk tier — a chunk read is one `pread` into a slab frame

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-28 · Status: **Accepted, MEASURED, and still default-off.** One p5, one fleet, one
flag apart: **10.841 → 18.544 GiB/s at 70B (1.711×)**, per-hit cost **96.3 → 26.3 ms (3.66×)**,
restart recovers 131 GiB of tier in **48 ms** and serves it with zero read-throughs. Five of seven
gates pass. **33.4 and 33.5 both still FAIL; O_DIRECT into the frame closed 47 % of the gap, the
single read was a null, and fio at the store's shape proves the remaining ~2.1× is in our code.
Slot headers now live in a per-extent region — for a simpler scan, NOT for throughput: stripe
alignment was measured at 0.046 % and is REFUTED as a lever. ⚠ And 33.5's recorded 19.9 was ~26 %
HIGH — the tier serves ~15.9 GiB/s, so the gap to fio is ~2.7×** — see § Gates.
Write-ups:
`bench/ladder/results/c5-dcp-chunk-store.md`.

~~It stays behind `config.diskTier=store` until 33.5 is settled honestly~~, because that gate's miss
is not yet explained: the tier served 19.9 GiB/s while the arm asked for 15.3, so it was never
driven to saturation and "it cannot reach 25" is unsupported.

**⚠ AMENDED 2026-09-14 by [ADR-0038](0038-chunk-store-is-the-default-disk-tier.md): the store is
now the DEFAULT wherever a slab is derived, and 33.5 is still open and still failing.** The
sentence above gated a default on an ABSOLUTE bar, which was the wrong test for it: a default
chooses between the two tiers this daemon ships, and on that question one within-node arm measured
`store` at 23.750 GiB/s against `foyer`'s 14.5 — with foyer serving **0.005 %** of its bytes from
the drives at all, i.e. its disk tier was the page cache
(`mountpoint-vs-pacer.md`). The default is
conditional on a slab existing because a store read without one is silently BUFFERED, and the
daemon now refuses that combination outright. 33.5 is untouched — see § Gates, which stands.

Narrows [ADR-0015](0015-chunk-granular-caching.md) (chunks remain the cache unit; only where
their *bytes* live changes) and completes [ADR-0028](0028-cache-ram-tier-is-the-registered-arena.md)
(the slab became the RAM tier; the disk tier still round-tripped through a codec on the way in).
foyer keeps the object headers.

## Context

**The device is not the constraint and never was.** Measured 2026-08-28 on a p5.48xlarge's
instance store, through the same `/mnt/k8s-disks/0` the cache dir uses
(`bench/ladder/results/nvme-device-truth.md`):

| | device | foyer's disk tier |
|---|---:|---|
| 16 MiB read | **1.335 ms** | **40–75 ms** (`c4-foyer-readpath.md`) |
| 4 KiB read | 82 µs p50 | — |
| aggregate | **43.565 GiB/s** | ~4.2–8.0 GiB/s per node |
| 4 KiB IOPS | 2 073 504 | — |

So foyer charges **30–56×** what the same 16 MiB costs off flash, and the tier runs **5.5×**
under the hardware. That run also eliminated every candidate outside our own code: the drives
(5.42–5.45 GiB/s each, 8 of them), the array (`md127` stripes even to within 0.01 %; its
both-sockets span costs uniformity, not throughput), the io engine (psync 43.565 vs io_uring
43.298 — a **null at the kernel**, so the +52.8 % `flushers` win was relieving a *foyer* funnel)
and concurrency (**one** thread at 16 MiB QD1 already does 11.696 GiB/s, above the daemon's best
with 32 io threads).

**What foyer charges per hit**, read from the 0.22.3 source and metered on hardware:

1. A fresh **page-aligned 16 MiB `Global.allocate`** per read — no buffer pool.
2. An indexer lookup and a header parse.
3. **One** io op for the whole entry, so a single hit can never use more than one io worker;
   the worker is chosen by `partition.id() % uring.threads`, and one flusher lays 64
   consecutive chunks into one 1 GiB block — the funnel `flushers` exists to relieve.
4. **XxHash64 over every byte** (the engine always passes `Some(header.checksum)`; not
   disableable through the public API), then the `V::decode` copy into the ADR-0028 frame.
   The pair measures **6.5 ms per 16 MiB = 2.4 GiB/s**.
5. All of it on the **main tokio runtime** by default (`Spawner::current()`), against the RDMA
   completion pumps that already burn ~14 cores.

Three further defects are structural rather than tunable: `submit_queue_size_threshold`
defaults to *one chunk*, so demotions are **silently dropped** (`storage_queue_channel_overflow`
non-zero, 1463–4229 observed); promotion inserts a disk hit at the LRU's **hot** end and evicts
~its own weight, and an `Age::Fresh` victim costs a real write; and the RAM tier hits only
**5–8 %** on a one-pass checkpoint sweep, so almost none of that machinery pays for itself.

Item 4 is why **chunk size is not the answer**. A 64 MiB chunk divides the per-*entry* costs
(1, 2, 3) by four but leaves the per-*byte* ones (the hash, the copy) exactly where they are, and
2.4 GiB/s of unavoidable CPU per reading task cannot reach 43 GiB/s. It is also blocked on the
`gpu-az*` hugepage reservation.

## Decision

**A chunk stops being a foyer entry.** A purpose-built chunk store owns the NVMe tier for chunk
*bodies*; foyer keeps the object headers, where its hybrid behaviour is both cheap and correct.

    read  hit  = index lookup -> ONE pread -> the bytes ARE in a registered slab frame
    read  miss = unchanged (peer, then backend)
    fill       = ONE pwrite, write-through, on the blocking pool

Five properties make this a much smaller thing to build than a general cache, and each is a
consequence of ADR-0015 rather than a new constraint:

* **Chunks are fixed-size.** Every chunk but an object's last is exactly `chunk_size`, so the
  disk tier is an array of equal **slots** — no allocator, no compaction, no fragmentation.
* **Chunks are immutable and content-addressed by key.** `"{bucket}/{key}#{size}:{index}"`
  embeds the chunk size, so a resize orphans rather than aliases. A slot is written once and
  read many times; there is no update path to make consistent.
* **Eviction is slot reuse.** LRU over slots, and reuse needs no write barrier because nothing
  else references the slot once the index drops it.
* **The destination already exists.** ADR-0028's slab hands out registered frames. `pread`
  straight into one and the bytes are RDMA-postable with **zero** copies after the DMA — where
  foyer needed the DMA, a hash pass and a decode copy.
* **Write-through costs nothing on this shape.** foyer writes a chunk on demotion; a one-pass
  checkpoint sweep demotes every chunk, so the byte count is identical — and the device does
  43 GiB/s of writes we are not close to using.

### On-disk layout

`diskCapacity / (4096 + chunk_size)` slots, laid out across extent files of `extentSlots`
slots each (one open fd per extent, so the fd budget bounds extents, not slots — the
constraint foyer's one-fd-per-block hit at ~1 TiB).

Every slot has a **4 KiB header** — one page, so both it and every body offset stay
page-aligned and `O_DIRECT` is reachable without a format change:

    magic | format version | body length | crc32c(body) | key length | key bytes

**The headers of an extent are collected into a region at its front, ahead of all the
bodies** (format version 2, amended 2026-08-31 — see § The headers moved out of the bodies):

    extent = [ header region: extentSlots × 4 KiB ][ body region: slots × chunk_size ]

      header(n) at  n × 4096                            page-aligned
      body(n)   at  extentSlots × 4096 + n × chunk_size  page- AND stripe-aligned

A slot still *costs* `4096 + chunk_size`, because the header region is one page per slot —
which is why this is an addressing change and the capacity arithmetic above is unaffected.
Extents are **preallocated** (`fallocate`, falling back to sparse with a warning where the
filesystem cannot): aligned file offsets only buy aligned device reads if the file's extents
are contiguous, and it also turns a mid-run ENOSPC into a startup failure.

### Integrity, and an explicit reduction in checking

`CachedChunk` carries no digest — foyer's unconditional XxHash64 is today the **only** bit-rot
check on the disk tier, so removing it is a decision, not a free deletion. This ADR splits it:

* **The header is always verified** — magic, version, length, and **the key must equal the key
  asked for**. That catches the failure mode *our own code can create*: an index/slot skew, a
  stale slot resurrected after a restart, a misdirected read. It costs one 4 KiB compare.
* **The body CRC is verified only when `verifyChunkBody` is on (default off).** It is written
  always, so turning it on needs no migration. Rationale: a full pass over 16 MiB at
  `crc32fast`'s rate is ~1.1 ms against a 1.335 ms device read — a ~45 % throughput tax to
  re-check what NVMe end-to-end protection already covers, on a path whose bytes the **client**
  verifies end to end (the delivery digest, ADR-0030). An operator who wants media checking
  gets it with a flag and pays for it knowingly.

Stating it plainly: **on the default path this tier does less checking than foyer's did.** The
header check is strictly better at catching our bugs; the body check is strictly worse at
catching the drive's. That is the trade, and it is recorded here rather than discovered later.

### Restart

The index is in memory and rebuilt by scanning slot headers, and collecting them made that
nearly free: the region is contiguous, so **one 16 MiB read recovers a whole extent's 4096
headers** instead of 4096 scattered 4 KiB reads. A 28 TB tier at a 16 MiB chunk is ~437
sequential reads totalling 6.8 GiB — where the original design needed 1.75 M random ones at
82 µs, 256 in flight, to reach a comparable ~0.6 s. So a restart keeps the tier, as it does
today — no snapshot file to fall out of sync with the slots it describes, and the slots stay
the single source of truth. Extent-level concurrency is available if a multi-TB tier ever needs
it; it replaced a `SCAN_CONCURRENCY` fan-out that is no longer there.

### The chunk RAM tier goes away, and that is a deliberate loss

With `diskTier=store` a chunk is **only** in the store. There is no foyer RAM tier in front of
it. Three reasons, in order of weight:

1. **It was measured at a 5–8 % hit rate** on a one-pass checkpoint sweep
   (`c4-foyer-readpath.md`), because a sequential sweep over a working set 2× the tier gets no
   LRU reuse. So the tier being dropped costs ~5–8 % of reads a 1.335 ms `pread`.
2. **ADR-0028's payoff survives intact**, which is the part worth being precise about. Its win
   (13.7 → 52.7 GiB/s) came from a holder posting its WRITE out of *registered* memory instead
   of staging a copy — not from cache residency. A read that `pread`s into a claimed frame and
   serves from it is still posting out of registered memory. The frames become a registered
   buffer pool rather than a resident set, and at ~50 concurrent reads against 256 frames that
   is not a constraint.
3. **Keeping it would reintroduce what this ADR removes.** A chunk in foyer's memory tier is
   piped to foyer's *storage* on eviction — the codec, the submit queue, the silent drops — so
   "RAM tier in foyer, disk tier in the store" would write every chunk twice, once through the
   path being replaced.

If a later arm shows residency matters for a serving (rather than restore) shape, a front cache
over the store is easy precisely because the store is a clean key→bytes interface. It is not in
v1 because nothing measured asks for it.

### What does NOT change

The ring, placement, the peer plane, delivery, the fill *decision* (`should_admit`,
`read_decision`), `ChunkConfig`, and the wire contract. **Object headers stay in foyer**, where
a hybrid LRU is both cheap and right for a few hundred hot bytes. `cachefill` keeps being the
one place that decides where bytes land.

The read and fill sites move behind one facade, `pacer_cache::tier::ChunkTier`, so the choice
is made once at startup and no call site branches: `get_chunk`/`put_chunk` for bodies,
`cache()` for headers. `read_chunk_entry` survives as the facade's foyer arm.

## ⬜ THE INTENDED END-STATE: fill foyer's `Engine` seat instead of replacing the cache

Prototyped 2026-08-31 on branch **`foyer-engine-prototype`** (**unmerged, and it cannot merge
— see the blocker**; this section and `planning/upstream/` are the
half of that branch that *did* land, 2026-09-03). This supersedes the two "Alternatives
rejected" entries below that concern reusing foyer, because both of them looked at the wrong
seam.

Replacing foyer's whole hybrid cache for chunk bodies bought 1.711×, but discarded three
things worth keeping: the RAM tier and its LRU, `get_or_fetch`'s single-flight coalescing,
and the capacity arithmetic (`memCapacity` counted toward chunk capacity, which is why
`diskCapacity` must now cover the whole working set — see Consequences). **foyer has a
sanctioned extension point that keeps all three.** `StoreBuilder::with_engine_config` takes a
`Box<dyn EngineConfig>`, and `Engine` is a public, documented trait — the same seat
`BlockEngineConfig` occupies:

| kept by foyer | supplied by us |
|---|---|
| RAM tier, LRU, weighter, capacity | the on-disk format |
| `get_or_fetch` single-flight coalescing | the index |
| promotion, the eviction pipe, metrics | the read: one `pread` into a slab frame |

Two facts make the fit better than it looks, both read from the 0.22.3 source:

1. **The checksum is the BlockEngine's, not the `Store`'s.** `checksum: u64` lives in
   `foyer-storage/src/engine/block/serde.rs`'s entry header, and the crate-level deserializer
   only verifies when an engine passes `Some(..)`. An engine that owns its format simply does
   not compute one — so the **11.7 ms per entry** of XxHash64 plus decode copy measured on
   hardware is gone by construction, not by a flag.
2. **`Engine::load` takes a hash, returns the key, and documents that the *caller* must check
   the key matches.** Storing the key beside the bytes and comparing it upstream is therefore
   already foyer's own contract — which is exactly what [`slot::SlotHeader`] was built to do.

### ⚠ Blocked on one line upstream

`Engine::enqueue` takes `PieceRef<K, V, P>` from foyer-storage's **private** `mod keeper`,
re-exported by neither `foyer-storage`'s prelude nor `foyer`'s. So the trait is public and
documented for implementors but **cannot be implemented outside the crate**. (`mod engine` is
private too, which independently closes the "bypass reads by reaching the indexer" route.)

The fix is `pub use crate::keeper::PieceRef;` in foyer-storage's prelude — a defensible PR
precisely because the trait is already public and advertised as the extension point. It is
sent, reviewed and approved as [foyer#1330](https://github.com/foyer-rs/foyer/pull/1330);
`planning/upstream/` carries both halves of the patch and what
review changed.

**Why the prototype cannot merge, stated as the general rule it is:** it carries that patch
as a `[patch.crates-io]` over a vendored 0.22.3, and `vendor/` is deliberately untracked, so
**the root manifest names a path no clone has** — which fails *resolution*, not compilation.
`cargo tree` alone exits 101, so a merge would break every cargo invocation in CI, in every
build pod, and in every other session's worktree, not just this crate's build. A workspace
member therefore cannot depend on an unreleased patch, and **`[patch.crates-io]` has no place
in the root manifest** while that is true.

Two ways out, and the branch waits for the first:

* **foyer#1330 releases.** Re-checked 2026-09-03: the PR is still **open** (its CI sits at
  `action_required` behind GitHub's first-time-contributor gate, which only a maintainer can
  release), and 0.22.4 shipped 2026-08-31 *without* it. When a release carries `PieceRef`,
  drop the patch block and the vendored crate, and the prototype builds against crates.io as
  an ordinary member of this workspace.
* **Or it lands as `spike/foyer-engine`** — its own workspace, in the root manifest's
  `exclude` list beside `spike/efa` and `spike/gds`, which exist for exactly this shape: rig
  code CI cannot build. The patch block then sits in the spike's own manifest where it harms
  nothing. Worth doing only if #1330 stalls, because the prototype's whole point is to become
  the shipped disk tier, and a spike that depends on `pacer-cache` by path is a worse home for
  it than a member crate.

### What the prototype proved, and what it did not

**Proved** (3 tests, host): a chunk that exists only on disk reads back through our engine
byte-exact with foyer's RAM tier above it; object headers are dropped rather than given a
16 MiB slot; and `wait`/`close` await an in-flight write count, so a demoted chunk has a
guaranteed point at which it is on disk.

**✅ The restart gap is CLOSED (2026-08-31).** The adapter rebuilds its hash→key map at
startup from the keys the store's own header scan recovered, so a chunk written before a
restart is readable after it *through the cache*, not merely out of the store — which is the
distinction the test now makes, because reading via `store.get` passed even when this was
broken. Deliberately **not** fixed by putting the hash in the slot header, which is what
foyer's `BlockEngine` does: `Engine::load` taking only a hash is the sole reason the map
exists, and both foyer#1287 and the "expose `Eq` via type erasure" idea would pass the key
down instead — at which point the map and any on-disk hash beside it are dead weight.
Hashing at startup costs nothing to delete; a format version bump does not. The coupling
accepted: it reproduces foyer's hash with `DefaultHasher`, so a cache built with a custom
hash builder degrades to a cold tier (never to a wrong serve — the slot's key is still
compared on every read).

**⬜ Headers still have no home**, and the answer is a **composite engine** rather than a
place for them in the slot store — variable-size entries in a fixed-slot layout would mean an
allocator, which is the complexity this design exists to avoid. The engine should hold an
inner `Arc<dyn Engine>` from a `BlockEngineConfig` and route by value type: chunks to slots,
headers to foyer's block engine, which is good at exactly the small entries slots are bad at.
`enqueue` delegates on value type; `load` tries the slots and falls back, so a hit costs one
lookup and a miss two. Buildable today — `EngineBuildContext`'s fields are all public and
cloneable — and not built, because the prototype's question was whether the seat works.

**Not measured.** No hardware arm was run against this, so there is no number for it — and
the reason to sequence it after the client-width sweep stands: gate 33.5 missed at 19.9 GiB/s
while the tier was never driven to saturation, so it is not yet known that the disk tier is
the current wall at all. If the wall is client width, this rework buys back the RAM tier and
the capacity arithmetic — real, but not a throughput fix.

## Alternatives rejected

* **Tune foyer further.** Exhausted, and the ceiling is not a knob: `flushers`/`reclaimers`/
  `submitQueueThreshold` already ship tuned (`5625918d`), `uring.threads` bought +16 %, depth
  bought nothing past 4, `promotion=never` was a null on rate (+2.5/+6.5 %). Items 1 and 4 above
  are not reachable through foyer's public API at all.
* **`HybridCache::storage()` + `Store::load` directly.** Public, but `load` *is* the codec —
  the hash and the decode copy are exactly what it does. It buys only the loss of single-flight
  coalescing. **Superseded by the `Engine` seat above**, which is the same instinct aimed at the
  layer that actually owns the format.
* **A read bypass that finds entries in foyer's own blocks.** Closed: the location lookup lives
  in `engine::block::indexer` (`Indexer::get` → `EntryAddress`), and `mod engine` is **private**,
  so the only downstream route would be re-deriving the index by scanning foyer's blocks and
  parsing a private on-disk header. Version-coupled to internals with no stability promise —
  and unnecessary, because owning reads means owning the format anyway.
* **A 64 MiB chunk.** Bounded by construction (see Context, item 4) and blocked on the node's
  hugepage reservation.
* **GDS / `O_DIRECT` into HBM (Track N).** Orthogonal and still worth doing, but it changes
  *where the DMA lands*, not the 30–56× of CPU above it. The 4 KiB page-aligned header keeps
  `O_DIRECT` reachable from this layout without a format change.
* **Replace foyer everywhere.** Headers are small, hot, and want exactly a hybrid LRU. Nothing
  measured says they are a problem, so they stay.

## Consequences

* **The disk tier's read cost becomes the device's.** Target: a 16 MiB hit at ~1.4 ms and a
  per-node rate bounded by the drives, not by 2.4 GiB/s of per-task CPU.
* **Two silent failure modes go away by construction** — the dropped demotion (no submit queue)
  and the hot-end promotion evicting fresh entries (no promotion; the store is the tier).
* **A new one is possible and must be metered**: the index and the slots can disagree. Hence the
  mandatory header key check, plus counters for every way it can fail.
* **`Promotion` loses its meaning for chunks** and applies only to headers. The knob stays
  parsed and documented; the chunk path no longer reads it.
* **`ioEngine`, `uring.*` and `StorageTuning` keep applying to the header cache only.** They are
  not deleted — a header cache still has a disk tier — but they stop being read-path controls,
  which is the honest end of the psync-vs-uring thread.
* ~~**Off by default.**~~ **Superseded 2026-09-14 by
  [ADR-0038](0038-chunk-store-is-the-default-disk-tier.md): `store` is the default wherever an
  ADR-0028 slab is derived** (the `efa.hugepages` gate), `foyer` where none is, and a daemon asked
  for `store` without a slab REFUSES TO START rather than reading buffered at ~half the rate.
  `config.diskTier` still selects either explicitly, in both directions — the regression is still
  one flag back, which is the half of this consequence that survives.
* **⚠ `diskCapacity` must now cover the WHOLE working set.** Found on the first arm, and it is a
  direct consequence of dropping the chunk RAM tier: foyer's effective capacity was
  `memCapacity + diskCapacity`, because a chunk lived in RAM and demoted — so a 131.417 GiB
  checkpoint fitted 48GiB + 100GiB with room over. The store has only `diskCapacity`, and at the
  chart's 100GiB default it would evict ~31 GiB mid-warm and send those chunks to S3 on the read,
  which reads as the store being slow when it is the tier being too small. Any `store` deployment
  must size `diskCapacity` to the working set alone.

## Gates

Nothing here is believed without these. **Measured 2026-08-28 on one p5**
(`c5-dcp-chunk-store.md`):

| gate | what it asserts | verdict |
|---|---|---|
| 33.1 | Byte integrity: every chunk read back equals what was written, across slot reuse, short last chunks, and a restart that rebuilt the index by scan. | **PASS** — 22 offline tests; 59 tensors byte-compared per arm, 0 mismatches |
| 33.2 | A wrong-key read is **refused, not served**: a slot whose header names another key reports a miss and counts it. Tested by planting a skewed index. | **PASS** — `key_mismatches 0` on hardware |
| 33.3 | Chunk reads land in slab frames — `slab_stores` tracks reads, `heap_fallbacks` **0** — so ADR-0028 still holds. A non-zero fallback count means the frames are being used as a pool faster than they recycle, which is the one way dropping the RAM tier could bite. | **PASS** — `heap_fallbacks 0`, 28 148 stores |
| 33.4 | **A 16 MiB hit's SERVICE time ≤ 3 ms** (vs foyer's 40–75, device 1.335). Restated 2026-08-31 — the original was judged on a queue-inclusive mean. | **FAIL — 40.4 ms** on the O_DIRECT path at its knee (`c5-dcp-store-odirect.md`); 26.9–34.9 ms buffered. ⚠ And every one of those figures is a **lifetime** mean, flattered downward by the verification pass's low-concurrency reads; the daemon now also exposes the summed seconds so an interval mean is derivable at all. |
| 33.5 | **Per-node disk-tier read rate ≥ 25 GiB/s**, `diskTier=foyer` as the control. | **FAIL — and the recorded 19.9 was ~26 % HIGH.** Its numerator was a whole-pod delta including the verification pass's 28.852 GiB, read after the clock stopped; corrected to the timed region the tier does **~15.9 GiB/s** (`c5-dcp-tier-accounting.md`). The device does 43.565. |
| 33.6 | The 70B DCP arm improves end to end. `c5-dcp-hbm-depth.md`'s 11.293 GiB/s is the number that has to move. | **PASS — 10.841 → 18.544, 1.711×** |
| 33.7 | Restart with a warm tier serves from it: read-throughs ~0 after the scan, scan wall clock recorded. | **PASS — 48 ms, 8440 recovered, `misses 0`** |

**33.4 is RESTATED, not retired** (2026-08-31): *a hit's **service** time ≤ 3 ms*.

The original reading was against the wrong quantity. `read_seconds_mean` is timed from before
the `spawn_blocking` hop — deliberately, so a saturated blocking pool cannot hide — which makes
it **queue-inclusive**. At ~74 concurrent reads, 26.3 ms is Little's law, not what a read costs,
and a 3 ms threshold was never that quantity.

The store now measures both, at different points on purpose: `service_nanos` inside the blocking
task (the header read, key compare, `pread`, and the CRC when on) and `read_nanos` around the
whole await. Their difference is the queueing, published as
`pacer_chunk_store_queue_seconds_mean`, and 33.4 is judged on
`pacer_chunk_store_service_seconds_mean` against the device's own **1.335 ms** for the same
16 MiB. **Unmeasured — it needs the next arm.**

The queue-inclusive mean stays published and stays in the write-ups, because foyer's own per-hit
number is the same shape and the two have to remain comparable: **96.3 → 26.3 ms, 3.66×**, of
which 11.7 ms per entry was XxHash64 plus the decode copy this design deletes outright.

**33.5's miss is now UNDERSTOOD, and it is the tier** (2026-08-31,
`c5-dcp-store-width-sweep.md`). The
width sweep closed the escape hatch: depth 8 → 16 → 30 — 30 being the cap, one span per mirror
over 30 shards — moves the tier 13.230 → 16.786 GiB/s for **4.7× the queueing**, with service time
never improving. The tier was driven as hard as this client can drive it and answered 16.786.

**The wall is per-read cost: 26.9–34.9 ms for a 16 MiB read, 20–26× the device's 1.335 ms** and
~4–5× slower than a single-core memcpy of the same size. Eliminated as causes: foyer's codec
(already gone, and worth 2.8×), the CRC (off), the blocking pool (that is the *queue* column,
which is separate and growing), concurrency starvation (more width is worse), the device, and
eviction. Two candidates remain and they imply different fixes — the **buffered read** (fix:
`O_DIRECT` into the slab frame) or the **destination**, i.e. the ADR-0028 slab's claim
serialising or hugepage-registered memory being slower to write than heap. **They separate on a
c8gd at ~$1.5/hr**, no GPU and no fabric: `pread` 16 MiB into a heap buffer versus a
2 MiB-hugepage buffer, at 1 and ~50 concurrent.

⚠ And note what this means for the `Engine` seat above, now unblocked upstream (foyer#1330 is
**approved**): it buys the RAM tier, the capacity arithmetic and single-flight coalescing, and
**none of them make a read cheaper**. It is not the fix for this wall.

### ✅ O_DIRECT is IN and worth +47 %, and it moved the knee (2026-08-31)

`c5-dcp-store-odirect.md`. A chunk read is
now **one DMA from NVMe into the registered frame** — no page cache, no heap buffer, and a
per-thread aligned scratch for the header, so the serve path allocates nothing. Tier read rate
**13.230 → 19.408 GiB/s (+47 %)**, delivery **10.211 → 14.979**, and per-hit queueing **12.0 →
0.8 ms**. First tier measurement here that touches the drives at all rather than the page cache.

**⚠ It worked for a different reason than predicted.** `O_DIRECT` was expected to cut service
time; it cut *queueing* 15× and left service slightly worse (34.6 → 40.4 ms). Under buffered
reads ~50 concurrent 16 MiB memcpys contended for memory bandwidth and threads waited on each
other; with the copy gone a thread blocks on the device instead.

**The knee moved down to depth 8.** Depth 30 buys +2.3 % while service doubles and queueing goes
to 58.6 ms — so client width is now spent, in the other direction from the buffered sweep.

**⬜ THE SINGLE READ IS A NULL, and fio proves the rest of the gap is ours** (2026-08-31,
`c5-dcp-store-single-read.md`). A frame
is now `chunk_size + SLOT_HEADER_BYTES` (ADR-0028 amendment) and one read fills header and body
together — worth **+2.7 %**, inside this arm's ±20 % spread, with service time slightly *worse*.
The hypothesis that two dependent reads cost ~2× is **refuted**.

**What the same session did settle:** fio on the same node at the store's exact shape — 48
concurrent 16 MiB `O_DIRECT` psync reads — gets **43.208 GiB/s at 17.3 ms per read** against the
store's **19.926 at 43.8 ms**; and sharing 4 files instead of 48, the store's own inode shape,
costs fio only **4.1 %**. So **gate 33.5's 25 GiB/s is achievable on this hardware**, the
remaining gap is **in our code**, and per-inode contention is not it.

⚠ **That 19.926 was ~26 % high** and the gap is **~2.7×**, not 2.1×
(`c5-dcp-tier-accounting.md`): the
numerator was a whole-pod counter delta while the denominator was the timed load, and the arm
reads 28.852 GiB back through the daemon *after* the clock stops to byte-compare its sampled
tensors. Corrected, the tier serves ~15.9 GiB/s during delivery. ⚠ Also **retracted from that
session's analysis: there is no ~29.5 % duplicate-read defect** — `read_bytes` exceeded
`bytes_consumed` because of that verification pass (22.6 %) plus span-edge alignment (3.8 %),
and `plan_mirrors` deals each shard to exactly one GPU, so nothing is delivered twice.

**⬜ NEXT STEP IS A PROFILE, NOT ANOTHER HYPOTHESIS.** Three mechanisms have now been predicted
and all three were wrong (the buffered cost was service not queueing; `O_DIRECT` cut queueing not
service; the header round trip was not the wall). A flamegraph of the daemon's blocking threads
during an arm would say where 43.8 ms goes — though note that a thread blocked in `pread`
profiles as `pread`, so the cheaper instrument is to split `service_nanos` into claim / syscall /
publish and read the next arm's counters. **Do not reach for io_uring on this evidence**:
queueing at the knee is 0.6 ms. And the ADR-0028 frame claim, called the leading suspect above,
is dead on arithmetic rather than on measurement: at 19.9 GiB/s the store makes **~1 270 claims
per second** over a `Mutex<Vec<u32>>`, which cannot hold milliseconds.

### ⬜ THE HEADERS MOVED OUT OF THE BODIES — and STRIPE ALIGNMENT IS A NULL (2026-08-31)

**The layout changed; the reason given for changing it was wrong, and was measured wrong within
the hour.** Recorded in that order because the refutation is the more useful half.

The single read forced `slot_stride = chunk_size + 4096`, congruent to 4096 modulo the RAID0
stripe, so ~99 % of body reads began part-way into a 512 KiB chunk and spanned **33** chunks
rather than 32. That was predicted at **~1.25× per-read latency and ~0.8× throughput**, on the
grounds that one member serves five chunk-units where the others serve four.

**⬜ REFUTED, on the mechanism**
(`nvme-stripe-offset.md`). The head and tail
chunks are *partial*, so every member serves exactly 2 MiB of a 16 MiB read at any offset:

    drive(c):  (512K − r) + 512K + 512K + 512K + r  =  4 × 512K  =  2 MiB
    others:                4 × 512K                 =  2 MiB

There is no straggler — the *count* of chunks touched differs, the *bytes* do not. An fio sweep
across a whole stripe (0, 4k, 128k, 512k, 1M, 2M; 16 × 16 MiB `O_DIRECT` psync) moved throughput
**0.046 %** and per-read latency **8 µs of 23 ms**, with the misaligned offsets 0.012 % *faster*.
The file was verified to be one extent starting exactly on a chunk boundary, so the arm really did
compare 32-chunk against 33-chunk reads. Per-device counters were even to the megabyte on every
arm. What survives the arithmetic is request splitting — 129 requests instead of 128, **0.8 %** —
which is below what the arm can resolve.

**So: the chunk store misaligned nearly every read for its entire existence, and it cost ~0 %.**
No layout decision here should be justified by stripe alignment, and the ~2.1× gap to fio is not
in it.

**What the change is worth on its remaining merits**, none of which is throughput:

* **The startup scan becomes one 16 MiB read per extent** instead of 4096 scattered 4 KiB ones,
  and `SCAN_CONCURRENCY` with its per-slot `spawn_blocking` fan-out is deleted (§ Restart). This is
  the only part that is structurally certain rather than measured.
* **Extents are preallocated** (`fallocate`), which removes body fragmentation as an unmeasured
  variable and turns a mid-run ENOSPC into a startup failure.
* **`slab_frame_headroom` returns to `0`** and a frame is exactly a chunk again.
* It costs one extra 4 KiB read per hit (~82 µs unloaded, against 0.6 ms of queueing at the knee)
  and a slot-format bump that discards a warm tier once.

All three configurations measure the same within noise — single read 19.926 GiB/s at 43.8 ms
service, two reads 19.408 at 40.4 ms, +2.7 % rate and −8 % service, both inside the arm's ±20 %
spread — so this is a code-quality choice made with the throughput question settled, not a
performance change.

**And no arm here measured NVMe.** Both tiers read buffered and a p5 holds 2 TiB of RAM against a
131 GiB working set, so every number above is a per-hit *software* cost — which is what this ADR
changes. Against the device's own 1.335 ms for the same 16 MiB, foyer's 96.3 ms was **72×**.
`O_DIRECT` in the store's read path is what would make an arm measure the drives; the 4 KiB slot
header keeps the body page-aligned so that stays reachable without a format change.
