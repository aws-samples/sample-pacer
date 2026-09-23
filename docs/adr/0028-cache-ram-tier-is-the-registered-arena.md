# ADR-0028: The cache's RAM tier IS the registered arena — cached chunks live in a hugepage slab, not on the heap

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-21 · Status: **Accepted; every gate met on hardware as of 2026-08-22, and
still off by default.**

Three things had to hold, and each is now measured rather than argued:

1. **Registration cost** (2026-08-21) — hugepages register at 239–433 GB/s, so a 96 GiB
   slab on all 32 rails costs ~8–14 s of startup, not the ~850 s the 4 KiB rate implied.
   This is what makes the design buildable at all, and why hugepages are a
   *precondition* of it.
2. **A completion that times out must not free the frame** (2026-08-22, found by review
   rather than by an arm) — **fixed**: the WRITE's source buffer transfers to the
   completion pump on every path that returns without proof the NIC is done.
3. **Byte integrity while frames actually recycle** (2026-08-22) — **106× refill ratio,
   512/512 digests verified across the peer plane, 0 heap fallbacks.** The arm that
   preceded it recycled zero frames, so it had established nothing about this.

**That decision is now taken: the slab is ON by default wherever hugepages are
configured** (`efa.hugepages` set ⇒ the chart derives `cluster.cacheSlabBytes`). Gated on
hugepages rather than on `efa.enabled` because registration is per page AND repeated per
rail, so the same slab costs ~19 s of startup on 2 MiB pages and ~300 s on 4 KiB ones —
the second trips the startup probe. See "Turning it on by default" below for the sizing
rule, the consequence for the node's hugepage reservation, and the escape hatch.

Supersedes the *holder* half of [ADR-0024](0024-registered-arena-rdma-buffers.md)
(its requester arena and its hugepage rationale stand) and removes a copy
[ADR-0018](0018-holder-driven-rdma-write-data-plane.md) accepted as inherent.

## Context

Three regions in today's daemon do one job between them, and one of them exists only
because the other two are separate:

| region | registered | purpose | cost |
|---|---|---|---|
| requester arena (hugepage, per rail) | yes | receive peers' WRITEs | pinned once at startup |
| **holder arena** (hugepage, per rail) | yes | **stage a copy of a cached chunk** so a WRITE has a registered source | `holder_copy` **5.32 ms/serve**, 0.56 cores, 3.6–4 % of the serve path (D4, planning/19) |
| the cache's RAM tier | **no** | hold the chunks | — |

RDMA can only source a WRITE from a registered MR, and a cached chunk is a
heap-allocated `Bytes`, so every serve copies the chunk into the holder arena first.
ADR-0024 accepted that ("the holder's bounce memcpy stays") because the alternative
looked like registering the allocator's heap, which is unsound: `ibv_reg_mr` pins the
pages present at registration, and if the allocator later returns pages to the OS and
they are re-faulted, the MR's mapping is stale — the NIC then reads the wrong physical
memory.

**Two facts make a third option available, and both were already true.**

1. **We choose where cached chunk bytes live — not foyer.** foyer stores the `Bytes`
   we hand it at insert; the allocations come from our own paths
   (`fetch_from_backend`'s SDK buffer, `retained_copy`'s `Bytes::copy_from_slice`).
   "Thousands of independent heap allocations" is our doing and is changeable. The
   mechanism is already in the tree: the requester path hands the S3 layer a
   `Bytes::from_owner(WrittenRange)` — a refcounted view of a *registered arena
   range* whose frame returns to the arena on last drop.
2. **Every cache entry is exactly one chunk** (ADR-0015). A slab of fixed
   `chunk_size` frames therefore has **no fragmentation problem at all** — the one
   property that usually sinks a slab design is absent by construction.

## Decision

**The cache's RAM tier is a hugepage-backed, pre-registered slab of `chunk_size`
frames. A cached chunk IS an MR range; nothing is staged to serve it.**

1. **One slab, carved into frames.** Sized to `mem_capacity`, mapped with explicit
   hugepages (ADR-0024's `ArenaPages`), registered at startup, carved into
   `chunk_size` frames over a free-index stack — the arena's own construction,
   repurposed from staging to storage.
2. **A cached chunk is `Bytes::from_owner(frame)`.** Insert copies the fetched bytes
   into a frame once (the copy a fill already pays) and hands foyer that `Bytes`;
   eviction drops it and the frame returns to the slab. The serve path then posts a
   WRITE **directly from the frame** — `holder_copy` goes to zero and the holder
   arena disappears (4 GiB of pinned memory at the defaults).
3. **Slab exhaustion is the admission signal.** Today `mem_capacity` (foyer's budget)
   and the arena sizes are independent budgets for the same DRAM; with a slab they
   must be one. A fill that cannot claim a frame is not admitted — the cache's own
   eviction is what frees frames, so the two accounts cannot drift.
4. **The disk tier keeps its bounce, for now.** foyer deserializes a demoted chunk
   into its own buffer on promotion, so only RAM-tier hits become zero-copy.
   NVMe → registered memory without a copy is Track N (GDS), not this ADR.
5. **`ObjectHeader` entries stay on the heap.** They are metadata, tiny, and never an
   RDMA source; forcing them into chunk-sized frames would waste a frame each.

## The gate: MEASURED 2026-08-21, and it passes

`spike/efa` gained a `reg-timing` role
(`regtiming.rs`, driver
`k8s/run-regtiming.sh`): map ONE slab, then
time `ibv_reg_mr` on each rail in turn. One p5.48xlarge, 8 GiB slab, serial
registration, every page pre-faulted so the timing is pinning and not first touch.

| pages | rails | total | per rail | pinning rate | extrapolated 96 GiB × 32 rails |
|---|---:|---:|---:|---:|---|
| 4 KiB | 1 | 0.700 s | 0.700 s | 12.3 GB/s | ~269 s |
| 4 KiB | 32 | 7.989 s | 0.250 s | 14.4 GB/s | ~96 s |
| **2 MiB** | 1 | 0.028 s | 0.028 s | **306 GB/s** | **~11 s** |
| **2 MiB** | 32 | 1.143 s | 0.036 s | **239 GB/s** | **~14 s** |
| **1 GiB** | 1 | 0.020 s | 0.020 s | **433 GB/s** | **~8 s** |
| **1 GiB** | 32 | 0.638 s | 0.020 s | **430 GB/s** | **~8 s** |

**Hugepages are worth ~20–30×**, which is what turns this ADR from impossible into
routine: a production-sized slab registers on every rail in **~8–14 s** of one-time
startup. Registration is essentially **linear in rails** (0.020 s × 32 = 0.638 s), so
nothing is shared across PDs — but at hugepage rates the linearity stops mattering,
and neither fallback in the original plan (lazy per-rail registration, hot-subset slab)
is needed. Hugepages therefore move from "an optimization ADR-0024 uses for
registration cost" to **a precondition of this ADR**: on 4 KiB pages the same design
costs 96–269 s and would be rejected.

**A correction this produced, worth keeping visible.** The 3.61 GB/s figure that made
this look impossible came from the *daemon's* delivery path on **tmpfs** pages, and was
attributed to page count. A raw anonymous 4 KiB mapping registers at **12–14 GB/s**
here — 3.4–4× faster — so the daemon's per-request cost is not pinning alone: it
includes `shm_open` + `mmap` + populating page tables for a **freshly mapped** segment
every request. The 4 KiB arms show the same effect internally (0.700 s for the first
registration versus a 0.250 s average once the tables are warm). Consequence for
ADR-0026: its declare-once handle should cache the **mapping**, not just the
registration.

## Original open question (kept for the record)

**An MR is protection-domain-scoped, and a p5 has 32 rails.** A frame must be
registered on every PD it may be served from, so the slab is registered 32 times.
At the pinning rate measured on 2026-08-21 — **3.61 GB/s on 4 KiB pages** — a 96 GiB
slab × 32 rails would be ~850 s of startup. That is disqualifying, and it is
precisely why this ADR is Proposed rather than Accepted.

Hugepages are the reason to expect it to be fine rather than fatal: pinning cost is
per *page*, so 2 MiB pages are ~512× fewer operations and 1 GiB pages ~262 144×. If
a hugepage slab registers at even 100 GB/s, 96 GiB × 32 rails is ~30 s of startup —
acceptable for a DaemonSet, and one-time.

**Measure before building:** map a hugepage slab and time `ibv_reg_mr` per rail at
2 MiB and 1 GiB pages, on 1 rail and on all 32. `spike/efa`'s `regbuf` already does
the mapping half. Outcomes:
* fast → adopt as above;
* slow but linear in rails → register lazily per rail on that rail's first serve, or
  cap the slab at a "hot, RDMA-servable" subset of `mem_capacity`;
* slow regardless → this ADR is rejected and ADR-0024's staging copy stands, with the
  reason recorded.

## Implementation, and where it deviates (2026-08-21)

Built and merged behind `PACER_CACHE_SLAB_BYTES` (`cluster.cacheSlabBytes`),
default **off**. `crates/pacer-transport/src/efa/slab.rs` maps one slab, registers
it on every rail's PD, and hands out frames as `Bytes::from_owner(CacheFrame)`;
`crates/pacer-daemon/src/proxy.rs`'s `cached_bytes` is the single place a cached
chunk's bytes are allocated, so both fill paths (owner fill and layer-1 admit) get
frames; `serve_via_write` posts from the frame when the body resolves inside the
mapping and stages a copy otherwise.

**A serve finds the frame by pointer identity.** The serve path holds a `Bytes`
and cannot downcast its owner, but the data pointer is enough: if it lies inside
the mapping, the offset yields that rail's SGE. No side table, and a body from
anywhere else (disk-tier promotion, an `ObjectHeader`) simply stages as before.

Two deliberate deviations, both narrowing:

1. **Frame exhaustion does NOT gate admission yet.** Point 3 says a fill that
   cannot claim a frame is not admitted. That couples cache admission to a slab
   whose serve path had never run on hardware, so a slab miss currently falls back
   to a heap allocation and increments `pacer_cache_slab_heap_fallbacks_total`.
   The counter is the point: it is the only way to see that the design has quietly
   stopped applying, since throughput would just look like the pre-slab numbers.

   **Amended 2026-08-25 — the in-place path HAS now run on hardware, and it is worth
   3.86×** (`bench/ladder/results/c5-slab-zerocopy-source.md`). Not the peer serve path
   this ADR was written for: the *client-delivery* path (ADR-0030), which leases from the
   same holder arena and therefore paid the same staging copy. Delivering 70B shards into
   eight H100s' HBM, slab on against slab off with hugepages mapped in both arms:
   **13.7 → 52.7 GiB/s**, `stores` one per chunk, `heap_fallbacks` 0, `staging_in_use` 0
   for the whole arm, and byte integrity re-proven (20 tensors against independent ranged
   GETs, 0 mismatches). The run-to-run spread also fell from ±18 % to ±3 %, because the
   variance lived in the copy.

   Two things that changes for this ADR. The win is **not** the CPU the copy cost: a
   16 MiB `copy_from_slice` sat inline in the task driving a request's whole window set,
   so removing it lifted in-flight WRITEs from ~29 to 142.7 — the copy was capping
   concurrency, which is why the effect is a multiple rather than a few percent. And
   coupling admission to frames (point 3, deferred above) is now the *next* thing this
   design needs rather than a risk to avoid, because the fallback it protects against is
   no longer hypothetical: a slab too small for the tier silently returns the arm to the
   13.7 number. `efa.hugepages` is what makes a slab exist at all, and it is now set on
   the GPU dev profile for exactly this reason.
2. **The slab is not NUMA-placed.** D5 registers each rail's arenas on that rail's
   NIC-local node for +65 %, but one slab shared by 32 rails has no single node to
   be local to. Unresolved tension, not an oversight — D5's win came from the
   requester arenas, which keep their placement. A per-NUMA-node slab set is the
   obvious answer if the serve-path arm comes back short.

**A sizing trap worth stating plainly:** a frame is held by everything
referencing the chunk — foyer's resident set *plus* every chunk in flight to a
client or a peer — so a slab sized at `mem_capacity` runs out of frames as soon as
the cache fills, and every later fill silently takes the heap. The slab must be
larger than `mem_capacity` by the node's concurrent in-flight chunk count. The
daemon warns at startup when it is not, because the failure is otherwise invisible.

**Validated on hardware 2026-08-21/22 — the copy is gone, the throughput claim is
not established.** Two p5, rung-1 shape, RAM-resident holder, 28 GiB slab vs off:
`pacer_rdma_holder_copy_seconds_total` went from **0.348 CPU-s/GiB to exactly
0.000** across 5,032 GiB served (same-pod control: 0.393 with heap-backed chunks
minutes earlier), with 512 stores, **0 heap fallbacks**, 1792 frames registered on
all 32 rails at `pages="2MiB"`, and 48 GiB pinned as budgeted. So point 2 of this
ADR is real: nothing is staged to serve a frame-backed chunk, and ~10 cores of
memcpy leave the holder at 28 GiB/s.

Two honest limits. **Throughput did not move** (27.899 vs 28.361, inside the 1.1 %
control-vs-control spread) because that rung is bandwidth-bound — the throughput
argument belongs at high fan-in and is untested. And **byte integrity was not
verified**: rung 1 reports fallbacks, not digests, and this is precisely the change
where frame lifetime versus NIC reads could corrupt data, so C2's digest arm is
owed before the slab is defaulted on. Write-up:
`bench/ladder/results/adr28-slab-serve-path.md`.

**The arm also found the deviation that mattered most.** The peer server's
read-through fill did not use the slab, so a *cold* holder cached on the heap and
staged every serve while the slab sat fully registered and
`pacer_cache_slab_stores_total` read 0 — invisible in every other metric. The
policy now lives once in `crates/pacer-daemon/src/cachefill.rs` (`ChunkFill`,
constructed in `main.rs`, shared by all three fill sites), which is the shape this
ADR should have had from the start.

## How far "the RAM tier IS hugepages" actually goes (foyer 0.22.3, checked 2026-08-22)

The intent of this ADR is that cached chunks live in hugepages, full stop. Three
things qualify that today, and only one of them is ours to fix locally.

**The memory tier already is hugepage-native, and needs nothing from foyer.** foyer
stores the `V` we hand it; it never allocates our bytes. So "make foyer use
hugepages" for the RAM tier has no meaning beyond "stop handing it heap `Bytes`",
which is what this ADR does. foyer's own overhead — hash table, S3-FIFO metadata —
is small, heap-resident, and never an RDMA source.

**A promoted chunk was NOT frame-backed. Fixed 2026-08-22, and it cost no extra copy.**
foyer serializes a demoted entry to disk and, on a later hit, reads a whole *region*
into its own buffer and decodes entries out of it — so the value it handed back was
freshly allocated by the decoder. Note what this did NOT mean: redirecting that
decode into a frame adds **no copy at all** — foyer already copies region-buffer →
value, and this only changes where it lands. An earlier version of this section
called it "one extra copy on promotion", which was wrong.

Built as a hand-written `foyer::Code` for `CacheValue`
([`crates/pacer-cache/src/codec.rs`](../../crates/pacer-cache/src/codec.rs)), not a
custom `Deserialize`: the trait that the disk tier actually serializes through is
`Code`, whose `decode` takes a `&mut impl Read` over the region buffer, so
`read_exact` straight into a claimed frame replaces the `Vec` one-for-one. serde was
never a route — `Deserialize` has nowhere to carry a slab — and foyer's `serde`
feature supplies a blanket `Code` impl that would overlap, so `CacheValue` gives up
its derives. Three consequences worth knowing:

* **The on-disk format is pinned to bincode's, byte for byte**, because foyer reuses
  the cache directory across restarts: a format change would make every entry a
  previous build wrote decode as garbage that still passes foyer's integrity check
  (the bytes are intact, only reinterpreted). A test asserts both directions against
  a mirror type that still derives serde.
* **The slab is reached through a process-wide seam**
  ([`pacer_cache::frames`](../../crates/pacer-cache/src/frames.rs)) installed at
  startup from the same `Option<CacheSlab>` `ChunkFill` gets, since `Code::decode`
  has no `self`. A promotion that finds no free frame falls back to the heap and
  counts itself.
* **An NVMe-served rung is now a valid slab arm**, which changes how to size one:
  see the gate below.

**Zero-copy NVMe → hugepage frame is NOT reachable through foyer's API**, and
should not be attempted locally. Two independent obstacles, both verified in
foyer-storage 0.22.3 `src/io/bytes.rs`:

1. **No buffer hook.** `Raw::new` calls `Global.allocate` with a 4 KiB-aligned
   layout, hardcoded — there is no allocator parameter or buffer pool to inject.
   `Raw::from_raw_parts` *is* public, so the type can wrap foreign memory; what is
   missing is any way to hand foyer a buffer for a specific read. That makes it an
   upstream PR (pluggable IO-buffer allocator), not a local workaround.
2. **Region granularity.** A read pulls a whole region containing many entries, so
   a single chunk's bytes were never that read's target. Landing one chunk directly
   in one frame would additionally need entry-aligned regions.

   The other route is to skip the host entirely — NVMe → HBM by DMA, which is
   planning/20 (track N, GDS) and makes this moot for GPU targets rather than
   solving it for host ones.

**`ObjectHeader` entries stay on the heap by design** (point 5): metadata, tiny,
never an RDMA source, and one would waste a whole chunk-sized frame.

So the reachable-today definition of "exclusively" is: slab on by default (after
the safety item below), plus a frame-landing decoder for promotions — **which now
exists**. Anything stronger is upstream foyer or GDS.

**One thing this opens that is not part of this ADR — MEASURED 2026-08-22, and all
three answers are favourable.** A GPU-delivering node (ADR-0027) copies a cached chunk
out of the slab with `cuMemcpyHtoD`, and a source that is not `cuMemHostRegister`ed is
staged through the driver's own pinned buffer first — F2 put the h2d leg at 32.3 % of a
realistic loader path. Whether CUDA will page-lock a `MAP_HUGETLB` mapping at all was
undocumented (hugetlbfs is exactly what that call has historically refused), and if
`cuMemHostRegister` and `ibv_reg_mr` could not hold the same pages then a
GPU-delivering node could not have a slab, whatever the cost. `spike/efa`'s `host-reg`
role asked all of it on one GPU node:

| pages | accepted | register 28 GiB | `ibv_reg_mr` after it | H2D pinned | H2D pageable |
|---|---|---:|---|---:|---:|
| **2 MiB** | **yes** | **0.385 s** (78.1 GB/s) | **yes** | **50.463 GiB/s** | 17.344 GiB/s |
| 1 GiB | yes | 0.388 s (77.5 GB/s) | yes | 50.460 GiB/s | 15.482 GiB/s |
| 4 KiB | yes | 0.651 s (46.2 GB/s) | yes | 50.459 GiB/s | 15.891 GiB/s |

So: hugepages are accepted, the cost is sub-second and one-time, the two registrations
coexist in the order a daemon would use them, and pinning the source is worth ~2.9–3.3×
on that copy. **Page size barely matters here (~1.7×), unlike for `ibv_reg_mr` (20–30×)
— because an MR is registered once per rail over the same pages and this is registered
once.** Hugepages stay a precondition of this ADR; CUDA is not the reason.

Consequence: a node that both serves RDMA out of the slab and delivers to GPU clients
should `cuMemHostRegister` it at startup. **Not built** — this establishes the option is
available and cheap, not that the daemon takes it. Write-up:
`bench/ladder/results/adr28-cuda-hostreg.md`.

## Safety item: a completion that times out must not free the frame — FIXED 2026-08-22

Found by review, not by the arm (2026-08-22). `await_write_completion` returned an
error on two paths where the work request may still be **outstanding in the NIC**:
the software `COMPLETION_TIMEOUT` elapsing, and the pump dropping the waiter. On
those paths `serve_via_write` returned, the caller's `Bytes` dropped, the frame
returned to the free list, and the next fill could overwrite bytes the NIC still
DMA-read — sending a *different chunk's* bytes to the requester, undetectably
(there is no per-chunk digest on the RDMA path).

This predates the slab: the holder's staging range has the identical window, and so
does the requester's destination range. What the slab changes is the blast radius —
the recycled memory is now the cache's own storage instead of scratch.

**The fix is NOT a timer and NOT a QP reset.** The device produces a completion for
every posted WQE while the QP lives, so the timeout is only *our* deadline, and a
sporadic timeout is not evidence the rail is broken. Instead, transfer the source
buffer's ownership to the completion pump: on the timeout path the pump keeps the
frame/lease alive in place of the cancelled waiter and drops it when the CQE
actually arrives, or when the QP is destroyed (which is itself proof no further DMA
can occur). Bounded by posted-but-unreaped WRITEs — already capped by serve
admission and `PACER_RDMA_RAIL_WINDOW` — so it needs no reclaim policy of its own.
Rail health stays a separate concern that escalates on *repeated* failures.

**Built** as described: `await_write_completion` takes the WRITE's source buffer and
transfers it to the pump on both paths; the pump drops it when the CQE arrives, or
with its own state when the context — and so the QP — is gone. The staged
`ArenaLease` goes the same way, since the holder arena has the identical window.
`pacer_rdma_write_sources_orphaned_total` and `..._held` are the external evidence,
and read as a rail-latency signal rather than a corruption one: a timeout is only
*our* deadline.

One design point worth keeping, because getting it backwards is the easy bug. The
pump's waiter map and its orphan map share **one** lock, because "is the waiter still
registered?" is an exact test for "has this completion been reaped?" only if the
removal and the insert cannot interleave with the reaper. A completion that lands in
that window means the NIC *is* done, and orphaning there would pin the frame forever
— no further CQE will arrive for that id. The pump-dropped-waiter path needs the
opposite treatment (its waiter is already gone, and it knows no completion was
dispatched), so it keeps its buffer unconditionally. Both directions are unit-tested
against a detached pump, which matters for a safety item that could otherwise only
ever be argued for.

### The remaining gate — MET on hardware 2026-08-22

**A byte-integrity arm with the keyset ABOVE the RAM tier, so frames actually churn
under live serves.** The 2026-08-21/22 arm recycled **zero** frames (512 stores,
`frames_in_use` pinned at 512) and verified no digests, so it exercised
claim-and-hold and nothing else. Those two gaps are one gap: frame recycling is
precisely where a frame could be handed to a new fill mid-DMA.

`bench/ladder/adr28-churn.sh`, two r8gd, 2 GiB RAM
tier / 6 GiB slab (384 frames) / 8 GiB keyset, 2 MiB pages realized:

| gate | result |
|---|---|
| frames recycled | **40,758 stores ÷ 384 frames = 106.14×** (previous arm: 1.00×) |
| heap fallbacks | **0** |
| RDMA fraction / miss ratio | **1.000** / 0.0020 |
| byte integrity | **512/512 digests match**, re-read from the requester |
| rung-1 rate | 4.702 GiB/s vs a 4.427 floor |

So the recycle path is safe: 40,758 frames were claimed, freed and re-filled under live
one-sided WRITEs and every object still hashed correctly across the peer plane. The
geometry is what made it a real test — `keyset > memCapacity` to force eviction,
`keyset < diskCapacity` so that churn stays promotions rather than becoming S3
read-throughs. Both sides of it ran through the slab only because of the frame-landing
codec above; before that every promotion took the heap and this arm was not
constructible. Write-up:
`bench/ladder/results/adr28-churn-gate.md`.

**What it does NOT establish.** `write_sources_orphaned_total` read **0**, so the
completion-deadline hand-over never fired — this run exercised the mechanism's absence,
not the mechanism, which remains covered by unit tests only (tripping a 5 s software
deadline on a healthy rail needs fault injection). And it is one rail on two nodes: the
throughput figure is a validity check, not the high-fan-in throughput claim, which is
still untested.

**Both gates are therefore met, and nothing else blocks the default.** Flipping it is a
separate change (`PACER_CACHE_SLAB_BYTES` defaulting to a real size, plus the pod's
hugepage request becoming load-bearing for the cache — see the last consequence below).

The requester's destination range has a window of its own that this does not close: a
requester that abandons a fetch drops its arena lease while the holder's WRITE may
still land. Different side, different lease, ADR-0024's arena rather than the cache —
recorded here because the symmetry makes it easy to assume it came along with the
holder-side fix.

## Amendment 2026-08-31: a frame is a chunk PLUS a slot header

**⬜ REVERTED the same day — a frame is one chunk again and the daemon passes
`slab_frame_headroom: 0`. The knob stays; see § Reverted at the end of this amendment.** The
rest of this section is kept because the three safety properties it establishes are what any
future format that reads something alongside a body would have to re-establish.

Frames were `chunk_size` rounded to a page. They are now
`chunk_size + slab_frame_headroom`, which the daemon sets to
`pacer_cache::slot::SLOT_HEADER_BYTES` — 4 KiB, so 0.02 % more slab at a 16 MiB chunk.

**Why the RAM tier's geometry moved for a disk-tier reason.** ADR-0033's store was issuing two
*dependent* direct reads per chunk — a 4 KiB slot header, then the body — where `fio` issues one
and is ~3.5× faster per read at comparable concurrency. With ~48 × 16 MiB queued at the block
layer the small read waits behind those transfers and the body cannot be submitted until it
returns. A frame that holds header *and* body lets one `O_DIRECT` read fill both
(`c5-dcp-store-odirect.md` § next lever).

**Three properties made it safe, and each was checked against the code rather than assumed:**

1. **`local_slice` recognises registered memory by RANGE CONTAINMENT in the mapping, not by
   frame-base identity.** So the served `Bytes` — a slice starting 4 KiB inside a frame — is
   still posted in place, which is this ADR's entire payoff. (Its doc calls this a
   "pointer-identity lookup"; that wording is loose and the behaviour is containment.)
2. **Frame stride stays page-aligned.** 16 MiB + 4 KiB = 4096 × 4097, so every frame start is a
   page multiple and `O_DIRECT` still accepts it as a destination.
3. **The headroom is passed IN, not derived in the transport.** `SLOT_HEADER_BYTES` is
   `pacer-cache`'s contract and `pacer-transport` must not depend on that crate, so
   `ArenaConfig::slab_frame_headroom` carries it. `0` reproduces the previous geometry exactly.

**The cost on the other tier**: with `diskTier=foyer` the headroom is dead space, 4 KiB per
frame. Not worth branching the transport's geometry on.

**⬜ MEASURED and it is a NULL** (`c5-dcp-store-single-read.md`): +2.7 %, inside the arm's ±20 %
spread, with service time slightly worse. The enlarged frame is kept because it is harmless
(4 KiB per frame) and because the read shape it enables is the right one — but it did not close
the gap, and the reason it did not is now known to be that the gap is elsewhere in our code, not
in the number of reads. One mechanism worth knowing: 16 MiB + 4 KiB is **not** a RAID0 stripe
multiple (512 KiB), so each read spans 32 whole stripes plus a 4 KiB tail on a 33rd drive and
completes only when that straggler does — a plausible reason service got 8 % worse.

**⚠ Test coverage this needed, and nearly did not get.** With no `FrameSource` installed, every
unit test takes the *heap* branch — so the branch production runs was exercised nowhere. It now
has an integration test (its own binary, because `install_frame_source` is a process-wide
`OnceLock`), and that test asserts the source's `stored` counter: without it the assertions on
bytes, hits and CRC all pass with the framed path completely bypassed.
(`crates/pacer-cache/tests/framed_read.rs`, which now also asserts the *negative*: a claim of
`chunk + header` must be refused, so the headers cannot creep back in front of the bodies
unnoticed.)

### ⬜ Reverted 2026-08-31: back to `slab_frame_headroom: 0`

The single read measured as a null (+2.7 %, inside a ±20 % spread, service 8 % worse), so the
enlarged frame bought nothing. ADR-0033 moved the slot headers into a per-extent region for a
cheaper startup scan; a body read is a whole chunk again and needs no headroom.

**And the straggler noted above is a null too, so it is not the reason either.** A `chunk + 4 KiB`
stride does misalign every body against the RAID0 stripe, but a misaligned 16 MiB read still puts
exactly 2 MiB on every member — the head and tail chunks are partial — so an fio sweep across a
whole stripe moved throughput **0.046 %**
(`nvme-stripe-offset.md`). The 8 %-worse
service in the single-read arm was noise, not the 33rd chunk.

**What survives:** `ArenaConfig::slab_frame_headroom` stays, documented, at `0`. It is the seam
that keeps `pacer-transport` from having to know `pacer-cache`'s on-disk layout, and the question
it answers ("a frame holds whatever the cache reads in one I/O") will recur. Properties 1–3 above
are the checklist for the next time it is non-zero.

## Turning it on by default (2026-08-22)

**Where.** In the chart, not the daemon: `PACER_CACHE_SLAB_BYTES` keeps its `0` default so
a hand-run daemon is unchanged, and `pacer.cacheSlabBytes` derives a size whenever
`efa.enabled` and `efa.hugepages` are both set. The chart is the right place because it is
where the two coupled quantities live — the cgroup memory limit and the hugepage request —
and a default that got either wrong would OOMKill the pod or silently degrade the slab.

**Gated on hugepages, not on `efa.enabled`.** ADR-0028 calls hugepages a precondition and
the arithmetic is why: registration is per page *and* repeated once per rail, so cost is
`bytes × rails ÷ rate` at 239–433 GB/s on hugepages versus 12–14 GB/s on 4 KiB pages. A
132 GiB slab on 32 rails is ~19 s of startup on 2 MiB pages and ~300 s on base pages, and
the second trips the startup probe and CrashLoops the pod. `efa.hugepages` being set is
also the operator's signal that the node pre-reserves them.

**The size is `memCapacity + 256 × chunkSize`**, which is this ADR's sizing rule with a
measured constant rather than a safety factor: a frame is held by foyer's resident set plus
every chunk in flight, and serve admission bounds in-flight bodies at `HOLDER_SERVE_SLOTS`
(256). The churn gate ran exactly that shape — a 2 GiB tier and a 6 GiB slab of 384 frames
— and recorded **0 heap fallbacks while recycling frames 106×**.

**The consequence to plan for: the node's hugepage reservation now scales with
`memCapacity`.** The slab maps from the same pool as the arenas, so the boot-time
reservation must cover `rdmaArenaBytes + 256 × chunkSize + slab` — at `memCapacity` 128 GiB
that is ~140 GiB, against the 64 GiB the `gpu-az*` EC2NodeClasses reserve today. The chart
**fails the render** with the exact figure instead of letting the request go unhonored,
because an unhonored hugepage request degrades to 4 KiB pages *with only a warning*, which
for the slab is a silent invalidation.

Two supported ways to run a large cache: raise the node reservation, or **cap the slab
below `memCapacity`** and accept that the excess falls back to the heap. The second is
worth naming because it is the "hot, RDMA-servable subset" fallback this ADR's gate
originally listed and then dismissed — dismissed on registration *time*, which hugepages
solved, whereas what binds here is hugepage *capacity*. Same shape, different reason, and
still correct: fallbacks are counted, and a non-frame body stages exactly as before.

## Consequences

- **The serve path loses a copy** and the holder arena's pinned memory, which also
  removes ADR-0024's `retained_copy` hazard: a peer-fetched chunk being admitted no
  longer has to be copied out of a scarce registered range, because the cache *is*
  registered memory.
- **It enables holder-side checksums.** The delivery path's integrity check costs
  **~26 % of the win at 4 GiB** — re-measured 2026-08-21 after the digest moved to
  the blocking pool (22.634 GiB/s with verification off versus 16.742 with it on;
  it was ~29 % when the digest ran on the async runtime) — because the daemon reads
  back bytes it never touched. With the chunk in a registered frame the **holder** can
  checksum bytes it already holds, and ship the digest on the fetch response.
- **It does NOT fix client-side pinning.** The 47 %-of-the-request cost measured in
  C1 is the *client's* buffer in the client's address space; that needs ADR-0026's
  declare-once handle plus lazy registration. Different side, different fix.
- **One more startup-fatal failure mode.** A slab that cannot be mapped or registered
  is fatal by ADR-0024's precedent (a silently smaller cache is worse), so
  `mem_capacity` becomes a hard reservation rather than a soft budget — and hugepage
  plumbing (pod request, node reservation) becomes load-bearing for the *cache*, not
  just the transport.
- **foyer stays unmodified**, which is what makes this tractable: we change what we
  insert, not how it stores it.
