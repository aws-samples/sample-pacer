# ADR-0032: A write populates the cache, and each chunk's home uploads its own part

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-25 · Status: **Accepted; built end to end, PROVEN CORRECT ON HARDWARE
2026-08-25, every gate PASSED, and DEFAULT-ON for Standard backends since 2026-09-01**
(§ 6 amendment — a scoped default, not a flipped boolean). The mechanism is complete, Phase 0 verified the two S3
semantics it rests on (`spike/mpu-scatter`: a non-creating session may upload parts, and a
`FULL_OBJECT` CRC32 is *enforced*, not merely stored), the in-process gates closed the
same day (`crates/pacer-daemon/tests/daemon/scatter/`), and the first hardware run closed gate
3.1 and the populate half — a 6 GiB shard scattered across four owners read back
byte-exact from S3 *and* from a non-writer's cache with **zero read-throughs**, and the
coordinator's own S3 egress was **18.75 %** of the object against an ideal 1/N = 20 %
(`bench/ladder/results/w1-write-scatter.md`).
**Gates 4.2 and 4.4 both PASSED on hardware 2026-08-27 at N=5** — the primary no-regression
gate came out **4.05×** against a bar of ≥ 0.95× (fully scattered, `fan-out=4.00`,
`refused{none}`), and 4.4 came out **4.09×**, 82 % of the ideal 5×, with the coordinator's own
egress at exactly 20.0 % = 1/N. **Gate 4.1 PASSED on hardware 2026-08-31** (the headline
restore after a populated save: 98.5 % of a 256-shard checkpoint's windows populated,
3203/3203 peer serves over RDMA, 256/256 digests on both paths) **and gate 4.5 on
2026-09-01 — 4.73× against a 2.00× bar**, with `fan-out=7.00` on all four writers of an
eight-node fleet and coordinator egress at 11.6 % against an ideal 12.5 %
(`bench/ladder/results/w1-gate41-restore.md`).
**Every gate has now passed, and the default moved the same day — SCOPED, not flipped.**
`PACER_SCATTER_ENABLED` and the chart's `scatter.enabled` are three-state: unset means on for
a general-purpose backend and off on Express, an explicit value wins either way, and an
explicit `true` on Express still fails at both layers. A bare `true` default was never
available — Express is `backendType`'s own default, `pacer.validateScatter` fails that render
and `scatter_config` `bail!`s on it — so scoping is what made a default possible at all.
Two consequences ride along and are stated in the § 6 amendment: the composite `-N` ETag is
now a default for objects at or above `minObjectBytes`, with the `FULL_OBJECT`
`x-amz-checksum-crc32` as the integrity check that survives reassembly; and the
staging-budget refusal path collapsing fan-out from 4.00 to 2.00–3.00 under a hard-pushing
client is **still unmeasured** since `ebe07c07` (gate 4.5 ran at client concurrency 1 and saw
`refused{none}`, which does not answer it).

Of the two defects the 2026-08-27 run found, the second — the daemon `OOMKilled` at the
chart's own computed 53 GiB limit — was **not a budgeting error but an ordering one, and is
FIXED in `ebe07c07` (2026-08-31)**: `windows_in_flight` now bounds buffered bytes as it was always documented
to (§ Context, amendment 2026-08-31). What is still owed there is a *measurement* rather
than a fix — a re-run of `bal-c8` recording peak RSS and `pacer_cgroup_memory_file_bytes`,
since the chart's headroom cannot be reduced and the page-cache term cannot be sized until
those two series exist. Those ratios were reported blocked on "an open defect in
ADR-0007's own passthrough path"; **that is resolved — there was no defect.** The
scatter-off arms failed because a single `PutObject` is capped at 5 GiB by S3 and the
integrity shard is 6 GiB; scatter-off writes succeed at every size up to and including
5 GiB, and a 6 GiB one answers `EntityTooLarge`. A comparison arm simply has to stay
at or under 5 GiB. See `planning/24-write-path.md` § Phases and
`bench/ladder/results/w1-write-scatter.md`. ·
**Supersedes [ADR-0007](0007-write-through-read-after-write.md)**
for general-purpose (Standard) backends; 0007's path stands unchanged on Express
directory buckets. 0007's *durability* principle — never acknowledge a write before
the backend has it — is preserved verbatim and is the one thing here that is not
negotiable.

## Context

ADR-0007 decided that all mutating operations are "proxied directly to S3 Express and
never populate or update the cache," and listed the cost plainly: "first read after a
write is always a miss," and "the daemon adds no write-path caching." That was the
right call for a two-node Phase 1 cache, and its read-after-write argument is still
correct.

Two things changed.

**The read path stopped being the bottleneck.** C5 delivers a real safetensors
checkpoint at 6.206 GiB/s to host and 4.984 into H100 HBM, and one window per GPU
scales to 18.549 GiB/s across eight (planning/19). The write path, meanwhile, is still
`self.inner.put_object(req).await` followed by a purge
([`proxy/`](../../crates/pacer-daemon/src/proxy/) `put_object`) — one node's
egress, and the bytes the daemon just held in its hands are discarded.

**The bytes are already in registerable memory.** ADR-0028 made the cache's RAM tier
the registered arena itself, so caching a chunk the daemon is holding costs no copy,
and (later) a chunk pushed to a peer can land directly in that peer's cache tier.

The premise that binds the design: **a single node cannot saturate an S3 endpoint on
writes — the limiting factor is always the node, never S3.** Accepted as a property of
the platform rather than measured here (`spike/mpu-scatter/README.md` records this). If S3
were the ceiling, recruiting other nodes would buy nothing. Because the node is the
ceiling, recruiting other nodes' egress is the only way a write goes faster than one node.

**The first hardware run supports the premise and narrows it, in a way worth acting on.**
On the same five nodes, one scattering coordinator sustained **0.42 GiB/s** while five
concurrent coordinators sustained **2.05 GiB/s aggregate** — 4.9×. S3 absorbed five times
the load linearly, so it is plainly not the ceiling at this scale. But note what the
comparison does *not* say: both arms already spread their windows over all five nodes'
egress, so the 4.9× cannot be "more nodes". A single write is therefore bounded inside the
**coordinator's own pipeline** — its `windows_in_flight × chunk_size` (16 × 16 MiB by
default) and the single client stream feeding it — not by the fabric, not by S3, and not by
the fleet's egress. That makes those two knobs, not the fan-out, the untested lever for
gate 4.4's single-writer speedup.

**Amendment, 2026-08-27 — those two knobs were tested, and only one of them is a lever**
(`bench/ladder/results/w1-write-scatter.md`). `windows_in_flight` is **not**: 16 → 32 leaves a
single writer at 0.388 → 0.386 GiB/s, and 64 is what `OOMKilled` three daemons. Client
concurrency **is**, but it saturates early — 1 → 4 buys 1.63× (0.388 → 0.632 GiB/s), 4 → 8
buys 2 %, and 16 *regresses*. Two further corrections to the paragraph above: the 0.42 GiB/s
single-writer figure is partly a **harness** number, since `--fill keyed` costs the client
39 % (0.632 → 0.876 GiB/s at identical settings with `--fill zero`); and pushing that hard
makes owners refuse for budget, **collapsing fan-out from 4.00 to 2.00–3.00** — so on this
build client concurrency and the scatter's own engagement trade against each other. The best
rate measured on one node is 0.876 GiB/s against a 4.66 GiB/s NIC (19 %), so the headroom is
real but is **not** reachable by configuration.

**Amendment, 2026-08-31 (`ebe07c07`) — `windows_in_flight × chunk_size` is a memory bound
now, and the "64 `OOMKilled` three daemons" above was an ORDERING defect, not a property of
the knob.** `ScatterCoordinator::dispatch` was a synchronous `fn`: it split a window out of
the body, moved the `Bytes` into a task and spawned it, and the task *then* awaited the
semaphore permit. So a parked task already held a full chunk body, the semaphore bounded
concurrent `UploadPart`s rather than buffered bytes, and the real footprint was `concurrent
PUTs × object bytes` — which is how the paragraph above could name that product as the
coordinator's own ceiling while three of five daemons died inside it. `dispatch` is now
`async` and takes the permit **before** the window's bytes; the body reader awaits it, so a
full pipeline stops the read and the client is backpressured through TCP. Two things this
does **not** claim. The residual is *not* `(windows_in_flight + 1) × chunk_size` — held
bytes are `windows_in_flight × chunk_size` plus the splitter's sub-window remainder plus
the one frame `body.next()` last yielded, whose size is the **sender's** framing and so
unbounded for an in-process caller. And the semaphore is **node-wide** (one coordinator per
daemon), so concurrency multiplies only that per-PUT residual, never the dominant term.
`tests/daemon/scatter/gate_pipeline.rs::a_full_pipeline_stops_the_body_read` pins the ordering. The chart's
`coordinatorConcurrency × object` term is retained as declared headroom pending a hardware
arm, and its render-time refusal past concurrency 1 is deleted; page cache remains real,
unbudgeted and unmeasured on a write arm.

Phase 0 also **refuted the argument this ADR was first drafted on.** Shard sizes were
said to be skewed, so hash-balancing the upload would cut a barrier-synchronized save's
tail. Across eleven reference checkpoints they are near-uniform — HuggingFace packs to
a target size — giving per-rank overshoot of 1.00x–1.15x and 1.01x–1.10x once dealt
across a real fleet. That argument is gone, and what replaces it is narrower and more
honest: the win is not flattening size skew, it is **making the upload assignment
chunk-granular instead of object-granular** — which matters exactly when object-granular
assignment is imbalanced, i.e. when a save writes few objects relative to the fleet.

**And a real customer workload says their save is not that shape.** A shared training
checkpoint: 900 GB of model plus 900 GB of optimizer state, 1800 GB total, **sharded to 256
files** — about 6.55 GiB each, near-uniform, across a job whose 256 ranks are perhaps 32
nodes. 256 objects over 32 nodes is already balanced, so for that save the scatter is worth
≈1× and reject-fast (§ 4) is the whole reason it does no harm. The consolidated
single-writer case the scatter is best at belongs to the *export* path
(`save_pretrained`, single-file safetensors, hub uploads), which nobody has shown us running.

This ADR is therefore accepted primarily for its **populate** half, which pays on every
shape, and secondarily for a scatter whose best case remains unconfirmed. The scatter is
built because it is the general mechanism and populate falls out of it as the degenerate
case — not because the fast path is the common one.

## Decision

### 1. Durability is unchanged

No write-back, no staging tier the client can outrun, no flush barrier. The client's
200 still means the backend has the object. Write-back was considered and rejected: it
would make a failed upload unreportable (nobody left to tell), require merging pending
keys into LIST/HEAD, and put a manifest-before-shards ordering hazard into a
checkpoint's fast path. None of that is worth buying with a weaker contract.

### 2. On a PUT of a new key, the chunk's home uploads that chunk as a part

The node the client hit is the **coordinator**. It:

1. `CreateMultipartUpload` on the real bucket;
2. streams the body into `chunk_size` windows — never holding the object — and hands
   window `i` to `home(chunk_key(object_key, i))` as part number `i + 1`;
3. collects the part ETags and calls `CompleteMultipartUpload`;
4. fans out a commit, writes the object header at `home(object_key)`, and answers 200.

Each owner runs its own `UploadPart` with its own node credentials (ADR-0006 already
strips and re-signs, so this needs no new IAM) and stages the chunk it just uploaded.
`chunk_size` (16 MiB default) exceeds S3's 5 MiB non-final part minimum, contiguous
coverage numbers parts 1..N consecutively, and the final part is a remainder of
arbitrary size — all three verified in `spike/mpu-scatter`.

An **overwrite takes ADR-0007's path unchanged**: proxy, then awaited invalidation. The
correctness precondition for everything below is that the key is *new*, not that
`replication_r` is 1 or that admission is off. A fresh key has no holders to
invalidate at any R. ADR-0015 already requires checkpoint writers to write a new name
per version, which is what makes the fast path the common one.

### 3. One fence, and commit needs neither atomicity nor an await

Nothing becomes cache-visible before `CompleteMultipartUpload` returns. After that
fence the chunk commits and the header write are unordered, mutually independent, and
idempotent.

The commit fan-out is **not** a distributed transaction and is deliberately not
awaited, because it converts a *guaranteed miss* into a *possible hit*, and a miss is
always a legal outcome — a reader whose chunk has not been committed falls through to
an object that now exists in S3 and gets correct bytes. This is the exact inverse of
0007's invalidation, which must be awaited precisely because a holder that misses it
serves stale bytes. Partial commit yields partial *warmth*, never partial correctness,
so the fan-out sits off the client's critical path and the write pays none of the
"tails on the slowest listed holder" cost 0007 records as a regression.

### 4. An owner never blocks on accept — reject-fast

An owner either has staged-byte budget and takes the chunk, or refuses **immediately**;
the coordinator then uploads that part itself and caches it locally, announcing as a
sharer (ADR-0016 layer 1). This single rule does three jobs:

* **Deadlock avoidance.** When every node is simultaneously coordinating its own write
  and homing others' chunks, a blocking accept deadlocks the all-to-all.
* **Load-aware policy.** A node busy saturating its own egress refuses, so the scatter
  self-limits to nodes with spare capacity instead of shuffling bytes between equally
  busy peers.
* **The degradation path.** With no budget anywhere, the whole design collapses to
  "today's PUT, plus populate" — strictly better than today and never worse.

### 5. Integrity is restored explicitly, because splitting removes it

Once the body is split, no single `UploadPart` sees the whole object, so the backend
can no longer validate a whole-body digest the way it does for a plain PUT today. The
coordinator *does* see every byte as it splits them, so it folds a running CRC32 and
hands it to Complete as a `FULL_OBJECT` checksum; each part additionally carries its
own CRC32, covering the coordinator→owner hop.

Both layers are **enforced**, not merely stored — verified in `spike/mpu-scatter`: a
single flipped bit draws `BadDigest` at Complete, a wrong part digest draws it at
`UploadPart`, and S3's reported full-object checksum equals a CRC32 computed
independently over the assembled bytes.

**Amended 2026-08-25 (gate 3.13).** A client-supplied whole-object **CRC32 is honoured**,
not a reason to decline. It is the same digest the coordinator folds anyway, so it is
compared against the computed one *before* Complete and a mismatch fails the write with
`BadDigest` — the answer S3 itself gives. This is not a refinement: every current AWS SDK
and boto3 computes a CRC32 for uploads by default, so declining on one meant the scatter
declined essentially every real client's PUT while reporting only
`declined{reason="client_checksum"}`. The digests that genuinely cannot be reproduced from
an assembly — `Content-MD5`, CRC32C, SHA-1, SHA-256, CRC64NVME — still refuse to scatter
(and are not validated at the coordinator; the earlier claim that `Content-MD5` was has
never been true of the code).

### 6. Standard only

Enabled by backend type (ADR-0023 is the seam). Express directory buckets keep 0007's
path, which removes this design's interaction with Express's `Content-MD5` rejection
and its consecutive-parts rule entirely rather than reasoning about them.

**Amended 2026-09-01 — this is now the DEFAULT, not merely the scope.** With the last gate
closed (4.5, 4.73× against a 2.00× bar; 4.1 the day before), `PACER_SCATTER_ENABLED` and the
chart's `scatter.enabled` became **three-state**: unset means *on where this section says the
design applies* — a general-purpose backend — and off on Express. An explicit value wins
either way, and an explicit `true` on Express still **fails** at render time and at startup
rather than being downgraded, because the derivation can never produce that combination, so
reaching it means someone asked for it.

Three things this amendment is deliberately careful about, each of which would otherwise be
a silent regression:

* **A boolean default was impossible.** Express is `backendType`'s own default, and § 6's
  refusal is a hard error at both layers — a blanket `true` would have refused to render
  and refused to start every Express deployment, which is most of them.
* **The client-visible ETag change (§ 5) is now a default**, not an opt-in. A scattered PUT
  is a multipart upload, so its ETag is the composite `-N` form. Anything comparing an ETag
  to a locally computed MD5 breaks; the replacement is the `x-amz-checksum-crc32` this
  design already sets as a `FULL_OBJECT` checksum, which S3 enforces and which survives
  reassembly. Only objects at or above `minObjectBytes` (128 MiB) are affected — smaller
  PUTs keep 0007's path and their MD5 ETag — and `enabled: false` restores the old
  behaviour on any backend.
* **The memory budget follows the derived value.** `pacer.memoryLimit` adds the staging and
  coordinator footprints, both heap and both invisible to `memCapacity`; a chart that
  defaulted the scatter on while budgeting as though it were off would OOMKill the daemon on
  the write path (exit 137, surfacing client-side as a truncated body). The chart resolves
  the tri-state once, in `pacer.scatterEnabled`, and every consumer — the ConfigMap, the
  staging budget, the coordinator term and the memory limit — reads that one helper.

What is still **not** settled by this: the staging-budget refusal path that collapsed
fan-out from 4.00 to 2.00–3.00 under a hard-pushing client, unmeasured since `ebe07c07`
made `windows_in_flight` a real byte bound. Gate 4.5 ran at client concurrency 1 and saw
`refused{none}`, which does not answer it. The default is on because every *gate* passed;
that measurement remains owed, and the cheapest form of it is the `few` shape at
concurrency 4–8 on an image carrying `2314bfe3`'s staged-bytes ceiling series.

### 7. Chunks carry their object's ETag, unchecked in v1, behind a new variant tag

`CachedChunk` is bare bytes today — [`chunk.rs`](../../crates/pacer-cache/src/chunk.rs)
keeps metadata once in the `ObjectHeader`, not per chunk. Add `e_tag`, populated at
commit, and **not** validated in v1: the only way to get a wrong answer is mixed-version
assembly (chunk 0 from one writer, chunk 1 from another), which immutable keys prevent.

It arrives as a **new `CacheValue` variant tag**, not as an extra field on the existing
one. [`codec.rs`](../../crates/pacer-cache/src/codec.rs) pins the disk tier's encoding
byte-for-byte to bincode's because foyer reuses the cache directory across restarts, so
changing what tag 1 means would make every entry a previous build wrote decode as
garbage that still passes foyer's integrity check — silent corruption on rollout, which
is precisely the failure that codec exists to prevent. A new tag instead is compatible in
both directions: an old entry decodes as a chunk with no ETag, and a new entry read by a
rolled-back build hits the unknown-tag path, which every call site swallows as a cache
miss (`if let Ok(Some(entry)) = self.cache.get(…)`).

Two distinct hazards were conflated in this ADR's first draft, and the distinction is
worth keeping: changing **`chunk_size`** changes the cache *key*, which orphans entries
and is benign ("orphan, never corrupt", `chunk.rs`); changing the **value encoding** is
not, because the keys still match. Only the first is a flush event.

Because a new variant tag is compatible whenever it lands, this field carries **no
migration cost that paying early would avoid** — it ships with the code that populates
it, not ahead of it.

## Consequences

**What the scatter is worth**, given the node is the write ceiling. Today a whole object is
uploaded by whichever node the client ran on, so per-node write load inherits the
*object*-size distribution. The scatter makes the assignment **chunk-granular** instead —
ADR-0015's argument applied to the write path — and chunk hashing is near-perfectly
balanced, so every node converges on `total ÷ N` bytes. Since each node's rate is the same
`R`, wall time is the busiest node's byte count, and:

```
speedup ≈ (busiest node's bytes, whole objects assigned) ÷ (total bytes ÷ N)
```

| Save shape | objects vs nodes | measured baseline imbalance | scatter buys |
|---|---|---|---|
| Consolidated, one writer (export path) | 1 ≪ N | N× | **up to N×** |
| Few objects (4 Llama-3.1-8B shards, 8 nodes) | 4 < 8 | 2.49x | **2.49×** |
| Many balanced shards (163 DeepSeek-V3.1, 8 nodes) | 163 ≫ 8 | 1.05x | **1.05× — nothing** |
| **Real customer training save** (256 files, ~32 nodes) | 256 ≫ 32 | ≈1.0 | **nothing** — reject-fast is what keeps it from being negative |

**The last row is the only one backed by a workload someone runs**, and it is the row where
the scatter is worth nothing. That is the honest position: the mechanism is built, its best
case is real but unobserved, and the design's value on the workload we can point at comes
entirely from populate.

**An unplanned consequence, established 2026-08-26: the scatter is the only way to PUT an
object larger than 5 GiB through the daemon.** ADR-0007's passthrough proxies one
`PutObject`, which S3 caps at 5 GiB — a 6 GiB shard answers `EntityTooLarge` — while the
scatter's multipart upload has no such cap and writes it fine. This was discovered the hard
way: it is why every scatter-off control arm at the 6 GiB integrity size failed, and it was
misread twice as a defect before the size ladder in
`bench/ladder/results/w1-write-scatter.md`
named it. Two implications worth keeping:

* **a comparison arm must use an object ≤ 5 GiB**, or its control measures an operation
  that does not exist;
* the customer's ~6.55 GiB shards (gate 0.5b) are **above** the passthrough's limit, so on
  a Standard backend the scatter is not merely an optimisation for that save shape — it is
  what makes it writable through the daemon at all. That is a stronger claim than anything
  in the table above, and it does not depend on imbalance.

**Also worth recording because it was mistaken for a defect twice:** a client checksum
*header* changes the framing the daemon forwards (the forwarding hop's body is always
streaming, so a user-set checksum makes its SDK skip `aws-chunked` and pass the header
through verbatim, where a client sending none gets `aws-chunked` + a trailer). Both arms
are pinned by `crates/pacer-daemon/tests/daemon/write_framing.rs`. It is a true description of the
daemon and **not** a fault: awscli's `--checksum-crc32 <value>` succeeds through the
passthrough.

**Object count does not limit a scatter's reach**, only the baseline's imbalance. A
4.66 GiB shard is ~298 chunks at the 16 MiB default, so four such shards are ~960 chunks
and fan out to all eight nodes. The only cap on reach is an object with fewer chunks than
nodes — smaller than `N × chunk_size` — which the minimum-size threshold excludes anyway.

**Cache population is universal and independent of all of that.** Every populated write
removes a guaranteed miss, in every row of the table, whether or not any part was
scattered. It is the unconditional half of this ADR and the reason it is worth
accepting even though the one measured workload is the bottom row. For that customer it is
worth 1676 GiB of cold reads on every resume, all of which ADR-0007 currently guarantees to
miss.

**Populate has a capacity precondition.** It helps a resume only if the fleet's aggregate
cache holds one whole checkpoint. The customer's 1676 GiB is 52 GiB per node across 32 nodes
— inside the default 100 GiB disk tier — where the same checkpoint on 8 nodes would need
210 GiB each and would not fit. Operator guidance: `aggregate cache ≥ one checkpoint`.

Other consequences:

- **The ETag shape changes** for a whole-object PUT that gets converted to a multipart
  upload: `"d509…22ba"` becomes `"6697…6746-4"`. This is the one deliberate
  client-visible break. It is bounded by the minimum-size threshold, and large writers
  are already multipart (boto3 switches at 8 MiB), so they already see composite ETags.
- **Read-after-write remains correct without a protocol**, as in 0007 — but now often
  *fast* rather than always a miss.
- **A shrinking overwrite** still orphans chunks beyond the new length, exactly as 0007
  documents; that path is untouched.
- **Staged bytes compete with cache capacity**, and owner-side receive competes with the
  read serve path. Both are bounded by explicit budgets, not by hope.
- **A coordinator's own buffered windows are bounded by `windows_in_flight × chunk_size`,
  node-wide — true as of `ebe07c07` (2026-08-31) and NOT true when this ADR was written.**
  The permit is taken before the window's bytes and the body reader awaits it, so the
  ceiling is real and the client stalls at it. What it excludes: the splitter's sub-window
  remainder, and the one frame `body.next()` last yielded, whose size is the sender's
  framing and therefore not something the daemon — or any Helm template — can bound. See
  § Context's 2026-08-31 amendment for what the old ordering cost.
- **The staging budget bounds concurrent upload, not load — and the measured save exceeds
  it.** A staged window is held until its object's Complete, and Complete waits for every
  window of that object, so peak staged bytes are `(concurrently-uploading bytes) ÷ N`. The
  customer's all-ranks save needs 1676 GiB ÷ 32 ≈ 52 GiB per node against a 2 GiB default, so
  reject-fast fires almost immediately and the write degrades to coordinator-local upload plus
  local populate. **That is the designed outcome and here the better one**: chunks land on
  whoever wrote them, so on a resume rank *R* reads the shard rank *R* wrote — local when the
  pod returns to the same node. Raising the budget is not the fix (52 GiB of staged bytes per
  node is unreasonable); staging into the **disk tier** is the option if a workload ever asks,
  and none does. Detail in `planning/24-write-path.md`.
- **A coordinator that dies between Create and Complete orphans a multipart upload**,
  which no client-side abort can cover — the bucket needs an
  `AbortIncompleteMultipartUpload` lifecycle rule. Staged chunks at owners need a TTL
  for the same reason.
- **Two writers racing the same fresh key with different bytes** can produce a cached
  assembly matching neither. Out of contract in v1 (a client bug for content-addressed
  checkpoint keys) and closable later via § 7's field without a format change.
- **A client-supplied multipart upload** arrives with the client's own part sizes, which
  need not align to the chunk grid. **Settled 2026-08-25 (gate 3.11): it populates
  nothing at all** — `upload_part` is passthrough, so even chunk-aligned parts are not
  cached, and the object warms on first read like any other. A warmth gap, not a
  correctness one; `part_size == chunk_size` remains the recommendation for whenever that
  path is implemented.
- **A failed write must tell every home in the plan, not every owner it heard back
  from.** Windows upload as independent tasks, so when one fails the rest are dropped
  mid-flight and an owner can be holding bytes for a window whose result the coordinator
  never collected. Discarding only against collected parts left those reservations to
  expire on their TTL — a node refusing offers for 15 minutes on behalf of a write that
  had already failed (found by gate 3.6, fixed in `coordinate::unwind`).

Design detail, gates and phasing: `planning/24-write-path.md`. Phase 0 evidence:
`spike/mpu-scatter/README.md`.
