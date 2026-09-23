> Design notes for the `scatter` keys in [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file keeps one short comment per key; the reasoning, the measurements and the failure history live here.

# Write scatter (ADR-0032)

## What it is

Write scatter (ADR-0032): a PUT of a new key becomes a multipart upload whose
parts are uploaded by the chunks' own homes, and every part is cached on the
node that uploaded it instead of being discarded. Two halves worth different
things — populate pays on every save shape, the scatter only when a save writes
few objects relative to the fleet (../../planning/24-write-path.md).

A PUT of a new key is decomposed onto the chunk grid, each chunk's home
uploads its own part and caches it, and CompleteMultipartUpload is the single
durability fence before the client's 200. Standard backends only —
`pacer.validateScatter` refuses the Express render, and the daemon refuses it
at startup.

ON BY DEFAULT WHERE THE DESIGN APPLIES, which means Standard backends only. Every
ADR-0032 gate passed on hardware — the headline restore-after-populated-save (4.1,
2026-08-31) and the few-object save (4.5, 2026-09-01, 4.73x against a 2.00x bar) —
so this is a three-state knob rather than a boolean.

## The three-state `enabled` knob

| Value | Effect |
| --- | --- |
| unset (`null`, the default) | ON when `config.backendType` is `standard`, OFF on `express`. `pacer.scatterEnabled` is the one place that resolves this, and the daemon applies the same rule for deployments that do not use this chart. |
| `true` / `1` / `on` / `yes` | ON. On an Express backend this FAILS the render (`pacer.validateScatter`) and the daemon `bail!`s at startup — an explicit ask for something impossible must not be silently downgraded. |
| `false` / `0` / `no` | OFF on any backend, and emitted into the ConfigMap explicitly so the daemon's own default cannot overrule you. |

`scatter.enabled` is a THREE-state knob, and the `pacer.scatterEnabled` helper is
the one place that resolves it: an explicit value wins, and `null` — the shipped
default — means **on where the design applies**, i.e. on a general-purpose
(Standard) backend and off on an Express directory bucket. Every gate in ADR-0032
§ Phases passed on hardware (gate 4.5 on 2026-09-01 at 4.73×), so the mechanism no
longer has to be asked for; what it still must not do is turn itself on where it
cannot run.

**A bare `enabled: true` default would have broken every Express deployment** —
Express is `config.backendType`'s own default, `pacer.validateScatter` FAILS the
render on that combination and the daemon `bail!`s at startup — so the default
cannot be a boolean flip. It has to be scoped, and scoped in a way that keeps an
explicit ask failing loudly rather than being silently downgraded.

Truthiness deliberately mirrors `crates/pacer-daemon/src/config.rs`: only
`1/true/on/yes` count, so a typo leaves the scatter in whatever state the backend
implies rather than flipping it. Anything else explicit (`false`, `0`, `no`) turns
it OFF on any backend — and the ConfigMap then emits that `false` explicitly,
because the daemon derives the same default from the same rule and would
otherwise turn it back on.

Whether the ConfigMap carries a `scatter:` block at all (`pacer.scatterConfigured`)
is a related but separate question. Two cases need one, and they are not the same
case: the scatter being ON, and an operator having explicitly turned it OFF. The
second is not redundant — the daemon applies the SAME backend-scoped default when
the file says nothing, so an omitted block on a Standard backend would turn the
scatter back on and silently overrule the operator. The block is left empty for
the one remaining combination — Express with the knob unset — so that every
existing Express release renders a byte-identical ConfigMap and its pods do not
roll for a default that does not apply to them.

In the rendered ConfigMap, `enabled` is emitted EXPLICITLY, in both directions,
and the false case is the load-bearing one: the daemon applies the same
backend-scoped default when the file says nothing (`config.rs`), so omitting the
block on a Standard backend would turn the scatter back on and overrule an
operator who asked for it off. The block is skipped entirely only for
Express-with-the-knob-unset, where the default does not apply and every existing
release must keep rendering a byte-identical ConfigMap.

## What default-on changes for a client

⚠ WHAT DEFAULT-ON CHANGES FOR A CLIENT, on a Standard backend: a scattered PUT is a
multipart upload, so the ETag a client sees is the composite `-N` form rather than
a plain MD5 (ADR-0032 § 5). Anything comparing ETags to a locally computed MD5
must either move to the `x-amz-checksum-crc32` the daemon sets — a FULL_OBJECT
CRC32 S3 enforces, so it survives the reassembly — or set `enabled: false` here.
Only objects at or above `minObjectBytes` (128MiB default) are affected; smaller
PUTs keep ADR-0007's path and their MD5 ETag unchanged.

## The staging budget

Node-wide ceiling on bytes this node holds for OTHER nodes between their
UploadPart and the coordinator's commit. Empty → the daemon default (2GiB), which
the chart then emits explicitly, because this number is also added to the
container memory limit (see `pacer.memoryLimit`) and the two must not drift.

This is the reject-fast threshold, and what it bounds is NOT load but
concurrency: a staged window is held until its object's CompleteMultipartUpload,
so peak staged bytes are `(bytes of concurrently-uploading objects) / N`. A save
where every rank writes at once needs the whole checkpoint divided by the fleet —
52 GiB/node for a 1676 GiB checkpoint on 32 nodes — which is deliberately far
above this default. Such a save runs the FALLBACK (the coordinator uploads its
own windows and caches them locally), which is the designed outcome; raising this
to fit is not the fix (../../planning/24-write-path.md § "The staging budget
bounds concurrency").

The effective ADR-0032 staging budget in bytes, when the write scatter is on,
else empty, is derived rather than left to the daemon's own default because this
number is charged to the cgroup twice over: it is heap `Bytes` a node holds on
behalf of OTHER nodes (`crates/pacer-daemon/src/staging.rs` — `Staged { body:
Bytes }`), and like the arenas and the ADR-0028 slab it is invisible to
`config.memCapacity`'s accounting. So `pacer.memoryLimit` has to add it, which
means the chart must KNOW it — and the only way for the budgeted number and the
enforced number to be the same number is to emit this one into the ConfigMap as
well.

The 2GiB default restates `crates/pacer-daemon/src/scatter.rs`
`DEFAULT_STAGING_BYTES`. That duplication is the price of budgeting for it; the
ConfigMap emission is what keeps it from drifting silently — a daemon whose
default changed would still run at the size this helper budgeted, not at a size
nobody accounted for.

In the ConfigMap, `staging-bytes` is emitted even when the operator left it
empty, because `pacer.memoryLimit` had to budget for a specific number and this
is how the daemon is held to the same one.

## Windows in flight, and the bound it really is

Windows a coordinator keeps in flight. Empty/0 → the daemon default (16).

This IS a memory bound — `windowsInFlight × chunkSize`, node-wide — as of
`ebe07c07` (2026-08-31), which made `ScatterCoordinator::dispatch` take the
semaphore permit BEFORE the window's bytes and made the body reader await it. A
full pipeline therefore stops the read of the client's socket, which is the
backpressure the daemon's docstring always claimed. ⚠ It was NOT true before
that commit: the permit was taken inside the spawned upload, after the window
had been allocated and moved into it, so a parked task held a full chunk body
and the semaphore bounded concurrent UPLOADS. That cost a paid arm — see
"The coordinator's own buffered windows" below. Two residuals sit outside the
bound and one of them no template can compute: the splitter's under-one-window
remainder, and the single frame `body.next()` last yielded, whose size is the
SENDER's framing (tens of KiB over a network; an in-process caller may hand over
the whole object in one frame).

It is still not a throughput lever: the 2026-08-27 sweep found 16→32 flat.

**`scatter.windowsInFlight` IS a byte bound, as of commit `ebe07c07`
(2026-08-31).** `ScatterCoordinator::dispatch` (`crates/pacer-daemon/src/coordinate.rs`)
is `async` and takes the permit BEFORE it hands the window's bytes to a task; the
single task that reads the client's body awaits `dispatch`, so a full pipeline
stops `run_pipeline` reaching its next `body.next()` and the client is
backpressured through TCP. The daemon's docstring on that field is accurate for
the first time. It was NOT true when this helper was written: the permit used to
be awaited inside the spawned upload, after the window had been allocated and
moved into it, so the semaphore bounded concurrent UPLOADS and a coordinator
could buffer the whole object per in-flight PUT.

Two facts decide the arithmetic, and both are easy to state slightly wrong:

* **The residual is NOT `(windowsInFlight + 1) × chunkSize`.** Held bytes are
  `windowsInFlight × chunkSize` PLUS the splitter's under-one-window remainder
  PLUS the one frame `body.next()` last yielded — and that last term is the
  SENDER's framing, not ours. Over a network it is tens of KiB; an in-process
  caller can hand over the whole object in one frame, so no ordering can bound
  it and NO HELM TEMPLATE CAN COMPUTE IT. The budget does not cover that term
  and does not pretend to.
* **The semaphore is NODE-WIDE, not per-PUT.** One `ScatterCoordinator` is
  constructed per daemon (`main.rs` `attach_scatter`, one call site) holding one
  `Arc<Semaphore>`, and `scatter(&self, ...)` takes `&self` — so every concurrent
  PUT shares it. Concurrency therefore does NOT multiply `windowsInFlight ×
  chunkSize`; it multiplies only the per-PUT residual above, since each
  in-flight PUT has its own WindowSplitter.

In the ConfigMap, `windows-in-flight` bounds the coordinator's own footprint to
this × chunkSize and backpressures the client's socket. Unset/0 → the daemon
default (16), which is the figure `pacer.memoryLimit` assumes when this is
empty.

## The coordinator's own buffered windows

`pacer.memoryLimit` adds `coordinatorConcurrency × the largest object this node
coordinates`, falling back to `windowsInFlight × chunkSize` when the object size
is not declared. Since `ebe07c07` that FALLBACK is the true bound, so these
knobs no longer make the limit correct — they make it generous, on purpose, and
only until a hardware arm says how generous it needs to be.

They exist because the alternative cost a paid arm rather than being predicted
(`../../bench/ladder/results/w1-write-scatter.md`, 2026-08-27, five
`r8gd.24xlarge`, `stagingBytes=8Gi`, `windowsInFlight=64`):

* `bal-c4` — five writers at client concurrency 4, 160 GiB — completed, at the
  best aggregate that session measured (3.136 GiB/s);
* `bal-c8` — concurrency 8, **the same 160 GiB, the same fleet, the same
  objects, the same limit** — had **three of five daemons OOMKilled** against
  the 53 GiB this chart computed for them.

Same bytes, different concurrency, so CONCURRENCY was the missing term: the
derivation was `windowsInFlight × chunkSize` with no concurrency in it at all,
1 GiB budgeted against up to 8 × 4 GiB = 32 GiB held.

⚠ WHY THE ARITHMETIC IS STILL HERE. The semaphore is NODE-WIDE — one
coordinator per daemon, one `Arc<Semaphore>`, `scatter(&self, …)` — so
concurrency multiplies only the per-PUT residual above, not the dominant term.
Dropping `coordinatorObjectBytes` would take the ladder's w1 stack at the
2026-08-27 sweep's own `zero-c16` shape (staging 8GiB, windowsInFlight 64,
concurrency 16 over 4 GiB objects) from 84.000 GiB to 21.000 GiB — the scatter
terms from 72 GiB to 9 GiB, an 8× cut on a derivation that has already been
WRONG TWICE. And `ebe07c07` removed the coordinator term, NOT the page-cache
one, which is rule 3 above `resources:` in `values.yaml`: real, charged to this
cgroup, unbudgeted, present in BOTH `bal-c4` and `bal-c8`, and unmeasured (the
arm predates `cgroup.rs`). The number shrinks in a later commit that carries a
re-run of `bal-c8` measuring peak RSS and `pacer_cgroup_memory_file_bytes`. Do
not shrink it before those two series exist.

Measured cost of the old ordering, kept because it is why this term exists at
all (`../../bench/ladder/results/w1-write-scatter.md`, 2026-08-27, five
`r8gd.24xlarge`): `bal-c4` — five writers at client concurrency 4 over 160 GiB —
ran at the best aggregate that session recorded, and `bal-c8` at concurrency
**8 over the same 160 GiB, same fleet, same objects, same limit** had **three of
five daemons `OOMKilled`** against the 53 GiB this chart computed. The
discriminating variable was CONCURRENCY, not bytes moved: 64 × 16 MiB = 1 GiB
budgeted against up to 8 × 4 GiB = **32 GiB** actually held.

Resolution order — UNCHANGED by `ebe07c07`, deliberately:

1. `scatter.coordinatorReservation` set → that value, including `0` for "add
   nothing" (the pre-2026-08-27 arithmetic, for a control arm).
2. otherwise → `coordinatorConcurrency × perPut`, where `perPut` is
   `scatter.coordinatorObjectBytes` when the operator has declared the largest
   object this node will coordinate, and `windowsInFlight × chunkSize` when
   they have not.

⚠ **WHY THIS STILL OVER-BUDGETS, AND WHY THAT IS NOT AN OVERSIGHT.** Since
`ebe07c07` the fallback in (2) is the real node-wide bound, so
`coordinatorObjectBytes` is no longer what makes the limit safe: it is
**voluntary headroom, retained pending a hardware arm.** Deleting it now would
take the ladder's w1 stack at the 2026-08-27 sweep's own `zero-c16` shape
(the W1 layers — `-f bench/ladder/values/release-shared-sa.yaml -f .../placement-ohio-cache.yaml
-f .../hw-efa-1rail-memlock.yaml`, which is what `values-w1.yaml` became — stagingBytes 8GiB, windowsInFlight 64,
concurrency 16 over 4 GiB objects) from **84.000 GiB to 21.000 GiB** — the
scatter terms themselves from 72 GiB to 9 GiB, exactly **8×** — on a derivation
that has already been WRONG TWICE. And the term `ebe07c07` did NOT remove is
page cache: the scatter caches every window it uploads and stages, both disk
tiers write buffered, so it is real, charged to this cgroup, unbudgeted, present
in **both** `bal-c4` and `bal-c8` — which is why it cannot be that pair's
discriminator and equally cannot be ruled out as a contributor — and
**unmeasured**, since the arm predates `cgroup.rs`.

So the number shrinks in a LATER commit, and that commit carries the hardware
arm that earns it: peak RSS and `pacer_cgroup_memory_file_bytes` on a re-run of
`bal-c8`. Until those two series exist this helper is deliberately generous, and
every rendered limit is byte-identical to what the measured fleets ran with.

What breaks if these are wrong: too small and the daemon OOMKills (exit 137) on
the WRITE path, which surfaces client-side as a truncated body or a vanished
endpoint and never as a memory problem. Too large and the DaemonSet is
unschedulable, which says so immediately.

The three individual knobs:

* **`coordinatorConcurrency`** — Concurrent scattered PUTs this node
  coordinates. 1 is the shape a training save has (one PUT per rank at a time)
  and the ladder's own default (`LADDER_WRITE_CONCURRENCY`,
  `../../bench/ladder/scatter.sh`). Any value renders: the chart used to REFUSE
  past 1 without one of the two knobs below, and that guard went with
  `ebe07c07` because the footprint it was protecting against no longer exists.
  `default 1` rather than the kindIs/trim dance the string knobs use, and
  deliberately: nil (an overlay nulling the key), `""` and `0` must ALL mean one
  PUT, because a concurrency of zero is not a control arm — it is a term of
  zero multiplying the whole footprint away, which is the arithmetic this helper
  exists to stop. `coordinatorReservation: 0` is the control arm.
* **`coordinatorObjectBytes`** — The largest object this node will coordinate.
  Empty → the chart falls back to `windowsInFlight × chunkSize`, which since
  `ebe07c07` is what the semaphore actually bounds — so leaving this empty is now
  DEFENSIBLE at any concurrency, not just the FLOOR it was described as. Set it
  to your shard size (a customer's 1800 GB checkpoint over 256 files is ~6.55
  GiB per shard) and the chart budgets `coordinatorConcurrency × this`, which
  buys headroom for the residuals the bound excludes and for page cache.
* **`coordinatorReservation`** — Escape hatch: state the coordinator's total
  footprint yourself instead of having it derived. `0` means "add nothing",
  i.e. the pre-2026-08-27 arithmetic, which exists so a control arm can ask for
  the under-budgeted limit deliberately. Anything else wins over both knobs
  above. "Set" must mean SET, including set to zero — the same trap
  `pacer.cacheSlabBytes` and `pacer.submitQueueThreshold` each carry a comment
  about: Helm coerces `--set scatter.coordinatorReservation=0` to the integer 0,
  which Go templates call falsey, so a bare `if` would re-derive the term and
  take the documented control arm away.

## Smallest object worth scattering

Smallest object to scatter. Empty → the daemon default (128MiB). Below it a PUT
keeps its original shape, which confines the ETag change to objects large enough
for a parallel upload to pay for it. Distinct from `config.minObjectSize`, which
decides what is CACHEABLE: an object can be worth caching and still too small to
be worth scattering.

In the ConfigMap: unset → the daemon default (128MiB). Confines the
composite-ETag change to objects big enough for a parallel upload to pay for it;
NOT `config.minObjectSize`, which decides what is cacheable.

## Saturated-owner cooldown

Seconds a coordinator stops offering windows to an owner that refused for load.
Empty/0 → the daemon default (5). Needed because an owner can only refuse AFTER
gRPC has already delivered the window, so re-offering to a saturated peer costs
one wasted transfer per window instead of one per peer.

In the ConfigMap: an owner can only refuse AFTER gRPC delivered the window, so
this turns one wasted transfer per window into one per peer. Unset/0 → the
default (5).

## What the chart validates

`pacer.validateScatter` refuses the write scatter on an Express backend at
RENDER time.

The daemon already refuses it at startup (ADR-0032 § 6 scopes the design to
general-purpose buckets, and `config.rs` bails rather than downgrading), so
without this the failure mode is a CrashLoopBackOff and a `helm --wait` that
dies on "context deadline exceeded" — five minutes to learn what a render can
say in one line. Same reasoning as `validateEfaAccess`: a configuration that
cannot work should fail where the operator is still looking at it.

The failure text: "scatter.enabled is set but config.backendType is
\"express\": the write scatter is scoped to general-purpose (Standard) buckets
— ADR-0032 § 6 supersedes ADR-0007 only there, and a directory bucket's write
path stays the plain proxy. Set config.backendType=standard (and point
config.bucketMap.cache at a regional bucket), or leave the scatter off. The
daemon refuses this at startup too, so rendering it would only buy you a
CrashLoop."

There used to be a SECOND check here, added after the 2026-08-27 OOMKill: it
failed the render when `coordinatorConcurrency > 1` with neither
`coordinatorObjectBytes` nor `coordinatorReservation` set, on the grounds that
the fallback in `pacer.scatterCoordinatorBytes` was only the semaphore's nominal
bound. **Deleted with `ebe07c07` (2026-08-31), which made that fallback real** —
`dispatch` takes the permit before the window's bytes and the body reader awaits
it, so `windowsInFlight × chunkSize` bounds a node's buffered windows whatever
its concurrency. The check was refusing a footprint that no longer exists, and
it made the 2026-08-27 arm's own shape unrenderable for the wrong reason. The
three `coordinator*` knobs stay and their arithmetic is unchanged: read the
headroom argument above `pacer.scatterCoordinatorBytes` before touching any of
them.

### The two write-path memory footprints, together

ADR-0032's two write-path footprints are both heap and both invisible to
`memCapacity`:

* **staging** — bytes this node holds for OTHER nodes between their UploadPart
  and the coordinator's commit (`pacer.scatterStagingBytes`). A HARD bound —
  the `StagingArea` refuses past the budget rather than queueing
  (`crates/pacer-daemon/src/staging.rs`), and an owner's refusal is the
  designed reject-fast, so this term is honest as written;
* **coordinator** — the buffered windows of this node's OWN in-flight PUTs
  (`pacer.scatterCoordinatorBytes`). A hard bound as of `ebe07c07` — `dispatch`
  takes the `windowsInFlight` permit BEFORE the window's bytes and the body
  reader awaits it, so `windowsInFlight × chunkSize` really is the node-wide
  ceiling and the client stalls at it. Two residuals it does NOT cover: the
  splitter's sub-window remainder, and the last frame `body.next()` yielded,
  whose size is the SENDER's framing. This term is now deliberately larger than
  the bound; that headroom is argued in full above, and it shrinks only with a
  hardware arm behind it.

Not pinned pages this time — plain allocations — but the cgroup does not care
which, and the failure mode is the one trap 23 in the harness notes already cost
a paid arm: the daemon OOMKills (exit 137) on the WRITE path while the read
path's own arms fit comfortably, and it surfaces client-side as a truncated body
or a vanished endpoint rather than as a memory problem.

**What is deliberately NOT added here, and why it is not an oversight: page
cache.** The scatter caches every window it uploads and every window it stages,
and BOTH disk tiers write buffered — `config.diskTier: store` makes only READS
O_DIRECT (`crates/pacer-cache/src/store.rs`: "Reads are O_DIRECT and writes are
not"), and neither tier drops the pages afterwards. So a write arm instantiates
page cache under either setting, it is charged to this cgroup, and this
derivation must NOT branch on `diskTier` hoping otherwise — that would
under-budget exactly the arm that died. It is left out because it is not a
bound: the clean portion is reclaimable and scales with the bytes a save moves,
which no template can know. Rule 3 of the sizing block above `resources:` in
`values.yaml` is where the operator sizes it, and `pacer_cgroup_memory_file_bytes`
is where they watch it.

## See also

* [memory-model.md](memory-model.md) — the memory sizing rule, including the
  rule 3 page-cache guidance referenced above.
* [ADR-0032: A write populates the cache, and each chunk's home uploads its own part](../adr/0032-write-scatter-populates-the-cache.md)
* planning/24-write-path.md
