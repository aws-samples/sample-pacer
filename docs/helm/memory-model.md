> Design notes for the `resources`, `allocator` and every memory-sizing key in
> [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file
> keeps one short comment per key; the reasoning, the measurements and the failure history
> live here.

# Sizing the daemon's memory

Read this before raising `config.memCapacity`.

This is the one place the whole rule is written down. Every memory note in the other files
in this directory describes ONE term; this describes how they add up, because every
OOMKill this project has had came from a term nobody added. The operational counterpart —
what to do at 02:00 when the daemon is already dying — is
[`docs/runbooks/daemon-oom.md`](../runbooks/daemon-oom.md).

`resources.limits.memory` in values.yaml is **NOT** the container's limit. It is the
budget for everything the cache's own accounting can see — the RAM tier, the S3 SDK's
buffers, in-flight chunk fills, hyper's buffers for a multi-hundred-MiB ranged GET, the
chunk directory — and `pacer.memoryLimit`
([`templates/_helpers.tpl`](../../deploy/helm/pacer/templates/_helpers.tpl)) then ADDS
the terms `config.memCapacity` cannot see, so the rendered limit is larger than what you
write. `helm template … | grep -A1 'limits:'` shows the number the kubelet will actually
apply.

## Rule 1 — what you must size yourself

`resources.limits.memory` has to hold `config.memCapacity` plus working room. The two
move together, and forgetting that cost a 256 GiB run: `memCapacity: 24GiB` against a
24Gi limit let the tier consume the entire budget, the daemon was OOMKilled mid-response
during the cold fill, and the client saw `IncompleteRead(… more expected)` — which reads
as a protocol bug and is a cgroup limit
(`bench/ladder/results/c4-dcp-256gib.md`).
**~2× memCapacity is the rule of thumb this repo's overlays use.**

The chart refuses only the unambiguous half of this: `pacer.validateMemoryBudget`
([`templates/_validate.tpl`](../../deploy/helm/pacer/templates/_validate.tpl)) fails the
render when `resources.limits.memory` is *below* `config.memCapacity`, because then the
tier cannot fit at all. Equality — the exact shape that killed the 256 GiB run — still
renders, because a chart cannot tell a deliberately tiny tier from an under-sized limit,
and it is the ~2× rule above that closes the gap.

## Rule 2 — what the chart adds for you

Four terms, each invisible to `config.memCapacity`, each added only when the feature is
on:

| term | what it is | sized by |
|---|---|---|
| `efa.pinnedPoolReservation` | the two registered RDMA arenas (ADR-0024), 8Gi default | `cluster.rdmaArenaBytes` + `HOLDER_ARENA_RANGES × config.chunkSize` |
| `cluster.cacheSlabBytes` | ADR-0028's registered slab, counted ONCE across all rails | `config.memCapacity` + in-flight headroom |
| `scatter.stagingBytes` | + `scatter.coordinatorConcurrency` × the largest object this node coordinates (ADR-0032) | see [write-scatter.md](write-scatter.md) for how that second factor resolves, and for why it is now HEADROOM rather than the bound |
| `delivery.pinnedReservation` | + `delivery.workingSetReservation` (ADR-0026) | see [delivery.md](delivery.md) |

Raise any of those knobs and the limit follows; you never add them by hand.

⚠ The scatter line USED to read `+ scatter.windowsInFlight × chunkSize`, and reading that
history is still the fastest way to learn how much to trust the other three. It restated
a bound the daemon documented but did not keep: the semaphore permit was acquired AFTER
the window was allocated, so a coordinator could buffer the whole object per in-flight
PUT. On 2026-08-27 the same 160 GiB save that completed at client concurrency 4 OOMKilled
three of five daemons at concurrency 8, against a limit this chart had computed — because
concurrency appeared nowhere in the formula. **The ordering was fixed in `ebe07c07`
(2026-08-31) and `windowsInFlight × chunkSize` is now a real, node-wide bound**; the
concurrency factor above is kept as declared headroom pending a hardware arm, which the
`coordinator*` knobs under `scatter:` explain. The lesson generalises and outlives the
defect: **a term derived from a knob is only as good as the code's promise about that
knob**, and this repo cites the code path for each one so the next reader can check
rather than trust.

## Rule 3 — the term no template can compute, and it is the one that kills a big load

foyer's disk tier does BUFFERED I/O, so on a large-memory node the "NVMe tier" is the
page cache — and page cache is charged to the cgroup that instantiated it, exactly like
anonymous memory. Measured on a p5 serving a 131 GiB checkpoint
(`bench/ladder/results/c4-fanout-depth.md`):
**512 GiB served from the disk tier, 0.52 GiB of actual md127 reads, `read_bytes: 0` on
the process, and 72.1 GiB resident in `Cached`.**

That is the missing term, and it explains the shape of the failure precisely:

* a 70B COLD load OOMKilled the daemon at a 24 GiB tier against a 48Gi limit, while the
  SAME tier's warm pass and body control both completed — a cold pass writes the
  checkpoint through the tier, a warm one only reads it back;
* 96Gi survives, and `96 − 24 = 72 GiB` of headroom is the 72.1 GiB measured above, for
  the same checkpoint;
* an 8B checkpoint (14.96 GiB) never hit it at the same tier and the same limit.

**So the term scales with the bytes a load moves, not with `memCapacity` and not with any
value in values.yaml.** A chart cannot know your checkpoint size, which is why this is a
rule and not a formula: budget roughly `min(checkpoint bytes, what you will let the page
cache hold)` above rule 1, or cap it at the source — `config.diskTier: store` (ADR-0033)
replaces foyer's read path with one `pread` into a registered frame, which is the lever
that removes this rather than budgets for it.

⚠ **`diskTier: store` caps the READ side ONLY, so it is not a mitigation for a WRITE
arm.** The store's reads are `O_DIRECT`; its writes are not, and nothing drops the pages
afterwards — "Reads are O_DIRECT and writes are not, which is the whole point"
([`crates/pacer-cache/src/store.rs`](../../crates/pacer-cache/src/store.rs)). So both
tiers instantiate page cache when they POPULATE, which is every cold read-through and,
under ADR-0032, every window the scatter caches locally or stages for a peer. A save
therefore pays this term at either setting, and `pacer.memoryLimit` deliberately does not
branch on `diskTier` for it — a branch would under-budget exactly the write arm that
already OOMKilled. Watch `pacer_cgroup_memory_file_bytes` on a write arm; do not expect
`store` to zero it.

## Rule 4 — the allocator floor, which is a term and not a leak

An idle daemon reports ~8 GiB "outside the cache" on chart defaults and ~20 GiB on the
ladder's, and none of it is a leak: it is registered RDMA memory, resident from startup
because `ibv_reg_mr` pins what it maps. On top of that, glibc's arena count defaults to
`8 × ncores` — 1536 arenas on a p5.48xlarge — each retaining its own free lists, and both
measured memory arms attributed what growth they saw to `free_retained` with `live` flat.
`allocator.arenaMax` makes that floor a constant instead of a function of the instance
type. It is a bound, not a fix:
`planning/17-phase4-plan.md`'s requester OOM has
measured flat twice, so budget for the floor rather than expecting the knob to buy
headroom back.

### Why `allocator` is a first-class block

It is rendered as `MALLOC_ARENA_MAX` and `MALLOC_TRIM_THRESHOLD_` env vars — in the
DaemonSet's **pod template**, not the ConfigMap, for two reasons: the daemon does not own
those names and so cannot express them in its config file, and a pod-template change is what
rolls the DaemonSet (`checksum/config` covers the ConfigMap only). It used to be an
`extraEnv` example.
`planning/17`'s requester-side OOM has been measured FLAT twice — 837 GiB through the
ladder fetch path (+0.13 % on the requester) and 60 GiB across six real restores (a
one-time +0.96 GiB step, then constant) — and in both arms the growth that did occur was
attributed to `free_retained` with `live` flat, i.e. memory the daemon returned and glibc
kept. `planning/06-roadmap.md`'s own conclusion is that
the honest reading is "an allocator floor plus memory-sizing arithmetic … closed by an env
default and a documented sizing rule rather than a code fix". The env default is
`allocator`; the sizing rule is this file.

**The two knobs are NOT symmetric, which is why one is on and one is not.**

`allocator.arenaMax` (default **8**) caps glibc's per-thread arenas. Empty/0 → glibc's own
default, which is not a constant: with `M_ARENA_MAX` unset the limit becomes `8 × ncores`,
so **1536 arenas on a p5.48xlarge** and 768 on an r8gd.24xlarge. Each arena carries its
own free lists and its own top chunk, and `M_TRIM_THRESHOLD` only ever trims the MAIN
arena's, so retained-but-free bytes scale with that count — the floor nobody sized. 8
keeps 8-way malloc concurrency while making the floor a constant instead of a function of
the instance type. What it CANNOT slow down is the daemon's hot path: a 16 MiB chunk body
is far above any mmap threshold and is served by mmap rather than from an arena, and small
allocations come from the per-thread tcache, which arena count does not touch. The
exposure is mid-size allocations only.

⚠ **Its warrant is reasoning, not measurement.** Nothing has measured this daemon with and
without it, and the OOM it addresses is twice-flat (above), so read it as cheap and safe
rather than as proven necessary. It also shifts what
`pacer_malloc_free_retained_bytes` reports, so a before/after comparison across this
change is not like-for-like.

`allocator.trimThreshold` (default **empty — off**) returns freed memory to the kernel
above this many bytes. Empty → glibc's own dynamic behaviour. It looks like the more
targeted of the two — `planning/17` names the mechanism precisely: glibc lets its dynamic
mmap threshold climb to 32 MiB, after which a freed 16 MiB chunk goes on a free list
instead of back to the kernel, which would also explain why the OOM needed an accumulated
floor and was never reproducible on demand. And setting this does address it, because
glibc DISABLES dynamic threshold adjustment as soon as the trim threshold is set
explicitly, pinning the mmap threshold at its 128 KiB default.

That is exactly why it is not a default: pinning the threshold puts every 16 MiB chunk
body through an mmap/munmap pair and re-faults 4096 pages on each one, on the delivery
and fill hot paths, and **nothing has measured what that costs this daemon**. This repo's
own precedent is not to default a knob whose effect is unattributed
(`config.tuning.storageRuntimeThreads` is left at 0 for the same reason). The arm that
would settle it is a C4/C5 delivery rung with and without this set, reading the rate
columns; until then it ships as a knob with the mechanism written down.

The chart renders it as **decimal bytes**, because glibc parses `MALLOC_TRIM_THRESHOLD_`
with `strtol`: a `128MiB` passed through verbatim would be read as 128 BYTES.

## Rule 5 — what will NOT warn you

`process_resident_memory_bytes` (and therefore `run.sh memwatch`, whose gate is
`RSS − foyer`) measures RSS, which EXCLUDES page cache — so the daemon can look flat to
the byte while its cgroup marches to the limit. The kill itself leaves nothing in the log,
because the kernel kills the container. The two symptoms to recognise are both client-side
and neither mentions memory: `IncompleteRead(… more expected)`, and
`EndpointConnectionError` to the daemon's IP — what a container restart looks like from a
client mid-load.

**What WILL warn you, and is what to alert on:** the daemon publishes its own cgroup's
accounting, which is the accounting the kernel kills on
([`crates/pacer-daemon/src/cgroup.rs`](../../crates/pacer-daemon/src/cgroup.rs)).

| series | what it is |
|---|---|
| `pacer_cgroup_memory_current_bytes` / `pacer_cgroup_memory_max_bytes` | proximity to the kill, page cache included. THE series to alert on. |
| `pacer_cgroup_memory_file_bytes` | the page-cache term rule 3 describes, measured rather than budgeted for. |
| `pacer_cgroup_memory_anon_bytes` | the part of `current` that tracks the process-level series. `current − file` is the same quantity computed from the two series a cgroup v1 hierarchy always has. |
| `pacer_cgroup_memory_oom_total` | reclaim failed under the limit. Counted BEFORE any kill and readable by a daemon still running, so it fires while there is still something to do about it. |
| `pacer_cgroup_memory_oom_kill_total` | tasks the OOM killer terminated in this cgroup. Resets with the container's cgroup, so it evidences a kill that did NOT end the container. |

All are 0 where the container has no cgroup memory controller to read, and
`pacer_cgroup_memory_max_bytes` is `+Inf` on an unlimited cgroup — both cases that any
alert expression has to survive. The chart ships those alerts: see
[monitoring.md](monitoring.md).

## The rendered limit, term by term

`pacer.memoryLimit` sums, in this order:

```
resources.limits.memory                                    always
+ efa.pinnedPoolReservation                                 if efa.enabled
+ pacer.cacheSlabBytes                                      if a slab is derived or set
+ delivery.pinnedReservation                                 if delivery.enabled
+ pacer.deliveryWorkingSetBytes                              if delivery.enabled
+ pacer.scatterStagingBytes + pacer.scatterCoordinatorBytes   if the scatter is on
```

When none of those apply, `resources.limits.memory` is emitted **unchanged, as a
quantity** rather than a byte count — which is what keeps this arithmetic off an install
that asked for none of it.

Since 2026-09-16 the default install is not one of those: `delivery.enabled` ships **true**,
so the two delivery terms apply and the shipped `4Gi` renders as **9Gi** (4Gi + a 4Gi pinned
reservation + a derived 1Gi working set). That is the number to size a cache node against,
and `delivery.enabled: false` is what returns both terms and the unchanged quantity.

The slab is added **once** even though it is registered once per rail: all 32
registrations pin the same pages. It is added on top of `pinnedPoolReservation` rather
than being folded into it because the two are sized by different rules — the arenas by
`rdmaArenaBytes` + holder ranges, the slab by `memCapacity` + in-flight headroom — and an
operator raising `memCapacity` must not have to remember to raise a second knob.

**What is deliberately NOT added: page cache.** It is not a bound (rule 3): the clean
portion is reclaimable and scales with the bytes a save moves, which no template can
know.

### The daemon re-derives this sum and refuses to start if it does not fit

`crates/pacer-daemon/src/memory_budget.rs` computes the same terms from the daemon's own
resolved configuration and compares them with the cgroup limit the kubelet actually
applied, before allocating anything (`config.memoryCheck`, default `enforce`).

It exists for the gap `pacer.validateMemoryBudget` documents as deliberate: that check
passes `limit == memCapacity`, because a chart cannot tell a deliberately tiny tier on a
large limit from an under-sized one — and `memCapacity: 24GiB` against a 24Gi limit is
[incident 1](../runbooks/daemon-oom.md). By startup the terms are resolved and the limit
is a file, so the *whole sum against the rendered limit* is a comparison neither key
alone supports. It caught incident 1 at 53 GiB of terms against the 52 GiB rendered.

Two consequences for anyone editing the helper above:

* **A term added there must be added in that module too**, and vice versa. A unit test
  (`helper_terms_are_all_present_in_the_chart`) reads this chart out of the repo and fails
  the build when a helper the module mirrors is renamed or deleted.
* **The daemon must never be stricter than this file**, or a fleet refuses to start on its
  own defaults. That is why `pacer.submitQueueThreshold`, foyer's flush buffers and the
  chunk-fill pipeline are published as `pacer_memory_budget_bytes{counted="false"}` and
  left out of the verdict: the limit here does not add them either.

`config.memoryHeadroomFraction` (default `0.10`) is the margin left for rule 3 and for the
measured delivery working set. It is an **upper** bound, not a safety factor:
`--set efa.enabled=true --set delivery.enabled=true` renders 21 GiB against 18 GiB of
counted terms, so anything above ~0.167 would refuse a stock render.

Because `pacer.validateArenaReservation` makes `efa.pinnedPoolReservation` ≥ the two
arenas, and the slab, delivery and scatter terms are the same numbers on both sides,
almost everything cancels and the check reduces to **rule 1 as an inequality**:

```
resources.limits.memory − config.memCapacity  ≥  headroom × total
```

Every values file used against this chart has been checked against it — 34 renders,
including the shipped example and every internal test overlay — and most clear it with
3-10× margin.

**Worth knowing where it gets tight**, because it is the one shape
`pacer.validateMemoryBudget` cannot refuse at render time: a 4 GiB RAM tier + 8 GiB arenas
+ an 8 GiB slab is 20 GiB against a 20 GiB limit, i.e. `base == tier`, leaving the tier no
room to actually fill. That configuration renders successfully and then runs out of memory
under load. If you enable the slab (`efa.hugepages`) on a node whose limit is close to
`tier + arenas + slab`, do this arithmetic by hand — the render will not do it for you.
Raise the memory limit, or shrink the tier, until the base leaves real slack.

## CPU, and why there is no CPU limit

The daemon runs **Burstable** by design: a CPU request, no CPU limit. The reasoning, the
numbers behind it and the conditions under which an operator should override it are in
[ADR-0035](../adr/0035-daemon-resources-and-qos-class.md) and summarised in
[scheduling.md](scheduling.md#cpu-qos-and-why-there-is-no-cpu-limit).

## See also

* [`docs/runbooks/daemon-oom.md`](../runbooks/daemon-oom.md) — the eight recorded
  incidents and the decision tree.
* [monitoring.md](monitoring.md) — the PrometheusRule the chart ships for these series.
* [cache-and-disk-tier.md](cache-and-disk-tier.md) — `memCapacity`, `diskCapacity`,
  `diskTier`.
* [efa-and-rdma.md](efa-and-rdma.md) — the arenas and ADR-0028's slab.
* [delivery.md](delivery.md), [write-scatter.md](write-scatter.md) — the other two
  invisible footprints.
* [`docs/adr/0024-registered-arena-rdma-buffers.md`](../adr/0024-registered-arena-rdma-buffers.md),
  [`docs/adr/0028-cache-ram-tier-is-the-registered-arena.md`](../adr/0028-cache-ram-tier-is-the-registered-arena.md),
  [`docs/adr/0032-write-scatter-populates-the-cache.md`](../adr/0032-write-scatter-populates-the-cache.md),
  [`docs/adr/0033-chunk-store-owns-the-disk-tier.md`](../adr/0033-chunk-store-owns-the-disk-tier.md).
