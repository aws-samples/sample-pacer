# Runbook — PACER daemon OOM

**The kill leaves nothing in the log.** The kernel kills the container, so the daemon's own
output ends mid-line. Both symptoms that reach a human are client-side and neither mentions
memory:

* `IncompleteRead(<n> bytes read, <m> more expected)` — the daemon died mid-response;
* `EndpointConnectionError` to the daemon's IP — what a container restart looks like from a
  client mid-load.

And **`process_resident_memory_bytes` will not have warned you.** RSS excludes page cache;
foyer's disk tier does buffered I/O; page cache is charged to the cgroup that instantiated it.
The daemon can be flat to the byte while its cgroup marches to the limit.

Alerts: [`docs/helm/monitoring.md`](../helm/monitoring.md). Sizing model:
[`docs/helm/memory-model.md`](../helm/memory-model.md).

---

## 1. Read the three cgroup series first

The daemon publishes its own cgroup's accounting — the numbers the kernel kills on
([`crates/pacer-daemon/src/cgroup.rs`](../../crates/pacer-daemon/src/cgroup.rs)):

| series | what it is |
|---|---|
| `pacer_cgroup_memory_current_bytes` | everything charged to the cgroup, **page cache included**. The quantity compared against the limit. |
| `pacer_cgroup_memory_max_bytes` | the limit itself. `+Inf` when the cgroup is unlimited — not 0. |
| `pacer_cgroup_memory_file_bytes` | page cache charged to this cgroup. The term RSS excludes and foyer's buffered tier fills. |
| `pacer_cgroup_memory_anon_bytes` | the part of `current` that tracks the process-level series. |
| `pacer_cgroup_memory_oom_total` | reclaim failed under the limit. Counted **before** any kill. |
| `pacer_cgroup_memory_oom_kill_total` | tasks the OOM killer terminated. Resets with the container's cgroup. |

**The one derivation to do by hand:**

```
anonymous (unreclaimable) = current − file
headroom that reclaim can free = file
```

`current / max` says how close the kill is. `(current − file) / max` says whether reclaim
can still save you: if anonymous memory alone is most of the limit, the next buffered write
has nowhere to go and page cache cannot be evicted out of the way fast enough. All series
read 0 where the container has no cgroup memory controller — 0 across the board means "no
sample", not "no usage".

```bash
kubectl -n pacer exec deploy/... -- true   # not needed; scrape the admin port instead
kubectl -n pacer port-forward ds/pacer 9090:9090
curl -s localhost:9090/metrics | grep pacer_cgroup_memory
```

Confirm the kill happened, and that it was a kill:

```bash
kubectl -n pacer get pod <pod> -o jsonpath='{.status.containerStatuses[0].lastState.terminated}'
# reason: OOMKilled, exitCode: 137  → cgroup OOM
# reason: Error,     exitCode: 137  → SIGKILL from something else (see § Scheduling starvation)
```

⚠ **`exit 137` is not always an OOM**, and `IncompleteRead` is not always an OOM either: a
transient S3 backend streaming error produced the same `IncompleteRead(… more expected)`
three times, fixed by `pacer_backend::retry` (`b83192d3`, 2026-08-27) —
`c4-foyer-readpath.md:140-155`. Read
`lastState.terminated.reason` before classifying.

## 2. The eight recorded incidents

Four on the daemon, four on a client. The class column drives § 3.

| # | date | arm / results file | fleet | class | root cause and fix |
|---|---|---|---|---|---|
| 1 | 2026-08-23 | `c4-dcp-256gib.md` | 2 × r8gd.24xlarge | **budget mismatch** (daemon) | `config.memCapacity` raised to 24 GiB against a 24Gi `resources.limits.memory` sized for a 128 MiB tier. Fix: limit → 48Gi. Symptom: `IncompleteRead(318767104 bytes read, 100663296 more expected)`. "`memCapacity` and `resources.limits.memory` must move together." |
| 2 | 2026-08-25 | `c4-dcp-hf-safetensors.md` (70B rung A2) | r8gd + m8gd.24xlarge | **budget mismatch** (daemon) — the *delivery working set*, not the tier | 24 GiB tier against a 48Gi limit; OOMKilled (exit 137) mid-delivery while the same tier's warm pass and body control completed. Fix: **96Gi**. Explicitly *not* the `memCapacity`-vs-limit pairing of #1. Client saw `EndpointConnectionError`. |
| 3 | 2026-08-27 | `w1-write-scatter.md` (`bal-c8`) | 5 × r8gd.24xlarge, S3 Standard | **budget mismatch** (daemon) — staging | `stagingBytes=8GiB`, `windowsInFlight=64`, client concurrency 8 over the same 160 GiB that completed at concurrency 4. **Three of five daemons OOMKilled** against the 53 GiB the chart computed. Root cause: `ScatterCoordinator::dispatch` took the permit AFTER allocating the window, so it bounded uploads and not bytes — 1 GiB budgeted against up to 8 × 4 GiB held. Fixed in `ebe07c07` (2026-08-31). |
| 4 | 2026-08-21 | `memgap-arms.md` | 2 × g7e.12xlarge spot, 64 GiB tier, 104 GiB limit, up 14 h | **page cache / allocator** (daemon) | A restore OOMKilled the requester daemon (exit 137, nothing in the log) after growing **15.27 GiB outside foyer** on a 131.4 GiB restore. **STILL OPEN.** No code fix; instruments added instead — `pacer_malloc_{in_use,free_retained,heap,mmapped}_bytes`, `run.sh memwatch`, and later the `pacer_cgroup_memory_*` series above. Two follow-up arms measured FLAT, and `planning/17-phase4-plan.md:305-333` amends them: they cleared *anonymous* memory only, and the cgroup kills on `memory.current`. |
| 5 | 2026-08-24 | `c5-safetensors-8b.md` (70B integrity) | 1 × p5.48xlarge | **client pinned memory** | The loader pod was OOM-killed: `pacer_nic._verify` copied a whole **4.4 GiB span per concurrent shard** on top of a 64 GiB pinned window. Fix: stream read-back in 64 MiB pieces; client pod memory is `C5_MEMORY` (default `32Gi`). |
| 6 | 2026-08-24 | `c5-multirail.md` (70B striped) | 1 × p5.48xlarge, 4 rails, HBM window | **client pinned memory** (residual, cause unknown) | A 70B integrity arm OOM-killed a 32 GiB client pod **even with both verification paths bounded to 64 MiB pieces**. Fix: client pod raised to **96 GiB + 6 workers**. Explicitly unresolved — "what consumes the difference is not yet known, and nothing here samples the *client's* RSS". |
| 7 | 2026-08-26 | `c4-foyer-readpath.md` (70B DCP warm pass) | 1 × p5.48xlarge | **client host memory** | DCP holds the whole destination state dict (~131 GiB) against `dcp.sh`'s `24Gi` default, so the warm pass is OOMKilled (exit 137) ~8 s in and the only upstream symptom is "the arm did not complete". Fix: **`LADDER_DCP_BENCH_MEMORY` is mandatory on a 70B arm** (docs use `200Gi`). |
| 8 | 2026-08-25 | `vllm-load-gate.md`, restated in `vllm-placement.md` | 1 × p5.48xlarge spot, TP=8, 131 GiB checkpoint | **client host memory** (comparison loader, not PACER) | A third-party loader's **distributed mode OOM-killed a 192 GiB pod**: its own streamer memory cap defaults to unlimited once distributed streaming is on. Deliberately NOT capped, because capping it would handicap the arm it exists to measure — the pod is sized instead, and **`C5_VLLM_MEMORY=768Gi` is mandatory for any distributed comparison arm**. The trap is that the driver's derived default is `32Gi` at TP=8. |

### A ninth occurrence that is exit 137 and NOT an OOM

2026-08-20/21, `c1-checkpoint-shape.md:153-164`
and `c1-fanout-isolation.md:9-16`: the
first lazy delivery arm died with **`exit 137` after a failed liveness probe**. `copy_in` was
a synchronous `memcpy` on tokio worker threads, so one 4 GiB delivery at
`delivery.parallelism: 256` put 256 concurrent 16 MiB copies on the async runtime and the
admin listener never got scheduled. Fixed with `spawn_blocking` (`be86fdde`); the C1 arm was
pinned to `parallelism: 32` for one session with that comment, and is back at 256 now that the
copy runs off the async workers — the value and its full history live in
`bench/ladder/values/delivery-wide-96gi.yaml`,
the layer the old `values-c1.yaml` carried it in. **No
`OOMKilled` reason is claimed in either file** — this is a probe-failure SIGKILL. It is the
reason § 3's fourth branch exists.

### Counted by class

| class | count | which |
|---|---|---|
| budget mismatch (daemon) | 3 | 1, 2, 3 |
| client pinned / host memory | 4 | 5, 6, 7, 8 |
| page cache / allocator (daemon) | 1 — the only OPEN one | 4 |
| scheduling starvation (daemon, exit 137, not an OOM) | 1 | the c1 pair |

## 3. Decision tree

Start from **which pod died** and then from `current − file`.

```
Was it the DAEMON pod?
├── NO → the client died: § Client pinned memory
└── YES
    ├── reason=Error (not OOMKilled) → § Scheduling starvation
    └── reason=OOMKilled
        ├── (current − file) ≈ current  → § Budget mismatch
        │      anonymous memory alone filled the limit: a term is under-sized
        └── file is a large share of current → § Page cache
               the tier's buffered I/O is the charge; no values key bounds it
```

### <a id="budget-mismatch"></a>Budget mismatch — which values keys

The rendered limit is `resources.limits.memory` **plus** every term
`config.memCapacity` cannot see. Read the number the kubelet will apply, not the one in the
values file:

```bash
helm template <release> deploy/helm/pacer -f <your overlays> | grep -A1 'limits:'
```

The running daemon publishes its own side of that arithmetic, so on a pod that is still up
there is no need to re-derive it by hand — `pacer_memory_budget_bytes{term,counted}` is one
series per term with `counted="false"` marking the ones the chart does not add either, and
`pacer_memory_budget_total_bytes` is the enforced sum the startup check compared with
`memory.max`. The term whose value does not match what the template above renders is the
drift. `PacerDaemonMemoryBudgetNearLimit` is that comparison as an alert.

Then walk the terms in
[`memory-model.md § rule 2`](../helm/memory-model.md#rule-2--what-the-chart-adds-for-you):

| if the load was… | the key to raise | note |
|---|---|---|
| a cold read that filled the RAM tier | `resources.limits.memory` — **~2× `config.memCapacity`** | incident 1. The chart refuses only `limit < memCapacity`; equality still renders. |
| a large delivery (ADR-0026) | `resources.limits.memory`, and check `delivery.pinnedReservation` ≥ `delivery.pinnedBytesMax` | incident 2. `delivery.workingSetReservation` is a FLOOR, not the measured working set. |
| a save on a Standard backend (ADR-0032) | `scatter.stagingBytes`, `scatter.coordinatorObjectBytes` / `coordinatorConcurrency` | incident 3. Raising `stagingBytes` to fit a whole-fleet save is **not** the fix — that save is meant to run the fallback ([write-scatter.md](../helm/write-scatter.md)). |
| RDMA startup / seed load | `efa.pinnedPoolReservation` ≥ `cluster.rdmaArenaBytes + 256 × config.chunkSize` | `pacer.validateArenaReservation` now fails the render on this. planning/16 §4.5. |
| a fleet whose `config.chunkSize` moved | all of the above — the holder arena, the slab and both derived footprints are multiples of it | |

If `efa.hugepages` is set, the node's **boot-time** reservation must cover the arenas plus
ADR-0028's slab; `pacer.validateHugepages` prints the exact figure. A hugepage request that
cannot be honoured degrades to 4 KiB pages *with a warning rather than failing*, which for
the slab is a silent invalidation.

### <a id="client-pinned-memory"></a>Client pinned memory — which client flags

Four of the eight incidents are the client, not the daemon, and none of them is fixed in the
chart. Check the **client pod's** own limit and the harness variable that sets it:

| client | variable | note |
|---|---|---|
| `pacer_st_bench` / the NIC loader (C5 arms) | `C5_MEMORY` (default `32Gi`) | incidents 5 and 6. A 70B integrity arm needs ~96 GiB even with verification bounded to 64 MiB pieces. |
| the DCP bench pod | `LADDER_DCP_BENCH_MEMORY` (`dcp.sh` default `24Gi`) | incident 7. **Mandatory** on a 70B arm; DCP holds the whole ~131 GiB destination state dict. Docs use `200Gi`. |
| the vLLM pod | `C5_VLLM_MEMORY` | incident 8. **Mandatory** for any distributed comparison arm (`DIST=1`) — `768Gi`. The driver's derived default is `32Gi` at TP=8. |

Two daemon-side keys bound what a client can make the *daemon* pin, and they are checked at
render time: `delivery.maxTargetBytes` (per request) must not exceed
`delivery.pinnedBytesMax` (node-wide), and `delivery.pinnedReservation` must not fall below
it. Neither bounds the client's own allocation.

### <a id="page-cache"></a>Page cache — what `rule 3` means and the knob

foyer's disk tier does **buffered** I/O, so on a large-memory node the "NVMe tier" **is** the
page cache, and page cache is charged to the cgroup that instantiated it exactly like
anonymous memory. Measured on a p5 serving a 131 GiB checkpoint: 512 GiB served from the disk
tier, 0.52 GiB of actual md127 reads, `read_bytes: 0` on the process, **72.1 GiB resident in
`Cached`**.

The term scales with **the bytes a load moves**, not with `memCapacity` and not with any value
in the chart — which is why `pacer.memoryLimit` does not budget for it and why this is a rule
and not a formula. Budget roughly `min(checkpoint bytes, what you will let the page cache
hold)` above the tier size. `96 − 24 = 72 GiB` is the same 72.1 GiB, for the same checkpoint.

The knob that removes it rather than budgets for it is **`config.diskTier: store`**
(ADR-0033): one `pread` into a registered frame instead of foyer's read path.

⚠ **It caps the READ side only.** The store's reads are `O_DIRECT`; its writes are not, and
nothing drops the pages afterwards — "Reads are O_DIRECT and writes are not, which is the
whole point"
([`crates/pacer-cache/src/store.rs`](../../crates/pacer-cache/src/store.rs)). Both tiers
instantiate page cache when they POPULATE, which is every cold read-through and, under
ADR-0032, every window the scatter caches or stages. **Do not expect `store` to zero
`pacer_cgroup_memory_file_bytes` on a write arm** — and note `pacer.memoryLimit` deliberately
does not branch on `diskTier`, because a branch would under-budget exactly the write arm that
already died.

Incident 4 is this class and is **still open**. If you are here, capture
`pacer_cgroup_memory_file_bytes` and peak RSS together — that pair is what the arm which
would close it is waiting for.

### <a id="scheduling-starvation"></a>Scheduling starvation — requests and priority

`reason=Error` with `exitCode: 137` and a failed liveness probe in the pod's events is the
runtime being starved, not the cgroup being full. The one recorded case was internal (a
synchronous `memcpy` on the tokio workers, fixed in `be86fdde`), but the same symptom comes
from outside:

* **`delivery.parallelism`** above ~32 was unsafe until `copy_in` moved to `spawn_blocking`.
  It is 256 again on the wide delivery arms
  (`bench/ladder/values/delivery-wide-96gi.yaml`),
  which is safe BECAUSE of that fix and is also the arm that proves it — if you see this
  symptom on a build predating `be86fdde`, 32 is the conservative value to fall back to.
* **`resources.requests`** — the daemon requests `cpu: 1` / `memory: 2Gi` while its derived
  limit can be tens of GiB. Under node memory pressure the kubelet ranks eviction by usage
  *above request*, so a large-tier daemon on a small request is a top candidate. Raise
  `resources.requests.memory` toward the tier size on a node you control.
* **`priorityClassName`** is `system-node-critical` by default. If an overlay lowered it, a
  preemption can take the cache out from under every client on the node.
* **No CPU limit** is deliberate ([ADR-0035](../adr/0035-daemon-resources-and-qos-class.md)):
  CFS throttling stalls the whole cgroup for the rest of a 100 ms period, and ADR-0025 gives
  every EFA rail a pinned OS thread whose fd wakeup cannot afford that. If an overlay set
  `resources.limits.cpu`, remove it before chasing anything else.
* **`terminationGracePeriodSeconds`** must stay above the 20 s drain deadline; a grace period
  at or below it turns every rollout into a SIGKILL mid-drain, which presents as
  client-visible errors on a *deliberate* restart.

## 4. Alerts that point here

[`templates/prometheusRule.yaml`](../../deploy/helm/pacer/templates/prometheusrule.yaml),
gated by `monitoring.prometheusRule.enabled`:

| alert | anchor it links to |
|---|---|
| `PacerDaemonMemoryNearLimit` | [§ Budget mismatch](#budget-mismatch) |
| `PacerDaemonAnonymousMemoryNearLimit` | [§ Page cache](#page-cache) |
| `PacerDaemonCgroupOom` | this page |
| `PacerDaemonOomKilled` | this page |
| `PacerDaemonMemoryBudgetNearLimit` | [§ Budget mismatch](#budget-mismatch) |

`PacerDaemonCgroupOom` is the one that fires **while there is still something to do about
it**: `memory.events oom` counts a reclaim failure *before* any kill, and a running daemon can
report it.

`PacerDaemonMemoryBudgetNearLimit` is the one that fires **before there is anything to see at
all**, because it is not a measurement. It compares the *configured* budget —
`pacer_memory_budget_total_bytes`, the enforced sum, with each term also published as
`pacer_memory_budget_bytes{term,counted}` — against `pacer_cgroup_memory_max_bytes`, applying
the same headroom the startup check does. That comparison is what § Budget mismatch below
walks by hand, so a firing alert has already done the arithmetic: read
`pacer_memory_budget_bytes` term by term, find the one that does not match what
`pacer.memoryLimit` renders, and raise the key in the table there. Note that the daemon
**refuses to start** when this ratio reaches 1, so if it is firing on a *running* pod the
limit was lowered under it, `config.memoryCheck` is `warn`, or the chart and the daemon
disagree about which terms count — the third is the only one that is a defect rather than a
configuration choice. It is a warning and not critical on purpose: nothing has necessarily
grown, and the pod may be serving perfectly.

## 5. Related history outside the eight

Same defect family, useful when a new incident does not match a row above:

* `planning/16-rdma-saturation.md:179-187` §4.5 — both
  daemons OOMKilled at a 32Gi limit under seed load, because the pinned pools are invisible to
  the cache budget. This is the incident `efa.pinnedPoolReservation` exists for.
* `planning/15-benchmarks-b4-restore-storm.md:683-692`
  — c=1000, both daemons OOMKilled, confirmed via `kubectl describe`.
* `planning/15-benchmarks-b4-restore-storm.md:944-1022`
  — the 2026-08-12 warm serve-path OOM (`probe-cpu` fan-in 7 killed the holder), fixed in
  `f3a655c`, validated 2026-08-13. Note line 1029 is an explicit **non**-OOM
  (`reason=Error, NOT OOMKilled`).
* `bench/b4/run.sh` — `wait_pod_terminal` exists because on
  2026-08-21 "a restore pod whose daemon had been OOMKilled sat in Failed while the harness
  waited 46 minutes of a 3600s budget". That is incident 4's kill seen from the harness.

## See also

* [`docs/helm/memory-model.md`](../helm/memory-model.md) — the five rules.
* [`docs/helm/monitoring.md`](../helm/monitoring.md) — the alerts.
* [`docs/helm/write-scatter.md`](../helm/write-scatter.md),
  [`docs/helm/delivery.md`](../helm/delivery.md),
  [`docs/helm/efa-and-rdma.md`](../helm/efa-and-rdma.md) — the three features whose
  footprints the chart budgets for.
