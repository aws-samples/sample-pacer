# ADR-0035: The daemon runs Burstable with no CPU limit; disruption is bounded by a PDB and a 30 s grace period

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-09-08 · Status: Accepted (reasoned from measurements already recorded; no new arm)

Fixes the pod's resource shape, which until now was an accident of what each key happened to
be set to. Does not change any memory arithmetic —
[ADR-0024](0024-registered-arena-rdma-buffers.md),
[ADR-0026](0026-client-supplied-target-memory.md),
[ADR-0028](0028-cache-ram-tier-is-the-registered-arena.md) and
[ADR-0032](0032-write-scatter-populates-the-cache.md) still own every term
`pacer.memoryLimit` sums. Constrains the placement discipline of
[ADR-0025](0025-rail-numa-placement-and-pinned-reapers.md) by refusing the one setting that
would defeat it.

## Context

The chart shipped `resources.requests: {cpu: "1", memory: 2Gi}` and
`resources.limits: {memory: 4Gi}` — a CPU request with no CPU limit, and a memory limit that
`pacer.memoryLimit` then grows by every pinned term the cache's own accounting cannot see.
That is **Burstable** QoS, and nothing recorded why. Three separate things were unstated:

1. **Whether the absence of a CPU limit is deliberate.** A reviewer adding one would be
   making an invisible decision, and it is not a small one for this daemon.
2. **What bounds disruption.** `updateStrategy.rollingUpdate.maxUnavailable: 1` bounds the
   DaemonSet *controller*'s own rollout, and its comment says why ("cache-tier restart = cold
   cache on that node"). It says nothing about node consolidation, a Karpenter drain or a
   cluster upgrade, none of which the controller owns. There was no PodDisruptionBudget.
3. **How long the pod gets to stop.** `terminationGracePeriodSeconds` was unstated, so the pod
   inherited Kubernetes' default 30 s by coincidence rather than by arithmetic — and a
   SIGTERM drain with a 20 s deadline is landing in the daemon (quality item R5), which makes
   the relationship between the two load-bearing.

What is already measured, and what the decision rests on:

* **The daemon's CPU profile is bursty and low on average.** The holder measured **0.76 of
  192 cores** while being called the bottleneck (planning/19 D4; restated in ADR-0025 and
  ADR-0026), the RDMA completion pumps **burn ~14 cores** at peak
  (`bench/ladder/results/nvme-device-truth.md`,
  ADR-0033), and ADR-0028 removed **~10 cores of memcpy** at 28 GiB/s
  (`adr28-slab-serve-path.md`). The
  ratio between the average and the peak is what makes a single quota unsizeable.
* **ADR-0025 gives every rail a pinned OS thread whose latency is the product.** "A rail's
  reaper blocks on its completion-channel fd. Unpinned, several rail threads pack onto shared
  cores and their wakeup latency starves the in-flight window … per-rail rate *collapses* as
  rails are added." On a p5.48xlarge that is 32 threads named `pacer-cq-<rail>`, plus 32
  arena-registration threads at startup.
* **This daemon has already been SIGKILLed once by runtime starvation, and it was not
  contention.** The first lazy delivery arm died with `exit 137 after a failed liveness
  probe`: `copy_in` was a synchronous `memcpy` on tokio workers, 256 concurrent 16 MiB copies
  at `delivery.parallelism: 256` starved the admin listener
  (`bench/ladder/results/c1-checkpoint-shape.md:153-164`).
  Fixed in `be86fdde`; the shape of the failure is the point.
* **A write arm's own report rules CPU throttling out as an explanation precisely because
  nothing constrained it**: "The write pod declares no `resources` block at all …, so the
  seeder is unconstrained on a 96-vCPU node. That is what rules out 'the client pod was
  CPU-throttled' as an explanation for any rate in this file"
  (`w1-write-ceilings.md:250-262`). Every
  rate this project has published was measured without a CPU quota anywhere in the path.

## Decision

**1. The daemon runs Burstable: a CPU request, no CPU limit.** The chart ships no
`resources.limits.cpu` and none should be added as a default.

The mechanism that decides it is CFS bandwidth control: a CPU quota is enforced per 100 ms
period, and once the cgroup exhausts it *every* thread in the container is descheduled until
the period rolls over. For a proxy that would be latency. For this daemon it is precisely the
failure ADR-0025 exists to prevent — a rail's reaper is blocked on an fd, and a throttled
wakeup is indistinguishable from an unpinned one from the in-flight window's point of view.
The one recorded runtime-starvation kill (exit 137, failed liveness probe) is what that class
looks like from outside, and a quota would make it reachable by configuration rather than by
a code defect.

A quota is also unsizeable here rather than merely risky. Sized for the average (0.76 cores
of holder work) it throttles the peak (~14 cores of completion pumps). Sized for the peak it
reserves nothing — a limit is not a reservation — and buys only a ceiling nobody wants.

**2. Guaranteed is rejected, and not because it is undesirable.** Guaranteed requires
`limits == requests` for CPU *and* memory. The memory limit here is derived and deliberately
much larger than the request: `pacer.memoryLimit` reaches 84 GiB on the ladder's w1 write
stack at the 2026-08-27 sweep's own shape. Making that a *request* asks the scheduler to
reserve it on every node in the pool, and the workloads this cache exists to serve then
cannot schedule against it — the same trade ADR-0027 already rules out for GPUs
("`resources.limits.nvidia.com/gpu` … makes them unallocatable and the training pods this
cache exists to serve cannot schedule at all"). A DaemonSet that cannot schedule is worse
than one that can be evicted.

**3. An operator may still set a CPU limit, and the chart renders it.** `templates/daemonset.yaml`
already emits every non-`memory` key of `resources.limits` verbatim, and
`values.schema.json` accepts `resources.limits.cpu`. This ADR does not forbid it; it records
that the *default* is deliberate and what to watch if you override it (§ Consequences).

**4. Disruption is bounded by a PodDisruptionBudget, on by default.**
`templates/pdb.yaml`, gated by `podDisruptionBudget.enabled` (default `true`) with
`maxUnavailable: 1`, selecting on `pacer.selectorLabels` (chart name + release instance).

It extends the controller's own one-at-a-time guarantee to everything that goes through the
**Eviction API**: node consolidation, a Karpenter drain, a cluster upgrade. `maxUnavailable: 1`
rather than `minAvailable` because a DaemonSet's replica count is the node count and changes
under it; an absolute floor would have to be re-tuned on every scale event.

Two limits are stated rather than implied. `kubectl drain --ignore-daemonsets` — the
documented way to drain a node running DaemonSets — does not evict DaemonSet pods at all, so
no PDB constrains that path. And a Pending pod *of this release* consumes the budget and
blocks every further eviction until it recovers; that is the intended reading (the healthy
remainder is the fleet), and it is why a bench fleet deliberately left partially Pending
should set `enabled: false`. A *different* release's Pending interloper — which
`session.exclusiveNodes` (ADR-0029) creates on purpose — carries a different
`app.kubernetes.io/instance` and cannot consume this release's budget, which is why the
selector is per-release and not per-chart.

**5. `terminationGracePeriodSeconds: 30`, stated rather than inherited.** The daemon's SIGTERM
drain has a **20 s** deadline. The grace period must exceed it: the kubelet SIGKILLs when the
period expires, so a drain that has not finished loses exactly the in-flight requests it
exists to finish, and the rollout pays the latency for nothing. 30 s is that deadline plus
room for the listeners to close and one final metrics scrape.

The relationship is enforced in two places on purpose: `exclusiveMinimum: 20` in
`values.schema.json`, and `pacer.validateGracePeriod` in `templates/_validate.tpl` for a
`--skip-schema-validation` render. The 20 is named once, as
`pacer.drainDeadlineSeconds`, so raising one without the other is visible.

## Consequences

* **The daemon stays a top eviction candidate under node memory pressure, and that is not
  fixed here.** The kubelet ranks eviction by usage *above request*, and
  `resources.requests.memory` is `2Gi` against a limit that can be tens of GiB. Raising the
  request toward the tier size is the right move on a node an operator controls, and the
  runbook says so — but the chart cannot default it, because a DaemonSet request no node in
  the pool can satisfy leaves the whole fleet Pending, which is the same failure as § 2 by
  another route. This is the one term this ADR leaves to the operator.
* **An operator who does set `resources.limits.cpu` should read
  `pacer_rdma_rail{metric="writes_in_flight"}` and the `node_local_rails` count on the "RDMA
  arenas registered" log line before and after.** Throttling presents as a rate that fell
  with every placement and buffer knob apparently correct — the exact signature ADR-0025 spent
  a track distinguishing from memory bandwidth. `container_cpu_cfs_throttled_periods_total`
  is the series that names it directly; without a CPU limit it is always 0, which is also how
  to confirm the default is in effect.
* **A PDB makes some drains slower, deliberately.** With several cache nodes selected for
  consolidation at once, terminations serialize. For a tier whose restart cost is a cold cache
  and a fleet-wide fallback to the backend, that is the trade taken on purpose. Anyone whose
  drain now blocks should read `kubectl get pdb` first: at `maxUnavailable: 1` a single
  unavailable pod is the whole budget.
* **The PDB is a fifth rendered object in the default install** (7-8 → 8-9 depending on
  `serviceAccount.create`). Any test asserting a resource count changes.
* **Nothing measured is invalidated.** No default that affects a rate moved; every rendered
  memory limit is byte-identical, which the CI `helm` job's own arithmetic assertions check.
* **The grace period is now coupled to a number the daemon owns.** If the drain deadline
  moves, `pacer.drainDeadlineSeconds` and the schema's `exclusiveMinimum` both have to move
  with it, and the render fails until they do. That coupling is the point: the previous state
  — an unstated grace period defaulting to 30 by coincidence — would have silently become
  wrong the moment the drain deadline was raised past it.

## Alternatives considered

* **Guaranteed with `requests == limits` on both.** Rejected in § 2: the memory limit is
  derived and large, so this is a reservation the pool cannot honour.
* **A CPU limit sized generously (e.g. 32).** Rejected: it is still a cliff, it still stalls
  the pinned reapers when crossed, and on a 192-vCPU p5 it would bind exactly on the
  high-fan-out serve the ADR-0018 flip gate was won on. A limit that only ever binds during
  the workload the product exists for is worse than none.
* **`minAvailable` instead of `maxUnavailable` on the PDB.** Rejected: a DaemonSet's replica
  count tracks the node count, so an absolute floor needs re-tuning on every scale event and
  is wrong in the direction that blocks all drains on a small fleet.
* **No PDB, relying on `priorityClassName: system-node-critical`.** Rejected: priority governs
  *preemption and scheduling*, not voluntary eviction, so it does nothing about the
  consolidation and upgrade paths this is for.
* **Leaving the grace period unstated.** Rejected: it is only correct by coincidence today
  (Kubernetes' default happens to be 30), and R5's 20 s drain makes it a real constraint. An
  invariant that holds by accident is one nobody will maintain.

## References

* [ADR-0025](0025-rail-numa-placement-and-pinned-reapers.md) — the pinned reapers a CPU quota
  would defeat.
* [ADR-0027](0027-gpu-memory-delivery-targets.md) — visibility is not allocation, the same
  argument applied to GPUs.
* ADR-0029 — why the PDB selector is
  per-release.
* `bench/ladder/results/c1-checkpoint-shape.md`
  — the runtime-starvation SIGKILL.
* `bench/ladder/results/nvme-device-truth.md`,
  `bench/ladder/results/adr28-slab-serve-path.md`,
  `bench/ladder/results/w1-write-ceilings.md`
  — the CPU figures.
* [`docs/helm/scheduling.md`](../../docs/helm/scheduling.md),
  [`docs/runbooks/daemon-oom.md`](../../docs/runbooks/daemon-oom.md) — the operator-facing
  form of this decision.
