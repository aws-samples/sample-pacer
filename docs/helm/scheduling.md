> Design notes for the `session`, `nodeSelector`, `tolerations`,
> `priorityClassName`, `resources`, `podDisruptionBudget`,
> `terminationGracePeriodSeconds`, `extraEnv`, `devMode`, `serviceAccount` and `karpenter`
> keys in [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values
> file keeps one short comment per key; the reasoning and the failure history live here.

# Scheduling, disruption and pod lifecycle

## Session isolation (ADR-0029) — how several fleets share one cluster

A dev/bench cluster is worked by several sessions at once, each wanting its own daemon
fleet. Distinct release names are necessary but NOT sufficient: the DaemonSet still targets
nodes by POOL label, so two releases put two daemons on one node, where they fight over the
single hostPath cache dir and the node's EFA devices. Two knobs close that.

[`scripts/dev/pacer-session`](../../scripts/dev/pacer-session) derives the token and claims
the nodes; every deploy path in this repo passes `--set session.id=<token>`. **A production
install leaves `id` empty and nothing below changes its scheduling.**

* **`session.id`** — an opaque token identifying the session that owns this release. When
  set it is stamped as `pacer.io/session` on every rendered object AND (per
  `requireClaimedNodes`) added to the DaemonSet's `nodeSelector`, so this release lands ONLY
  on nodes that session has claimed. Empty = a single unowned fleet, the production shape.
  It is deliberately NOT part of `pacer.selectorLabels`: a DaemonSet's `spec.selector` is
  immutable, so adding a key there would make every existing release unupgradeable. It goes
  on object metadata and on the pod TEMPLATE (a superset of the selector is legal), which is
  what `kubectl get pods -l pacer.io/session=<id>` needs.
* **`session.requireClaimedNodes`** (default true) — confine the DaemonSet to nodes labelled
  `pacer.io/session: <id>`. This is what makes a release physically unable to deploy onto
  another session's node — without it, `session.id` is a label with no teeth. Ignored when
  `id` is empty. It is merged inside `pacer.nodeSelector` rather than asked of each caller,
  so no deploy path can forget it and quietly land on a node another session is measuring
  on.
* **`session.exclusiveNodes`** (default true) — at most one pacer daemon per node, whatever
  release it belongs to: a required `podAntiAffinity` over `app.kubernetes.io/name` (the
  CHART name, shared by every release) at hostname topology. Safe in production because
  one-daemon-per-node is already the invariant there — a DaemonSet cannot violate it by
  itself. What this catches is a SECOND release selecting the same node: rather than two
  daemons corrupting one cache dir and racing for EFA devices, the interloper's pod stays
  Pending and says so. The `RollingUpdate` strategy deletes before it creates, so an
  in-place upgrade never trips it.

## `nodeSelector` and the null-skipping helper

`pacer.nodeSelector` does two jobs. First, it **skips null-valued entries**: Helm's
`key: null` deletion only works against chart DEFAULTS, not between `-f` overlay files, so a
later overlay nulling an earlier overlay's selector
([`bench/ladder/values/placement-ohio-gpu-dev8x.yaml`](../../bench/ladder/values/placement-ohio-gpu-dev8x.yaml)
nulling [`values-gpu-ohio.yaml`](../../deploy/helm/pacer/values-gpu-ohio.yaml)'s pool label) would
otherwise render an API-invalid null value. Second, it adds `pacer.io/session: <id>` when
`session.requireClaimedNodes` is on.

`tolerations` is a plain list, and **a list REPLACES rather than merges**, which is what
lets a GPU overlay drop a cache-pool toleration wholesale.

## `priorityClassName`

`system-node-critical` by default. The cache tier is infrastructure for the workloads on
its node; a preemption that evicts it turns every co-located client's reads into backend
round-trips.

## CPU, QoS, and why there is no CPU limit

The daemon ships **Burstable**: `resources.requests.cpu` is set, `resources.limits` carries
**memory only**. That is a decision with a record —
[ADR-0035](../../planning/adr/0035-daemon-resources-and-qos-class.md) — not an omission.
The short form:

* **CFS throttling is the wrong failure mode for this daemon.** ADR-0025 gives every EFA
  rail its own OS thread pinned to a distinct CPU, and a rail's reaper blocks on a
  completion-channel fd; a CPU quota stalls the whole cgroup for the remainder of a 100 ms
  period, which is precisely the "wakeup latency starves the in-flight window" collapse
  ADR-0025 exists to prevent.
* **This daemon has already been killed once by runtime starvation, not by contention.**
  The first lazy delivery arm died with `exit 137 after a failed liveness probe` because
  `copy_in` was a synchronous `memcpy` on tokio workers and 256 concurrent 16 MiB copies
  starved the admin listener
  ([`bench/ladder/results/c1-checkpoint-shape.md`](../../bench/ladder/results/c1-checkpoint-shape.md)).
  A CPU quota reproduces that class deliberately and fleet-wide.
* **Guaranteed is not reachable without making the DaemonSet unschedulable.** Guaranteed
  requires `limits == requests` for CPU *and* memory, and the memory limit here is
  DERIVED and deliberately much larger than the request — up to 84 GiB on the w1 write
  stack. Requesting that as a reservation is the same mistake ADR-0027 rules out for GPUs:
  the cache would hold capacity the workloads it exists to serve then cannot schedule
  against.
* The measured CPU profile is bursty and low on average — the holder measured **0.76 of 192
  cores** while the RDMA completion pumps burn **~14 cores** at peak, and ADR-0028 removed
  **~10 cores of memcpy** at 28 GiB/s. A limit sized for the average throttles the peak; a
  limit sized for the peak reserves nothing (limits do not reserve) and only adds a ceiling.

An operator on a shared node who needs a ceiling can still set `resources.limits.cpu` — the
DaemonSet renders every non-`memory` key of `resources.limits` verbatim — and ADR-0035 § 
Consequences says what to watch when they do.

## PodDisruptionBudget

`podDisruptionBudget.enabled` defaults to **true** with `maxUnavailable: 1`.

**What it protects.** A cache-tier restart is a cold cache on that node, and the
`updateStrategy` already rolls one pod at a time for exactly that reason. A PDB extends the
same guarantee to disruptions the DaemonSet controller does not own: node consolidation,
`karpenter.sh` drains, cluster upgrades, anything that calls the **Eviction API**. Without
one, a consolidation event can take several cache nodes at once and every client on them
falls back to the backend simultaneously.

**What it does not protect.** `kubectl drain --ignore-daemonsets` — the documented way to
drain a node that runs DaemonSets — does not evict DaemonSet pods at all, so no PDB
constrains that path. The PDB binds only callers that go through eviction.

**The selector is per-release** (`pacer.selectorLabels` = chart name + release instance), so
another session's Pending interloper (see `session.exclusiveNodes`) cannot consume this
release's budget. This release's OWN Pending pod does: a PDB at `maxUnavailable: 1` with one
pod already unavailable blocks every further eviction until it recovers. That is the
intended behaviour — the remaining nodes are the fleet — but it is the reason to set
`enabled: false` on a fleet that is deliberately partially Pending, which is a bench shape
and not a production one.

## `terminationGracePeriodSeconds`

**30 seconds.** The daemon drains on SIGTERM with a **20 s** deadline, so the grace period
has to exceed it or the kubelet SIGKILLs mid-drain and the in-flight requests the drain
exists to finish are lost anyway. 30 s = the 20 s drain plus room for the listener shutdown
and the final metrics scrape; raise both together, never just one.

## `extraEnv`

Extra container environment, appended **last** (so it wins over everything the chart
renders, `allocator` included). For variables the daemon does NOT own and so cannot express
in its config file, and which the chart has no first-class key for. It is also how a control
arm turns one of the two `MALLOC_*` entries back to glibc's default without editing the
chart.

`PACER_*` knobs do not belong here: they go in `config`, whose checksum annotation rolls the
pods (ADR-0013).

It needs no checksum of its own, and the comment that used to be here — "nothing here is
checksummed, so changing it needs a manual roll" — was **wrong**. These entries render into
the DaemonSet's POD TEMPLATE, and any change to a pod template rolls the DaemonSet by itself;
`checksum/config` exists only because a ConfigMap is NOT part of the pod template, so
editing one would otherwise be invisible to the controller. The one case where the old
warning holds is an entry whose value is a `valueFrom` reference: the reference is in the
template but the referenced VALUE is not, so changing the underlying ConfigMap or Secret does
need a manual roll.

## `serviceAccount`

Backend auth is EKS Pod Identity ([ADR-0006](../../planning/adr/0006-strip-and-resign-auth.md)):
associate this ServiceAccount with the role holding `s3express:CreateSession` on the bucket
ARN, cluster-side —

```bash
aws eks create-pod-identity-association --cluster-name <cluster> \
  --namespace <ns> --service-account <this SA> --role-arn <role>
```

— so no annotation is needed here. `annotations` is a generic passthrough for anything else
that annotates the SA.

⚠ A release-named SA is the trap: a Pod Identity association names exactly one
`(namespace, serviceAccount)` pair, so a session-scoped release with `create: true` boots,
passes `/healthz`, and fails EVERY backend call with `InternalError` — no credentials, and
nothing in the pod says so. Every dev and bench overlay therefore sets
`serviceAccount: {create: false, name: pacer}`.

## `devMode`

Dev iteration mode ([`scripts/dev/`](../../scripts/dev/)): replaces the distroless image
with a shell image running a supervisor that execs `/pacer-dev/pacer-daemon` and swaps it
whenever a new binary is pushed (`pacer-dev push` — cross-compiled locally, no image
rebuild). **Never enable in a real deployment.** Pods start with NO daemon binary and stay
NotReady until the first push, so `devMode` also drops the liveness and startup probes.

`devMode.image` empty means "the release image this deploy would otherwise run"
(`image.repository:image.tag`), which is the only default that can actually work: the binary
pushed in by `build-pod swap` / `pacer-dev push` is dynamically linked against glibc, and
under `--features efa` against libibverbs/libefa too. This was `busybox:1.36`, which has
neither — the supervisor exec'd the binary, the loader refused it, and the pod sat NotReady
in a restart loop repeating `error while loading shared libraries: libibverbs.so.1`
(measured on gpu-ohio, 2026-08-22; the dev loop had never produced a running daemon). The
runtime image carries `/bin/sh`, glibc 2.34 and the EFA userspace —
[`ci/Dockerfile.release`](../../ci/Dockerfile.release) COPYs `/efa-rootfs` — so it can host
anything the builder image produces, and matches what production runs. Set it explicitly
only for a binary with no such dependencies.

## `karpenter`

A Karpenter nodepool for the cache tier, rendered only if `enabled` — requires Karpenter v1
CRDs installed. See
[`templates/karpenter.yaml`](../../deploy/helm/pacer/templates/karpenter.yaml) for the
invariants (single AZ-ID, Nitro v4+, RAID0, EFA).

`zoneId` is an **AZ ID (not a name!)** of the directory bucket, e.g. `use1-az4`. AZ names
shuffle per-account; the bucket lives in an AZ ID.

`instanceFamilies` lists families with Nitro v4+ (RDMA READ capable —
[ADR-0004](../../planning/adr/0004-nitro-v4-plus-nodepool.md)). g5/c5n are deliberately
absent: no RDMA, they'd silently degrade to gRPC.

## The image

`image.repository` defaults to the public multi-arch image published to GHCR by the
tag-driven release workflow (`.github/workflows/release.yaml`). Override for a
private-registry build — e.g. same-account ECR pulls credential-free on EKS via the node
role's credential provider, no pullSecret needed. `image.pullSecrets` names existing
`docker-registry` secrets in the release namespace; empty is correct for ECR pulled via the
node IAM role, and naming a secret that does not exist only adds a kubelet warning to every
pod event list.

The init container `chown-cache` hands the hostPath NVMe dir to uid 65532: the image runs as
nonroot but the hostPath dir is created root-owned.

## See also

* [ADR-0029](../../planning/adr/0029-session-scoped-fleets-and-exclusive-nodes.md),
  [ADR-0035](../../planning/adr/0035-daemon-resources-and-qos-class.md).
* [`scripts/dev/README-session.md`](../../scripts/dev/README-session.md) — the cluster
  protocol these knobs implement.
* [memory-model.md](memory-model.md) — `resources.limits.memory` is a budget, not the limit.
* [`docs/runbooks/daemon-oom.md`](../runbooks/daemon-oom.md) § "Scheduling starvation".
