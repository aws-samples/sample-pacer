> Design notes for the `efa` and `cluster` keys in
> [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file
> keeps one short comment per key; the reasoning, the measurements and the failure history
> live here.

# The cluster tier and the EFA-RDMA transport

## The cluster cache tier

`cluster.enabled` (default **true**) turns on the Phase 2 cluster cache tier: a
rendezvous-hash ring over the DaemonSet, membership from the headless peer Service's
EndpointSlices, peer blob fetch over gRPC
([ADR-0008](../adr/0008-grpc-first-build-order.md),
[ADR-0012](../adr/0012-owner-read-through-cluster-fill.md)). Disabled = a
fleet of independent Phase 1 nodes.

`cluster.channelCapacity` (default **8**) is the number of chunks buffered per peer blob
stream ([ADR-0013](../adr/0013-yaml-config-file-layered-over-env.md)). Small
on purpose: the stream itself is the backpressure.

`cluster.replicationR` is the co-home count R
([ADR-0016](../adr/0016-multi-copy-replication.md)). Empty → the daemon default
(2). Set to 1 to force a single-home ownership split — with R ≥ 2 in a small ring every key
homes on the local node, so a cross-node fetch never crosses the peer plane, which is why
the A4 peer-plane benchmark pins it to 1.

`cluster.localAdmissionThreshold` is the requester-local admission threshold (ADR-0016
layer 1). Empty → daemon default (2). Set very high to disable local admission, so a
transport benchmark keeps every GET on the peer plane instead of serving hot keys from a
locally-admitted copy after a couple of hits.

## Why EFA is not the base default

Phase 3's EFA-RDMA path is off in the base chart; the block that turns it on ships
commented at the end of
[`values-example.yaml`](../../deploy/helm/pacer/values-example.yaml).

The A4 benchmark cleared the ADR-0008 flip gate
(`planning/14-benchmarks-a4-efa.md`, 2026-07-21):
the holder-driven WRITE data plane beats gRPC on throughput AND holder CPU at high
fan-out, so EFA is now the intended default *wherever EFA hardware exists* — set
`efa.enabled: true` on an EFA nodepool. The base default
stays `false` because `enabled: true` requests an EFA device, which makes the pod
UNSCHEDULABLE on non-EFA nodes (dev, plain installs, CI): EFA is hardware-gated, so it
can't be a universal chart default. The daemon probes for EFA at startup and speaks
gRPC-only where it's absent regardless
([ADR-0018](../adr/0018-holder-driven-rdma-write-data-plane.md)), and every
RDMA error falls back to gRPC per-peer
([ADR-0003](../adr/0003-efa-rdma-cross-node-reads-only-grpc-fallback.md)) —
the fallback path is the product for non-EFA nodes forever (ADR-0008).

**The three knobs are deliberately independent** — enabling the RDMA transport does NOT
require the other two
(`planning/11-efa-a2-infra-runbook.md`
finding #7):

* **`efa.enabled` alone is sufficient for RDMA.** It requests the EFA device (what makes
  Karpenter attach an EFA interface) and nothing else. `ibv_reg_mr` registers memory on
  ordinary 4 KiB pages, so the spike ran with neither hugepages nor IPC_LOCK.
* **`efa.hugepages` is NOT a throughput knob.**
  `planning/18-p5-bandwidth-investigation.md`
  measured page size as irrelevant to bandwidth. What it buys is a cheap registration and
  a small NIC translation footprint for one arena spanning tens of GiB
  ([ADR-0024](../adr/0024-registered-arena-rdma-buffers.md) point 3). Keep it
  off unless the node pre-reserves 2Mi pages at boot: a `hugepages-2Mi` *request* on a
  node with zero reserved makes Karpenter refuse to launch (the fresh AMI pre-allocates
  none, and Karpenter also needs a NodeOverlay declaring the capacity plus the NodeOverlay
  feature gate). When set, it must cover `efa.pinnedPoolReservation` — the arena maps from
  the hugepage pool — and the ConfigMap tells the daemon to map with 2Mi pages
  (`rdma-arena-page-mib: 2`, derived from the SAME value rather than being its own knob, so
  the pod request and the daemon mapping can never drift: an arena mapping a size the pod
  never requested fails ENOMEM and would degrade to 4 KiB pages, silently wasting the
  reservation).
* **`efa.ipcLock`** adds the IPC_LOCK capability, needed only if memory registration hits
  `RLIMIT_MEMLOCK` at startup. Current EKS EFA manifests omit it; turn it on only if MR
  registration fails.

⚠ **"hugepages buys registration cost and TLB footprint, NOT bandwidth" (planning/18
RESULT 5) is true of the PEER plane and false of client delivery.** It predates ADR-0028:
`pacer.cacheSlabBytes` derives a slab only when hugepages are set, and with a slab a WRITE
reads the cache in place instead of memcpying a chunk into a holder range — measured
2026-08-25 as **13.7 → 52.7 GiB/s (3.86×)** delivering 70B shards into 8 H100s.
So on a pool whose nodes DO pre-reserve 2 MiB pages, setting `efa.hugepages` is the largest
delivery win available.

**The ceiling is the boot reservation, not the value you set here.** Hugepages must be
reserved at boot (kernel cmdline or instance userData), and the slab is carved from what
was reserved, so the reservation — not `efa.hugepages` — decides how much of the RAM tier
can be slab-backed. Two worked numbers from the pools this was measured on: a node
reserving **16 GiB** of 2 MiB pages covers a tier of only ~4 GiB once the arenas have taken
8, so raising the tier there needs a bigger reservation first; a node reserving
`32768 × 2Mi` = **64 GiB** cannot slab-back a tier above ~52 GiB. Size the reservation
first, then set this value to match it.

## Reaching a device without requesting one

`efa.shareHostDevices` hostPath-mounts `/dev/infiniband` instead of asking the device
plugin for `vpc.amazonaws.com/efa`
([ADR-0030](../adr/0030-delivery-registration-belongs-to-the-memory-owner.md)
point 9). Pair it with `ipcLock`, since nothing else grants memlock either way.

Why it exists: one device serves many protection domains and queue pairs at once, so the
extended resource buys the uverbs mount plus SCHEDULER ACCOUNTING — and on a
one-interface node (r8gd/m8gd.24xlarge, the cheap cache pool) the daemon holding that one
unit makes every other EFA pod unschedulable. Under ADR-0030 that other pod is the
delivery CLIENT, which registers its window on its OWN NIC; it is also a tenant's NCCL
job. The cache is a guest on the node's fabric exactly as it is a guest on its GPUs:
**visibility is not allocation.**

One ordering constraint this does NOT remove: Karpenter attaches an EFA ENI because a
PENDING POD REQUESTS one (planning/14), so something must still ask at launch time — the
tenant's own pods in production, the launcher in a bench arm (which drops its request once
the ENI is attached).

**And it is not sufficient by itself.** `efa.privileged` is what actually lets the
container OPEN the devices `shareHostDevices` mounts.

Measured on a p5 with 32 free EFA units, 2026-08-24: with the mount alone the rails
enumerate (through `/sys/class/infiniband*`, which every pod sees) and then every
`ibv_open_device` fails EPERM — `head -c0 /dev/infiniband/uverbs0` says `Operation not
permitted` — because the device cgroup only ever admits devices the kubelet was told to
inject, and only a DEVICE PLUGIN ALLOCATION tells it that. A hostPath is a mount, not an
allocation. The daemon then reported "rail placement resolved rails=32" and served every
delivery as a body, which reads exactly like the delivery path declining.

So the three ways to give this daemon a fabric are: request a unit (forbidden for the
reason above), run privileged (`efa.privileged`), or advertise the same devices through a
plugin/DRA driver of our own that allows many holders.

**`efa.privileged` is the supported production mechanism (decided 2026-08-24)**, because
the target is a single-tenant LLM training cluster: the team installing this owns the
nodes, the EFA device plugin on the same node already runs privileged as root, and what
this actually grants is device-cgroup allow-all rather than capabilities (measured:
`CapEff` is all zeros for uid 65532). It consumes no EFA unit, which is what ADR-0030
point 9 was protecting. The plugin/DRA route is optional polish for installs whose policy
forbids a privileged pod — see ADR-0030 point 9 for both, and
`spike/cdi/README.md` for why the CDI-by-annotation shortcut
is not available.

It is still off by default, for one reason: **a chart must never silently escalate its own
privileges.** An install that wants RDMA opts in — and if it forgets while
`shareHostDevices` is on, `pacer.validateEfaAccess` fails the render rather than booting a
daemon that falls back to gRPC and looks merely slow.

## The pinned arenas

`efa.pinnedPoolReservation` (default **8Gi**) is pinned RDMA arena memory to ADD to the
container memory limit when `efa.enabled`
(`planning/16-rdma-saturation.md` §4.5 OOM
finding). The daemon registers two arenas at startup (ADR-0024), sized:

```
requester = cluster.rdmaArenaBytes                        (default 4Gi)
holder    = HOLDER_ARENA_RANGES × config.chunkSize        (256 × 16Mi = 4Gi)
```

…so 8Gi total at the defaults — PLUS `cluster.cacheSlabBytes` when ADR-0028's slab is
enabled (counted once, not per rail: every rail registers the same pages). Note the slab
does not add to the node's memory footprint the way the arenas do — it RELOCATES the
cache's RAM tier into pinned pages — but the cgroup limit must still cover it, because
`config.memCapacity`'s own accounting does not. This memory is invisible to
`config.memCapacity`; omitting it from the cgroup limit OOMKills the pod under seed load.
Keep it in lock-step with BOTH terms — raising `rdmaArenaBytes` or `chunkSize` raises the
pinned total — and remember the serve gate admits another `HOLDER_SERVE_SLOTS ×
chunkSize` of resident bodies on top.

`pacer.validateArenaReservation`
([`templates/_validate.tpl`](../../deploy/helm/pacer/templates/_validate.tpl)) is the
render-time check that this lock-step held: it fails when
`cluster.rdmaArenaBytes + 256 × config.chunkSize` exceeds `efa.pinnedPoolReservation`,
which is exactly the arithmetic above and exactly what raising `chunkSize` on an EFA node
breaks silently.

`cluster.rdmaArenaBytes` is the requester-side RDMA arena (ADR-0024) — the registered
bytes this node pins to receive peers' WRITEs, node-wide. Concurrency is
`bytes ÷ config.chunkSize` (4Gi ÷ 16Mi = 256 concurrent peer fetches), and on the
zero-copy path a range stays leased until the S3 client drains it, so size it as
`throughput × hold_time`: ~37Gi sustains the measured ~58 GiB/s at a 645 ms drain. Empty →
transport default (4Gi). **Raising it must raise `efa.pinnedPoolReservation` by the same
amount.**

## ADR-0028's cache slab

`cluster.cacheSlabBytes` maps the cache's RAM tier as ONE registered slab of chunk-sized
frames, so a holder posts a WRITE straight out of the cache instead of staging a copy into
an arena (`holder_copy` measured 0.348 → exactly 0.000 CPU-s/GiB,
`planning/19-saturation-roadmap.md` § C1b).

**ON BY DEFAULT wherever `efa.hugepages` is set** — leave it empty and the chart derives
`config.memCapacity + 256 × config.chunkSize`. That is ADR-0028's sizing rule with a
measured constant: a frame is held by foyer's resident set PLUS every chunk in flight, and
serve admission bounds in-flight bodies at 256 (`HOLDER_SERVE_SLOTS`,
[`crates/pacer-daemon/src/peer.rs`](../../crates/pacer-daemon/src/peer.rs)). The churn gate
ran exactly this shape — memCapacity 2 GiB (128 frames) plus 256 × 16 MiB = a 6 GiB slab
of 384 frames — and recorded **0 heap fallbacks while recycling frames 106×**
(`bench/ladder/results/adr28-churn-gate.md`).
The 256 term is therefore the measured headroom, not a safety factor.

Set a value to override; set `0` to run without a slab (chunks stay on the heap and holders
stage, exactly as before ADR-0028). "Set" means SET, including set to zero: the helper
tests for absent/empty rather than truthiness, because Helm coerces
`--set cluster.cacheSlabBytes=0` to the integer 0 and Go templates call 0 falsey — so a
bare `if` would silently re-derive a slab instead of disabling one.

**Why hugepages are the gate rather than `efa.enabled`.** ADR-0028 calls hugepages a
*precondition* and the arithmetic says why. Registration is per PAGE and repeated once per
RAIL over the same pages, so the cost is `bytes × rails / rate`: measured at
239-433 GB/s on hugepages versus 12-14 GB/s on 4 KiB pages. A 132 GiB slab on 32 rails is
~19 s of startup on 2 MiB pages and ~300 s on base pages — the second trips the startup
probe and CrashLoops the pod. So a slab is only defaulted on where `efa.hugepages` is set,
which is also the operator's signal that the node pre-reserves them.

⚠ **The node's hugepage reservation now scales with `memCapacity`.** The slab maps from the
same pool as the arenas, so the boot-time reservation must cover
`rdmaArenaBytes + 256 × chunkSize + the slab` — at `memCapacity` 128GiB that is ~140Gi,
against the 64Gi our EFA EC2NodeClasses currently reserve. `pacer.validateHugepages`
FAILS THE RENDER with the exact number rather than letting the request go unhonored (which
degrades to 4 KiB pages, silently invalidating the slab). Two supported ways out: raise the
node reservation, or cap the slab BELOW `memCapacity` and accept that the excess falls back
to the heap — a hot RDMA-servable subset, which is the right trade when hugepage *capacity*
rather than registration *time* is what binds.

`pacer_cache_slab_heap_fallbacks_total` is the runtime check; anything but ~0 means the slab
is smaller than the working set needs. The container memory limit already includes it
(`pacer.memoryLimit` adds it ONCE — all rails register the same pages, so 32 registrations
pin one slab, not 32).

## Rail and queue-pair shape

`cluster.rdmaRailWindow` bounds the WRITEs one EFA rail may have on the wire at once
(planning/19 D5.1). Empty/0 = unbounded, which is what every measurement through D5 step 0
ran at. Set a value to make per-rail depth explicit and sweepable; exhaustion is
backpressure (the serve waits), never an error, so it cannot push a fetch onto the gRPC
fallback. Read `pacer_rdma_rail{metric="writes_in_flight"}` alongside it: pinned at this
value means the window binds; well below means something upstream does.

`cluster.rdmaAffinity` pins each rail's RDMA completion reaper — and registers its arenas —
on that rail's own NIC NUMA node (planning/19 D5,
[ADR-0025](../adr/0025-rail-numa-placement-and-pinned-reapers.md)). Empty → on.
Set to `false` ONLY to reproduce the pre-D5 daemon: it is the control arm for the
~58-vs-15.7 GiB/s comparison, not a tuning knob. Placement is best-effort either way (a
host whose sysfs topology is unreadable simply runs unplaced), and the daemon logs which of
the two it got — read `node_local_rails` on the "RDMA arenas registered" line before
interpreting any throughput number.

`cluster.efaRails` is the number of EFA rails (devices) the transport brings up (A5
multi-rail). Empty/0 = ALL rails the node exposes (1 on r8gd, 32 on p5.48xlarge); set 1 to
force the single-rail behavior every pre-A5 benchmark measured. Arena memory does not scale
with rails (the byte budget is split across them, so the node keeps its concurrency instead
of dividing it — planning/19 D2).

`cluster.efaQpsPerRail` is SRD queue pairs per rail (A5 multi-QP). Empty/1 = one QP per rail
(the pre-multi-QP behavior); raise it (2/4/8) on high-bandwidth NICs where a single SRD QP
cannot saturate a rail — the holder round-robins its outbound WRITEs across this many QPs
per rail. The extra QPs share the rail's one PD/CQ/completion-pump, so the only per-QP cost
is a send queue.

## The startup probe budget is load-bearing

The daemon registers its RDMA arenas (ADR-0024) BEFORE it binds the admin listener, so
`/healthz` does not answer until that finishes — and `ibv_reg_mr` pins and page-tables
every byte, measured at ~2.5 GiB/s on p5.48xlarge (37 GiB over 64 MRs took ~15 s). Without
a `startupProbe`, kubelet's liveness probe kills the container after
`initialDelay + 3 × period ≈ 35 s`, so any arena past ~80 GiB CrashLoops forever with no
hint as to why (observed 2026-08-19 at `efa.pinnedPoolReservation=260Gi`). A `startupProbe`
suspends liveness until it first succeeds, so the budget in
[`templates/daemonset.yaml`](../../deploy/helm/pacer/templates/daemonset.yaml) — 5 s × 60 =
5 minutes, ~750 GiB of registration — is what makes a large arena deployable at all. Raise
`failureThreshold`, not the liveness delays: liveness must stay tight once the daemon IS
serving.

## See also

* [memory-model.md](memory-model.md) — how the arenas and the slab enter the cgroup limit.
* [delivery.md](delivery.md) — the client-side registered memory ADR-0030 inverts.
* [cache-and-disk-tier.md](cache-and-disk-tier.md) — `chunkSize`, which sizes the holder
  arena and the slab.
* ADRs [0018](../adr/0018-holder-driven-rdma-write-data-plane.md),
  [0021](../adr/0021-efadv-ibverbs-not-libfabric.md),
  [0022](../adr/0022-gpudirect-hbm-target-and-multi-rail-efa.md),
  [0024](../adr/0024-registered-arena-rdma-buffers.md),
  [0025](../adr/0025-rail-numa-placement-and-pinned-reapers.md),
  [0028](../adr/0028-cache-ram-tier-is-the-registered-arena.md),
  [0030](../adr/0030-delivery-registration-belongs-to-the-memory-owner.md).
