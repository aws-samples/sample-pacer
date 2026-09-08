> Design notes for the `delivery` keys in [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file keeps one short comment per key; the reasoning, the measurements and the failure history live here.

# Delivery into client-supplied memory (ADR-0026)

## What it is, and why it is off by default

Delivery into client-supplied memory (ADR-0026, planning/19 Track C C1): a cooperating client sends `x-pacer-target: shm:/<name>;offset=…;len=…` on a GET and the daemon delivers the bytes into that segment, answering with a header-only 200 — removing the HTTP/TCP leg that D5.3 measured at 87% of the daemon's throughput. A client that sends no header is unaffected, so this is safe to enable on a shared endpoint.

Off by default because it is a privilege, not a tuning knob: the daemon maps a segment a *client* named and pins its pages (and, on an EFA node, hands a peer an rkey into them). Two things must line up for it to work end to end:

1. `sharedShm` — the client pod and the daemon must see the SAME tmpfs, which means a hostPath, NOT an emptyDir: an emptyDir (even `medium: Memory`) is created per POD, so sharing one across pods is impossible and every target would look absent (a 4xx, not a silent wrong read). This mounts `shmHostPath` at /dev/shm in the daemon; the client pod must mount the same host path. Consequence to weigh: any pod that mounts it sees every segment on that node, so the segment-name validation and the pinned-byte quotas are what bound the exposure (ADR-0026 § New trust surface).
2. `pinnedReservation` — pinned client pages are invisible to `config.memCapacity`, exactly like `efa.pinnedPoolReservation`, so the container limit has to cover them or the pod OOMKills under load.

The rendered ConfigMap's own comment on `delivery.enabled` restates the same safety argument at the point the key is emitted: a cooperating client names a shm segment it owns and the daemon delivers into it, answering with a header-only 200; clients that send no `x-pacer-target` header are unaffected, which is why enabling this cannot break an existing workload. It does mean the daemon maps and PINS memory a client named, so it stays opt-in.

## Sharing the segment: `sharedShm`, `shmHostPath`, `shmDir`

`sharedShm` mounts the node's tmpfs at /dev/shm so client pods can share segments with the daemon — hostPath, for the reason above: an emptyDir cannot work here.

`shmHostPath` is the host path to mount. /dev/shm is already a tmpfs (~50% of RAM) on every supported distro; its size is the ceiling on concurrently live client segments, and exhausting it surfaces as the CLIENT's own allocation failing before any GET reaches the daemon.

`shmDir` is where `shm:/name` resolves inside the daemon. Empty → /dev/shm (where shm_open puts POSIX segments), which is what `sharedShm` mounts. The mount path follows this value, so the two cannot drift apart — set both to /dev/hugepages together (see hugepages below). The rendered ConfigMap's comment on `shm-dir` says the same thing more tersely: override only if the tmpfs the client and the daemon share is mounted elsewhere.

The DaemonSet template's comments on the `shared-shm` volumeMount and volume repeat and sharpen why, because getting either of these wrong fails silently rather than loudly. On the volumeMount:

> Client-memory delivery (ADR-0026): the NODE's tmpfs, where a client's shm segments live. It has to be a hostPath — an `emptyDir` (even `medium: Memory`) is created per POD and can never be shared with the client, so a delivery target would always look absent and every GET naming one would get a 4xx. The client pod must mount the SAME host path at the same place.
>
> The mount path FOLLOWS `shmDir`, because that is the directory the daemon resolves `shm:/<name>` under: mounting the segments anywhere else makes every target look absent. This is also what lets the segments be hugepage-backed — point both at a hugetlbfs mount (/dev/hugepages) and registration goes from 12-14 GB/s to 239-433 (ADR-0028's reg-timing spike) with no protocol change, because the daemon opens a path and never calls shm_open.

And on the `shared-shm` volume's hostPath:

> Default /dev/shm: on every distro this daemon runs on that is already a tmpfs sized ~half of RAM, so segments are RAM-backed with no extra plumbing. Point it at a dedicated host tmpfs instead if you would rather not share the node's default one — or at /dev/hugepages (with `shmDir` matching) for hugepage-backed client segments, which is the cheapest thing that moves per-request registration.

## Hugepage-backed client segments

Set `shmHostPath` AND `shmDir` to a hugetlbfs mount (/dev/hugepages) and the client's segments become hugepage-backed. It is the cheapest lever on the host-memory delivery path and it needs no protocol change, because the daemon resolves a segment name by open()-ing it under `shmDir` and never calls shm_open. `ibv_reg_mr` pins per PAGE, so ADR-0028's reg-timing spike measured registration at 12.3-14.4 GB/s on 4 KiB pages against 239-433 GB/s on 2 MiB — ~1.19 s versus ~15 ms to pin a 4 GiB window, and per-request registration was the binding cost of this whole design (ADR-0026 § Measured). Requirements:

* the node has boot-reserved hugepages (the s3-pacer EC2NodeClass userData reserves 2Mi pages; ADR-0028 needs them too);
* the CLIENT pod requests `hugepages-2Mi` covering its segments, because hugepages are charged to the cgroup that faults them and the client prefaults. The daemon maps pages that already exist, so it needs no hugepages request of its own — do NOT add one speculatively, since an unsatisfiable hugepages request makes Karpenter refuse to launch the node.

## The two quotas: `maxTargetBytes` and `pinnedBytesMax`

`maxTargetBytes` — per-request ceiling on client memory one GET may pin, as a size (e.g. 8GiB — a whole-checkpoint window). Empty → the daemon default (1GiB). Over it, the read is body-delivered rather than failed.

`pinnedBytesMax` — node-wide ceiling on concurrently pinned client memory. Empty → the daemon default (8Gi). Keep `pinnedReservation` in step with it — the render fails if it falls below this, because those pages are charged to the cgroup either way.

The rendered ConfigMap carries a shorter version of each rule at the point it emits the key. On `max-target-bytes`: "Per-request ceiling on client memory one GET may pin. Over it, the read is served as a body instead (never failed), so this bounds a client's mistake rather than its workload." On `pinned-bytes-max`: "Node-wide ceiling on concurrently pinned client memory. Independent of `cluster.rdmaArenaBytes` — those pages are the daemon's, these are clients' — and both have to fit the node, so raise `delivery.pinnedReservation` with it."

## Why the per-request ceiling ships at 4Gi

**4Gi, not the daemon's own 1GiB, and the reason is a property of published checkpoints rather than of any harness: a checkpoint's embedding is ONE tensor and it grows with the vocabulary.**

| Model | Embedding size | vs 1 GiB |
|---|---|---|
| Llama-3.1-8B | 128 256 x 4 096 x 2 B = 0.978 GiB | 2.1% UNDER 1 GiB |
| Llama-3.3-70B | 128 256 x 8 192 x 2 B = 1.957 GiB | DOUBLE it |
| Qwen3-235B | 151 936 x 4 096 x 2 B = 1.160 GiB | |

A span the client's reusable window cannot hold gets a one-off segment, so that tensor is offered as a single ~2 GiB target — and at 1GiB the daemon DECLINES it and the GET degrades to an ordinary body read. The whole acceleration path is lost on exactly the model the project's headline numbers use (planning/19 § "A quota finding that gates every rung above 8B"; bench/ladder/results/c4-dcp-hf-safetensors.md), and a bench overlay had to raise it by hand to measure a 70B rung at all.

WHY 4Gi is the number. It is ~2x the largest tensor measured, so one quota value outlasts several rungs instead of being re-tuned per model (a 405B-class hidden size takes the embedding past 2 GiB). And it costs the node NOTHING: the bound on pinned client memory is `pinnedBytesMax` (8Gi by default), which this is subordinate to — raising the per-request ceiling lets ONE GET take half the node-wide budget instead of an eighth, it does not raise the budget. Which is also why it is not 8Gi (what several bench overlays use): a per-request ceiling EQUAL to the node-wide one gives no isolation at all — one delivering GET could hold the entire budget and every concurrent one would be declined. 4Gi keeps at least two.

Raising this past `pinnedBytesMax` FAILS THE RENDER (`pacer.validateDelivery`): the node-wide gate is checked second, so the excess would be a ceiling the daemon can never honour — dead configuration that reads as a working one.

An operator who hits it can tell, and this is where to look: the daemon logs `client-memory delivery declined; serving the body instead` at WARN and counts **`pacer_delivery_rejects_total{reason="quota"}`** (on the Grafana dashboard's "Client delivery" row). Client-side the tell is the ABSENCE of the `x-pacer-delivered` response header — a defined answer, not an error.

## What the chart validates

Two helpers resolve the effective quotas to a NUMBER of bytes before anything compares them, because the two values are written in different dialects across this repo's overlays (`4Gi` here, `8GiB` elsewhere) — a string comparison would rank `8GiB` below `16Gi` lexically and pass a configuration that cannot work.

`pacer.deliveryMaxTargetBytes` resolves `delivery.maxTargetBytes`: the explicit value if set, else the daemon's own default restated in the template — `crates/pacer-daemon/src/delivery.rs`'s `DEFAULT_MAX_TARGET_BYTES` (1 GiB). The values file ships 4Gi, so the fallback branch fires only for an install that blanks it deliberately. It still has to be right: the guard's job is to compare the number the daemon will actually enforce, not the number the chart happens to have written down.

`pacer.deliveryPinnedBytesMax` does the same for `delivery.pinnedBytesMax`, restating `DEFAULT_PINNED_BYTES_MAX` (8 GiB) when it is left empty. This is the number that actually bounds what a client can make this daemon pin, which is why `pacer.validateDelivery` measures the per-request ceiling AND the cgroup reservation against it rather than against each other.

`pacer.validateDelivery` then refuses a delivery configuration whose quotas cannot mean what they say. Two checks, both about numbers that must move together and historically have not:

1. `maxTargetBytes > pinnedBytesMax`. `DeliveryQuota::reserve` tests the per-request ceiling FIRST and the node-wide total second, so a target between the two passes one gate and fails the other — the operator's per-request ceiling is dead configuration above the node-wide one, and it fails as an over-quota DECLINE (a body-delivered read), i.e. as the delivery path merely looking slow. This is the exact trap raising the quota for a 70B embedding walks into: `maxTargetBytes` is the knob the model forces you to touch, and it is not the one that bounds the node.
2. `pinnedReservation < pinnedBytesMax`. Pinned client pages are invisible to `config.memCapacity`, so `pacer.memoryLimit` adds `pinnedReservation` to the cgroup limit; if that is smaller than what the daemon will let clients pin, the limit is not a limit and the pod OOMKills under load — the planning/16 §4.5 failure with a different class of pages. values.yaml has said "should match" since ADR-0026 landed; this makes it checkable.

Same reasoning as `validateScatter` and `validateEfaAccess`: a configuration that cannot work should fail where the operator is still looking at it, not become a slow path nobody can attribute.

## Remote write (planning/19 C3): the holder writes the client directly

planning/19 **C3 (parallel fill)**: have a remote chunk's HOLDER write it straight into the reading client's own registered window, instead of routing it through the node the client is talking to.

Only affects `nic:` targets (ADR-0030 — memory the CLIENT registered). On `shm:` the daemon registered the window itself and holders have written into it directly since ADR-0026; there is no hop left to remove.

WHAT IT CHANGES. Off, a remote chunk lands in the reading node's memory and is WRITTEN into the client from there — correct, but two hops, and every byte of a checkpoint crosses ONE node's rails however many holders are serving it. On, N holders write one client buffer at once, which is the only shape in which a single read can approach the fabric's own ceiling (Track H measured 360.709 GiB/s into HBM; one node's share of that is a fraction).

WHY IT IS OFF BY DEFAULT, and it is NOT a safety valve. Both paths deliver the same bytes with the same integrity guarantee: a holder that cannot reach the client — no announce, no address handle, geometry disagreement — streams the body instead and the reading node writes it, i.e. the OFF path is exactly the ON path's fallback, reached per chunk. So nothing about enabling it can fail a read that would otherwise have succeeded. It is off because it is the **control arm**: the hop it removes is what C3 exists to remove, and "how much is that hop worth" has no answer unless both arms can be run on one image.

THE ONE REAL COST, and what to watch. Every HOLDER now pays first contact per client endpoint — its own announce SEND, its own `ibv_create_ah` per rail — where before only the reading node did. A fleet of N holders serving one loader runs N announces per client rail instead of one. It is paid once each and the retry ladder absorbs it, but it is the thing to look at if a run is slower rather than faster: `pacer_delivery_declines_total{reason="writer_not_installed"}` rising is that race, and `{reason="not_announceable"}` is a client whose receive pump is starved.

READING THE RESULT. `pacer_delivery_chunks_total{source="peer_rdma"}` is what this moves — holder-written chunks — against `{source="peer_stream"}`, which is a holder that declined and had its bytes written by the reading node. On the OFF arm every remote chunk of a `nic:` delivery is `peer_stream` by construction, so the split between those two series IS the arm.

The rendered ConfigMap's comment on `remote-write` restates the framing at the point it is emitted: off by default as the arm's control — both paths deliver the same bytes with the same integrity guarantee, since a holder that cannot reach the client streams instead. No effect on `shm:` targets, where the daemon registered the window and holders have always written into it directly.

## GPU targets: nothing to configure, and that is the design

**A delivery into GPU memory needs no chart setting at all.** This section documented two — `delivery.gpuTargets` (put `NVIDIA_VISIBLE_DEVICES=all` and `NVIDIA_DRIVER_CAPABILITIES=compute,utility` on the daemon so the runtime injects `libcuda.so.1`) and `delivery.gpuHostIpc` (share the host IPC namespace) — and **both were removed on 2026-09-08** with the scheme that needed them.

They existed because ADR-0027 had the daemon *import* a client's `cudaIpcMemHandle_t`, which requires the daemon to see the device the client allocated on and possibly to share its IPC namespace. That mechanism cannot work: a `cuIpcOpenMemHandle` pointer is not a provenance `cuMemGetHandleForAddressRange` will dma-buf export, and with `nvidia_peermem` unable to load on EFA dma-buf is the only way to register device memory with the NIC (`bench/ladder/results/c2-vmm-dmabuf-provenance.md`). ADR-0030 moved registration to the memory's owner instead: the CLIENT registers its own HBM on its own rails and sends a `nic:` token, so a holder writes into device memory the daemon never maps, never sees and needs no driver for.

Three consequences worth stating, because each removes something an operator used to have to reason about:

- **No GPU visibility on the daemon.** No `NVIDIA_*` env, no dependence on the container toolkit's `accept-nvidia-visible-devices-envvar` (which the GPU Operator can turn off and CDI-only setups ignore), and no per-node verification that a driverless pod can `dlopen` libcuda.
- **No `hostIPC` on the DaemonSet.** The pod spec cannot render it any more. That namespace boundary was the one real cost of the old shape, and it is now unconditionally kept.
- **The GPU requirement moved to the client**, which is where it belongs: a `nic:` client needs an EFA device and the memory to register, and it already has both.

An overlay that still sets either key now **fails the render** (`values.schema.json` is `additionalProperties: false`) rather than configuring nothing.

## Delivery parallelism

Windows delivered concurrently per GET. Empty → the daemon default (64). NOT `config.fillParallelism`: that bounds an ORDERED body stream, where look-ahead past the reorder window is wasted. Delivery windows are disjoint destinations in the client's buffer, and the workload this exists for is one GET for a multi-GiB checkpoint — thousands of windows, where a bound of 8 would serialize a fabric-limited transfer into ~N/8 rounds.

The rendered ConfigMap's comment on `parallelism` draws the same distinction at emission time: distinct from `policy.fill-parallelism` on purpose — that one bounds an ordered body stream; these are disjoint destinations with no ordering constraint, and one checkpoint GET is thousands of them.

## The two memory reservations

`pinnedReservation` — client-pinned memory to ADD to the container memory limit when `delivery.enabled` — the analogue of `efa.pinnedPoolReservation` for pages the daemon pins on a client's behalf. Should match `pinnedBytesMax` (or the daemon default of 8Gi when that is empty); the render fails if it is smaller, since the cgroup limit would then not cover what the daemon admits.

`workingSetReservation` — the delivery path's OWN in-flight bodies, ADDED to the container memory limit — a second footprint, independent of the client pages above. Empty → DERIVED as `parallelism x config.chunkSize` (1 GiB at the shipped defaults), which is the bound in the code: `Proxy::run_delivery` buffers `min(parallelism, windows)` chunk resolutions for one span and each holds a chunk body. Set 0 for the pre-fix arithmetic (a control arm), or a size to override.

⚠ THE DERIVED VALUE IS A FLOOR, NOT THE MEASURED WORKING SET, and the gap is large: a 70B cold load OOMKilled the daemon (exit 137) with a 24 GiB tier against a 48Gi base limit — the cache tier, both arenas and the pinned client pages all inside their budgets — while the SAME tier's warm pass and body control completed. 96Gi survives it. Read the sizing rule above `resources:` in values.yaml before enabling delivery on a large checkpoint: the missing term scales with the bytes a load moves, which no template can know.

The `pacer.deliveryWorkingSetBytes` helper computes that derived floor, and its own comment carries the same caveat further: it is the fourth term `pacer.memoryLimit` adds for something `config.memCapacity` cannot see, alongside the arenas, the ADR-0028 slab and ADR-0032's staging budget — and unlike those three it is a FLOOR rather than the working set. The measured working set is much larger and is NOT derivable from values at all.

What IS derivable is the one bound in the code: `Proxy::run_delivery` buffers `min(delivery.parallelism, windows)` chunk resolutions for ONE span, and each in-flight window holds a chunk body. So `parallelism x chunkSize` is the heap the delivery fan-out can hold at once, and it tracks both knobs that set it — an install that raises `delivery.parallelism` to 256 at a 64 MiB chunk is asking for 16 GiB of in-flight bodies, which is worth having in the limit even though it is not the whole story.

Resolution order, the same shape as `pacer.cacheSlabBytes` and `pacer.scatterStagingBytes`:

1. `delivery.workingSetReservation` set explicitly → that value, including `0` for "add nothing" (a control arm has to be able to ask for the pre-fix arithmetic).
2. otherwise → DERIVED as above, restating `crates/pacer-daemon/src/delivery.rs`'s `DEFAULT_DELIVERY_PARALLELISM` (64) for the same reason `pacer.submitQueueThreshold` floors foyer's own `flushers` at 1: the chart has to budget the number the daemon will actually run, not the empty string in the values file.

Only computed when `delivery.enabled`: with ADR-0026 off no client names a target, no window is ever buffered, and the body path's own look-ahead is `config.fillParallelism` — a different bound that the base `resources.limits.memory` has always covered.

`pacer.memoryLimit` adds both terms when delivery is on. On `pinnedReservation`, its comment reads: "Client-memory delivery (ADR-0026) pins a second, independent class of pages: a window of a segment a CLIENT allocated, mapped and registered by the daemon for the life of a request. Those pages are equally invisible to memCapacity and to the EFA arena accounting, so `delivery.pinnedReservation` is added on the same terms. Both can be on at once, and then both are added." On `workingSetReservation`: "The delivery path's own in-flight bodies, on top of the client pages it pins — a second, independent footprint that this derivation was missing until a 70B arm OOMKilled the daemon (exit 137) with the cache tier and the pinned pools all inside budget. Plain heap, not pinned, and a FLOOR rather than the measured working set."

## See also

- [memory-model.md](memory-model.md) — how the arenas, the ADR-0028 slab, the write-scatter staging budget and this section's two reservations all add up against `resources.limits.memory`.
- [efa-and-rdma.md](efa-and-rdma.md) — the peer plane `remoteWrite`'s holder-direct writes depend on, and the one every GPU delivery now goes through.
- ADR-0026 — Client-supplied target memory
- ADR-0027 — GPU memory delivery targets
- ADR-0030 — Delivery registration belongs to the memory owner
