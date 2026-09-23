# ADR-0027: A client's delivery target may be GPU memory, named by a CUDA IPC handle

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-20 · Status: **Goal SHIPPED by
[ADR-0030](0030-delivery-registration-belongs-to-the-memory-owner.md)'s mechanism; this
ADR's own mechanism was superseded 2026-08-23 and REMOVED FROM THE TREE 2026-09-08**
(§ Removal). A delivery target may be GPU memory — that stands and is measured. The
`cuda-ipc:` scheme, the daemon's CUDA, and the chart keys that fed it are gone.

> **Why the mechanism fell.** A CUDA IPC handle is serializable, which is what this ADR
> chose it for, but a `cuIpcOpenMemHandle` pointer is not one of the provenances
> `cuMemGetHandleForAddressRange` will dma-buf export — and with `nvidia_peermem` unable to
> load on EFA, dma-buf is the only way to register device memory at all. The *other*
> exportable provenance does work from a non-owning process (measured, `fd=37`), but it
> requires `cuMemCreate`, and PyTorch tensors are `cudaMalloc` memory with
> `allowed_handle_types=0`. So no serializable handle lets the daemon register a real
> tensor. ADR-0030 moves registration to the client and ships a NIC-scoped token instead.
> Evidence: `bench/ladder/results/c2-vmm-dmabuf-provenance.md`.

The GPU half of ADR-0026, and what finally gives [ADR-0022](0022-gpudirect-hbm-target-and-multi-rail-efa.md)'s
HBM target an actual consumer.

## Context

Three measurements now point at the same place.

- **H2 measured 360.709 GiB/s** into GPU HBM across 32 rails on p5.48xlarge —
  6.2× the ~58 GiB/s host-memory aggregate, and within 0.2 % of the external
  calibration point planning/18 used. Host DRAM *was* the ceiling; which memory the
  NIC talks to is the binding variable.
- **H0 proved the machinery on this AMI** (planning/21, 8/8): the EFA driver exposes
  `ibv_reg_dmabuf_mr`, a CUDA allocation yields a registrable dmabuf fd via
  `cuMemGetHandleForAddressRange`, and a cross-node one-sided WRITE lands in a peer's
  HBM MR.
- **D5.3 showed the daemon's own ceiling is the client-delivery leg** (27.502 of
  31.589 GiB/s), so a faster fabric alone changes nothing for a consumer that reads
  over TCP.

Put together: the fabric can move 360 GiB/s into HBM, we can register HBM, and the
thing standing between that and a model loader is *where the bytes are asked to land*.
ADR-0026 answers that for host memory. A checkpoint loader does not want host memory —
it wants tensors in HBM, and every byte that transits host DRAM on the way is a copy it
pays for twice (NIC→DRAM, DRAM→HBM).

The obstacle is naming: a dmabuf fd cannot travel in an HTTP header, and passing fds
means `SCM_RIGHTS` over a unix socket, which would fork ADR-0026's transport.

## Decision

**A delivery target may be GPU memory, named by a CUDA IPC handle in the same
`x-pacer-target` header.**

1. **CUDA IPC is the handle format**, because it is *serializable*.
   `cudaIpcGetMemHandle` yields a fixed-size opaque handle the client can base64 into a
   header; the daemon calls `cudaIpcOpenMemHandle` to obtain a device pointer into the
   client's own allocation. No fd passing, no unix socket, no second listener — the
   protocol, the fallback and the integrity rules of ADR-0026 all carry over unchanged:

   ```
   x-pacer-target: cuda-ipc:<base64 handle>;offset=0;len=16777216;device=3
   ```

2. **The daemon registers it as an MR the same way H0 validated.** From the mapped
   device pointer, `cuMemGetHandleForAddressRange` yields a dmabuf fd and
   `ibv_reg_dmabuf_mr` registers it on the rail's PD with the EFA-valid access set
   (`LOCAL_WRITE | REMOTE_READ | REMOTE_WRITE`, never `PERMISSIVE` — spike finding 7).
   The holder then WRITEs into the client's HBM directly.
3. **Tier behaviour mirrors ADR-0026 point 4, with the local case as `cudaMemcpy`:**

   | chunk location | how it lands in the client's HBM |
   |---|---|
   | remote peer's cache | holder-driven **RDMA WRITE**, peer NIC → client HBM (no host DRAM on the path) |
   | local daemon RAM | `cudaMemcpy` H2D from the cached `Bytes` |
   | local NVMe tier | today: read → `cudaMemcpy`; with GDS (planning/20): NVMe → HBM, host RAM off the path |

4. **Device identity must be a UUID, not an ordinal** (amended 2026-08-22 after the
   first hardware gate; the original text said `device=3`, an ordinal, which cannot
   work). A CUDA ordinal is scoped to the container that enumerates it: a client
   holding `nvidia.com/gpu: 1` sees exactly one device and calls it 0, while the
   daemon — which sees all of them — has its own 0. The gate's
   `cuIpcOpenMemHandle` failed `invalid argument` because the daemon retained the
   primary context for *its* device 0, which need not be the GPU the client
   allocated on (`bench/ladder/results/c2-gpu-gate-run1.md`).

   So the descriptor carries `cuDeviceGetUuid` and the daemon maps UUID → its own
   ordinal. That is also what makes the affinity rule below implementable, since a
   UUID resolves to a PCI bus ID and therefore to a NUMA node.

   **Both pods must mount the HOST's `/dev/shm`** (established 2026-08-22 after three
   failed gates). A CUDA IPC handle references a shared-memory ref-counter file there,
   and containerd gives each pod sandbox its own tmpfs — so without the mount the two
   sides look at different directories and `cuIpcOpenMemHandle` fails
   (`invalid device context` when only one side has the host's, `invalid resource
   handle` when both have their own). `delivery.sharedShm` already does this for the
   daemon because the shm delivery path needs it; a GPU client pod needs it too, and
   it must be a **hostPath, never an emptyDir** — an emptyDir shadows the host's and
   breaks IPC identically. See LMCache's operator guide, which documents the same
   requirement: https://docs.lmcache.ai/mp/operator.html
   `hostIPC: true` is NOT a substitute on this platform: a gate ran with `hostIPC` and
   `hostPID` on both pods, without the mount, and still failed.

5. **Device affinity is part of the target, and it is not cosmetic.** The client names
   its device; the daemon must WRITE from a rail whose NUMA node matches that GPU's,
   because ADR-0025 measured +65 % from getting exactly this kind of placement right on
   the host side. A mismatch is allowed (it still works) but must be logged, since it
   silently costs inter-socket bandwidth.
6. **HBM is not a cache tier.** The client's buffer is a *destination*, never storage
   the daemon owns, evicts, or reuses. The daemon deregisters and closes the IPC mapping
   when the request completes (ADR-0026 point 7), so no daemon state outlives a delivery.
7. **Same-node only.** CUDA IPC handles are meaningful within one host's driver context.
   A cross-node target would require exporting to a different machine's GPU, which is
   not what this is: the *peer* is remote, the *client* is local.

## Consequences

- **The full path a loader gets** is: peer's cache → peer's registered host arena →
  [one RDMA WRITE] → the loader's HBM. Host DRAM appears once, on the *holder*, and only
  because the holder's bounce copy survives (ADR-0024 kept it deliberately at ~4 %). The
  requester's DRAM and the whole TCP/veth leg are gone.
- **The realistic bar for this path is 107 GiB/s, not 360.** H2b measured the ceiling to
  be **per leg**: 57 host→host, **107 host→HBM**, 361 HBM→HBM. A holder that sources from
  its host-memory cache and writes into client HBM is the *middle* case, so C2's target is
  ~107 — still 3.4× the 31.589 GiB/s TCP path, and 3.9× the daemon's current 27.502.
  Reaching 361 additionally requires the holder's **source** to be HBM or NVMe-direct,
  which is track N's half. Quoting 360 as this ADR's target would be wrong.
- **This is Phase 4's F2 and the destination half of track N.** F2 ("GPU→HBM restore")
  and N1 ("holder source = HBM") converge here rather than being separate builds: N
  removes host DRAM from the *source* side, this removes it from the *destination* side,
  and they compose.
- **It gives H1 its scope.** H1 was "an HBM buffer tier behind the S1 seam" with no
  consumer; the consumer is a client-supplied target. That also settles what H1 should
  NOT be: the daemon does not need to own an HBM arena to serve HBM clients.
- **CUDA becomes a daemon-side runtime dependency** for this path — `dlopen`ed, exactly
  as planning/20's probe does it, so a node without CUDA keeps building and running and
  simply refuses `cuda-ipc` targets (falling back to a body-delivered read, per
  ADR-0026 point 8).
- **A malformed or foreign IPC handle is an attack surface.** `cudaIpcOpenMemHandle` on
  an untrusted handle maps memory the daemon did not allocate; combined with an rkey
  handed to a holder, that is a write primitive. Same-node + same-tenant is the
  assumption (ADR-0018), and the daemon must reject a handle it cannot attribute to the
  requesting client rather than trusting the header.
- **The daemon needs every GPU VISIBLE but must allocate NONE** (2026-08-22; an
  earlier version of this bullet said "request `nvidia.com/gpu: 8`", which is
  wrong — a DaemonSet holding every GPU makes them unallocatable and the training
  pods PACER exists to serve cannot schedule at all).

  Two facts force visibility: `libcuda.so.1` is injected only into containers the
  NVIDIA runtime considers GPU-enabled, and CUDA IPC requires the *importing*
  process to have the exporting allocation's device visible — so a daemon that can
  see one GPU silently serves only the clients that allocated on that one, which
  reads as "GPU delivery is flaky" rather than as a scheduling mistake.

  The way to get visibility without consuming the resource is the env pair the
  container runtime keys on, with **no `nvidia.com/gpu` request at all**:

  ```yaml
  env:
    - { name: NVIDIA_VISIBLE_DEVICES, value: all }
    - { name: NVIDIA_DRIVER_CAPABILITIES, value: compute,utility }
  ```

  The scheduler then allocates zero GPUs while the runtime injects the driver and
  every device node. **Verify before relying on it**: this works only while the
  toolkit accepts env-based visibility (`accept-nvidia-visible-devices-envvar`,
  default true; the GPU Operator can disable it, and CDI-only setups ignore it).
  On the cluster this was validated on the `ClusterPolicy` sets no `toolkit.env` override, so the default
  applies — but that is a config read, not a test, and the two-minute check on the
  first GPU node is `ls /dev/nvidia*` plus `python3 clients/python/pacer_cuda.py`
  in a pod with those two vars and no GPU request.

  **If a cluster forbids it, the fallback is better architecture anyway** and is
  worth doing on its own merits: have the CLIENT export the dma-buf (it already has
  CUDA) and hand the fd to the daemon over a unix socket in the same shared
  hostPath the shm path uses, keyed by a token the header carries. That removes the
  CUDA dependency from the daemon completely, and removes this ADR's last
  consequence too — the daemon would register an fd a client deliberately gave it
  instead of opening an IPC handle it cannot attribute. The open question it
  introduces is the *local* window: with no CUDA the daemon cannot `cuMemcpyHtoD`,
  so it would have to post a loopback RDMA WRITE from its own host arena into the
  client's MR (H2b's 107 GiB/s host→HBM leg, but self-addressed SRD is UNTESTED —
  H0 never tried it). That is a spike, not a guess.

  `pacer_cuda.device_count()` is in the client shim so a bench log records what the
  client could see.
- **Unmeasured.** Every number above is transport-only (H2) or host-side (D5.3). What
  this ADR predicts — a loader pulling shards into HBM at multiples of 31.589 GiB/s —
  has not been run, and the honest gate is ADR-0026's host arm first: if naming host
  memory does not beat the TCP path, naming GPU memory will not either.

## Status / relationship to other ADRs

- **ADR-0026** — the protocol, compatibility gate, integrity rule and lifetime
  contract. This ADR changes only the handle format and the registration call.
- **ADR-0022** — its HBM-target half is now *scoped*: the target belongs to a client,
  not to a daemon tier. Its multi-QP half stays refuted (planning/18 RESULT 2).
- **ADR-0025** — point 4 is ADR-0025's finding applied to GPUs: place the rail against
  the consumer's NUMA node, not arbitrarily.
- **planning/20 (track N)** — GDS turns the local-NVMe row of point 3 into
  NVMe → HBM with host RAM off the path.
- **planning/21 (H0/H2)** — the feasibility and the ceiling this ADR is built on.

## Revision (2026-08-22): the client exports the dma-buf, the daemon never opens a handle

Measured on one p5.48xlarge (`bench/ladder/results/c2-gpu-gate-run1.md`). The
decision — a client may name device memory and the daemon delivers into it — **stands**.
The mechanism above does not, for one hardware reason:

> `cuMemGetHandleForAddressRange(DMA_BUF_FD)` succeeds on a range from `cuMemAlloc`
> (`fd=36` for a 64 MiB window) and fails `invalid argument` on the *same window* reached
> through `cuIpcOpenMemHandle` — with `flags=0` and with
> `CU_MEM_RANGE_FLAG_DMA_BUF_MAPPING_TYPE_PCIE`, at a base that is both host-page and
> GPU-page aligned.

This is the API's documented rule rather than a platform quirk: it lists the provenances
it supports — `cuMemAlloc`, `cuMemAddressReserve` fully mapped by `cuMemMap`, and
`cuMemAllocHost`/`cuMemHostAlloc` — and an IPC-imported range is none of them. Nor is
there a peer-memory escape: `nvidia_peermem` ships on the AMI but will not load, because
upstream `ib_core` does not export `ib_register_peer_memory_client` (zero hits in
`/proc/kallsyms`) — that API is an out-of-tree patch AWS's `efa` does not carry. **On EFA,
dma-buf is the only way to register device memory with the NIC.**

The daemon therefore cannot turn an opened IPC handle into the fd `ibv_reg_dmabuf_mr`
needs. Everything else in the chain was proven working first, so this is the residual
constraint and not a symptom: cross-pod IPC opens correctly, the UUID→ordinal mapping
resolves (matched ordinals 1, 3, 4 while the client called it 0), and a daemon sees every
GPU while allocating none.

**The descriptor changes** from `cuda-ipc:<handle>;device=<uuid>` to naming a
client-exported fd: the client owns the allocation, so the client exports, and the daemon
registers what it is handed. How the fd travels is **an open decision**, and the
client-facing API is identical in all three candidates — `DeviceTarget(nbytes)` plus a
header, with the library doing the export — so this is a pod-contract and privilege trade
rather than a developer-experience one:

| | client | daemon needs | data-path cost |
|---|---|---|---|
| **`pid;fd` + `pidfd_getfd`** | header only | `CAP_SYS_PTRACE`; `hostPID` on client pods | none |
| **`SCM_RIGHTS`** on a hostPath socket | one connect + `sendmsg`, inside the library | no privileges | none |
| **daemon HBM slab + `cuMemcpyDtoD`** | header only, pure IPC handle | HBM per GPU | one extra D2D copy |

Recommended: `pidfd_getfd` — the only one that is literally "allocate, name it in a header,
done" at zero data-path cost, and `hostPID` is already normal for pods doing GPU IPC. An fd
cannot travel in a header as a bare number, since it indexes one process's descriptor
table; `pid;fd` works because the daemon re-derives its own descriptor from the pair.
`SCM_RIGHTS` is the fallback if `CAP_SYS_PTRACE` on the daemon is unacceptable. The third
option is the only one needing no fd at all, and is the one to take if neither privilege nor
socket is wanted — it is allowed precisely because `cuMemcpyDtoD` *is* legal on
IPC-imported memory, which is what LMCache-style consumers rely on.

**This deletes the whole failure class the gate ran into**, which is the strongest
argument for it: no IPC open, so no CUDA in the daemon at all, no `/dev/nvidia-uvm` in the
daemon's container (the DaemonSet starts before those nodes exist and a container's `/dev`
is fixed at creation), no `hostPID: true` on the client (required, empirically, only for
the daemon to open a handle), and no device identity to resolve — the fd already names
the memory.

Two consequences to carry forward:

- **Point 4 (rail↔GPU affinity) needs another source of device identity.** With no UUID
  in the descriptor, affinity must come from the dma-buf itself or from the client stating
  its device. Unresolved; it only matters for placement, not correctness.
- **The local (non-peer) path still has no answer.** With no CUDA the daemon cannot do the
  H2D copy for a local hit, so a local window needs a loopback RDMA WRITE — untested on
  EFA and the first thing to measure next.

`crates/pacer-daemon/src/cuda.rs` becomes dead weight for delivery under this revision.
It is kept until the fd path is measured, because it is what established the constraint —
and because `cuMemGetHandleForAddressRange` is still the right call on the *client* side.

## Removal (2026-09-08): the condition above was met, so the mechanism is out of the tree

The revision kept `cuda.rs` "until the fd path is measured". **It has been measured, four
times over** — the C2 token gate (`results/c2-token-gate.md`, which passed with
`delivery.gpuTargets` *off*), C4 on published safetensors, C5 into HBM, and the vLLM
serving arm — all of them ADR-0030 tokens, none of them this ADR's scheme. So the code
that implemented it is removed rather than carried.

**The decision this ADR records still stands and is shipped**: a client's delivery target
may be GPU memory. Only the *mechanism* is gone, and it was already marked superseded at
the top of this file since 2026-08-23. What went:

| removed | why it existed |
|---|---|
| `crates/pacer-daemon/src/cuda.rs` (678 lines, `dlopen`ed driver FFI) | importing a client's IPC handle |
| `TargetMemory::CudaIpc`, `DeliveryTarget::Gpu`, the `cuda-ipc:` scheme and its `device=` parameter | the descriptor |
| `DeviceUuid` + the base64 handle decoder | the descriptor's two fields |
| `EfaRdmaTransport::register_client_dmabuf` | registering the imported range — the call that cannot work |
| `delivery.gpuTargets`, `delivery.gpuHostIpc`, and the DaemonSet's `hostIPC: true` | giving the daemon the driver, the devices and the namespace to do it |
| `clients/python/pacer_gpu_gate.py`, `run-c2-gate.sh`, `c2-gate-pod.yaml`, `c2Gate` values, and the `gate` subcommand of the driver that ran them | the gate that exercised it |

That driver itself was **kept** — the rest of it is the single-GPU-node fleet plumbing every
other GPU arm depends on — and was renamed on 2026-09-08 from `bench/ladder/c2-gpu-gate.sh`
to `bench/ladder/c2-gpu-fleet.sh`, since a name
promising a `gate` that no longer exists reads in a run-book as if it still proved
something. The old path survives for one transition as a shim that execs the new one.

Three things deliberately **kept**:

- **`clients/python/pacer_cuda.py`, minus its `descriptor()`.** It is the frameworkless HBM
  allocator `pacer_ipc_probe` and `pacer_vmm_probe` are built on, and those probes are the
  evidence for the constraint in the revision above. Deleting them would delete the reason.
- **Every `results/*.md` under `bench/ladder/`.** `c2-gpu-gate-run1.md` and
  `c2-vmm-dmabuf-provenance.md` are why this decision looks the way it does.
- **`TargetRejection::Unsupported`.** ADR-0027's arm was its first user; ADR-0030's
  "no usable RDMA path to this token right now" is its current one, and it still degrades
  to a body rather than failing a read (ADR-0026 point 8).

Two of the revision's "consequences to carry forward" are now closed by that history
rather than by this change: point 4's device identity comes from the client's own rail
choice (the token names one rail per registration, and the client picks the one PCIe-local
to the tensor), and the local-hit question was answered by the loopback WRITE gate
(`results/c2-loopback-gate.md`, GO).

**Compatibility.** `cuda-ipc:` is no longer parsed, so a client still sending one gets
`InvalidRequest` naming the scheme — not a silent reinterpretation, which is the one
answer it must never get. `rejects_malformed_descriptors` asserts exactly that, and a
`device=` parameter on any scheme is likewise a parse error rather than an ignored field.
No shipped loader sends either: every one in `clients/python/` is `nic:` or `shm:`.
