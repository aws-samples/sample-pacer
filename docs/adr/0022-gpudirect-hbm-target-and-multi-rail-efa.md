# ADR-0022: GPUDirect HBM-target RDMA WRITE + multi-rail EFA (A5 scope)

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-21 · Status: Accepted (direction) — **implementation deferred to
phase A5, activation benchmark-gated per ADR-0008/0010** · Amends the GPUDirect
stance in planning/04 §4 · Extends ADR-0018 (data plane) and ADR-0004 (nodepool)

## Context

Two capabilities are named but unbuilt in the shipped transport, and the docs
disagree about one of them:

1. **HBM-target WRITE.** planning/04:55 states "**GPUDirect RDMA is irrelevant to
   us** … Our path is NVMe → host RAM → EFA." ADR-0018 point 5 later walked this
   back: buffers are "tier-agnostic … GPUDirect (WRITE straight into requester HBM
   on p5) is a later registration-flag change, not a redesign." The built code
   sides with planning/04: the buffer pool allocates host `Box<[u8]>` and registers
   it via `ibv_reg_mr` ([pacer-transport/src/efa/pool.rs:50-57]); the data path is
   NVMe → host RAM → EFA ([efa/mod.rs:13-18]); there is no CUDA / dmabuf /
   `ibv_reg_dmabuf_mr` / FI_HMEM path anywhere in the crate.

2. **Multi-rail.** The transport opens exactly one device —
   `ibverbs::devices().iter().next()`, first in the list, no index, one PD, one SRD
   QP, one CQ ([efa/context.rs:160-168], [efa/completion.rs:10]). planning/04:48
   already records that p5 has 32 EFA cards and multi-card instances "can bond
   several," but no ADR ever designed it. On a single rail an r8gd tops out at
   4.66 GiB/s (A4, planning/14); on p5 a single rail leaves ~31/32 of the NIC idle.

The A4 exit gate (planning/14) proved the *shape* of holder-driven one-sided WRITE
(ADR-0018) on a single host-RAM rail. The flagship PACER story — S3 → peer NVMe →
**remote GPU HBM**, with host RAM and CPU off the path on both ends — needs both
capabilities above, and neither exists.

## Decision

**Adopt HBM-as-target-MR as the direction, and supersede planning/04's "GPUDirect
is irrelevant" stance.** ADR-0018's tier-agnostic framing is correct: the
holder-driven WRITE targets a *registered memory region*; whether that region is
host DRAM or GPU HBM is a property of the requester's buffer registration, not of
the data-plane shape. The holder is unchanged. Concretely:

1. The requester-side buffer pool (ADR-0018 point 1) gains an **HBM tier**: buffers
   backed by GPU memory and registered via **`ibv_reg_dmabuf_mr`** (dmabuf fd
   exported from the CUDA/HIP allocation), not `ibv_reg_mr` over host heap. Access
   flags unchanged (`LOCAL_WRITE | REMOTE_READ | REMOTE_WRITE`, no atomics — the
   ADR-0018 finding-7 constraint holds identically for HBM MRs).
2. The holder path is **byte-for-byte unchanged** — it WRITEs into
   `(buffer_addr, rkey, length)` regardless of the target's physical tier. This is
   the "registration-flag change, not a redesign" ADR-0018 promised, made explicit.
3. **Multi-rail** (ADR-0004 nodepool): the transport enumerates all EFA devices on
   the node instead of `.next()`, with a per-rail PD/QP/CQ and chunk- or
   slot-striped assignment across rails. A new `PACER_EFA_RAILS` knob (default: all
   available) caps it. This is a genuine transport extension, not a flag.

   **Implementation status (2026-08-18): BUILT** (`crates/pacer-transport/src/efa/`),
   ahead of the HBM items because the p5 rung re-runs needed it. Shape as designed,
   with the design detail the build fixed: an rkey is only valid at the protection
   domain that issued it, so rails pair **by index** across nodes — the handshake's
   `EfaEndpoint` carries every rail's endpoint (`rail_endpoints`, field 1 stays
   rail 0 for wire compat), the fetch pins `(rail, rkey, addr)` in `RdmaBuffer.rail`,
   and the holder posts from its same-index rail. Chunk-level striping (round-robin
   over usable rails per fetch; no intra-chunk split), per-rail completion pumps and
   health (a dead rail drops out, the others keep serving; rail 0 failing = the
   gRPC-only capability-probe result), pool slot totals DISTRIBUTED across rails
   (pinned memory does not scale with rail count), all-rail AH eviction on
   completion error (a peer restart invalidates every rail at once). New gauge
   `pacer_rdma_rails`. Hardware validation: the p5 rung re-runs (this session's
   ladder work) are the multi-rail A/B.

   **Multi-QP-per-rail refinement (2026-08-18):** the p5 A/B surfaced that a
   single SRD QP tops out well below a 100 Gbps rail's line rate (~75 Gbps peak
   on a completely clean fabric — 0 retransmits/timeouts — so it is a per-QP
   send-pipeline ceiling, not fabric loss); one QP per rail therefore cannot
   saturate a p5 rail. Each rail now brings up `PACER_EFA_QPS_PER_RAIL` SRD QPs
   (default 1 = the original behavior) sharing the rail's one PD, one CQ, and
   one completion pump, and the holder round-robins its outbound WRITEs across
   them (`EfaContext::post`). Only the SENDER's QP count scales throughput —
   SRD is connectionless and a one-sided WRITE places data by rkey+addr, so the
   rail still advertises a single endpoint (`qps[0]`) and the wire protocol /
   handshake are unchanged; the peer needs only one valid destination QP. Pinned
   memory is unaffected (the buffer pools are per-rail, not per-QP); the only
   per-QP cost is a send queue. The CQ is sized `CQ_DEPTH × qps_per_rail` so
   every QP keeps its completion headroom.

**Both land in a new phase A5** (see roadmap), *after* B4 ships the scaling exit
gate on the host-RAM single-rail transport. B4 does not block on either.

## Feasibility is not assumed — A5 opens with a spike

Whether HBM-target is truly "just a registration-flag change" is **unproven on our
AMI/driver**. A5 begins with an A0-style decisive probe before the full build,
answering:
- Does the EFA driver on the `s3-pacer` AMI support `ibv_reg_dmabuf_mr`
  (needs amzn-drivers ≥ r1.6.0 per planning/04:15) with GPUDirect enabled?
- Can PyTorch hand us a registrable device pointer / dmabuf fd for the target
  tensor buffers, or must we stage through our own CUDA allocation?
- Does a cross-node WRITE into a peer's HBM MR actually complete on p5/p5en EFA
  (re-run the ADR-0018 capability probe, finding 6 symmetric-AH included)?

GO/NO-GO on that spike gates the A5 build, exactly as A0 gated A1.

## Consequences

Pros:
- Unlocks the flagship differentiator: direct-to-VRAM checkpoint restore, host RAM
  and CPU off the path on both ends — the number Phase-4's README headline needs.
- Multi-rail lifts the per-node ceiling from one rail to the instance's full EFA
  fabric (~32× on p5), which is what makes a 100 GB/s-class restore storm feasible.
- No data-plane redesign: ADR-0018's holder logic and the gRPC control/fallback
  path (ADR-0003/0019) are untouched; the change is localized to requester buffer
  registration and rail enumeration.

Cons / risks:
- HBM registration couples the transport to the GPU stack (CUDA/dmabuf, driver
  version, AMI enablement) — a real dependency surface the host-RAM path avoids.
- Multi-rail multiplies PD/QP/CQ state and the CQ→tokio bridge per rail; failure
  and AH-rebuild handling (ADR-0019/0021) must be per-rail.
- p5.48xl is expensive (~$98/hr); A5 benchmarking must be short and scoped.

## Status / relationship to other ADRs

- **Supersedes** planning/04 §4's "GPUDirect RDMA is irrelevant to us" — that line
  is now historical; this ADR is the current stance.
- **Extends** ADR-0018 point 5 (tier-agnostic buffers) from an aside into a
  committed, scoped direction.
- **Extends** ADR-0004 (Nitro v4+ nodepool) with the multi-rail enumeration; the
  hardware-support facts in planning/04:48 (rails per family) are the input.
- **Does not affect B4** (planning/15): B4 is the host-RAM single-rail scaling
  gate and ships first. A5 is the HBM/multi-rail follow-on, and its benchmark
  re-runs B4's Steps 4–5 (GPU restore) on the direct-to-VRAM path.
