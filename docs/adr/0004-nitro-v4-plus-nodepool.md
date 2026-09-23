# ADR-0004: Nodepool restricted to Nitro v4+ (EFA v2+) instance families

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-10 · Status: Accepted

## Context

EFA capability varies by Nitro generation (official mapping: Nitro v3=EFA v1 … v6=EFA v4).
Our RDMA primitives (ADR-0003) — holder-driven WRITE for the data plane (ADR-0018) and
one-sided READ for the directory (ADR-0020) — require Nitro v4+. c5n and g5 have NO RDMA
at all (send/recv only); p4d/p4de have the one-sided ops but their EFA traffic doesn't
interoperate with other instance types (mesh partition risk). Full matrix in planning/04.

## Decision

The cache nodepool (Karpenter EC2NodeClass) allows **Nitro v4+ EFA-capable families**
across both arches: c6in, g6/g6e, m7i/r7i-class, p5/p5e(+), trn1/trn2, hpc7a, and the
arm64/Graviton EFA families **c8gn/c9gn-class** (Nitro v5+, WRITE-capable). Excluded:
c5n, g5 (no RDMA at all), and p4d/p4de (one-sided ops but non-interoperable EFA traffic —
mesh partition risk, unless the whole pool is p4d).
**Named exception — c7gn/hpc7g:** EFAv4+ but *without* one-sided WRITE support. They may
join the pool but serve only directory READs (ADR-0020); their data plane falls back to
gRPC (ADR-0003). Every other allowed family is WRITE-capable.
Also required per node: instance-store NVMe (RAID0 via `instanceStorePolicy`), ENA Express
support, EFA interface, cluster placement group, self-referencing allow-all SG.

## Consequences

- Capability negotiation (ADR-0003) becomes a formality instead of a compatibility maze; older nodes joining anyway silently use the gRPC path.
- The binding capability is now **WRITE (data plane, ADR-0018) + READ (directory,
  ADR-0020)**, not READ alone as first written. Any family added to the pool later
  must pass the `fi_getinfo` WRITE probe (ADR-0018), not just have EFA — a
  WRITE-incapable EFA generation (the named c7gn/hpc7g exception above) serves
  directory reads but falls back to gRPC for all data transfer. This is a
  Nitro/EFA-generation check, **not an arch check**: WRITE-capable EFA spans both
  amd64 (p5/p5en) and arm64 (c8gn/c9gn-class Graviton). Re-verify WRITE support per
  family, not just RDMA presence, when re-checking the AWS table.
- Bandwidth envelope known per family: c6in 200 Gbps (2 cards) … p5 3.2 Tbps
  (32 cards). One EFA per network card. This envelope is the input to ADR-0018's
  per-node RDMA buffer-pool and fetch-parallelism sizing.
- Excludes some cheap families from the cache pool — acceptable: this pool is for data-hungry workloads.
- Re-verify the live AWS instance table at nodepool definition time (matrix moves with every launch).
