# ADR-0008: Build order — gRPC baseline first, EFA behind a feature flag, flip on benchmark win

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-10 · Status: Accepted · **Spike step 2 DONE 2026-07-16 — GO.** The
hardware spike this ADR mandated passed: one-sided WRITE **and** READ + memory
registration on EFA/SRD, cross-node, via jonhoo/ibverbs (rev d9a5c01, `efa`).
ADR-0005's load-bearing question — does ibverbs-main expose one-sided EFA verbs +
MR management — is answered **yes**; the libfabric-FFI reopen is **not** needed.
Full result + 10 findings in planning/09; API constraints folded into ADR-0018/0019
(symmetric AHs, no-atomics MR flags) and ADR-0021 (efadv-not-libfabric probe).
A1 (`EfaRdmaTransport`) is unblocked; the flip-on-benchmark-win discipline (step 3)
still governs whether it becomes default.

## Context

EFA-RDMA is the decided default for cross-node reads (ADR-0003), but: no production Rust-on-EFA
exists; the enabling crate feature is unreleased (git pin); bulk-blob transfer is the workload
where RDMA's edge is smallest (planning/04 §7); and heterogeneous capability requires a
negotiated fallback regardless. Building the riskiest transport first would block the whole
cluster tier on unproven code.

## Decision

1. **Phase 2 ships the gRPC/TCP transport first**, with ENA Express enabled (SRD benefits, zero code), and records a Warp benchmark baseline.
2. **Phase 3 builds the EFA transport behind the `efa` feature flag**, starting with a hardware spike (below) before any real investment. The RDMA surface is **three independent primitives with independent flip decisions**, not one flag: (a) the holder-driven WRITE data plane (ADR-0018) — the primary Phase 3 target; (b) the one-sided READ directory (ADR-0020) — a further gate on the "home-CPU lookup wall" showing up under storm; (c) control-plane messaging (ADR-0019) — **deferred**, control stays on gRPC (ADR-0003), revisited only if small-chunk or directory-RPC-throughput data demands it. The spike must **exercise one-sided WRITE and READ + memory registration** on a Nitro v4+ node — not only the two-sided `efa_srd` example, which validates the wrong verbs; per ADR-0005 the load-bearing unknown is whether ibverbs-main exposes one-sided EFA verbs + MR management. If it can't, the libfabric-FFI question (ADR-0005) reopens before, not after, investment.
3. **Each primitive becomes the shipped default only when it beats the gRPC/RPC baseline** on its own benchmark (WRITE: throughput AND CPU at high fan-out; directory READ: home-CPU under storm). If one doesn't win meaningfully, its flag stays off and the result is documented — independently of the others.

**Flip status — WRITE data plane (a): DEFAULT-ON as of 2026-07-21.** The A4
benchmark (planning/14, corrected re-run on 4× r8gd.24xlarge) cleared this gate:
at high fan-out (many→one) the holder-driven WRITE data plane beats gRPC on
**both** axes — 2.25 vs 1.85 GiB/s throughput and 0.40 vs 1.24 holder CPU-s/GiB
(3.1× less holder CPU, the offload win), served fraction 1.000, zero fallbacks,
all arms under the NIC line rate. EFA is therefore the default peer transport
**wherever EFA hardware exists**: `deploy/helm/pacer/values-efa.yaml` sets
`efa.enabled: true`. It is *not* a universal base-chart default because
`enabled: true` requests an EFA device (unschedulable on non-EFA nodes) — EFA is
hardware-gated, and the gRPC fallback remains the product for non-EFA nodes
(consequence below). Primitives (b) one-sided READ directory and (c) control
messaging remain OFF, still gated on B4 / their own data.

## Consequences

- The risky code lands against a working, measured baseline — regressions and wins are provable, not assumed.
- The fallback path gets production hardening from day one (it IS the product for non-EFA nodes forever).
- The spike is cheap and decisive: if the ibverbs EFA feature doesn't work on real hardware, we learn it in days.
- Honest possibility acknowledged: ENA-Express-boosted gRPC may be close enough that RDMA stays optional — that outcome is a documented benchmark result, not a failure.
- The `PeerTransport` trait + capability handshake are built in Phase 2, so Phase 3 slots in without refactoring.
