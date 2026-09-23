# ADR-0003: EFA/RDMA usage map — WRITE for cache-miss data, READ for directory, control on gRPC; gRPC fallback

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-10 · Revised 2026-07-12 · Status: Accepted (user decision) ·
Consolidates the RDMA-usage picture after ADR-0018 (data plane), ADR-0019
(control plane), and ADR-0020 (directory lookup). The original decision here was
a single primitive — requester-issued RDMA READ for cross-node reads, RDMA for
nothing else — chosen before the cache internals existed; it is preserved in git
history. The **EFA-default / gRPC-fallback split** and the **capability-negotiated,
never-a-hard-dependency** discipline are the parts that stand unchanged; the
primitive and its scope are what this revision restates.

## Context

Cross-node peer fetch moves 4 MB–multi-GB blobs NVMe→NVMe. Options researched
(planning/04): plain gRPC/TCP, gRPC + ENA Express (SRD under TCP, free, zero
code), RDMA over EFA (SRD via libfabric, kernel bypass). Evidence: bulk streaming
is where RDMA helps least (kernel TCP saturates 100 Gbps at similar CPU —
i10/NSDI'20); the S3-cache category ships TCP; but AWS's own FSx for Lustre ships
EFA/SRD as its data path with ENA fallback — the exact pattern proposed. Rust-on-EFA
has no production precedent; jonhoo `ibverbs` main has unreleased EFA/SRD support.

Design review after the cache internals landed reshaped *which* RDMA primitive
serves *which* path (ADR-0018/0020): a requester-issued READ needs the holder's
byte address, but foyer's offsets are private and its NVMe tier isn't
RDMA-READ-addressable at all — so the data plane inverted to holder-driven WRITE,
while READ found its real home in the fixed-slot directory table.

## Decision

EFA is the default cross-node transport; gRPC over TCP (tonic) with ENA Express
enabled is the fallback tier, the bootstrap/membership channel, **and the shipped
control plane**. RDMA over the libfabric **`efa` fabric** (not `efa-direct`)
serves two paths, and **nothing outside this list rides RDMA** (backend S3 traffic
and client-facing HTTP stay on HTTPS/HTTP):

1. **Cache-miss data plane → holder-driven one-sided RDMA WRITE** into
   requester-pooled buffers (ADR-0018). This is the peer chunk-fetch path. Not
   READ — foyer's private offsets and the NVMe tier rule out a requester-issued
   READ (see Context and ADR-0018).
2. **Directory lookup → requester-issued one-sided RDMA READ** of a fixed-slot,
   self-verifying table (ADR-0020, v2; RPC is v1). This is the one place a
   one-sided READ works cleanly: small, fixed-address, read-mostly,
   checksum-validated.

**Control stays on gRPC.** The per-read fetch command carries `(chunk_key,
generation, buffer_addr, rkey, length)` (ADR-0018) and its **response doubles as
the done signal**: the holder posts the RDMA WRITE, waits for its own SRD
send-completion (reliable delivery ⇒ bytes are in requester memory), then returns
the response. No separate completion machine, no last-byte polling — consistent
with ADR-0018 point 3, and simpler than a second messaging stack. Admit/evict/
invalidate and v1 directory lookups likewise ride gRPC.

**libfabric two-sided messaging (`fi_send`/`fi_recv` over SRD) is a documented
future optimization, not a shipped decision** (ADR-0019): moving fetch commands
and v1 lookups onto messaging pays off only for small chunks (ADR-0015's knob) or
to dodge the ~10⁵-RPC/s/core directory cliff at ~1000 nodes. Explore it behind the
ADR-0008 benchmark schedule if the storm data demands it; until then the shipped
shape is control-over-gRPC + data-over-RDMA-WRITE + directory-over-RDMA-READ.

Runtime capability negotiation per peer pair (`fi_getinfo` probe: `FI_RMA` +
WRITE for the data plane, READ for the directory; `FI_OPT_EFA_EMULATED_READ ==
false` both ends); automatic per-peer fallback to the gRPC tier on any RDMA
error. RDMA is never a hard dependency — a node lacking a required capability
advertises gRPC-only and peers use the fallback tier to it.

## Consequences

- Unsafe/RDMA surface stays confined to the transport crate, split across two
  narrow one-sided paths (WRITE data plane, READ directory); the `PeerTransport`
  trait exposes no RDMA-typed methods to the rest of the daemon, and control flow
  stays on ordinary gRPC.
- The gRPC control path is *simpler* than a bespoke messaging plane, not just
  safer: the fetch response is the done signal for free, so there is no explicit
  completion message, credit-based flow control, or poller stall to own unless
  and until the messaging optimization is taken.
- Consistency never depends on RDMA: all mutations ride HTTPS to the backend
  (ADR-0007); the directory is soft state whose staleness is benign (ADR-0017/0020).
- The holder is on the data path and observes every fetch command (ADR-0018) —
  this is the heat signal ADR-0020 relies on once NIC-served lookups blind the
  home. (This reverses the original ADR's "owner doesn't observe reads" note,
  which assumed a requester-issued READ.)
- Fallback must be production-grade in its own right: ENA Express gives the gRPC
  path SRD benefits (25 Gbps single-flow, tail-latency cuts) for free, and it is
  the permanent product for non-EFA nodes (ADR-0008).
- We accept pioneering risk: first known production Rust-on-EFA (mitigation:
  ADR-0008 build order, hardware spike first; each RDMA primitive probed via
  `fi_getinfo` before its message/table formats are frozen — ADR-0018/0020
  bring-up tasks).
