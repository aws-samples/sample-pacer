# ADR-0019: Per-read control on gRPC; libfabric messaging as a benchmark-gated optimization

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-12 · Revised 2026-07-12 · Status: Accepted · Amends ADR-0003 ·
**Reframed:** the original decision moved *all* per-read control onto libfabric
two-sided messaging. Per ADR-0003 the shipped design keeps control on **gRPC**
(the fetch RPC's response is the done signal, for free — no second messaging
machine); libfabric messaging is retained here as a **documented, benchmark-gated
optimization path**, not a committed cutover. The analysis below is the case for
*when* to take it, not a statement that it is taken.

## Context

ADR-0018 gives the data plane one-sided WRITEs, but every read is bracketed by
small control messages: directory lookup (until ADR-0020), fetch command, done
signal, admit/evict announcements, invalidations. The question this ADR answers is
*what transport those ride*.

The case for moving them off gRPC (the reason this path is kept open):

- An in-cluster gRPC hop costs ~100–500 µs (kernel TCP, HTTP/2, protobuf,
  scheduling). For a *small* chunk (ADR-0015's knob tuned down) the control
  bracket can rival the data wire time (16 MiB at 100 Gbps ≈ 1.3 ms; 4 MiB ≈
  335 µs) — an RDMA data plane bracketed by TCP RPCs is dominated by the brackets.
- A directory home for a storm-hot chunk becomes a small-RPC hotspot: gRPC tops
  out ~10⁵ small RPCs/s/core; two-sided RDMA messaging reaches ~10⁷ (eRPC, NSDI
  '19) — the metadata layer's answer to the 999-on-1 concern.

The case against taking it now (why gRPC ships first):

- With holder-driven WRITE (ADR-0018) the fetch RPC's **response is the done
  signal for free** — no explicit completion message, no credit-based flow
  control, no poller-stall class of bug. gRPC is *simpler*, not just safer.
- At the default 16 MiB chunk the control bracket is a few percent of wire time,
  not 40–60%. The bracket-dominance argument only bites at small chunks.
- tonic is battle-tested; the messaging path is framing, credits, timeouts, and a
  completion-poller PACER would own.

eRPC's core result — two-sided messaging over datagrams beats clever one-sided
designs for RPC workloads, and FaRM/HERD both converged on requests-as-messages —
is the prior-art anchor for the optimization, not a mandate to adopt it before the
data shows it's needed.

## Decision

**Per-read control ships on gRPC** (ADR-0003): fetch commands, done-as-response,
admit/evict/invalidate, and v1 directory lookups. gRPC also owns bootstrap (EFA
address + rkey + capability exchange), membership (ADR-0014), and the non-EFA
fallback tier (ADR-0003/0008).

**Every node's directory shard is readable by every other node without a prior
handshake — eager static-descriptor distribution.** Each node's RDMA directory
descriptor — `(fi_addr, directory table base address, rkey, ABI version)`
(ADR-0020) — is **static** for the life of the process (registered once at
startup, never relocated), so it is distributed cluster-wide alongside membership
rather than exchanged per-peer on first contact. It rides the gRPC membership
channel, carried next to each endpoint and keyed by the node-name-ascending
identity (ADR-0014) so it lines up with the sharer-bitmap index. On any membership
change a node learns the new peer's descriptor at the same time it learns the peer
exists. This is the mechanism that makes ADR-0020's one-sided lookup usable during
a storm: a node can issue a directory READ to a home it has never talked to,
with **no bootstrap round-trip first**.

This does **not** contradict the "lazy establishment" default below, because on
EFA/SRD (`FI_EP_RDM`, connectionless via an address vector `fi_av`) distributing an
*address* is separable from establishing a *connection*: loading N peers'
`(fi_addr, base, rkey)` into the AV is MB-scale with no per-peer QP — the ~10⁶-QP
cost that argued for lazy setup is a connection cost, not an address cost. Only the
static *descriptor* is eager; QP/stream setup for the data-plane WRITE stays lazy
(on first fetch). **Caveat:** the `fi_av`/connectionless one-sided-READ behavior is
confirmed by the ADR-0008 hardware spike before this is frozen; if it doesn't hold,
fall back to per-peer descriptor exchange on first lookup (one extra hop, storm cold
path only).

**Hardware finding (A0 spike, 2026-07-16 — the descriptor must be BIDIRECTIONAL,
and it's an ibverbs SRD endpoint, not an `fi_addr`).** Two corrections from the
spike (planning/09 findings 9-10; ADR-0021 already reframed libfabric→efadv):
(1) Per ADR-0021 the shipped stack is efadv/ibverbs, so the distributed
descriptor is an ibverbs **`QueuePairEndpoint` (qp_num + GID)** loaded into the
`ibv`-level address handle, not a libfabric `fi_addr` — the AV reasoning holds,
the type name changes. (2) **EFA one-sided RDMA needs SYMMETRIC address handles:**
because SRD is reliable, the *target* of a WRITE/READ must hold an AH for the
*initiator* to return transport ACKs (else vendor err 14, UNKNOWN_PEER). So the
fetch-command edge (ADR-0018 point 1) must carry the **requester's** GID to the
holder, and the holder AH-inserts it before serving — i.e. the eager
same-with-membership distribution is **bidirectional**: every node needs every
peer's endpoint whether it will *initiate* to that peer (data-plane WRITE target,
directory-READ home) or *respond* to it. Loading all peers' endpoints into the AV
at membership time (already the plan here) satisfies this for free; the only new
requirement is that the per-fetch handshake also names the requester so an AH is
inserted before the first WRITE. The connectionless-addressing caveat above is
thereby **resolved (confirmed working)**; the refinement is the bidirectionality,
not a fallback to per-peer exchange.

Deliberately **not** distributed this way: dynamic per-node state (load / pressure).
Broadcasting load every X seconds through the membership/K8s control plane is
etcd-as-a-metrics-bus and the gossip dead-end ADR-0017 rejected; load signal stays
on the data path (holder sees every fetch, ADR-0018) or sampled admits (ADR-0020).
How to share pressure cluster-wide is left open (a scaling question), separate from
this read-path mechanism.

**libfabric two-sided messaging (`fi_send`/`fi_recv` over SRD) is a documented
optimization, gated on the ADR-0008 restore-storm benchmark.** Take it only if the
storm data shows (a) small chunks are placement-optimal *and* the gRPC bracket
dominates their read, or (b) the directory-RPC-throughput cliff binds at ~1000
nodes before ADR-0020's one-sided lookups relieve it. If taken, it reuses the data
plane's queue pairs, completion poller, and buffer discipline — one machine, not
two — and carries these obligations (all *deferred* until then): self-framed
protobuf, credit-based flow control for pre-posted receives, application-level
timeouts/retries (SRD guarantees delivery, not liveness), per-message-type
metrics, and a CQ-to-tokio-waker bridge.

Note ADR-0020 is a *separate* one-sided-READ optimization for directory lookups
specifically; it does not require this messaging path (it needs only the RDMA
READ verb + the fixed-slot table), so the two optimizations are independent gates.

## Trade-offs

Pros (of shipping gRPC, deferring messaging):
- The done-as-response property means the shipped control plane has no bespoke
  completion/flow-control code — the smallest possible surface that works.
- Messaging stays available as a proven lever if small-chunk or directory-cliff
  data demands it, with the cost analysis already done here.
- The optimization, if taken, is one code path shared with the data plane.

Cons / what taking the optimization later would cost:
- Real protocol code with real failure modes (framing bugs, credit leaks, poller
  stalls) replaces tonic — bounded to a handful of fixed message types.
- Debugging/observability regress vs. gRPC tooling; rebuilt as metrics + a
  message-trace flag.
- Dual-stack tax: every control feature must also work over the gRPC fallback
  tier for non-EFA clusters — which, since gRPC is the shipped path, it already
  does. Adopting messaging means maintaining *both*.

## Knobs

Live today (shipped gRPC control path):

- `rpc_timeout` (default **50 ms** control; data-done budgeted by chunk size) and
  `rpc_retries` (default **2**) before declaring the peer down → fallback tier.
- Lazy vs. eager peer-pair **connection** establishment (default **lazy**, on
  first fetch): 1000-node eager QP/stream setup is ~10⁶ connections cluster-wide;
  lazy bounds it to actual traffic pairs. (Applies to RDMA data-plane QP setup and
  gRPC channels, both dialed lazily and cached, ADR-0014.) **Distinct from
  descriptor distribution:** the static RDMA directory descriptor `(fi_addr, base,
  rkey, ABI ver)` is distributed **eagerly** with membership (see Decision) — it is
  an address, not a connection, so it carries no QP cost and enables one-sided
  directory READs to never-contacted homes.

Apply only if the messaging optimization is taken:

- `recv_credits` (default **128** per peer pair) — pre-posted receives / the
  flow-control window. At 1000 peers, credits × msg size × peers is the dominant
  pinned-memory concern.
- `ctrl_msg_size` (default **1 KiB**) — fixed receive-buffer size; must cover the
  largest control message (fetch command with rkey; directory replies capped by
  `max_sharers_tracked`, ADR-0017).
- `cq_poll_mode`: `busy_spin` (one dedicated core, lowest latency) vs
  `interrupt_hybrid` (default) — storm benchmark decides.
