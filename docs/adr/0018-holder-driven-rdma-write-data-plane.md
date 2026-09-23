# ADR-0018: Data plane — holder-driven RDMA WRITE into requester buffers

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-12 · Status: Accepted · Amends ADR-0003 · **Hardware-validated
2026-07-16 (A0 spike, planning/09): the holder-driven one-sided WRITE + own
send-completion-as-done was proven cross-node on EFA/SRD. Two API constraints the
paper design missed are folded in below (see "Hardware findings").** · **DEFAULT-ON
2026-07-21:** the A4 benchmark (planning/14) cleared the ADR-0008 flip gate — this
WRITE data plane beats gRPC on throughput AND holder CPU at high fan-out (2.25 vs
1.85 GiB/s, 0.40 vs 1.24 CPU-s/GiB), so it is now the default peer transport
wherever EFA hardware exists (`values-efa.yaml`), with automatic gRPC fallback
(ADR-0003) unchanged.

## Context

ADR-0003 chose "RDMA READ over EFA" for cross-node reads, written before the
cache internals existed. Design review shows requester-issued one-sided READs
fight the actual system: a remote READ needs the chunk's byte address on the
holder, but foyer relocates and evicts entries (its offsets are private), the
NVMe tier isn't addressable by RDMA READ at all (that would require NVMe-oF),
and every published one-sided-READ design (Pilaf, FaRM, XStore) spends its
complexity budget on exactly this address-volatility problem. Meanwhile the
pattern used by NVMe-oF, NFS/RDMA, 3FS's storage service, and Mooncake's
Transfer Engine is the inverse: the requester asks, the *holder* pushes.

Target hardware (ADR-0004 nodepool, now p5/EFAv2 and p5en/EFAv3): SRD
transport — reliable but **out-of-order** delivery, **no RDMA atomics**, and
write-with-immediate support varies by EFA generation.

## Decision

Cross-node chunk reads use **requester-prepared buffers, holder-driven
one-sided RDMA WRITE**, superseding ADR-0003's READ choice (its EFA-default/
gRPC-fallback split stands):

1. Requester picks a holder from the directory (ADR-0017) and sends a fetch
   command (gRPC, ADR-0003/0019) carrying `(chunk_key, generation, buffer_addr,
   rkey, length)`. The buffer comes from a **pre-registered per-node pool**
   (registration is expensive; pooled and reused, never registered per-read).
2. The holder resolves the chunk *locally* (DRAM hit, or NVMe → registered
   bounce buffer — a pipeline the holder controls), then issues an RDMA
   WRITE into the requester's buffer.
3. Completion is an **explicit done signal** — NOT write-with-immediate, NOT
   last-byte polling: SRD's out-of-order delivery makes "last byte present"
   meaningless, and write-with-imm support must be probed per EFA generation
   (`fi_getinfo`) rather than assumed. In the shipped design (ADR-0003) the done
   signal **is the gRPC fetch RPC's response**: the holder issues the RDMA WRITE,
   waits on its **own SRD send-completion** — and because SRD is a *reliable*
   transport, local send-completion ⇒ the bytes are in the requester's registered
   buffer — then returns the response carrying the chunk's metadata (ETag
   fragment, actual length), which doubles as the integrity signal. If ADR-0019's
   libfabric-messaging optimization is later taken, the same done signal moves to
   a two-sided message on the RDMA channel; the semantics are identical.
4. Holder-side miss/eviction (directory was stale, generation mismatch) →
   `NotCached`-class reply; requester retries another listed holder, then
   falls back per ADR-0012 rule 3. The `generation` in the command is a staleness
   check, not a hard contract: a v2 reader carries the entry-level generation
   (ADR-0017/0020), and any mismatch simply triggers this benign retry path —
   never a wrong or torn serve (chunks are immutable, ADR-0015). Peer failure is
   never client-visible.
5. Buffers are **tier-agnostic**: the pool abstraction must not assume host
   DRAM, so GPUDirect (WRITE straight into requester HBM on p5) is a later
   registration-flag change, not a redesign.

### Hardware findings (A0 spike, 2026-07-16) — two constraints this design must honor

Both were proven on 2× r8gd.24xlarge cross-node (planning/09 findings 7, 10);
neither changes the *shape* above, but each is load-bearing for the A1 code:

6. **EFA one-sided RDMA needs SYMMETRIC address handles.** SRD is a *reliable*
   transport, so the WRITE generates transport ACKs the **requester** must be able
   to receive — which means the **holder must hold an address handle for the
   requester's EFA GID before it issues the WRITE**, even though the holder is the
   sender. Without it the WRITE completes with
   `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER` (vendor err 14, "no valid AH at
   remote side"). Concretely this **amends point 1**: the fetch command (and/or the
   eager descriptor distribution, ADR-0019) must carry the **requester's EFA GID**,
   and the holder must insert an AH for it (address-vector insert, cached per peer
   like the QP) **before** posting the WRITE. This is not one-sided in *addressing*
   even though it is one-sided in *data movement*; it is the address-vector pattern
   libfabric/aws-ofi-nccl use. AH insert is cheap and per-peer-once (cache it beside
   the lazy QP/stream setup, ADR-0019 knobs).
7. **The pre-registered buffer pool (point 1) registers MRs WITHOUT the atomic
   access bit.** `ibv_reg_mr` with `IBV_ACCESS_REMOTE_ATOMIC` fails `EOPNOTSUPP` on
   EFA (no hardware atomics, planning/04 §2). Register pool buffers with
   `LOCAL_WRITE | REMOTE_READ | REMOTE_WRITE` only — enough for the WRITE data plane
   (REMOTE_WRITE) and the ADR-0020 directory READ (REMOTE_READ). A "permissive"
   bundle that includes the atomic bit is a startup failure, not a slow path.

The gRPC streaming path (Phase 2, ADR-0008) remains the fallback tier,
byte-for-byte unchanged, for non-EFA environments and transport errors.

## Trade-offs

Pros:
- No remote-address problem: byte locations never leave the holder, so foyer
  keeps full freedom to relocate/evict, and the directory stays address-free
  (ADR-0017). The entire FaRM/XStore versioning-and-leases apparatus is
  avoided.
- NVMe tier composes naturally — same command, holder pipelines SSD reads;
  a requester-side READ simply cannot reach it.
- One round trip: a single fetch RPC from the requester's view, with the RDMA
  WRITE nested inside its service time (command out → holder WRITEs → holder's
  send-completion → response/done). A READ design pays an address-discovery RPC
  *before* the transfer anyway.
- Holder CPU sees every fetch → natural point for admission, load shedding,
  and the heat signal ADR-0020 removes from the home.

Cons:
- Holder CPU is on the data path (unlike a pure one-sided READ from DRAM).
  Accepted: it's unavoidable for the NVMe tier, which dominates capacity;
  the DRAM-hit CPU cost is a doorbell ring, not a copy.
- Requester must size/lease buffers before knowing the chunk is servable —
  a stale-directory miss wastes a pooled buffer lease for one round trip.
- Buffer pool sizing becomes a real resource knob (below): too small
  throttles storm-time parallelism, too large pins memory permanently.

## Knobs

- `rdma_buffer_pool_size` (default **256 × chunk_size**, i.e. 4 GiB at
  16 MiB chunks) — pinned, registered at startup. Bounds per-node in-flight
  fetch parallelism; size against storm ingest target (~100 GB/s needs
  ~O(100) in-flight chunks at 16 MiB).
- `fetch_parallelism` (default **64**) — concurrent outstanding fetch
  commands per node; must be ≤ pool slots.
- `holder_retry_limit` (default **2**) — stale-holder retries before backend
  fallback (ADR-0012 rule 3).
- `bounce_buffer_pool` (holder side, default **64 × chunk_size**) — NVMe→NIC
  staging; bounds concurrent NVMe-tier serves.
- Capability probe at startup gates the whole plane: a node that can't create an
  SRD QP with one-sided send-ops advertises gRPC-only and peers use the fallback
  tier to it. Per ADR-0021 the probe is **efadv/ibverbs** (create+activate an SRD
  QP, `efadv_query_device` caps), not libfabric `fi_getinfo` — functionally
  equivalent, and on EFA v2+ SRD one-sided ops are hardware by construction (no
  emulated path). **Validated 2026-07-16** on r8gd (Nitro v5/EFAv3); the message
  formats `(chunk_key, generation, buffer_addr, rkey, length)` + the requester GID
  (finding 6) can now be frozen. Re-confirm on p5/p5en at their bring-up (same
  probe, expected identical).
