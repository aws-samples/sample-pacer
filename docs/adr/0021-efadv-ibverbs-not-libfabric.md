# ADR-0021: The RDMA userspace stack is efadv/ibverbs, not libfabric — reconciling the probe/descriptor wording

Date: 2026-07-15 · Status: Accepted (API-level; hardware-gated per ADR-0008) ·
Amends ADR-0003, ADR-0018, ADR-0019, ADR-0020 · Confirms ADR-0005

## Context

ADR-0005 chose the **jonhoo/ibverbs** crate (git-pinned `main`, `efa` feature)
for the EFA path. But ADR-0003/0018/0019/0020 — written while the transport was
still a paper design — describe the *mechanism* in **libfabric (OFI)** terms:

- ADR-0003: "RDMA over the libfabric **`efa` fabric** (not `efa-direct`)"; the
  capability probe as "`fi_getinfo`" and "`FI_OPT_EFA_EMULATED_READ == false`".
- ADR-0018: "`fi_getinfo` capability probe at startup"; done-signal via SRD
  send-completion (transport-agnostic, fine).
- ADR-0019/0020: the eagerly-distributed directory descriptor as
  `(fi_addr, base, rkey, ABI ver)` — an **`fi_addr`**.

libfabric and ibverbs/efadv are **two different userspace stacks over the same
EFA kernel driver**. You cannot call `fi_getinfo` or hold an `fi_addr` through
ibverbs. So the ADRs, read literally, name a stack we are not using. This was
flagged during the Phase 3 A0 spike (planning/09) and is resolved here rather
than left as latent drift between the design and the code.

The A0 spike settled the load-bearing question ADR-0005 raised: reading
jonhoo/ibverbs at the pinned rev (`d9a5c01`), the crate **does** expose one-sided
WRITE and READ on the **SRD** queue pair (`AddressedSendOp<Srd>::{write,
write_imm, read}`) plus external-buffer registration (`register_from_raw` →
`rkey`/remote descriptor). So the ibverbs path is viable and the rejected
libfabric-FFI fallback (ADR-0005) is **not** needed — subject to the same
verbs *working on hardware* (spike S4/S5/S6), which is why this ADR is
"API-level, hardware-gated."

## Decision

**The shipped RDMA userspace stack is rdma-core / efadv via jonhoo/ibverbs
(ADR-0005), not libfabric.** Where ADR-0003/0018/0019/0020 name libfabric
primitives, read them as their efadv/ibverbs equivalents — the *intent* is
unchanged, only the API surface differs:

| ADR wording (libfabric) | Shipped equivalent (efadv/ibverbs) | Same intent |
|---|---|---|
| `fi_getinfo` capability probe | create+activate an SRD QP with one-sided send-ops; `efadv_query_device` caps | "does this node do hardware one-sided RDMA?" |
| `FI_OPT_EFA_EMULATED_READ/WRITE == false` | no emulated path exists in efadv on EFA v2+ — SRD one-sided ops are hardware by construction (a passing probe on an EFA device *is* the non-emulated result) | "not software-emulated" |
| the **`efa` fabric** (not `efa-direct`) | efadv SRD QP (`EFADV_QP_DRIVER_TYPE_SRD` via `efadv_create_qp_ex`) | reliable, unordered, no size cap |
| directory descriptor `(fi_addr, base, rkey, ABI ver)` | `(QueuePairEndpoint{qp_num,lid,gid}, base, rkey, ABI ver)` — a 23-byte SRD endpoint, not an `fi_addr` | "everything a peer needs to one-sided-READ this shard, distributed eagerly" |
| addressing an `fi_addr` | per-WR `AddressHandle` + `(remote_qpn, qkey)` — SRD is connectionless (datagram) | "reach a specific peer QP" |

Consequences that follow directly (and are frozen wire contracts, ADR-0014
discipline):

- **The eager descriptor (ADR-0019/0020) carries an ibverbs `QueuePairEndpoint`,
  not an `fi_addr`.** Its wire form is the crate's stable 23-byte encoding
  (`qp_num`, `lid`, `gid`); the remote-memory descriptor is the 20-byte
  `RemoteMemorySlice` (`addr`, `len`, `rkey`). The ADR-0020 directory-table ABG
  and ADR-0019 distribution are otherwise unchanged.
- **The done signal (ADR-0018) is unaffected** — it is the SRD *send-completion*,
  a transport-level event both stacks expose identically.
- **The libfabric-messaging optimization (ADR-0019, deferred) would still be a
  separate stack.** If that optimization is ever taken it means adding libfabric
  *alongside* efadv, a genuine second dependency — an extra reason it stays
  gated on benchmark evidence.
- **The `efa-direct` MTU caveat (planning/04) is moot** on the efadv path — it
  was a libfabric-fabric distinction; SRD QPs segment/reassemble at the driver.

## Trade-offs

Pros:
- The design docs and the code name the same stack — no latent contradiction for
  the next reader (or the next daemon version) to trip over.
- Confirms ADR-0005's bet with source evidence: ibverbs *does* reach one-sided
  WRITE/READ on SRD, so the libfabric-FFI fallback stays rejected.
- One RDMA dependency, not two, unless/until the messaging optimization forces it.

Cons:
- Four ADRs now carry a "read libfabric-term X as efadv-equivalent Y" indirection.
  Mitigated by the table above being the single mapping.
- **Hardware-gated, not final:** "the crate exposes the verbs" (confirmed) is not
  "the verbs work on the NIC" (spike S4/S5/S6, planning/09). If the spike fails on
  hardware, the libfabric-FFI path reopens (ADR-0005/0018) and this ADR's stack
  choice is revisited by amendment — the divergence-reconciliation above would
  then flip to libfabric wholesale, which is why the mapping is documented rather
  than the ADRs rewritten.

## Knobs

None new. The stack choice is fixed per build (the `efa` cargo feature pins
ibverbs); there is no runtime libfabric/efadv switch.
