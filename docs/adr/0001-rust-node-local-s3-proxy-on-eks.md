# ADR-0001: Rust node-local S3 caching proxy, deployed as an EKS DaemonSet

Date: 2026-07-10 · Status: Accepted · The initial cache policy noted here
(whole-object, >4 MB, LRU) is later refined: chunk-granular caching (ADR-0015),
cacheable range reads (ADR-0011→0015), and admission/eviction policy (ADR-0005,
ADR-0016). The GET-only / write-through property (ADR-0007) and the DaemonSet +
node-local endpoint shape stand.

## Context

GPU nodes on EKS read the same large objects out of S3 over and over — model weights,
checkpoints, datasets — once per pod, per rank and per restart, every time across the node's
own NIC. The bytes are read far more often than they change, and the instance store sitting
under them is idle.

What that argues for is a cache that is **node-local**, **shared across the pods on that
node**, and reachable **without changing any client**: the workloads doing the reading are
existing S3 SDKs, frameworks and loaders, and a cache only earns adoption if repointing an
endpoint is the whole integration. A same-node HTTP endpoint speaking the S3 API satisfies
that; a FUSE mount or a bespoke client library does not (prior-art survey in planning/02).

## Decision

A **Rust daemon running as an EKS DaemonSet**, exposing an **S3-compatible HTTP endpoint**
to same-node pods via a Service with `internalTrafficPolicy: Local`. Cache on instance-store
NVMe (+ RAM tier); initial policy GET-only, objects > 4 MB, whole objects, LRU.

## Consequences

- Zero client changes — any S3 SDK works by repointing its endpoint. This is the core UX bet.
- Rust: performance headroom for a data-plane daemon + memory safety for an "untrusted node daemon".
- DaemonSet + `internalTrafficPolicy: Local` avoids hostNetwork/link-local hacks
  (kept as a fallback pattern) — and, load-bearing later, guarantees a node's
  daemon sees GETs only from same-node client pods. The cluster read path
  (ADR-0012) builds its owner-fills-requester-relays model directly on this.
- The initial policy values (4 MB threshold, whole-object, LRU) are a deliberate starting
  point and not a result: they are the conventional shape for a node-local object cache, cheap
  to implement, and each is revisited once there is a measurement to revisit it with —
  whole-object by ADR-0015, the threshold and admission by ADR-0005/ADR-0016.
- Nothing already in this stack provides the combination (node-local + shared + S3 API), which
  is what makes it worth building rather than adopting (planning/02).
