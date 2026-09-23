# ADR-0012: Cluster read path — read-through fill at the home, requesters relay

Date: 2026-07-11 · Revised 2026-07-12 · Status: Accepted · This ADR defines the
**Phase 2 baseline** cluster read path (whole objects, one copy at the rendezvous
owner). Phase 3 layers on top without discarding it: the cache unit becomes the
chunk (ADR-0015), copies become plural (ADR-0016), "compute the owner" becomes a
directory lookup at the home (ADR-0017), and write-invalidation moves to ADR-0007.
What carries and what lifts is spelled out in "Phase 3 generalization" below.

## Context

Phase 2 turns N independent node caches into one cluster cache: rendezvous hashing
assigns each key an owner node, and `PeerTransport.fetch_blob` moves cached bytes
between nodes (ADR-0003, ADR-0008). What the roadmap leaves open is *who fills the
cache on a cluster-wide miss*.

Clients reach the daemon through an `internalTrafficPolicy: Local` Service, so a
node's daemon only ever sees GETs from client pods on that node (ADR-0001). If a
miss were filled on the *requesting* node (Phase 1 behavior applied naively):

- the owner's cache never warms for keys whose readers happen to sit elsewhere,
  so peer fetches keep missing forever;
- every node that reads a hot object duplicates it, and aggregate capacity
  collapses toward single-node capacity (the opposite of a cluster cache).

## Decision (Phase 2 baseline)

On a cacheable GET, the daemon checks its **local cache first** (a hit is a hit,
regardless of ownership — entries filled before a membership change stay servable
until evicted). On a local miss it computes the owner:

1. **Owner is self (or the ring is empty/single-node)** → Phase 1 path: backend
   GET, dual-stream tee fill.
2. **Owner is a peer** → `fetch_blob(owner, key, range)`. The owner serves from
   its cache; on its own miss it performs the **read-through itself**: backend
   GET, fill its cache (normal admission policy), stream the bytes back. The
   requester relays the body to the client.
3. **Any transport error** → the requester falls back to a direct backend GET
   *without filling*, counts a fallback metric, and the client read succeeds.
   Peer failure is never client-visible.

Invariants that hold at every phase (these are the load-bearing ones):

- **Read-through happens at the home, and the home is the fill serialization
  point** — one backend GET cluster-wide per key/chunk, not one per reading node.
  The in-flight fill guard lives at the home, so concurrent cluster-wide reads of
  a cold key collapse to a single backend fetch.
- **The home fills only what it currently believes it owns.** If two nodes' ring
  views skew across a membership change, the home still serves the read-through
  but skips the fill (self-healing, no wrong-node residue).
- **Backend fallback on any peer error, without filling** — peer/directory
  failure degrades to a correct backend read, never a client-visible error.
- **`Cache-Control` travels with the request**: `no-cache` bypasses straight to
  the backend on the requesting node (never a peer hop); `no-store` is forwarded
  as a no-fill peer read.
- **ADR-0011 cost invariant applies at the home**: a range miss is a ranged
  backend GET, never a whole-object fill (Phase 3: never beyond the covering
  chunks, ADR-0015).

## Phase 2-only corollaries (lifted in Phase 3)

These followed from whole-object + single-copy and are **superseded**, kept here
to mark exactly what changed:

- ~~Exactly one cluster-wide copy per object (owner's cache).~~ → ADR-0016 allows
  requester-local admitted copies (frequency-gated) and top-R co-homes; capacity
  is traded for hot-chunk fan-out relief. Cold data still keeps ~one copy.
- ~~Requesters never insert peer-owned keys.~~ → ADR-0016 layer 1: a requester
  MAY admit a peer-owned chunk once it proves hot, and registers it in the
  directory so others can fetch from it.
- ~~Only two nodes can hold a key (owner + writer), so invalidation needs no
  broadcast.~~ → ADR-0016/0017: the holder set is the directory sharer set + R
  co-homes; still a known, bounded, non-broadcast fan-out (see ADR-0007).
- Small objects (≤ min size) pay one extra hop on the owner path (size unknown
  until the backend answers). Accepted; Phase 3's header entry (ADR-0015) caches
  length after first read, shortcutting repeats.

## Phase 3 generalization

Everything above operates **per chunk** (ADR-0015), and "compute the owner"
becomes "compute the home → directory lookup → fetch from a listed holder"
(ADR-0017): the home is the chunk's rendezvous owner and its directory shard, so
for a warm-but-not-hot chunk (home filled it first) lookup and fetch still collapse
to one hop. The read-through-at-home, home-is-fill-serialization-point,
skip-fill-on-skew, and backend-fallback invariants all carry over unchanged. The
cross-node data move is holder-driven RDMA WRITE (ADR-0018), not the Phase 2
gRPC stream, on EFA nodes.

## Write invalidation

Moved to **ADR-0007** (revised): write-through + awaited invalidation to every
holder the directory names. The Phase 2 "invalidate local + owner, two nodes only"
rule is the special case of that fan-out when the sharer set is ≤ 2.

## Consequences

- Peer fetch of a warm object costs zero Express bytes — the Phase 2 exit
  criterion ("peer fetch beats re-fetching from Express on bytes billed") holds by
  construction whenever the home is warm; misses cost exactly one backend GET
  cluster-wide.
- A hot object's read fan-out concentrates on its owner node in Phase 2. This is
  the hotspot Phase 3 attacks directly: chunk placement spreads a large object
  over many homes (ADR-0015), replication spreads a hot chunk over R + local
  copies (ADR-0016), and the holder-driven RDMA WRITE data plane (ADR-0018)
  offloads the serving cost off the home CPU.
- Cold reads via a peer relay the body through one extra node (the home). Same-AZ
  bandwidth is free and ENA Express-boosted; measured in the Phase 2 baseline.
