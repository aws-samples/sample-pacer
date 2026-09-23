# ADR-0014: Ring ownership and membership semantics are a wire contract

Date: 2026-07-12 · Status: Accepted

## Context

A design review of the Phase 2 implementation found several load-bearing
invariants that exist only in code and tests, not in any ADR — yet peer nodes
running *different daemon versions* must agree on them during a rolling update,
which makes them wire contracts, not implementation details:

- `pacer-ring` uses **rendezvous (HRW) hashing** — ADR-0012 names the algorithm
  but not the hash: xxh3 with pinned seed `0x10_7a`, a `0xff` separator between
  node name and key, pinned by a golden test (`PINNED_SCORE`). Two versions
  disagreeing on a single score bit would reshuffle ownership mid-rollout.
- **Ownership binds to the stable node name, not the pod address.** A pod
  restart (new IP, same node) moves no keys; the transport heals by evicting
  its cached channel on error and re-dialing.
- **Readiness is the membership signal**: only EndpointSlice endpoints with
  `ready == true` are in the ring. Failing the readiness probe *is* leaving
  the ring. Relists are buffered (`Init…InitDone`) so a half-received relist
  never publishes a shrunken ring; membership is the union across slices.
  The membership set is also **totally ordered by node name (ascending)** — a
  deterministic order every node computes identically, needed wherever a
  position index is derived from membership (ADR-0020's sharer bitmap indexes
  by this order).
- **Membership changes move keys, never data**: no rebalancing, no bulk
  invalidation. Local hits are honored regardless of current ownership until
  LRU evicts them; an owner that does not own a key *in its own ring view*
  refuses the fill (serves or returns `NOT_FOUND`), which contains skew
  between two nodes' views.

## Decision

The above are **frozen as versioned protocol invariants**. Changing any of
them (hash function, seed, separator, score-to-owner rule, name-vs-addr
identity, readiness-gated membership, **or the node-name-ascending membership
ordering**) requires a new ADR **and** a versioned rollout plan (both hash
generations computed side by side, or a full cache flush accepted explicitly).
The golden `PINNED_SCORE` test is the enforcement point: it may only be updated
together with such an ADR.

Phase 3 structures that derive placement from the same hash (chunk homes,
directory shards, ADR-0015/0017) inherit this contract wholesale. ADR-0020 adds
a *second* frozen wire ABI (directory table layout + its two hash seeds + slot
count) governed by the identical migration discipline — a dual-version transition
or an accepted flush; its ABI-version field is that contract's `PINNED_SCORE`.

## Trade-offs

Pros:
- Rolling updates are safe by construction; no dual-version reshuffle storms.
- Pod churn is invisible to placement; only true node departure re-homes keys.
- Skewed ring views degrade to a backend fallback, never a wrong-node fill.

Cons:
- The hash is effectively unchangeable without a migration ADR — a better
  hash later costs a cluster-wide cache flush or a dual-hash transition.
- Readiness-gated membership couples cache placement to probe tuning: a
  flapping readiness probe re-homes that node's keys on every flap.
- Membership ordering is now load-bearing (ADR-0020): a node-name collision or a
  change to the sort key reindexes every bitmap. Node names are unique per node
  (K8s node object) so this is stable, but it is a contract, not an accident.

## Knobs

Per ADR-0013 these live in the config file, overridable by env:

- `PACER_PEERS` — static `name=addr` membership for dev/test; **wins over the
  EndpointSlice watch** when set.
- Readiness probe period/thresholds (Helm values, not daemon config) — the
  effective membership-change dampener; tune to trade failover latency
  against flap-induced re-homing.
- `pacer_ring_members` gauge sample interval (currently 5 s) — observability
  only, no placement effect.
