# ADR-0016: Multiple copies — requester-local admission and top-R homes

Date: 2026-07-12 · Status: Accepted (implemented, B3 — see
planning/13-multi-copy-replication.md)

## Context

ADR-0012 mandates exactly one cluster-wide copy per key to maximize aggregate
capacity; ADR-0015 spreads *placement* of large objects across chunk homes.
What neither solves is a **hot chunk**: under a 1000-node restore storm every
node wants the same chunks in the same seconds, and a single home per chunk
still serializes that fan-in on one NIC. Prior art is unanimous that the fix
is *bounded replication of hot data, not global duplication*: Facebook's
memcache (hot-key replication + leases), CRUSH (primary + replicas),
consistent-hashing-with-bounded-loads.

## Decision

Two independent, composable relaxations of ADR-0012's single-copy rule:

1. **Requester-local fill with frequency-gated admission.** Requesters MAY
   keep a local copy of a peer-owned chunk, admitted only after the chunk
   proves hot (TinyLFU-style: admit on the K-th fetch within a window, not
   the first). Cold chunks keep ADR-0012 semantics exactly — stream-through,
   never stored — so aggregate capacity still ≈ sum of node capacities for
   the cold tail. A hot chunk's home serves each *node* at most a few times;
   after that the cluster's read load is fully local. Locally admitted copies
   are registered with the directory (ADR-0017) so other nodes can fetch from
   them, turning every admitting requester into additional serving capacity.
2. **Top-R homes.** The rendezvous ranking (`ranked()`, already in
   `pacer-ring`) designates the top **R** nodes as co-homes per chunk. All R
   fill on read-through; requesters pick among them (random-of-two on the
   ring rank, or least-loaded once the directory carries load hints). R
   multiplies both serving bandwidth and fill-storm resilience at the cost of
   R× storage for *every* chunk, hot or not.

Layer 1 is the primary mechanism (it self-tunes to actual heat); layer 2 is
the floor that removes the single-point-of-departure problem (a home dying
mid-storm) and smooths the cold-start spike before layer 1 has signal.

Write invalidation (ADR-0007/0012) loses its "only two nodes can hold a key"
bound: invalidation now goes to all R homes plus the directory's sharer set
(ADR-0017), awaited as before. The set is known — no broadcast, just fan-out
to listed holders.

## Trade-offs

Pros:
- Hot-chunk fan-in drops from O(total reads) to O(nodes × admission
  threshold) on the home, then to ~zero as local copies serve.
- Storm resilience: R co-homes mean a node failure during restore degrades
  bandwidth by 1/R instead of stalling that chunk's readers entirely.
- Both layers preserve deterministic placement — no coordination or
  rebalancing added; ADR-0014's contract untouched.

Cons:
- Aggregate effective capacity shrinks by the replicated fraction; the
  admission gate bounds this to genuinely hot data, R bounds it to R× for
  the rest. (Checkpoint working sets are far below cluster capacity:
  ~1.8 TB vs. hundreds of TB of NVMe at 1000 nodes — accepted.)
- Invalidation fan-out grows from ≤2 nodes to R + sharer-set size, and its
  latency (awaited, per ADR-0007) now tails on the slowest listed holder.
  Mitigation: checkpoint objects are effectively immutable (new name per
  write); mutable-object workloads should keep R low.
- TinyLFU admission adds a frequency sketch per node (memory: fixed, small)
  and a new class of tuning (see Knobs).
- **Replication relieves *data* fan-in, not *metadata* fan-in.** Layer 2
  replicates chunk data across R homes, but the directory shard for a chunk is
  single-homed (ADR-0017) and **cannot** be replicated the same way — ADR-0020's
  one-sided lookup requires a single writer per shard. So a storm-hot chunk's
  directory entry still lives on one node with no layer-2 equivalent; metadata
  heat is relieved *only* by ADR-0020's NIC-served lookups, and only on EFA
  clusters. On the v1/RPC path (or non-EFA nodes) the directory hotspot on a
  scorching chunk is unmitigated — a known ceiling, bounded by entry size (bytes,
  not chunk bytes) and the fallback path, and the reason ADR-0020 exists.
  **Open (future roadmap):** k directory replicas per home are possible *because*
  the directory is soft state — they need not stay in sync (each just points at
  where data is cached, and a stale pointer costs one `NotCached` retry, ADR-0017).
  Each replica stays a single-writer table (ADR-0020 ABI per replica); readers
  pick one, divergence is more benign staleness. Not in scope now; noted as the
  metadata-replication floor if the storm benchmark shows the single-home ceiling
  binds before ADR-0020 lands.
- Layer 1's benefit (drain the data hotspot) comes with a metadata cost:
  locally-admitted copies announce `admit` to the chunk's directory home
  (ADR-0017), so under a storm thousands of admits for one hot chunk converge on
  its single home. `max_sharers_tracked` bounds the entry *size* and the "widely
  held" sentinel lets the home stop recording, but each admit still lands there —
  the same metadata hotspot as above, drained by ADR-0020 on EFA and bounded by
  the fallback elsewhere.

## Knobs

- `replication_r` (default **2**) — co-homes per chunk. 1 = ADR-0012
  semantics (layer 2 off). Raise only with benchmark evidence; each step
  costs cluster-wide storage and invalidation fan-out.
- `local_admission_threshold` (default **2** fetches) and
  `local_admission_window` (default **60 s**) — how hot a peer-owned chunk
  must be before a requester keeps it. Threshold 1 ≈ CDN-style
  cache-everything (kills capacity); very high ≈ ADR-0012. Note the storm-profile
  tension: in a *pure* restore storm each node reads each chunk **once**, so
  threshold-2 may never fire on the storm's own reads — layer 2 (top-R) carries
  the storm, and layer 1 fires for workloads with intra-node re-reads (repeated
  epochs, multi-run resume). If the benchmark shows layer 1 must contribute during
  the storm itself, drop the threshold to 1 for the storm profile at the known
  capacity cost. The default assumes re-read workloads; the storm leans on layer 2.
- `local_copy_capacity_fraction` (default **25 %** of cache capacity) —
  bounds how much of a node's cache peer-owned hot copies may occupy, so
  storm heat cannot evict the node's own homed chunks wholesale.
- Requester home-selection policy (`ring_rank_random_2` default vs.
  `least_loaded` once ADR-0017 load hints exist).
