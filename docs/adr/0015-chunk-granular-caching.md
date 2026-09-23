# ADR-0015: Chunk-granular caching, placement by chunk

Date: 2026-07-12 · Status: Accepted

## Context

Phases 1–2 cache **whole objects** keyed `"{bucket}/{key}"`, with exactly one
copy cluster-wide on the rendezvous owner (ADR-0012), and refuse to fill on
range reads (ADR-0011). The target workload breaks both assumptions: LLM
checkpoint restore — order of 1.8 TB in ~256 files (~7 GB/file) — read by up
to ~1000 nodes near-simultaneously. Whole-object placement puts each 7 GB
file on **one** owner: its NIC and cache become the serialization point for
999 readers (the hotspot ADR-0012 explicitly deferred to Phase 3). And
training frameworks read checkpoint *slices* (tensor ranges), which today's
whole-object cache can't fill from (ADR-0011).

Prior art: 3FS and Mooncake both cache/place at block granularity for exactly
this reason; a hot large object then spreads across hundreds of placement
targets automatically.

## Decision

The cluster cache unit becomes the **chunk**: a fixed-size, aligned slice of
an object, keyed `"{bucket}/{key}#{chunk_index}"`. The chunk size is **part of
the key derivation** — changing it changes every chunk key, so old and new
entries never alias (they orphan, never corrupt; see Knobs). There is no separate
"epoch" counter; key-embedding is the whole mechanism. All
Phase 2 placement machinery applies per chunk, unchanged: rendezvous hash
(ADR-0014 contract) assigns each **chunk** a home; owner read-through,
invalidation, and fallback (ADR-0012) operate per chunk.

- **Range reads become cacheable**: a ranged GET maps to the covering chunk
  set; each chunk is fetched via its own home (or locally) and the response
  is assembled. This **supersedes ADR-0011's range restriction** — the cost
  argument that motivated it (a 1 GB range on a 500 GB object must not pull
  500 GB) is preserved *by construction*: only covering chunks are ever
  filled, never the whole object.
- Object metadata needed to serve responses (`ETag`, `Content-Type`, total
  length, `Last-Modified`) is cached once per object alongside chunk 0's
  home in a small header entry, filled on first read-through.
- Write invalidation (ADR-0007/0012) invalidates the header entry and all
  covering chunk keys at their respective homes. A whole-object PUT knows the
  length and thus the chunk set; `DeleteObject(s)` and multipart complete
  invalidate via the cached header's length (or, if the header is cold,
  nothing is cached that could be stale — same reasoning as ADR-0012's
  two-holders argument, now per chunk).
- Small objects (≤ 1 chunk) degenerate to exactly the Phase 2 path.

## Trade-offs

Pros:
- A hot 7 GB file spreads over ~450 homes (16 MiB chunks) instead of 1 —
  the 999-on-1 fan-in disappears at the placement layer before any
  replication (ADR-0016) is even considered.
- Checkpoint slice reads (the ADR-0011 pain case) are finally cacheable.
- Restore-storm S3 cost: each chunk is read from Express exactly once
  cluster-wide, regardless of reader count.
- Chunks are the natural transfer unit for the RDMA data plane (ADR-0018)
  and the entry unit for the directory (ADR-0017).

Cons:
- Key-space and metadata multiply by objects/chunk-size (the target workload
  is ~115k chunks — trivial; a many-small-files workload is unaffected).
- A whole-object GET on a cold cluster fans out to many homes (~450 backend
  GETs for a 7 GB file at 16 MiB) instead of one — a request-count amplification
  on cold fill. Accepted because Express cost is bytes-dominated (ADR-0002) and
  each chunk is still read exactly once cluster-wide; per-chunk read-through must
  be pipelined to keep the client stream ordered (implementation risk, not a
  design risk).
- **Workload precondition (shared with ADR-0016/0007):** cross-chunk read
  atomicity holds only because the target workload's objects are effectively
  immutable — checkpoint writers write a new name per version, never overwrite in
  place. Under that precondition a multi-chunk reader cannot observe a torn object
  (the bytes it assembles all belong to one immutable version). A **mutable-object
  workload breaks this** — concurrent overwrite + multi-chunk read can tear across
  boundaries; such workloads must keep objects whole (≤ 1 chunk) or accept the
  window. This is a documented boundary of PACER's applicability, not just an
  accepted edge case. ADR-0007's write-through + awaited invalidation bounds the
  window to in-flight reads regardless.

## Knobs

- `chunk_size` (default **16 MiB**, config per ADR-0013) — the central Phase 3
  tunable. Smaller → better placement spread and less read amplification on
  small slices, but more per-chunk overhead (directory entries, commands,
  fills) and worse S3 GET efficiency on cold fill; larger → the reverse.
  16 MiB starts near foyer's happy range and S3 Express's sweet spot;
  **decide finally via the restore-storm benchmark** (Phase 3 exit criterion).
  Changing it is a **cache-flush event**: chunk keys embed the size, so a
  changed value orphans (never corrupts) all existing entries. Pin per
  cluster; never mix values across a rolling update.
- `min_object_size` (existing) — objects below it bypass chunking entirely. It
  gates *whether* to cache; `chunk_size` gates *how* to split what is cached. With
  defaults (4 MiB min, 16 MiB chunk) a 4–16 MiB object is cached as a single chunk
  (the ADR-0012 degenerate path).
- `max_object_size` (existing, ADR-0002) — an *optional* whole-object admission
  cap; objects above it are proxied through uncached. Under whole-object caching
  it was mandatory and clamped down to `block_size` — a foyer block is the largest
  single entry the disk tier can persist, so a bigger object-as-entry would sit in
  RAM and never flush. **Chunk-granular caching removes the rationale entirely**:
  an object is stored as many `chunk_size` entries (never one object-sized entry),
  and a multi-chunk read's memory is bounded to `fill_parallelism × chunk_size`
  regardless of object length — so there is no memory or disk reason to cap object
  size at all. `max_object_size` therefore **defaults to unbounded** (unset): PACER
  chunk-caches an object of any size, distributed across the ring — the whole point
  of chunking is a checkpoint far larger than any one node's RAM. A value is now
  only an operator safety valve for pathological objects, decoupled from
  `block_size`; the sole residual block-size constraint is `chunk_size ≤
  block_size` (16 MiB ≤ 1 GiB by default), which the daemon *warns* about rather
  than silently clamps. The old mandatory clamp bypassed every object larger than
  one block — exactly the large checkpoints this ADR exists to cache (found in
  Phase 4 F1: a 3.74 GiB Llama-3.1-8B shard bypassed the cache entirely, so no
  chunk, peer-fetch, or RDMA path was ever exercised).
- Per-chunk fill parallelism for multi-chunk client GETs (bounded pipeline
  depth; default TBD in implementation, benchmark under storm).
