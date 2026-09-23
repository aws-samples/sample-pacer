# ADR-0007: Writes are never cached (write-through) → read-after-write without invalidation

Date: 2026-07-10 · Revised 2026-07-12 · Status: Accepted · The write-through /
read-after-write *principle* is original and unchanged. The invalidation
*mechanism* is restated here to match the cluster the system actually became:
per-chunk placement (ADR-0015), plural copies (ADR-0016), and a directory sharer
set (ADR-0017) replaced the original "only the owner can hold a key, so drop its
entry" argument with "every holder the directory lists is invalidated, awaited."
The Phase 1/2 two-node model is preserved as a note at the end.

## Context

Read-after-write consistency is reachable with NO invalidation protocol at all, provided
writes are proxied straight to the backend and only GETs ever touch the cache — the cache
then has nothing stale to invalidate, because it never held the new bytes.
Directory buckets offer no event notifications (planning/03), so
event-driven invalidation isn't even available — the write-through choice is both
forced and clean.

What changed after Phase 2: the cache unit is the chunk (ADR-0015), a chunk may be
held by more than its home (requester-local admission + top-R co-homes, ADR-0016),
and the set of holders is tracked as soft state in the ring-sharded directory
(ADR-0017). "Drop the one owner's entry" is no longer sufficient, because a key can
now be resident on many nodes. The invalidation target becomes *the holder set the
directory names*, which is still bounded and known — never a broadcast.

## Decision

All mutating operations (PUT, multipart complete, DELETE, COPY, Append, Rename) are
**proxied directly to S3 Express and never populate or update the cache.** On a
write, the daemon invalidates every cached representation of the affected object
before returning success to the client:

1. **Locally**, drop any cached chunks + the object header entry (ADR-0015) on the
   writing node.
2. **At each affected chunk's home**, send an **awaited `Invalidate`** (ADR-0012,
   upgraded from the original best-effort hint). The home clears its entry and
   **fans the invalidation out to every holder in that chunk's directory sharer
   set + all R co-homes** (ADR-0016/0017), also awaited. The sharer set is exactly
   the "who can hold this" bound — a known list (≤ `max_sharers_tracked` + R,
   ADR-0016/0017), so the fan-out is targeted, never a broadcast.
3. The object **header entry** (ETag/Content-Type/length, ADR-0015) is invalidated
   too; a whole-object PUT knows the length and thus the covering chunk set,
   `DeleteObject(s)` and multipart-complete derive it from the cached header (or,
   if the header is cold, nothing cacheable could be stale — the same reasoning as
   the original two-holder argument, now per chunk).

Subsequent GETs re-fetch fresh from Express (single-digit ms, ADR-0002).

**Failure handling** (inherited from ADR-0012, generalized): the backend write
already succeeded, so the client write must succeed even if invalidation is
partial. An unreachable home/holder → the write returns success with a warning and
a fallback metric; the bounded stale window is self-limiting because a holder
unreachable for invalidation is unreachable for *reads* too, so peer fetches to it
fail over to the fresh backend (ADR-0012 rule 3). Correctness degrades to eventual
freshness, never to a wrong answer served indefinitely.

## Consequences

- Read-after-write consistency still falls out of the design; there is no
  invalidation *protocol* to converge — only a targeted fan-out to a known holder
  set, awaited before the write returns.
- Consistency never depends on the RDMA path (reads/data only — ADR-0003/0018);
  invalidation rides the gRPC control plane.
- Cost: first read after a write is always a miss (single-digit ms from Express —
  acceptable). Write throughput = Express throughput (100k write TPS/bucket); the
  daemon adds no write-path caching.
- Invalidation latency now tails on the **slowest listed holder** (awaited
  fan-out), not a single owner — a real regression from the two-node model.
  Mitigation (ADR-0016): checkpoint objects are effectively immutable (new name
  per write), so the hot path rarely invalidates; mutable-object workloads should
  keep R and sharer counts low.
- Edge cases (in-flight reads racing a write, a reader assembling chunks across a
  write) resolve to eventual freshness and can observe a torn object across chunk
  boundaries — accepted, same class as ADR-0015 documents, bounded by the awaited
  invalidation window.

## Original Phase 1/2 model (superseded, kept for the record)

Before chunks and replication, exactly two nodes could hold a key: its rendezvous
owner (by design) and the writing node (from pre-epoch history). Invalidation was
"drop locally + best-effort hint to the owner," and the authoritative guard was
that ownership hashing routed a key's reads through its owner, whose entry had been
dropped. ADR-0012 made the owner hint an awaited RPC; ADR-0016/0017 replaced the
two-node bound with the directory sharer set. The principle held throughout; only
the holder-set size and the await semantics changed.
