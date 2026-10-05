# ADR-0049: A restarted daemon confirms each object's version before serving it from cache

Date: 2026-10-05 · Status: **Accepted.** Fixes
[#64](https://github.com/aws-samples/sample-pacer/issues/64). Extends the version witness
[0041](0041-requester-identity-auth-mode.md) introduced for `auth.mode: requester` to
`auth.mode: node`.

## Context

The cache directory is a `hostPath` on the node (`cache.hostPath`), so it outlives the daemon's
pod. A rolling update, an OOM kill or a crash brings the next daemon up over the previous one's
files, and both disk tiers rebuild from them: the chunk store ([0033](0033-chunk-store-owns-the-disk-tier.md)) by
scanning its slot headers, and foyer by its own recovery, which is on by default and which a
graceful shutdown feeds by flushing every in-memory entry to disk.

Nothing recovered that way says whether it is still current. Two cases serve the wrong bytes,
and a test that restarts a daemon over its own directory reproduces both on both tiers
(`crates/pacer-daemon/tests/daemon/restart.rs`):

1. **An overwrite while the node was down.** A node that is not in the ring is sent no
   invalidation ([0007](0007-write-through-read-after-write.md)), so when it comes back it serves the
   chunks and the header it had before the write.
2. **An invalidation the node did receive.** `ChunkStore::remove` updates only the in-memory
   index; the slot on disk keeps its header until a later write reuses it. foyer keeps no
   tombstone for a removed entry unless its tombstone log is enabled, and it is not. A restart
   therefore recovers entries an invalidation had removed. With foyer the result can be a mix:
   chunks still in memory at the invalidation are gone, chunks already demoted to disk come
   back.

In node mode a cached chunk carries no version witness at all ([0015](0015-chunk-granular-caching.md)
left it unchecked), so there is nothing to compare a recovered chunk against.

## Decision

**A cached object header is used only once this process has confirmed it against the backend,
and a cached chunk is served only under the ETag of the header the read resolved — in both auth
modes.**

1. **Headers are confirmed once per object per process.** The proxy keeps the set of object keys
   whose current version it has seen from the backend. It starts empty, so after a restart the
   first read of each object sends one `HeadObject` even when a header is cached. A matching
   ETag confirms the cached header; anything else replaces it with the backend's and removes the
   cached one. A cache miss sends the same `HeadObject` it always did and confirms the object
   too. Steady state is unchanged: one `HeadObject` per object per process, not per read.
2. **The set is bounded and starts over when full** (65 536 objects). Forgetting an object costs
   one more `HeadObject` on its next read, so an exact LRU would buy nothing a reset does not.
3. **Every node-mode fill records the version it filled.** The proxy's own fills are tagged with
   the ETag of the header the read resolved; a peer's read-through is tagged with the ETag of the
   ranged GET response it filled from. The write paths already recorded one.
4. **A chunk is served only when its witness equals the read's ETag**, locally and from a peer,
   on the body path and on the delivery path ([0026](0026-client-supplied-target-memory.md)),
   including a holder's one-sided write into client memory, whose response now carries the
   holder's witness. A mismatched local copy is **forgotten**, not just skipped, because the chunk
   store's `put` ignores a key it already holds and the re-fill would otherwise never replace it.
   A mismatched peer copy is skipped and the holder is sent an `Invalidate` for that key, so a
   stale copy at a home does not stay uncached cluster-wide.
5. **A confirmation that cannot reach the backend fails the read**, as a miss already does. A
   recovered header is never served unconfirmed.
6. **No ETag, no cache.** An object whose backend reports no ETag has nothing to compare, so its
   header is re-checked on every read and its chunks are never served from cache. S3 always
   returns an ETag.

## Consequences

- **A restart keeps its warm cache.** Over an unchanged object, the restarted daemon sends one
  `HeadObject` and serves every chunk from the recovered tier; nothing is re-read.
- **Invalidation stays an in-memory operation.** No write is added to the invalidation path; the
  witness check makes a resurrected entry a miss instead of preventing the resurrection.
- **Chunks filled before this change have no witness**, so the first read of each after the
  upgrade re-reads it from the backend. A rolling upgrade also re-reads chunks that peers still
  on the previous version serve, since those carry no witness either.
- **Two new series:** `pacer_cache_revalidations_total{outcome="current"|"stale"}` counts cached
  headers checked against the backend, and `pacer_cache_stale_chunks_total` counts chunks dropped
  for the wrong witness. After a restart the first climbs by one per object read, then stops.
- **Not covered:** an object overwritten around PACER while the node stays up is still served from
  cache until it is evicted, as before. Bounding that is
  [#72](https://github.com/aws-samples/sample-pacer/issues/72), which can reuse this mechanism by
  letting a confirmation expire.
