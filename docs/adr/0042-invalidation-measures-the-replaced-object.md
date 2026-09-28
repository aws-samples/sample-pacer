# ADR-0042: A write's invalidation measures the object it replaces, and fails closed

Date: 2026-09-28 · Status: **Accepted.** Amends [0007](0007-write-through-read-after-write.md)
decision 3 and the last sentence of [0015](0015-chunk-granular-caching.md)'s workload
precondition. Fixes [#20](https://github.com/aws-samples/sample-pacer/issues/20). No
measurement: the defect is established from the code and each case is pinned by a test.

## Context

A write invalidates the object's header key and every chunk key the object covers
([0007](0007-write-through-read-after-write.md)). To enumerate the chunk keys it needs
a length. It took that length from the header cached on the writing node, or failing
that from a backend `HeadObject` sent **after** the write. When neither answered, it
dropped the header key and no chunk, on every node.

Decision 3 of 0007 justified that last step: "if the header is cold, nothing cacheable
could be stale". That argument comes from the Phase 1/2 model, where the one node that
held a key's header also held its chunks. It is false in three cases:

1. **DELETE through a node without the header.** After a DELETE, the HEAD can only
   answer 404. Headers are cached only on the object key's home, and chunks on their
   own homes ([0016](0016-multi-copy-replication.md)), so a DELETE through
   any other node left every chunk cached. A later write of the same key that does not
   purge them, such as a scattered PUT ([#21](https://github.com/aws-samples/sample-pacer/issues/21)),
   then serves the deleted object's bytes.
2. **Any HEAD failure.** The HEAD treated a throttle, a timeout or a 5xx like a 404. The
   write had already reached the backend, so the header was purged, the old chunks
   survived, and the next GET assembled the old bytes under the new object's length and
   ETag.
3. **An evicted header.** The header and the chunks share one LRU tier, so the header can
   be evicted first. On a single node the code skipped the HEAD entirely, on the same
   "no header, no chunks" reasoning.

A HEAD after the write has a fourth, smaller flaw: it measures the *new* object. An
overwrite with a shorter object left the old chunks past the new length orphaned. They were
unreachable, so this wasted capacity but never served a wrong answer.

## Decision

1. **Measure before forwarding.** Every write that invalidates (PUT passthrough,
   `CopyObject`, `DeleteObject`, `DeleteObjects`, `CompleteMultipartUpload`) learns the
   length of the object it is about to replace **before** it reaches the backend. It uses
   the local header if there is one, otherwise a backend `HeadObject`, on every node,
   single-node included.
2. **A clean 404 means nothing to purge.** The key did not exist, so no chunk under it
   can be current. Only the header key is dropped.
3. **Anything else refuses the write.** A HEAD that fails for any other reason, or that
   answers without a length, returns `503 ServiceUnavailable` to the client. Nothing has
   changed yet, so the client's retry (every AWS SDK retries a 503) finds the backend and
   the cache still in agreement. `pacer_writes_refused_total` counts these refusals. A
   `DeleteObjects` batch with one unmeasurable key is refused whole.
4. **Invalidation itself still never fails a write.** An unreachable node after the write
   is logged and counted, as 0007 decided. This ADR changes when the length is learned
   and what an unknown length means, not how the purge reaches the holders.

## Consequences

- **Correct by induction, not by case analysis.** If every write purges the chunks of the
  object it replaced, then after any write no chunk of any earlier version remains. That
  includes the shrinking overwrite, which no longer orphans anything. The base case is a
  key whose history went entirely through PACER. Writes made directly to the bucket are
  outside this, as they always were
  ([#26](https://github.com/aws-samples/sample-pacer/issues/26)).
- **Cost: one `HeadObject` per write on a node that holds no header for the key.** Before,
  this was paid after the write, and only in cluster mode. Now it is paid before the write,
  on every node. It is a metadata call with no per-GB charge. Writes are the rare path for
  the target workload, which writes a new name per version
  ([0016](0016-multi-copy-replication.md)). `DeleteObjects` pays one per
  key, serially.
- **A new client-visible failure.** A backend that is failing HEADs now fails writes that
  used to succeed. That is the intended trade: a write that succeeds and then serves stale
  bytes is worse than one the client retries.
- **Still racy under concurrent writers to one key.** Two writers can each measure the
  same predecessor. This was already outside 0015's immutable-key precondition, and this
  ADR does not widen it.
- **Not covered here:** the scattered PUT skips invalidation for a key the backend does not
  have ([#21](https://github.com/aws-samples/sample-pacer/issues/21)), and it caches the
  header on the coordinator ([#19](https://github.com/aws-samples/sample-pacer/issues/19)).
  Until those are fixed, 0015's statement that invalidation "bounds the window to in-flight
  reads regardless" holds for every write except the scatter.
