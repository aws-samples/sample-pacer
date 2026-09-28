# ADR-0044: A write's invalidation poisons any in-flight fill of the same key

Date: 2026-09-28 · Status: **Accepted.** Amends
[0040](0040-one-backend-read-per-chunk-key.md)'s claim registry. Fixes
[#22](https://github.com/aws-samples/sample-pacer/issues/22). No measurement: the defect is
established from the code and pinned by a test that races an invalidation against a held
backend read.

## Context

A read that misses a chunk claims the key, reads the backend, and inserts what it read
([0016](0016-multi-copy-replication.md)/[0017](0017-sharded-soft-state-directory.md)). A write
that lands on the same key invalidates it: `PacerPeer::invalidate` (the RPC handler a remote
node runs) and `PacerProxy::invalidate_key` (the writer's own node) both drop the chunk from
the tier and, on the peer path, the directory entry. Nothing before this ADR ordered the
read's insert against the write's invalidation. A reader that missed the chunk *before* the
write, and whose backend GET returns *after* it, can insert the pre-write bytes right back in
— and because there is no per-chunk version epoch on the wire
([`InvalidateRequest`](../../crates/pacer-proto/proto/pacer/v1/peer.proto) carries only the
key), nothing evicts them again until LRU does, which is not bounded by the write at all.

[0040](0040-one-backend-read-per-chunk-key.md) widens this window rather than narrowing it,
which is also what makes the fix small. Before 0040, a claim existed only for the short span
around the insert (`FillGuard::for_fill`, `FillState::Exclusive` in this ADR's terms). Since
0040, the home's own read claims the key **before** issuing the backend GET
(`FillCtx::fetch_owned`) and holds it — as `FillState::Fetching`, then `Filled` — for the whole
read, so an `Invalidate` that lands during that read now always has something claimed to act
on. A follower that arrives while the leader is reading gets the leader's bytes too
(`FillClaim::Follow`), so an unfenced leader would hand *two* requesters the pre-write body
instead of one.

`CachedChunk`'s ETag witness ([`crates/pacer-cache/src/chunk.rs`](../../crates/pacer-cache/src/chunk.rs))
cannot close this: an ordinary read-path fill never sets it, and even where it is set (the
write path, and a read-path fill under `auth.mode=requester`, [0041](0041-requester-identity-auth-mode.md))
it does not survive the chunk store's disk slot format ([0033](0033-chunk-store-owns-the-disk-tier.md)'s
`SlotHeader` has no ETag field). Validating it needs a slot format change this ADR does not
make; see the corrected doc comment on `CachedChunk` for exactly what does and does not hold
today.

## Decision

1. **A fourth claim state, `Poisoned`.** `FillRegistry` (`crates/pacer-daemon/src/proxy/fill.rs`)
   gains `FillRegistry::poison(key)`, which replaces whatever claim `key` currently holds —
   `Fetching`, `Filled`, or `Exclusive` — with `Poisoned`, and `FillGuard::is_poisoned()`, which
   a claim's own holder reads. Poisoning a live `Fetching` drops its `broadcast::Sender`, which
   is what a follower's `await_published_fill` needs to fall back instead of hanging: the
   channel closes and it fetches its own bytes, exactly as it would have if no leader existed.
2. **`Invalidate` poisons before it forgets.** Both invalidation paths — `PacerPeer::invalidate`
   (`crates/pacer-daemon/src/peer.rs`) and `PacerProxy::invalidate_key`
   (`crates/pacer-daemon/src/proxy/write.rs`) — call `filling.poison(cache_key)` alongside the
   `tier.forget` they already did. A poisoned key with nobody holding it is a no-op: there is
   nothing in flight to fence, and `tier.forget` already covers a chunk that finished landing
   before the invalidation arrived.
3. **Every insert checks the claim on both sides of the write.** `insert_fenced`
   (`crates/pacer-daemon/src/proxy/fill.rs`, shared with the peer server's read-through pump in
   `peer.rs`) checks `is_poisoned()` before calling `tier.put_chunk`, and again after it returns.
   The first check is the common case — an invalidation that outran the whole backend read. The
   second is what the leader/`put_chunk` race in #22 actually needs: an `Invalidate` landing
   *during* the write is invisible to a before-only check, and re-reading the same claim the
   caller is still holding (nothing else can claim `key` until this guard releases it) is the
   only way to see it. When the second check finds the claim poisoned, `insert_fenced` forgets
   what it just wrote before returning, so the pre-write bytes are never left resident even for
   the length of one race.
4. **A poisoned leader still serves its own requester.** The bytes are already read by the time
   `Invalidate` can act on them; there is nothing else to hand that one caller. What the fence
   removes is everything downstream of that one response: the tier entry, and a *second*
   requester's copy — an already-parked follower is woken empty-handed rather than handed the
   leader's bytes, and a follower arriving after the poison is told to fetch its own
   (`FillClaim::Busy`) rather than take a `Filled` claim's parked ones.

## Consequences

- **The tier cannot hold a chunk older than its own key's last invalidation**, which is what
  #22 asked for. `crates/pacer-daemon/tests/daemon/fill_coalesce.rs`'s
  `an_invalidation_mid_fill_fences_the_leader_and_its_follower` holds a leader mid-read,
  invalidates, then releases it, and asserts both that the tier ends up empty for that key and
  that the follower's own read (not the leader's) is what answered it — no sleep, two explicit
  sync points (`pacer_fill_waiters`, the ranged-GET counter).
- **A follower already handed bytes via the broadcast channel before the poison keeps them.**
  `tokio::sync::broadcast` buffers nothing for a receiver created after a send
  ([0040](0040-one-backend-read-per-chunk-key.md)), so by the time a `Fetching` claim reaches
  `Filled` every earlier subscriber has either already received its value or is about to — this
  ADR cannot revoke a send that already happened, and does not try to. This is strictly no
  worse than before 0040 existed (a coalesced read that already reached its subscriber is the
  same request that would otherwise have read the backend itself and gotten the same stale
  answer), and the design still leans on 0015's immutable-key precondition for the general case;
  stating that precondition where users read it is [#26](https://github.com/aws-samples/sample-pacer/issues/26),
  not this ADR.
- **One more registry state, no new lock.** `poison`/`is_poisoned` take the same
  `std::sync::Mutex<HashMap<String, FillState>>` every other `FillRegistry` method does, so the
  ordering argument is unchanged: every critical section is one hash lookup with no `.await`
  inside it, and the two-sided check in `insert_fenced` reads that same map on both sides of an
  `.await`, never holding the lock across it.
- **A put_chunk failure no longer counts `fills_completed`.** `insert_fenced` returns whether
  the chunk is left in the tier, and callers now count `fills_completed`/`bytes_filled` only
  when it does — before this change, a `put_chunk` I/O error was logged but still counted as a
  completed fill. Tightening, not loosening; no test pinned the old count.
- **The peer read-through pump gained the same fence it never had.** `pump_read_through`
  (`peer.rs`) holds an `Exclusive` claim while it streams a home's own miss to a requesting
  peer, and its insert had exactly #22's race independent of 0040 (0040 only widens the window,
  it did not create it — see Context). It now calls the same `insert_fenced`.
