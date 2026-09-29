# ADR-0046: A read that may insert takes a ticket before it is issued, and an invalidation stales it

Date: 2026-09-29 · Status: **Accepted.** Amends
[0044](0044-poison-in-flight-fills-on-invalidate.md), whose guarantee held only on the path
that claims before reading. Fixes [#55](https://github.com/aws-samples/sample-pacer/issues/55).
No measurement: the defect is established from the code and reproduced by a test, and the fix
is pinned by tests, not a benchmark.

## Context

[0044](0044-poison-in-flight-fills-on-invalidate.md) fences a fill against a write's
`Invalidate` by poisoning the key's claim in `FillRegistry`, and `insert_fenced` refuses a
poisoned claim. Its argument was that since [0040](0040-one-backend-read-per-chunk-key.md) the
home's read claims the key before issuing its backend GET, so an invalidation during that read
always has a claim to act on.

That is true of the coalescing leader (`FillCtx::fetch_owned`, `FillClaim::Lead`) and of no
other path that inserts. These read first and claim only to insert:

- `FillCtx::fetch_from_backend` with `fill = true`, reached when coalescing is off
  (`PACER_FILL_COALESCE=false`), on `FillClaim::Busy`, and on a follower's fallback after its
  leader failed or was poisoned;
- the layer-1 admit of peer-fetched bytes (`FillCtx::maybe_admit_local`,
  [0016](0016-multi-copy-replication.md)), claimed after the peer fetch returns;
- the peer server's read-through (`PacerPeer::read_through`), claimed after the backend
  response arrives.

An invalidation landing during one of these reads finds no claim, so `poison` does nothing, and
`tier.forget` removes nothing because nothing is cached yet. The read then claims a free key
and inserts the bytes it read before the write. They stay cached until evicted. With coalescing
off, this is open on every miss.

`fill_coalesce.rs`'s `a_read_in_flight_across_an_overwrite_does_not_cache_the_old_bytes`
reproduces it: a backend GET that has already read the object is held while an overwrite
lands, and before this change the next GET was served the pre-write bytes from cache.

## Decision

1. **A read ticket, taken before the read is issued.** `FillRegistry::begin_read(key)` returns
   a `ReadTicket` recording the key's current epoch in a second map, `reads`. The map holds an
   entry only while at least one ticket for that key is alive, so it is bounded by reads in
   flight, not by keys ever read.
2. **`poison` also bumps the epoch** of an existing `reads` entry. A ticket whose epoch no
   longer matches is stale.
3. **The insert checks the ticket with the claim.** `FillGuard::fenced_by(ticket)` attaches the
   ticket to the claim taken for the insert, and `FillGuard::is_poisoned` is true when either the
   claim is poisoned or the ticket is stale. `insert_fenced` is unchanged and still checks on
   both sides of the tier write.
4. **Every path that claims only to insert takes a ticket first:** `fetch_from_backend` when it
   will fill, the non-home path before its peer fetch, and the peer read-through before its
   backend GET. The coalescing leader needs none; its claim already spans its read.

The epoch, rather than a flag on the key, keeps the fence precise. A write invalidates only
after its backend mutation has committed (`PacerProxy::invalidate_key`), so a read whose ticket
was taken after the poison was issued after the commit and returns the new bytes. Such a read
is not fenced and may fill. Only a read that was in flight across the invalidation is refused.

## Consequences

- **The tier cannot hold a chunk read before its key's last invalidation on this node**, on
  every insert path, which is the guarantee 0044 stated.
- **The guarantee is per node.** An invalidation fences reads on the nodes it reaches. Whether
  it reaches every node that can hold the key is [0007](0007-write-through-read-after-write.md)'s
  fan-out to the co-homes and the directory sharer set, which this ADR does not change.
- **A requester-mode populate** (a non-home pushing bytes it read to a home,
  [0041](0041-requester-identity-auth-mode.md)) is not fenced by this ticket. It carries the
  ETag of the version it read, and a read under a different ETag treats the chunk as a miss.
- **One mutex operation per fill-capable read**, on a lock separate from the claims map and
  never held with it or across an `.await`.
- **`an_invalidation_mid_fill_fences_the_leader_and_its_follower` is now deterministic.** It
  released the leader without waiting for the follower's ungated fallback read. If the leader
  released its claim first, the fallback (which read after the write) correctly inserted, and
  the test's `fills_completed == 0` failed. The test now waits for the follower first.
