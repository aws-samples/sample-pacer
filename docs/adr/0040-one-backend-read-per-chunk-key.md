# ADR-0040: One backend read per chunk key, not one insert

Date: 2026-09-23 · Status: **Accepted, on by default, one flag back.** No measurement of what it
is worth on hardware; the defect it removes is established from the code and pinned by a test
that counts backend reads.

The read path already claimed "at most one fill per chunk key node-wide". It was true, and it
was not the claim anybody wanted. `FillGuard` was taken **after** the backend read, from inside
`maybe_fill`, so N clients arriving together on one cold chunk each issued a full ranged
`GetObject` and then N−1 of them found the key claimed and skipped the insert. **The writes
were deduped; the reads were not.** Nothing in `/metrics` said so — duplicated backend GETs
were inferable only from the gap between `pacer_cache_misses_total` and
`pacer_fills_completed_total`, which also folds in every read that was never admitted.

## The decision

1. A home's read of a missed chunk **claims the key first**, then reads
   (`FillCtx::fetch_owned`). Whoever finds the key free **leads**.
2. A request that finds a leader mid-read **waits for the leader's bytes** instead of issuing
   its own GET, and is served from them.
3. **Only the leader inserts.** The follower never touches the tier, so the two-holder
   invariant of [0012](0012-owner-read-through-cluster-fill.md) is untouched and
   `pacer_fills_completed_total` counts one fill where it used to count one or two depending on
   timing.
4. The leader **publishes before it inserts**. A waiter needs bytes, not a cache entry, and
   `put_chunk` is a device write that can be slow or fail.
5. `config.fillCoalesce` / `PACER_FILL_COALESCE` restores the old path. Default `true`.

The delivery path ([0026](0026-client-supplied-target-memory.md)) routes through the same
`fetch_owned`, because its own doc already promised that its source order "mirrors
`resolve_chunk` exactly" and because that is the path where the fan-in actually has this shape:
N ranks of one job pulling one checkpoint into N pieces of their own memory are N requests for
the same chunk keys, differing only in destination.

## Why this can remove a backend read and cannot add one

The two non-leading outcomes are **the same call the path used to make**, `admit` and all:

* A key claimed by a path that publishes nothing — the peer server's read-through, or a layer-1
  admit of peer-fetched bytes — answers `Busy`, and the caller fetches its own exactly as
  before. Those two callers already hold the bytes they are about to insert, so there is no
  read to share; making a waiter block on them would couple a local client's latency to how
  fast a *remote* requester drains its stream, which buys nothing.
* A leader that publishes nothing — its read failed, or its client disconnected and took the
  future with it — wakes its waiters and they fetch their own.

So the worst case is the status quo plus one channel wake-up, and
`pacer_fill_coalesce_fallbacks_total` counts exactly how often that happened.

## The registry is three states, and the third one is the small-change lever

`FillState` is `Fetching(Sender)` / `Filled(Bytes)` / `Exclusive`, not a bare key, because a
second arrival has to tell three situations apart: bytes are coming (wait), bytes are already
here (take them), nothing will ever be published (fetch your own). `Exclusive` is what keeps
this change small — the peer read-through and the layer-1 admit claim through the unchanged
`FillGuard::for_fill` and behave exactly as they did.

`Filled` closes a race that the obvious two-state shape has and loses silently.
`tokio::sync::broadcast` buffers nothing for a receiver created after a send, so between the
leader's publish and the release of its claim — a whole `put_chunk` wide — a subscriber would
get a channel that has already fired, wait for it to close, and *then* fall back. Correct, and
a pessimisation against not coalescing at all. Parking the bytes in the entry makes that window
a fast path instead.

## What it costs, stated plainly

**One request's latency now depends on another request's read.** That coupling did not exist
before and it is the reason this is a knob rather than a constant. Three things bound it:

* A leader that fails or vanishes releases its claim in `Drop`, which drops the last `Sender`
  and wakes every waiter at once. There is no timer, because a waiter is subject to exactly the
  retry budget its leader is (`pacer_backend::retry`, three attempts with a clamped backoff) —
  a leader started *earlier*, so its remaining time is no worse in expectation than a fresh
  read's.
* A waiter's future being dropped is safe: `WaiterTicket` lowers `pacer_fill_waiters` in `Drop`
  for the same reason `FillGuard` exists at all — `chunked_body`'s pipeline drops a chunk
  resolution's future outright on client disconnect, so the line written after the `.await` is
  precisely the one that never runs.
* It is observable *during* the incident, not only after: `pacer_fill_waiters` pinned with
  `pacer_fill_coalesced_total` flat is a stuck leader holding N requests.

**Memory goes down, not up.** N concurrent readers of one cold chunk used to hold N separate
chunk buffers; they now share one `Bytes`, and each slices its own view of it. The per-request
bound is still `fill_parallelism × chunk_size`.

**The published value is the backend's `Bytes`, never `cached_bytes`' output.** On an
[0028](0028-cache-ram-tier-is-the-registered-arena.md) node that output is a registered slab
frame, and handing N waiters a refcount on one frame would pin it until the slowest of them
finished streaming — the frame pool is bounded and sized for the resident set, not for readers
in flight.

## Scope: the home's read, and nothing else

A non-home request still fetches from a peer and falls back to the backend unchanged
([0016](0016-multi-copy-replication.md) layer 2), and the peer server's read-through is still
exclusion-only. Two gaps are therefore left open on purpose, both recorded rather than fixed
here: a local GET and a peer read-through of the same chunk still double-fetch, and concurrent
readers of a chunk that is *already cached* under `diskTier=store` still each cost a `pread`
(the coalescing [0033](0033-chunk-store-owns-the-disk-tier.md) gave up, documented in
`read_chunk_entry`). Both are fan-in shapes; neither has an arm that moved its variable.

## How you can tell it engaged

`pacer_fill_coalesced_bytes_total` — backend bytes not re-fetched — is the headline, and
`pacer_fill_coalesced_total` alone cannot state it because chunks differ in size
([0015](0015-chunk-granular-caching.md) clamps the last one to the object).
`pacer_fill_waiters` is the live view. `pacer_fill_coalesce_fallbacks_total` is the cost.

`pacer_fill_inflight` keeps its definition but changes shape: a leader holds its claim across
the whole backend read, so a cold multi-chunk read now keeps `fill_parallelism` claims up for
the duration of the reads rather than only for the inserts after them. A step change there at
this ADR is the mechanism engaging, not a leak.

## Consequences

* **A checkpoint fan-in stops multiplying the backend.** How much that is worth is
  **unmeasured** — this ADR claims a defect removed, not a rate. The arm that would say is the
  one that runs the same fan-in with `fillCoalesce` false, which is why the knob exists and why
  the chart renders it in both directions.
* **The old path was worse than its own account of it.** The insert dedup only ever worked while
  two inserts *overlapped*. `without_coalescing_the_same_race_costs_two` pins the case where
  they do not: two reads finishing at different times wrote the same chunk twice.
  `fills_completed` was 2, not 1. Only the leader inserting makes that unconditional.
* **`FillGuard` now has two claim kinds**, and a caller wiring a publish to the exclusive one
  loses the optimisation rather than corrupting the entry — asserted, because the failure would
  otherwise be silent.
