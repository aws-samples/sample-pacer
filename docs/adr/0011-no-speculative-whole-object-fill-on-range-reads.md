# ADR-0011: Cold range reads never trigger speculative whole-object fills

Date: 2026-07-11 · Status: Accepted (supersedes the Phase 1 background-fill behavior) ·
Range restriction superseded by ADR-0015: chunked caching makes range reads cacheable
at chunk granularity while preserving this ADR's cost invariant (only covering chunks
fill, never the whole object). The no-speculative-whole-object-fill *principle* stands;
two specifics below are reversed by 0015 and marked inline — the "never beyond the
requested range" absolute (0015 fills whole *chunks*) and the "pre-stage is the only way
to warm range-read objects" consequence (0015 warms them from the range reads themselves).

## Context

Phase 1 as first implemented treated a cold range read as a signal: proxy the requested
range for fast TTFB, and concurrently fetch the ENTIRE object in the background to
populate the cache (whole-object caching was the only mode — ADR-0001's initial policy).

That speculation is wrong for our primary workload. LLM checkpoint loading is
range-read-heavy where a node often needs only a fraction of the file: resuming a
sharded/TP-partitioned checkpoint reads each rank's slice; safetensors readers pull the
header then seek to specific tensors; some nodes touch a few hundred MB of a
multi-hundred-GB consolidated checkpoint. For every such read the speculative fill:

- **multiplies cost** — S3 Express One Zone bills retrieval per GB, so a 100 MB slice
  of an 80 GB checkpoint pays for 80 GB the moment one range touches it, per node;
- **wastes NVMe and evicts warmer data** — an 80 GB entry nobody asked for whole
  churns the LRU;
- **doubles backend traffic at the worst time** — checkpoint-resume storms are exactly
  when every node range-reads the same objects simultaneously, and each miss spawns a
  competing whole-object stream;
- **hides behind the client** — the client sees a fast range response while the daemon
  quietly runs an 80 GB transfer it never observes, so the cost has no user-visible
  back-pressure.

The original rationale (second range read on the same object becomes a hit) only pays
off when most bytes of the object are eventually read on that node. That is the
whole-object-GET workload, which the dual-stream tee already covers at zero extra cost —
it caches bytes the client was sent anyway.

## Decision

**A GET with a `Range` header never initiates a backend fetch beyond that range.**
→ **Refined by ADR-0015**: a ranged GET fetches its *covering chunks* (whole chunks, so
up to ~2 chunks beyond a sub-chunk range), never the whole object. The cost invariant —
bytes billed ≈ bytes in the covering chunks, never the full object — is preserved; the
"never beyond the range" absolute is not.

- Cold range read → proxy the range, populate nothing. Cost = bytes requested, always.
- Warm range read (whole object already cached from a prior whole-object GET or
  pre-stage) → slice from cache, as before.
- Whole-object cache population happens on exactly two triggers:
  1. a whole-object GET (dual-stream tee — client paid for the bytes anyway);
  2. an **explicit pre-stage** request (Phase 4: `HeadObject` with `Range: bytes=0-0`),
     which is the operator/framework saying "I will read most of this."

Partial-object (block/chunk) caching — caching just the ranges actually read — is
explicitly out of scope for now; it changes the cache data model and invalidation story
and would need its own ADR.

## Consequences

- Retrieval cost is proportional to bytes clients actually request; no hidden per-GB
  amplification. Checkpoint-restore reads of a fraction F of an object cost F, not 1.
- Repeated cold range reads of the same object stay misses until a whole-object GET or
  pre-stage warms it. For checkpoint workloads this is the correct trade: same-AZ
  Express misses are single-digit-ms and bill only the bytes read.
- The pre-stage trigger (already on the Phase 4 roadmap) is promoted from
  nice-to-have to the *only* mechanism for warming objects that are consumed via
  ranges. Data-loading frameworks that do want whole-object warmth issue it explicitly.
  → **Reversed by ADR-0015**: range reads now warm the chunk cache directly, so pre-stage
  is no longer the *only* warming path for range-consumed objects — it remains the way to
  warm an object *ahead* of first read.
- Daemon loses the `spawn_fill` background path entirely — less code, no fill
  thundering-herd risk on resume storms.
- Metric `pacer_fills_aborted_total` no longer counts abandoned speculative fills;
  fills happen only inside client-observed streams (tee) where back-pressure is natural.
