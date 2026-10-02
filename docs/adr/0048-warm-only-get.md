# ADR-0048: A warm-only GET fills the cache and answers header-only

Date: 2026-09-29 · Status: **Accepted.** Fixes
[#37](https://github.com/aws-samples/sample-pacer/issues/37).

## Context

Today the cache warms only as a side effect of a client's own read: the first job to touch
an object pays the S3 fill for every chunk, and so does every job after an eviction. There is
no way to have a model or a checkpoint already warm before the workload that needs it starts.

The read path already has the shape a warm needs. `serve_get`
([`crates/pacer-daemon/src/proxy/read.rs`](../../crates/pacer-daemon/src/proxy/read.rs)) and its
`auth.mode: requester` counterpart `serve_get_requester` resolve an object's header, decide
admission, and — in `requester` mode — run the caller's own authorization probe, all before a
single chunk is fetched. What they do with a resolved chunk (stream it) is the only part a warm
does differently (drop it). Client-memory delivery (ADR-0026) already sends a similar
header-only `200`, so this reuses that shape rather than inventing a new one.

## Decision

**A GET carrying `x-pacer-warm: 1` runs the read path through admission, resolves every
covering chunk with the read path's own bounded parallelism, and answers `200` with an empty
body and `x-pacer-warmed: <bytes>`.**

1. **Same authorization as the read it stands for.** In `auth.mode: node` this is unchanged —
   the daemon's own identity reads S3. In `auth.mode: requester` the warm runs the authorization
   probe first, exactly as a read does; a caller who cannot read an object cannot warm it either.
2. **Synchronous, deliberately.** Nothing runs after the answer. In `requester` mode the daemon
   can only read S3 on the caller's signature, which lives no longer than the request; and
   ADR-0011 already recorded why a fill nobody is waiting on has no back-pressure. A warm of a
   very large object is the caller's job to slice into several requests, not the daemon's job to
   run in the background.
3. **A request the cache would not serve is skipped, not streamed.** The same bypasses an
   ordinary GET takes — `Cache-Control` that bypasses or stores nothing, a conditional the cache
   cannot honour, an object outside the admitted size band — answer `x-pacer-warmed: 0` plus
   `x-pacer-warm-skipped: <reason>` instead of running `inner.get_object` (node mode) or
   `pass_through` (`requester` mode). Streaming an object a warm cannot cache back to a caller
   that asked for nothing would silently turn "warm this" into "read this", at whatever size the
   object happens to be.
4. **Layer-1 admission is skipped.** A warm read through a non-home node
   ([0016](0016-multi-copy-replication.md) layer 1) must not admit a local copy on the node that
   ran the warm — that node is not one of the workload's own readers, and a warm should place
   copies at a chunk's homes, not wherever it happened to run. `FillCtx` gains a `warm: bool` that
   `maybe_admit_local` checks; everything else about chunk resolution — a local hit, a peer fetch,
   a backend fill — is the read path's own, unchanged.
5. **A daemon that predates this degrades safely.** It ignores the unknown header and answers a
   normal body, which the caller drains; the object ends up warm either way. `x-pacer-warmed`'s
   *presence* is therefore the completion signal a caller checks, the same contract
   `x-pacer-delivered` already uses.
6. **`pacer-daemon warm`**, a subcommand of the daemon binary (not a second image to build, scan
   and pin), does the caller's half: expand `s3://bucket/prefix/`, an exact key, or a manifest of
   either into objects (`ListObjectsV2` / `HeadObject`, through the daemon exactly as a read is);
   slice each object into bounded byte ranges (`--slice`, default 1 GiB) so no single request
   runs long regardless of object size; send warm GETs at bounded concurrency (`--concurrency`);
   refuse a total above `--max-bytes`; and report what warmed, what was skipped, and what failed.
   Re-running it is safe and cheap — an already-warm slice is a cache hit — which is what makes
   it safe to run as a Kubernetes Job that a caller submits and does not wait on.
7. **The command speaks each mode's own client contract.** `--endpoint` is `node` mode's: the
   daemon as the S3 endpoint, bucket aliases, placeholder credentials. `--proxy` is `requester`
   mode's ([auth.md](../helm/auth.md#the-client-contract)): the daemon's TLS listener as a
   forwarding proxy, real bucket names, the caller's own credentials, SDK endpoint resolution
   (so an Express bucket is addressed on its zonal endpoint). Exactly one is required.
   The stock SDK HTTP client cannot do the second: through a proxy it opens a `CONNECT` tunnel
   for any `https://` target, and the daemon refuses tunnels because it cannot see into one. So
   `--proxy` swaps in a small client (`crates/pacer-daemon/src/warm/forward.rs`) that dials the
   daemon over TLS whatever the target and marks the connection as a proxy, which makes hyper
   send each request in absolute form inside that session — botocore's
   `proxy_use_forwarding_for_https`. **Both hops are TLS, always:** `--proxy` accepts only an
   `https://` URL, and because the daemon follows the scheme a request names when it forwards,
   a plaintext client hop would have made its hop to S3 plaintext too. S3 Express's zonal
   endpoints do not answer plain HTTP in any case.

## Consequences

- **Cost is proportional to what is warmed, not open-ended.** A warm reads exactly the covering
  chunks of what it names, at the caller's own concurrency — the same cost invariant ADR-0011
  and ADR-0015 already established for a read.
- **A warm fills one of each chunk's R homes, and at R = 2 that is about half of what a reader
  elsewhere will ask for.** A read resolves a chunk at a co-home chosen from the reader's own
  node name, so a reader on the node that ran the warm asks the co-homes the warm filled, and a
  reader on any other node asks the unfilled one for roughly half the chunks — whose first read
  then comes from S3. Observed on a three-node, R = 2 ring in both modes: a read from the warming
  node was all hits, while a re-warm from another node read 34 of 64 chunks from S3.
  Filling every co-home is [#38](https://github.com/aws-samples/sample-pacer/issues/38), and is
  what makes a warm worth its name for a multi-node restore.
- **In `requester` mode a warm's copies at other homes are pushed, not awaited.** A chunk whose
  home is another node is read by the node that received the warm and pushed to that home
  fire-and-forget, as for any read; the warm can answer before the home commits it. The same
  change as #38 — synchronous pushes to every co-home — closes this.
- **A byte range is an internal slicing detail, not a caller-facing feature.** The command warms
  whole objects; a future user-facing partial-object warm would be its own decision.
- **`pacer_warm_requests_total`** (labeled `warmed`/`skipped`/`failed`) and
  **`pacer_warm_bytes_total`** are new; a warm's chunks still move the read path's own
  `pacer_cache_hits_total`/`pacer_bytes_filled_total` like any read's, so the two together answer
  both "how much of this was a warm" and "did it hit or fill".
- **`ops_total{op="warm_object"}`** is counted apart from `get_object`, for the reason the
  ADR-0030 pre-flight is left out of `ops_total` altogether: a warm is not a read, and folding
  it into `get_object` would make `pacer_delivery_requests_total ÷ ops_total{op="get_object"}`-shaped
  ratios lie.
