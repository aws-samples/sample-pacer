# ADR-0043: A scattered write invalidates every co-home it did not land on, so freshness means cache-fresh

Date: 2026-09-28 · Status: **Accepted.** Amends [0032](0032-write-scatter-populates-the-cache.md)
§ 2's precondition. Fixes [#21](https://github.com/aws-samples/sample-pacer/issues/21). No
measurement: the defect is established from the code and pinned by two regression tests
(`crates/pacer-daemon/tests/daemon/scatter/gate_cache_fresh.rs`).

## Context

[0032](0032-write-scatter-populates-the-cache.md) § 2 lets a PUT of a **new** key skip the
awaited invalidation [0007](0007-write-through-read-after-write.md) otherwise requires,
because "a fresh key has no holders to invalidate at any R." "Fresh" is decided by
`key_already_exists`, one backend `HeadObject`: a clean 404 means scatter, anything else
means overwrite and take 0007's path.

A 404 says the key is new **to the backend**. It says nothing about the cache, and "at any
R" undersells its own claim: `replication_r` (ADR-0016 layer 2, default **2**) can name more
than one home per chunk key, and a scattered write only ever offers, commits to, or (before
this ADR) invalidates the **first** of them. Two distinct ways a chunk key ends up wrong:

1. **A home an earlier write's invalidation did not reach.** An unreachable node during a
   DELETE's best-effort fan-out ([0042](0042-invalidation-measures-the-replaced-object.md)
   fixed the common cause of this, a DELETE through a node with no header, but an
   invalidation that misses an unreachable node is unaffected: it still never fails the
   write) keeps whatever it cached from the deleted version. A retried checkpoint save
   produces exactly this shape: delete the bad object, then write it again under the same
   name.
2. **A co-home the write path never contacts at all.** Only `window.homes[0]` — the
   rendezvous home — is ever offered a window, told to commit, or invalidated.
   `window.homes[1..]` hear nothing from a scattered write, accepted or not; the only way one
   of them ever holds anything is filling itself on an ordinary read, the same as any home
   does on a miss. That copy can outlive a DELETE the same way case 1's can, for the same
   reason: only `homes[0]` was ever asked.

When the next PUT of that key scatters, `homes[0]` either **accepts** its window and
overwrites whatever it had — correct, and the common case ADR-0032 § 4 expects in a balanced
all-ranks save — or **refuses** (reject-fast, § 4) or cannot be reached, in which case the
coordinator uploads and caches the new bytes itself and `homes[0]` keeps its old copy. Either
way, `homes[1..]` are untouched regardless of what `homes[0]` did. Nothing tells any of them
to drop a stale copy, because § 2's precondition said there was nothing to drop.

The read path then makes this a wrong answer, not a miss. [`chunk_sources`]
(`crates/pacer-daemon/src/proxy/cluster.rs`) resolves a chunk's source from the ring alone —
the R co-homes, rotated per reader — and does not consult the directory. A reader whose
rotation lands on a stale co-home receives its old chunk, and the correct bytes never get
asked for.

## Decision

1. **The precondition is corrected: cache-fresh, not backend-fresh, at every R.** § 2's "a
   fresh key has no holders to invalidate at any R" is true of a key new to PACER. It is not
   true of a key merely new to the backend, and it does not become true by only checking
   `homes[0]`. The corrected precondition: no co-home this write's plan can reach is left
   holding a copy this write did not land fresh bytes on.
2. **Every window invalidates every co-home but the one holding its fresh bytes.** A window's
   fresh bytes land at exactly one node: the accepting owner (`homes[0]`, when `part.owner`
   is `Some`) or this node, when nobody accepted it. `publish` now invalidates every *other*
   entry in that window's `homes` list — not only a fallen-back window's `homes[0]`, and not
   only when R > 1 happens to matter for a given window — the same awaited `Invalidate`
   [0007](0007-write-through-read-after-write.md) sends for an ordinary overwrite, addressed
   at that one chunk key, before anything commits or the header becomes visible. A co-home
   that is this node itself, when another node holds the fresh bytes, is invalidated locally
   with no RPC. That local drop goes through the same fence as a peer's `Invalidate`: it
   poisons any fill of the key this node still has in flight before forgetting the copy
   ([0044](0044-poison-in-flight-fills-on-invalidate.md)). Otherwise a local fill that is
   already past its backend read could re-insert the pre-write chunk once this write
   returns.
3. **Bounded concurrency, not one round trip per chunk.** Every invalidation this write owes
   is collected up front and run with a fixed concurrency bound
   (`INVALIDATION_CONCURRENCY`, 8), rather than one chunk key after another. A scattered
   write already has as many windows as an object has chunks; repeating the fully-serial
   shape [#23](https://github.com/aws-samples/sample-pacer/issues/23) is about, once per
   co-home on top of that, is not a cost worth adding silently.
4. **Detection is unchanged.** `key_already_exists` still asks the backend, one `HeadObject`,
   not the cache. The alternative — asking every chunk's home before deciding to scatter — is
   the cost this design exists to avoid on the common case where nothing is stale, and the
   issue's own ranking put it second, not first.

## Consequences

- **Cost is honest, not free: roughly `windows × (R − 1)` invalidations per scattered write,
  paid every time, not only on a fallen-back window.** At the suite's default R = 1 this is
  zero — there is no co-home to invalidate — but production's default `replication_r` is
  **2**, so a fully-accepted scatter that used to invalidate nothing now pays one
  invalidation per window. This is strictly more than the narrower fix this ADR first shipped
  (invalidating only a fallen-back window's own home), which left an accepted window's
  untouched co-homes exactly as wrong as before — caught by
  `an_accepted_windows_stale_co_home_is_invalidated_too`, which fails against that narrower
  version. Bounded concurrency keeps the added round trips from stacking serially on the
  client's PUT, but it does not make them free.
- **Awaited, not fire-and-forget, and that is the point.** Everything else `publish` does
  after this step is deliberately unawaited, because each of those steps only turns a
  guaranteed miss into a possible hit. This step is different: skipping it, or racing it
  against the commits below, would let a reader observe the fresh object with a stale chunk
  still in the mix. It runs first and is awaited before the client-visible write is
  considered fully published — on the client's PUT response, the same position the existing
  per-owner commit loop already occupies.
- **What this does not close, unchanged from ADR-0007.** An invalidation that cannot reach a
  home — the node is genuinely partitioned, not merely busy enough to refuse — leaves that
  home's stale copy exactly as before, whether the miss happened here or at an earlier
  DELETE. No write in PACER makes an unreachable node consistent; only that node's own
  eviction, or a later write it *is* reachable for, corrects it. That was already true of
  0007's invalidation before this ADR, on every write that was never a scatter; this ADR
  brings the scatter to the same standard, not past it.
- **The header is out of scope here.** A scattered write's object header is written on the
  coordinator ([#19](https://github.com/aws-samples/sample-pacer/issues/19)), not at the
  object key's home, and that is tracked separately.
