# ADR-0039: An `If-Match` GET may be served from cache when the ETag agrees

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-09-15 · Status: **Accepted, on by default, one flag back.** No new measurement was
taken for it; the figure that motivates it is from an arm already on record.

A GET carrying `If-Match` bypassed the cache unconditionally, because
`PacerProxy::cacheable_shape` rejected every conditional shape. That rule is correct for the
other conditionals and wrong for this one, and the cost is not theoretical:
**Mountpoint-for-S3 puts `If-Match` on every GET it issues**, so measured against it PACER's
hit rate was exactly **0 %** and **100 %** of its reads bypassed
(`mountpoint-vs-pacer.md`). That write-up
called it "our gate, fixable". This is the fix.

## The decision

1. `If-Match` leaves `cacheable_shape` and is decided **after** the header resolves, by
   `if_match_allows_cache`, because it cannot be judged from the request alone.
2. **ETag agrees → serve from cache.** The client asked for version X; we have version X.
3. **ETag disagrees, or is weak, or we have no ETag → pass through to the backend.**
   Never `412`.
4. `config.conditionalGetFromCache` / `PACER_CONDITIONAL_GET_FROM_CACHE` restores the old
   bypass. Default `true`.

`if_none_match`, `if_modified_since`, `if_unmodified_since`, `version_id` and
`sse_customer_algorithm` still bypass unconditionally, unchanged. `if_none_match` would have
to answer `304`, the two date-based ones would have to treat a cached `Last-Modified` as
authoritative, and none of the three is on the path that motivated this ADR.

## Why a mismatch passes through instead of answering 412

This is the part that is easy to get wrong, and it is the difference between a cache and a
liability. **S3 may well hold the ETag the client named while this node holds an older one.**
A `412` would then turn a perfectly serviceable request into a hard failure that the client
cannot route around — a cache inventing an error about state it does not own. Passing through
is exactly the pre-ADR-0039 behaviour, so decision 3 can only ever cost the optimisation and
can never cost correctness.

## What is genuinely traded, stated plainly

**On a hit, the ETag compared against is the one in our cache, not a fresh `HeadObject`.** So
a client cannot use `If-Match` *through PACER* to detect that an object was replaced in place.
That is a real deviation from S3 and it is the reason this is an ADR and not a patch.

Three things bound it:

* **It is not a new staleness class.** A plain GET through PACER already returns cached bytes
  for an object S3 has since replaced. `If-Match` does not make that worse; it just stops
  being an instrument that would have noticed.
* **The invariant the header exists to protect is preserved.** A client using `If-Match` to
  avoid *mixing* versions across a multi-range read still gets that: every range resolves
  against the same cached header, so a read is served entirely from one version.
* **ADR-0015 requires a new name per version.** Under this cache's own usage contract an
  object is never replaced in place, so the condition can never legitimately fail. A
  deployment that violates that contract has a stale-read problem with or without this ADR.

An operator who needs the strict reading sets `conditionalGetFromCache: false` and gets the
old behaviour exactly.

## Why on by default

Off, this ADR does nothing for the client that motivated it — and "the cache is unreachable
for a whole class of client" is a worse default than "a conditional is answered against
cached state under a contract that forbids in-place replacement". ⚠ Noted deliberately
against [ADR-0038](0038-chunk-store-is-the-default-disk-tier.md), which was written the day
before about the danger of flipping a default: the difference is that ADR-0038's risk was a
**silent half-rate path**, while this one's is a semantic deviation that only manifests under
a usage pattern this repo's own contract rules out — and it is observable rather than silent
(see the counter below).

## The grammar is s3s's, not ours

`dto::ETagCondition` is the already-parsed header, so nothing in this change unquotes a string
or looks for `W/` by hand:

* `Any` (`*`) is satisfied by a representation existing, and resolving a header is that.
* `ETag(_)` yields its value through `as_strong()`, which is `None` for a **weak** validator —
  which is exactly the rule, since `If-Match` requires the strong comparison function and S3
  emits no weak ETags.

Letting s3s own it means quoting and weakness are parsed in the one place upstream tests them.
A `W/"<our digest>"` is asserted **not** to match, because that is the case a hand-rolled
unquote would have let through.

## How you can tell it engaged

`pacer_conditional_get_served_total`. It exists because a client whose *every* GET is
conditional looks identical in `cache_hits`/`cache_bypass` whether this ADR engaged or the
request was turned away — and that ambiguity is precisely why the Mountpoint arm's 0 % hit
rate went unexplained until someone read the request headers.

## Consequences

* **Mountpoint-for-S3 composes with PACER at all**, which it did not before. ⚠ This ADR does
  **not** claim a rate: the Mountpoint arm also found that client self-throttles to ~10 Gbps
  in a pod, and every PACER-vs-Mountpoint row in that write-up differs on the client too. What
  changes is that its reads can now be *cached*; what it is worth is unmeasured.
* **Any client that sends `If-Match` defensively now benefits**, not only Mountpoint.
* **The gate is no longer decidable from the request alone**, so `cacheable_shape` is no longer
  the whole story of what bypasses. Both halves name each other, because a reader who finds
  only the first would conclude conditionals still bypass.
