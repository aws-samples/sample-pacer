# ADR-0045: A cached header carries user metadata and the representation headers, and its on-disk format versions by appending

Date: 2026-09-28 · Status: **Accepted.** Fixes
[#25](https://github.com/aws-samples/sample-pacer/issues/25). No measurement: the defect is
established from the code, and the fix is pinned by tests, not a benchmark.

## Context

A GET on the cached path rebuilds its response from `ObjectHeader`
([0015](0015-chunk-granular-caching.md)): object length, ETag, Content-Type and
Last-Modified. Everything else S3 would return — `x-amz-meta-*`, Content-Encoding,
Content-Disposition, Content-Language, Cache-Control, Expires, and (unmodelled here, see
Consequences) storage class, version id, tagging count and the SSE headers — was silently
dropped on a hit. `head_object` always passes through, so a HEAD of the same object returned
every header a GET on the cached path did not.

The gap was worse than missing metadata: the cached/passthrough split is decided by object
size ([0015](0015-chunk-granular-caching.md)'s admission band), not by anything about the header, so a
`Content-Encoding: gzip` object just above `minObjectSize` served a client compressed bytes
with no header saying so — a decoding bug the client's own size, not the cache, decided
whether to inflict. `Content-Disposition` changes client behaviour the same way. A scattered
write ([0032](0032-write-scatter-populates-the-cache.md)) built its header with
`last_modified: None`, because `CompleteMultipartUpload` does not return it, so a GET of a
scattered object dropped even that field, though S3 and PACER's own HEAD both had it.

## Decision

1. **`ObjectHeader` gains a `RepresentationHeaders` sub-struct**: `metadata`
   (`x-amz-meta-*`, by suffix, in a `BTreeMap` for deterministic iteration — nothing depends
   on the order, a test's byte comparison just stays reproducible), `content_encoding`,
   `content_disposition`, `content_language`, `cache_control` and `expires_epoch_secs`
   (mirroring how `last_modified_epoch_secs` already stores Last-Modified). Grouped apart
   from `ObjectHeader`'s own fields purely to keep `ObjectHeader::new`'s argument count under
   the workspace's `too_many_arguments` limit.

   **Deliberately not modelled**: storage class, the response's version id, tagging count and
   the SSE headers. A version id has no cheap staleness check the way an ETag does, and this
   cache is not version-aware (`cacheable_shape` already bypasses any GET that names one) — a
   cached version id could describe a version a later overwrite of the same key already
   superseded. Storage class and tagging count can change with no write through this proxy at
   all (a lifecycle transition, `PutObjectTagging`), so a cached value has no invalidation
   path and could go stale indefinitely — unlike every field above, which only changes by a
   write this cache already observes. SSE-C already forces passthrough
   (`sse_customer_algorithm`); bucket-default SSE metadata is static per object and rarely
   consulted, so it was not worth the added surface.

2. **Every constructor is updated, not just the common one.** The read path's cache-miss
   `HeadObject` (`header_for`) and the write scatter's post-Complete `HeadObject` (below) now
   share one mapping, `object_header_from_head`, so the two paths cannot independently drift
   on which field means what — which is exactly how issue #25's Last-Modified gap happened in
   the first place. The requester-mode authorization probe (ADR-0041), which derives a header
   from a real HTTP response's raw headers rather than a typed SDK output, gained the
   equivalent `representation_from_headers`. Both response builders — `read::get_output` and
   `deliver::delivered_output` — now spread a shared `representation_output_fields(&header.representation)`
   into the `GetObjectOutput` they build, so the two cannot independently drop a field either.

3. **The scatter path's Last-Modified and representation headers come from a `HeadObject`
   issued INLINE, in `write_header`, before anything is cached — never from the write itself,
   and never from a background task.** `CompleteMultipartUpload` returns neither field, and a
   scattered PUT does not forward representation headers to `CreateMultipartUpload` today (a
   separate, pre-existing gap this fix does not extend to closing). One `HeadObject` per
   scattered PUT is accepted here: it sits next to a write that already opened a multipart
   upload and uploaded every window over the peer plane, so it is not the dominant cost, and
   `write_header` — like every other step `publish` takes after `Complete` — cannot fail the
   client's PUT; a `HeadObject` failure only means the header is not cached this time.

   **What is cached is gated on [`header_if_etag_agrees`]: the HEAD's own ETag must equal the
   ETag `Complete` just minted (compared unquoted — a raw S3 ETag is quoted and nothing
   guarantees the two spellings agree), or nothing is cached at all.** A first version of this
   fix cached a partial header immediately and backfilled the rest from a **detached**
   background `HeadObject`, and review caught two races in it before it shipped:

   - **Not atomic.** Its read-check-insert on the cache was three separate steps; a DELETE's or
     an overwrite's invalidation landing between the check and the insert could resurrect a
     header the write path had just dropped, serving a GET of a deleted object from cache.
   - **Guarded the wrong ETag.** It compared the *cached* header's ETag against this write's,
     never the HEAD's *own* answer against it. A newer write B completing on the same key
     before A's HEAD returned could have B's current data cached while the guard still passed
     on A's stale comparison.

   Doing the HEAD inline, before the one insert `write_header` makes, removes both races at
   once: there is no separate check-then-insert for an invalidation to land inside, and the
   guard is on the HEAD's own answer rather than on a second, independently-racing read of the
   cache. It also keeps this at one call site — the same one PR #32 (gh19) moves to an RPC to
   `home(object_key)` — so that rebase only has to change what this function *sends*, not
   discover a second, now-stale insert somewhere else.

4. **The on-disk format versions by appending, and an old entry decodes as a miss.**
   `pacer_cache::codec` bincode-encodes a `CacheValue::Header` by delegating straight to
   `ObjectHeader`'s derived `Serialize`/`Deserialize` — unlike a chunk's hand-rolled tag
   ([0032](0032-write-scatter-populates-the-cache.md) § 7), there is no separate framing to
   version. `RepresentationHeaders` was added as a new field at the end of `ObjectHeader`,
   after `last_modified_epoch_secs`, rather than reordering anything a previous build wrote.
   Reading an old, shorter header entry with the new field runs bincode's reader out of bytes
   partway through it — `foyer`'s disk tier decodes a value from a slice bounded to exactly
   that entry's recorded length (`EntryDeserializer::deserialize`), so there are no
   neighbouring entries' bytes to spill into — and `CacheValue::decode` returns `Err`. Every
   call site already treats a decode failure as a cache miss (`if let Ok(Some(entry)) =
   self.tier.cache().get(...)`, the same pattern the pre-existing chunk-witness rollback path
   relies on), so an old header costs one re-fetch, never a panic and never the old,
   incomplete answer served as if it were current.
   `codec::tests::a_pre_representation_headers_entry_is_a_miss` pins this directly, encoding a
   `MirrorHeaderV1` (the exact pre-#25 four-field shape) and asserting the decode fails.

5. **Tests pin both the header contents and the ETag guard.**
   `correctness::cached_get_headers_match_head` PUTs one object carrying every field this ADR
   adds, HEADs it, then compares a cold GET (a cache miss) and a warm GET (a cache hit — the
   shape that was actually broken) against that HEAD, field by field. The one documented
   exception is checksums: `PacerProxy::cacheable_shape`'s own comment already names leaving
   them off a cached response as deliberate, so the test does not assert on them.
   `coordinate::tests::a_head_that_disagrees_with_complete_is_not_cached` pins
   `header_if_etag_agrees` directly: a `HeadObjectOutput` built with a different ETag than the
   one handed in yields `None`, so `write_header` never reaches its one `insert`.

## Consequences

- **A scattered PUT now pays one extra `HeadObject` before the client's response**, next to
  the multipart upload and every window's own upload it already paid for — small in comparison,
  but no longer zero, unlike the detached version this ADR replaced. A `HeadObject` failure, or
  a disagreeing ETag, costs only that header's warmth: `write_header` still cannot fail the
  client's PUT, exactly as every other step `publish` takes after `Complete` already could not.
- **A scattered PUT still does not forward representation headers to `CreateMultipartUpload`**,
  so until that separate gap closes, the post-Complete `HeadObject` this ADR adds will not
  find `Content-Encoding` et al. for a scattered object even once it resolves — there is
  nothing on the backend yet for it to read. It will find Last-Modified, which is what issue
  #25's checklist named as the minimum bar for the scatter path.
- **`RepresentationHeaders`'s exclusions are a standing decision, not a placeholder.** Storage
  class, version id, tagging count and SSE headers stay unmodelled until one of them gets a
  cheap staleness check (an ETag-like witness, or an invalidation path this cache does not
  currently have for a lifecycle-only or tagging-only mutation) — adding the field without
  the check would trade a missing header for a wrong one.
- **The header codec's compatibility rule going forward**: a header field is added at the end
  of `ObjectHeader` (or of `RepresentationHeaders`, itself always the last field), never
  inserted or reordered. That is what makes an old on-disk entry fail to decode rather than
  decode into the wrong field.
