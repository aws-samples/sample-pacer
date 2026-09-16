//! The cached read path: GET → policy decision → header + covering chunks.
//!
//! Read path (ADR-0002/ADR-0015, cache policy): GET → policy decision → the
//! object is addressed as a cached [`ObjectHeader`] plus N fixed-size chunks
//! ([`pacer_cache::chunk::ChunkConfig`]). A read resolves the requested byte
//! range against the
//! header's object length, computes the covering chunk set, and serves those
//! chunks in order — each either a local cache hit, a peer fetch (cluster
//! mode), or a backend ranged GET that fills the chunk. A ranged read fills
//! exactly its covering chunks, never the whole object (this supersedes
//! ADR-0011's no-fill-on-range restriction while preserving its cost
//! invariant: only covering chunks are ever retrieved).
//!
//! The covering chunks are resolved through a bounded look-ahead pipeline
//! (`stream::buffered`, `fill_parallelism` in flight) that emits them to the
//! client IN ORDER while fetching misses concurrently, so a multi-chunk read
//! never materializes the whole object in RAM (memory bound =
//! `fill_parallelism × chunk_size`).
//!
//! The range and covering arithmetic itself is NOT here: it belongs to
//! [`pacer_cache::resolve_range`] and `ChunkConfig::covering`, and this module
//! only adapts an HTTP `Range` to them and an [`ObjectHeader`] to a
//! `GetObjectOutput`. Resolving one chunk is `super::fill`'s job; delivering the
//! bytes into memory a client named instead of into a body is `super::deliver`'s.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use bytes::Bytes;
use futures::StreamExt;
use pacer_cache::chunk::{CachedChunk, ObjectHeader};
use pacer_cache::tier::ChunkTier;
use pacer_cache::{
    object_key, read_decision, resolve_range, should_admit, CacheValue, ReadDecision,
};
use s3s::dto::{self, ETag, Range as HttpRange, StreamingBlob, Timestamp};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use super::cluster::is_home;
use super::fill::FillCtx;
use super::PacerProxy;

/// Resolve the object header (length + response metadata) needed to compute
/// the covering chunk set. A cache hit returns the stored header; a miss
/// issues a backend `HeadObject` (a metadata op — no per-GB retrieval
/// charge, ADR-0015). Returns `(header, was_cached)` so the caller inserts
/// a freshly-discovered header once admission is decided.
///
/// A free function for the same reason [`super::cluster::chunk_sources`] is: the pre-flight
/// query needs an
/// object's length to compute the same covering chunk set the read will, and a second HEAD
/// path could disagree about which lengths are cached. Its cache read also *warms* the header
/// the GET that follows will want, so the pre-flight is not purely a cost.
///
/// # Errors
///
/// Maps a backend `NoSuchKey` to the same S3 error the passthrough would
/// return; any other backend failure surfaces as `InternalError`.
pub(super) async fn header_for(
    tier: &ChunkTier,
    backend: &aws_sdk_s3::Client,
    object_key: &str,
    bucket: &str,
    key: &str,
) -> S3Result<(ObjectHeader, bool)> {
    match tier.cache().get(object_key).await {
        Ok(Some(entry)) => {
            if let Some(h) = entry.value().as_header() {
                return Ok((h.clone(), true));
            }
        }
        Ok(None) => {}
        Err(e) => warn!(key = %object_key, error = %e, "cache read failed; heading backend"),
    }
    let head = backend
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await
        .map_err(|e| {
            let svc = e.into_service_error();
            // A HEAD 404 surfaces either as the modeled NotFound variant or
            // just a NoSuchKey code (no response body to model) — treat both
            // as the object being absent (see head_list_delete_passthrough).
            if svc.is_not_found() || svc.meta().code() == Some("NoSuchKey") {
                s3_error!(NoSuchKey, "The specified key does not exist.")
            } else {
                warn!(key = %object_key, error = %svc, "backend HeadObject failed");
                s3_error!(InternalError, "backend HeadObject failed")
            }
        })?;
    let object_len = head
        .content_length()
        .and_then(|l| u64::try_from(l).ok())
        .ok_or_else(|| s3_error!(InternalError, "backend HeadObject without content length"))?;
    let header = ObjectHeader::new(
        object_len,
        head.e_tag().map(|e| e.trim_matches('"').to_owned()),
        head.content_type().map(str::to_owned),
        head.last_modified()
            .map(aws_sdk_s3::primitives::DateTime::secs),
    );
    Ok((header, false))
}

/// Whether a GET's `If-Match` permits serving from the cache, given the ETag this node
/// resolved for the object (ADR-0039).
///
/// # What this decides, and what it deliberately does not
///
/// `true` means "the client named the version we have, so our bytes are the bytes it asked
/// for". `false` means **pass through to the backend** — never answer `412`. That asymmetry
/// is the correctness argument and it is not a shortcut:
///
/// * On a match we serve version X for a request that asked for version X. Self-consistent,
///   and every range of a multi-range read comes from the same cached header, so a client
///   using `If-Match` to avoid *mixing* versions within one read still gets that.
/// * On a mismatch our cache is simply not the right source. Answering `412` would be
///   **wrong**, because S3 may well hold the ETag the client named while we hold an older
///   one — a cache would be turning a serviceable request into a hard failure. Passing
///   through is exactly the pre-ADR-0039 behaviour, so a mismatch can only ever cost the
///   optimisation, never correctness.
///
/// What is genuinely given up: a client cannot use `If-Match` **through PACER** to detect
/// that an object was replaced in place, because on a hit the ETag compared against is the
/// cached one rather than a fresh `HeadObject`. ADR-0015 requires a new name per version,
/// under which that condition can never legitimately fail; ADR-0039 records the trade and
/// `PACER_CONDITIONAL_GET_FROM_CACHE=false` restores the old bypass.
///
/// # The grammar is s3s's job, not ours (RFC 9110 § 13.1)
///
/// `dto::ETagCondition` is already the parsed header, so nothing here unquotes a string or
/// looks for a `W/` prefix by hand:
///
/// * `Any` is `*`, satisfied by a representation existing — and resolving a header is that.
/// * `ETag(_)` yields its value through `as_strong()`, which is `None` for a **weak**
///   validator. That is exactly the rule: `If-Match` requires the strong comparison
///   function, S3 emits no weak ETags, and a weak one here is a client this proxy should
///   not guess for. Letting s3s decide it means the one place quoting and weakness are
///   parsed is the one place upstream tests them.
///
/// Ours comes from [`header_for`], which stores it unquoted, so the comparison is a plain
/// string equality between two already-normalised strong values.
fn if_match_allows_cache(if_match: Option<&dto::ETagCondition>, header_etag: Option<&str>) -> bool {
    let Some(condition) = if_match else {
        return true; // no condition to satisfy
    };
    let requested = match condition {
        // `*` asks only that the representation exist, which a resolved header settles —
        // decided before our own ETag is consulted, since it does not depend on the value.
        dto::ETagCondition::Any => return true,
        // A weak validator gives `None` here and therefore falls through to the bypass.
        dto::ETagCondition::ETag(tag) => match tag.as_strong() {
            Some(value) => value,
            None => return false,
        },
    };
    // No ETag of our own is not a mismatch, it is an inability to decide — and an
    // undecidable condition must pass through rather than be assumed either way.
    header_etag.is_some_and(|ours| ours.trim_matches('"') == requested)
}

impl PacerProxy {
    /// Whether this node is one of `cache_key`'s R co-homes — single-node (no
    /// cluster), or the ring ranks this node in the top-R for the key (ADR-0016
    /// layer 2). A home fills the key and holds it in its directory shard, so a
    /// write's invalidation must reach every home (fan-out, ADR-0007).
    fn owns_key(&self, cache_key: &str) -> bool {
        match &self.cluster {
            None => true,
            Some(cluster) => is_home(cluster, cache_key),
        }
    }

    /// A GET is only served from / admitted to the cache when it carries no
    /// semantics that whole-object slicing can't reproduce. `checksum_mode`
    /// deliberately does NOT bypass: modern SDKs send it by default, and a
    /// cached response simply omits checksum headers (clients treat that as
    /// "no checksum to validate").
    ///
    /// **`if_match` is deliberately NOT here** (ADR-0039). It cannot be decided from the
    /// request alone — it depends on the ETag this node resolves — so it is checked after
    /// the header, by [`if_match_allows_cache`]. Every other conditional stays an
    /// unconditional bypass: `if_none_match` would have to answer 304, and the two
    /// date-based ones would have to compare a `Last-Modified` this cache does not treat as
    /// authoritative. None of the three is on the path that motivated ADR-0039.
    fn cacheable_shape(input: &dto::GetObjectInput) -> bool {
        input.if_none_match.is_none()
            && input.if_modified_since.is_none()
            && input.if_unmodified_since.is_none()
            && input.version_id.is_none()
            && input.sse_customer_algorithm.is_none()
    }

    /// Resolve the object header (length + response metadata) needed to compute
    /// the covering chunk set — see [`header_for`], which this delegates to so the
    /// pre-flight query resolves an object's length exactly as the read does.
    ///
    /// # Errors
    ///
    /// See [`header_for`].
    async fn header_for(
        &self,
        object_key: &str,
        bucket: &str,
        key: &str,
    ) -> S3Result<(ObjectHeader, bool)> {
        header_for(&self.tier, &self.backend, object_key, bucket, key).await
    }

    /// Record what the header resolution found: cache a freshly-discovered header
    /// when this node may hold it, and count the miss that discovered it.
    ///
    /// The insert is gated on three things, and dropping any one of them is a
    /// correctness bug rather than a policy change. `admit` — a `no-store` read must
    /// populate nothing (ADR-0012). `!header_cached` — re-inserting what is already
    /// there costs a clone of the header for nothing. And [`Self::owns_key`] — a
    /// header obeys the same two-holder invariant a chunk does (ADR-0012), so
    /// caching it on a non-owner would leave a copy that a write's owner-targeted
    /// invalidation cannot reach; a non-owner discovers the length per request (via
    /// HEAD) instead.
    fn remember_header(
        &self,
        object_key: &str,
        header: &ObjectHeader,
        header_cached: bool,
        admit: bool,
    ) {
        if admit && !header_cached && self.owns_key(object_key) {
            self.tier
                .cache()
                .insert(object_key.to_owned(), CacheValue::Header(header.clone()));
        }
        if !header_cached {
            self.metrics.cache_misses.inc();
        }
    }

    /// Build the ordered fill pipeline for covering chunks `[first, last)` and
    /// wrap it as the response body.
    ///
    /// The pipeline is `stream::iter(first..last).map(resolve_chunk).buffered(N)`
    /// with `N = fill_parallelism`: `buffered` yields results IN ASCENDING
    /// INPUT ORDER while driving up to `N` chunk resolutions concurrently, so
    /// the client sees chunks in order and at most `N` chunks are resolved
    /// ahead of the cursor — memory is bounded to `N × chunk_size`, independent
    /// of object size. Each resolved chunk is trimmed via
    /// [`CachedChunk::slice_for`] to the requested `[start, end)` (the first and
    /// last covering chunks are partial; middle chunks pass whole) and relayed
    /// to the client through a bounded channel (a spawned task drives the
    /// pipeline — [`StreamingBlob::wrap`] requires `Sync`, which the buffered
    /// backend/peer futures are not).
    fn chunked_body(&self, ctx: Arc<FillCtx>, start: u64, end: u64) -> StreamingBlob {
        let covering = ctx.chunk.covering(start, end);
        let parallelism = self.fill_parallelism.max(1);
        let (tx, rx) = mpsc::channel::<Result<Bytes, std::io::Error>>(parallelism);
        tokio::spawn(async move {
            let mut pipeline = futures::stream::iter(covering.clone())
                .map(|idx| {
                    let ctx = Arc::clone(&ctx);
                    async move { ctx.resolve_chunk(idx).await }
                })
                .buffered(parallelism);
            let mut idx = covering.start;
            while let Some(result) = pipeline.next().await {
                let msg = match result {
                    Ok(bytes) => {
                        // covering() guarantees idx is in-bounds.
                        let chunk_start = ctx
                            .chunk
                            .chunk_bounds(idx, ctx.object_len)
                            .map_or(start, |b| b.start);
                        Ok(CachedChunk::new(bytes).slice_for(chunk_start, start, end))
                    }
                    Err(e) => Err(std::io::Error::other(e.to_string())),
                };
                let failed = msg.is_err();
                if tx.send(msg).await.is_err() || failed {
                    return; // client went away, or a fetch failed mid-stream
                }
                idx += 1;
            }
        });
        StreamingBlob::wrap(ReceiverStream::new(rx))
    }

    /// Assemble the `GetObjectOutput` for a served byte range `[start, end)` of
    /// an object described by `header`, with `body` the ordered chunk stream.
    /// `ranged` sets `Content-Range` (a whole-object read omits it).
    fn get_output(
        header: &ObjectHeader,
        start: u64,
        end: u64,
        ranged: bool,
        body: StreamingBlob,
    ) -> S3Response<dto::GetObjectOutput> {
        let content_length = end - start;
        let content_range =
            ranged.then(|| format!("bytes {}-{}/{}", start, end - 1, header.object_len));
        let last_modified = header.last_modified_epoch_secs.map(|s| {
            Timestamp::from(SystemTime::UNIX_EPOCH + Duration::from_secs(s.max(0) as u64))
        });
        let output = dto::GetObjectOutput {
            body: Some(body),
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(content_length as i64),
            content_range,
            content_type: header.content_type.clone(),
            e_tag: header.e_tag.clone().map(ETag::Strong),
            last_modified,
            ..Default::default()
        };
        S3Response::new(output)
    }

    /// Serve one GET: the whole of the S3 trait's `get_object`, in five steps —
    /// header, admission, range, the covering-chunk pipeline, and (ADR-0026) the
    /// chance to deliver into memory the client named instead of into a body.
    ///
    /// Anything the cache cannot reproduce byte for byte is handed to `inner`
    /// untouched instead: a `Cache-Control` that bypasses, a conditional or
    /// versioned or SSE-C shape ([`PacerProxy::cacheable_shape`]), or an object
    /// outside the admitted size band (ADR-0002's small-object bypass).
    ///
    /// # Errors
    ///
    /// `InvalidRange` for a range the object cannot satisfy, and whatever the
    /// header resolution ([`header_for`]) or the first unresolvable chunk
    /// ([`FillCtx::resolve_chunk`]) fails with.
    pub(super) async fn serve_get(
        &self,
        mut req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.map_bucket(&mut req.input.bucket);
        // ADR-0030's pre-flight exchange, and **the first thing this function does** — before the
        // op counter, before the cache-control decision, before any cache read and any backend
        // request. That ordering is the requirement, not an optimisation: asking "who holds this?"
        // must never move a byte of the object, and the only way to be sure is to answer before
        // there is any code left that could. It is not counted as a `get_object` either, because
        // `pacer_delivery_requests_total ÷ ops_total{op="get_object"}` is the share of READS that
        // took the accelerated path, and a pre-flight is not a read.
        if let Some(raw) = self.requested_endpoints(&req.headers) {
            return self
                .answer_endpoints(&req.input.bucket, &req.input.key, &raw)
                .await;
        }
        self.count("get_object");
        let cache_control = req
            .headers
            .get(hyper::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok());
        let decision = read_decision(cache_control, req.input.part_number);
        if decision == ReadDecision::Bypass || !Self::cacheable_shape(&req.input) {
            self.metrics.cache_bypass.inc();
            return self.inner.get_object(req).await;
        }

        let (bucket, key) = (req.input.bucket.clone(), req.input.key.clone());
        let object_key = object_key(&bucket, &key);

        // 1. Header: cache hit, else a backend HeadObject (ADR-0015 — no per-GB
        //    charge). Learns object_len + response metadata to compute the set.
        let (header, header_cached) = self.header_for(&object_key, &bucket, &key).await?;

        // 1b. ADR-0039: an `If-Match` can only be judged once the ETag exists, so it is
        //     decided here rather than in `cacheable_shape`. A mismatch (or the knob being
        //     off) passes through, which is what EVERY conditional GET did before — so this
        //     branch can lose the optimisation and cannot lose correctness. Counted as a
        //     bypass because that is what it is.
        if !self.conditional_get_from_cache && req.input.if_match.is_some() {
            self.metrics.cache_bypass.inc();
            return self.inner.get_object(req).await;
        }
        if !if_match_allows_cache(req.input.if_match.as_ref(), header.e_tag.as_deref()) {
            self.metrics.cache_bypass.inc();
            return self.inner.get_object(req).await;
        }
        if req.input.if_match.is_some() {
            // The number that proves the ADR-0039 composition actually engaged. Without it a
            // client whose every GET is conditional looks identical whether it is being
            // served or bypassed — which is precisely how the Mountpoint arm's 0 % hit rate
            // went unexplained until someone read the request headers.
            self.metrics.conditional_get_served.inc();
        }

        // 2. Admission gates on the object length, decided once at the header.
        //    Below-min / above-max objects are served but never cached (the
        //    small-object bypass, ADR-0002); no-store serves without filling.
        let size_admitted = should_admit(
            Some(header.object_len),
            self.min_object_size,
            self.max_object_size,
        );
        // Below-min / above-max: bypass entirely (proxy through, cache nothing)
        // — exactly the small-object behavior of ADR-0002. The header was not
        // cached (see below), so no chunked footprint is left behind.
        if !size_admitted {
            self.metrics.cache_bypass.inc();
            return self.inner.get_object(req).await;
        }
        let admit = decision == ReadDecision::CacheAndFill;
        self.remember_header(&object_key, &header, header_cached, admit);

        // 3. Resolve the requested byte range against object_len; whole-object
        //    reads span [0, object_len). 416 if unsatisfiable.
        let ranged = req.input.range.is_some();
        let (start, end) = match resolve_input_range(req.input.range.as_ref(), header.object_len) {
            Some(r) => (r.start, r.end),
            None => {
                return Err(s3_error!(
                    InvalidRange,
                    "The requested range is not satisfiable"
                ))
            }
        };

        // 4. Serve the covering chunks in order through the bounded look-ahead
        //    pipeline, filling misses per chunk.
        let ctx = self.fill_ctx(object_key, bucket, key, header.object_len, decision);

        // 5. ADR-0026: a client that named memory it owns gets the bytes
        //    delivered into it and a header-only 200. Everything below this
        //    branch is what every other client still gets, byte for byte — the
        //    gate is the whole compatibility story, and `stock_get_is_untouched`
        //    is the test that keeps it honest.
        if let Some(raw) = self.requested_target(&req.headers) {
            if let Some(delivered) = self
                .deliver(&ctx, &raw, &header, start..end, ranged)
                .await?
            {
                return Ok(delivered);
            }
        }
        let body = self.chunked_body(ctx, start, end);
        Ok(Self::get_output(&header, start, end, ranged, body))
    }
}

/// Resolve a request's optional HTTP range into a byte range `[start, end)`
/// against `object_len`. `None` (no `Range` header) is the whole object
/// `[0, object_len)`; a present-but-unsatisfiable range yields `None` (416).
fn resolve_input_range(range: Option<&HttpRange>, object_len: u64) -> Option<std::ops::Range<u64>> {
    match range {
        None => Some(0..object_len),
        Some(r) => {
            let (first, last, suffix) = match *r {
                HttpRange::Int { first, last } => (Some(first), last, None),
                HttpRange::Suffix { length } => (None, None, Some(length)),
            };
            resolve_range(first, last, suffix, object_len)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::if_match_allows_cache;
    use s3s::dto::{ETag, ETagCondition};

    /// Our own ETag, as [`super::header_for`] stores it: unquoted.
    const OURS: &str = "d41d8cd98f00b204e9800998ecf8427e";

    /// The condition as s3s would have parsed it off the wire, so these cases exercise the
    /// real parser rather than a string this test invented.
    fn condition(header_value: &str) -> ETagCondition {
        ETagCondition::parse_http_header(header_value.as_bytes())
            .expect("the fixture must be a header s3s accepts")
    }

    #[test]
    fn no_condition_is_always_cacheable() {
        assert!(if_match_allows_cache(None, Some(OURS)));
        assert!(if_match_allows_cache(None, None));
    }

    #[test]
    fn the_etag_the_client_named_matches_ours() {
        // Sent quoted, as every S3 client sends it and RFC 9110 requires; s3s unquotes,
        // and `header_for` stored ours unquoted, so the two meet already normalised.
        assert!(if_match_allows_cache(
            Some(&condition(&format!("\"{OURS}\""))),
            Some(OURS)
        ));
    }

    #[test]
    fn a_different_etag_falls_through_rather_than_matching() {
        // Not a 412: the caller passes through, because S3 may hold the version the client
        // named while this node holds an older one, and a cache must not turn a serviceable
        // request into a hard failure.
        assert!(!if_match_allows_cache(
            Some(&condition("\"something-else\"")),
            Some(OURS)
        ));
    }

    #[test]
    fn star_is_satisfied_by_having_resolved_a_header_at_all() {
        // `*` asks only that a representation exist, and resolving a header is that — so it
        // does not consult our ETag, which the second case pins.
        assert!(if_match_allows_cache(Some(&ETagCondition::Any), Some(OURS)));
        assert!(if_match_allows_cache(Some(&ETagCondition::Any), None));
    }

    #[test]
    fn a_weak_validator_never_matches() {
        // `If-Match` requires the STRONG comparison function and S3 emits no weak ETags, so
        // `W/` here is a client this proxy should not guess for. The digest inside is
        // deliberately OURS: that is the case a naive unquote would have let through.
        assert!(!if_match_allows_cache(
            Some(&condition(&format!("W/\"{OURS}\""))),
            Some(OURS)
        ));
        // And the variant directly, so the case survives a change in what s3s parses.
        assert!(!if_match_allows_cache(
            Some(&ETagCondition::ETag(ETag::Weak(OURS.to_owned()))),
            Some(OURS)
        ));
    }

    #[test]
    fn no_etag_of_our_own_is_undecidable_and_passes_through() {
        // A header with no ETag cannot settle the condition either way, and guessing is the
        // one thing this function must not do.
        assert!(!if_match_allows_cache(
            Some(&condition(&format!("\"{OURS}\""))),
            None
        ));
    }

    #[test]
    fn a_multipart_composite_etag_matches_like_any_other() {
        // ADR-0032's converted PUTs produce `<digest>-<parts>`, and a scattered write is
        // exactly the object a Mountpoint client would then read back.
        let composite = "4fcec74691ff529f6d016ec3629ff11b-5";
        assert!(if_match_allows_cache(
            Some(&condition(&format!("\"{composite}\""))),
            Some(composite)
        ));
    }
}
