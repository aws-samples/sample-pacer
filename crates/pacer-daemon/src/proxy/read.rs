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
use pacer_cache::chunk::{CachedChunk, ObjectHeader, RepresentationHeaders};
use pacer_cache::tier::ChunkTier;
use pacer_cache::{
    object_key, read_decision, resolve_range, should_admit, CacheValue, ReadDecision,
};
use s3s::dto::{self, ETag, Range as HttpRange, StreamingBlob, Timestamp};
use s3s::{s3_error, S3Request, S3Response, S3Result};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tracing::warn;

use super::cluster::is_home;
use super::fill::{same_version, FillCtx, RequesterRead};
use super::PacerProxy;
use crate::warm::SkipReason;

/// `ops_total` label for a warm-only GET (ADR-0048), which is counted apart from
/// `get_object` because it is not a read.
const OP_WARM: &str = "warm_object";

/// `outcome` label values of `pacer_cache_revalidations_total` (ADR-0049). Consts for
/// the reason the fill path's labels are: a dashboard depends on the exact string.
///
/// The backend still has the version the cached header describes.
const REVALIDATION_CURRENT: &str = "current";
/// It does not: the header was replaced by the backend's current one.
const REVALIDATION_STALE: &str = "stale";

/// This node's cached header for `object_key`, if the foyer tier holds one.
///
/// A read failure is logged and treated as absent, because the caller's fallback — a
/// `HeadObject` — is what a miss costs anyway.
async fn cached_header(tier: &ChunkTier, object_key: &str) -> Option<ObjectHeader> {
    match tier.cache().get(object_key).await {
        Ok(Some(entry)) => entry.value().as_header().cloned(),
        Ok(None) => None,
        Err(e) => {
            warn!(key = %object_key, error = %e, "cache read failed; heading backend");
            None
        }
    }
}

/// The backend's current header for `bucket`/`key`, by `HeadObject` (a metadata op — no
/// per-GB retrieval charge, ADR-0015).
///
/// # Errors
///
/// Maps a backend `NoSuchKey` to the same S3 error the passthrough would
/// return; any other backend failure surfaces as `InternalError`.
async fn head_backend(
    backend: &aws_sdk_s3::Client,
    object_key: &str,
    bucket: &str,
    key: &str,
) -> S3Result<ObjectHeader> {
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
    Ok(object_header_from_head(&head, object_len))
}

/// Map a backend `HeadObjectOutput` onto an [`ObjectHeader`], `object_len`
/// taken separately because a scatter's inline post-Complete `HeadObject`
/// ([`crate::coordinate`]) already knows it from the multipart upload and a
/// `HeadObject` run concurrently with a client's own overwrite could
/// otherwise report a length for a *different* version than the ETag this
/// header is about to carry.
///
/// The one mapping every caller shares: [`head_backend`]'s HEAD and
/// the scatter path's inline post-Complete `HeadObject` both need it, and issue #25 is
/// exactly the bug that opened up when a second call site built an
/// `ObjectHeader` by hand instead of reusing this one.
pub(crate) fn object_header_from_head(
    head: &aws_sdk_s3::operation::head_object::HeadObjectOutput,
    object_len: u64,
) -> ObjectHeader {
    let representation = RepresentationHeaders {
        metadata: head
            .metadata()
            .map(|m| m.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default(),
        content_encoding: head.content_encoding().map(str::to_owned),
        content_disposition: head.content_disposition().map(str::to_owned),
        content_language: head.content_language().map(str::to_owned),
        cache_control: head.cache_control().map(str::to_owned),
        // `expires_string`, not the deprecated `expires`: the raw value, reparsed
        // through the same HTTP-date grammar `header_from_probe` uses, so a
        // header built from a `HeadObjectOutput` and one built from raw HTTP
        // headers agree on what an unparseable Expires means (`None`, not a
        // deprecation warning promoted to an error under `-D warnings`).
        expires_epoch_secs: head.expires_string().and_then(parse_http_date),
    };
    ObjectHeader::new(
        object_len,
        head.e_tag().map(|e| e.trim_matches('"').to_owned()),
        head.content_type().map(str::to_owned),
        head.last_modified()
            .map(aws_sdk_s3::primitives::DateTime::secs),
        representation,
    )
}

/// Convert epoch seconds — how [`ObjectHeader`] stores both Last-Modified and
/// Expires — into an s3s [`Timestamp`]. Shared by the two fields precisely
/// because they use the same on-disk encoding.
pub(crate) fn timestamp_from_epoch_secs(secs: i64) -> Timestamp {
    Timestamp::from(SystemTime::UNIX_EPOCH + Duration::from_secs(secs.max(0) as u64))
}

/// The issue #25 fields of a `GetObjectOutput`, built from `header`'s
/// representation headers. Meant as a `..` base for a caller that also sets
/// `body`/`content_range`/`content_type`/`e_tag`/`last_modified` — [`get_output`]
/// and `deliver::delivered_output` both do — so this mapping is written once.
///
/// An empty metadata map becomes `None`, matching a real backend, which sends
/// no `x-amz-meta-*` header at all when an object carries none.
pub(crate) fn representation_output_fields(
    representation: &RepresentationHeaders,
) -> dto::GetObjectOutput {
    let metadata = (!representation.metadata.is_empty()).then(|| {
        representation
            .metadata
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    });
    dto::GetObjectOutput {
        metadata,
        content_encoding: representation.content_encoding.clone(),
        content_disposition: representation.content_disposition.clone(),
        content_language: representation.content_language.clone(),
        cache_control: representation.cache_control.clone(),
        expires: representation
            .expires_epoch_secs
            .map(timestamp_from_epoch_secs),
        ..Default::default()
    }
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
/// Ours comes from [`head_backend`], which stores it unquoted, so the comparison is a plain
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

/// Whether a GET asks S3 to override any response header (`response-content-type` and
/// its five siblings). S3 honours these on the object it returns; the cached path builds
/// its response from the stored header alone, so it would ignore them without an error.
fn overrides_response_headers(input: &dto::GetObjectInput) -> bool {
    input.response_cache_control.is_some()
        || input.response_content_disposition.is_some()
        || input.response_content_encoding.is_some()
        || input.response_content_language.is_some()
        || input.response_content_type.is_some()
        || input.response_expires.is_some()
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
    ///
    /// Three more shapes bypass because a cached answer would silently drop what they ask
    /// S3 to do. `expected_bucket_owner` is a check only S3 can make — the caller's guard
    /// against a bucket name now owned by another account — and a hit would answer 200
    /// without it. `request_payer` acknowledges charges S3 has to see. And the
    /// `response-*` overrides ([`overrides_response_headers`]) rewrite headers a cached
    /// response is rebuilt without.
    fn cacheable_shape(input: &dto::GetObjectInput) -> bool {
        input.if_none_match.is_none()
            && input.if_modified_since.is_none()
            && input.if_unmodified_since.is_none()
            && input.version_id.is_none()
            && input.sse_customer_algorithm.is_none()
            && input.expected_bucket_owner.is_none()
            && input.request_payer.is_none()
            && !overrides_response_headers(input)
    }

    /// Resolve the object header (length + response metadata) needed to compute
    /// the covering chunk set. Returns `(header, was_cached)` so the caller inserts
    /// a freshly-discovered header once admission is decided.
    ///
    /// A cached header is used as-is only once this process has confirmed its object
    /// against the backend (ADR-0049). Until then it may be one the disk tier recovered
    /// from a previous process, describing a version that has since been overwritten, so
    /// the first read of each object costs one `HeadObject`: a matching ETag confirms the
    /// cached header, anything else replaces it with the backend's. A miss costs the same
    /// `HeadObject` it always did, and confirms the object too.
    ///
    /// Two ETags are only "the same version" when both exist. A backend that reports none
    /// gives nothing to compare, so its objects are re-headed rather than trusted — and
    /// their chunks, whose witness is that same absent ETag, are never served from cache
    /// ([`FillCtx::witness_matches`]).
    ///
    /// # Errors
    ///
    /// See [`head_backend`]. A confirmation that cannot reach the backend fails the GET
    /// exactly as a miss would: a recovered header is never served unconfirmed.
    async fn header_for(
        &self,
        object_key: &str,
        bucket: &str,
        key: &str,
    ) -> S3Result<(ObjectHeader, bool)> {
        let cached = cached_header(&self.tier, object_key).await;
        if let Some(header) = &cached {
            if self.revalidated.is_confirmed(object_key) {
                return Ok((header.clone(), true));
            }
        }
        let current = head_backend(&self.backend, object_key, bucket, key).await?;
        self.revalidated.confirm(object_key);
        let Some(header) = cached else {
            return Ok((current, false));
        };
        if same_version(header.e_tag.as_deref(), current.e_tag.as_deref()) {
            self.count_revalidation(REVALIDATION_CURRENT);
            return Ok((header, true));
        }
        // Removed rather than left for `remember_header` to overwrite: a `no-store` read
        // inserts nothing, and the stale header would then pass as confirmed.
        self.tier.cache().remove(object_key);
        self.count_revalidation(REVALIDATION_STALE);
        Ok((current, false))
    }

    /// Count one cached header checked against the backend (ADR-0049).
    fn count_revalidation(&self, outcome: &str) {
        self.metrics
            .revalidations
            .with_label_values(&[outcome])
            .inc();
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
        let output = dto::GetObjectOutput {
            body: Some(body),
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(content_length as i64),
            content_range,
            content_type: header.content_type.clone(),
            e_tag: header.e_tag.clone().map(ETag::Strong),
            last_modified: header
                .last_modified_epoch_secs
                .map(timestamp_from_epoch_secs),
            ..representation_output_fields(&header.representation)
        };
        S3Response::new(output)
    }

    /// Serve one GET: the whole of the S3 trait's `get_object`, in five steps —
    /// header, admission, range, the covering-chunk pipeline, and (ADR-0026) the
    /// chance to deliver into memory the client named instead of into a body.
    ///
    /// Anything the cache cannot reproduce byte for byte is handed to `inner`
    /// untouched instead: a `Cache-Control` that bypasses, a conditional,
    /// versioned, SSE-C, bucket-owner, requester-pays or response-override shape
    /// ([`PacerProxy::cacheable_shape`]), or an object
    /// outside the admitted size band (ADR-0002's small-object bypass).
    ///
    /// # Errors
    ///
    /// `InvalidRange` for a range the object cannot satisfy, and whatever the
    /// header resolution ([`Self::header_for`]) or the first unresolvable chunk
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
        // ADR-0048: a warm is the read path to its last step, then no body. Counted apart
        // from `get_object` for the pre-flight's reason — a warm is not a read.
        let warm = Self::requested_warm(&req.headers)?;
        self.count(if warm { OP_WARM } else { "get_object" });
        let cache_control = req
            .headers
            .get(hyper::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok());
        let decision = read_decision(cache_control, req.input.part_number);
        if decision == ReadDecision::Bypass || !Self::cacheable_shape(&req.input) {
            self.metrics.cache_bypass.inc();
            return self.bypass_get(req, warm, SkipReason::Uncacheable).await;
        }
        if warm && decision == ReadDecision::CacheNoFill {
            return Ok(self.warm_skipped(SkipReason::NoStore));
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
            return self.bypass_get(req, warm, SkipReason::Uncacheable).await;
        }
        if !if_match_allows_cache(req.input.if_match.as_ref(), header.e_tag.as_deref()) {
            self.metrics.cache_bypass.inc();
            return self.bypass_get(req, warm, SkipReason::Uncacheable).await;
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
            return self.bypass_get(req, warm, SkipReason::ObjectSize).await;
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
        let ctx = self.fill_ctx(object_key, bucket, key, &header, decision, None);
        if warm {
            return self.warm_range(ctx, &header, start, end).await;
        }
        let ctx = Arc::new(ctx);

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

    /// `auth.mode: requester`'s `get_object` (ADR-0041 § 2.4): the caller's
    /// own signature authorizes every request instead of this node's
    /// identity — the probe answers "is this caller allowed", never
    /// `self.backend`.
    ///
    /// Every shape node mode hands to `inner` — a `Cache-Control` bypass, an `If-Match`
    /// the cache cannot honour, an object outside the admitted size band — is a
    /// [`Self::pass_through`] of the caller's own request instead. One simplification
    /// against the full design, stated rather than hidden: the probe runs before the
    /// cache is touched, never overlapped with it, so a hit costs one S3 round trip.
    ///
    /// # Errors
    ///
    /// `InternalError` if the front door did not hold the request (a classification
    /// bug — see [`crate::authz::RequesterFront`]), and whatever the probe transport or
    /// the first unresolvable chunk fails with. The probe's own denial (403/404/…) is
    /// mapped to the matching S3 error; a pass-through returns S3's.
    pub(super) async fn serve_get_requester(
        &self,
        mut req: S3Request<dto::GetObjectInput>,
        forwarder: &Arc<crate::authz::Forwarder>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.map_bucket(&mut req.input.bucket);
        // ADR-0030's pre-flight is refused here, not answered: node mode answers it
        // before any authorization and learns the object's length with this node's
        // own `HeadObject`, so in this mode it would tell an unauthorized caller
        // whether an object exists, how long it is and who holds it — under the
        // node's identity. It only serves client-memory delivery, which this mode
        // does not offer yet (see below).
        if self.requested_endpoints(&req.headers).is_some() {
            return Err(s3_error!(
                NotImplemented,
                "requester mode does not serve the delivery pre-flight"
            ));
        }
        // ADR-0048: allowed here, unlike the pre-flight, because a warm is authorized
        // exactly like the read it stands for — by the probe below, on the caller's own
        // signature — and it answers nothing a read would not.
        let warm = Self::requested_warm(&req.headers)?;
        self.count(if warm { OP_WARM } else { "get_object" });
        let held = req
            .extensions
            .get::<Arc<crate::authz::HeldRequest>>()
            .cloned()
            .ok_or_else(|| {
                s3_error!(
                    InternalError,
                    "requester mode: no held request — a front-door classification bug"
                )
            })?;
        let cache_control = req
            .headers
            .get(hyper::header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok());
        let decision = read_decision(cache_control, req.input.part_number);
        if decision == ReadDecision::Bypass || !Self::cacheable_shape(&req.input) {
            return self
                .pass_through_or_skip(&held, forwarder, warm, SkipReason::Uncacheable)
                .await;
        }
        if warm && decision == ReadDecision::CacheNoFill {
            return Ok(self.warm_skipped(SkipReason::NoStore));
        }

        let header = self.probe_header(&held, forwarder).await?;
        if !self.requester_may_cache(&req, &header) {
            let reason = self.requester_skip_reason(&header);
            return self
                .pass_through_or_skip(&held, forwarder, warm, reason)
                .await;
        }

        let (bucket, key) = (req.input.bucket.clone(), req.input.key.clone());
        let object_key = object_key(&bucket, &key);
        let admit = decision == ReadDecision::CacheAndFill;
        self.remember_header(&object_key, &header, false, admit);

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
        let ctx = self.fill_ctx(
            object_key,
            bucket,
            key,
            &header,
            decision,
            Some(RequesterRead {
                held,
                forwarder: Arc::clone(forwarder),
                e_tag: header.e_tag.clone(),
            }),
        );
        if warm {
            return self.warm_range(ctx, &header, start, end).await;
        }
        let ctx = Arc::new(ctx);
        // No ADR-0026 delivery into client memory in this mode, yet: its placement
        // path reads the tier directly and would bypass the version-witness check
        // `FillCtx::resolve_chunk` applies. A client that names a target simply gets
        // an ordinary body, which is the documented fallback for that header.
        let body = self.chunked_body(ctx, start, end);
        Ok(Self::get_output(&header, start, end, ranged, body))
    }

    /// Run the authorization probe and derive the object header from it
    /// (ADR-0041 § 2.4 points 3–4): `HeadObject` is never issued in this
    /// mode. Split out of [`Self::serve_get_requester`] to keep that
    /// function under the line budget, and because the three probe metrics
    /// belong with the request that produced them, not spread across two
    /// call sites.
    async fn probe_header(
        &self,
        held: &crate::authz::HeldRequest,
        forwarder: &crate::authz::Forwarder,
    ) -> S3Result<ObjectHeader> {
        let t0 = std::time::Instant::now();
        let probe = forwarder.send_range(held, "bytes=0-0").await;
        self.metrics
            .authz
            .probe_seconds
            .with_label_values::<&str>(&[])
            .observe(t0.elapsed().as_secs_f64());
        let probe = probe.map_err(|e| {
            self.metrics
                .authz
                .probe_total
                .with_label_values(&["error"])
                .inc();
            warn!(error = %e, "requester mode: authorization probe failed");
            s3_error!(InternalError, "authorization probe failed")
        })?;
        if !probe_allows(probe.status.as_u16()) {
            self.metrics
                .authz
                .probe_total
                .with_label_values(&["deny"])
                .inc();
            return Err(probe_denial(probe.status.as_u16()));
        }
        self.metrics
            .authz
            .probe_total
            .with_label_values(&["allow"])
            .inc();
        header_from_probe(&probe)
    }

    /// Whether a probed GET may be served through the cache: node mode's own post-header
    /// admission — an `If-Match` the cache can honour (ADR-0039), an object inside the
    /// admitted size band (ADR-0002). `false` means [`Self::pass_through`], exactly as
    /// node mode passes the same shapes through to `inner`.
    fn requester_may_cache(
        &self,
        req: &S3Request<dto::GetObjectInput>,
        header: &ObjectHeader,
    ) -> bool {
        let if_match = req.input.if_match.as_ref();
        if if_match.is_some() && !self.conditional_get_from_cache {
            return false;
        }
        if !if_match_allows_cache(if_match, header.e_tag.as_deref()) {
            return false;
        }
        if !should_admit(
            Some(header.object_len),
            self.min_object_size,
            self.max_object_size,
        ) {
            return false;
        }
        if if_match.is_some() {
            self.metrics.conditional_get_served.inc();
        }
        true
    }

    /// Serve a GET the cache does not serve by re-emitting the caller's request exactly
    /// as it arrived and streaming S3's answer back — status, headers and body — so the
    /// caller gets what S3 would have sent it, authorized by its own signature. The
    /// requester-mode counterpart of node mode's `inner` passthrough; nothing is cached.
    ///
    /// # Errors
    ///
    /// S3's own error for a non-2xx answer, with its status and code; `InternalError`
    /// when S3 could not be reached at all.
    pub(super) async fn pass_through(
        &self,
        held: &crate::authz::HeldRequest,
        forwarder: &crate::authz::Forwarder,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.metrics.cache_bypass.inc();
        let resp = forwarder.send_held(held).await.map_err(|e| {
            warn!(error = %e, "requester mode: pass-through failed");
            s3_error!(InternalError, "pass-through to S3 failed")
        })?;
        let (parts, body) = resp.into_parts();
        let mut headers = parts.headers;
        let hop_by_hop: Vec<_> = headers
            .keys()
            .filter(|n| crate::authz::is_hop_by_hop(n.as_str()))
            .cloned()
            .collect();
        for name in hop_by_hop {
            headers.remove(name);
        }
        if !parts.status.is_success() {
            return Err(s3_error_from(parts.status, body).await);
        }
        let output = dto::GetObjectOutput {
            body: Some(StreamingBlob::from(s3s::Body::http_body(body))),
            // Present exactly when S3 answered 206; it is what makes s3s answer 206 too.
            content_range: headers
                .get(hyper::header::CONTENT_RANGE)
                .and_then(|v| v.to_str().ok())
                .map(str::to_owned),
            ..Default::default()
        };
        // Replaces every header s3s would derive, so `x-amz-meta-*`, checksums and the
        // version id reach the caller exactly as S3 sent them.
        Ok(S3Response::with_headers(output, headers))
    }
}

/// Upper bound on an S3 error body a pass-through reads to learn the error code. S3's
/// error documents are a few hundred bytes of XML; anything larger is not one, and the
/// status alone is returned.
const MAX_ERROR_BODY: usize = 16 << 10;

/// S3's error answer to a pass-through, as the `S3Error` s3s will serialize: S3's
/// status, and its `<Code>` when the body is the small XML document S3 sends. A body
/// that is not one keeps the status and falls back to a code derived from it.
async fn s3_error_from(status: hyper::StatusCode, body: hyper::body::Incoming) -> s3s::S3Error {
    use http_body_util::BodyExt;
    let limited = http_body_util::Limited::new(body, MAX_ERROR_BODY);
    let text = match limited.collect().await {
        Ok(collected) => String::from_utf8_lossy(&collected.to_bytes()).into_owned(),
        Err(_) => String::new(),
    };
    let code = text
        .split_once("<Code>")
        .and_then(|(_, rest)| rest.split_once("</Code>"))
        .and_then(|(code, _)| s3s::S3ErrorCode::from_bytes(code.trim().as_bytes()))
        .or_else(|| {
            s3s::S3ErrorCode::from_bytes(
                status
                    .canonical_reason()
                    .unwrap_or("")
                    .replace(' ', "")
                    .as_bytes(),
            )
        })
        .unwrap_or(s3s::S3ErrorCode::InternalError);
    let mut err = s3s::S3Error::with_message(code, "returned by S3");
    err.set_status_code(status);
    err
}

/// Whether the probe's status means the caller is authorized (ADR-0041 §
/// 2.4 point 4): `2xx`/`206` is a normal answer, and `416` is a zero-byte
/// object — S3 evaluates authorization before range satisfiability, so a
/// `416` here is "authorized but unsatisfiable", not a deny.
fn probe_allows(status: u16) -> bool {
    matches!(status, 200..=299 | 416)
}

/// Map the probe's denial to the S3 error a client would recognize for it.
/// Anything other than the two modeled codes stays `InternalError` rather
/// than guessing at a code S3 never actually returned.
fn probe_denial(status: u16) -> s3s::S3Error {
    match status {
        403 => s3_error!(AccessDenied, "Access Denied"),
        404 => s3_error!(NoSuchKey, "The specified key does not exist."),
        _ => s3_error!(InternalError, "authorization probe denied"),
    }
}

/// Derive an [`ObjectHeader`] from the probe's response headers (ADR-0041 §
/// 2.4 point 4): its `Content-Range` total is the object length — the only
/// source of it in this mode, since `HeadObject` is never issued — and its
/// `ETag`/`Content-Type`/`Last-Modified` are the object's own.
///
/// # Errors
///
/// The probe response carried no `Content-Range`, or it did not parse: a
/// probe response in a shape this daemon cannot derive a header from.
fn header_from_probe(resp: &crate::authz::HeldResponse) -> S3Result<ObjectHeader> {
    let content_range = resp
        .headers
        .get(hyper::header::CONTENT_RANGE)
        .and_then(|v| v.to_str().ok())
        .ok_or_else(|| s3_error!(InternalError, "probe response had no Content-Range"))?;
    let object_len = content_range
        .rsplit('/')
        .next()
        .and_then(|total| total.parse::<u64>().ok())
        .ok_or_else(|| {
            s3_error!(
                InternalError,
                "probe response's Content-Range did not parse"
            )
        })?;
    let e_tag = resp
        .headers
        .get(hyper::header::ETAG)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.trim_matches('"').to_owned());
    let content_type = resp
        .headers
        .get(hyper::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_owned);
    let last_modified_epoch_secs =
        http_date_epoch_secs(&resp.headers, hyper::header::LAST_MODIFIED);
    Ok(ObjectHeader::new(
        object_len,
        e_tag,
        content_type,
        last_modified_epoch_secs,
        representation_from_headers(&resp.headers),
    ))
}

/// Parse an HTTP-date header (RFC 9110 § 5.6.7) into epoch seconds — the shape
/// [`RepresentationHeaders::expires_epoch_secs`] and this function's own
/// Last-Modified caller both want, so the date grammar is accepted in exactly
/// one place.
fn http_date_epoch_secs(
    headers: &hyper::HeaderMap,
    name: hyper::header::HeaderName,
) -> Option<i64> {
    headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .and_then(parse_http_date)
}

/// Parse one HTTP-date value (RFC 9110 § 5.6.7) into epoch seconds. The one
/// place this grammar is accepted: [`http_date_epoch_secs`] (a header map) and
/// [`object_header_from_head`] (a `HeadObjectOutput`'s raw `expires_string`,
/// read that way specifically to avoid the deprecated, pre-parsed `expires`)
/// both go through it, so a value neither can parse means the same thing —
/// `None` — everywhere.
fn parse_http_date(value: &str) -> Option<i64> {
    aws_sdk_s3::primitives::DateTime::from_str(
        value,
        aws_sdk_s3::primitives::DateTimeFormat::HttpDate,
    )
    .ok()
    .map(|dt| dt.secs())
}

/// [`RepresentationHeaders`] read off a raw HTTP response — the shape a
/// requester-mode authorization probe returns
/// ([`crate::authz::HeldResponse`]), as opposed to the typed
/// `HeadObjectOutput` [`object_header_from_head`] reads. The probe is a real
/// ranged GET forwarded with the caller's own signature (ADR-0041 § 2.4), so
/// the backend's full header set — including `x-amz-meta-*` — arrives on
/// `resp.headers` exactly as S3 sent it; this is that response's counterpart
/// to [`object_header_from_head`], not a lesser version of it.
///
/// `x-amz-meta-*` is matched by prefix and stored by its suffix, the same
/// shape `HeadObjectOutput::metadata` already normalises to.
fn representation_from_headers(headers: &hyper::HeaderMap) -> RepresentationHeaders {
    /// The one HTTP header prefix S3 uses for user metadata. Matched
    /// case-sensitively because every header name reaching this proxy over
    /// HTTP/1.1 or HTTP/2 already arrives lower-cased (RFC 9113 § 8.2.1;
    /// `hyper` lower-cases HTTP/1.1 names on receipt too).
    const AMZ_META_PREFIX: &str = "x-amz-meta-";
    let metadata = headers
        .iter()
        .filter_map(|(name, value)| {
            let suffix = name.as_str().strip_prefix(AMZ_META_PREFIX)?;
            Some((suffix.to_owned(), value.to_str().ok()?.to_owned()))
        })
        .collect();
    let header_str = |name: hyper::header::HeaderName| {
        headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_owned)
    };
    RepresentationHeaders {
        metadata,
        content_encoding: header_str(hyper::header::CONTENT_ENCODING),
        content_disposition: header_str(hyper::header::CONTENT_DISPOSITION),
        content_language: header_str(hyper::header::CONTENT_LANGUAGE),
        cache_control: header_str(hyper::header::CACHE_CONTROL),
        expires_epoch_secs: http_date_epoch_secs(headers, hyper::header::EXPIRES),
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

    /// Our own ETag, as [`super::head_backend`] stores it: unquoted.
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
        // and `head_backend` stored ours unquoted, so the two meet already normalised.
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
