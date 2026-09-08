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
    fn cacheable_shape(input: &dto::GetObjectInput) -> bool {
        input.if_match.is_none()
            && input.if_none_match.is_none()
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
