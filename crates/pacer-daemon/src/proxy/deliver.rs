//! Delivery into client-supplied memory: the decision, the fan-out, the answer.
//!
//! ADR-0026 point 1 (a request may name memory it owns), point 2 (the answer is
//! then a header-only 200 whose `x-pacer-delivered` PRESENCE is the completion
//! signal), point 5 (with a CRC32, the only integrity check left once the body and
//! the SDK's own checksum are gone) and point 8 (a target this node cannot honour
//! degrades to a body, it never fails a read). ADR-0030 adds the pre-flight
//! exchange answered here — "who may write into my window for this object?", from
//! the ring alone, moving no bytes.
//!
//! **The invariant this module owns: the response is all-or-nothing.** The CRC32
//! folds only the windows that landed, so a partially-filled window a client
//! believes is complete is silent corruption — which is why one window that cannot
//! be placed degrades the WHOLE request to a body rather than being reported as a
//! short delivery. Fan-out is `delivery.parallelism`, deliberately not the body
//! path's `fill_parallelism`: windows are disjoint destinations in the client's own
//! buffer, so there is no ordering constraint to bound the look-ahead with.

use std::sync::Arc;
use std::time::{Duration, SystemTime};

use pacer_cache::chunk::ObjectHeader;
use pacer_cache::object_key;
use s3s::dto::{self, ETag, Timestamp};
use s3s::{S3Response, S3Result};
use tracing::trace;

use crate::delivery::{
    chunk_windows, ChunkWindow, DeliveryDigest, TargetRejection, TargetSpec, CHECKSUM_HEADER,
    DELIVERED_HEADER, TARGET_HEADER,
};

use super::fill::FillCtx;
use super::place::{digest_delivered, Placement};
use super::target::ClientMemory;
use super::PacerProxy;

/// One window's delivery result: how many bytes landed, where, and (unless the
/// client opted out) that window's own CRC32.
///
/// The digest travels with the window rather than being computed afterwards
/// because a single trailing pass over the delivered bytes is O(bytes) and
/// serial — at checkpoint sizes that is tens of seconds added after the last byte
/// has already landed. `dst_at` rides along so the caller can fold the digests in
/// **offset order**, which CRC32 combination requires.
struct DeliveredWindow {
    dst_at: usize,
    bytes: u64,
    digest: Option<DeliveryDigest>,
}

impl PacerProxy {
    /// The target descriptor this request asked for, or `None` when delivery is
    /// disabled or the header is absent (ADR-0026 point 1).
    ///
    /// A header whose bytes are not UTF-8 comes back as its lossy form on
    /// purpose: it then fails [`TargetSpec::parse`] and the client is told its
    /// descriptor is malformed, rather than being silently served a body it did
    /// not ask for.
    pub(super) fn requested_target(&self, headers: &hyper::HeaderMap) -> Option<String> {
        if !self.delivery.enabled {
            return None;
        }
        let raw = headers.get(TARGET_HEADER)?;
        Some(String::from_utf8_lossy(raw.as_bytes()).into_owned())
    }

    /// The range whose holders this request wants named instead of its bytes, or `None` when
    /// delivery is disabled or the header is absent (ADR-0030's pre-flight exchange).
    ///
    /// Gated on `delivery.enabled` **exactly as [`Self::requested_target`] is**, and for the same
    /// reason: with delivery off there is no window for anyone to write into, so there is nothing
    /// to prime, and a disabled daemon must not even parse the value. The consequence for a
    /// client is that a disabled daemon is indistinguishable from one that predates this — both
    /// answer the GET normally — which is why a pre-flight sends `Range: bytes=0-0` and reads the
    /// *response* header rather than the status: the fallback then costs one byte instead of a
    /// checkpoint (see [`crate::preflight::ENDPOINTS_HEADER`]).
    pub(super) fn requested_endpoints(&self, headers: &hyper::HeaderMap) -> Option<String> {
        if !self.delivery.enabled {
            return None;
        }
        let raw = headers.get(crate::preflight::GET_ENDPOINTS_HEADER)?;
        Some(String::from_utf8_lossy(raw.as_bytes()).into_owned())
    }

    /// Answer "who may write into my window for this object?" from the ring alone.
    ///
    /// **Moves no bytes, by construction rather than by promise.** It never resolves the object's
    /// header, so it reads no cache entry and issues no backend request: the chunk indices come
    /// from the marker's own offset and the concurrency bound, and a chunk key needs only an
    /// index. `an_endpoint_query_moves_no_bytes` asks for a key that does not exist and still
    /// gets an answer, which a `HeadObject` would have turned into a 404.
    ///
    /// `bucket` must already be alias-resolved ([`Self::map_bucket`]), because chunk keys are.
    ///
    /// # Errors
    ///
    /// `InvalidRequest` when the marker's value does not parse — a client-side bug, and the same
    /// treatment a malformed target descriptor gets.
    pub(super) async fn answer_endpoints(
        &self,
        bucket: &str,
        key: &str,
        raw: &str,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let object_key = object_key(bucket, key);
        let answered = crate::preflight::answer(
            self.cluster.as_ref(),
            &self.chunk,
            &object_key,
            raw,
            self.delivery.parallelism,
        )
        .await;
        let (resp, outcome, nodes) = match answered {
            Ok(answered) => answered,
            Err(e) => {
                self.note_preflight(crate::preflight::OUTCOME_MALFORMED, 0);
                return Err(e);
            }
        };
        self.note_preflight(outcome, nodes);
        trace!(
            key,
            outcome,
            nodes,
            "answered a pre-flight endpoint query (ADR-0030)"
        );
        Ok(resp)
    }

    /// Count one pre-flight answer by outcome, and the holder fan-out it named.
    fn note_preflight(&self, outcome: &str, nodes: usize) {
        self.metrics
            .delivery
            .preflight
            .with_label_values(&[outcome])
            .inc();
        self.metrics.delivery.preflight_nodes.inc_by(nodes as u64);
    }

    /// Deliver object bytes `range` into the client's own memory and answer with
    /// a header-only 200 (ADR-0026 point 2).
    ///
    /// `Ok(None)` means **serve the body instead**: the ADR's degradable path,
    /// reached when honouring the target would exceed a pinned-memory ceiling.
    /// The client detects it by the absence of `x-pacer-delivered`.
    ///
    /// # Errors
    ///
    /// `InvalidRequest` when the descriptor is malformed, names a segment that
    /// cannot back it, or names a window smaller than the requested range — all
    /// client-side bugs, none of which a body fallback would help. Otherwise the
    /// underlying read's own error (a backend failure with no bytes to serve),
    /// in which case no `x-pacer-delivered` is sent and the client must not trust
    /// its buffer.
    pub(super) async fn deliver(
        &self,
        ctx: &Arc<FillCtx>,
        raw: &str,
        header: &ObjectHeader,
        range: std::ops::Range<u64>,
        ranged: bool,
    ) -> S3Result<Option<S3Response<dto::GetObjectOutput>>> {
        let spec = match TargetSpec::parse(raw) {
            Ok(spec) => spec,
            Err(e) => return self.target_rejected(e),
        };
        let target = match self.open_client_memory(&spec) {
            Ok(target) => target,
            Err(e) => return self.target_rejected(e),
        };
        let want = range.end - range.start;
        if want > target.window_len() as u64 {
            return self.target_rejected(TargetRejection::Unusable(format!(
                "window is {} B, too small for the {want} B requested",
                target.window_len()
            )));
        }
        // No registration yet — see `ClientMemory`: a read the node can serve locally
        // never needs one, and pinning is ~3.6 GB/s of pure latency when it is not.
        let client_memory = Arc::new(target);
        let windows = chunk_windows(&ctx.chunk, header.object_len, range.start, range.end);
        let Some(mut landed) = self
            .run_delivery(ctx, &client_memory, windows, spec.checksum)
            .await?
        else {
            // A window could not be placed at all — only possible for a client-registered
            // target, and a degradation rather than a fault (ADR-0026 point 8).
            return self.target_rejected(TargetRejection::Unsupported(
                "client-registered memory (no usable RDMA path to it right now)".to_owned(),
            ));
        };
        let delivered: u64 = landed.iter().map(|w| w.bytes).sum();
        self.metrics.delivery.requests.inc();
        self.metrics.delivery.bytes.inc_by(delivered);
        // CRC32 is position-dependent, so windows fold in ASCENDING OFFSET.
        // `buffered` happens to yield in input order; sorting makes the digest
        // independent of that rather than quietly dependent on it.
        landed.sort_unstable_by_key(|w| w.dst_at);
        let digest = landed.iter().filter_map(|w| w.digest.as_ref()).fold(
            DeliveryDigest::empty(),
            |mut acc, d| {
                acc.absorb(d);
                acc
            },
        );
        let checksum = spec.checksum.then(|| digest.header_value());
        trace!(
            target = spec.memory.label(),
            delivered,
            windows = landed.len(),
            ?checksum,
            "delivered into client memory (ADR-0026)"
        );
        Ok(Some(Self::delivered_output(
            header,
            range,
            ranged,
            delivered,
            checksum.as_deref(),
        )))
    }

    // Registration deliberately does NOT live here: it is lazy, on `ClientMemory`.
    // The obvious place to put it is beside the mapping, and doing that pinned the
    // whole window on every read — ~47 % of a 4 GiB request on reads the node could
    // serve locally, where no rkey is ever needed (planning/19 § Track C).

    /// Fan-out is `delivery.parallelism`, NOT `fill_parallelism`, and that
    /// distinction is why this is a separate function at all.
    ///
    /// The body path's bound exists because it must emit bytes to the client **in
    /// order**, so look-ahead deeper than its reorder window buys nothing.
    /// Delivery has no ordering constraint: every window is a disjoint
    /// destination in the client's own buffer. Inheriting the body path's 8 was
    /// measurably wrong for the shape that matters — one GET for a multi-GiB
    /// checkpoint is thousands of windows, and 8 at a time turns a fabric-limited
    /// transfer into ~N/8 serial rounds.
    ///
    /// Each window's CRC32 is computed by the task that delivered it (when the
    /// client wants one), so integrity costs no extra serial pass at the end —
    /// which at checkpoint sizes would be tens of seconds after the last byte
    /// already landed.
    ///
    /// ## Why a decline is not the end of the request
    ///
    /// A declined window used to degrade the whole GET immediately, discarding the
    /// acceleration for every other window in it (planning/19 § Track C, "one declined chunk
    /// degrades the whole GET to a body"). Two things are wrong with that and one is not.
    ///
    /// **What is not wrong: the response really is all-or-nothing, and must stay so.** The
    /// answer is a header-only 200 whose `x-pacer-delivered` *presence* is the completion
    /// signal (ADR-0026 point 2, ADR-0030 point 3), and the CRC32 folds only the windows that
    /// landed. A partially-filled window the client believes is complete is silent
    /// corruption, and both shipped shims would accept it: they read the byte count, verify
    /// the checksum over exactly that many bytes — which *passes*, because the daemon
    /// digested the same subset — and hand back a buffer with a hole in it. So a per-window
    /// fallback to the body is NOT expressible without a protocol change, and this does not
    /// attempt one.
    ///
    /// **What is wrong is treating every decline as final.** Two of the five reasons a token
    /// WRITE declines are "the client's pump has not caught up yet"
    /// (`TokenDecline::is_transient`) and resolve in milliseconds. Re-attempting *that one
    /// window* keeps the request on the fast path; it is the same recoverable-not-impossible
    /// reasoning ADR-0030 point 2 applies to the announce.
    ///
    /// **And it is wrong to keep working after a decline that is final.** `buffered` would
    /// otherwise resolve every remaining window — a staging copy, a WRITE and a digest each,
    /// thousands of them on a checkpoint — and then throw all of it away, while the client
    /// waits to be told to re-read the object over the body path. A terminal decline
    /// short-circuits instead.
    ///
    /// # Errors
    ///
    /// The first window that cannot be resolved (a backend failure with no bytes
    /// to serve); remaining windows are abandoned.
    async fn run_delivery(
        &self,
        ctx: &Arc<FillCtx>,
        client_memory: &Arc<ClientMemory>,
        windows: Vec<ChunkWindow>,
        checksum: bool,
    ) -> S3Result<Option<Vec<DeliveredWindow>>> {
        use futures::StreamExt;
        let mut resolving = futures::stream::iter(windows)
            .map(|window| {
                let (ctx, client_memory) = (Arc::clone(ctx), Arc::clone(client_memory));
                async move {
                    // Width and latency, measured together because neither alone is
                    // interpretable: a per-chunk cost derived from the wall clock assumes
                    // the fan-out was 1, and a fan-out with no latency beside it cannot say
                    // whether the width bought anything.
                    let in_flight = ctx.metrics.delivery.chunk_in_flight();
                    let started = std::time::Instant::now();
                    let resolved = ctx.place_with_retry(window, &client_memory).await?;
                    let elapsed = started.elapsed().as_secs_f64();
                    drop(in_flight);
                    let Placement::Placed(placed) = resolved else {
                        // Every re-attempt this window was owed is spent. Only a token
                        // target reaches here (see `place_window`) — there is no memcpy to
                        // fall back to, so the REQUEST degrades to a body.
                        return Ok(None);
                    };
                    ctx.metrics
                        .delivery
                        .chunk_seconds
                        .with_label_values(&[placed.source])
                        .observe(elapsed);
                    let digest = match (checksum, placed.digest) {
                        // A token target digested what it sent — the window is not readable.
                        (true, Some(digest)) => Some(digest),
                        // A mapped target's digest is read back from the client's memory,
                        // which proves what is in it rather than what we sent.
                        (true, None) => match client_memory.mapped_target() {
                            Some(target) => Some(
                                digest_delivered(target, window.dst_at, placed.bytes as usize)
                                    .await?,
                            ),
                            None => None,
                        },
                        (false, _) => None,
                    };
                    // Annotated because the pipeline is now driven by hand: `try_collect`
                    // used to pin this async block's error type, and a bare `Ok` leaves it
                    // ambiguous between the several `From` impls `S3Error` has.
                    Ok::<_, s3s::S3Error>(Some(DeliveredWindow {
                        dst_at: window.dst_at,
                        bytes: placed.bytes,
                        digest,
                    }))
                }
            })
            .buffered(self.delivery.parallelism.max(1));
        // Driven by hand rather than `try_collect`ed, so that a decline stops the pipeline
        // instead of being noticed after every remaining window has been delivered and
        // thrown away. Dropping `resolving` cancels what is still queued; WRITEs already
        // posted are reaped by their completion pump, which is the same path a mid-request
        // error has always taken (ADR-0028's orphaned source guards).
        //
        // A `Vec` of *placed* windows, never of `Option`s: the caller's contract is that
        // `Some` means EVERY window landed, and building a shorter list on the way to
        // deciding that is how a partial delivery would come to look like a complete one.
        let mut landed = Vec::new();
        while let Some(resolved) = resolving.next().await {
            match resolved? {
                Some(window) => landed.push(window),
                // Out of re-attempts. The request degrades to a body — windows already
                // written are harmless, because a 200 without `x-pacer-delivered` tells the
                // client to read the body (ADR-0030).
                None => return Ok(None),
            }
        }
        Ok(Some(landed))
    }

    /// The header-only 200 a delivery answers with (ADR-0026 points 2 and 5):
    /// no body, `Content-Length: 0`, the delivered byte count, and a checksum of
    /// the delivered bytes — which is the ONLY integrity check left once the body
    /// (and with it the SDK's own) is gone.
    ///
    /// The object's `ETag`/`Content-Type`/`Last-Modified` still ride along: a
    /// loader that records provenance should not have to issue a HEAD to get
    /// what this response already knows.
    fn delivered_output(
        header: &ObjectHeader,
        range: std::ops::Range<u64>,
        ranged: bool,
        delivered: u64,
        checksum: Option<&str>,
    ) -> S3Response<dto::GetObjectOutput> {
        let last_modified = header.last_modified_epoch_secs.map(|s| {
            Timestamp::from(SystemTime::UNIX_EPOCH + Duration::from_secs(s.max(0) as u64))
        });
        let mut resp = S3Response::new(dto::GetObjectOutput {
            body: None,
            accept_ranges: Some("bytes".to_owned()),
            content_length: Some(0),
            content_range: ranged.then(|| {
                format!(
                    "bytes {}-{}/{}",
                    range.start,
                    range.end - 1,
                    header.object_len
                )
            }),
            content_type: header.content_type.clone(),
            e_tag: header.e_tag.clone().map(ETag::Strong),
            last_modified,
            ..Default::default()
        });
        resp.headers.insert(
            hyper::header::HeaderName::from_static(DELIVERED_HEADER),
            hyper::header::HeaderValue::from(delivered),
        );
        // Absent when the client asked for `checksum=none`: the header's PRESENCE
        // is what tells a shim whether to verify, so omitting it is the signal.
        if let Some(value) = checksum.and_then(|c| hyper::header::HeaderValue::from_str(c).ok()) {
            resp.headers.insert(
                hyper::header::HeaderName::from_static(CHECKSUM_HEADER),
                value,
            );
        }
        resp
    }
}
