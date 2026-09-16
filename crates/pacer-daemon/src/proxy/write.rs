//! The write path: every mutating op, and the two shapes it can take.
//!
//! Write path (ADR-0007): all mutating ops are proxied to the backend and
//! NEVER populate the cache; the object's header and every covering chunk are
//! dropped so the next GET re-fetches (read-after-write).
//!
//! ADR-0032 adds the one exception: a new key large enough to gain is decomposed
//! onto the chunk grid and uploaded by the chunks' homes
//! ([`crate::coordinate::ScatterCoordinator`]). [`PacerProxy::try_scatter`] is the
//! decision, and everything it declines — plus every overwrite — takes ADR-0007's
//! path unchanged. ADR-0023's Express/Standard differences (Content-MD5, part
//! ordering) are applied here too, because they are write-request normalization
//! and nothing on the read path can observe them.
//!
//! **The invariant this module owns: a mutation never leaves a reachable stale
//! copy.** [`PacerProxy::invalidate`] drops the header key AND every covering
//! chunk key, locally and at every node that can hold one (the R co-homes union
//! the directory home's sharer set, ADR-0016/0017), and it is AWAITED before the
//! write returns so read-after-write holds cluster-wide.

use pacer_backend::BackendType;
use pacer_cache::object_key;
use s3s::dto::{self, ETag};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};
use tracing::warn;

use crate::coordinate::ScatterTarget;
use crate::scatter::{scatter_verdict, ScatterVerdict};

use super::cluster::{append_holders, Cluster};
use super::PacerProxy;

/// The metric label for a decline (ADR-0032). One arm per variant rather than a
/// `Debug` rendering, so a label a dashboard depends on cannot change because
/// someone renamed an enum.
fn verdict_label(verdict: ScatterVerdict) -> &'static str {
    match verdict {
        // Never reached: the caller only labels declines.
        ScatterVerdict::Scatter => "scatter",
        ScatterVerdict::TooSmall => "too_small",
        ScatterVerdict::TooManyParts { .. } => "too_many_parts",
        ScatterVerdict::ChunkBelowPartMinimum => "chunk_below_part_minimum",
        ScatterVerdict::ChunkAbovePeerMessageLimit => "chunk_above_peer_message_limit",
    }
}

/// Whether `numbers` (the part numbers of a `CompleteMultipartUpload`) satisfy
/// the backend's part-ordering rule (ADR-0023).
///
/// - **Express** directory buckets require part numbers **consecutive from 1**
///   (a gap is a 400 at the backend), so this fails fast on any gap. Matches
///   the pre-ADR-0023 behavior exactly (sort, then require `{1..=n}`).
/// - **Standard** general-purpose buckets only require the numbers **strictly
///   ascending and ≥ 1** (gaps are allowed — a sparse set like `1, 3, 5` is a
///   valid completion), so a set Express would reject is accepted and proxied.
///
/// An empty part list is invalid for either backend.
fn part_numbers_ok(numbers: &[i32], backend_type: BackendType) -> bool {
    if numbers.is_empty() {
        return false;
    }
    if backend_type.is_express() {
        let mut sorted = numbers.to_vec();
        sorted.sort_unstable();
        return sorted.iter().enumerate().all(|(i, n)| *n == i as i32 + 1);
    }
    // Standard: ascending, ≥ 1, gaps permitted.
    numbers.first().is_some_and(|first| *first >= 1) && numbers.windows(2).all(|w| w[0] < w[1])
}

/// Request headers that describe how a client framed a PUT body.
///
/// An **allowlist**, not the whole header map, and that is the point: the map also
/// carries `authorization` (a SigV4 signature) and `x-amz-security-token`, and a
/// failure log is precisely where those must never appear. Every name here is
/// framing metadata that contains no secret.
///
/// Why framing is worth logging at all: on 2026-08-25 every scatter-off PUT from one
/// client failed while the same PUT from another client succeeded through the same
/// daemon, and nothing in the logs could tell the two apart
/// (`bench/ladder/results/w1-write-scatter.md`). These headers are what distinguishes
/// them — `content-encoding: aws-chunked` with `x-amz-decoded-content-length` and
/// `x-amz-trailer` is the streaming-trailer shape a current SDK uses for a body it
/// cannot hold in memory, a bare `x-amz-checksum-*` is the in-memory header shape,
/// and `x-amz-content-sha256` says whether the payload was signed or `UNSIGNED-PAYLOAD`.
const PUT_FRAMING_HEADERS: [&str; 7] = [
    "content-length",
    "content-encoding",
    "content-md5",
    "x-amz-decoded-content-length",
    "x-amz-content-sha256",
    "x-amz-sdk-checksum-algorithm",
    "x-amz-trailer",
];

/// Prefix of the per-algorithm request-checksum headers (`x-amz-checksum-crc32`,
/// `…-sha256`, …). Matched by prefix rather than enumerated so a checksum algorithm
/// added to S3 after this was written still shows up in the log.
const CHECKSUM_HEADER_PREFIX: &str = "x-amz-checksum-";

/// Render a PUT's framing headers as one `k=v` line for a log field.
///
/// Absent headers are omitted rather than printed as empty, so the line reads as the
/// set the client actually sent. A non-UTF-8 value is lossy-rendered instead of
/// dropped: a malformed framing header is itself a candidate explanation for a
/// rejected PUT, so it must not vanish from the record.
fn put_framing(headers: &hyper::HeaderMap) -> String {
    let mut parts: Vec<String> = Vec::new();
    for name in PUT_FRAMING_HEADERS {
        if let Some(value) = headers.get(name) {
            parts.push(format!(
                "{name}={}",
                String::from_utf8_lossy(value.as_bytes())
            ));
        }
    }
    for (name, value) in headers {
        if name.as_str().starts_with(CHECKSUM_HEADER_PREFIX) {
            parts.push(format!(
                "{name}={}",
                String::from_utf8_lossy(value.as_bytes())
            ));
        }
    }
    parts.join(" ")
}

impl PacerProxy {
    // ---- write path: scatter (ADR-0032), else proxy + invalidate (ADR-0007) ----

    /// Scatter this PUT if it qualifies, or `None` to leave it to ADR-0007's path.
    ///
    /// The order of the checks is deliberate and is the difference between a cheap
    /// decision and an expensive one. The free ones come first — coordinator
    /// present, length known, no client checksum we would have to replace, and the
    /// size/grid verdict — so the only PUTs that pay for the existence `HEAD` are
    /// ones already large enough to scatter, where a few milliseconds against a
    /// multi-GiB body is noise. Putting the `HEAD` first would tax every small write
    /// for a path it can never take.
    ///
    /// # Errors
    ///
    /// Only a failed scatter, which is a failed write: by then the body is consumed
    /// and there is no plain PUT to fall back to (see `coordinate`). Every
    /// *decision* not to scatter is `Ok(None)`, never an error.
    async fn try_scatter(
        &self,
        req: &mut S3Request<dto::PutObjectInput>,
    ) -> S3Result<Option<S3Response<dto::PutObjectOutput>>> {
        let Some(coordinator) = &self.scatter else {
            return Ok(None);
        };
        let Some(object_len) = req.input.content_length.and_then(|n| u64::try_from(n).ok()) else {
            // No length means no plan: the window count and every home are computed
            // before the body arrives precisely so the object never has to be
            // buffered to discover them.
            self.note_no_scatter("no_content_length");
            return Ok(None);
        };
        if Self::unreproducible_client_checksum(&req.input) {
            // A digest the coordinator cannot recompute from the bytes as it splits
            // them. Declining is the honest answer — the alternative is holding the
            // object to hash it, which is the one thing the streaming design refuses
            // to do. A client CRC32 is NOT in this set: see `expected_crc32`.
            self.note_no_scatter("client_checksum");
            return Ok(None);
        }
        let verdict = scatter_verdict(&self.chunk, object_len, self.scatter_min_object_bytes);
        if !verdict.is_scatter() {
            if verdict.is_misconfiguration() {
                warn!(?verdict, "scatter is configured but can never engage");
            }
            self.note_no_scatter(verdict_label(verdict));
            return Ok(None);
        }
        if self
            .key_already_exists(&req.input.bucket, &req.input.key)
            .await
        {
            // An overwrite needs ADR-0007's awaited invalidation of every holder;
            // populating a new version over an old one's keys would leave stale
            // copies at holders this write never hears about. Racy by nature — a
            // concurrent writer can slip past — which is why ADR-0015's
            // new-name-per-version precondition is the real guarantee and this is
            // only the cheap guard for the ordinary overwrite.
            self.note_no_scatter("overwrite");
            return Ok(None);
        }
        let Some(body) = req.input.body.take() else {
            self.note_no_scatter("no_body");
            return Ok(None);
        };
        let object_key = object_key(&req.input.bucket, &req.input.key);
        let target = ScatterTarget {
            bucket: &req.input.bucket,
            key: &req.input.key,
            object_key: &object_key,
            object_len,
            content_type: req.input.content_type.as_deref(),
            expected_crc32: req.input.checksum_crc32.as_deref(),
        };
        let result = coordinator.scatter(target, body).await.map_err(|e| {
            warn!(key = %req.input.key, error = %e, "scattered PUT failed");
            match e {
                // The client's own digest disagreed with the body: its error, and
                // the status S3 answers with, not a 500 from us.
                crate::coordinate::ScatterError::ClientDigestMismatch { .. } => {
                    s3_error!(
                        BadDigest,
                        "the object's CRC32 does not match the one supplied"
                    )
                }
                crate::coordinate::ScatterError::Failed(_) => {
                    s3_error!(InternalError, "scattered write failed")
                }
            }
        })?;
        self.note_scattered(&result);
        Ok(Some(S3Response::new(dto::PutObjectOutput {
            // `Strong`, matching how the read path replays a cached ETag: a
            // multipart ETag is composite but not weak, and S3 quotes it itself.
            e_tag: Some(ETag::Strong(result.e_tag)),
            ..Default::default()
        })))
    }

    /// Whether the client asked the backend to validate a whole-body digest the
    /// coordinator cannot reproduce.
    ///
    /// Each of these expects the object checked as one piece, and a multipart
    /// assembly cannot promise it: the composite ETag is a digest of digests, and
    /// per-part checksums cover parts. Rather than silently substitute one, the
    /// scatter stands aside.
    ///
    /// **CRC32 is deliberately absent.** It is the one digest the coordinator
    /// already computes over the whole body in body order, to hand to Complete as a
    /// `FULL_OBJECT` checksum — so a client CRC32 is honoured and enforced rather
    /// than declined (see [`crate::coordinate::ScatterTarget::expected_crc32`]).
    /// Every current AWS SDK sends one by default, so declining on it would have
    /// meant the scatter never engaged for a real client.
    fn unreproducible_client_checksum(input: &dto::PutObjectInput) -> bool {
        input.content_md5.is_some()
            || input.checksum_crc32c.is_some()
            || input.checksum_crc64nvme.is_some()
            || input.checksum_sha1.is_some()
            || input.checksum_sha256.is_some()
    }

    /// Whether `key` already exists in the backend.
    ///
    /// A failure that is not a clean 404 answers "assume it exists", so an unrelated
    /// backend problem routes the write down the conservative path rather than
    /// letting it populate over something it could not see.
    ///
    /// Both 404 shapes count as absent — the modeled `NotFound` variant and a bare
    /// `NoSuchKey` code — for the same reason the read path accepts both
    /// (`head_list_delete_passthrough`): a HEAD has no response body to model the
    /// error from, so which one arrives depends on the backend. Accepting only the
    /// first would make every write look like an overwrite against a backend that
    /// answers the second, and the scatter would never engage there.
    async fn key_already_exists(&self, bucket: &str, key: &str) -> bool {
        match self
            .backend
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
        {
            Ok(_) => true,
            Err(e) => {
                let svc = e.into_service_error();
                !(svc.is_not_found() || svc.meta().code() == Some("NoSuchKey"))
            }
        }
    }

    /// Record a PUT that did not scatter, and why.
    fn note_no_scatter(&self, reason: &str) {
        self.metrics
            .scatter
            .declined
            .with_label_values(&[reason])
            .inc();
    }

    /// Record a scattered PUT's shape: how the windows split between owners and
    /// this node, how many owners took part, and how many windows nobody cached.
    fn note_scattered(&self, result: &crate::coordinate::ScatterResult) {
        let s = &self.metrics.scatter;
        s.scattered.inc();
        s.windows
            .with_label_values(&["owner"])
            .inc_by(result.scattered_windows as u64);
        s.windows
            .with_label_values(&["local"])
            .inc_by(result.local_windows as u64);
        s.uncached_windows.inc_by(result.uncached_windows as u64);
        s.owners_engaged.inc_by(result.distinct_owners as u64);
    }

    /// Drop an object's cache footprint on a write (ADR-0007/ADR-0015): the
    /// header key AND every covering chunk key. If the header is cached, its
    /// `object_len` bounds the chunk set to remove; if not, nothing chunked can
    /// be stale for this object (a covering chunk is only ever inserted after
    /// its header, so no header ⇒ no chunks — same reasoning as ADR-0012's
    /// two-nodes-can-hold-it argument), and only the header key is dropped.
    ///
    /// Each key is removed locally and, in cluster mode, at its own owner (the
    /// only two nodes that can hold it — ADR-0012). Awaited before the write
    /// returns so read-after-write holds cluster-wide; an unreachable owner
    /// logs and counts but never fails the write (the backend mutation already
    /// happened, and reads fall back to the fresh backend anyway).
    async fn invalidate(&self, bucket: &str, key: &str) {
        let object_key = object_key(bucket, key);
        // Learn the chunk set to purge. Prefer a locally cached header; if none
        // (this node is not the header's owner, e.g. a write through a
        // non-owner), the PRE-overwrite object length still tells us how many
        // chunk keys the old object occupied — a HEAD reflects the NEW length,
        // which for a same-or-larger overwrite still covers every stale chunk
        // key, and for a delete returns 404 (nothing to purge beyond the
        // header). A missing length falls back to header-only removal.
        let object_len = self.invalidation_object_len(&object_key, bucket, key).await;
        self.invalidate_key(&object_key).await;
        if let Some(object_len) = object_len {
            for idx in 0..self.chunk.chunk_count(object_len) {
                let chunk_key = self.chunk.chunk_key(&object_key, idx);
                self.invalidate_key(&chunk_key).await;
            }
        }
        // Known bound — shrinking overwrite: if the object is overwritten SMALLER,
        // chunks beyond the new length are not in this covering set, so they
        // ORPHAN (stale keys) rather than being purged. This is a bounded capacity
        // leak, never a wrong serve: the header is invalidated, so a later read
        // re-HEADs the new (shorter) length and its covering set never includes
        // the orphaned indices — they are unreachable, and LRU reclaims them.
        // ADR-0015's immutability precondition (checkpoint writers write a new
        // name per version, never overwrite in place) means this case does not
        // arise for the target workload; the leak is the accepted cost otherwise.
    }

    /// The object length used to compute an invalidation's covering chunk set:
    /// a locally cached header wins; otherwise a backend `HeadObject` (a
    /// non-owner writer holds no header). A 404 (deleted) or any HEAD failure
    /// yields `None` — only the header key is then dropped, which is correct
    /// (no header ⇒ no chunk was inserted under this object at this node).
    async fn invalidation_object_len(
        &self,
        object_key: &str,
        bucket: &str,
        key: &str,
    ) -> Option<u64> {
        if let Ok(Some(entry)) = self.tier.cache().get(object_key).await {
            if let Some(h) = entry.value().as_header() {
                return Some(h.object_len);
            }
        }
        // Only a clustered writer needs the backend HEAD: single-node, a
        // no-header object never had chunks inserted, so there is nothing more
        // to purge.
        self.cluster.as_ref()?;
        let head = self
            .backend
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .ok()?;
        head.content_length().and_then(|l| u64::try_from(l).ok())
    }

    /// Remove one cache key locally and, in cluster mode, at every node that
    /// can hold it (ADR-0016/0017). With replication the holder set is no
    /// longer "the one owner": it is the R co-homes (computed from the ring,
    /// each fills on read-through) UNION the directory home's sharer set (the
    /// layer-1 admitters, ADR-0016). All fan-out targets are invalidated and
    /// AWAITED before the write returns, so read-after-write holds cluster-wide
    /// (ADR-0007); an unreachable target is logged and counted but never fails
    /// the write (the backend mutation already happened, and a stale copy at a
    /// missed node is corrected by that node's own eventual re-fetch).
    ///
    /// The sharer set is the "who can hold this" bound (ADR-0017) — this is
    /// writer-driven fan-out over that bound rather than a home-side re-fan,
    /// which keeps all fan-out where the transport lives and avoids a second
    /// awaited hop; invalidation is a rare path (checkpoint objects are
    /// immutable, ADR-0016), so the writer-side cost is immaterial.
    async fn invalidate_key(&self, cache_key: &str) {
        self.tier.forget(cache_key).await;
        let Some(cluster) = &self.cluster else {
            return;
        };
        for target in self.invalidation_targets(cluster, cache_key).await {
            if target.name() == cluster.local_node {
                continue;
            }
            if let Err(e) = cluster.transport.invalidate(&target, cache_key).await {
                self.metrics.peer_fallbacks.inc();
                warn!(key = %cache_key, target = %target.name(), error = %e,
                    "peer invalidation failed; stale window until that node re-fetches");
            }
        }
    }

    /// Every node that can hold `cache_key` (ADR-0016/0017): the R co-homes
    /// (from the ring) plus the directory home's listed sharers (layer-1
    /// admitters), de-duplicated. A directory-lookup failure or miss is not
    /// fatal — the R homes are always invalidated (they hold copies regardless
    /// of the directory), so a lost lookup only risks a stale layer-1 copy,
    /// which that node's next write-through or re-fetch corrects.
    async fn invalidation_targets(
        &self,
        cluster: &Cluster,
        cache_key: &str,
    ) -> Vec<pacer_ring::NodeId> {
        let mut targets = cluster.ring.homes(cache_key, cluster.replication_r);
        if let Some(home) = targets.first().cloned() {
            if let Ok(Some(set)) = cluster.transport.lookup_sharers(&home, cache_key).await {
                append_holders(cluster, set.holders, &mut targets);
            }
        }
        targets
    }

    // ---- write path: proxy + invalidate (ADR-0007) ----

    /// Serve one PUT: the scatter if it qualifies (ADR-0032), else proxy the body
    /// through and invalidate the object's cache footprint (ADR-0007).
    ///
    /// # Errors
    ///
    /// A failed scatter (by then the body is consumed, so there is no plain PUT to
    /// fall back to) or the backend's own rejection of the passthrough — the latter
    /// logged with the request's framing first, because that log line is the only
    /// record of WHY on the node that forwarded it.
    pub(super) async fn scatter_or_write_through(
        &self,
        mut req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.count("put_object");
        self.map_bucket(&mut req.input.bucket);
        // Express directory buckets REJECT Content-MD5 (501), so on Express we
        // strip it and rely on the backend SDK's CRC32 request checksum: the
        // MD5 the client sent guarded client→daemon, daemon→backend gets a
        // fresh CRC32 over the same bytes. Standard general-purpose buckets
        // accept Content-MD5, so we forward it (ADR-0023) and the backend
        // validates the digest end-to-end — parity, not an Express assumption.
        if self.backend_type.is_express() {
            req.input.content_md5 = None;
        }
        // ADR-0032: a new key large enough to gain is decomposed onto the chunk
        // grid and uploaded by the chunks' homes. Everything this declines — and
        // every overwrite — keeps ADR-0007's path below, unchanged.
        if let Some(scattered) = self.try_scatter(&mut req).await? {
            return Ok(scattered);
        }
        let (bucket, key) = (req.input.bucket.clone(), req.input.key.clone());
        // A failed passthrough PUT used to propagate silently: the client saw the
        // s3s-wrapped SDK error and the daemon logged nothing, so a write that the
        // backend rejected left no record of WHY on the node that forwarded it. The
        // scatter path warns on failure (`try_scatter`); this path is the one every
        // decline falls back to, and it is where the 2026-08-25 run went blind
        // (bench/ladder/results/w1-write-scatter.md).
        let framing = put_framing(&req.headers);
        let resp = self.inner.put_object(req).await.inspect_err(|e| {
            warn!(
                key = %key,
                code = ?e.code(),
                status = ?e.status_code(),
                message = e.message().unwrap_or("<none>"),
                request_id = e.request_id().unwrap_or("<none>"),
                source = ?e.source(),
                framing = %framing,
                "passthrough PUT rejected by the backend",
            );
        })?;
        self.invalidate(&bucket, &key).await;
        Ok(resp)
    }

    /// Proxy a COPY and invalidate its destination (ADR-0007).
    ///
    /// # Errors
    ///
    /// The backend's own; the invalidation itself never fails a write.
    pub(super) async fn copy_and_invalidate(
        &self,
        mut req: S3Request<dto::CopyObjectInput>,
    ) -> S3Result<S3Response<dto::CopyObjectOutput>> {
        self.count("copy_object");
        self.map_bucket(&mut req.input.bucket);
        let (bucket, key) = (req.input.bucket.clone(), req.input.key.clone());
        let resp = self.inner.copy_object(req).await?;
        self.invalidate(&bucket, &key).await;
        Ok(resp)
    }

    /// Proxy a DELETE and invalidate the key it removed (ADR-0007).
    ///
    /// # Errors
    ///
    /// The backend's own; the invalidation itself never fails a write.
    pub(super) async fn delete_and_invalidate(
        &self,
        mut req: S3Request<dto::DeleteObjectInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectOutput>> {
        self.count("delete_object");
        self.map_bucket(&mut req.input.bucket);
        let (bucket, key) = (req.input.bucket.clone(), req.input.key.clone());
        let resp = self.inner.delete_object(req).await?;
        self.invalidate(&bucket, &key).await;
        Ok(resp)
    }

    /// Proxy a batch DELETE and invalidate every key it names (ADR-0007).
    ///
    /// The keys are captured BEFORE the request is forwarded, because forwarding
    /// consumes it — and each is invalidated even if the backend refused that one,
    /// which is the conservative direction: a dropped cache entry costs a re-fetch,
    /// a kept one could serve a deleted object.
    ///
    /// # Errors
    ///
    /// The backend's own; the invalidations themselves never fail a write.
    pub(super) async fn delete_many_and_invalidate(
        &self,
        mut req: S3Request<dto::DeleteObjectsInput>,
    ) -> S3Result<S3Response<dto::DeleteObjectsOutput>> {
        self.count("delete_objects");
        self.map_bucket(&mut req.input.bucket);
        let bucket = req.input.bucket.clone();
        let keys: Vec<String> = req
            .input
            .delete
            .objects
            .iter()
            .map(|o| o.key.clone())
            .collect();
        let resp = self.inner.delete_objects(req).await?;
        for key in &keys {
            self.invalidate(&bucket, key).await;
        }
        Ok(resp)
    }

    /// Validate the part order for this backend (ADR-0023), proxy the completion,
    /// and invalidate the object it just made visible (ADR-0007).
    ///
    /// # Errors
    ///
    /// `InvalidPartOrder` for a part set this backend would reject anyway — failed
    /// fast, with the error the backend would have given — or the backend's own.
    pub(super) async fn complete_mpu_and_invalidate(
        &self,
        mut req: S3Request<dto::CompleteMultipartUploadInput>,
    ) -> S3Result<S3Response<dto::CompleteMultipartUploadOutput>> {
        self.count("complete_multipart_upload");
        self.map_bucket(&mut req.input.bucket);
        // Validate part ordering per the backend's rule (ADR-0023) and fail
        // fast with the same error the backend would give: Express requires
        // consecutive-from-1 parts, Standard only ascending-with-gaps. Gating
        // this on the backend is a correctness requirement — a valid sparse
        // Standard completion (e.g. parts 1, 3, 5) must NOT be rejected here.
        if let Some(parts) = req
            .input
            .multipart_upload
            .as_ref()
            .and_then(|u| u.parts.as_ref())
        {
            let numbers: Vec<i32> = parts.iter().filter_map(|p| p.part_number).collect();
            if !part_numbers_ok(&numbers, self.backend_type) {
                return Err(s3_error!(
                    InvalidPartOrder,
                    "part numbers must be ascending (Express: consecutive from 1)"
                ));
            }
        }
        let (bucket, key) = (req.input.bucket.clone(), req.input.key.clone());
        let resp = self.inner.complete_multipart_upload(req).await?;
        self.invalidate(&bucket, &key).await;
        Ok(resp)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn express_requires_consecutive_parts_standard_allows_gaps() {
        // Express: consecutive from 1 only (sort-insensitive, matching the
        // pre-ADR-0023 behavior).
        assert!(part_numbers_ok(&[1, 2, 3], BackendType::Express));
        assert!(part_numbers_ok(&[3, 1, 2], BackendType::Express)); // set {1,2,3}
        assert!(!part_numbers_ok(&[1, 3], BackendType::Express)); // gap → reject
        assert!(!part_numbers_ok(&[2], BackendType::Express)); // must start at 1
        assert!(!part_numbers_ok(&[], BackendType::Express)); // empty invalid

        // Standard: ascending with gaps is valid; only non-ascending / empty /
        // < 1 is rejected. This is the parity fix — a sparse completion that
        // Express rejects must be accepted here.
        assert!(part_numbers_ok(&[1, 3, 5], BackendType::Standard));
        assert!(part_numbers_ok(&[2, 7], BackendType::Standard));
        assert!(part_numbers_ok(&[1], BackendType::Standard));
        assert!(!part_numbers_ok(&[3, 1], BackendType::Standard)); // not ascending
        assert!(!part_numbers_ok(&[1, 1], BackendType::Standard)); // duplicate
        assert!(!part_numbers_ok(&[0, 1], BackendType::Standard)); // part < 1
        assert!(!part_numbers_ok(&[], BackendType::Standard)); // empty invalid
    }

    /// Build a header map from `(name, value)` pairs.
    fn headers(pairs: &[(&str, &str)]) -> hyper::HeaderMap {
        let mut map = hyper::HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                hyper::header::HeaderName::from_bytes(name.as_bytes()).unwrap(),
                hyper::header::HeaderValue::from_str(value).unwrap(),
            );
        }
        map
    }

    #[test]
    fn put_framing_reports_the_streaming_trailer_shape() {
        // What a current AWS SDK sends for a body it cannot hold in memory. All
        // four markers have to survive into the log together — it is their
        // combination, not any one of them, that names the shape.
        let line = put_framing(&headers(&[
            ("content-length", "16777770"),
            ("content-encoding", "aws-chunked"),
            ("x-amz-decoded-content-length", "16777216"),
            ("x-amz-content-sha256", "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
            ("x-amz-trailer", "x-amz-checksum-crc32"),
        ]));
        assert!(line.contains("content-encoding=aws-chunked"), "{line}");
        assert!(
            line.contains("x-amz-decoded-content-length=16777216"),
            "{line}"
        );
        assert!(
            line.contains("x-amz-trailer=x-amz-checksum-crc32"),
            "{line}"
        );
        assert!(
            line.contains("x-amz-content-sha256=STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            "{line}"
        );
    }

    #[test]
    fn put_framing_reports_the_in_memory_header_shape_and_omits_what_is_absent() {
        // The other shape: one whole-body checksum as a header, no chunked framing.
        // Matched by prefix, so it is reported even though it is not in the
        // allowlist by name.
        let line = put_framing(&headers(&[
            ("content-length", "4294967296"),
            ("x-amz-checksum-crc32", "q1FlJw=="),
            ("x-amz-sdk-checksum-algorithm", "CRC32"),
        ]));
        assert!(line.contains("x-amz-checksum-crc32=q1FlJw=="), "{line}");
        assert!(line.contains("content-length=4294967296"), "{line}");
        // Nothing framed it as chunked, so those names must be absent rather than
        // present-and-empty — the line has to read as what the client really sent.
        assert!(!line.contains("content-encoding"), "{line}");
        assert!(!line.contains("x-amz-trailer"), "{line}");
        assert!(!line.contains("x-amz-decoded-content-length"), "{line}");
    }

    #[test]
    fn put_framing_never_logs_a_credential() {
        // The reason the allowlist exists. A PUT's header map always carries a
        // SigV4 signature, and this line goes to a log that gets pasted into
        // bench write-ups.
        let line = put_framing(&headers(&[
            ("content-length", "1024"),
            (
                "authorization",
                "AWS4-HMAC-SHA256 Credential=AKIAEXAMPLE/20260826/us-east-2/s3/aws4_request, \
                 SignedHeaders=host, Signature=deadbeef",
            ),
            ("x-amz-security-token", "FwoGZXIvYXdzEXAMPLETOKEN"),
        ]));
        assert_eq!(line, "content-length=1024");
    }

    #[test]
    fn put_framing_of_no_framing_headers_is_empty() {
        assert_eq!(put_framing(&headers(&[("host", "127.0.0.1:9000")])), "");
    }
}
