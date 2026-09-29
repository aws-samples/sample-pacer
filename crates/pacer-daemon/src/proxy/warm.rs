//! The warm-only GET (ADR-0048): the read path up to its last step, then no body.
//!
//! Everything before the body is the read path's own — [`super::read`] resolves the header,
//! decides admission and, in `auth.mode: requester`, runs the caller's authorization probe,
//! then hands a warm here instead of building a body. What this module adds is only what a
//! body would have done implicitly: drive every covering chunk to resolution. Order does not
//! matter without a body, so the chunks resolve `buffer_unordered` at the same
//! `fill_parallelism` bound a body uses; the memory bound is therefore the read path's own.
//!
//! **A warm's answer is honest in one direction only.** `200` means every covering chunk was
//! resolved through the cache path, and a chunk that could not be read fails the warm with
//! the read path's own error. It does not promise every chunk is *held*: a fill the read
//! path declines — a peer that cannot be reached, a home that refuses a `requester`-mode
//! populate — resolves the bytes without keeping them, exactly as it would for a client
//! read. ADR-0048 records that as the first version's limit.

use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};
use pacer_cache::chunk::ObjectHeader;
use pacer_cache::should_admit;
use s3s::dto::{self, ETag};
use s3s::{s3_error, S3Request, S3Response, S3Result, S3};

use crate::warm::{SkipReason, WARMED_HEADER, WARM_HEADER, WARM_REQUESTED, WARM_SKIPPED_HEADER};

use super::fill::FillCtx;
use super::PacerProxy;

/// `outcome` labels of `pacer_warm_requests_total`. Consts because a dashboard
/// depends on the exact strings.
///
/// Every covering chunk resolved.
const OUTCOME_WARMED: &str = "warmed";
/// A request or object the cache never holds; nothing was read.
const OUTCOME_SKIPPED: &str = "skipped";
/// A chunk could not be read, and the caller was told so.
const OUTCOME_FAILED: &str = "failed";

impl PacerProxy {
    /// Whether this GET is a warm (ADR-0048).
    ///
    /// # Errors
    ///
    /// `InvalidArgument` for a [`WARM_HEADER`] whose value is not [`WARM_REQUESTED`]: a
    /// caller that asked for something must not silently get a full body instead.
    pub(super) fn requested_warm(headers: &hyper::HeaderMap) -> S3Result<bool> {
        let Some(value) = headers.get(WARM_HEADER) else {
            return Ok(false);
        };
        if value.as_bytes() == WARM_REQUESTED.as_bytes() {
            return Ok(true);
        }
        Err(s3_error!(
            InvalidArgument,
            "x-pacer-warm accepts only the value 1"
        ))
    }

    /// The answer to a warm that read nothing, because `reason` says the cache would
    /// never hold it.
    pub(super) fn warm_skipped(&self, reason: SkipReason) -> S3Response<dto::GetObjectOutput> {
        self.metrics
            .warm
            .requests
            .with_label_values(&[OUTCOME_SKIPPED])
            .inc();
        let mut resp = S3Response::new(dto::GetObjectOutput {
            content_length: Some(0),
            ..Default::default()
        });
        insert_warmed(&mut resp, 0);
        resp.headers.insert(
            hyper::header::HeaderName::from_static(WARM_SKIPPED_HEADER),
            hyper::header::HeaderValue::from_static(reason.as_str()),
        );
        resp
    }

    /// Node mode's bypass: `inner`'s passthrough for a read, the skip answer for a warm —
    /// a warm of something the cache never holds must not stream the object through
    /// instead, which is what the passthrough would do.
    ///
    /// # Errors
    ///
    /// `inner`'s, for a read.
    pub(super) async fn bypass_get(
        &self,
        req: S3Request<dto::GetObjectInput>,
        warm: bool,
        reason: SkipReason,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        if warm {
            return Ok(self.warm_skipped(reason));
        }
        self.inner.get_object(req).await
    }

    /// `auth.mode: requester`'s bypass: [`Self::pass_through`] for a read, the skip answer
    /// for a warm — see [`Self::bypass_get`].
    ///
    /// # Errors
    ///
    /// The pass-through's, for a read.
    pub(super) async fn pass_through_or_skip(
        &self,
        held: &crate::authz::HeldRequest,
        forwarder: &crate::authz::Forwarder,
        warm: bool,
        reason: SkipReason,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        if warm {
            return Ok(self.warm_skipped(reason));
        }
        self.pass_through(held, forwarder).await
    }

    /// Why a probed object the cache will not serve was not warmed. The command never
    /// sends `If-Match`, so for a warm the size band is the reason that matters; the
    /// conditional is folded into [`SkipReason::Uncacheable`].
    pub(super) fn requester_skip_reason(&self, header: &ObjectHeader) -> SkipReason {
        if should_admit(
            Some(header.object_len),
            self.min_object_size,
            self.max_object_size,
        ) {
            SkipReason::Uncacheable
        } else {
            SkipReason::ObjectSize
        }
    }

    /// Resolve every chunk covering `[start, end)` and answer header-only.
    ///
    /// # Errors
    ///
    /// The first chunk that could not be resolved, as the read path reports it — the one
    /// place a warm is *better* than a body, which has already sent its `200` by then.
    pub(super) async fn warm_range(
        &self,
        ctx: FillCtx,
        header: &ObjectHeader,
        start: u64,
        end: u64,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let ctx = Arc::new(FillCtx { warm: true, ..ctx });
        let covering = ctx.chunk.covering(start, end);
        let resolved = futures::stream::iter(covering)
            .map(|idx| {
                let ctx = Arc::clone(&ctx);
                async move { ctx.resolve_chunk(idx).await.map(drop) }
            })
            .buffer_unordered(self.fill_parallelism.max(1))
            .try_collect::<()>()
            .await;
        let outcome = if resolved.is_ok() {
            OUTCOME_WARMED
        } else {
            OUTCOME_FAILED
        };
        self.metrics
            .warm
            .requests
            .with_label_values(&[outcome])
            .inc();
        resolved?;
        let warmed = end - start;
        self.metrics.warm.bytes.inc_by(warmed);
        let mut resp = S3Response::new(dto::GetObjectOutput {
            content_length: Some(0),
            e_tag: header.e_tag.clone().map(ETag::Strong),
            ..Default::default()
        });
        insert_warmed(&mut resp, warmed);
        Ok(resp)
    }
}

/// Set [`WARMED_HEADER`] — present on every warm answer, which is what tells a caller the
/// daemon understood the request rather than streaming a body past it.
fn insert_warmed(resp: &mut S3Response<dto::GetObjectOutput>, bytes: u64) {
    resp.headers.insert(
        hyper::header::HeaderName::from_static(WARMED_HEADER),
        hyper::header::HeaderValue::from(bytes),
    );
}
