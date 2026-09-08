//! The backend read retry (`pacer_backend::retry`), as a client experiences it.
//!
//! The defect these tests pin down: a chunked read (ADR-0015) turns one client
//! GET into N ranged backend GETs, and a single transient failure on any one of
//! them used to end the whole client response. The client cannot even see that
//! as an error in the usual sense — the `200` and the `Content-Length` were sent
//! before the failing chunk was requested — so the failure mode is a *truncated
//! body*, which is the one shape a caller may not notice.
//!
//! The fault is injected where it actually happened: the response headers are
//! accepted normally and the **body stream** dies partway. That is precisely the
//! failure `aws-sdk-s3`'s own retry does not cover, so a test that faults the
//! request instead would pass with no retry code at all.
//!
//! The stack is the same in-process assembly `correctness.rs` uses, with one
//! wrapper added around the backend:
//!
//! client (aws-sdk-s3, placeholder creds)
//!   → S3Service[auth = PlaceholderAuth, s3 = PacerProxy]
//!     → aws-sdk-s3 (daemon identity)
//!       → S3Service[s3 = FlakyFs → s3s_fs::FileSystem]

use std::sync::atomic::{AtomicUsize, Ordering};

use aws_sdk_s3::primitives::ByteStream;
use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use pacer_backend::retry::RetryPolicy;
use pacer_daemon::metrics::Metrics;
use s3s::dto::{self, StreamingBlob};
use s3s::{S3Request, S3Response, S3Result};

use crate::common::{
    backend_service, create_test_bucket, daemon_core_over, seeded_body, BackendPair, Daemon,
    DaemonSpec, BUCKET,
};

/// Objects at or below this are proxied uncached, so the fixture must exceed it.
const MIN_OBJECT_SIZE: u64 = 4 << 20;
/// Small enough that the fixture spans several chunks — one transient fault must
/// break exactly one of them, which is the whole point of the scenario.
const CHUNK_SIZE: u64 = 1 << 20;
/// The `outcome` label of `pacer_backend_read_failures_total` for a read that
/// spent every attempt on a retryable fault.
const OUTCOME_EXHAUSTED: &str = "exhausted";

struct Harness {
    /// The daemon, reached in process. Held whole so its cache and backend
    /// directories outlive the test.
    daemon: Daemon,
    metrics: Metrics,
    /// Ranged backend GETs whose body was severed so far.
    faults: std::sync::Arc<Faults>,
}

/// How many more ranged GET bodies to sever, and how many have been severed.
///
/// A budget rather than a flag because the interesting cases are "fewer faults
/// than attempts" (the retry absorbs them) and "more faults than attempts" (it
/// cannot), and the difference between them is just this number.
struct Faults {
    budget: AtomicUsize,
    injected: AtomicUsize,
}

impl Faults {
    fn new(budget: usize) -> Self {
        Self {
            budget: AtomicUsize::new(budget),
            injected: AtomicUsize::new(0),
        }
    }

    /// Claim one fault, if any are left. `fetch_update` rather than a
    /// `load`/`store` pair: `FILL_PARALLELISM` chunk reads race here, and two of
    /// them must not spend the same unit of budget.
    fn take(&self) -> bool {
        let claimed = self
            .budget
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
            .is_ok();
        if claimed {
            self.injected.fetch_add(1, Ordering::SeqCst);
        }
        claimed
    }

    fn injected(&self) -> usize {
        self.injected.load(Ordering::SeqCst)
    }
}

/// An `s3s-fs` backend whose ranged GET bodies can be severed mid-stream.
///
/// Only *ranged* GETs are faulted, because only those are the daemon's chunk
/// reads: a whole-object GET through this backend is the cache-bypass path, and
/// faulting it would test something else.
struct FlakyFs {
    inner: s3s_fs::FileSystem,
    faults: std::sync::Arc<Faults>,
}

/// Everything the response promised, then a severed connection.
///
/// Half the bytes rather than none: a body that fails before its first byte
/// could plausibly be retried by a transport, while one that dies partway is the
/// failure that reached production, and it is also the one that would corrupt a
/// naive "resume from where we stopped" fix.
fn severed(body: &Bytes) -> StreamingBlob {
    let half = body.slice(..body.len() / 2);
    StreamingBlob::wrap(futures::stream::iter(vec![
        Ok(half),
        Err(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "injected S3 stream reset",
        )),
    ]))
}

/// Drain a body the backend produced, so it can be replayed as a severed one.
async fn drain(mut blob: StreamingBlob) -> Bytes {
    let mut buf = BytesMut::new();
    while let Some(chunk) = blob.next().await {
        buf.extend_from_slice(&chunk.expect("the filesystem backend never fails a body"));
    }
    buf.freeze()
}

#[async_trait::async_trait]
impl s3s::S3 for FlakyFs {
    async fn create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.inner.put_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.inner.head_object(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        let ranged = req.input.range.is_some();
        let mut resp = self.inner.get_object(req).await?;
        if !ranged || !self.faults.take() {
            return Ok(resp);
        }
        // `content_length` is left as the backend set it: the response still
        // promises the whole chunk, which is what makes this a truncation rather
        // than an honest short read.
        if let Some(body) = resp.output.body.take() {
            resp.output.body = Some(severed(&drain(body).await));
        }
        Ok(resp)
    }
}

/// Assemble the daemon over a backend that will sever `fault_budget` ranged GET
/// bodies, with `retry` deciding how hard a chunk read tries.
///
/// The one thing this arm changes about the shared bring-up is *which* `S3` sits at
/// the bottom, which is why [`common::daemon_core_over`] takes the backend rather
/// than building it: [`FlakyFs`] wraps `s3s-fs` and the daemon must not be able to
/// tell.
///
/// [`common::daemon_core_over`]: crate::common::daemon_core_over
async fn harness(fault_budget: usize, retry: RetryPolicy) -> Harness {
    let backend_dir = tempfile::tempdir().unwrap();
    let faults = std::sync::Arc::new(Faults::new(fault_budget));
    let (service, creds) = backend_service(FlakyFs {
        inner: s3s_fs::FileSystem::new(backend_dir.path()).unwrap(),
        faults: std::sync::Arc::clone(&faults),
    });
    let pair = BackendPair::over(&service, &creds);
    // Before any fault budget is claimed: `create_bucket` is not a ranged GET, so it
    // could not spend one, but seeding the bucket through the faulty backend is still
    // the first thing that happens and it must succeed.
    create_test_bucket(&pair.truth).await;

    let mut core = daemon_core_over(
        DaemonSpec {
            min_object_size: MIN_OBJECT_SIZE,
            max_object_size: None,
            chunk_size: CHUNK_SIZE,
            ..DaemonSpec::default()
        },
        pair,
    )
    .await;
    core.proxy = core.proxy.with_read_retry(retry);
    core.dirs.push(backend_dir);
    let metrics = core.metrics.clone();

    Harness {
        daemon: core.in_process(),
        metrics,
        faults,
    }
}

/// A multi-chunk fixture with position-dependent bytes, so a body assembled from
/// the wrong offsets fails the comparison instead of matching by luck.
fn fixture() -> Bytes {
    seeded_body(0, (MIN_OBJECT_SIZE + 1024) as usize)
}

async fn put(h: &Harness, key: &str, body: &Bytes) {
    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
}

/// Read `key` through the daemon, returning the bytes the client actually got.
///
/// The body is collected *inside* this helper on purpose: `send()` succeeding
/// proves only that headers arrived, and the whole defect lives after that
/// point. A test that stopped at `send().is_ok()` would pass on a truncated
/// read.
async fn get(h: &Harness, key: &str) -> Result<Bytes, String> {
    let resp = h
        .daemon
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .map_err(|e| format!("send failed: {e}"))?;
    resp.body
        .collect()
        .await
        .map(aws_sdk_s3::primitives::AggregatedBytes::into_bytes)
        .map_err(|e| format!("body failed: {e}"))
}

#[tokio::test]
async fn a_severed_backend_body_is_retried_and_the_client_get_is_whole() {
    // The reported defect, fixed: two of the covering chunks lose their backend
    // body mid-stream, and the client still gets every byte.
    let h = harness(2, RetryPolicy::default()).await;
    let body = fixture();
    put(&h, "flaky.bin", &body).await;

    assert_eq!(get(&h, "flaky.bin").await.unwrap(), body);
    assert_eq!(
        h.faults.injected(),
        2,
        "both faults must have been injected"
    );
    assert_eq!(
        h.metrics.backend_read.retries.get(),
        2,
        "one retry per severed chunk, and not one more"
    );
    assert_eq!(
        h.metrics
            .backend_read
            .failures
            .with_label_values(&[OUTCOME_EXHAUSTED])
            .get(),
        0,
        "a fault the retry absorbed must not be counted as a failure"
    );
}

#[tokio::test]
async fn a_single_severed_body_still_breaks_a_read_that_does_not_retry() {
    // The control arm: same injected fault, one attempt allowed. This is the
    // behaviour that was reported, and it is what proves the fault injection is
    // real rather than the retry being untested.
    let h = harness(1, RetryPolicy::with_max_attempts(1)).await;
    let body = fixture();
    put(&h, "no-retry.bin", &body).await;

    // The read may surface as an error or as a short body; what it must never be
    // is the whole object.
    if let Ok(bytes) = get(&h, "no-retry.bin").await {
        assert_ne!(
            bytes, body,
            "one severed chunk was served as a whole object"
        );
    }
    assert_eq!(h.metrics.backend_read.retries.get(), 0, "none were allowed");
    assert_eq!(
        h.metrics
            .backend_read
            .failures
            .with_label_values(&[OUTCOME_EXHAUSTED])
            .get(),
        1
    );
}

#[tokio::test]
async fn a_backend_that_severs_every_attempt_fails_the_read_rather_than_hanging() {
    // The other end: the backend is not momentarily flaky, it is broken. The
    // retry must bound itself and give up, and the giving up must be visible.
    let h = harness(usize::MAX, RetryPolicy::default()).await;
    let body = fixture();
    put(&h, "broken.bin", &body).await;

    if let Ok(bytes) = get(&h, "broken.bin").await {
        assert_ne!(
            bytes, body,
            "a chunk no attempt could read must not be served as whole bytes"
        );
    }
    let failures = h
        .metrics
        .backend_read
        .failures
        .with_label_values(&[OUTCOME_EXHAUSTED])
        .get();
    assert!(failures >= 1, "the exhausted read must be counted");
    // Every failed read spends its whole budget, so the retries it logged are
    // one fewer than the attempts it was allowed.
    let per_read = u64::from(RetryPolicy::default().max_attempts - 1);
    assert_eq!(h.metrics.backend_read.retries.get(), failures * per_read);
}
