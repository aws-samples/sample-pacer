//! The GET parameters a cached object cannot honour, each shown to reach the backend.
//!
//! `x-amz-expected-bucket-owner`, `x-amz-request-payer` and the six `response-*`
//! overrides all ask S3 to do something on the way out — check who owns the bucket,
//! record who pays, rewrite a header. A response rebuilt from the cache does none of
//! that, so before this module's subject landed a warm object answered 200 and the
//! parameter was dropped without an error. `cacheable_shape` now bypasses on each.
//!
//! # Why the assertion is made at the backend
//!
//! `pacer_cache_bypass_total` going up says the daemon *chose* to pass through; it does
//! not say the parameter survived the trip. The owner check is only a check if S3 sees
//! it, so every arm also asserts that the backend received exactly one GET and that it
//! carried the parameter.
//!
//! # Why the object is warmed first, and why there is a control
//!
//! A cold object would reach the backend anyway, so "it reached the backend" would pass
//! without the fix. Each arm therefore warms the object, and
//! `a_plain_get_of_the_warm_object_never_reaches_the_backend` shows the same read
//! *without* the parameter is served entirely from cache — the arm that proves the
//! fixture can tell the two apart.
//!
//! client (aws-sdk-s3, placeholder creds)
//!   → S3Service[auth = PlaceholderAuth, s3 = PacerProxy]
//!     → aws-sdk-s3 (daemon identity)
//!       → S3Service[s3 = RecordingFs → s3s_fs::FileSystem]

use std::sync::{Arc, Mutex};

use aws_sdk_s3::operation::get_object::builders::GetObjectFluentBuilder;
use aws_sdk_s3::primitives::{ByteStream, DateTime};
use aws_sdk_s3::types::RequestPayer;
use bytes::Bytes;
use pacer_daemon::metrics::Metrics;
use rstest::rstest;
use s3s::dto;
use s3s::{S3Request, S3Response, S3Result};

use crate::common::{
    backend_service, create_test_bucket, daemon_core_over, seeded_body, wait_for_fills,
    BackendPair, Daemon, DaemonSpec, BUCKET,
};

/// Objects at or below this are proxied uncached, so the fixture must exceed it.
const MIN_OBJECT_SIZE: u64 = 4 << 20;
/// Small enough that the fixture spans several chunks, so warming it is a real fill.
const CHUNK_SIZE: u64 = 1 << 20;
/// Above [`MIN_OBJECT_SIZE`], so the object takes the cached path at all.
const OBJECT_SIZE: usize = (MIN_OBJECT_SIZE as usize) + (1 << 20);
/// Chunks the fixture spans; the warm-up waits for this many fills.
const OBJECT_CHUNKS: u64 = (OBJECT_SIZE as u64).div_ceil(CHUNK_SIZE);
/// Key under test.
const KEY: &str = "weights.safetensors";
/// AWS's documentation placeholder account ID. `s3s-fs` does not check ownership, so any value
/// proves forwarding; what S3 would do with it is S3's business, which is the point.
const OWNER: &str = "123456789012";

/// The parameters one backend GET carried, by the name the arms below use.
type Seen = Vec<&'static str>;
/// A GET request being built, so a case can name its closure's argument type briefly.
type Builder = GetObjectFluentBuilder;
/// What a case does to the request: add the one parameter under test, or nothing.
type Shape = fn(Builder) -> Builder;

/// An `s3s-fs` backend that records, for every GET, which of the parameters under test
/// it carried. Everything else is delegated untouched.
struct RecordingFs {
    inner: s3s_fs::FileSystem,
    gets: Arc<Mutex<Vec<Seen>>>,
}

/// Which of the parameters under test a GET carried.
fn carried(input: &dto::GetObjectInput) -> Seen {
    [
        (
            "expected_bucket_owner",
            input.expected_bucket_owner.is_some(),
        ),
        ("request_payer", input.request_payer.is_some()),
        (
            "response_cache_control",
            input.response_cache_control.is_some(),
        ),
        (
            "response_content_disposition",
            input.response_content_disposition.is_some(),
        ),
        (
            "response_content_encoding",
            input.response_content_encoding.is_some(),
        ),
        (
            "response_content_language",
            input.response_content_language.is_some(),
        ),
        (
            "response_content_type",
            input.response_content_type.is_some(),
        ),
        ("response_expires", input.response_expires.is_some()),
    ]
    .into_iter()
    .filter_map(|(name, present)| present.then_some(name))
    .collect()
}

#[async_trait::async_trait]
impl s3s::S3 for RecordingFs {
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
        self.gets.lock().unwrap().push(carried(&req.input));
        self.inner.get_object(req).await
    }
}

/// The daemon over a recording backend, with the fixture object already warm.
struct Harness {
    /// Held whole so the cache and backend directories outlive the test.
    daemon: Daemon,
    metrics: Metrics,
    gets: Arc<Mutex<Vec<Seen>>>,
    body: Bytes,
}

impl Harness {
    /// Backend GETs recorded so far.
    fn backend_gets(&self) -> Vec<Seen> {
        self.gets.lock().unwrap().clone()
    }

    /// Read the fixture through the daemon with `shape` applied, asserting the bytes.
    async fn read(&self, shape: Shape) {
        let got = shape(self.daemon.client.get_object().bucket(BUCKET).key(KEY))
            .send()
            .await
            .expect("s3s-fs accepts every parameter under test");
        assert_eq!(got.body.collect().await.unwrap().into_bytes(), self.body);
    }
}

/// Assemble the daemon, seed the fixture, and warm every one of its chunks.
async fn warm_harness() -> Harness {
    let backend_dir = tempfile::tempdir().unwrap();
    let gets = Arc::new(Mutex::new(Vec::new()));
    let (service, creds) = backend_service(RecordingFs {
        inner: s3s_fs::FileSystem::new(backend_dir.path()).unwrap(),
        gets: Arc::clone(&gets),
    });
    let pair = BackendPair::over(&service, &creds);
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
    core.dirs.push(backend_dir);
    let metrics = core.metrics.clone();
    let h = Harness {
        daemon: core.in_process(),
        metrics,
        gets,
        body: seeded_body(0x3c, OBJECT_SIZE),
    };

    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from(h.body.clone()))
        .send()
        .await
        .unwrap();
    h.read(|b| b).await;
    wait_for_fills(&h.metrics, OBJECT_CHUNKS).await;
    h
}

#[tokio::test]
async fn a_plain_get_of_the_warm_object_never_reaches_the_backend() {
    let h = warm_harness().await;
    let (hits, gets) = (h.metrics.cache_hits.get(), h.backend_gets().len());

    h.read(|b| b).await;

    assert!(
        h.metrics.cache_hits.get() > hits,
        "the warm object should be a hit"
    );
    assert_eq!(
        h.backend_gets().len(),
        gets,
        "a hit must not GET from the backend — or the arms below prove nothing"
    );
}

#[rstest]
#[case::expected_bucket_owner(
    "expected_bucket_owner",
    |b: Builder| b.expected_bucket_owner(OWNER)
)]
#[case::request_payer(
    "request_payer",
    |b: Builder| b.request_payer(RequestPayer::Requester)
)]
#[case::response_cache_control(
    "response_cache_control",
    |b: Builder| b.response_cache_control("no-store")
)]
#[case::response_content_disposition(
    "response_content_disposition",
    |b: Builder| b.response_content_disposition("attachment")
)]
#[case::response_content_encoding(
    "response_content_encoding",
    |b: Builder| b.response_content_encoding("identity")
)]
#[case::response_content_language(
    "response_content_language",
    |b: Builder| b.response_content_language("en")
)]
#[case::response_content_type(
    "response_content_type",
    |b: Builder| b.response_content_type("text/plain")
)]
#[case::response_expires(
    "response_expires",
    |b: Builder| b.response_expires(DateTime::from_secs(0))
)]
#[tokio::test]
async fn a_parameter_the_cache_cannot_honour_reaches_the_backend(
    #[case] parameter: &'static str,
    #[case] shape: Shape,
) {
    let h = warm_harness().await;
    let (hits, bypass) = (h.metrics.cache_hits.get(), h.metrics.cache_bypass.get());
    let before = h.backend_gets().len();

    h.read(shape).await;

    assert_eq!(
        h.metrics.cache_hits.get(),
        hits,
        "{parameter}: a warm object must not be served from cache"
    );
    assert_eq!(
        h.metrics.cache_bypass.get(),
        bypass + 1,
        "{parameter}: counted as the bypass it is"
    );
    assert_eq!(
        h.backend_gets()[before..],
        [vec![parameter]],
        "{parameter}: exactly one backend GET, carrying the parameter"
    );
}
