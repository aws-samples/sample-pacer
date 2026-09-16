//! Real-S3 arm of the Express + Standard backend matrix (workstream D2, ADR-0023).
//!
//! The in-process `correctness.rs` suite runs every backend-*agnostic* behavior
//! against BOTH backends over an `s3s-fs` filesystem. But the two backends
//! *diverge* precisely where a local filesystem cannot follow:
//!
//!   - **Sparse multipart completion.** S3 Standard accepts ascending-with-gaps
//!     part numbers (e.g. 1, 3, 5); Express (and `s3s-fs`) require consecutive
//!     parts from 1. The daemon relaxes its own `part_numbers_ok` gate for
//!     Standard, but proving the *real* bucket then accepts the completion needs
//!     a real bucket.
//!   - **`Content-MD5` pass-through.** Express directory buckets reject
//!     `Content-MD5`, so the daemon strips it; Standard forwards it, so S3
//!     validates it. `s3s-fs` ignores the header entirely and cannot exercise
//!     either behavior.
//!
//! These tests therefore talk to a REAL bucket using the ambient AWS identity,
//! through the actual daemon stack (`pacer_backend::build_client` →
//! [`PacerProxy`] → placeholder-auth front, driven by an in-process `aws-sdk-s3`
//! client — the same shape as `correctness.rs`, only the backend is real S3).
//!
//! # Honest gating — never a false pass
//!
//! Every test is `#[ignore]`d, so a plain `cargo test` reports them as
//! **ignored**, not passed. When explicitly run (`cargo test -- --ignored`),
//! the *first* thing each test does is [`require_env`] the target bucket: if it
//! is unset the test **panics** (fails loudly) rather than returning. There is
//! no code path on which one of these tests reports success without having
//! actually round-tripped against a real bucket.
//!
//! ## Running
//!
//! ```text
//! # S3 Standard behaviors (regional bucket, plain SigV4):
//! export AWS_REGION=us-east-2                # + AWS credentials for the daemon identity
//! export PACER_TEST_S3_STANDARD_BUCKET=my-standard-bucket
//! cargo test -p pacer-daemon --test backend_matrix_s3 -- --ignored standard_
//!
//! # S3 Express behavior (directory bucket, zonal endpoint):
//! export PACER_TEST_S3_EXPRESS_BUCKET=my-bucket--use2-az1--x-s3
//! export PACER_TEST_S3_EXPRESS_ENDPOINT=https://s3express-use2-az1.us-east-2.amazonaws.com
//! cargo test -p pacer-daemon --test backend_matrix_s3 -- --ignored express_
//! ```

// Each test is one linear real-S3 scenario; splitting to satisfy a line count
// would hurt readability (mirrors correctness.rs).
#![allow(clippy::too_many_lines)]

use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use bytes::Bytes;
use pacer_backend::{BackendConfig, BackendType};

use crate::common::{daemon_core_over, seeded_body, BackendPair, Daemon, DaemonSpec};

/// Regional S3 Standard bucket the daemon identity may read/write. Required by
/// the `standard_*` tests; absent → they panic (see module doc).
const ENV_STANDARD_BUCKET: &str = "PACER_TEST_S3_STANDARD_BUCKET";
/// S3 Express One Zone directory bucket (`*--x-s3`). Required by the
/// `express_*` test.
const ENV_EXPRESS_BUCKET: &str = "PACER_TEST_S3_EXPRESS_BUCKET";
/// Zonal endpoint for [`ENV_EXPRESS_BUCKET`]'s AZ — Express addressing needs it
/// explicitly (there is no default regional resolution for a directory bucket).
const ENV_EXPRESS_ENDPOINT: &str = "PACER_TEST_S3_EXPRESS_ENDPOINT";

/// A syntactically valid but deliberately WRONG `Content-MD5` (base64 of 16
/// zero bytes). It cannot match any real body, so S3 rejects it *iff* the
/// daemon forwarded it — the discriminator between Standard (forwards → reject)
/// and Express (strips → accept).
const WRONG_CONTENT_MD5: &str = "AAAAAAAAAAAAAAAAAAAAAA==";

/// S3's minimum multipart part size (every part but the last must meet it).
const S3_MIN_PART_SIZE: usize = 5 << 20;

/// Daemon stack fronting a REAL S3 backend, plus a direct handle to that same
/// backend for cleanup.
///
/// The size gates are [`DaemonSpec::default`]'s, which are the same numbers
/// `correctness.rs` asserts against — this arm is that file's scenarios against a real
/// bucket, so the two must not diverge silently.
type RealHarness = Daemon;

/// Fetch a required env var or fail loudly. Panicking here is the whole point:
/// an `--ignored` run without a configured bucket must FAIL, never silently
/// pass (see module doc).
///
/// # Panics
///
/// If `name` is unset or empty.
fn require_env(name: &str) -> String {
    match std::env::var(name) {
        Ok(v) if !v.trim().is_empty() => v,
        _ => panic!(
            "{name} is not set: this real-S3 test was run (via `--ignored`) without a target \
             bucket. It round-trips against a REAL S3 bucket using ambient AWS credentials, and \
             refuses to report success without one. Export {name} (plus AWS credentials and \
             AWS_REGION) and re-run — see the module docs."
        ),
    }
}

/// Deterministic pseudo-random body (the same one `correctness.rs` uses).
fn big_body(seed: u8, len: usize) -> Bytes {
    seeded_body(seed, len)
}

/// A collision-free key under a dedicated prefix, so concurrent/repeated runs
/// never clash and cleanup is unambiguous.
fn unique_key(prefix: &str) -> String {
    format!("pacer-d2-matrix/{prefix}-{}", uuid::Uuid::new_v4())
}

/// Assemble the daemon over a real S3 backend of the given shape. `endpoint` is
/// the zonal Express endpoint for [`BackendType::Express`], `None` for Standard
/// (default regional resolution from `AWS_REGION`).
///
/// The backend client is built by the SAME production code path the daemon uses (this
/// is what gates `CreateSession`/session-auth on shape), and it is handed to
/// [`common::daemon_core_over`] as *both* halves of the [`BackendPair`]: the "oracle"
/// half is only ever used to delete the test objects afterwards, and against a real
/// bucket there is nothing to gain from a second client. `daemon_core_over` is also the
/// entry point that does **not** `CreateBucket`, which is the whole reason it exists —
/// this arm's bucket is real and pre-existing.
///
/// [`common::daemon_core_over`]: crate::common::daemon_core_over
async fn daemon_over_real_backend(
    backend_type: BackendType,
    endpoint: Option<String>,
) -> RealHarness {
    let backend = pacer_backend::build_client(&BackendConfig {
        backend_type,
        endpoint,
        force_path_style: false,
    })
    .await;
    daemon_core_over(
        DaemonSpec {
            backend_type,
            ..DaemonSpec::default()
        },
        BackendPair {
            daemon: backend.clone(),
            truth: backend,
        },
    )
    .await
    .in_process()
}

#[tokio::test]
#[ignore = "hits a real S3 Standard bucket; set PACER_TEST_S3_STANDARD_BUCKET (+ AWS creds/region) and run with --ignored"]
async fn standard_sparse_multipart_completion_end_to_end() {
    let bucket = require_env(ENV_STANDARD_BUCKET);
    let h = daemon_over_real_backend(BackendType::Standard, None).await;
    let key = unique_key("sparse-mpu");

    // Ascending-WITH-GAPS part numbers: rejected by Express (and s3s-fs),
    // accepted by Standard. All parts meet S3's minimum size.
    let sparse_parts = [1_i32, 3, 5];
    let mpu = h
        .client
        .create_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    let upload_id = mpu.upload_id().unwrap().to_string();

    let mut completed = Vec::new();
    let mut expected = Vec::new();
    for (i, part_number) in sparse_parts.iter().enumerate() {
        let body = big_body(30 + i as u8, S3_MIN_PART_SIZE);
        expected.extend_from_slice(&body);
        let up = h
            .client
            .upload_part()
            .bucket(&bucket)
            .key(&key)
            .upload_id(&upload_id)
            .part_number(*part_number)
            .body(ByteStream::from(body))
            .send()
            .await
            .unwrap();
        completed.push(
            CompletedPart::builder()
                .part_number(*part_number)
                .e_tag(up.e_tag().unwrap())
                .build(),
        );
    }

    // The daemon must NOT reject sparse parts on Standard, and the real bucket
    // must then accept the completion.
    h.client
        .complete_multipart_upload()
        .bucket(&bucket)
        .key(&key)
        .upload_id(&upload_id)
        .multipart_upload(
            CompletedMultipartUpload::builder()
                .set_parts(Some(completed))
                .build(),
        )
        .send()
        .await
        .expect("Standard sparse-part multipart completion must succeed end-to-end");

    // Read back through the daemon: parts concatenated in part-number order.
    let got = h
        .client
        .get_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await
        .unwrap();
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.len(), expected.len());
    assert_eq!(&bytes[..], &expected[..]);

    let _ = h
        .backend
        .delete_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await;
}

#[tokio::test]
#[ignore = "hits a real S3 Standard bucket; set PACER_TEST_S3_STANDARD_BUCKET (+ AWS creds/region) and run with --ignored"]
async fn standard_content_md5_is_forwarded_not_stripped() {
    let bucket = require_env(ENV_STANDARD_BUCKET);
    let h = daemon_over_real_backend(BackendType::Standard, None).await;
    let key = unique_key("md5-forward");

    // Standard forwards Content-MD5, so a wrong digest reaches S3 and is
    // rejected — proving the daemon did NOT strip it. (Express strips it; see
    // the express_ test for the other side of the divergence.)
    let err = h
        .client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .content_md5(WRONG_CONTENT_MD5)
        .body(ByteStream::from_static(b"content-md5 forwarding probe"))
        .send()
        .await
        .expect_err("a wrong Content-MD5 must be forwarded to Standard and rejected");

    let code = err.into_service_error().meta().code().map(str::to_owned);
    assert!(
        matches!(code.as_deref(), Some("BadDigest") | Some("InvalidDigest")),
        "expected BadDigest/InvalidDigest from the forwarded Content-MD5, got {code:?}"
    );

    // Belt-and-suspenders cleanup in case an SDK retry ever landed the object.
    let _ = h
        .backend
        .delete_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await;
}

#[tokio::test]
#[ignore = "hits a real S3 Express directory bucket; set PACER_TEST_S3_EXPRESS_BUCKET + PACER_TEST_S3_EXPRESS_ENDPOINT (+ AWS creds/region) and run with --ignored"]
async fn express_content_md5_is_stripped_so_wrong_digest_is_accepted() {
    let bucket = require_env(ENV_EXPRESS_BUCKET);
    let endpoint = require_env(ENV_EXPRESS_ENDPOINT);
    let h = daemon_over_real_backend(BackendType::Express, Some(endpoint)).await;
    let key = unique_key("md5-strip");

    // Express directory buckets reject Content-MD5 outright. The daemon strips
    // it, so the PUT succeeds DESPITE the (wrong) header the client supplied.
    // Without the strip this same call would fail — that is the proof.
    h.client
        .put_object()
        .bucket(&bucket)
        .key(&key)
        .content_md5(WRONG_CONTENT_MD5)
        .body(ByteStream::from_static(b"content-md5 strip probe"))
        .send()
        .await
        .expect("Express must strip Content-MD5 so the PUT is accepted");

    let _ = h
        .backend
        .delete_object()
        .bucket(&bucket)
        .key(&key)
        .send()
        .await;
}
