//! What framing the daemon's passthrough PUT (ADR-0007) sends to the backend, as a
//! function of what the client sent it.
//!
//! # Why this file exists
//!
//! On 2026-08-25 the first hardware run of the write path found an asymmetry nobody
//! could explain from the logs: **every scatter-off PUT from the Rust seeder failed
//! against S3 Standard with a bare `service error`, while `aws s3api put-object`
//! through the same daemon in the same configuration succeeded**
//! (`bench/ladder/results/w1-write-scatter.md`). That blocked the no-regression
//! ratios of gates 4.2 and 4.4, because there was no scatter-off baseline to compare
//! against.
//!
//! The write-up's leading hypothesis was that the two CLIENTS framed their bodies
//! differently — a current AWS SDK using `aws-chunked` with a trailing CRC32 versus
//! awscli's header checksum. **That is refuted by the SDK's own source**: the seeder
//! passes `ByteStream::from(Bytes)`, an in-memory body, and
//! `aws-sdk-s3`'s request-checksum interceptor only takes the trailer path when
//! `body().bytes()` is `None`. Both clients send a header, so client framing cannot
//! be the difference.
//!
//! What IS different is downstream of that, and it is the daemon's own doing. The
//! forwarding hop wraps the client's body with `SdkBody::from_body_1_x`, whose
//! `bytes_contents` is `None` — so from the daemon's SDK the body is always
//! *streaming*. Whether that streaming body gets `aws-chunked` framing then depends
//! entirely on whether the client happened to supply a checksum header:
//!
//! | client sent | daemon's SDK | framing the backend sees |
//! |---|---|---|
//! | `x-amz-checksum-crc32` (any current SDK) | short-circuits: user set a checksum | the client's header, verbatim, **no** `aws-chunked` |
//! | nothing (awscli 2.15) | computes its own over a streaming body | `aws-chunked` + `x-amz-trailer` |
//!
//! So a client's checksum header silently flips the framing of the request the daemon
//! makes. These tests pin both arms of that table, so the next change to the write path
//! cannot move it without saying so.
//!
//! # What this file does NOT prove — and the failure it does NOT explain
//!
//! Only what the daemon *sends*. `s3s-fs` accepts both shapes, so neither arm here
//! reproduces any S3 rejection.
//!
//! **These tests were written believing the forwarded-header shape was what S3 rejected.
//! It is not.** Hardware on 2026-08-26 settled the open defect: it was S3's 5 GiB limit
//! on a single `PutObject` (`EntityTooLarge`), which the scatter-off passthrough proxies
//! straight through. A scatter-off ladder writes 16 MiB through 5 GiB cleanly and fails
//! at 6 GiB, and awscli's `--checksum-crc32 <value>` — a request header, the seeder's
//! exact shape — **succeeds** at 16 MiB. See
//! `bench/ladder/results/w1-write-scatter.md`.
//!
//! The table above is therefore a true description of the daemon's behaviour and a
//! useful regression guard, and nothing more. Do not cite it as a defect.

use std::sync::{Arc, Mutex};

use aws_sdk_s3::config::{ConfigBag, RequestChecksumCalculation};
use aws_sdk_s3::primitives::ByteStream;
use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::context::BeforeTransmitInterceptorContextRef;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use bytes::Bytes;
use pacer_backend::BackendType;

use crate::common::{
    create_test_bucket, daemon_core_over, fs_backend_service, sdk_client_for,
    sdk_client_intercepting, seeded_body, BackendPair, Daemon, DaemonSpec, BUCKET,
};

/// Cache admission floor. Irrelevant to framing — a PUT is proxied either way — but
/// it has to be set, and mirroring `correctness.rs` keeps the harnesses comparable.
const MIN_OBJECT_SIZE: u64 = 4 << 20;
/// Cache admission ceiling, as in `correctness.rs`.
const MAX_OBJECT_SIZE: Option<u64> = Some(64 << 20);
/// Chunk size, as in `correctness.rs`.
const CHUNK_SIZE: u64 = 1 << 20;
/// Object size for the framing arms.
///
/// Deliberately **small**. The hardware run reached for 4 GiB because it believed
/// S3's 5 GiB single-PUT ceiling was involved; that reading is retracted (the 4 GiB
/// control failed too), and framing is set by the request builder with no reference
/// to length. A few chunks is enough to be a realistic multi-chunk PUT.
const OBJECT_SIZE: usize = 5 << 20;

/// The `Content-Encoding` value that marks a body the SDK framed as `aws-chunked`.
const AWS_CHUNKED: &str = "aws-chunked";
/// Header naming which trailer carries the checksum, present only on the chunked path.
const TRAILER_HEADER: &str = "x-amz-trailer";
/// The client's whole-object CRC32, present only on the in-memory header path.
const CRC32_HEADER: &str = "x-amz-checksum-crc32";
/// Length of the *decoded* body, which only an `aws-chunked` request carries.
const DECODED_LENGTH_HEADER: &str = "x-amz-decoded-content-length";

/// Every request the daemon's own SDK client sent to the backend, captured after
/// signing and after the checksum interceptor has framed the body.
///
/// `read_before_transmit` is the hook that sees the final wire shape: it runs after
/// `modify_before_transmit`, which is where `aws-chunked` wrapping happens. Reading
/// the headers any earlier would show the intent rather than the result.
/// One recorded request's headers, as `(name, value)` in wire order.
type RecordedHeaders = Vec<(String, String)>;

#[derive(Clone, Debug, Default)]
struct SentRequests(Arc<Mutex<Vec<RecordedHeaders>>>);

impl SentRequests {
    /// Header value from the most recent request, case-insensitively.
    ///
    /// # Panics
    /// If no request was recorded — a test asserting on framing that never reached
    /// the backend is a test asserting on nothing, so this fails loudly.
    fn last_header(&self, name: &str) -> Option<String> {
        let sent = self.0.lock().unwrap();
        let last = sent.last().expect("no request reached the backend");
        last.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.clone())
    }
}

impl Intercept for SentRequests {
    fn name(&self) -> &'static str {
        "SentRequests"
    }

    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _components: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let headers = context
            .request()
            .headers()
            .iter()
            .map(|(k, v)| (k.to_owned(), v.to_owned()))
            .collect();
        self.0.lock().unwrap().push(headers);
        Ok(())
    }
}

/// The daemon stack over `s3s-fs`, with a recorder on the daemon's BACKEND client.
struct Harness {
    /// The daemon, reached in process. PUTs to its client take ADR-0007's
    /// passthrough, because no cluster is configured and so the scatter is off —
    /// which is the path the hardware run could not get a baseline out of. Held
    /// whole so its cache and backend directories outlive the test.
    daemon: Daemon,
    /// What the daemon forwarded to the backend.
    sent: SentRequests,
}

/// Assemble [`Harness`].
///
/// The recorder has to go on the daemon's *own* backend client, which is why this
/// builds the [`BackendPair`] by hand rather than letting
/// [`common::daemon_core`] do it: the whole subject is the request the daemon
/// produces, and only an interceptor on that client sees it.
///
/// [`common::daemon_core`]: crate::common::daemon_core
async fn harness() -> Harness {
    let backend_dir = tempfile::tempdir().unwrap();
    let (service, creds) = fs_backend_service(backend_dir.path());
    let sent = SentRequests::default();
    let pair = BackendPair {
        daemon: sdk_client_intercepting(service.clone(), creds.clone(), sent.clone()),
        truth: sdk_client_for(service, creds),
    };
    create_test_bucket(&pair.truth).await;

    let mut core = daemon_core_over(
        DaemonSpec {
            // Standard, because that is the backend the write path runs on: ADR-0032 § 6
            // scopes the scatter to general-purpose buckets, and it is where the open
            // defect was seen. It also keeps `Content-MD5` forwarded rather than stripped.
            backend_type: BackendType::Standard,
            min_object_size: MIN_OBJECT_SIZE,
            max_object_size: MAX_OBJECT_SIZE,
            chunk_size: CHUNK_SIZE,
            ..DaemonSpec::default()
        },
        pair,
    )
    .await;
    core.dirs.push(backend_dir);

    Harness {
        daemon: core.in_process(),
        sent,
    }
}

/// A position-dependent body, so nothing here can pass on all-zero bytes.
fn body(len: usize) -> Bytes {
    seeded_body(0, len)
}

#[tokio::test]
async fn a_clients_checksum_header_is_forwarded_verbatim_and_suppresses_aws_chunked() {
    // The Rust seeder's exact shape: an in-memory body, so the client's SDK attaches
    // its default CRC32 as a HEADER (aws-sdk-s3 `http_request_checksum.rs` takes the
    // trailer branch only when `body().bytes()` is None).
    let h = harness().await;
    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key("client-checksum.bin")
        .body(ByteStream::from(body(OBJECT_SIZE)))
        .send()
        .await
        .unwrap();

    // The daemon forwarded the client's own digest rather than computing one. This is
    // the short-circuit in the SDK's `modify_before_retry_loop`: a user-set checksum
    // header disables aws-chunked outright.
    assert!(
        h.sent.last_header(CRC32_HEADER).is_some(),
        "the daemon should forward the client's whole-object CRC32"
    );
    assert_ne!(
        h.sent.last_header("content-encoding").as_deref(),
        Some(AWS_CHUNKED),
        "a client checksum header must suppress aws-chunked framing"
    );
    assert_eq!(
        h.sent.last_header(TRAILER_HEADER),
        None,
        "no trailer is negotiated when the checksum is already a header"
    );
    assert_eq!(
        h.sent.last_header(DECODED_LENGTH_HEADER),
        None,
        "a non-chunked body has one length, not a decoded one"
    );
}

#[tokio::test]
async fn a_client_that_sends_no_checksum_makes_the_daemon_frame_the_body_as_aws_chunked() {
    // awscli 2.15's shape — the client that SUCCEEDED on hardware. It predates
    // default request checksums, so it sends none, and the daemon's own SDK then
    // computes one. Because the forwarded body is streaming
    // (`SdkBody::from_body_1_x` has no `bytes_contents`), that checksum can only
    // travel as a trailer, which drags aws-chunked framing in with it.
    let h = harness().await;
    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key("no-client-checksum.bin")
        .body(ByteStream::from(body(OBJECT_SIZE)))
        // Suppressing the checksum on the CLIENT is what makes it awscli-2.15-like;
        // the daemon's own client is untouched and still defaults to `WhenSupported`.
        .customize()
        .config_override(
            aws_sdk_s3::Config::builder()
                .request_checksum_calculation(RequestChecksumCalculation::WhenRequired),
        )
        .send()
        .await
        .unwrap();

    assert_eq!(
        h.sent.last_header("content-encoding").as_deref(),
        Some(AWS_CHUNKED),
        "with no client checksum the daemon's SDK must frame the stream as aws-chunked"
    );
    assert_eq!(
        h.sent.last_header(TRAILER_HEADER).as_deref(),
        Some(CRC32_HEADER),
        "and carry its own CRC32 as a trailer"
    );
    assert!(
        h.sent.last_header(DECODED_LENGTH_HEADER).is_some(),
        "an aws-chunked request must declare the decoded length"
    );
}

#[tokio::test]
async fn the_two_client_shapes_produce_different_framing() {
    // The asymmetry itself, in one assertion: the same bytes, through the same
    // daemon, on the same path, reach the backend framed two different ways
    // depending only on whether the client attached a checksum. That is the whole
    // finding, and it is the thing a future change must not silently alter.
    let h = harness().await;
    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key("with.bin")
        .body(ByteStream::from(body(OBJECT_SIZE)))
        .send()
        .await
        .unwrap();
    let with_checksum = h.sent.last_header("content-encoding");

    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key("without.bin")
        .body(ByteStream::from(body(OBJECT_SIZE)))
        .customize()
        .config_override(
            aws_sdk_s3::Config::builder()
                .request_checksum_calculation(RequestChecksumCalculation::WhenRequired),
        )
        .send()
        .await
        .unwrap();
    let without_checksum = h.sent.last_header("content-encoding");

    assert_ne!(
        with_checksum, without_checksum,
        "a client checksum header changes the framing the daemon forwards"
    );
}
