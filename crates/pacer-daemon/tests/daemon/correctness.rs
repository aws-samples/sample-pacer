//! Phase 1 correctness suite. The full daemon stack — placeholder auth,
//! PacerProxy, foyer hybrid cache — is assembled in-process in front of an
//! `s3s-fs` filesystem backend, and exercised through an unmodified
//! `aws-sdk-s3` client (same SigV4 path as boto3/AWS CLI).
//!
//! client (aws-sdk-s3, placeholder creds)
//!   → S3Service[auth = PlaceholderAuth, s3 = PacerProxy]
//!     → aws-sdk-s3 (daemon identity)
//!       → S3Service[s3 = s3s_fs::FileSystem]
//!
//! The stack itself is [`common::DaemonCore::in_process`]; this file is the
//! scenarios. Its old bring-up was the one six of the nine binaries had copied,
//! which is why the shared fixture is shaped after it.

// Tests are linear scenarios; splitting them to satisfy a line count would
// hurt readability (CLAUDE.md: size limits target production code).
#![allow(clippy::too_many_lines)]

use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use pacer_backend::BackendType;
use rstest::rstest;

use crate::common::{self, daemon_core, seeded_body, wait_for_fills, Daemon, DaemonSpec, BUCKET};

/// Cache admission floor: only objects strictly larger are admitted (ADR-0002).
const MIN_OBJECT_SIZE: u64 = 4 << 20;
/// Cache admission ceiling.
const MAX_OBJECT_SIZE: Option<u64> = Some(64 << 20);
/// Small chunk size so a few-MiB object spans several chunks — exercises the
/// covering-set math and the ordered fill pipeline in-process (ADR-0015).
const CHUNK_SIZE: u64 = 1 << 20;

/// The daemon this file asserts against, at `backend_type`.
///
/// The three sizes are restated here rather than taken from [`DaemonSpec::default`]
/// because the assertions below are *arithmetic in them* — `MIN_OBJECT_SIZE + 1024`,
/// `CHUNK_SIZE - 256` — so a test reading one number and a daemon built from another
/// would fail as a wrong byte count with no hint that the two had drifted.
fn spec(backend_type: BackendType) -> DaemonSpec {
    DaemonSpec {
        backend_type,
        min_object_size: MIN_OBJECT_SIZE,
        max_object_size: MAX_OBJECT_SIZE,
        chunk_size: CHUNK_SIZE,
        ..DaemonSpec::default()
    }
}

/// Assemble the daemon stack over an `s3s-fs` backend, on the ADR-0002 baseline
/// shape most tests exercise. Note: `s3s-fs` itself enforces consecutive multipart
/// parts, so the Standard-only *sparse-part* acceptance is proven by the
/// `part_numbers_ok` unit test (crates/pacer-daemon/src/proxy.rs), not here.
async fn harness() -> Daemon {
    harness_with(BackendType::Express).await
}

/// [`harness`] at an explicit backend shape, for the ADR-0023 parity matrix.
async fn harness_with(backend_type: BackendType) -> Daemon {
    daemon_core(spec(backend_type)).await.in_process()
}

/// A body whose bytes depend on `seed` and on their own offset.
fn big_body(seed: u8, len: usize) -> Bytes {
    seeded_body(seed, len)
}

/// Chunks an object of `len` bytes occupies at the harness chunk size — the
/// per-object fill/hit count now that the cache unit is a chunk (ADR-0015).
fn chunks_for(len: usize) -> u64 {
    (len as u64).div_ceil(CHUNK_SIZE)
}

#[tokio::test]
async fn roundtrip_small_object_uncached() {
    let h = harness().await;
    let body = Bytes::from_static(b"hello pacer");
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("small.txt")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("small.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    // Below min size → never admitted.
    assert_eq!(h.metrics.fills_completed.get(), 0);
    assert_eq!(h.metrics.cache_hits.get(), 0);
}

#[tokio::test]
async fn large_object_fills_then_hits() {
    let h = harness().await;
    let body = big_body(1, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("large.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    let n_chunks = chunks_for(body.len());

    // Cold read: one header miss per GET, then one fill per covering chunk.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("large.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_misses.get(), 1);
    wait_for_fills(&h.metrics, n_chunks).await;
    assert_eq!(h.metrics.fills_completed.get(), n_chunks);

    // Warm read: every chunk served from cache, bytes identical.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("large.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_hits.get(), n_chunks);

    // Warm read observes backend deletion lag? No — but bytes must match the
    // etag/content-length contract.
    assert_eq!(got.content_length, Some(body.len() as i64));
}

#[tokio::test]
async fn range_reads_from_cached_object() {
    let h = harness().await;
    let len = (MIN_OBJECT_SIZE + 4096) as usize;
    let body = big_body(7, len);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    // Warm the cache with a whole read (fills every covering chunk).
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    wait_for_fills(&h.metrics, chunks_for(len)).await;

    // A range straddling a chunk boundary returns exactly the requested bytes.
    let start = (CHUNK_SIZE - 256) as usize;
    let end = (CHUNK_SIZE + 256) as usize; // exclusive
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .range(format!("bytes={start}-{}", end - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.content_range(),
        Some(format!("bytes {start}-{}/{len}", end - 1).as_str())
    );
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(start..end)
    );

    // A sub-chunk range (wholly inside chunk 0) returns exactly its bytes.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .range("bytes=100-199")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(100..200)
    );

    // bytes=100-4195 from cache.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .range("bytes=100-4195")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.content_range(),
        Some(format!("bytes 100-4195/{len}").as_str())
    );
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(100..4196)
    );

    // Suffix range.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .range("bytes=-1000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(len - 1000..len)
    );

    // Open-ended range.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .range(format!("bytes={}-", len - 512))
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(len - 512..len)
    );

    // Unsatisfiable range → InvalidRange error.
    let err = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("ranged.bin")
        .range(format!("bytes={len}-"))
        .send()
        .await
        .unwrap_err();
    let svc = err.into_service_error();
    assert_eq!(svc.meta().code(), Some("InvalidRange"));
}

#[tokio::test]
async fn read_after_write_sees_fresh_data() {
    let h = harness().await;
    let v1 = big_body(2, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("raw.bin")
        .body(ByteStream::from(v1.clone()))
        .send()
        .await
        .unwrap();

    // Cache v1 (one fill per covering chunk).
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("raw.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    let v1_chunks = chunks_for(v1.len());
    wait_for_fills(&h.metrics, v1_chunks).await;
    assert_eq!(h.metrics.fills_completed.get(), v1_chunks);

    // Overwrite through the daemon → cached v1 must be dropped.
    let v2 = big_body(3, (MIN_OBJECT_SIZE + 2048) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("raw.bin")
        .body(ByteStream::from(v2.clone()))
        .send()
        .await
        .unwrap();

    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("raw.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), v2);

    // Same for DELETE: next read is a clean 404.
    h.client
        .delete_object()
        .bucket(BUCKET)
        .key("raw.bin")
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("raw.bin")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_no_such_key());
}

#[tokio::test]
async fn cache_control_no_cache_bypasses() {
    let h = harness().await;
    let body = big_body(4, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("nocache.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("nocache.bin")
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("cache-control", "no-cache");
        })
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_bypass.get(), 1);
    assert_eq!(h.metrics.cache_misses.get(), 0);

    // no-store: read misses but must NOT populate.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("nocache.bin")
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("cache-control", "no-store");
        })
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_misses.get(), 1);
    // `no-store` read: nothing may be admitted. Watched throughout the window rather
    // than checked once after it, so a fill that does happen names the moment it did.
    common::no_fills_beyond(&h.metrics, 0).await;
    assert_eq!(h.metrics.fills_completed.get(), 0);
}

#[tokio::test]
async fn multipart_upload_roundtrip_and_part_order() {
    let h = harness().await;
    let part_size = 5 << 20; // S3 minimum part size
    let p1 = big_body(5, part_size);
    let p2 = big_body(6, 1024);

    let mpu = h
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("multi.bin")
        .send()
        .await
        .unwrap();
    let upload_id = mpu.upload_id().unwrap();

    let mut completed = Vec::new();
    for (i, part) in [p1.clone(), p2.clone()].into_iter().enumerate() {
        let n = i as i32 + 1;
        let up = h
            .client
            .upload_part()
            .bucket(BUCKET)
            .key("multi.bin")
            .upload_id(upload_id)
            .part_number(n)
            .body(ByteStream::from(part))
            .send()
            .await
            .unwrap();
        completed.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(n)
                .e_tag(up.e_tag().unwrap())
                .build(),
        );
    }

    // Non-consecutive part numbers must be rejected before hitting the backend
    // (directory-bucket rule enforced at the proxy).
    let bad = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .parts(completed[1].clone())
        .build();
    let err = h
        .client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("multi.bin")
        .upload_id(upload_id)
        .multipart_upload(bad)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidPartOrder")
    );

    let good = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed))
        .build();
    h.client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("multi.bin")
        .upload_id(upload_id)
        .multipart_upload(good)
        .send()
        .await
        .unwrap();

    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("multi.bin")
        .send()
        .await
        .unwrap();
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(bytes.len(), part_size + 1024);
    assert_eq!(&bytes[..part_size], &p1[..]);
    assert_eq!(&bytes[part_size..], &p2[..]);
}

#[tokio::test]
async fn standard_backend_writes_multipart_and_cached_reads_roundtrip() {
    // ADR-0023 full parity: the read/cache path, write-through, and multipart
    // all work against a Standard-configured daemon exactly as against Express.
    // (Cross-AZ latency is out of scope here — that is the D2 benchmark; this is
    // functional parity on the s3s-fs backend.)
    let h = harness_with(BackendType::Standard).await;

    // Write-through + cached read: cold fill then warm all-local hit.
    let body = big_body(11, (MIN_OBJECT_SIZE + 2048) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("std.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let n_chunks = chunks_for(body.len());
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("std.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    wait_for_fills(&h.metrics, n_chunks).await;
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("std.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_hits.get(), n_chunks);

    // Multipart round-trip (consecutive parts — the case s3s-fs supports; the
    // Standard-only sparse-part acceptance is unit-tested in the proxy).
    let part_size = 5 << 20; // S3 minimum part size
    let p1 = big_body(12, part_size);
    let p2 = big_body(13, 4096);
    let mpu = h
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("std-multi.bin")
        .send()
        .await
        .unwrap();
    let upload_id = mpu.upload_id().unwrap();
    let mut completed = Vec::new();
    for (i, part) in [p1.clone(), p2.clone()].into_iter().enumerate() {
        let n = i as i32 + 1;
        let up = h
            .client
            .upload_part()
            .bucket(BUCKET)
            .key("std-multi.bin")
            .upload_id(upload_id)
            .part_number(n)
            .body(ByteStream::from(part))
            .send()
            .await
            .unwrap();
        completed.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(n)
                .e_tag(up.e_tag().unwrap())
                .build(),
        );
    }
    let good = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed))
        .build();
    h.client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("std-multi.bin")
        .upload_id(upload_id)
        .multipart_upload(good)
        .send()
        .await
        .unwrap();
    let bytes = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("std-multi.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(bytes.len(), part_size + 4096);
    assert_eq!(&bytes[..part_size], &p1[..]);
    assert_eq!(&bytes[part_size..], &p2[..]);
}

#[tokio::test]
async fn head_list_delete_passthrough() {
    let h = harness().await;
    for key in ["a/1.bin", "a/2.bin", "b/3.bin"] {
        h.client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(Bytes::from_static(b"x")))
            .send()
            .await
            .unwrap();
    }

    let head = h
        .client
        .head_object()
        .bucket(BUCKET)
        .key("a/1.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length, Some(1));

    let list = h
        .client
        .list_objects_v2()
        .bucket(BUCKET)
        .prefix("a/")
        .send()
        .await
        .unwrap();
    let keys: Vec<_> = list.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys.len(), 2);
    assert!(keys.contains(&"a/1.bin") && keys.contains(&"a/2.bin"));

    h.client
        .delete_object()
        .bucket(BUCKET)
        .key("a/1.bin")
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .head_object()
        .bucket(BUCKET)
        .key("a/1.bin")
        .send()
        .await
        .unwrap_err();
    // HeadObject errors have no body; the SDK surfaces the backend's NoSuchKey
    // code via metadata rather than the modeled NotFound variant.
    let svc = err.into_service_error();
    assert!(svc.is_not_found() || svc.meta().code() == Some("NoSuchKey"));
}

#[tokio::test]
async fn wrong_placeholder_credentials_rejected() {
    let h = harness().await;
    // Re-point a client at the daemon with the wrong access key.
    let bad = h.client.config().clone();
    let conf = bad
        .to_builder()
        .credentials_provider(aws_sdk_s3::config::SharedCredentialsProvider::new(
            Credentials::new("intruder", "nope", None, None, "test"),
        ))
        .build();
    let bad_client = aws_sdk_s3::Client::from_conf(conf);
    let err = bad_client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap_err();
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InvalidAccessKeyId")
    );
}

#[tokio::test]
async fn writes_reach_backend_unmodified() {
    let h = harness().await;
    let body = Bytes::from_static(b"observed at the backend");
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("wt.txt")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    // Ground truth read straight from the backend, not through the daemon.
    let got = h
        .backend
        .get_object()
        .bucket(BUCKET)
        .key("wt.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
}

#[tokio::test]
async fn cold_range_read_fills_only_covering_chunks() {
    // ADR-0015 supersedes ADR-0011: a cold ranged read now fills exactly its
    // covering chunks (never the whole object — the cost invariant holds by
    // construction), so a second read of the same range is an all-local hit.
    let h = harness().await;
    let len = (MIN_OBJECT_SIZE + 4 * CHUNK_SIZE) as usize; // spans many chunks
    let body = big_body(9, len);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("slice.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    // Read exactly chunk 0's bytes.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("slice.bin")
        .range(format!("bytes=0-{}", CHUNK_SIZE - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(0..CHUNK_SIZE as usize)
    );
    assert_eq!(h.metrics.cache_misses.get(), 1);

    // Exactly ONE chunk filled (the covering chunk), not the whole object.
    wait_for_fills(&h.metrics, 1).await;
    assert_eq!(h.metrics.fills_completed.get(), 1);
    assert_eq!(h.metrics.bytes_filled.get(), CHUNK_SIZE);

    // A second identical range read is now an all-local hit (the header is
    // cached, so no second header miss; the covering chunk is served locally).
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("slice.bin")
        .range(format!("bytes=0-{}", CHUNK_SIZE - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(0..CHUNK_SIZE as usize)
    );
    assert_eq!(h.metrics.cache_hits.get(), 1);
    assert_eq!(h.metrics.cache_misses.get(), 1); // unchanged: header was warm
                                                 // Still only one chunk ever filled — the rest of the object was untouched.
    assert_eq!(h.metrics.fills_completed.get(), 1);
}

// ---------------------------------------------------------------------------
// ADR-0023 Express + Standard parity matrix (workstream D2)
// ---------------------------------------------------------------------------
// The backend-agnostic behaviors — write-through, cold fill + warm hit, range
// math, read-after-write / delete invalidation, and the *consecutive*-part
// multipart path — must be byte-identical on BOTH backends. Each scenario below
// is written once and run against `BackendType::Express` AND
// `BackendType::Standard` by `parity_matrix!`, so a future change that regresses
// only one shape fails here.
//
// What is NOT covered locally, and why (honest boundary — see the module doc and
// `tests/daemon/backend_matrix_s3.rs`): the two backends *diverge* only on write-path
// normalization — Express strips `Content-MD5` and requires consecutive
// multipart parts; Standard forwards `Content-MD5` and accepts sparse
// (ascending-with-gaps) parts. The local `s3s-fs` backend ignores `Content-MD5`
// and itself rejects non-consecutive parts, so it *cannot* exercise those
// divergent behaviors. The part-number *rule* is unit-tested in
// `crates/pacer-daemon/src/proxy.rs` (`part_numbers_ok`); the end-to-end sparse
// completion and `Content-MD5` pass-through against a real bucket live in the
// `#[ignore]`-gated `tests/daemon/backend_matrix_s3.rs` arm (which fails loudly rather
// than passing when no bucket is configured — never a silent skip).

// Each scenario below is one `#[rstest]` with a case per backend shape, replacing the
// hand-rolled `parity_matrix!` macro that used to write each `#[tokio::test]` twice —
// which is precisely the "two tests differing by one parameter" shape a parameterised
// test is for. The cases are named, so a failure reads
// `parity_straddling_range_from_cache::case_2_standard` rather than `case_2`.
//
// Every case builds its OWN daemon via `harness_with`, so a scenario's metric assertions
// start from zero: a shared fixture would make "one header miss" mean "one so far".

/// Write-through then cold fill + warm all-local hit: bytes round-trip and the
/// cache accounting (one header miss, one fill per covering chunk, one hit per
/// chunk on the warm read) is identical on both backends.
async fn scenario_write_through_then_warm_hit(h: &Daemon) {
    let body = big_body(21, (MIN_OBJECT_SIZE + 2048) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("parity/wt.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let n_chunks = chunks_for(body.len());

    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("parity/wt.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_misses.get(), 1);
    wait_for_fills(&h.metrics, n_chunks).await;
    assert_eq!(h.metrics.fills_completed.get(), n_chunks);

    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("parity/wt.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.metrics.cache_hits.get(), n_chunks);
}

/// A range straddling a chunk boundary returns exactly the requested bytes from
/// the warm cache on both backends.
async fn scenario_straddling_range_from_cache(h: &Daemon) {
    let len = (MIN_OBJECT_SIZE + 4096) as usize;
    let body = big_body(22, len);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("parity/range.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("parity/range.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    wait_for_fills(&h.metrics, chunks_for(len)).await;

    let start = (CHUNK_SIZE - 256) as usize;
    let end = (CHUNK_SIZE + 256) as usize; // exclusive
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("parity/range.bin")
        .range(format!("bytes={start}-{}", end - 1))
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.content_range(),
        Some(format!("bytes {start}-{}/{len}", end - 1).as_str())
    );
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(start..end)
    );
}

/// Overwrite-through drops the cached copy and a subsequent DELETE-through
/// yields a clean 404 — invalidation is backend-agnostic.
async fn scenario_overwrite_and_delete_invalidate(h: &Daemon) {
    let v1 = big_body(23, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("parity/raw.bin")
        .body(ByteStream::from(v1.clone()))
        .send()
        .await
        .unwrap();
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("parity/raw.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    wait_for_fills(&h.metrics, chunks_for(v1.len())).await;

    let v2 = big_body(24, (MIN_OBJECT_SIZE + 2048) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("parity/raw.bin")
        .body(ByteStream::from(v2.clone()))
        .send()
        .await
        .unwrap();
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("parity/raw.bin")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), v2);

    h.client
        .delete_object()
        .bucket(BUCKET)
        .key("parity/raw.bin")
        .send()
        .await
        .unwrap();
    let err = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("parity/raw.bin")
        .send()
        .await
        .unwrap_err();
    assert!(err.into_service_error().is_no_such_key());
}

/// A write reaches the backend byte-for-byte on both shapes (ground truth read
/// straight from the backend, bypassing the daemon).
async fn scenario_write_reaches_backend_unmodified(h: &Daemon) {
    let body = Bytes::from_static(b"parity: observed at the backend");
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("parity/wt.txt")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let got = h
        .backend
        .get_object()
        .bucket(BUCKET)
        .key("parity/wt.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
}

/// A consecutive-part multipart upload completes and reads back as the
/// concatenation on both shapes. (Sparse parts — the Standard-only relaxation —
/// need a real bucket; see `tests/daemon/backend_matrix_s3.rs`.)
async fn scenario_consecutive_multipart_roundtrip(h: &Daemon) {
    let part_size = 5 << 20; // S3 minimum part size
    let p1 = big_body(25, part_size);
    let p2 = big_body(26, 4096);
    let mpu = h
        .client
        .create_multipart_upload()
        .bucket(BUCKET)
        .key("parity/multi.bin")
        .send()
        .await
        .unwrap();
    let upload_id = mpu.upload_id().unwrap();
    let mut completed = Vec::new();
    for (i, part) in [p1.clone(), p2.clone()].into_iter().enumerate() {
        let n = i as i32 + 1;
        let up = h
            .client
            .upload_part()
            .bucket(BUCKET)
            .key("parity/multi.bin")
            .upload_id(upload_id)
            .part_number(n)
            .body(ByteStream::from(part))
            .send()
            .await
            .unwrap();
        completed.push(
            aws_sdk_s3::types::CompletedPart::builder()
                .part_number(n)
                .e_tag(up.e_tag().unwrap())
                .build(),
        );
    }
    let good = aws_sdk_s3::types::CompletedMultipartUpload::builder()
        .set_parts(Some(completed))
        .build();
    h.client
        .complete_multipart_upload()
        .bucket(BUCKET)
        .key("parity/multi.bin")
        .upload_id(upload_id)
        .multipart_upload(good)
        .send()
        .await
        .unwrap();
    let bytes = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("parity/multi.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes();
    assert_eq!(bytes.len(), part_size + 4096);
    assert_eq!(&bytes[..part_size], &p1[..]);
    assert_eq!(&bytes[part_size..], &p2[..]);
}

#[rstest]
#[case::express(BackendType::Express)]
#[case::standard(BackendType::Standard)]
#[tokio::test]
async fn parity_write_through_then_warm_hit(#[case] backend: BackendType) {
    scenario_write_through_then_warm_hit(&harness_with(backend).await).await;
}

#[rstest]
#[case::express(BackendType::Express)]
#[case::standard(BackendType::Standard)]
#[tokio::test]
async fn parity_straddling_range_from_cache(#[case] backend: BackendType) {
    scenario_straddling_range_from_cache(&harness_with(backend).await).await;
}

#[rstest]
#[case::express(BackendType::Express)]
#[case::standard(BackendType::Standard)]
#[tokio::test]
async fn parity_overwrite_and_delete_invalidate(#[case] backend: BackendType) {
    scenario_overwrite_and_delete_invalidate(&harness_with(backend).await).await;
}

#[rstest]
#[case::express(BackendType::Express)]
#[case::standard(BackendType::Standard)]
#[tokio::test]
async fn parity_write_reaches_backend_unmodified(#[case] backend: BackendType) {
    scenario_write_reaches_backend_unmodified(&harness_with(backend).await).await;
}

#[rstest]
#[case::express(BackendType::Express)]
#[case::standard(BackendType::Standard)]
#[tokio::test]
async fn parity_consecutive_multipart_roundtrip(#[case] backend: BackendType) {
    scenario_consecutive_multipart_roundtrip(&harness_with(backend).await).await;
}

// ---- ADR-0039: a conditional GET reaches the cache when the ETag agrees --------------
//
// Motivated by a measured 0 %: Mountpoint-for-S3 puts `If-Match` on EVERY GET, so against
// it PACER's hit rate was exactly zero and 100 % of its reads bypassed
// (`bench/ladder/results/mountpoint-vs-pacer.md`). These three cases are the whole
// behaviour — honoured on a match, passed through on a mismatch, and never a 412.

#[tokio::test]
async fn an_if_match_get_hits_the_cache_when_the_etag_agrees() {
    let h = harness().await;
    let body = big_body(2, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("cond.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let n_chunks = chunks_for(body.len());

    // Warm the cache with an ordinary GET, and learn the ETag the way a client does.
    let cold = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("cond.bin")
        .send()
        .await
        .unwrap();
    let etag = cold
        .e_tag()
        .expect("the backend returns an ETag")
        .to_owned();
    assert_eq!(cold.body.collect().await.unwrap().into_bytes(), body);
    wait_for_fills(&h.metrics, n_chunks).await;
    let hits_before = h.metrics.cache_hits.get();

    // The conditional read: same bytes, and it HITS rather than bypassing.
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("cond.bin")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body,
        "a conditional GET must return the same bytes as an unconditional one"
    );
    assert_eq!(
        h.metrics.cache_hits.get(),
        hits_before + n_chunks,
        "every covering chunk should have been served from cache"
    );
    assert_eq!(
        h.metrics.conditional_get_served.get(),
        1,
        "the ADR-0039 counter is the only series that says the conditional was honoured"
    );
}

#[tokio::test]
async fn an_if_match_get_with_an_unmatched_etag_is_delegated_not_answered() {
    // The assertion is deliberately about what PACER does and NOT about the status code:
    // this harness's backend is `s3s-fs`, which does not implement conditional GETs at all,
    // so it answers 200 where real S3 answers 412. That difference is the whole point —
    // whatever the precondition's verdict is, it is the BACKEND's to give, and this proxy
    // must neither serve cached bytes for a version nobody named nor invent a 412 of its own
    // (S3 may hold the ETag the client asked for while this node holds an older one).
    let h = harness().await;
    let body = big_body(3, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("cond-stale.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    // Warm the cache first, so "not served from cache" is a real claim rather than a cold
    // miss that could not have been served anyway.
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("cond-stale.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    let n_chunks = chunks_for(body.len());
    wait_for_fills(&h.metrics, n_chunks).await;
    let hits_before = h.metrics.cache_hits.get();
    let bypass_before = h.metrics.cache_bypass.get();

    // An ETag this node does not hold.
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("cond-stale.bin")
        .if_match("\"00000000000000000000000000000000\"")
        .send()
        .await
        .expect("the backend decides the precondition; this one accepts it")
        .body
        .collect()
        .await
        .unwrap();

    assert_eq!(
        h.metrics.cache_hits.get(),
        hits_before,
        "an unmatched ETag must not be served from cache even when the object IS cached"
    );
    assert_eq!(
        h.metrics.cache_bypass.get(),
        bypass_before + 1,
        "it should be counted as the bypass it is"
    );
    assert_eq!(
        h.metrics.conditional_get_served.get(),
        0,
        "and must not be counted as honoured"
    );
}

#[tokio::test]
async fn the_knob_restores_the_unconditional_bypass() {
    // `PACER_CONDITIONAL_GET_FROM_CACHE=false` is the documented way back to the
    // pre-ADR-0039 behaviour, and it has to be checked because the whole point of the
    // escape hatch is that an operator who needs strict conditional semantics gets them.
    let h = daemon_core(DaemonSpec {
        conditional_get_from_cache: false,
        ..spec(BackendType::Express)
    })
    .await
    .in_process();
    let body = big_body(4, (MIN_OBJECT_SIZE + 1024) as usize);
    h.client
        .put_object()
        .bucket(BUCKET)
        .key("cond-off.bin")
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let n_chunks = chunks_for(body.len());
    h.client
        .get_object()
        .bucket(BUCKET)
        .key("cond-off.bin")
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    let cold = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("cond-off.bin")
        .send()
        .await
        .unwrap();
    let etag = cold.e_tag().expect("an ETag").to_owned();
    cold.body.collect().await.unwrap();
    wait_for_fills(&h.metrics, n_chunks).await;
    let hits_before = h.metrics.cache_hits.get();

    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("cond-off.bin")
        .if_match(&etag)
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(
        h.metrics.cache_hits.get(),
        hits_before,
        "with the knob off a matching ETag must still bypass"
    );
    assert_eq!(h.metrics.conditional_get_served.get(), 0);
}
