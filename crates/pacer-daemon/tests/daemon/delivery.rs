//! ADR-0026 (planning/19 Track C C1): delivery into client-supplied memory, end
//! to end through [`PacerProxy`] over an `s3s-fs` backend.
//!
//! These call the proxy **directly** rather than through an `aws-sdk-s3` client,
//! and that choice is forced by what the protocol actually is: a *header-only*
//! 200. The SDK's modeled `GetObjectOutput` drops unmodeled response headers, so
//! an SDK-level assertion literally cannot observe `x-pacer-delivered` — it would
//! test that the body is empty while being blind to the completion signal that
//! makes an empty body correct. Seeding goes through the backend client, and the
//! ordinary SDK read path stays covered by `correctness.rs`.
//!
//! What is NOT exercised here: the one-sided RDMA source (`peer_rdma`). It needs
//! two nodes and EFA hardware — planning/19 Track C's own measurement step — so
//! these tests cover the protocol, the mapping, the window arithmetic, the quota
//! fallback, and the compatibility gate, which are the parts that can be wrong
//! without hardware.
//!
//! Nor is **lazy registration** provable here, and it would be dishonest to assert it
//! with a passing test: on this build there is no RDMA plane, so
//! `pacer_delivery_registrations_total` is trivially 0 whether the code is lazy or
//! not. The property — a locally-served delivery performs ZERO `ibv_reg_mr` calls —
//! is checked on hardware by `run.sh deliver`, which prints that counter against the
//! delivered request count for exactly this reason.

// Tests are linear scenarios; splitting them to satisfy a line count would hurt
// readability (CLAUDE.md: size limits target production code).
#![allow(clippy::too_many_lines)]

use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use futures::StreamExt;
use pacer_daemon::delivery::{
    DeliveryConfig, DeliveryQuota, CHECKSUM_HEADER, DELIVERED_HEADER, TARGET_HEADER,
};
use pacer_daemon::metrics::Metrics;
use pacer_daemon::preflight::{ENDPOINTS_HEADER, ENDPOINTS_VERSION, GET_ENDPOINTS_HEADER};
use pacer_daemon::proxy::PacerProxy;
use rstest::rstest;
use s3s::dto;
use s3s::{S3Request, S3};

use crate::common::{daemon_core, CacheSpec, DaemonSpec, BUCKET};

/// Small chunk so a few-MiB object spans several chunks: the covering-window math
/// and the concurrent delivery pipeline are what these tests are about.
const CHUNK_SIZE: u64 = 1 << 20;
/// Everything in this file is meant to be cacheable, so the floor is nominal.
const MIN_OBJECT_SIZE: u64 = 1 << 10;
/// Three chunks: a first, a middle and a last, which is the smallest object that
/// can show a partial edge window next to a whole one.
const OBJECT_LEN: usize = (CHUNK_SIZE * 3) as usize;
/// Chunks of one object, kept beside [`OBJECT_LEN`] so the fan-out test's expected
/// width and the object it delivers cannot drift apart.
const OBJECT_CHUNKS: i64 = 3;
/// Concurrent delivery windows every test but the fan-out pair runs with. Small on
/// purpose: these objects are 3 chunks, and a fan-out wider than the window count
/// would hide an ordering bug in the digest fold.
const DELIVERY_PARALLELISM: usize = 2;
/// Filler the segment is pre-written with, so "delivery stayed inside its window"
/// is checkable byte for byte rather than by absence.
const SEGMENT_FILLER: u8 = 0xAA;
/// A quota ceiling provably above anything an arm here asks for, so a rejection can
/// only have come from the ceiling the arm *did* tighten.
const ROOMY_CEILING: u64 = 1 << 30;
/// A quota ceiling provably below one chunk, so the reservation cannot fit whichever
/// of the two ADR-0026 ceilings it is applied to.
const TIGHT_CEILING: u64 = 4096;

/// The daemon these tests drive, and the pieces they observe it through.
///
/// This is the one arm that keeps a [`common::DaemonCore`] and never chooses a shape:
/// the assertions are on the *proxy's* return value, not on a client's, because the
/// protocol is a header-only 200 and no SDK client can see it.
///
/// [`common::DaemonCore`]: crate::common::DaemonCore
struct Harness {
    proxy: PacerProxy,
    /// Direct backend client — seeds objects without going through the daemon.
    backend: aws_sdk_s3::Client,
    metrics: Metrics,
    quota: Arc<DeliveryQuota>,
    /// Stands in for `/dev/shm`: the directory `shm:/name` resolves under.
    shm: tempfile::TempDir,
    /// The backend's and the cache's directories, held so they outlive the proxy.
    _dirs: Vec<tempfile::TempDir>,
}

/// A daemon with delivery enabled and ceilings well above anything these tests
/// ask for.
async fn harness() -> Harness {
    harness_with(true, ROOMY_CEILING, ROOMY_CEILING).await
}

/// [`harness`] with the two ADR-0026 ceilings and the enable flag under the caller's
/// control, at the suite's default fan-out.
async fn harness_with(enabled: bool, pinned_bytes_max: u64, max_target_bytes: u64) -> Harness {
    harness_with_parallelism(
        enabled,
        pinned_bytes_max,
        max_target_bytes,
        DELIVERY_PARALLELISM,
    )
    .await
}

/// [`harness_with`], plus the concurrent-window knob the fan-out arm varies.
async fn harness_with_parallelism(
    enabled: bool,
    pinned_bytes_max: u64,
    max_target_bytes: u64,
    parallelism: usize,
) -> Harness {
    let shm = tempfile::tempdir().unwrap();
    let mut core = daemon_core(DaemonSpec {
        min_object_size: MIN_OBJECT_SIZE,
        max_object_size: None,
        chunk_size: CHUNK_SIZE,
        cache: CacheSpec {
            mem_capacity: 64 << 20,
            disk_capacity: 256 << 20,
            block_size: 16 << 20,
        },
        ..DaemonSpec::default()
    })
    .await;

    let quota = Arc::new(DeliveryQuota::new(pinned_bytes_max, max_target_bytes));
    core.metrics.set_delivery_quota(Arc::clone(&quota));
    core.proxy = core.proxy.with_delivery(
        DeliveryConfig {
            enabled,
            shm_dir: shm.path().to_path_buf(),
            max_target_bytes,
            pinned_bytes_max,
            parallelism,
            // Nothing here is a cluster: there are no holders, so the holder-writes-directly
            // half (planning/19 C3) has nothing to reach and this suite exercises the same
            // code either way. Left at its default so the suite keeps testing the shipped one.
            remote_write: DeliveryConfig::default().remote_write,
        },
        Arc::clone(&quota),
    );

    Harness {
        proxy: core.proxy,
        backend: core.backend,
        metrics: core.metrics,
        quota,
        shm,
        _dirs: core.dirs,
    }
}

impl Harness {
    /// Seed `key` with `len` deterministic bytes, straight into the backend.
    async fn seed(&self, key: &str, len: usize) -> Bytes {
        let body: Bytes = (0..len)
            .map(|i| (i % 251) as u8)
            .collect::<Vec<u8>>()
            .into();
        self.backend
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();
        body
    }

    /// Create a client segment of `size` bytes, pre-filled with
    /// [`SEGMENT_FILLER`], and return its path.
    fn segment(&self, name: &str, size: usize) -> std::path::PathBuf {
        let path = self.shm.path().join(name);
        std::fs::write(&path, vec![SEGMENT_FILLER; size]).unwrap();
        path
    }

    /// GET `key`, optionally naming a delivery target and/or a byte range.
    async fn get(
        &self,
        key: &str,
        target: Option<&str>,
        range: Option<dto::Range>,
    ) -> s3s::S3Result<s3s::S3Response<dto::GetObjectOutput>> {
        let mut headers = hyper::HeaderMap::new();
        if let Some(target) = target {
            headers.insert(
                hyper::header::HeaderName::from_static(TARGET_HEADER),
                hyper::header::HeaderValue::from_str(target).unwrap(),
            );
        }
        self.get_with_headers(key, range, headers).await
    }

    /// GET `key` with an arbitrary header map — what the pre-flight needs, since its marker is a
    /// request header on an ordinary read rather than a route of its own.
    async fn get_with_headers(
        &self,
        key: &str,
        range: Option<dto::Range>,
        headers: hyper::HeaderMap,
    ) -> s3s::S3Result<s3s::S3Response<dto::GetObjectOutput>> {
        let req = S3Request {
            input: dto::GetObjectInput {
                bucket: BUCKET.to_owned(),
                key: key.to_owned(),
                range,
                ..Default::default()
            },
            method: hyper::Method::GET,
            uri: format!("http://pacer.local/{BUCKET}/{key}")
                .parse()
                .unwrap(),
            headers,
            extensions: hyper::http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        };
        self.proxy.get_object(req).await
    }
}

impl Harness {
    /// Ask for `key`'s endpoints the way a shim does: an ordinary GET carrying the marker header,
    /// with `Range: bytes=0-0` as the fallback bound (a daemon that ignores the marker then
    /// answers one byte rather than a checkpoint).
    ///
    /// Returns the whole response so a test can assert on the marker header, the body, and the
    /// absence of the object metadata this path never resolves.
    async fn preflight(&self, key: &str, ask: &str) -> s3s::S3Response<dto::GetObjectOutput> {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::HeaderName::from_static(GET_ENDPOINTS_HEADER),
            hyper::header::HeaderValue::from_str(ask).unwrap(),
        );
        self.get_with_headers(
            key,
            Some(dto::Range::Int {
                first: 0,
                last: Some(0),
            }),
            headers,
        )
        .await
        .expect("a well-formed pre-flight is answered")
    }

    /// The pre-flight's document, having first asserted it is marked as one.
    async fn preflight_document(&self, key: &str, ask: &str) -> String {
        let resp = self.preflight(key, ask).await;
        assert_eq!(
            header(&resp, ENDPOINTS_HEADER),
            Some(ENDPOINTS_VERSION.to_string()),
            "the answer must be MARKED: its absence is how a client tells this from an older \
             daemon — or one with delivery off — answering the read normally"
        );
        drain_string(resp.output.body).await
    }
}

/// Drain a response body to a string.
async fn drain_string(body: Option<dto::StreamingBlob>) -> String {
    String::from_utf8(drain(body).await.to_vec()).expect("the pre-flight answer is JSON")
}

/// Drain a response body into bytes (`None` = the header-only delivery answer).
async fn drain(body: Option<dto::StreamingBlob>) -> Bytes {
    let Some(mut body) = body else {
        return Bytes::new();
    };
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.unwrap());
    }
    Bytes::from(out)
}

/// `crc32=<hex>` over `bytes`, the way the shim re-checks a delivery.
fn expected_checksum(bytes: &[u8]) -> String {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(bytes);
    format!("crc32={:08x}", hasher.finalize())
}

fn header(resp: &s3s::S3Response<dto::GetObjectOutput>, name: &str) -> Option<String> {
    resp.headers
        .get(name)
        .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
}

/// Bytes counted as chunks landed in client memory from `source` —
/// `pacer_delivery_chunk_bytes_total{source}`, the delivery path's bandwidth series.
fn chunk_bytes(h: &Harness, source: &str) -> u64 {
    h.metrics
        .delivery
        .chunk_bytes
        .with_label_values(&[source])
        .get()
}

/// The compatibility gate, and the reason it is structural rather than
/// aspirational (ADR-0026 "Consequences"): with delivery ENABLED, a client that
/// sends no target header still gets the whole object in the body and sees none
/// of the extension's headers.
#[tokio::test]
async fn stock_get_is_untouched() {
    let h = harness().await;
    let body = h.seed("stock.bin", OBJECT_LEN).await;

    let resp = h.get("stock.bin", None, None).await.unwrap();
    let (delivered, checksum) = (
        header(&resp, DELIVERED_HEADER),
        header(&resp, CHECKSUM_HEADER),
    );
    assert_eq!(resp.output.content_length, Some(OBJECT_LEN as i64));
    assert_eq!(drain(resp.output.body).await, body);
    assert_eq!(delivered, None);
    assert_eq!(checksum, None);
    assert_eq!(h.metrics.delivery.requests.get(), 0);
    assert_eq!(h.quota.in_use(), 0);
}

/// A whole-object delivery: no body, `Content-Length: 0`, the delivered count and
/// a checksum in headers, and the object's bytes in the client's own segment.
/// The second read proves the local tier delivers by `memcpy` (ADR-0026 point 4)
/// once the chunks are cached.
#[tokio::test]
async fn delivers_a_whole_object_into_client_memory() {
    let h = harness().await;
    let body = h.seed("whole.bin", OBJECT_LEN).await;
    let path = h.segment("loader-1", OBJECT_LEN);
    let target = format!("shm:/loader-1;offset=0;len={OBJECT_LEN}");

    let resp = h.get("whole.bin", Some(&target), None).await.unwrap();
    assert!(resp.output.body.is_none(), "a delivery carries no body");
    assert_eq!(resp.output.content_length, Some(0));
    assert_eq!(
        header(&resp, DELIVERED_HEADER),
        Some(OBJECT_LEN.to_string())
    );
    assert_eq!(
        header(&resp, CHECKSUM_HEADER),
        Some(expected_checksum(&body))
    );
    // The ETag still rides along, so a loader need not issue a HEAD for it.
    assert!(resp.output.e_tag.is_some());
    // The client's own view of its segment: exactly the object, nothing else.
    assert_eq!(Bytes::from(std::fs::read(&path).unwrap()), body);

    // Cold: every chunk was read through from the backend.
    let n_chunks = OBJECT_LEN as u64 / CHUNK_SIZE;
    assert_eq!(
        h.metrics
            .delivery
            .chunks
            .with_label_values(&["backend"])
            .get(),
        n_chunks
    );
    assert_eq!(h.metrics.delivery.requests.get(), 1);
    assert_eq!(h.metrics.delivery.bytes.get(), OBJECT_LEN as u64);
    // The per-chunk byte series agrees with the per-request one on a whole-object read,
    // and attributes every byte to the source that supplied it.
    assert_eq!(chunk_bytes(&h, "backend"), OBJECT_LEN as u64);
    // The reservation is released with the request, not held for the process.
    assert_eq!(h.quota.in_use(), 0);

    // Warm: the same read now delivers from this node's cache.
    std::fs::write(&path, vec![SEGMENT_FILLER; OBJECT_LEN]).unwrap();
    let resp = h.get("whole.bin", Some(&target), None).await.unwrap();
    assert_eq!(
        header(&resp, DELIVERED_HEADER),
        Some(OBJECT_LEN.to_string())
    );
    assert_eq!(Bytes::from(std::fs::read(&path).unwrap()), body);
    assert_eq!(
        h.metrics
            .delivery
            .chunks
            .with_label_values(&["local"])
            .get(),
        n_chunks
    );
    assert_eq!(chunk_bytes(&h, "local"), OBJECT_LEN as u64);
}

/// One delivered GET fans out `min(delivery.parallelism, windows in the request)`
/// chunk resolutions — and **the second term is the one that binds in practice.**
///
/// Worth a test rather than a comment because a whole benchmark arm turned on the
/// difference. The 2026-08-25 32-rail C4 run raised `parallelism` to 256 against
/// 512 MiB spans of 16 MiB chunks — 32 windows per GET — and then read a per-chunk
/// cost off the wall clock as though the knob had taken effect. It could not have:
/// there were never more than 32 windows to hand it, so 8× of that setting was
/// unreachable and the "one outstanding read" hypothesis it produced was measuring
/// something else (`bench/ladder/results/c4-dcp-hf-safetensors.md`).
///
/// `pacer_delivery_inflight_chunks_peak` is what makes the real width visible on
/// hardware; this pins the arithmetic behind it.
#[tokio::test]
async fn the_fan_out_is_bounded_by_the_windows_a_request_has() {
    // Knob below the window count: the knob binds.
    let narrow =
        harness_with_parallelism(true, ROOMY_CEILING, ROOMY_CEILING, DELIVERY_PARALLELISM).await;
    narrow.seed("fanout-narrow.bin", OBJECT_LEN).await;
    narrow.segment("loader-fanout-1", OBJECT_LEN);
    let target = format!("shm:/loader-fanout-1;offset=0;len={OBJECT_LEN}");
    let resp = narrow
        .get("fanout-narrow.bin", Some(&target), None)
        .await
        .unwrap();
    assert_eq!(
        header(&resp, DELIVERED_HEADER),
        Some(OBJECT_LEN.to_string())
    );
    assert_eq!(
        narrow.metrics.delivery.inflight_chunks_peak.get(),
        DELIVERY_PARALLELISM as i64,
        "with {DELIVERY_PARALLELISM} permitted and {OBJECT_CHUNKS} windows, the knob is the bound"
    );

    // Knob above the window count: the REQUEST binds, and raising the knob further
    // cannot widen anything — which is the whole finding.
    let wide = harness_with_parallelism(
        true,
        ROOMY_CEILING,
        ROOMY_CEILING,
        DELIVERY_PARALLELISM + OBJECT_CHUNKS as usize,
    )
    .await;
    wide.seed("fanout-wide.bin", OBJECT_LEN).await;
    wide.segment("loader-fanout-2", OBJECT_LEN);
    let target = format!("shm:/loader-fanout-2;offset=0;len={OBJECT_LEN}");
    let resp = wide
        .get("fanout-wide.bin", Some(&target), None)
        .await
        .unwrap();
    assert_eq!(
        header(&resp, DELIVERED_HEADER),
        Some(OBJECT_LEN.to_string())
    );
    assert_eq!(
        wide.metrics.delivery.inflight_chunks_peak.get(),
        OBJECT_CHUNKS,
        "a request of {OBJECT_CHUNKS} windows cannot fan out wider than {OBJECT_CHUNKS}"
    );

    // And the gauge is a gauge: every resolution released its slot, so a later
    // request starts from zero rather than inheriting a leaked count.
    assert_eq!(wide.metrics.delivery.inflight_chunks.get(), 0);
    // Latency was observed once per delivered chunk, under the source that served it.
    assert_eq!(
        wide.metrics
            .delivery
            .chunk_seconds
            .with_label_values(&["backend"])
            .get_sample_count(),
        OBJECT_CHUNKS as u64
    );
}

/// A ranged delivery lands at the client's own offset, delivers exactly the
/// requested bytes, and touches nothing else in the segment — the disjointness
/// `MappedTarget` relies on, observed from the client's side. The range starts and
/// ends mid-chunk, so both edge windows take the trim path rather than the
/// whole-chunk one.
#[tokio::test]
async fn ranged_delivery_writes_only_its_window() {
    let h = harness().await;
    let body = h.seed("ranged.bin", OBJECT_LEN).await;
    // A window at a non-zero offset inside a larger segment, with guard bytes on
    // both sides.
    const WINDOW_OFFSET: usize = 4096;
    let (first, last) = (CHUNK_SIZE / 2, CHUNK_SIZE * 2 + 7);
    let want = (last - first + 1) as usize;
    let segment_len = WINDOW_OFFSET + want + 4096;
    let path = h.segment("loader-2", segment_len);
    let target = format!("shm:/loader-2;offset={WINDOW_OFFSET};len={want}");

    let resp = h
        .get(
            "ranged.bin",
            Some(&target),
            Some(dto::Range::Int {
                first,
                last: Some(last),
            }),
        )
        .await
        .unwrap();
    assert_eq!(header(&resp, DELIVERED_HEADER), Some(want.to_string()));
    assert_eq!(resp.output.content_length, Some(0));
    // Content-Range still describes what was delivered, even with no body.
    assert_eq!(
        resp.output.content_range,
        Some(format!("bytes {first}-{last}/{OBJECT_LEN}"))
    );

    let seen = std::fs::read(&path).unwrap();
    let expected = &body[first as usize..=last as usize];
    assert_eq!(&seen[WINDOW_OFFSET..WINDOW_OFFSET + want], expected);
    assert_eq!(
        header(&resp, CHECKSUM_HEADER),
        Some(expected_checksum(expected))
    );
    assert!(
        seen[..WINDOW_OFFSET].iter().all(|b| *b == SEGMENT_FILLER),
        "bytes before the window must be untouched"
    );
    assert!(
        seen[WINDOW_OFFSET + want..]
            .iter()
            .all(|b| *b == SEGMENT_FILLER),
        "bytes after the window must be untouched"
    );

    // Why the byte series exists rather than reading bandwidth off the chunk count: this
    // range starts and ends mid-chunk, so its edge windows are partial and `chunks ×
    // chunk size` over-states what moved. The byte series is exact; the multiplication is
    // not, and a ranged read is the shape a tensor-parallel loader issues.
    let chunks = h
        .metrics
        .delivery
        .chunks
        .with_label_values(&["backend"])
        .get();
    assert_eq!(chunk_bytes(&h, "backend"), want as u64);
    assert!(
        chunks * CHUNK_SIZE > want as u64,
        "the chunk count must over-state a partial-window delivery ({chunks} chunks vs {want} B)"
    );
}

/// ADR-0026 point 8: an over-quota target degrades to a body-delivered read, never
/// a failed one — and the client detects it by the absence of `x-pacer-delivered`.
///
/// ADR-0026 has **two** ceilings — a node-wide pinned-bytes budget and a per-request
/// one — and the point is that they degrade identically. One `#[rstest]` with a case
/// per ceiling, rather than the two halves this used to be in one body: the halves
/// were the same eleven assertions with one number moved, and running them as one test
/// meant the second half's failure was reported as the first half's test failing.
#[rstest]
#[case::node_wide_pinned_bytes(TIGHT_CEILING, ROOMY_CEILING)]
#[case::per_request_target(ROOMY_CEILING, TIGHT_CEILING)]
#[tokio::test]
async fn over_quota_target_falls_back_to_the_body(
    #[case] pinned_bytes_max: u64,
    #[case] max_target_bytes: u64,
) {
    let h = harness_with(true, pinned_bytes_max, max_target_bytes).await;
    let body = h.seed("quota.bin", OBJECT_LEN).await;
    h.segment("loader-quota", OBJECT_LEN);
    let target = format!("shm:/loader-quota;offset=0;len={OBJECT_LEN}");

    let resp = h.get("quota.bin", Some(&target), None).await.unwrap();
    assert_eq!(
        header(&resp, DELIVERED_HEADER),
        None,
        "no completion header"
    );
    assert_eq!(resp.output.content_length, Some(OBJECT_LEN as i64));
    assert_eq!(drain(resp.output.body).await, body, "the read still works");
    assert_eq!(
        h.metrics
            .delivery
            .rejects
            .with_label_values(&["quota"])
            .get(),
        1
    );
    assert_eq!(h.metrics.delivery.requests.get(), 0);
}

/// A descriptor the client got wrong is a 4xx, not a silent body: a client that
/// asked for delivery and would otherwise read its own uninitialized buffer must
/// be told. Over-quota is the ONLY degradable rejection.
#[tokio::test]
async fn a_bad_target_is_a_client_error() {
    let h = harness().await;
    h.seed("bad.bin", OBJECT_LEN).await;
    h.segment("loader-5", CHUNK_SIZE as usize);

    for target in [
        "shm:/loader-5",                                     // no len
        "cuda-ipc:0xdead;len=64",                            // ADR-0027's retired scheme
        "shm:/../escape;len=64",                             // traversal
        "shm:/absent;len=64",                                // no such segment
        &format!("shm:/loader-5;offset=0;len={OBJECT_LEN}"), // segment too small
        // A window that parses and maps, but is smaller than the read.
        "shm:/loader-5;offset=0;len=4096",
    ] {
        let err = h
            .get("bad.bin", Some(target), None)
            .await
            .expect_err(&format!("{target} must be rejected"));
        assert_eq!(
            *err.code(),
            s3s::S3ErrorCode::InvalidRequest,
            "{target}: {err:?}"
        );
    }
    // Nothing was pinned by any of the failures.
    assert_eq!(h.quota.in_use(), 0);
    assert_eq!(h.metrics.delivery.requests.get(), 0);
}

// ---- ADR-0030's pre-flight endpoint exchange (crates/pacer-daemon/src/preflight.rs) ----

/// **Asking who holds an object must never move a byte of it**, and this is the test that makes
/// that structural rather than a comment: the key does not exist in the backend at all, and the
/// pre-flight still answers.
///
/// A `HeadObject` would have turned this into a `NoSuchKey`; a cache read would have counted a
/// miss; a chunk fetch would have moved bytes. None of them happen, because the answer needs no
/// object length — the chunk indices come from the marker's own offset and the concurrency bound.
/// That is the whole reason the marker is checked before anything else in `get_object`.
#[tokio::test]
async fn an_endpoint_query_moves_no_bytes() {
    let h = harness().await;
    // Deliberately never seeded.
    let body = h.preflight_document("never-uploaded.bin", "offset=0").await;
    assert!(body.contains("\"nodes\":[]"), "{body}");

    assert_eq!(h.metrics.cache_misses.get(), 0, "no header was resolved");
    assert_eq!(h.metrics.cache_hits.get(), 0);
    assert_eq!(h.metrics.fills_completed.get(), 0);
    assert_eq!(h.metrics.bytes_from_cache.get(), 0);
    assert_eq!(h.metrics.bytes_filled.get(), 0);
    assert_eq!(chunk_bytes(&h, "backend"), 0);
    // And it is not counted as a read: that counter is the denominator of "what share of reads
    // took the accelerated path", which a pre-flight would deflate.
    assert_eq!(
        h.metrics.ops_total.with_label_values(&["get_object"]).get(),
        0
    );
    assert_eq!(h.metrics.delivery.requests.get(), 0);
    assert_eq!(h.quota.in_use(), 0);
}

/// On a single-node daemon with delivery on there is no RDMA plane, so the answer names **no**
/// endpoints — "nothing to prime", a defined answer rather than a failure. The client proceeds
/// and announce covers whatever writes.
#[tokio::test]
async fn a_node_with_no_rdma_plane_answers_an_empty_endpoint_list() {
    let h = harness().await;
    h.seed("preflight.bin", OBJECT_LEN).await;

    let body = h.preflight_document("preflight.bin", "offset=0").await;
    assert!(
        body.contains(&format!("\"version\":{ENDPOINTS_VERSION}")),
        "{body}"
    );
    assert!(body.contains("\"plane\":\"none\""), "{body}");
    assert!(body.contains("\"nodes\":[]"), "{body}");
    assert_eq!(
        h.metrics
            .delivery
            .preflight
            .with_label_values(&["no_plane"])
            .get(),
        1
    );
    assert_eq!(h.metrics.delivery.preflight_nodes.get(), 0);
}

/// The answer is legible as "not your object": marked, JSON, and carrying none of the object
/// metadata — no ETag, no Last-Modified, no `Content-Range` — because this path resolved no
/// header and an invented value would be worse than an absent one.
#[tokio::test]
async fn an_endpoint_answer_is_not_mistakable_for_object_bytes() {
    let h = harness().await;
    h.seed("preflight-shape.bin", OBJECT_LEN).await;

    let resp = h.preflight("preflight-shape.bin", "offset=0").await;
    assert_eq!(
        header(&resp, ENDPOINTS_HEADER),
        Some(ENDPOINTS_VERSION.to_string())
    );
    assert_eq!(
        resp.output.content_type.as_deref(),
        Some("application/json")
    );
    assert!(resp.output.e_tag.is_none());
    assert!(resp.output.last_modified.is_none());
    assert!(
        resp.output.content_range.is_none(),
        "no Content-Range, which is what distinguishes this 200 from the 206 the same request \
         would get without the marker"
    );
    // The body is the document, and `Content-Length` describes it rather than the object.
    let body = drain_string(resp.output.body).await;
    assert_eq!(resp.output.content_length, Some(body.len() as i64));
    assert_ne!(body.len(), OBJECT_LEN, "this is not the object");
}

/// **The compatibility gate, from the other side of `stock_get_is_untouched`.** The same key, the
/// same range, without the marker: the object's bytes, byte for byte, and none of the extension's
/// response headers.
///
/// `stock_get_is_untouched` proves a client that sends nothing is unaffected. This proves the
/// marker is the *only* thing that diverts a read — so a loader that primes and then reads gets
/// the object from the second call, and a loader that never primes is on exactly the old path.
#[tokio::test]
async fn the_same_get_without_the_marker_returns_the_object() {
    let h = harness().await;
    let body = h.seed("marker-or-not.bin", OBJECT_LEN).await;

    // With the marker: a document.
    let document = h.preflight_document("marker-or-not.bin", "offset=0").await;
    assert!(document.starts_with("{\"version\":"), "{document}");

    // Without it, same key: the object.
    let resp = h.get("marker-or-not.bin", None, None).await.unwrap();
    let (marked, delivered, checksum) = (
        header(&resp, ENDPOINTS_HEADER),
        header(&resp, DELIVERED_HEADER),
        header(&resp, CHECKSUM_HEADER),
    );
    assert_eq!(resp.output.content_length, Some(OBJECT_LEN as i64));
    assert_eq!(drain(resp.output.body).await, body);
    assert_eq!(marked, None, "an ordinary read must not be marked");
    assert_eq!(delivered, None);
    assert_eq!(checksum, None);
    assert_eq!(h.metrics.delivery.requests.get(), 0);
    assert_eq!(h.quota.in_use(), 0);

    // And a ranged read of the same key still answers 206-shaped, not a document.
    let ranged = h
        .get(
            "marker-or-not.bin",
            None,
            Some(dto::Range::Int {
                first: 0,
                last: Some(0),
            }),
        )
        .await
        .unwrap();
    assert_eq!(header(&ranged, ENDPOINTS_HEADER), None);
    assert_eq!(
        ranged.output.content_range,
        Some(format!("bytes 0-0/{OBJECT_LEN}"))
    );
    assert_eq!(drain(ranged.output.body).await, body.slice(0..1));
}

/// With delivery off the marker is ignored **exactly as a target descriptor is** — the GET is
/// served as an ordinary read and nothing is counted.
///
/// The consequence is deliberate and is why a pre-flight sends `Range: bytes=0-0`: a disabled
/// daemon is indistinguishable from one that predates this, and both cost the client one byte
/// rather than a checkpoint. Bounding the fallback is the shim's job; refusing to parse is this
/// daemon's.
#[tokio::test]
async fn a_disabled_daemon_ignores_the_marker_like_any_other_delivery_header() {
    let h = harness_with(false, ROOMY_CEILING, ROOMY_CEILING).await;
    let body = h.seed("preflight-off.bin", OBJECT_LEN).await;

    let mut headers = hyper::HeaderMap::new();
    headers.insert(
        hyper::header::HeaderName::from_static(GET_ENDPOINTS_HEADER),
        // Deliberately a value that WOULD be refused if parsed.
        hyper::header::HeaderValue::from_static("offset=wat"),
    );
    let resp = h
        .get_with_headers(
            "preflight-off.bin",
            Some(dto::Range::Int {
                first: 0,
                last: Some(0),
            }),
            headers,
        )
        .await
        .expect("a disabled daemon must serve the read, not refuse it");
    assert_eq!(
        header(&resp, ENDPOINTS_HEADER),
        None,
        "unmarked, which is what tells the client to skip priming"
    );
    // One byte, because the shim's `Range: bytes=0-0` is what bounds this fallback.
    assert_eq!(resp.output.content_length, Some(1));
    assert_eq!(drain(resp.output.body).await, body.slice(0..1));
    for outcome in ["served", "no_plane", "malformed"] {
        assert_eq!(
            h.metrics
                .delivery
                .preflight
                .with_label_values(&[outcome])
                .get(),
            0,
            "a disabled daemon must not even parse the marker ({outcome})"
        );
    }
}

/// A marker value the shim built wrong is a 4xx naming the field, not an answer for the wrong
/// windows — the same treatment a malformed target descriptor gets, and counted separately so a
/// rising rate reads as the client-side bug it is.
#[tokio::test]
async fn a_malformed_marker_is_a_client_error() {
    let h = harness().await;
    h.seed("preflight-bad.bin", OBJECT_LEN).await;

    let bad = [
        "offset",
        "offset=wat",
        "offset=-1",
        "first=0",
        "offset=0;stride=4",
    ];
    for ask in bad {
        let mut headers = hyper::HeaderMap::new();
        headers.insert(
            hyper::header::HeaderName::from_static(GET_ENDPOINTS_HEADER),
            hyper::header::HeaderValue::from_str(ask).unwrap(),
        );
        let err = h
            .get_with_headers("preflight-bad.bin", None, headers)
            .await
            .expect_err(&format!("{ask:?} must be refused"));
        assert_eq!(
            *err.code(),
            s3s::S3ErrorCode::InvalidRequest,
            "{ask}: {err:?}"
        );
    }
    assert_eq!(
        h.metrics
            .delivery
            .preflight
            .with_label_values(&["malformed"])
            .get(),
        bad.len() as u64
    );
    // Refused before anything was read, exactly like a well-formed one.
    assert_eq!(h.metrics.cache_misses.get(), 0);
    assert_eq!(h.metrics.cache_hits.get(), 0);
}

/// The pre-flight leaves the read path alone: after answering, the same object still reads
/// byte-for-byte as a body, and no delivery was recorded.
///
/// Worth asserting beside `stock_get_is_untouched` because the pre-flight now shares the read's
/// own entry point — a short-circuit that consumed the request, or left a counter moved, would
/// show up here and nowhere else.
#[tokio::test]
async fn a_preflight_does_not_disturb_the_read_that_follows() {
    let h = harness().await;
    let body = h.seed("preflight-then-read.bin", OBJECT_LEN).await;
    h.preflight_document("preflight-then-read.bin", "offset=0")
        .await;

    let resp = h.get("preflight-then-read.bin", None, None).await.unwrap();
    let delivered = header(&resp, DELIVERED_HEADER);
    assert_eq!(resp.output.content_length, Some(OBJECT_LEN as i64));
    assert_eq!(drain(resp.output.body).await, body);
    assert_eq!(delivered, None);
    assert_eq!(h.metrics.delivery.requests.get(), 0);
    assert_eq!(h.quota.in_use(), 0);
    // The read that followed is the only one counted as a read.
    assert_eq!(
        h.metrics.ops_total.with_label_values(&["get_object"]).get(),
        1
    );
}

/// A shard read asks about its own range, and a whole-object read about the burst — never about
/// every holder of the object. `preflight::tests` pins the arithmetic; this pins that the value
/// travels from the header at all, which the marker's grammar exists for.
#[tokio::test]
async fn the_ask_carries_the_range_the_read_will_touch() {
    let h = harness().await;
    h.seed("preflight-range.bin", OBJECT_LEN).await;

    for ask in [
        "",
        "offset=0",
        &format!("offset={CHUNK_SIZE};length={CHUNK_SIZE}"),
        &format!("length={OBJECT_LEN}"),
    ] {
        let body = h.preflight_document("preflight-range.bin", ask).await;
        assert!(
            body.contains("\"nodes\":[]"),
            "{ask:?} must be answerable: {body}"
        );
    }
    assert_eq!(
        h.metrics
            .delivery
            .preflight
            .with_label_values(&["no_plane"])
            .get(),
        4
    );
}

/// With the knob off, the header is ignored entirely: no mapping, no pinning, no
/// 4xx for a descriptor the daemon never looked at.
#[tokio::test]
async fn disabled_delivery_ignores_the_header() {
    let h = harness_with(false, ROOMY_CEILING, ROOMY_CEILING).await;
    let body = h.seed("off.bin", OBJECT_LEN).await;

    // Deliberately a target that WOULD be rejected as malformed if parsed.
    let resp = h.get("off.bin", Some("shm:/nope"), None).await.unwrap();
    let delivered = header(&resp, DELIVERED_HEADER);
    assert_eq!(resp.output.content_length, Some(OBJECT_LEN as i64));
    assert_eq!(drain(resp.output.body).await, body);
    assert_eq!(delivered, None, "the header must be ignored, not honoured");
    assert_eq!(h.metrics.delivery.requests.get(), 0);
    assert_eq!(
        h.metrics
            .delivery
            .rejects
            .with_label_values(&["malformed"])
            .get(),
        0,
        "a disabled daemon must not even parse the descriptor"
    );
}
