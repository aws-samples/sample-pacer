//! ADR-0041 step 3 (`planning/29-auth-modes.md` § 4): strip-and-hold, the
//! authorization probe, and the held-signature chunk fill — a full
//! `requester`-mode daemon over real TCP against an in-process fake S3.
//!
//! The fake upstream is address-addressed (path-style: `/bucket/key`) and
//! answers three shapes: the real object (any `Range`, including the
//! `bytes=0-0` probe), a key that is always denied, and everything else
//! `404`. What is under test is entirely in `RequesterFront`/`RequesterS3` —
//! whether the probe gates the read, whether a denial propagates instead of
//! serving cached bytes, and whether a signed `Range` bypasses the cache
//! instead of being rewritten.
//!
//! client ⇢ TCP ⇢ listen::serve_s3_on(RequesterFront[RequesterS3]) ⇢ TCP ⇢ fake upstream

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::Full;
use hyper::body::Incoming;
use pacer_cache::chunk::ChunkConfig;
use pacer_cache::tier::ChunkTier;
use pacer_daemon::authz::{Forwarder, RequesterFront};
use pacer_daemon::listen::ListenLimits;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::proxy::{PacerProxy, RequesterS3};
use pacer_daemon::shutdown::Shutdown;
use pacer_daemon::warm::{WARMED_HEADER, WARM_HEADER};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::common::{build_cache, fs_backend_service, sdk_client_for, CacheSpec};

/// Path-style: bucket `bkt`, key `obj` — small enough to span three chunks
/// at [`CHUNK_SIZE`], which is the point (exercises the covering-chunk
/// pipeline, not just a single read).
pub(super) const OBJECT_PATH: &str = "/bkt/obj";
pub(super) const OBJECT_BYTES: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCD"; // 40 bytes
const OBJECT_ETAG: &str = "fixture-etag";
pub(super) const CHUNK_SIZE: u64 = 16;
/// User metadata the fake S3 attaches to every object response: a pass-through must
/// deliver it, since the cache path has no way to reproduce it.
const OBJECT_META_HEADER: &str = "x-amz-meta-fixture";
const OBJECT_META_VALUE: &str = "from-s3";
/// A key the fake upstream always denies, regardless of `Range` — proves a
/// probe denial propagates instead of ever reaching the cache.
const DENIED_PATH: &str = "/bkt/denied";

/// The fake S3: serves [`OBJECT_PATH`] at whatever `Range` it is asked for
/// (including the probe's `bytes=0-0`), denies [`DENIED_PATH`] outright, and
/// 404s everything else.
async fn fake_s3(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    fake_s3_serving(req, OBJECT_BYTES, OBJECT_ETAG).await
}

/// [`fake_s3`] with the object's bytes and ETag supplied, so a test can replace
/// the object between two reads.
async fn fake_s3_serving(
    req: Request<Incoming>,
    object: &[u8],
    e_tag: &str,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_owned();
    if path == DENIED_PATH {
        return Ok(status_only(StatusCode::FORBIDDEN));
    }
    if path != OBJECT_PATH {
        return Ok(status_only(StatusCode::NOT_FOUND));
    }
    let range = req
        .headers()
        .get(http::header::RANGE)
        .and_then(|v| v.to_str().ok());
    let len = object.len() as u64;
    let (start, end) = range.map_or((0, len), |r| parse_closed_range(r, len));
    let slice = Bytes::copy_from_slice(&object[start as usize..end as usize]);
    let status = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let mut resp = Response::builder()
        .status(status)
        .header(http::header::CONTENT_LENGTH, slice.len())
        .header(http::header::ETAG, format!("\"{e_tag}\""))
        .header(http::header::CONTENT_TYPE, "application/octet-stream")
        .header(OBJECT_META_HEADER, OBJECT_META_VALUE);
    if range.is_some() {
        resp = resp.header(
            http::header::CONTENT_RANGE,
            format!("bytes {start}-{}/{len}", end - 1),
        );
    }
    Ok(resp.body(Full::new(slice)).expect("a well-formed response"))
}

fn status_only(status: StatusCode) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::new()));
    *resp.status_mut() = status;
    resp
}

/// Parse a closed `bytes=start-end` range (both this daemon's probe and its
/// chunk fill only ever send this shape), clamping `end` to `len - 1`.
fn parse_closed_range(header: &str, len: u64) -> (u64, u64) {
    let spec = header.trim_start_matches("bytes=");
    let (start, end) = spec.split_once('-').expect("a closed byte range");
    let start: u64 = start.parse().expect("a numeric range start");
    let end: u64 = end.parse().expect("a numeric range end");
    (start, (end + 1).min(len))
}

/// Spawns the fake upstream and returns a counter of every request it
/// received — the one thing that tells strip-and-hold's extra probe
/// request apart from the raw-forward path's single pass-through, since
/// both ultimately serve the same bytes for a ranged read.
pub(super) async fn spawn_fake_s3() -> (SocketAddr, Shutdown, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("bound port has an address");
    let shutdown = Shutdown::new();
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&requests);
    let service = hyper::service::service_fn(move |req| {
        let counted = Arc::clone(&counted);
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            fake_s3(req).await
        }
    });
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        service,
        ListenLimits::default(),
        Metrics::new().expect("a fresh registry"),
        shutdown.signal(),
    ));
    (addr, shutdown, requests)
}

/// The object a [`spawn_replaceable_s3`] upstream serves: its bytes and ETag.
type Replaceable = Arc<std::sync::Mutex<(Vec<u8>, String)>>;

/// [`spawn_fake_s3`] serving whatever `object` holds at the moment each request
/// arrives, so a test can overwrite the object behind the daemon's back.
async fn spawn_replaceable_s3(object: Replaceable) -> (SocketAddr, Shutdown, Arc<AtomicUsize>) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("bound port has an address");
    let shutdown = Shutdown::new();
    let requests = Arc::new(AtomicUsize::new(0));
    let counted = Arc::clone(&requests);
    let service = hyper::service::service_fn(move |req| {
        let counted = Arc::clone(&counted);
        let (bytes, e_tag) = object.lock().expect("fixture lock").clone();
        async move {
            counted.fetch_add(1, Ordering::SeqCst);
            fake_s3_serving(req, &bytes, &e_tag).await
        }
    });
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        service,
        ListenLimits::default(),
        Metrics::new().expect("a fresh registry"),
        shutdown.signal(),
    ));
    (addr, shutdown, requests)
}

/// A `requester`-mode daemon: `RequesterS3` (only `get_object` reachable)
/// behind `RequesterFront`, with the domain list set to `upstream` itself —
/// so path-style addressing (`/bucket/key`) resolves exactly as it does when
/// a real deployment's `PACER_AUTH_S3_DOMAINS` names the real S3 host.
async fn spawn_requester_daemon(upstream: SocketAddr) -> (SocketAddr, Shutdown) {
    spawn_requester_daemon_with(upstream, 0).await
}

/// [`spawn_requester_daemon`] with a cacheable-size floor, so a test can put the
/// fixture object below it.
async fn spawn_requester_daemon_with(
    upstream: SocketAddr,
    min_object_size: u64,
) -> (SocketAddr, Shutdown) {
    let backend_dir = tempfile::tempdir().expect("a temp dir for the throwaway backend");
    let (service, creds) = fs_backend_service(backend_dir.path());
    let backend = sdk_client_for(service, creds);
    let cache_dir = tempfile::tempdir().expect("a temp dir for the cache");
    let cache = build_cache(cache_dir.path(), CacheSpec::default()).await;
    let tier = ChunkTier::foyer(cache, Default::default());
    let metrics = Metrics::new().expect("a fresh registry");
    // Every other test passes 0: the fixture is a tiny object, and the shipped
    // 4 MiB floor would send it down the pass-through instead of the cache.
    let proxy = PacerProxy::new(
        backend,
        tier,
        metrics.clone(),
        min_object_size,
        None,
        ChunkConfig::new(CHUNK_SIZE),
        4,
    );
    let forwarder = Arc::new(Forwarder::new());
    let domains = Arc::new(
        s3s::host::MultiDomain::new([upstream.to_string()])
            .expect("the fake upstream's own address is a valid domain"),
    );
    let mut b =
        s3s::service::S3ServiceBuilder::new(RequesterS3::new(proxy, Arc::clone(&forwarder)));
    b.set_host(s3s::host::MultiDomain::new([upstream.to_string()]).expect("a valid domain"));
    let inner = b.build();
    let front = RequesterFront::new(inner, domains, forwarder, metrics.clone());

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("bound port has an address");
    let shutdown = Shutdown::new();
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        front,
        ListenLimits::default(),
        metrics,
        shutdown.signal(),
    ));
    (addr, shutdown)
}

/// A bare-minimum SigV4-shaped `Authorization` header. `signed` names the
/// headers this test cares about signing — real SigV4 signs more, but
/// nothing here re-verifies the signature, only whether `range` is in the
/// list.
pub(super) fn authorization(signed: &str) -> String {
    format!(
        "AWS4-HMAC-SHA256 Credential=test/20260924/us-east-2/s3/aws4_request, \
         SignedHeaders={signed}, Signature=deadbeef"
    )
}

pub(super) async fn read_response(stream: &mut TcpStream) -> (u16, Vec<u8>) {
    let (status, _headers, body) = read_response_with_headers(stream).await;
    (status, body)
}

/// [`read_response`], plus the raw header block — for a test that has to see a response
/// header the SDK's modelled output would drop (ADR-0048's `x-pacer-warmed`).
pub(super) async fn read_response_with_headers(stream: &mut TcpStream) -> (u16, String, Vec<u8>) {
    let mut buf = Vec::new();
    let header_end = loop {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "connection closed before headers completed");
        buf.extend_from_slice(&chunk[..n]);
        if let Some(pos) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let head = String::from_utf8_lossy(&buf[..header_end]).into_owned();
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("a status line");
    let content_length: usize = head
        .lines()
        .find(|line| line.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|line| line.split_once(':'))
        .and_then(|(_, v)| v.trim().parse().ok())
        .unwrap_or(0);
    let mut body = buf[header_end..].to_vec();
    while body.len() < content_length {
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "connection closed before body completed");
        body.extend_from_slice(&chunk[..n]);
    }
    (status, head, body)
}

/// The value of header `name` in a raw `head` block, case-insensitively — `None` when it
/// is absent, which is the completion signal itself for [`WARMED_HEADER`].
pub(super) fn header_value<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines().find_map(|line| {
        let (k, v) = line.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

/// A whole-object GET, unsigned `range`, authorized by the fake upstream:
/// the probe allows, the header comes from it (not a `HeadObject`), and the
/// three covering chunks are each filled with the caller's own signature —
/// never `self.backend`, which points at an unrelated throwaway s3s-fs.
#[tokio::test]
async fn authorized_get_is_served_chunk_by_chunk_from_the_held_signature() {
    let (upstream, _upstream_shutdown, requests) = spawn_fake_s3().await;
    let (daemon, _daemon_shutdown) = spawn_requester_daemon(upstream).await;
    let mut client = TcpStream::connect(daemon)
        .await
        .expect("connect to the daemon");
    let request = format!(
        "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: {auth}\r\n\
         Connection: close\r\n\
         \r\n",
        auth = authorization("host;x-amz-date"),
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, body) = read_response(&mut client).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    assert_eq!(body, OBJECT_BYTES);
    // One probe (bytes=0-0) plus one ranged GET per covering chunk: 40 bytes
    // at CHUNK_SIZE=16 is chunks [0,16) [16,32) [32,40) — three of them.
    assert_eq!(
        requests.load(Ordering::SeqCst),
        4,
        "expected the probe plus exactly 3 chunk reads"
    );
}

/// The fake upstream denies [`DENIED_PATH`] at the probe, before any chunk
/// is ever read — the client sees the denial, not cached bytes from some
/// other request, because there is nothing cached to leak: the probe runs
/// before the cache is ever touched.
#[tokio::test]
async fn a_probe_denial_propagates_and_never_reads_a_chunk() {
    let (upstream, _upstream_shutdown, requests) = spawn_fake_s3().await;
    let (daemon, _daemon_shutdown) = spawn_requester_daemon(upstream).await;
    let mut client = TcpStream::connect(daemon)
        .await
        .expect("connect to the daemon");
    let request = format!(
        "GET http://{upstream}{DENIED_PATH} HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: {auth}\r\n\
         Connection: close\r\n\
         \r\n",
        auth = authorization("host;x-amz-date"),
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, body) = read_response(&mut client).await;
    assert_eq!(status, 403, "{}", String::from_utf8_lossy(&body));
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "a denial must stop at the probe, never reach a chunk read"
    );
}

/// A caller who signs their own `Range` cannot have it rewritten — the
/// SigV4 asymmetry ADR-0041's whole design rests on — so the front door
/// forwards it verbatim instead of strip-and-holding it. Content alone
/// cannot tell the two paths apart (both would serve the same bytes for
/// this range), so the proof is the upstream's request count: strip-and-
/// hold would cost a probe plus chunk reads, a raw forward costs exactly
/// the one request the client made.
#[tokio::test]
async fn a_signed_range_bypasses_the_cache_and_is_forwarded_verbatim() {
    let (upstream, _upstream_shutdown, requests) = spawn_fake_s3().await;
    let (daemon, _daemon_shutdown) = spawn_requester_daemon(upstream).await;
    let mut client = TcpStream::connect(daemon)
        .await
        .expect("connect to the daemon");
    let request = format!(
        "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Range: bytes=2-5\r\n\
         Authorization: {auth}\r\n\
         Connection: close\r\n\
         \r\n",
        auth = authorization("host;range;x-amz-date"),
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, body) = read_response(&mut client).await;
    assert_eq!(status, 206, "{}", String::from_utf8_lossy(&body));
    assert_eq!(body, &OBJECT_BYTES[2..=5]);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "a signed range must bypass to a single raw forward, never a probe"
    );
}

/// The version witness (ADR-0041, planning/30 § 3.4). The object is replaced at
/// S3 behind the daemon's back — same length, different bytes, new ETag — as an
/// out-of-band overwrite would do. The second GET's probe reports the new ETag, so
/// the chunks cached under the old one must be re-read, never served: without the
/// witness this read returned the first version's bytes under the second
/// version's header.
#[tokio::test]
async fn a_replaced_object_is_never_served_from_the_old_versions_chunks() {
    let first = OBJECT_BYTES.to_vec();
    let second: Vec<u8> = OBJECT_BYTES.iter().rev().copied().collect();
    let object: Replaceable = Arc::new(std::sync::Mutex::new((first.clone(), "v1".to_owned())));
    let (upstream, _upstream_shutdown, requests) = spawn_replaceable_s3(Arc::clone(&object)).await;
    let (daemon, _daemon_shutdown) = spawn_requester_daemon(upstream).await;
    let get = || async {
        let mut client = TcpStream::connect(daemon)
            .await
            .expect("connect to the daemon");
        let request = format!(
            "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
             Host: {upstream}\r\n\
             Authorization: {auth}\r\n\
             Connection: close\r\n\
             \r\n",
            auth = authorization("host;x-amz-date"),
        );
        client
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        read_response(&mut client).await
    };

    let (status, body) = get().await;
    assert_eq!(status, 200);
    assert_eq!(body, first);
    let (status, body) = get().await;
    assert_eq!(
        (status, body),
        (200, first),
        "control: an unchanged object is a hit"
    );
    let after_hit = requests.load(Ordering::SeqCst);
    assert_eq!(
        after_hit, 5,
        "first read: probe + 3 chunks; the control read: its probe alone"
    );

    *object.lock().expect("fixture lock") = (second.clone(), "v2".to_owned());
    let (status, body) = get().await;
    assert_eq!(status, 200);
    assert_eq!(body, second, "a stale chunk must never be served");
    assert_eq!(
        requests.load(Ordering::SeqCst) - after_hit,
        4,
        "the probe plus a re-read of every one of the three chunks"
    );
}

/// An object below the cacheable-size floor — node mode's small-object bypass
/// (ADR-0002) — is served by passing the caller's own request through, not refused:
/// S3's body and headers (user metadata included) reach the client, every read costs
/// the probe plus the pass-through, and nothing is cached.
#[tokio::test]
async fn an_object_below_the_size_floor_is_passed_through_with_its_metadata() {
    let (upstream, _upstream_shutdown, requests) = spawn_fake_s3().await;
    let floor = OBJECT_BYTES.len() as u64 + 1;
    let (daemon, _daemon_shutdown) = spawn_requester_daemon_with(upstream, floor).await;
    for read in 1..=2 {
        let mut client = TcpStream::connect(daemon)
            .await
            .expect("connect to the daemon");
        let request = format!(
            "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
             Host: {upstream}\r\n\
             Authorization: {auth}\r\n\
             Connection: close\r\n\
             \r\n",
            auth = authorization("host;x-amz-date"),
        );
        client
            .write_all(request.as_bytes())
            .await
            .expect("write request");
        let mut raw = Vec::new();
        client
            .read_to_end(&mut raw)
            .await
            .expect("read the response");
        let text = String::from_utf8_lossy(&raw);
        assert!(text.starts_with("HTTP/1.1 200"), "read {read}: {text}");
        assert!(
            text.to_ascii_lowercase()
                .contains(&format!("{OBJECT_META_HEADER}: {OBJECT_META_VALUE}")),
            "read {read}: S3's user metadata must reach the client: {text}"
        );
        assert!(
            text.ends_with(std::str::from_utf8(OBJECT_BYTES).unwrap()),
            "read {read}: {text}"
        );
        assert_eq!(
            requests.load(Ordering::SeqCst),
            2 * read,
            "read {read}: the probe and the pass-through, never a cache hit"
        );
    }
}

/// ADR-0048 under `auth.mode: requester`: a warm-only GET is authorized exactly like the
/// read it stands for — the probe still runs on the caller's own signature — and it
/// answers header-only. A later ordinary read then costs only the probe: every chunk is a
/// cache hit, so nothing crosses to the fake upstream a second time.
#[tokio::test]
async fn a_warm_only_get_is_authorized_and_answers_header_only() {
    let (upstream, _upstream_shutdown, requests) = spawn_fake_s3().await;
    let (daemon, _daemon_shutdown) = spawn_requester_daemon(upstream).await;
    let auth = authorization("host;x-amz-date");
    let mut client = TcpStream::connect(daemon)
        .await
        .expect("connect to the daemon");
    let request = format!(
        "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: {auth}\r\n\
         {WARM_HEADER}: 1\r\n\
         Connection: close\r\n\
         \r\n",
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, head, body) = read_response_with_headers(&mut client).await;
    assert_eq!(status, 200, "{head}");
    assert!(body.is_empty(), "a warm answers header-only: {head}");
    assert_eq!(
        header_value(&head, WARMED_HEADER),
        Some(OBJECT_BYTES.len().to_string().as_str()),
        "{head}"
    );
    assert_eq!(
        requests.load(Ordering::SeqCst),
        4,
        "the probe plus one read per covering chunk, exactly like an ordinary GET"
    );

    let mut client = TcpStream::connect(daemon)
        .await
        .expect("connect to the daemon");
    let request = format!(
        "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: {auth}\r\n\
         Connection: close\r\n\
         \r\n",
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, body) = read_response(&mut client).await;
    assert_eq!(status, 200);
    assert_eq!(body, OBJECT_BYTES);
    assert_eq!(
        requests.load(Ordering::SeqCst),
        5,
        "the probe, and nothing else: every chunk was warmed"
    );
}
