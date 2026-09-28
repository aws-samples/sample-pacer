//! ADR-0041 / planning/30 § 7 T4: requester mode's write tee against an in-process S3 that
//! stores what it is sent. Every assertion that a read "is a hit" is the fake's own count of
//! GETs: a hit costs exactly one — the read's authorization request — and a miss adds one
//! per chunk.
//!
//! client ⇢ TCP ⇢ RequesterFront(+WriteTee) ⇢ TCP ⇢ fake S3 (PUT / multipart / GET)

use std::collections::{BTreeMap, HashMap};
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use pacer_cache::chunk::ChunkConfig;
use pacer_cache::tier::ChunkTier;
use pacer_daemon::authz::{Forwarder, RequesterFront};
use pacer_daemon::listen::ListenLimits;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::populate::{AwsChunkedDecoder, WriteTee};
use pacer_daemon::proxy::{PacerProxy, RequesterS3};
use pacer_daemon::shutdown::Shutdown;
use pacer_daemon::staging::StagingArea;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use super::authz_requester_get::{authorization, read_response, CHUNK_SIZE};
use crate::common::{self, build_cache, fs_backend_service, sdk_client_for, CacheSpec};

/// 40 bytes on a 16-byte grid: chunks `[0,16) [16,32) [32,40)`.
const OBJECT: &[u8] = b"0123456789abcdefghijklmnopqrstuvwxyzABCD";
const PATH: &str = "/bkt/obj";
/// Windows the tee may hold at once; ample for these objects.
const WINDOWS_IN_FLIGHT: usize = 8;
const STAGING_BYTES: usize = 1 << 20;
const STAGING_TTL: Duration = Duration::from_secs(900);

/// What the fake S3 holds, and how many object GETs it has served.
#[derive(Default)]
struct Store {
    objects: HashMap<String, (Vec<u8>, String)>,
    uploads: HashMap<String, (String, BTreeMap<i32, Vec<u8>>)>,
    next: u64,
    gets: usize,
    /// Answer every Complete with `200 OK` and an `<Error>` body, as S3 may.
    fail_complete: bool,
}

type FakeS3 = Arc<Mutex<Store>>;

fn text(status: StatusCode, body: String) -> Response<Full<Bytes>> {
    let mut resp = Response::new(Full::new(Bytes::from(body)));
    *resp.status_mut() = status;
    resp
}

fn query_value<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .find_map(|kv| kv.strip_prefix(key)?.strip_prefix('='))
}

/// Decode an `aws-chunked` body the way S3 would, with the daemon's own decoder — the
/// daemon's tee and this fake must agree on the object's bytes for the test to mean
/// anything, and the decoder has its own unit tests.
fn object_bytes(headers: &http::HeaderMap, raw: Bytes) -> Vec<u8> {
    let chunked = headers
        .get("content-encoding")
        .is_some_and(|v| v.to_str().unwrap_or("").contains("aws-chunked"));
    if !chunked {
        return raw.to_vec();
    }
    let mut out = Vec::new();
    AwsChunkedDecoder::new()
        .feed(&raw, &mut out)
        .expect("a valid aws-chunked body");
    out.concat()
}

async fn fake_s3(
    store: FakeS3,
    req: Request<Incoming>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = req.into_parts();
    let (path, query) = (
        parts.uri.path().to_owned(),
        parts.uri.query().unwrap_or("").to_owned(),
    );
    let raw = body
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();
    let body = object_bytes(&parts.headers, raw);
    let mut s = store.lock().unwrap();
    s.next += 1;
    let n = s.next;
    Ok(
        match (parts.method.as_str(), query_value(&query, "uploadId")) {
            ("GET", _) => get(&mut s, &path, parts.headers.get(http::header::RANGE)),
            ("PUT", None) => {
                s.objects.insert(path, (body, format!("put{n}")));
                let mut resp = text(StatusCode::OK, String::new());
                resp.headers_mut()
                    .insert("etag", format!("\"put{n}\"").parse().unwrap());
                resp
            }
            ("PUT", Some(id)) => {
                let part: i32 = query_value(&query, "partNumber").unwrap().parse().unwrap();
                s.uploads.get_mut(id).unwrap().1.insert(part, body);
                text(StatusCode::OK, String::new())
            }
            ("POST", None) => {
                s.uploads.insert(format!("up{n}"), (path, BTreeMap::new()));
                text(StatusCode::OK, format!("<InitiateMultipartUploadResult><UploadId>up{n}</UploadId></InitiateMultipartUploadResult>"))
            }
            ("POST", Some(id)) => complete(&mut s, id, n),
            ("DELETE", Some(id)) => {
                s.uploads.remove(id);
                text(StatusCode::NO_CONTENT, String::new())
            }
            _ => text(StatusCode::NOT_IMPLEMENTED, String::new()),
        },
    )
}

fn complete(s: &mut Store, id: &str, n: u64) -> Response<Full<Bytes>> {
    if s.fail_complete {
        return text(
            StatusCode::OK,
            "<Error><Code>InternalError</Code></Error>".into(),
        );
    }
    let (path, parts) = s.uploads.remove(id).unwrap();
    let e_tag = format!("mpu{n}-{}", parts.len());
    let bytes = parts.into_values().flatten().collect();
    s.objects.insert(path, (bytes, e_tag.clone()));
    text(
        StatusCode::OK,
        format!("<CompleteMultipartUploadResult><ETag>&quot;{e_tag}&quot;</ETag></CompleteMultipartUploadResult>"),
    )
}

fn get(s: &mut Store, path: &str, range: Option<&http::HeaderValue>) -> Response<Full<Bytes>> {
    s.gets += 1;
    let Some((bytes, e_tag)) = s.objects.get(path) else {
        return text(StatusCode::NOT_FOUND, String::new());
    };
    let len = bytes.len();
    let (start, end) = range.and_then(|r| r.to_str().ok()).map_or((0, len), |r| {
        let (a, b) = r.trim_start_matches("bytes=").split_once('-').unwrap();
        (
            a.parse().unwrap(),
            (b.parse::<usize>().unwrap() + 1).min(len),
        )
    });
    let mut resp = Response::new(Full::new(Bytes::copy_from_slice(&bytes[start..end])));
    *resp.status_mut() = if range.is_some() {
        StatusCode::PARTIAL_CONTENT
    } else {
        StatusCode::OK
    };
    let h = resp.headers_mut();
    h.insert("etag", format!("\"{e_tag}\"").parse().unwrap());
    h.insert("content-length", (end - start).into());
    if range.is_some() {
        h.insert(
            "content-range",
            format!("bytes {start}-{}/{len}", end - 1).parse().unwrap(),
        );
    }
    resp
}

/// A fake S3, and a requester-mode daemon with the write tee in front of it.
struct Rig {
    upstream: SocketAddr,
    daemon: SocketAddr,
    store: FakeS3,
    metrics: Metrics,
    _guards: (Shutdown, Shutdown, tempfile::TempDir, tempfile::TempDir),
}

async fn serve<S, B>(service: S, metrics: Metrics) -> (SocketAddr, Shutdown)
where
    S: hyper::service::Service<Request<Incoming>, Response = Response<B>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("a bound port has an address");
    let shutdown = Shutdown::new();
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        service,
        ListenLimits::default(),
        metrics,
        shutdown.signal(),
    ));
    (addr, shutdown)
}

async fn rig() -> Rig {
    let store = FakeS3::default();
    let s = Arc::clone(&store);
    let fake = hyper::service::service_fn(move |req| fake_s3(Arc::clone(&s), req));
    let (upstream, up_shutdown) = serve(fake, Metrics::new().unwrap()).await;

    let backend_dir = tempfile::tempdir().expect("a temp dir for the throwaway backend");
    let (service, creds) = fs_backend_service(backend_dir.path());
    let cache_dir = tempfile::tempdir().expect("a temp dir for the cache");
    let tier = ChunkTier::foyer(
        build_cache(cache_dir.path(), CacheSpec::default()).await,
        Default::default(),
    );
    let metrics = Metrics::new().expect("a fresh registry");
    let chunk = ChunkConfig::new(CHUNK_SIZE);
    let proxy = PacerProxy::new(
        sdk_client_for(service, creds),
        tier.clone(),
        metrics.clone(),
        0,
        None,
        chunk,
        4,
    );
    let staging = Arc::new(StagingArea::new(STAGING_BYTES, STAGING_TTL));
    let tee = WriteTee::new(
        staging,
        None,
        tier,
        chunk,
        WINDOWS_IN_FLIGHT,
        STAGING_TTL,
        metrics.clone(),
    );
    let forwarder = Arc::new(Forwarder::new());
    let domains = || s3s::host::MultiDomain::new([upstream.to_string()]).expect("a valid domain");
    let mut b =
        s3s::service::S3ServiceBuilder::new(RequesterS3::new(proxy, Arc::clone(&forwarder)));
    b.set_host(domains());
    let front = RequesterFront::new(b.build(), Arc::new(domains()), forwarder, metrics.clone())
        .with_write_tee(tee);
    let (daemon, daemon_shutdown) = serve(front, metrics.clone()).await;
    Rig {
        upstream,
        daemon,
        store,
        metrics,
        _guards: (up_shutdown, daemon_shutdown, backend_dir, cache_dir),
    }
}

impl Rig {
    /// Send one request through the daemon and return its status and body.
    async fn send(&self, method: &str, target: &str, headers: &str, body: &[u8]) -> (u16, Vec<u8>) {
        let mut client = TcpStream::connect(self.daemon)
            .await
            .expect("connect to the daemon");
        let head = format!(
            "{method} http://{up}{target} HTTP/1.1\r\nHost: {up}\r\nAuthorization: {auth}\r\n\
             Content-Length: {len}\r\n{headers}Connection: close\r\n\r\n",
            up = self.upstream,
            auth = authorization("host;x-amz-date"),
            len = body.len(),
        );
        client.write_all(head.as_bytes()).await.expect("write head");
        client.write_all(body).await.expect("write body");
        read_response(&mut client).await
    }

    async fn get(&self) -> Vec<u8> {
        let (status, body) = self.send("GET", PATH, "", b"").await;
        assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
        body
    }

    fn gets(&self) -> usize {
        self.store.lock().unwrap().gets
    }

    fn windows(&self, outcome: &str) -> u64 {
        self.metrics
            .authz
            .populate_windows
            .with_label_values(&[outcome])
            .get()
    }

    /// Wait for the spawned commit or discard after a write to land.
    async fn settled(&self, outcome: &'static str, windows: u64) {
        common::poll_until(&format!("{windows} window(s) {outcome}"), || async move {
            self.windows(outcome) >= windows
        })
        .await;
    }

    /// Read the object and return how many GETs S3 served for it.
    async fn read_cost(&self, expected: &[u8]) -> usize {
        let before = self.gets();
        assert_eq!(self.get().await, expected);
        self.gets() - before
    }

    /// Upload `OBJECT` as a multipart upload with `part` bytes per part.
    async fn multipart(&self, part: usize) -> (u16, Vec<u8>) {
        let (status, body) = self.send("POST", &format!("{PATH}?uploads"), "", b"").await;
        assert_eq!(status, 200);
        let text = String::from_utf8(body).unwrap();
        let id = text
            .split("<UploadId>")
            .nth(1)
            .unwrap()
            .split('<')
            .next()
            .unwrap()
            .to_owned();
        let mut list = String::new();
        for (i, bytes) in OBJECT.chunks(part).enumerate() {
            let n = i + 1;
            let (status, _) = self
                .send(
                    "PUT",
                    &format!("{PATH}?partNumber={n}&uploadId={id}"),
                    "",
                    bytes,
                )
                .await;
            assert_eq!(status, 200);
            list.push_str(&format!("<Part><PartNumber>{n}</PartNumber></Part>"));
        }
        let doc = format!("<CompleteMultipartUpload>{list}</CompleteMultipartUpload>");
        self.send("POST", &format!("{PATH}?uploadId={id}"), "", doc.as_bytes())
            .await
    }
}

#[tokio::test]
async fn a_put_populates_the_cache_and_the_next_read_is_a_hit() {
    let r = rig().await;
    let (status, _) = r.send("PUT", PATH, "", OBJECT).await;
    assert_eq!(status, 200);
    r.settled("committed", 3).await;
    assert_eq!(
        r.read_cost(OBJECT).await,
        1,
        "the authorization request alone"
    );
}

#[tokio::test]
async fn an_aws_chunked_put_populates_the_decoded_object() {
    let r = rig().await;
    let body = b"10;chunk-signature=a\r\n0123456789abcdef\r\n18;chunk-signature=b\r\nghijklmnopqrstuvwxyzABCD\r\n\
                 0;chunk-signature=c\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
    let headers = "Content-Encoding: aws-chunked\r\nx-amz-decoded-content-length: 40\r\n\
                   x-amz-content-sha256: STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER\r\n";
    let (status, _) = r.send("PUT", PATH, headers, body).await;
    assert_eq!(status, 200);
    r.settled("committed", 3).await;
    assert_eq!(
        r.read_cost(OBJECT).await,
        1,
        "the framing must never reach the cache"
    );
}

#[tokio::test]
async fn a_multipart_upload_on_the_grid_populates_every_full_window() {
    let r = rig().await;
    let (status, _) = r.multipart(CHUNK_SIZE as usize).await;
    assert_eq!(status, 200);
    // Two full windows; a part's short tail is never staged (it is a chunk only if the part
    // is last, which nothing says until Complete), so the final chunk is one miss.
    r.settled("committed", 2).await;
    assert_eq!(
        r.read_cost(OBJECT).await,
        2,
        "the authorization request and the last chunk"
    );
}

#[tokio::test]
async fn parts_off_the_grid_populate_nothing_and_serve_correctly() {
    let r = rig().await;
    let (status, _) = r.multipart(CHUNK_SIZE as usize / 2).await;
    assert_eq!(status, 200);
    assert_eq!(
        r.read_cost(OBJECT).await,
        4,
        "the authorization request and all three chunks"
    );
    assert_eq!(r.windows("committed"), 0);
}

#[tokio::test]
async fn a_complete_that_fails_in_its_body_makes_nothing_visible() {
    let r = rig().await;
    r.store.lock().unwrap().fail_complete = true;
    let (status, body) = r.multipart(CHUNK_SIZE as usize).await;
    assert_eq!(status, 200, "S3 can say 200 and mean failure");
    assert!(
        String::from_utf8_lossy(&body).contains("<Error>"),
        "relayed untouched"
    );
    r.settled("discarded", 2).await;
    assert_eq!(r.windows("committed"), 0);
}

#[tokio::test]
async fn an_overwrite_is_served_as_the_new_version() {
    let r = rig().await;
    r.send("PUT", PATH, "", OBJECT).await;
    r.settled("committed", 3).await;
    assert_eq!(r.read_cost(OBJECT).await, 1);
    let newer: Vec<u8> = OBJECT.iter().rev().copied().collect();
    r.send("PUT", PATH, "", &newer).await;
    r.settled("committed", 6).await;
    assert_eq!(
        r.read_cost(&newer).await,
        1,
        "the new version, from the cache"
    );
}
