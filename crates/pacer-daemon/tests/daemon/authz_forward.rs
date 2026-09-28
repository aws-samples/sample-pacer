//! ADR-0041 step 2 (`planning/29-auth-modes.md` § 4): `RequesterFront`'s
//! `CONNECT` refusal and its byte-transparent forward, over real TCP against
//! an in-process fake upstream. The upstream echoes back exactly what it
//! received — method, path, every header, and the body — so an assertion
//! here is about what `RequesterFront` did to the request on the way
//! through, never about S3 itself.
//!
//! client ⇢ TCP 127.0.0.1:0 ⇢ listen::serve_s3_on(RequesterFront) ⇢ TCP ⇢ fake upstream

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http::{Request, Response, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use pacer_daemon::authz::{Forwarder, RequesterFront};
use pacer_daemon::listen::ListenLimits;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::shutdown::Shutdown;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// Start the fake upstream: one line each for the method+path and every
/// request header, a blank line, then the body verbatim.
async fn spawn_echo_upstream() -> (SocketAddr, Shutdown) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("bound port has an address");
    let shutdown = Shutdown::new();
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        hyper::service::service_fn(echo),
        ListenLimits::default(),
        Metrics::new().expect("a fresh registry"),
        shutdown.signal(),
    ));
    (addr, shutdown)
}

async fn echo(req: Request<Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let (parts, body) = req.into_parts();
    let body = body.collect().await.expect("a body").to_bytes();
    let path_and_query = parts
        .uri
        .path_and_query()
        .map_or("/", http::uri::PathAndQuery::as_str);
    let mut text = format!("{} {}\n", parts.method, path_and_query);
    for (name, value) in &parts.headers {
        text.push_str(name.as_str());
        text.push_str(": ");
        text.push_str(value.to_str().unwrap_or(""));
        text.push('\n');
    }
    text.push('\n');
    text.push_str(&String::from_utf8_lossy(&body));
    Ok(Response::new(Full::new(Bytes::from(text))))
}

/// Start `RequesterFront` on its own loopback port, the same
/// [`pacer_daemon::listen::serve_s3_on`] `main` calls in `requester` mode.
async fn spawn_requester_front() -> (SocketAddr, Shutdown) {
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("bound port has an address");
    let shutdown = Shutdown::new();
    let metrics = Metrics::new().expect("a fresh registry");
    // None of this file's requests are a GET shaped like a cacheable object
    // GET (they are PUT, CONNECT, or a relative URI), so `inner` is never
    // actually called — strip-and-hold is exercised in `authz_requester_get.rs`.
    let inner = hyper::service::service_fn(|_: Request<Incoming>| async {
        Ok::<_, Infallible>(unreachable_inner_response())
    });
    let domains =
        Arc::new(s3s::host::MultiDomain::new(["example.com"]).expect("a valid domain list"));
    let forwarder = Arc::new(Forwarder::new());
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        RequesterFront::new(inner, domains, forwarder, metrics),
        ListenLimits::default(),
        Metrics::new().expect("a fresh registry"),
        shutdown.signal(),
    ));
    (addr, shutdown)
}

fn unreachable_inner_response() -> Response<s3s::Body> {
    panic!("inner s3s service must not be called by this file's requests")
}

/// Read one HTTP/1.1 response off `stream` far enough to assert against: the
/// status code and the header block plus body as one string. A fixture
/// parser, not a general one — it trusts `Content-Length` because both
/// servers in this file always send one.
async fn read_response<S: tokio::io::AsyncRead + Unpin>(stream: &mut S) -> (u16, String) {
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
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// The core of ADR-0041 § 2.2: the caller's own signature and every other
/// header reach the upstream verbatim, but the hop-by-hop set does not, and
/// the response comes back byte for byte.
#[tokio::test]
async fn forward_copies_headers_and_body_but_not_hop_by_hop() {
    let (upstream, _upstream_shutdown) = spawn_echo_upstream().await;
    let (front, _front_shutdown) = spawn_requester_front().await;

    let mut client = TcpStream::connect(front)
        .await
        .expect("connect to the front door");
    let body = b"the-object-bytes";
    let request = format!(
        "PUT http://{upstream}/bucket/key?partNumber=1 HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential=test\r\n\
         X-Amz-Content-Sha256: abc\r\n\
         Connection: keep-alive\r\n\
         Proxy-Authorization: should-not-arrive\r\n\
         Content-Length: {len}\r\n\
         \r\n",
        len = body.len(),
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    client.write_all(body).await.expect("write body");

    let (status, text) = read_response(&mut client).await;
    assert_eq!(status, 200, "{text}");
    assert!(text.starts_with("PUT /bucket/key?partNumber=1\n"), "{text}");
    assert!(
        text.contains("authorization: AWS4-HMAC-SHA256 Credential=test"),
        "the caller's own signature must reach the upstream verbatim: {text}"
    );
    assert!(text.contains("x-amz-content-sha256: abc"), "{text}");
    assert!(
        !text.to_ascii_lowercase().contains("connection:"),
        "a hop-by-hop header must not cross the proxy: {text}"
    );
    assert!(
        !text.to_ascii_lowercase().contains("proxy-authorization:"),
        "a hop-by-hop header must not cross the proxy: {text}"
    );
    assert!(text.ends_with("the-object-bytes"), "{text}");
}

/// ADR-0041 § 2.1: a tunnel is opaque to a proxy that must see every header,
/// so `CONNECT` is refused rather than served.
#[tokio::test]
async fn connect_is_refused() {
    let (front, _front_shutdown) = spawn_requester_front().await;
    let mut client = TcpStream::connect(front)
        .await
        .expect("connect to the front door");
    client
        .write_all(b"CONNECT example.com:443 HTTP/1.1\r\nHost: example.com:443\r\n\r\n")
        .await
        .expect("write request");
    let (status, _) = read_response(&mut client).await;
    assert_eq!(status, StatusCode::METHOD_NOT_ALLOWED.as_u16());
}

/// ADR-0041 § "the client contract": `requester` mode is a proxy, not an
/// endpoint. A relative request-target means an SDK was pointed at this
/// listener as an endpoint — the `node`-mode contract — and there is no host
/// to forward it to.
#[tokio::test]
async fn a_relative_request_target_is_a_bad_request() {
    let (front, _front_shutdown) = spawn_requester_front().await;
    let mut client = TcpStream::connect(front)
        .await
        .expect("connect to the front door");
    client
        .write_all(b"GET /bucket/key HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("write request");
    let (status, _) = read_response(&mut client).await;
    assert_eq!(status, StatusCode::BAD_REQUEST.as_u16());
}

/// A requester front on a TLS listener to a throwaway self-signed certificate for
/// `localhost` (ADR-0041 § 9), plus the certificate a client must trust and the
/// listener's registry. The key exists only in a temp dir for the test's lifetime.
async fn spawn_requester_front_tls() -> (
    SocketAddr,
    Shutdown,
    rustls::pki_types::CertificateDer<'static>,
    Metrics,
    tempfile::TempDir,
) {
    let rcgen::CertifiedKey { cert, key_pair } =
        rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])
            .expect("a self-signed certificate");
    let dir = tempfile::tempdir().expect("a temp dir for the key pair");
    let (cert_path, key_path) = (dir.path().join("tls.crt"), dir.path().join("tls.key"));
    std::fs::write(&cert_path, cert.pem()).expect("write the certificate");
    std::fs::write(&key_path, key_pair.serialize_pem()).expect("write the key");
    let acceptor = pacer_daemon::authz::tls_acceptor(&pacer_daemon::config::TlsListenConfig {
        cert: cert_path,
        key: key_path,
        listen_addr: String::new(),
    })
    .expect("the minted pair must load");

    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let addr = listener.local_addr().expect("bound port has an address");
    let shutdown = Shutdown::new();
    let metrics = Metrics::new().expect("a fresh registry");
    let inner = hyper::service::service_fn(|_: Request<Incoming>| async {
        Ok::<_, Infallible>(unreachable_inner_response())
    });
    let domains =
        Arc::new(s3s::host::MultiDomain::new(["example.com"]).expect("a valid domain list"));
    tokio::spawn(pacer_daemon::listen::serve_s3_tls_on(
        listener,
        acceptor,
        RequesterFront::new(inner, domains, Arc::new(Forwarder::new()), metrics.clone()),
        ListenLimits::default(),
        metrics.clone(),
        shutdown.signal(),
    ));
    (addr, shutdown, cert.der().clone(), metrics, dir)
}

/// ADR-0041 § 9's recommended client shape: TLS to the daemon's own certificate, the
/// absolute-form request inside that session. The front door must see and forward it
/// exactly as it does over plaintext — the caller's signature included.
#[tokio::test]
async fn the_tls_listener_forwards_an_absolute_form_request() {
    let (upstream, _upstream_shutdown) = spawn_echo_upstream().await;
    let (front, _front_shutdown, cert, _metrics, _dir) = spawn_requester_front_tls().await;

    let mut roots = rustls::RootCertStore::empty();
    roots.add(cert).expect("trust the minted certificate");
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::aws_lc_rs::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .expect("default protocol versions")
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = TcpStream::connect(front)
        .await
        .expect("connect to the front door");
    let mut tls = tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect(
            rustls::pki_types::ServerName::try_from("localhost").expect("a DNS name"),
            tcp,
        )
        .await
        .expect("the TLS handshake with the daemon's own certificate");

    let request = format!(
        "PUT http://{upstream}/bucket/key HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: AWS4-HMAC-SHA256 Credential=tls\r\n\
         Content-Length: 3\r\n\
         \r\n\
         abc"
    );
    tls.write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, text) = read_response(&mut tls).await;
    assert_eq!(status, 200, "{text}");
    assert!(
        text.contains("authorization: AWS4-HMAC-SHA256 Credential=tls"),
        "{text}"
    );
    assert!(text.ends_with("abc"), "{text}");
}

/// A client that speaks plaintext to the TLS port fails the handshake and is closed —
/// and its connection slot comes back, or a port scanner could exhaust the cap.
#[tokio::test]
async fn plaintext_on_the_tls_listener_is_closed_and_frees_its_slot() {
    let (front, _front_shutdown, _cert, metrics, _dir) = spawn_requester_front_tls().await;
    let mut tcp = TcpStream::connect(front)
        .await
        .expect("connect to the front door");
    tcp.write_all(b"GET http://example.com/bucket/key HTTP/1.1\r\nHost: example.com\r\n\r\n")
        .await
        .expect("write request");
    let mut buf = [0u8; 512];
    let read = tokio::time::timeout(crate::common::PATIENCE, tcp.read(&mut buf))
        .await
        .expect("the daemon must close, not hang");
    assert!(
        !String::from_utf8_lossy(&buf[..read.unwrap_or(0)]).starts_with("HTTP/"),
        "a plaintext request must never be answered on the TLS port"
    );
    crate::common::poll_until("the failed handshake's slot is released", || async {
        metrics.listener.connections_active.get() == 0
    })
    .await;
}
