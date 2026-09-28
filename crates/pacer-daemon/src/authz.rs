//! `auth.mode: requester`'s front door (ADR-0041 § 2.1–2.3): every request is
//! classified once, then either refused (`CONNECT`), forwarded byte-
//! transparently to the host the caller actually signed for, or — for a GET
//! whose shape the cache could serve, and whose caller left `Range` unsigned
//! — stripped of its `Authorization` header and handed to an anonymous
//! `s3s` service with the original request held in an extension
//! ([`HeldRequest`]) for [`crate::proxy`] to re-emit with the caller's own
//! signature (the authorization probe, and a chunk's own ranged read).
//!
//! What actually reads S3 under a `HeldRequest` — the probe, the chunk
//! fill — lives in `crate::proxy`, not here; this module owns exactly the
//! front-door classification and the HTTP client both sides of it share.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use http::{request, HeaderMap, HeaderValue, Method, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty};
use hyper::body::Incoming;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::client::legacy::Client;
use hyper_util::rt::TokioExecutor;
use s3s::host::{MultiDomain, S3Host};
use tracing::warn;

use crate::metrics::Metrics;

/// Headers that describe the hop, not the request, and must never cross a
/// proxy verbatim (RFC 7230 § 6.1; the last two are proxy-specific and would
/// otherwise carry this daemon's own presence to S3, which never asked for a
/// proxy and does not authenticate one).
const HOP_BY_HOP_HEADERS: [&str; 9] = [
    "connection",
    "keep-alive",
    "proxy-connection",
    "proxy-authenticate",
    "proxy-authorization",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
];

type HttpsConnector = hyper_rustls::HttpsConnector<HttpConnector>;

/// Every body the raw forward sends, erased to one type so a plain body, a teed one and one
/// re-sent from bytes share a client and its connection pool.
pub(crate) type OutBody =
    http_body_util::combinators::UnsyncBoxBody<Bytes, Box<dyn std::error::Error + Send + Sync>>;

/// Erase `body` to [`OutBody`].
pub(crate) fn out_body<B>(body: B) -> OutBody
where
    B: http_body::Body<Data = Bytes> + Send + 'static,
    B::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    body.map_err(Into::into).boxed_unsync()
}

/// The client's request exactly as it arrived — method, URI, headers,
/// **including** `Authorization` — captured before that header is stripped
/// for the anonymous `s3s` parse (ADR-0041 § 2.3). Lives exactly as long as
/// the `S3Request` it rides in as an extension; never cached, never logged
/// (the existing "never log a credential" discipline already covers
/// `Authorization`).
#[derive(Debug)]
pub struct HeldRequest {
    method: Method,
    uri: http::Uri,
    headers: HeaderMap,
}

impl HeldRequest {
    /// Capture `parts` before anything is removed from it.
    fn capture(parts: &request::Parts) -> Self {
        Self {
            method: parts.method.clone(),
            uri: parts.uri.clone(),
            headers: parts.headers.clone(),
        }
    }

    /// Rebuild a bodyless GET against the same host/path/headers — so the
    /// same `Authorization` covers it — with `Range` substituted for
    /// whatever the original request carried (ADR-0041 § 2.4 points 3–5):
    /// the `bytes=0-0` authorization probe, or one chunk's own range.
    ///
    /// Legal only because the caller left `range` unsigned — this method
    /// does not check that; the front door decides it once, before a
    /// request is ever held (see [`signed_range`]).
    fn with_range(&self, range: &str) -> Request<Empty<Bytes>> {
        self.rebuild(Some(range))
    }

    /// The held request as the caller sent it — `Range` included, if any — for a
    /// pass-through the cache cannot serve. S3 judges the caller's own signature.
    fn as_sent(&self) -> Request<Empty<Bytes>> {
        self.rebuild(None)
    }

    /// Rebuild the held GET, bodyless, minus the hop-by-hop headers (they describe the
    /// client's hop to this proxy, not the request) and with `Range` replaced when
    /// `range` is given.
    fn rebuild(&self, range: Option<&str>) -> Request<Empty<Bytes>> {
        let mut builder = Request::builder()
            .method(self.method.clone())
            .uri(self.uri.clone());
        if let Some(headers) = builder.headers_mut() {
            for (name, value) in &self.headers {
                let replaced = range.is_some() && name == http::header::RANGE;
                if !replaced && !is_hop_by_hop(name.as_str()) {
                    headers.append(name, value.clone());
                }
            }
            if let Some(range) = range {
                headers.insert(
                    http::header::RANGE,
                    HeaderValue::from_str(range).expect("a well-formed byte-range string"),
                );
            }
        }
        builder
            .body(Empty::new())
            .expect("method/uri/headers came from an already-parsed request")
    }
}

/// A held request re-emitted and fully read: the authorization probe's
/// verdict, or one chunk's bytes.
pub(crate) struct HeldResponse {
    pub(crate) status: StatusCode,
    pub(crate) headers: HeaderMap,
    pub(crate) body: Bytes,
}

/// Whether the caller signed their own `Range` — parsed from `Authorization`'s
/// `SignedHeaders` list. The whole design rests on the SigV4 asymmetry this
/// answers: a caller who sends no `Range` (or sends one but excludes it from
/// signing) leaves this proxy free to inject any; a caller who signed it
/// cannot have it rewritten without breaking the signature, so that request
/// must be forwarded verbatim instead (ADR-0041 § 2.4 point 2).
fn signed_range(headers: &HeaderMap) -> bool {
    let Some(auth) = headers
        .get(http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let Some((_, rest)) = auth.split_once("SignedHeaders=") else {
        return false;
    };
    let list = rest.split(',').next().unwrap_or("");
    list.split(';').any(|h| h.eq_ignore_ascii_case("range"))
}

/// The bucket and key a request addresses, as s3s would parse them: the bucket from the
/// virtual host or the first path segment, the key percent-decoded — so the chunk keys the
/// write path stages under are the ones the read path looks up. `None` for a request that
/// addresses no object.
pub(crate) fn object_address<B>(
    req: &Request<B>,
    domains: &MultiDomain,
) -> Option<(String, String)> {
    let host = req.headers().get(http::header::HOST)?.to_str().ok()?;
    let vh = domains.parse_host_header(host).ok()?;
    let path = req.uri().path().trim_start_matches('/');
    let (bucket, key) = match vh.bucket() {
        Some(bucket) => (bucket.to_owned(), path),
        None => {
            let (bucket, key) = path.split_once('/')?;
            (bucket.to_owned(), key)
        }
    };
    if key.is_empty() {
        return None;
    }
    Some((bucket, percent_decode(key)?))
}

/// Percent-decode `s`; `None` when an escape is malformed or the result is not UTF-8.
pub(crate) fn percent_decode(s: &str) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = std::str::from_utf8(bytes.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The percent-decoded value of query parameter `key`, if present with a value.
pub(crate) fn query_value(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        (k == key).then(|| percent_decode(v)).flatten()
    })
}

/// Whether `query` names `key` as a bare or valued parameter.
pub(crate) fn query_has(query: &str, key: &str) -> bool {
    query.split('&').any(|kv| kv.split('=').next() == Some(key))
}

/// ADR-0041 § 2.1's "GET object, cacheable shape": a GET addressing an
/// object (not a bucket-level operation), with none of the query/header
/// shapes the read path bypasses anyway (`partNumber`, `versionId`, SSE-C,
/// every conditional but `If-Match` — mirrors `PacerProxy::cacheable_shape`,
/// lifted to the raw request `s3s` has not parsed yet).
///
/// `domains` must be the same list the inner `s3s` service resolves virtual
/// hosts against, or this predicate's idea of "has a bucket" can disagree
/// with `s3s`'s.
fn is_object_get_shape(req: &Request<Incoming>, domains: &MultiDomain) -> bool {
    if req.method() != Method::GET {
        return false;
    }
    let Some(host) = req
        .headers()
        .get(http::header::HOST)
        .and_then(|v| v.to_str().ok())
    else {
        return false;
    };
    let Ok(vh) = domains.parse_host_header(host) else {
        return false;
    };
    let path = req.uri().path().trim_start_matches('/');
    let has_key = if vh.bucket().is_some() {
        // Virtual-hosted: the bucket came from Host, so any non-empty path
        // is the key.
        !path.is_empty()
    } else {
        // Path-style: the first segment is the bucket, so a key needs a
        // second one.
        path.split_once('/').is_some_and(|(_, key)| !key.is_empty())
    };
    if !has_key {
        return false;
    }
    let query = req.uri().query().unwrap_or("");
    if query_has(query, "partNumber") || query_has(query, "versionId") {
        return false;
    }
    let headers = req.headers();
    !headers.contains_key("if-none-match")
        && !headers.contains_key("if-modified-since")
        && !headers.contains_key("if-unmodified-since")
        && !headers.contains_key("x-amz-server-side-encryption-customer-algorithm")
}

/// `auth.mode: requester`'s whole request classification, as a
/// `hyper::service::Service` — see [`crate::listen::serve_s3_on`], which
/// serves this in `requester` mode and `s3s::service::S3Service` directly in
/// `node` mode.
///
/// `inner` is the anonymous `s3s` service (no auth provider, `set_host` on
/// the same `domains` this classifies against) that a strip-and-hold GET is
/// handed to; everything else this daemon can be asked for is refused by
/// `inner`'s own `S3` trait defaults (`RequesterS3` implements only
/// `get_object`), which is the belt to this module's suspenders — a
/// classification mistake here degrades to a clear error, never to an
/// unauthenticated request executing with this node's own S3 access.
#[derive(Clone)]
pub struct RequesterFront<S> {
    inner: S,
    domains: Arc<MultiDomain>,
    forwarder: Arc<Forwarder>,
    metrics: Metrics,
    /// Tees writes into the cache (planning/30); `None` forwards them untouched.
    tee: Option<crate::populate::WriteTee>,
}

impl<S> RequesterFront<S> {
    /// Tee `PutObject` and `UploadPart` bodies into the cache through `tee`.
    #[must_use]
    pub fn with_write_tee(mut self, tee: crate::populate::WriteTee) -> Self {
        self.tee = Some(tee);
        self
    }

    /// Build the front door. `domains` and `forwarder` are shared with
    /// [`crate::proxy`]'s `RequesterS3`, which needs the same forwarding
    /// client for the probe and chunk fills.
    pub fn new(
        inner: S,
        domains: Arc<MultiDomain>,
        forwarder: Arc<Forwarder>,
        metrics: Metrics,
    ) -> Self {
        Self {
            inner,
            domains,
            forwarder,
            metrics,
            tee: None,
        }
    }
}

impl<S> hyper::service::Service<Request<Incoming>> for RequesterFront<S>
where
    S: hyper::service::Service<Request<Incoming>, Response = Response<s3s::Body>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    type Response = Response<s3s::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let front = self.clone();
        Box::pin(async move { Ok(front.classify(req).await) })
    }
}

impl<S> RequesterFront<S>
where
    S: hyper::service::Service<Request<Incoming>, Response = Response<s3s::Body>>
        + Clone
        + Send
        + Sync
        + 'static,
    S::Future: Send + 'static,
    S::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    /// ADR-0041 § 2.1's classification. `CONNECT` is refused outright: the
    /// forward path's whole guarantee is that it can see and copy every
    /// header, which a tunnel makes impossible by construction.
    async fn classify(&self, req: Request<Incoming>) -> Response<s3s::Body> {
        if req.method() == Method::CONNECT {
            return refuse_connect();
        }
        if req.uri().scheme().is_none() {
            // The client contract (ADR-0041 § "the client contract for
            // requester mode") is proxy configuration, which puts the
            // request in absolute form. A relative URI means the caller
            // pointed an SDK at this listener as an *endpoint* instead —
            // the `node`-mode contract — and there is nothing this path
            // can forward it to.
            return bad_request(
                "PACER requester mode is a proxy, not an endpoint: point the SDK's proxy \
                 setting here, not its endpoint_url",
            );
        }
        if is_object_get_shape(&req, &self.domains) {
            if signed_range(req.headers()) {
                self.metrics.authz.signed_range_bypass.inc();
            } else {
                return self.strip_and_hold(req).await;
            }
        }
        if let Some(tee) = &self.tee {
            if let Some(op) = crate::requester_write::WriteOp::of(&req, &self.domains) {
                return crate::requester_write::forward(&self.forwarder, tee, req, op).await;
            }
        }
        match self.forwarder.forward(req).await {
            Ok(resp) => resp,
            Err(e) => {
                warn!(error = %e, "requester-mode forward failed");
                bad_gateway()
            }
        }
    }

    /// ADR-0041 § 2.3: lift `Authorization` off the request so `s3s` parses
    /// it as anonymous, hold the original (signature included) in an
    /// extension, and hand it to the inner service. `s3s` copies request
    /// extensions into `S3Request::extensions` (`ops::build_s3_request`),
    /// which is where `crate::proxy`'s requester-mode read path finds it.
    async fn strip_and_hold(&self, req: Request<Incoming>) -> Response<s3s::Body> {
        let (mut parts, body) = req.into_parts();
        let held = HeldRequest::capture(&parts);
        parts.headers.remove(http::header::AUTHORIZATION);
        parts.extensions.insert(Arc::new(held));
        let stripped = Request::from_parts(parts, body);
        match self.inner.call(stripped).await {
            Ok(resp) => resp,
            Err(e) => {
                let e: Box<dyn std::error::Error + Send + Sync> = e.into();
                warn!(error = %e, "requester-mode strip-and-hold failed");
                bad_gateway()
            }
        }
    }
}

fn refuse_connect() -> Response<s3s::Body> {
    text_response(
        StatusCode::METHOD_NOT_ALLOWED,
        "PACER cannot serve a CONNECT tunnel: a tunnel is opaque to a proxy that must see \
         every header. Configure the SDK for absolute-form forwarding instead (an http:// \
         endpoint through this proxy, or proxy_use_forwarding_for_https for TLS).\n",
    )
}

fn bad_request(msg: &str) -> Response<s3s::Body> {
    text_response(StatusCode::BAD_REQUEST, msg)
}

fn bad_gateway() -> Response<s3s::Body> {
    text_response(StatusCode::BAD_GATEWAY, "")
}

fn text_response(status: StatusCode, body: &str) -> Response<s3s::Body> {
    let mut resp = Response::new(s3s::Body::from(body.to_owned()));
    *resp.status_mut() = status;
    resp
}

/// The byte-transparent forward path (ADR-0041 § 2.2) and the held-signature
/// re-emission (§ 2.4): one pooled HTTPS client per shape, since a raw
/// forward streams the client's own body while a probe or chunk read always
/// synthesizes a bodyless GET.
pub struct Forwarder {
    /// Raw forward: streams the client's body through — as it arrived, wrapped in the write
    /// tee, or re-sent from bytes already read (a `CompleteMultipartUpload`'s part list).
    client: Client<HttpsConnector, OutBody>,
    /// Probe / chunk-range re-emission: always a bodyless GET.
    held_client: Client<HttpsConnector, Empty<Bytes>>,
}

impl Forwarder {
    /// Native roots, because every target this daemon ships for (EKS nodes)
    /// already trusts the standard CA bundle — there is no daemon-specific
    /// store to provision, and S3's certificate is never impersonated (the
    /// caller signed for the real host; this client dials it as itself).
    #[must_use]
    pub fn new() -> Self {
        Self {
            client: Client::builder(TokioExecutor::new()).build(https_connector()),
            held_client: Client::builder(TokioExecutor::new()).build(https_connector()),
        }
    }

    /// Reissue `req` to the host named in its own (absolute-form) URI,
    /// verbatim except the hop-by-hop headers. The caller signed for this
    /// exact method, URI and header set — that signature is what authorizes
    /// the request at S3, so nothing here may rewrite any of them.
    ///
    /// # Errors
    ///
    /// Whatever the underlying connection raises: DNS failure, TLS failure,
    /// connection reset. Never an S3-level error — those come back as a
    /// normal response with S3's own status and body.
    async fn forward(
        &self,
        req: Request<Incoming>,
    ) -> Result<Response<s3s::Body>, hyper_util::client::legacy::Error> {
        let (parts, body) = req.into_parts();
        let resp = self.send(parts, out_body(body)).await?;
        let (parts, body) = resp.into_parts();
        Ok(Response::from_parts(parts, s3s::Body::http_body(body)))
    }

    /// [`Self::forward`] with the body supplied separately and S3's response returned as it
    /// arrived, for the write path, which reads some responses before relaying them.
    ///
    /// # Errors
    ///
    /// As [`Self::forward`].
    pub(crate) async fn send(
        &self,
        parts: request::Parts,
        body: OutBody,
    ) -> Result<Response<Incoming>, hyper_util::client::legacy::Error> {
        let mut builder = Request::builder().method(parts.method).uri(parts.uri);
        if let Some(headers) = builder.headers_mut() {
            for (name, value) in &parts.headers {
                if !is_hop_by_hop(name.as_str()) {
                    headers.append(name, value.clone());
                }
            }
        }
        let outbound = builder
            .body(body)
            .expect("method/uri/headers were copied from an already-parsed request");
        self.client.request(outbound).await
    }

    /// Re-emit `held` exactly as the caller sent it and hand back S3's response, body
    /// still streaming — the pass-through for a GET the cache does not serve.
    ///
    /// # Errors
    ///
    /// A connection failure. An S3-level status is a normal response.
    pub(crate) async fn send_held(
        &self,
        held: &HeldRequest,
    ) -> Result<Response<Incoming>, ForwardError> {
        self.held_client
            .request(held.as_sent())
            .await
            .map_err(ForwardError::Connect)
    }

    /// Re-emit `held` with `Range: {range}`, and read the whole response —
    /// the authorization probe (`bytes=0-0`) or one chunk's own range
    /// (ADR-0041 § 2.4 points 3–5).
    ///
    /// # Errors
    ///
    /// A connection failure, or the response body failing to arrive whole.
    /// Never for an S3-level status: that comes back as
    /// [`HeldResponse::status`] for the caller to interpret.
    pub(crate) async fn send_range(
        &self,
        held: &HeldRequest,
        range: &str,
    ) -> Result<HeldResponse, ForwardError> {
        let req = held.with_range(range);
        let resp = self
            .held_client
            .request(req)
            .await
            .map_err(ForwardError::Connect)?;
        let (parts, body) = resp.into_parts();
        let body = body
            .collect()
            .await
            .map_err(|e| ForwardError::Body(e.to_string()))?
            .to_bytes();
        Ok(HeldResponse {
            status: parts.status,
            headers: parts.headers,
            body,
        })
    }

    /// One held-signature attempt at chunk `range`, classified into the same
    /// three buckets the SDK attempt is (`pacer_backend::retry::AttemptError`)
    /// so both attempt kinds share [`pacer_backend::retry::read_range_with`]'s
    /// retry/backoff/jitter (ADR-0041 § 2.4 point 5).
    pub(crate) async fn read_chunk_range(
        &self,
        held: &HeldRequest,
        range: &str,
    ) -> Result<Bytes, pacer_backend::retry::AttemptError> {
        use pacer_backend::retry::AttemptError;
        let resp = self
            .send_range(held, range)
            .await
            .map_err(|e| AttemptError::Transient(e.to_string()))?;
        if resp.status.is_success() {
            let promised = resp
                .headers
                .get(http::header::CONTENT_LENGTH)
                .and_then(|v| v.to_str().ok())
                .and_then(|v| v.parse::<u64>().ok());
            if let Some(promised) = promised {
                if resp.body.len() as u64 != promised {
                    return Err(AttemptError::Transient(format!(
                        "short body: got {} of {promised} bytes",
                        resp.body.len()
                    )));
                }
            }
            return Ok(resp.body);
        }
        classify_status(resp.status)
    }
}

impl Default for Forwarder {
    fn default() -> Self {
        Self::new()
    }
}

/// Classify a held-signature read's non-success status into the retry
/// module's three buckets — the same status ranges
/// `pacer_backend::retry::retryable_response` uses for the SDK path, so a
/// held-signature read and an SDK read agree on what is worth retrying.
fn classify_status<T>(status: StatusCode) -> Result<T, pacer_backend::retry::AttemptError> {
    use pacer_backend::retry::AttemptError;
    if status == StatusCode::NOT_FOUND {
        return Err(AttemptError::Missing);
    }
    if status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS {
        return Err(AttemptError::Transient(format!(
            "held-signature read: HTTP {status}"
        )));
    }
    Err(AttemptError::Permanent(format!(
        "held-signature read: HTTP {status}"
    )))
}

/// A [`Forwarder::send_range`] failure — a connection fault or a body that
/// did not arrive whole. Never an S3-level status; that is
/// [`HeldResponse::status`].
#[derive(Debug, thiserror::Error)]
pub(crate) enum ForwardError {
    #[error("connecting to hold the caller's signature: {0}")]
    Connect(hyper_util::client::legacy::Error),
    #[error("reading the held response body: {0}")]
    Body(String),
}

/// The one crypto provider this daemon's own TLS uses, named rather than taken from
/// rustls' process default — which is ambiguous, and a startup panic, as soon as any
/// dependency enables a second backend.
pub(crate) fn crypto_provider() -> Arc<rustls::crypto::CryptoProvider> {
    Arc::new(rustls::crypto::aws_lc_rs::default_provider())
}

/// The acceptor for requester mode's TLS listener (ADR-0041 § 9), from the daemon's own
/// PEM certificate chain and key. HTTP/1.1 only over ALPN: the client shape this listener
/// exists for — botocore's `proxy_use_forwarding_for_https` — speaks nothing else.
///
/// # Errors
///
/// Either file unreadable or not valid PEM, or a key that does not match the chain.
pub fn tls_acceptor(
    cfg: &crate::config::TlsListenConfig,
) -> anyhow::Result<tokio_rustls::TlsAcceptor> {
    use rustls::pki_types::pem::PemObject;
    use rustls::pki_types::{CertificateDer, PrivateKeyDer};
    let certs = CertificateDer::pem_file_iter(&cfg.cert)
        .and_then(Iterator::collect::<Result<Vec<_>, _>>)
        .map_err(|e| anyhow::anyhow!("reading TLS certificate {}: {e}", cfg.cert.display()))?;
    let key = PrivateKeyDer::from_pem_file(&cfg.key)
        .map_err(|e| anyhow::anyhow!("reading TLS key {}: {e}", cfg.key.display()))?;
    let mut server = rustls::ServerConfig::builder_with_provider(crypto_provider())
        .with_safe_default_protocol_versions()?
        .with_no_client_auth()
        .with_single_cert(certs, key)?;
    server.alpn_protocols = vec![b"http/1.1".to_vec()];
    Ok(tokio_rustls::TlsAcceptor::from(Arc::new(server)))
}

fn https_connector() -> HttpsConnector {
    hyper_rustls::HttpsConnectorBuilder::new()
        .with_provider_and_native_roots(crypto_provider())
        .expect("platform TLS roots must be available")
        .https_or_http()
        .enable_http1()
        .build()
}

pub(crate) fn is_hop_by_hop(name: &str) -> bool {
    HOP_BY_HOP_HEADERS
        .iter()
        .any(|h| h.eq_ignore_ascii_case(name))
}
