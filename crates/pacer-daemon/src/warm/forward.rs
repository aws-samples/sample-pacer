//! The HTTP client `pacer-daemon warm --proxy` sends every request through: TLS to the
//! daemon, and the whole request — absolute `https://` URL, the caller's own signature —
//! forwarded inside that session (`auth.mode: requester`, ADR-0041).
//!
//! ```text
//!   warm ══ TLS to the daemon (its own certificate) ══► daemon ══ its own TLS ══► S3
//!           GET https://bucket.s3express-….amazonaws.com/key
//!           Authorization: <the caller's SigV4>
//! ```
//!
//! This is the shape botocore calls `proxy_use_forwarding_for_https`, and the only one the
//! daemon can cache through: a `CONNECT` tunnel carries TLS end to end to S3, so the daemon
//! could see nothing and refuses it. The stock SDK client has no such mode — it tunnels any
//! `https://` target — hence this client, and why it is small: hyper-util already sends
//! absolute-form URIs on a connection marked as a proxy, so all it has to do is dial the
//! daemon over TLS whatever the target, and say so.
//!
//! **TLS on both hops, always.** The proxy URL must be `https://`, and the targets are the
//! SDK's own endpoint resolution, which is `https://` for Express and Standard alike. The
//! daemon follows the scheme a request names when it forwards, so a plaintext hop here
//! would make its hop to S3 plaintext too.
//!
//! The connector settings the SDK hands a client (its connect and read timeouts) are not
//! applied here; the operation timeouts the SDK enforces itself still are.

use std::future::Future;
use std::path::Path;
use std::pin::Pin;
use std::task::{Context, Poll};

use anyhow::{bail, Context as _};
use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use http::Uri;
use hyper_rustls::{HttpsConnector, MaybeHttpsStream};
use hyper_util::client::legacy::connect::{Connected, Connection, HttpConnector as TcpConnector};
use hyper_util::client::legacy::Client;
use hyper_util::rt::{TokioExecutor, TokioIo};
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::CertificateDer;
use tokio::net::TcpStream;

/// An SDK HTTP client that forwards every request to the daemon at `proxy` inside TLS.
///
/// `ca_pem` is the CA that signed the daemon's certificate (the chart's
/// `auth.requester.tls.secretName`); `None` trusts the platform's roots, for a certificate
/// from a public CA.
///
/// # Errors
///
/// A proxy URL that is not `https://host[:port]`, or a CA file that cannot be read or holds
/// no certificate.
pub fn forwarding_client(proxy: &str, ca_pem: Option<&Path>) -> anyhow::Result<ForwardingClient> {
    let proxy: Uri = proxy
        .parse()
        .with_context(|| format!("--proxy {proxy:?} is not a URL"))?;
    if proxy.scheme_str() != Some("https") || proxy.host().is_none() {
        bail!(
            "--proxy must be https://host[:port] — the daemon's TLS listener; a plaintext \
             proxy would make the daemon's hop to S3 plaintext too"
        );
    }
    let tls = tls_config(ca_pem)?;
    let mut tcp = TcpConnector::new();
    tcp.enforce_http(false);
    let https = hyper_rustls::HttpsConnectorBuilder::new()
        .with_tls_config(tls)
        .https_only()
        .enable_http1()
        .wrap_connector(tcp);
    let client = Client::builder(TokioExecutor::new()).build(ToProxy { https, proxy });
    Ok(ForwardingClient { client })
}

/// TLS to the daemon: the given CA, or the platform's roots.
fn tls_config(ca_pem: Option<&Path>) -> anyhow::Result<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    match ca_pem {
        Some(path) => {
            let certs = CertificateDer::pem_file_iter(path)
                .and_then(Iterator::collect::<Result<Vec<_>, _>>)
                .map_err(|e| anyhow::anyhow!("reading --proxy-ca {}: {e}", path.display()))?;
            if certs.is_empty() {
                bail!("--proxy-ca {} holds no certificate", path.display());
            }
            for cert in certs {
                roots.add(cert).context("adding a --proxy-ca certificate")?;
            }
        }
        None => {
            for cert in rustls_native_certs::load_native_certs().certs {
                // A platform root rustls cannot parse is skipped, as the SDK's own client does.
                let _ = roots.add(cert);
            }
        }
    }
    // No ALPN here: hyper-rustls writes it from `enable_http1` (`http/1.1`, the only
    // protocol the daemon's TLS listener speaks) and refuses a config that already has one.
    Ok(
        rustls::ClientConfig::builder_with_provider(crate::authz::crypto_provider())
            .with_safe_default_protocol_versions()?
            .with_root_certificates(roots)
            .with_no_client_auth(),
    )
}

/// Dials the daemon — never the request's own host — and marks the connection as a proxy,
/// which is what makes hyper-util send the request in absolute form.
#[derive(Clone, Debug)]
struct ToProxy {
    https: HttpsConnector<TcpConnector>,
    proxy: Uri,
}

/// The future a [`ToProxy`] dial resolves to.
type Dialing = Pin<Box<dyn Future<Output = Result<Proxied, BoxError>> + Send>>;

/// The error type hyper-util's connector contract wants.
type BoxError = Box<dyn std::error::Error + Send + Sync>;

impl tower_service::Service<Uri> for ToProxy {
    type Response = Proxied;
    type Error = BoxError;
    type Future = Dialing;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.https.poll_ready(cx)
    }

    fn call(&mut self, _target: Uri) -> Self::Future {
        let dialing = self.https.call(self.proxy.clone());
        Box::pin(async move { Ok(Proxied(dialing.await?)) })
    }
}

/// A TLS connection to the daemon that reports itself as a proxy connection.
#[derive(Debug)]
struct Proxied(MaybeHttpsStream<TokioIo<TcpStream>>);

impl Connection for Proxied {
    fn connected(&self) -> Connected {
        self.0.connected().proxy(true)
    }
}

impl hyper::rt::Read for Proxied {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: hyper::rt::ReadBufCursor<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_read(cx, buf)
    }
}

impl hyper::rt::Write for Proxied {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.0).poll_write(cx, buf)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.0).poll_shutdown(cx)
    }
}

/// The SDK-facing half: an [`HttpClient`] whose every connector is the forwarding client.
#[derive(Clone, Debug)]
pub struct ForwardingClient {
    client: Client<ToProxy, SdkBody>,
}

impl HttpConnector for ForwardingClient {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let client = self.client.clone();
        HttpConnectorFuture::new(async move {
            let request = request
                .try_into_http1x()
                .map_err(|e| ConnectorError::other(e.into(), None))?;
            let response = client.request(request).await.map_err(|e| {
                if e.is_connect() {
                    ConnectorError::io(e.into())
                } else {
                    ConnectorError::other(e.into(), None)
                }
            })?;
            HttpResponse::try_from(response.map(SdkBody::from_body_1_x))
                .map_err(|e| ConnectorError::other(e.into(), None))
        })
    }
}

impl HttpClient for ForwardingClient {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        SharedHttpConnector::new(self.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_plaintext_or_hostless_proxy_is_refused() {
        for bad in ["http://pacer:9000", "pacer:9443", "https://"] {
            assert!(forwarding_client(bad, None).is_err(), "{bad} was accepted");
        }
    }

    #[test]
    fn a_ca_file_without_a_certificate_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let empty = dir.path().join("ca.pem");
        std::fs::write(&empty, "not a certificate\n").unwrap();
        let err = forwarding_client("https://pacer:9443", Some(&empty)).unwrap_err();
        assert!(
            format!("{err:#}").contains("holds no certificate"),
            "{err:#}"
        );
    }
}
