//! Admin endpoint: /healthz (liveness), /readyz (readiness), /metrics
//! (Prometheus: daemon counters + foyer cache metrics, one registry).

use std::convert::Infallible;

use http_body_util::Full;
use hyper::body::Bytes;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use tracing::{info, warn};

use crate::metrics::Metrics;

/// Serve the admin endpoint forever on `addr`.
///
/// # Errors
///
/// Only when binding or accepting on `addr` fails; per-connection errors are
/// logged and absorbed.
pub async fn serve(addr: String, metrics: Metrics) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    info!(%addr, "admin endpoint listening");
    serve_on(listener, metrics).await
}

/// Serve the admin endpoint forever on an already-bound `listener`.
///
/// Exists for the same reason [`crate::listen::serve_s3_on`] does: a caller that
/// needs to know the port *before* the server exists has to bind it itself. Here
/// that caller is a test — scraping `/metrics` over HTTP is the only way to assert
/// on the text a Prometheus scrape actually sees, and [`serve`] picks the address,
/// so a test using it would have to guess a free port, which is how a socket suite
/// becomes flaky on a busy runner.
///
/// # Errors
///
/// Only when accepting fails; per-connection errors are logged and absorbed.
pub async fn serve_on(listener: tokio::net::TcpListener, metrics: Metrics) -> anyhow::Result<()> {
    loop {
        let (stream, _) = listener.accept().await?;
        let metrics = metrics.clone();
        tokio::spawn(async move {
            let served = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(
                    TokioIo::new(stream),
                    service_fn(move |req| handle(req, metrics.clone())),
                )
                .await;
            if let Err(e) = served {
                warn!(error = %e, "admin connection error");
            }
        });
    }
}

async fn handle(
    req: Request<hyper::body::Incoming>,
    metrics: Metrics,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let (status, body) = match req.uri().path() {
        "/healthz" => (StatusCode::OK, "ok".to_owned()),
        // Ready once the cache and S3 service are built — preconditions for
        // this server existing at all.
        "/readyz" => (StatusCode::OK, "ready".to_owned()),
        "/metrics" => (StatusCode::OK, metrics.encode()),
        _ => (StatusCode::NOT_FOUND, "not found".to_owned()),
    };
    Ok(Response::builder()
        .status(status)
        .body(Full::new(Bytes::from(body)))
        .expect("static response"))
}
