//! Standalone fake S3 backend for local, off-cluster daemon development
//! (quality item L5).
//!
//! Serves an `s3s-fs` filesystem-backed S3 implementation over a real TCP
//! listener with static SigV4 credentials, so `scripts/dev/local-daemon` can
//! point a plain `cargo build -p pacer-daemon` binary at it without any
//! cluster, real S3 bucket, or IAM identity. Before this existed, the only
//! place `s3s-fs` was wired up was in-process inside the integration tests
//! (`crates/pacer-daemon/tests/daemon/correctness.rs`'s `harness_with`), which never
//! opens a socket — the test's daemon and backend talk over
//! `s3s_aws::Client::from(service)`, an in-memory adapter. A daemon started
//! as its own OS process (which is what a developer wants to `curl` or point
//! `aws s3` at) needs the backend on the other end of a real port, so this
//! binary exists to be that port.
//!
//! The auth wiring (`S3ServiceBuilder` + `s3s::auth::SimpleAuth`) mirrors
//! `harness_with`'s backend half; the accept loop mirrors both
//! `pacer_daemon::main::serve_s3` (this crate's own S3 listener) and
//! `s3s-fs`'s own bundled binary (`s3s-fs-0.14.1/src/main.rs`, already a dev
//! dependency of this crate) — that binary is the one place in this
//! dependency graph that already serves this exact service over TCP, so the
//! loop below is that pattern with bucket pre-creation added and the CLI
//! surface trimmed to what the driver script needs. No existing Rust source
//! file was edited to build this: everything here is new.
//!
//! What this does NOT exercise (see `scripts/dev/README-local.md`): no NVMe
//! disk tier behavior beyond what a tmpfs/APFS dir gives you, no peer plane,
//! no RDMA, and no real S3 semantics (Express `CreateSession`, cross-AZ,
//! eventual consistency) — `s3s-fs` is a strongly-consistent local
//! filesystem, not S3.

use std::net::SocketAddr;
use std::path::PathBuf;

use hyper_util::rt::{TokioExecutor, TokioIo};
use s3s::auth::SimpleAuth;
use s3s::service::S3ServiceBuilder;
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Default bind address: loopback-only (this backend is never meant to be
/// reachable from outside the dev box) on a port that collides with neither
/// the daemon's own default S3 port (9000) nor its admin port (9090).
const DEFAULT_ADDR: &str = "127.0.0.1:9099";

/// Default SigV4 access key the fake backend accepts. Distinct from the
/// daemon's own placeholder credentials (ADR-0006's `pacer`/`pacer`, which a
/// *client* signs the daemon's S3 port with) so a log line or a captured
/// request can never be misread as coming from the wrong hop.
const DEFAULT_ACCESS_KEY: &str = "pacer-backend";
/// Default SigV4 secret key pairing [`DEFAULT_ACCESS_KEY`]. Not a secret —
/// same status as the daemon's own placeholders (ADR-0006): it only gates
/// malformed requests on a loopback dev socket.
const DEFAULT_SECRET_KEY: &str = "pacer-backend-secret";

/// Parsed command-line invocation.
struct Args {
    /// Address to listen on (`--addr`, default [`DEFAULT_ADDR`]).
    addr: SocketAddr,
    /// Filesystem root `s3s-fs` stores objects under (`--root`, required —
    /// deliberately no default, so a driver script always states it rather
    /// than this binary silently picking a directory to write into).
    root: PathBuf,
    /// SigV4 access key to require (`--access-key`, default
    /// [`DEFAULT_ACCESS_KEY`]).
    access_key: String,
    /// SigV4 secret key to require (`--secret-key`, default
    /// [`DEFAULT_SECRET_KEY`]).
    secret_key: String,
    /// Bucket names to create (as subdirectories of `root`) before serving,
    /// so a caller never races the fake backend's own startup with a
    /// `CreateBucket` call. At least one is required.
    buckets: Vec<String>,
}

/// Parse `argv`. No `clap`: this binary takes five flags total and adding a
/// dependency (even a dev-only one) for that would outweigh what it buys.
///
/// # Errors
///
/// A flag missing its value, an unparseable `--addr`, a missing `--root`, or
/// no bucket names given.
fn parse_args() -> anyhow::Result<Args> {
    let mut addr = DEFAULT_ADDR.to_string();
    let mut root = None;
    let mut access_key = DEFAULT_ACCESS_KEY.to_string();
    let mut secret_key = DEFAULT_SECRET_KEY.to_string();
    let mut buckets = Vec::new();

    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        let mut flag_value = |name: &str| {
            argv.next()
                .ok_or_else(|| anyhow::anyhow!("{name} needs a value"))
        };
        match arg.as_str() {
            "--addr" => addr = flag_value("--addr")?,
            "--root" => root = Some(PathBuf::from(flag_value("--root")?)),
            "--access-key" => access_key = flag_value("--access-key")?,
            "--secret-key" => secret_key = flag_value("--secret-key")?,
            bucket => buckets.push(bucket.to_string()),
        }
    }

    let root = root.ok_or_else(|| anyhow::anyhow!("--root <dir> is required"))?;
    if buckets.is_empty() {
        anyhow::bail!("at least one bucket name is required (positional arguments)");
    }
    Ok(Args {
        addr: addr
            .parse()
            .map_err(|e| anyhow::anyhow!("--addr {addr:?}: {e}"))?,
        root,
        access_key,
        secret_key,
        buckets,
    })
}

/// Create `root` and one subdirectory per requested bucket. `s3s-fs` has no
/// separate bucket-metadata store (`FileSystem::create_bucket` is exactly
/// `fs::create_dir`, see `s3s-fs-0.14.1/src/s3.rs`), so this is the whole of
/// bucket provisioning — no S3 round trip to itself needed before it can
/// serve one.
///
/// # Errors
///
/// Any directory creation failing (permissions, a path component that is a
/// file, etc.).
fn provision_buckets(root: &std::path::Path, buckets: &[String]) -> anyhow::Result<()> {
    std::fs::create_dir_all(root)
        .map_err(|e| anyhow::anyhow!("creating backend root {}: {e}", root.display()))?;
    for bucket in buckets {
        let path = root.join(bucket);
        std::fs::create_dir_all(&path)
            .map_err(|e| anyhow::anyhow!("creating bucket dir {}: {e}", path.display()))?;
    }
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args = parse_args()?;
    provision_buckets(&args.root, &args.buckets)?;

    let fs = s3s_fs::FileSystem::new(&args.root)
        .map_err(|e| anyhow::anyhow!("opening {} as an s3s-fs root: {e:?}", args.root.display()))?;
    let service = {
        let mut b = S3ServiceBuilder::new(fs);
        b.set_auth(SimpleAuth::from_single(
            args.access_key.clone(),
            args.secret_key.clone(),
        ));
        b.build()
    };

    let listener = TcpListener::bind(args.addr).await?;
    info!(
        addr = %args.addr,
        root = %args.root.display(),
        buckets = ?args.buckets,
        "fake s3 backend listening"
    );
    loop {
        let (stream, _) = listener.accept().await?;
        let service = service.clone();
        tokio::spawn(async move {
            let served = hyper_util::server::conn::auto::Builder::new(TokioExecutor::new())
                .serve_connection(TokioIo::new(stream), service)
                .await;
            if let Err(e) = served {
                warn!(error = %e, "fake s3 connection error");
            }
        });
    }
}
