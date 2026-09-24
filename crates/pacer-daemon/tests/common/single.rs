//! One daemon, in either of the suite's two shapes.
//!
//! [`DaemonCore`] is the assembly — placeholder auth → [`PacerProxy`] → a foyer hybrid
//! cache → a backend reached with the daemon's own identity. [`DaemonCore::in_process`]
//! and [`DaemonCore::served`] are the only things that differ between an arm that does
//! not care about sockets and one whose whole subject is them.

use std::net::SocketAddr;
use std::time::Duration;

use aws_sdk_s3::config::Credentials;
use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use pacer_backend::BackendType;
use pacer_cache::chunk::ChunkConfig;
use pacer_cache::tier::ChunkTier;
use pacer_daemon::auth::PlaceholderAuth;
use pacer_daemon::listen::ListenLimits;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::proxy::PacerProxy;
use pacer_daemon::shutdown::Shutdown;
use s3s::service::{S3Service, S3ServiceBuilder};

use super::{
    body_for_key, build_cache, build_cache_with_metrics, create_test_bucket, fs_backend_service,
    placeholder_credentials, poll_until_within, sdk_client_for, tcp_client, CacheSpec, BUCKET,
    FILL_PARALLELISM, LOOPBACK_ANY_PORT, PATIENCE, PLACEHOLDER_KEY,
};

/// What one daemon in this suite is parameterised by.
///
/// A struct with a documented [`Default`] rather than a builder, for the reason
/// [`CacheSpec`] gives: an arm overrides the one or two fields its subject is about, so
/// the diff between two arms is exactly the knob that differs.
#[derive(Debug, Clone, Copy)]
pub struct DaemonSpec {
    /// The bucket shape the write path normalises for (ADR-0023).
    pub backend_type: BackendType,
    /// Read-path cache floor (ADR-0002): objects at or below this are proxied uncached.
    pub min_object_size: u64,
    /// Read-path cache ceiling, or `None` for no upper bound.
    pub max_object_size: Option<u64>,
    /// The cache's grid — and, where the scatter is on, the window and S3 part size.
    pub chunk_size: u64,
    /// In-flight chunk resolutions per GET.
    pub fill_parallelism: usize,
    /// foyer's capacities.
    pub cache: CacheSpec,
    /// Register foyer's own metrics into the daemon's registry, as `main` does. Off by
    /// default because it is only observable to an arm that reads the exposition, and
    /// on for the one arm whose subject is the memory tier's ceiling.
    pub foyer_metrics: bool,
    /// Whether an `If-Match` GET may be served from cache on an ETag match (ADR-0039).
    /// `true` — the shipped default — so every other suite exercises what a deployment
    /// runs; the one arm that turns it off is testing the escape hatch itself.
    pub conditional_get_from_cache: bool,
    /// Whether concurrent readers of one missed chunk share its backend read (ADR-0040).
    /// `true` — the shipped default — so every other suite exercises what a deployment
    /// runs; `fill_coalesce`'s arm runs both sides of it, because the only way to show
    /// the mechanism did anything is to show what happens without it.
    pub fill_coalesce: bool,
}

impl Default for DaemonSpec {
    fn default() -> Self {
        Self {
            // The ADR-0002 baseline most arms exercise.
            backend_type: BackendType::Express,
            min_object_size: 4 << 20,
            max_object_size: Some(64 << 20),
            // Small enough that a few-MiB object spans several chunks, which is what
            // exercises the covering-set math and the ordered fill pipeline.
            chunk_size: 1 << 20,
            fill_parallelism: FILL_PARALLELISM,
            cache: CacheSpec::default(),
            foyer_metrics: false,
            conditional_get_from_cache: true,
            fill_coalesce: true,
        }
    }
}

/// The two clients that reach one backend: the one the daemon forwards to, and the
/// ground-truth one a test reads with to bypass the daemon entirely.
///
/// Separate clients over the *same* backend, not one shared handle: an arm that asserts
/// "the bytes are durable" has to observe them through something the daemon is not in.
pub struct BackendPair {
    /// The client the daemon forwards to.
    pub daemon: aws_sdk_s3::Client,
    /// The oracle — same backend, no daemon in the path.
    pub truth: aws_sdk_s3::Client,
}

impl BackendPair {
    /// Two clients over one [`S3Service`].
    pub fn over(service: &S3Service, creds: &Credentials) -> Self {
        Self {
            daemon: sdk_client_for(service.clone(), creds.clone()),
            truth: sdk_client_for(service.clone(), creds.clone()),
        }
    }
}

/// The daemon assembled but not yet reachable: its proxy is still owned, so an arm can
/// attach a retry policy, a delivery config or a scatter coordinator before choosing a
/// shape.
///
/// `proxy` is public and moved out by [`Self::in_process`] / [`Self::served`], so the
/// sequence reads `core.proxy = core.proxy.with_x(..)` and then one of the two — which
/// is the whole variation the nine old harnesses expressed by each writing the assembly
/// out again.
pub struct DaemonCore {
    /// The daemon's `S3` implementation, before it is sealed into a service.
    pub proxy: PacerProxy,
    /// The daemon's registry — the source of every `/metrics` reading.
    pub metrics: Metrics,
    /// The chunk tier, so a test can close it the way `main`'s drain does.
    pub tier: ChunkTier,
    /// The oracle client: same backend, no daemon in the path.
    pub backend: aws_sdk_s3::Client,
    /// This daemon's chunk size, so a test can size an object in chunks.
    pub chunk_size: u64,
    /// The backend's and the cache's directories.
    ///
    /// Public because an arm that keeps the proxy instead of choosing a shape has to
    /// keep these too: dropping them deletes the backend's objects and the cache's
    /// files out from under a live proxy, and the failure lands as an I/O error from
    /// somewhere unrelated to whatever the test was asserting.
    pub dirs: Vec<tempfile::TempDir>,
}

/// Assemble a daemon over a fresh `s3s-fs` backend in a temp dir, with [`BUCKET`]
/// created.
///
/// # Panics
///
/// If the backend, the cache or the bucket cannot be brought up.
pub async fn daemon_core(spec: DaemonSpec) -> DaemonCore {
    let backend_dir = tempfile::tempdir().expect("a temp dir for the backend");
    let (service, creds) = fs_backend_service(backend_dir.path());
    let pair = BackendPair::over(&service, &creds);
    create_test_bucket(&pair.truth).await;
    let mut core = daemon_core_over(spec, pair).await;
    core.dirs.push(backend_dir);
    core
}

/// [`daemon_core`] over a backend the caller built — a fault-injecting one, a
/// request-recording one, or a real S3 bucket. The caller owns the bucket's existence:
/// this deliberately does not `CreateBucket`, because one of those backends is real.
///
/// # Panics
///
/// If the cache cannot be brought up.
pub async fn daemon_core_over(spec: DaemonSpec, backend: BackendPair) -> DaemonCore {
    let cache_dir = tempfile::tempdir().expect("a temp dir for the cache");
    let metrics = Metrics::new().expect("a fresh registry");
    let cache = if spec.foyer_metrics {
        build_cache_with_metrics(cache_dir.path(), spec.cache, &metrics).await
    } else {
        build_cache(cache_dir.path(), spec.cache).await
    };
    // No slab: these daemons have no RDMA plane, so cached chunks belong on the heap
    // (ADR-0028's default).
    let tier = ChunkTier::foyer(cache, Default::default());
    let proxy = PacerProxy::new(
        backend.daemon,
        tier.clone(),
        metrics.clone(),
        spec.min_object_size,
        spec.max_object_size,
        ChunkConfig::new(spec.chunk_size),
        spec.fill_parallelism,
    )
    .with_backend_type(spec.backend_type)
    .with_conditional_get_from_cache(spec.conditional_get_from_cache)
    .with_fill_coalesce(spec.fill_coalesce);
    DaemonCore {
        proxy,
        metrics,
        tier,
        backend: backend.truth,
        chunk_size: spec.chunk_size,
        dirs: vec![cache_dir],
    }
}

/// Seal a proxy behind placeholder auth. Both shapes start here.
pub(super) fn daemon_service(proxy: PacerProxy) -> S3Service {
    let mut b = S3ServiceBuilder::new(proxy);
    b.set_auth(PlaceholderAuth::new(
        PLACEHOLDER_KEY.to_owned(),
        PLACEHOLDER_KEY.to_owned(),
    ));
    b.build()
}

impl DaemonCore {
    /// Shape 1: the service plugged straight into the SDK's HTTP client. No socket, so
    /// nothing in ADR-0036 is observable — and nothing in ADR-0036 is charged for
    /// either, which is why most arms take this one.
    pub fn in_process(self) -> Daemon {
        let client = sdk_client_for(daemon_service(self.proxy), placeholder_credentials());
        Daemon {
            client,
            backend: self.backend,
            metrics: self.metrics,
            tier: self.tier,
            chunk_size: self.chunk_size,
            addr: None,
            shutdown: None,
            listener: None,
            _dirs: self.dirs,
        }
    }

    /// Shape 2: served on a kernel-assigned loopback port under `limits`, through the
    /// same [`pacer_daemon::listen::serve_s3_on`] `main` calls.
    ///
    /// The listener is bound here and handed over, so the test knows the port before the
    /// server exists — guessing a free port is how a socket suite becomes flaky on a
    /// busy runner.
    ///
    /// # Panics
    ///
    /// If loopback cannot be bound.
    pub async fn served(self, limits: ListenLimits) -> Daemon {
        let listener = tokio::net::TcpListener::bind(LOOPBACK_ANY_PORT)
            .await
            .expect("binding loopback");
        let addr = listener.local_addr().expect("a bound port has an address");
        let shutdown = Shutdown::new();
        let task = tokio::spawn(pacer_daemon::listen::serve_s3_on(
            listener,
            daemon_service(self.proxy),
            limits,
            self.metrics.clone(),
            shutdown.signal(),
        ));
        Daemon {
            client: tcp_client(addr),
            backend: self.backend,
            metrics: self.metrics,
            tier: self.tier,
            chunk_size: self.chunk_size,
            addr: Some(addr),
            shutdown: Some(shutdown),
            listener: Some(task),
            _dirs: self.dirs,
        }
    }
}

/// A live daemon, reachable, plus the handles a test needs to drive it.
///
/// Dropping it raises the shutdown flag and abandons the listener — see the [`Drop`]
/// impl for why that is not optional.
pub struct Daemon {
    /// A client that reaches this daemon: in process, or over TCP.
    pub client: aws_sdk_s3::Client,
    /// The oracle: same backend, no daemon in the path.
    pub backend: aws_sdk_s3::Client,
    /// This daemon's registry.
    pub metrics: Metrics,
    /// This daemon's chunk tier.
    pub tier: ChunkTier,
    /// This daemon's chunk size.
    pub chunk_size: u64,
    /// `Some(127.0.0.1:<port>)` when served over TCP; `None` in process.
    addr: Option<SocketAddr>,
    shutdown: Option<Shutdown>,
    listener: Option<tokio::task::JoinHandle<anyhow::Result<()>>>,
    _dirs: Vec<tempfile::TempDir>,
}

impl Daemon {
    /// This daemon's `/metrics` body.
    ///
    /// `Metrics::encode` is what the admin listener returns verbatim for `/metrics`
    /// (`health::handle`), so this is the same text a scrape would see.
    pub fn scrape(&self) -> String {
        self.metrics.encode()
    }

    /// `127.0.0.1:<port>`, for the arms that open their own sockets.
    ///
    /// # Panics
    ///
    /// If this daemon is served in process rather than on a port.
    pub fn addr(&self) -> SocketAddr {
        self.addr
            .expect("this daemon is served in process, not on a port")
    }

    /// Seed `key` through the backend as exactly `len` bytes, and return them so a test
    /// can compare what came out of the daemon against ground truth.
    ///
    /// # Panics
    ///
    /// If the seed write fails.
    pub async fn seed(&self, key: &str, len: u64) -> Bytes {
        let body = body_for_key(key, len);
        self.backend
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .expect("seeding an object");
        body
    }

    /// [`Self::seed`], sized as `chunks` × this daemon's chunk size.
    ///
    /// Always more than one chunk where a body has to *stream*: a single-chunk object
    /// could be produced in one piece, and a body that never streams cannot prove
    /// anything about a body that is mid-flight when something happens to it.
    ///
    /// # Panics
    ///
    /// If the seed write fails.
    pub async fn seed_chunks(&self, key: &str, chunks: u64) -> Bytes {
        self.seed(key, self.chunk_size * chunks).await
    }

    /// Read a whole object through the daemon, body included.
    ///
    /// The body is collected *here* on purpose: `send()` succeeding proves only that
    /// headers arrived, and a truncated body is the one failure shape a caller may not
    /// notice.
    ///
    /// # Panics
    ///
    /// If the read or the body fails.
    pub async fn get(&self, key: &str) -> Bytes {
        get_whole(&self.client, key).await
    }

    /// Read a whole object straight from the backend — the durability oracle.
    ///
    /// # Panics
    ///
    /// If the read or the body fails.
    pub async fn read_backend(&self, key: &str) -> Bytes {
        get_whole(&self.backend, key).await
    }

    /// Raise the shutdown flag, as SIGTERM does. No-op in process.
    ///
    /// The same code path `Shutdown::on_signal` takes after SIGTERM — the signal itself
    /// is process-global and cannot be raised inside one test of a shared binary.
    pub fn trigger_shutdown(&self) {
        if let Some(shutdown) = &self.shutdown {
            shutdown.trigger();
        }
    }

    /// Take the listener's handle, so a test can await or abort the drain itself.
    ///
    /// **This handle is the drain**: `main`'s is a `tokio::time::timeout` around exactly
    /// it. Taking it opts this daemon out of the [`Drop`] guard, which is correct — a
    /// test holding the handle is the thing driving the shutdown.
    ///
    /// # Panics
    ///
    /// If this daemon is served in process, or the handle was already taken.
    pub fn take_listener(&mut self) -> tokio::task::JoinHandle<anyhow::Result<()>> {
        self.listener
            .take()
            .expect("a served daemon's listener handle, taken once")
    }

    /// Trigger shutdown and require the listener to report a completed drain inside
    /// `deadline`.
    ///
    /// # Panics
    ///
    /// If the drain does not complete in time, if the listener task panicked, or if it
    /// exited with an error.
    pub async fn shutdown_and_drain(&mut self, deadline: Duration) {
        self.trigger_shutdown();
        let task = self.take_listener();
        tokio::time::timeout(deadline, task)
            .await
            .expect("the drain must report completion inside its deadline")
            .expect("the listener task must not panic")
            .expect("a drained listener exits Ok");
    }

    /// Trigger shutdown and give the listener [`PATIENCE`] to notice, asserting nothing
    /// — for the arms whose subject is not the drain.
    pub async fn shutdown_quietly(&mut self) {
        self.trigger_shutdown();
        if let Some(task) = self.listener.take() {
            let _ = tokio::time::timeout(PATIENCE, task).await;
        }
    }
}

/// Whether `addr` stops accepting connections within [`PATIENCE`].
///
/// Polled rather than asserted once: the accept loop observes the shutdown flag
/// asynchronously, so "refused" is an eventual property and a single immediate check
/// would be a race dressed up as an assertion. The listener is dropped when the accept
/// loop breaks, which the kernel then answers with `ECONNREFUSED`.
///
/// # Panics
///
/// If connections are still accepted after [`PATIENCE`].
pub async fn wait_until_connection_refused(addr: SocketAddr) {
    poll_until_within(
        "a new connection is refused once the drain has started",
        PATIENCE,
        || async {
            match tokio::net::TcpStream::connect(addr).await {
                Err(_) => true,
                // Accepted into the backlog by a listener that has not been dropped
                // yet; try again.
                Ok(stream) => {
                    drop(stream);
                    false
                }
            }
        },
    )
    .await;
}

/// Read a whole object through `client`.
///
/// # Panics
///
/// If the read or the body fails.
async fn get_whole(client: &aws_sdk_s3::Client, key: &str) -> Bytes {
    client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap_or_else(|e| panic!("GET {key}: {e}"))
        .body
        .collect()
        .await
        .expect("a whole body")
        .into_bytes()
}

impl Drop for Daemon {
    /// Stop a served daemon's accept loop.
    ///
    /// Not a nicety. The nine old binaries each ran in their own process, so a listener
    /// a test forgot to shut down died with it; in one merged binary it would outlive
    /// the test, keep its `Arc`s to a cache whose temp dir is being deleted, and answer
    /// on a port a later test could still be probing. Triggering the flag *and* aborting
    /// is deliberate: a drain cannot be awaited from `drop`, and a daemon whose drain a
    /// test actually cares about has taken the handle already ([`Daemon::take_listener`]),
    /// so there is nothing here to cut short.
    fn drop(&mut self) {
        self.trigger_shutdown();
        if let Some(task) = self.listener.take() {
            task.abort();
        }
    }
}
