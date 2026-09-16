//! The one fixture every arm of `tests/daemon.rs` is built from.
//!
//! Before this module the nine integration binaries under `crates/pacer-daemon/tests/`
//! each carried their own copy of the same bring-up — `sdk_client_for` appeared six
//! times, the `s3s-fs`-behind-an-`S3Service` backend five, the foyer cache config
//! seven, and a fixed `tokio::time::sleep` stood in for a poll eight times. A change
//! to the daemon's constructor meant nine edits, and a fixture bug had nine places to
//! hide.
//!
//! # Two shapes, one core
//!
//! [`DaemonCore`] assembles the daemon exactly once — placeholder auth → [`PacerProxy`]
//! → a foyer hybrid cache → an `s3s-fs` backend reached with the daemon's own identity
//! (strip-and-re-sign, ADR-0006). What differs between arms is only how a client
//! reaches it:
//!
//! * [`DaemonCore::in_process`] plugs the `S3Service` straight into the SDK's HTTP
//!   client (`s3s_aws::Client::from`), which never opens a socket. Cheapest, and blind
//!   to everything in ADR-0036 — connection caps, socket deadlines, drains.
//! * [`DaemonCore::served`] binds a loopback port and hands it to
//!   [`pacer_daemon::listen::serve_s3_on`], so the socket layer is real. The listener
//!   is bound *here* and handed over, so a test knows the port before the server
//!   exists: guessing a free port is how a socket suite becomes flaky on a busy
//!   runner.
//!
//! Both hand back a [`Daemon`], whose [`Drop`] raises the shutdown flag and abandons
//! the listener. That is the point of the type: a served daemon that a test forgot to
//! tear down used to leak an accept loop into every later test in the same binary, and
//! merging nine binaries into one made that a cross-file hazard rather than a local
//! one.
//!
//! A test that needs the [`PacerProxy`] itself rather than a client — because the
//! protocol under test lives in response headers the SDK's modelled output drops —
//! keeps the [`DaemonCore`] and never chooses a shape. `daemon/delivery.rs` is that
//! case, and it is the only one.
//!
//! # Multi-node
//!
//! [`node_parts`] and [`serve_node`] are the same assembly per node, split at the one
//! point where the two multi-node arms diverge: `daemon/cluster.rs` wants a plain
//! proxy, `daemon/scatter/` wants one carrying a [`ScatterCoordinator`] and a peer
//! server sharing that node's [`StagingArea`]. Everything either of them does before
//! and after that point is here.

// Not every arm uses every helper — `daemon/backend_matrix_s3.rs` is `#[ignore]`d by
// default and `daemon/delivery.rs` never takes a shape — so an unused-code warning
// here would only be a report about which arms ran, and `-D warnings` would make it
// fatal.
#![allow(dead_code)]

mod fleet;
mod single;

// Flattened back into `common::` so a call site never has to know which file of the
// fixture a helper lives in — the split is here to keep three subjects apart, not to
// make the callers spell it out.
pub use fleet::{node_parts, serve_node, NodeParts, NodeSpec};
pub use single::{
    daemon_core, daemon_core_over, wait_until_connection_refused, BackendPair, Daemon, DaemonSpec,
};

use std::future::Future;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;

use aws_sdk_s3::config::{Credentials, Region};
use bytes::Bytes;
use pacer_daemon::metrics::Metrics;
use s3s::service::{S3Service, S3ServiceBuilder};

// ------------------------------------------------------------------- identities

/// Bucket every arm reads and writes. One name across the suite, because a test that
/// creates one bucket and addresses another fails as "NoSuchBucket" from the backend
/// rather than as the assertion it meant to make.
pub const BUCKET: &str = "test-bucket";

/// Region the in-process clients are configured for. Never resolved — the endpoint is
/// overridden — but SigV4 needs a region to sign with, and it has to be the same one
/// on both sides of the hop or the daemon's re-sign fails.
pub const TEST_REGION: &str = "us-east-2";

/// Endpoint the in-process clients address. Never dialled: `s3s_aws::Client` answers
/// the request in process, so this only has to be a syntactically valid authority.
pub const DAEMON_ENDPOINT: &str = "http://pacer.local";

/// Bind address for anything that wants a real port: loopback, kernel-assigned.
pub const LOOPBACK_ANY_PORT: &str = "127.0.0.1:0";

/// The access key a client presents to the daemon. [`PlaceholderAuth`] is configured
/// with the same pair, so this is the *only* key the daemon's front end accepts —
/// which is what `wrong_placeholder_credentials_rejected` relies on.
pub const PLACEHOLDER_KEY: &str = "pacer";

/// The daemon's own identity at the backend. Distinct from [`PLACEHOLDER_KEY`] on
/// purpose: the strip-and-re-sign hop is only observable if the two differ.
pub const DAEMON_KEY: &str = "daemon";

/// Secret for [`DAEMON_KEY`].
pub const DAEMON_SECRET: &str = "daemon-secret";

/// Credentials a client presents to the daemon's placeholder auth.
pub fn placeholder_credentials() -> Credentials {
    Credentials::new(PLACEHOLDER_KEY, PLACEHOLDER_KEY, None, None, "placeholder")
}

/// Credentials the daemon presents to its backend.
pub fn daemon_credentials() -> Credentials {
    Credentials::new(DAEMON_KEY, DAEMON_SECRET, None, None, "test")
}

// ------------------------------------------------------------ production defaults
//
// Values the daemon ships with, restated once here so an arm that does not care about
// a knob cannot accidentally test a different daemon than production runs.

/// In-flight chunk resolutions per GET.
pub const FILL_PARALLELISM: usize = 8;

/// Peer chunk-channel buffering.
pub const CHANNEL_CAPACITY: usize = 8;

/// Layer-1 admission threshold (ADR-0016): admit a peer-owned chunk on its 2nd fetch
/// inside the window.
pub const LOCAL_ADMISSION_THRESHOLD: u32 = 2;

/// Layer-1 admission window.
pub const LOCAL_ADMISSION_WINDOW: Duration = Duration::from_secs(60);

/// Layer-1 admission sampling ratio. 1.0 = admit every eligible chunk, so nothing in
/// the suite depends on a coin flip.
pub const LOCAL_ADMISSION_RATIO: f64 = 1.0;

// ------------------------------------------------------------------- time budgets

/// Interval between polls in [`poll_until`]. Short enough that a settled condition is
/// observed promptly, long enough that the loop is not a spin.
pub const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// Polls a fire-and-forget effect gets before [`poll_until`] calls it lost.
pub const SETTLE_POLLS: usize = 200;

/// Total budget [`poll_until`] allows — [`SETTLE_POLLS`] × [`POLL_INTERVAL`]. Two
/// seconds: orders of magnitude above the sub-millisecond a spawned fill, commit or
/// announce takes in process, and short enough that a genuinely absent effect fails
/// the run rather than stalling it.
pub const SETTLE_BUDGET: Duration = Duration::from_secs(2);

/// Ceiling on any single "this must happen" wait that crosses a real socket.
/// Generously above every deadline under test, so a loaded runner cannot fail an
/// assertion by being slow, while still failing fast when the behaviour is absent.
pub const PATIENCE: Duration = Duration::from_secs(10);

/// Window in which a thing that must NOT happen is given every chance to. Longer than
/// a loopback round trip by orders of magnitude.
pub const SETTLE: Duration = Duration::from_millis(500);

/// Ceiling on a whole test body. A regression that deadlocks a budget then hangs for
/// seconds and names itself rather than hanging the suite: an exhausted budget that
/// waits where it should refuse is exactly the failure shape that presents as "the run
/// never finished".
pub const TEST_BUDGET: Duration = Duration::from_secs(60);

// ------------------------------------------------------------------------ clients

/// An SDK client wired straight into an in-process [`S3Service`] — no socket.
pub fn sdk_client_for(service: S3Service, creds: Credentials) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        base_config(creds)
            .http_client(s3s_aws::Client::from(service))
            .build(),
    )
}

/// [`sdk_client_for`] with an interceptor attached, so a test can assert on the
/// request the daemon *produces* rather than only on the one its client supplied.
/// `daemon/write_framing.rs` is the reason this exists: no public `aws-sdk-s3` API
/// exposes the final wire shape any other way.
pub fn sdk_client_intercepting<I>(
    service: S3Service,
    creds: Credentials,
    interceptor: I,
) -> aws_sdk_s3::Client
where
    I: aws_smithy_runtime_api::client::interceptors::Intercept + 'static,
{
    aws_sdk_s3::Client::from_conf(
        base_config(creds)
            .http_client(s3s_aws::Client::from(service))
            .interceptor(interceptor)
            .build(),
    )
}

/// An SDK client that speaks real HTTP/1.1 to `addr`, with its own connection pool.
pub fn tcp_client(addr: SocketAddr) -> aws_sdk_s3::Client {
    aws_sdk_s3::Client::from_conf(
        base_config(placeholder_credentials())
            .endpoint_url(format!("http://{addr}"))
            .build(),
    )
}

/// Everything the suite's clients agree on, whatever they talk to.
fn base_config(creds: Credentials) -> aws_sdk_s3::config::Builder {
    aws_sdk_s3::Config::builder()
        .credentials_provider(creds)
        .region(Region::new(TEST_REGION))
        .endpoint_url(DAEMON_ENDPOINT)
        .force_path_style(true)
        .behavior_version(aws_sdk_s3::config::BehaviorVersion::latest())
}

// ------------------------------------------------------------------------ backend

/// Wrap any `S3` implementation as the suite's signed backend, and hand back the
/// identity that reaches it.
///
/// Generic on purpose: two arms substitute their own `S3` in front of `s3s-fs` — a
/// checksum-enforcing, fault-injecting one (`daemon/scatter/`) and one that severs
/// response bodies mid-stream (`daemon/backend_retry.rs`) — and both want the rest of
/// the bring-up unchanged.
pub fn backend_service<S: s3s::S3 + 'static>(s3: S) -> (S3Service, Credentials) {
    let mut b = S3ServiceBuilder::new(s3);
    b.set_auth(s3s::auth::SimpleAuth::from_single(
        DAEMON_KEY,
        DAEMON_SECRET,
    ));
    (b.build(), daemon_credentials())
}

/// `s3s-fs` over `dir`, behind the suite's signed [`S3Service`].
///
/// # Panics
///
/// If `dir` cannot back a filesystem bucket store.
pub fn fs_backend_service(dir: &Path) -> (S3Service, Credentials) {
    backend_service(s3s_fs::FileSystem::new(dir).expect("s3s-fs over a temp dir"))
}

/// Create [`BUCKET`]. Every arm's first act, and not one of them tolerates failure.
///
/// # Panics
///
/// If the bucket cannot be created.
pub async fn create_test_bucket(client: &aws_sdk_s3::Client) {
    client
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("creating the test bucket");
}

// -------------------------------------------------------------------------- cache

/// The three foyer capacities an arm might care about.
///
/// A struct with a documented [`Default`] rather than a builder: an arm overrides the
/// one field its subject is about (`CacheSpec { mem_capacity: .., ..default() }`), so
/// the diff between two arms is exactly the knob that differs.
#[derive(Debug, Clone, Copy)]
pub struct CacheSpec {
    /// In-memory tier capacity, in bytes.
    pub mem_capacity: usize,
    /// Disk tier capacity, in bytes. Sparse on every filesystem the suite runs on.
    pub disk_capacity: usize,
    /// Disk block size — foyer's eviction unit **and** the largest cacheable entry, so
    /// it must exceed every chunk size an arm uses.
    pub block_size: usize,
}

impl Default for CacheSpec {
    fn default() -> Self {
        Self {
            mem_capacity: 256 << 20,
            disk_capacity: 1 << 30,
            block_size: 64 << 20,
        }
    }
}

impl CacheSpec {
    /// The `pacer_cache` config this spec denotes, over `dir`.
    ///
    /// `max_object_bytes: None` takes the crate's default throughout the suite: it is
    /// the legacy pre-chunking (ADR-0002) whole-object admission ceiling, and every
    /// read here goes through the chunked path, whose own size gate is the proxy's
    /// `min_object_size`.
    fn config(self, dir: &Path) -> pacer_cache::CacheConfig {
        pacer_cache::CacheConfig {
            dir: dir.to_path_buf(),
            mem_capacity: self.mem_capacity,
            disk_capacity: self.disk_capacity,
            block_size: self.block_size,
            flush_buffer_size: 0,
            io_engine: pacer_cache::IoEngine::Psync,
            uring: pacer_cache::UringConfig::default(),
            tuning: pacer_cache::StorageTuning::default(),
            max_object_bytes: None,
        }
    }
}

/// Build the chunk-granular hybrid cache under `dir`.
///
/// # Panics
///
/// If foyer cannot open the directory.
pub async fn build_cache(dir: &Path, spec: CacheSpec) -> pacer_cache::ChunkCache {
    pacer_cache::build_chunk_cache(&spec.config(dir))
        .await
        .expect("building the hybrid cache")
}

/// [`build_cache`], with foyer's own metrics registered into the daemon's registry —
/// the wiring `main` does, and the only way the memory tier's residency reaches
/// `/metrics`. Without it the tier under test has no series at all and a ceiling could
/// only be argued, not read.
///
/// # Panics
///
/// If foyer cannot open the directory.
pub async fn build_cache_with_metrics(
    dir: &Path,
    spec: CacheSpec,
    metrics: &Metrics,
) -> pacer_cache::ChunkCache {
    let registry: mixtrics::metrics::BoxedRegistry = Box::new(
        mixtrics::registry::prometheus_0_14::PrometheusMetricsRegistry::new(
            metrics.registry().clone(),
        ),
    );
    pacer_cache::build_chunk_cache_with_metrics(&spec.config(dir), registry)
        .await
        .expect("building the hybrid cache")
}

// -------------------------------------------------------------------------- bodies

/// A body whose bytes depend on `seed` and on their own offset, so a misplaced,
/// duplicated or short chunk shows up as wrong bytes rather than as bytes that happen
/// to match.
///
/// 251 is the largest prime below 256: the period is coprime with every power-of-two
/// chunk size in the suite, so no chunk boundary ever lands on a repeat.
pub fn seeded_body(seed: u8, len: usize) -> Bytes {
    let mut v = Vec::with_capacity(len);
    for i in 0..len {
        v.push(seed.wrapping_add((i % 251) as u8));
    }
    Bytes::from(v)
}

/// [`seeded_body`] with the seed derived from `key`, for the arms that write several
/// objects at once and need each one's bytes to identify it.
pub fn body_for_key(key: &str, len: u64) -> Bytes {
    let seed = key
        .bytes()
        .fold(1u8, |a, b| a.wrapping_mul(31).wrapping_add(b));
    seeded_body(
        seed,
        usize::try_from(len).expect("a test body fits in memory"),
    )
}

// ---------------------------------------------------------------------- polling

/// Wait until `ready` reports true, polling every [`POLL_INTERVAL`] for up to
/// [`SETTLE_BUDGET`].
///
/// Replaces the fixed `tokio::time::sleep` that stood in for this eight times across
/// the old binaries. A sleep is wrong in both directions: too short and the assertion
/// races the effect, too long and every run pays for the worst case. `awaited` names
/// the effect in the present tense ("the owner has filled chunk 0"), because that
/// string is the entire failure message when it never arrives.
///
/// # Panics
///
/// If `ready` has not reported true within [`SETTLE_BUDGET`].
pub async fn poll_until<F, Fut>(awaited: &str, ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    poll_until_within(awaited, SETTLE_BUDGET, ready).await;
}

/// [`poll_until`] with an explicit budget, for the effects that cross a real socket
/// and so want [`PATIENCE`] rather than [`SETTLE_BUDGET`].
///
/// # Panics
///
/// If `ready` has not reported true within `budget`.
pub async fn poll_until_within<F, Fut>(awaited: &str, budget: Duration, mut ready: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + budget;
    loop {
        if ready().await {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "waited {budget:?} for: {awaited}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Watch `unwanted` for [`SETTLE`] and fail the moment it becomes true.
///
/// The negative counterpart of [`poll_until`], and the honest replacement for the
/// `sleep`-then-assert that used to stand where this is called. A negative cannot be
/// *polled to a conclusion* — waiting longer never turns "not yet" into "never" — so the
/// window is still a window, and it is still the thing bounding the claim. What changes
/// is that the condition is checked throughout the window instead of once at its end: a
/// fill that lands 1 ms in fails here, named, where the sleep reported the same failure
/// 99 ms later with no indication of when it happened or how close the call was.
///
/// # Panics
///
/// If `watched` reports true at any point inside [`SETTLE`].
pub async fn stays_false<F, Fut>(unwanted: &str, mut watched: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + SETTLE;
    while tokio::time::Instant::now() < deadline {
        assert!(!watched().await, "must not happen, and did: {unwanted}");
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// Watch, for [`SETTLE`], that `metrics` records no chunk fill beyond `allowed`.
///
/// The shape four arms need: a read that must populate nothing (`no-store`, a
/// cache-control bypass, a non-home requester, a peer fallback) has to be shown not to
/// have filled, and "it has not filled *yet*" is what a bare assertion after the body
/// drains would prove.
///
/// # Panics
///
/// If a fill settles inside [`SETTLE`].
pub async fn no_fills_beyond(metrics: &Metrics, allowed: u64) {
    stays_false(
        &format!("a chunk fill past {allowed} settled on a read that must populate nothing"),
        || async { metrics.fills_completed.get() + metrics.fills_aborted.get() > allowed },
    )
    .await;
}

/// Poll until at least `want` chunk fills have completed *or* aborted on `metrics`.
///
/// A fill is a spawned tee that settles as the response body streams, so a test that
/// asserts on it the instant the body drains is racing it. Aborts count because the
/// question is whether the fill pipeline has *settled*, not whether it succeeded — an
/// arm that cares about the difference asserts on `fills_completed` afterwards.
///
/// # Panics
///
/// If fewer than `want` fills have settled within [`SETTLE_BUDGET`].
pub async fn wait_for_fills(metrics: &Metrics, want: u64) {
    poll_until(&format!("{want} chunk fill(s) settle"), || async {
        metrics.fills_completed.get() + metrics.fills_aborted.get() >= want
    })
    .await;
}

// -------------------------------------------------------------------- /metrics

/// One series' value out of a Prometheus text exposition, by name and labels.
///
/// `labels` is matched as a subset, so `&[]` addresses an unlabelled series and
/// `&[("op", "evict")]` picks one child of a vec. **Exactly one line must match**: a
/// series that is absent and a label set that under-specifies its family are both test
/// bugs, and both would otherwise pass silently — an absent counter read as 0 makes
/// "the tier evicted" indistinguishable from "the series was renamed".
///
/// # Panics
///
/// If no line matches, or more than one does; the message lists every line whose name
/// matched, so the diagnosis is in the failure rather than a rerun away.
pub fn metric_value(text: &str, name: &str, labels: &[(&str, &str)]) -> f64 {
    let mut seen: Vec<&str> = Vec::new();
    let mut matched: Vec<f64> = Vec::new();
    for line in text.lines().filter(|l| !l.starts_with('#')) {
        let Some(rest) = line.strip_prefix(name) else {
            continue;
        };
        // The name must end where the series' name ends, or `foo_total` would also
        // match `foo_total_sum` and `foo_total_bucket`.
        let (block, value) = match rest.as_bytes().first() {
            Some(b'{') => {
                let Some(end) = rest.find('}') else { continue };
                (&rest[1..end], rest[end + 1..].trim())
            }
            Some(b' ') => ("", rest.trim()),
            _ => continue,
        };
        seen.push(line);
        if labels
            .iter()
            .all(|(k, v)| block.contains(&format!("{k}=\"{v}\"")))
        {
            matched.push(
                value
                    .parse()
                    .unwrap_or_else(|e| panic!("{name} has a non-numeric value {value:?}: {e}")),
            );
        }
    }
    assert_eq!(
        matched.len(),
        1,
        "expected exactly one {name}{labels:?} in /metrics, found {}; lines with that \
         name:\n{}",
        matched.len(),
        seen.join("\n")
    );
    matched[0]
}

/// Every non-zero series under `prefix`, as text.
///
/// Goes in a failure message so a test that finds a path did not engage also says
/// *why* — the daemon's declines and refusals are labelled by reason precisely so that
/// question has an answer.
pub fn nonzero_lines(text: &str, prefix: &str) -> String {
    text.lines()
        .filter(|line| line.starts_with(prefix) && !line.ends_with(" 0"))
        .collect::<Vec<_>>()
        .join("\n")
}
