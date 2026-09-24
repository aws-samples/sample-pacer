//! ADR-0040's single flight, counted at the backend: two clients asking for the same
//! cold bytes at the same time cost **one** ranged GET.
//!
//! # Why this arm needs a gate rather than just two concurrent GETs
//!
//! Firing two GETs and counting backend reads proves nothing on its own. If the second
//! arrives after the first has finished filling, it is an ordinary cache hit and the
//! count is 1 whether this ADR exists or not — so a test written that way passes against
//! the code it is meant to reject. The property is specifically about the *window* while
//! a fill is in flight, so the fixture has to hold the backend open inside that window:
//!
//! client A → daemon → backend GET (blocks here, having signalled that it arrived)
//! client B → daemon → finds the key being filled → parks
//! test     → sees `pacer_fill_waiters == 1` → releases the backend
//!
//! Every step waits on a *condition* through the fixture's [`poll_until`], never on a
//! fixed sleep: a timed test of a race passes on a quiet runner and fails on a busy one,
//! and a timeout that names what never happened is the difference between a diagnosis and
//! a shrug.
//!
//! # Why both arms of the knob run here
//!
//! `coalesced_reads_cost_one_backend_get` says the mechanism engages;
//! `without_coalescing_the_same_race_costs_two` says the choreography it engages *in* is
//! real. Without the second, a bug that made B's request never reach the daemon at all
//! would pass the first — the count would be 1 because nothing raced, and the arm would
//! be measuring its own fixture. They differ in exactly one value, `fill_coalesce`.
//!
//! The stack is `correctness.rs`'s in-process assembly with one wrapper added around the
//! backend, the same shape `backend_retry.rs` uses:
//!
//! client (aws-sdk-s3, placeholder creds)
//!   → S3Service[auth = PlaceholderAuth, s3 = PacerProxy]
//!     → aws-sdk-s3 (daemon identity)
//!       → S3Service[s3 = GatedFs → s3s_fs::FileSystem]

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use pacer_daemon::metrics::Metrics;
use s3s::dto;
use s3s::{S3Request, S3Response, S3Result};
use tokio::sync::Notify;

use crate::common::{
    backend_service, create_test_bucket, daemon_core_over, poll_until, seeded_body, BackendPair,
    Daemon, DaemonSpec, BUCKET,
};

/// Objects at or below this are proxied uncached, so the fixture must exceed it.
const MIN_OBJECT_SIZE: u64 = 4 << 20;
/// One chunk wider than the fixture object, so the whole read is a **single** chunk key.
///
/// Deliberately one and not several: the subject is what two requests for one key do to
/// each other, and a multi-chunk object would let the two clients' pipelines interleave
/// across keys, so a count of backend reads could come out right for the wrong reason.
const CHUNK_SIZE: u64 = 8 << 20;
/// The fixture object's size: above [`MIN_OBJECT_SIZE`] so it is cacheable, below
/// [`CHUNK_SIZE`] so it is exactly one chunk.
const OBJECT_SIZE: usize = (MIN_OBJECT_SIZE as usize) + (1 << 20);
/// Key under test. One object, read twice at once.
const KEY: &str = "checkpoint.safetensors";

/// An `s3s-fs` backend that counts ranged GETs and holds the first one open until the
/// test lets it go.
///
/// Only *ranged* GETs are gated and counted, because only those are the daemon's chunk
/// reads: a whole-object GET through here is the cache-bypass path and a `HeadObject` is
/// the header resolution, and neither is the subject.
struct GatedFs {
    inner: s3s_fs::FileSystem,
    /// Ranged GETs that have arrived, ever. The number this arm is really about.
    arrived: Arc<AtomicUsize>,
    /// Raised by the test to let the held read finish.
    release: Arc<Notify>,
}

#[async_trait::async_trait]
impl s3s::S3 for GatedFs {
    async fn create_bucket(
        &self,
        req: S3Request<dto::CreateBucketInput>,
    ) -> S3Result<S3Response<dto::CreateBucketOutput>> {
        self.inner.create_bucket(req).await
    }

    async fn put_object(
        &self,
        req: S3Request<dto::PutObjectInput>,
    ) -> S3Result<S3Response<dto::PutObjectOutput>> {
        self.inner.put_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        self.inner.head_object(req).await
    }

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        if req.input.range.is_none() {
            return self.inner.get_object(req).await;
        }
        // Held BEFORE the inner read, so the daemon has genuinely not received a byte
        // while the test sets the race up. The first arrival waits; any later one is let
        // straight through, which is what makes the no-coalescing arm able to observe its
        // own second read rather than deadlocking against this gate.
        let first = self.arrived.fetch_add(1, Ordering::SeqCst) == 0;
        if first {
            self.release.notified().await;
        }
        self.inner.get_object(req).await
    }
}

/// Everything an arm drives, with the backend's two controls hoisted out of it.
struct Harness {
    /// Held whole so the cache and backend directories outlive the test.
    daemon: Daemon,
    metrics: Metrics,
    arrived: Arc<AtomicUsize>,
    release: Arc<Notify>,
}

impl Harness {
    /// Ranged backend GETs so far — the count this whole file exists to assert on.
    fn ranged_gets(&self) -> usize {
        self.arrived.load(Ordering::SeqCst)
    }

    /// Requests parked on another request's fill right now, read off the gauge that
    /// exists to report exactly this.
    fn waiters(&self) -> i64 {
        self.metrics.fill_coalesce.waiters.get()
    }
}

/// Assemble the daemon over a gated backend, with `fill_coalesce` deciding whether
/// ADR-0040 is in the path at all.
async fn harness(fill_coalesce: bool) -> Harness {
    let backend_dir = tempfile::tempdir().unwrap();
    let arrived = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let (service, creds) = backend_service(GatedFs {
        inner: s3s_fs::FileSystem::new(backend_dir.path()).unwrap(),
        arrived: Arc::clone(&arrived),
        release: Arc::clone(&release),
    });
    let pair = BackendPair::over(&service, &creds);
    // Seeded before anything is gated: `create_bucket` is not a ranged GET, so it cannot
    // be the arrival the gate holds.
    create_test_bucket(&pair.truth).await;

    let mut core = daemon_core_over(
        DaemonSpec {
            min_object_size: MIN_OBJECT_SIZE,
            max_object_size: None,
            chunk_size: CHUNK_SIZE,
            fill_coalesce,
            ..DaemonSpec::default()
        },
        pair,
    )
    .await;
    core.dirs.push(backend_dir);
    let metrics = core.metrics.clone();

    Harness {
        daemon: core.in_process(),
        metrics,
        arrived,
        release,
    }
}

/// Position-dependent bytes, so a body assembled from the wrong offsets fails the
/// comparison instead of matching by luck.
fn fixture() -> Bytes {
    seeded_body(0x5a, OBJECT_SIZE)
}

/// Put the fixture through the daemon (a passthrough PUT — the gate only holds GETs).
async fn seed(h: &Harness, body: &Bytes) {
    h.daemon
        .client
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
}

/// One whole-object GET through the daemon, collected to bytes. Spawned by the arms, so
/// it owns everything it touches.
async fn get_whole(client: aws_sdk_s3::Client) -> Bytes {
    let out = client
        .get_object()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await
        .expect("the GET must succeed");
    out.body
        .collect()
        .await
        .expect("the body must stream")
        .into_bytes()
}

/// **The property.** Two clients read the same cold object at the same time; the backend
/// serves one ranged GET; both get the right bytes.
#[tokio::test(flavor = "multi_thread")]
async fn coalesced_reads_cost_one_backend_get() {
    let h = harness(true).await;
    let body = fixture();
    seed(&h, &body).await;

    // A: enters the gated read and stops there, so the fill is genuinely in flight.
    let first = tokio::spawn(get_whole(h.daemon.client.clone()));
    poll_until("the first ranged GET has reached the backend", || async {
        h.ranged_gets() == 1
    })
    .await;

    // B: arrives during that window, finds the key claimed, and parks. That it parks
    // rather than fetching is the whole mechanism, and `pacer_fill_waiters` is how the
    // test observes it without reaching inside the daemon.
    let second = tokio::spawn(get_whole(h.daemon.client.clone()));
    poll_until("the second read has parked on the first's fill", || async {
        h.waiters() == 1
    })
    .await;
    assert_eq!(
        h.ranged_gets(),
        1,
        "a parked read must not have issued a backend GET of its own"
    );

    h.release.notify_waiters();
    let (a, b) = (first.await.unwrap(), second.await.unwrap());

    assert_eq!(a, body, "the leading read must return the object");
    assert_eq!(
        b, body,
        "and the coalesced read must return the same bytes, not a truncated or empty body"
    );
    assert_eq!(
        h.ranged_gets(),
        1,
        "two concurrent reads of one chunk must cost exactly one backend ranged GET"
    );
    assert_eq!(h.metrics.fill_coalesce.served.get(), 1);
    assert_eq!(
        h.metrics.fill_coalesce.bytes.get(),
        OBJECT_SIZE as u64,
        "the bytes counter must report the backend traffic actually avoided"
    );
    assert_eq!(
        h.metrics.fill_coalesce.fallbacks.get(),
        0,
        "nothing fell back: the leader published"
    );
    assert_eq!(h.waiters(), 0, "and no waiter is left parked");
    // The leader still filled, so the chunk is cached for everyone after these two.
    assert_eq!(h.metrics.fills_completed.get(), 1);
}

/// The same race with `fill_coalesce` off, which is the only thing that makes the arm
/// above meaningful: it shows the second request really was concurrent, because without
/// the mechanism it reaches the backend on its own.
#[tokio::test(flavor = "multi_thread")]
async fn without_coalescing_the_same_race_costs_two() {
    let h = harness(false).await;
    let body = fixture();
    seed(&h, &body).await;

    let first = tokio::spawn(get_whole(h.daemon.client.clone()));
    poll_until("the first ranged GET has reached the backend", || async {
        h.ranged_gets() == 1
    })
    .await;

    // Not gated (only the first arrival is), so this one runs to completion while the
    // leader is still held — which is exactly the duplicated read ADR-0040 removes.
    let second = tokio::spawn(get_whole(h.daemon.client.clone()));
    poll_until("the second read has issued its own backend GET", || async {
        h.ranged_gets() == 2
    })
    .await;

    h.release.notify_waiters();
    let (a, b) = (first.await.unwrap(), second.await.unwrap());

    assert_eq!(a, body);
    assert_eq!(b, body, "the old path is correct, just wasteful");
    assert_eq!(
        h.ranged_gets(),
        2,
        "with coalescing off the same two reads must cost two backend GETs"
    );
    assert_eq!(
        h.metrics.fill_coalesce.served.get(),
        0,
        "and nothing may be counted as coalesced"
    );
    // And the old path wrote the chunk TWICE, which is worth pinning because it is one
    // more than the account of it suggests. The fill guard dedupes two inserts only while
    // they OVERLAP: here B's read finishes while A is still held at the gate, so B claims,
    // inserts and releases, and A then finds the key free and inserts the same bytes over
    // the top. Two simultaneous reads that happen to return together would still collapse
    // to one insert — so this is the pre-ADR-0040 path's *timing-dependent* half, and
    // ADR-0040 makes it unconditional by having only the leader insert at all.
    assert_eq!(
        h.metrics.fills_completed.get(),
        2,
        "with reads that do not overlap, the guard dedupes nothing and the chunk is \
         written twice"
    );
}
