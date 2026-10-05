//! A daemon restarted over the cache directory it left behind (#64).
//!
//! The cache directory is a `hostPath` (`cache.hostPath` in the chart), so it outlives the
//! pod: a rolling update, an OOM kill and a crash all bring the next daemon up over the
//! previous one's files, and both disk tiers rebuild from them — the slot store from its
//! slot headers (ADR-0033), foyer from its blocks (recovery on by default, every in-memory
//! entry flushed to disk on close). Each arm here closes one daemon the way `main`'s drain
//! does, overwrites an object one way or another, opens a second daemon over the same
//! directory and the same backend, and requires the **current** bytes back.
//!
//! The overwrite always keeps the object's length. A different length would change the
//! header's `object_len` and so the covering chunks' bounds, but not their keys
//! (`{object_key}#{chunk_size}:{index}`) — same-length is the case where nothing but a
//! version check can tell the old chunks from the new ones.

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use rstest::rstest;

use crate::common::{
    create_test_bucket, daemon_core_in, fs_backend_service, metric_value, seeded_body,
    wait_for_fills, BackendPair, CacheSpec, Daemon, DaemonSpec, TierKind, BUCKET,
};

/// Chunks in the object under test: more than one, so a stale read is a stale
/// *chunk set* rather than one entry, and the object clears the default admission floor
/// (4 MiB at the default 1 MiB chunk) with room to spare.
const OBJECT_CHUNKS: u64 = 6;

/// Seed of the body every arm reads first.
const OLD_SEED: u8 = 1;
/// Seed of the overwrite — any other value; [`seeded_body`] makes the two differ at
/// every offset.
const NEW_SEED: u8 = 2;

/// A memory tier smaller than the object, so foyer has demoted its chunks to disk
/// before the overwrite: a chunk foyer still held in memory would be removed outright
/// and never flushed, and the arm would pass without exercising recovery at all.
const SMALL_MEM_TIER: usize = 2 << 20;

/// The key every arm reads.
const KEY: &str = "restart/overwritten.bin";

/// One backend and one cache directory, outliving any number of daemons over them.
struct Node {
    backend: s3s::service::S3Service,
    creds: aws_sdk_s3::config::Credentials,
    kind: TierKind,
    spec: DaemonSpec,
    cache_dir: tempfile::TempDir,
    _backend_dir: tempfile::TempDir,
}

impl Node {
    async fn new(kind: TierKind, cache: CacheSpec) -> Self {
        let backend_dir = tempfile::tempdir().expect("a temp dir for the backend");
        let (backend, creds) = fs_backend_service(backend_dir.path());
        create_test_bucket(&BackendPair::over(&backend, &creds).truth).await;
        Self {
            backend,
            creds,
            kind,
            spec: DaemonSpec {
                cache,
                ..DaemonSpec::default()
            },
            cache_dir: tempfile::tempdir().expect("a temp dir for the cache"),
            _backend_dir: backend_dir,
        }
    }

    /// Start a daemon over this node's directory — a fresh one the first time, a
    /// restart every time after.
    async fn start(&self) -> Daemon {
        daemon_core_in(
            self.spec,
            BackendPair::over(&self.backend, &self.creds),
            self.cache_dir.path(),
            self.kind,
        )
        .await
        .in_process()
    }

    fn object_len(&self) -> usize {
        usize::try_from(self.spec.chunk_size * OBJECT_CHUNKS).expect("a test object fits")
    }
}

/// Stop `daemon` the way `main`'s drain does: close the tier, which flushes foyer's
/// memory tier to disk.
async fn stop(daemon: Daemon) {
    daemon
        .tier
        .cache()
        .close()
        .await
        .expect("closing the cache tier");
}

/// Write `body` under [`KEY`] with `client` — the daemon's for a write through PACER,
/// the backend's for one around it.
async fn put(client: &aws_sdk_s3::Client, body: &Bytes) {
    client
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .expect("overwriting the object");
}

/// Read [`KEY`] through `daemon` and fail unless the bytes are `want`, naming which
/// version came back instead — "the old body" is the bug, anything else is a different one.
async fn assert_serves(daemon: &Daemon, want: &Bytes, old: &Bytes, when: &str) {
    let got = daemon.get(KEY).await;
    assert!(
        got == *want,
        "{when}: the daemon served {} bytes that are {}",
        got.len(),
        if got == *old {
            "the OLD body — the pre-overwrite version came back"
        } else {
            "neither version"
        }
    );
}

/// Read the object once so every chunk is filled, and wait until the fills land.
async fn warm(daemon: &Daemon, old: &Bytes) {
    assert_serves(daemon, old, old, "the first read").await;
    wait_for_fills(&daemon.metrics, OBJECT_CHUNKS).await;
}

/// The issue's case: the object is overwritten while this node's daemon is down, so no
/// invalidation can reach it. The restarted daemon must not serve what it cached before.
#[rstest]
#[case::foyer(TierKind::Foyer)]
#[case::store(TierKind::Store)]
#[tokio::test(flavor = "multi_thread")]
async fn an_overwrite_while_down_is_served_after_restart(#[case] kind: TierKind) {
    let node = Node::new(kind, CacheSpec::default()).await;
    let old = seeded_body(OLD_SEED, node.object_len());
    let new = seeded_body(NEW_SEED, node.object_len());

    let first = node.start().await;
    put(&first.backend, &old).await;
    warm(&first, &old).await;
    stop(first).await;

    // Around PACER, with no daemon running: nothing is told.
    let truth = BackendPair::over(&node.backend, &node.creds).truth;
    put(&truth, &new).await;

    let second = node.start().await;
    assert_serves(&second, &new, &old, "after restart").await;
}

/// A write through PACER invalidates this node before it stops — and the restart must
/// not undo that invalidation by recovering the removed entries from disk.
///
/// `read_before_stop` is the confound this arm exists to keep visible. A read between the
/// write and the stop refills every chunk, and the store hands a fill the slot freed most
/// recently — the one its stale copy just left — so the refill overwrites the old slot
/// header on disk and there is nothing left to recover. Without that read the invalidated
/// slots are still on disk at the restart, which is the case a write followed by a
/// rolling update produces.
#[rstest]
#[tokio::test(flavor = "multi_thread")]
async fn an_invalidation_before_restart_survives_it(
    #[values(
        (TierKind::Foyer, CacheSpec { mem_capacity: SMALL_MEM_TIER, ..CacheSpec::default() }),
        (TierKind::Store, CacheSpec::default())
    )]
    tier: (TierKind, CacheSpec),
    #[values(true, false)] read_before_stop: bool,
) {
    let node = Node::new(tier.0, tier.1).await;
    let old = seeded_body(OLD_SEED, node.object_len());
    let new = seeded_body(NEW_SEED, node.object_len());

    let first = node.start().await;
    put(&first.backend, &old).await;
    warm(&first, &old).await;
    put(&first.client, &new).await;
    if read_before_stop {
        assert_serves(&first, &new, &old, "after the write, before restart").await;
    }
    stop(first).await;

    let second = node.start().await;
    assert_serves(&second, &new, &old, "after restart").await;
}

/// What the check above must not cost: a restart over an **unchanged** object serves
/// every chunk from the tier it recovered, after exactly one `HeadObject` to confirm the
/// version (ADR-0049). Without this arm, the two above would also pass on a daemon that
/// simply never trusted its disk tier again — correct, and a full re-read of every
/// checkpoint after each rolling update.
#[rstest]
#[case::foyer(TierKind::Foyer)]
#[case::store(TierKind::Store)]
#[tokio::test(flavor = "multi_thread")]
async fn an_unchanged_object_is_served_from_the_recovered_tier(#[case] kind: TierKind) {
    let node = Node::new(kind, CacheSpec::default()).await;
    let old = seeded_body(OLD_SEED, node.object_len());

    let first = node.start().await;
    put(&first.backend, &old).await;
    warm(&first, &old).await;
    stop(first).await;

    let second = node.start().await;
    assert_serves(&second, &old, &old, "after restart").await;
    let text = second.scrape();
    assert_eq!(
        metric_value(
            &text,
            "pacer_cache_revalidations_total",
            &[("outcome", "current")]
        ),
        1.0,
        "the recovered header is confirmed once, by one HeadObject"
    );
    assert_eq!(
        second.metrics.fills_completed.get(),
        0,
        "every chunk came from the recovered tier: nothing was re-read and re-filled"
    );
    assert_eq!(
        second.metrics.stale_chunks.get(),
        0,
        "no recovered chunk was refused"
    );
    assert_eq!(
        second.metrics.cache_hits.get(),
        OBJECT_CHUNKS,
        "one hit per chunk"
    );
}
