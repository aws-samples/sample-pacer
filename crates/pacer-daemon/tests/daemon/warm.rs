//! ADR-0048: warming the cache ahead of first read, end to end — the `pacer-daemon warm`
//! command's own [`run`] driving a daemon over an `s3s-fs` backend.
//!
//! The command is driven rather than a hand-built request because the command *is* the
//! contract's other half: it adds the warm header and the slice `Range` after signing, and
//! reads an answer the SDK's modelled output drops. A test that built its own request would
//! prove the daemon agrees with the test, not with the tool a Job runs.
//!
//! What a warm must leave behind is asserted through what a later read costs: every chunk
//! a hit, and no backend read. The cluster arm (a warm through a non-home admits no local
//! copy) is in `cluster.rs`, beside the layer-1 admission tests it is the counterpart of,
//! and the `auth.mode: requester` arm is in `authz_requester_get.rs`, which owns that
//! fixture.

// Tests are linear scenarios; splitting them to satisfy a line count would hurt
// readability (CLAUDE.md: size limits target production code).
#![allow(clippy::too_many_lines)]

use aws_sdk_s3::primitives::ByteStream;
use pacer_daemon::warm::command::{run, WarmArgs};
use pacer_daemon::warm::plan::Source;
use pacer_daemon::warm::{SkipReason, WARM_HEADER};

use crate::common::{daemon_core, seeded_body, wait_for_fills, Daemon, DaemonSpec, BUCKET};

/// Cache admission floor: only objects strictly larger are admitted (ADR-0002).
const MIN_OBJECT_SIZE: u64 = 4 << 20;
/// Small chunk so a few-MiB object spans several chunks.
const CHUNK_SIZE: u64 = 1 << 20;
/// Five and a half chunks: a whole-chunk middle and a partial last chunk, above the floor.
const OBJECT_LEN: u64 = 5 * CHUNK_SIZE + CHUNK_SIZE / 2;
/// [`OBJECT_LEN`] in chunks, kept beside it so the hit counts below cannot drift from it.
const OBJECT_CHUNKS: u64 = 6;

async fn harness() -> Daemon {
    daemon_core(DaemonSpec {
        min_object_size: MIN_OBJECT_SIZE,
        max_object_size: None,
        chunk_size: CHUNK_SIZE,
        ..DaemonSpec::default()
    })
    .await
    .in_process()
}

/// Put an [`OBJECT_LEN`] object at `key`, straight into the backend so nothing is cached.
async fn seed(h: &Daemon, key: &str, len: u64) {
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(seeded_body(7, len as usize)))
        .send()
        .await
        .unwrap();
}

/// The command's arguments for `uris`, at its defaults.
fn warm_args(uris: &[&str]) -> WarmArgs {
    WarmArgs {
        endpoint: "in-process".into(),
        sources: uris.iter().map(|u| Source::parse(u).unwrap()).collect(),
        manifest: None,
        concurrency: 4,
        slice_bytes: 1 << 30,
        max_bytes: None,
        dry_run: false,
    }
}

/// `uri` for `key` in the suite's bucket.
fn uri(key: &str) -> String {
    format!("s3://{BUCKET}/{key}")
}

/// Read `key` whole through the daemon.
async fn read(h: &Daemon, key: &str) -> usize {
    h.client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes()
        .len()
}

fn warm_outcome(h: &Daemon, outcome: &str) -> u64 {
    h.metrics.warm.requests.with_label_values(&[outcome]).get()
}

#[tokio::test]
async fn a_warm_fills_every_chunk_so_the_first_read_is_all_hits() {
    let h = harness().await;
    seed(&h, "model/weights.bin", OBJECT_LEN).await;

    let summary = run(&h.client, &warm_args(&[&uri("model/weights.bin")]))
        .await
        .unwrap();
    assert_eq!(summary.objects, 1);
    assert_eq!(summary.warmed_bytes, OBJECT_LEN);
    assert_eq!(summary.drained_bytes, 0, "the daemon understood the warm");
    assert_eq!(summary.failed, 0);
    wait_for_fills(&h.metrics, OBJECT_CHUNKS).await;
    assert_eq!(warm_outcome(&h, "warmed"), 1);
    assert_eq!(h.metrics.warm.bytes.get(), OBJECT_LEN);
    assert_eq!(
        h.metrics.cache_hits.get(),
        0,
        "a cold warm reads, it does not hit"
    );

    let fills = h.metrics.fills_completed.get();
    assert_eq!(read(&h, "model/weights.bin").await as u64, OBJECT_LEN);
    assert_eq!(h.metrics.cache_hits.get(), OBJECT_CHUNKS);
    assert_eq!(
        h.metrics.fills_completed.get(),
        fills,
        "the read after a warm fills nothing"
    );
}

#[tokio::test]
async fn slices_that_do_not_align_with_chunks_still_warm_every_chunk_once() {
    let h = harness().await;
    seed(&h, "model/weights.bin", OBJECT_LEN).await;
    let args = WarmArgs {
        // One and a half chunks: every slice boundary but the last cuts a chunk in two.
        slice_bytes: CHUNK_SIZE + CHUNK_SIZE / 2,
        ..warm_args(&[&uri("model/weights.bin")])
    };

    let summary = run(&h.client, &args).await.unwrap();
    assert_eq!(summary.warmed_bytes, OBJECT_LEN);
    assert_eq!(
        warm_outcome(&h, "warmed"),
        OBJECT_LEN.div_ceil(args.slice_bytes)
    );
    wait_for_fills(&h.metrics, OBJECT_CHUNKS).await;

    let hits = h.metrics.cache_hits.get();
    read(&h, "model/weights.bin").await;
    assert_eq!(h.metrics.cache_hits.get() - hits, OBJECT_CHUNKS);
}

#[tokio::test]
async fn a_rerun_warm_reads_nothing_from_the_backend() {
    let h = harness().await;
    seed(&h, "model/weights.bin", OBJECT_LEN).await;
    let args = warm_args(&[&uri("model/weights.bin")]);
    run(&h.client, &args).await.unwrap();
    wait_for_fills(&h.metrics, OBJECT_CHUNKS).await;
    let fills = h.metrics.fills_completed.get();

    let again = run(&h.client, &args).await.unwrap();
    assert_eq!(again.warmed_bytes, OBJECT_LEN);
    assert_eq!(h.metrics.fills_completed.get(), fills);
    assert_eq!(h.metrics.cache_hits.get(), OBJECT_CHUNKS);
}

#[tokio::test]
async fn a_prefix_and_a_manifest_expand_to_each_object_once_and_skip_small_ones() {
    let h = harness().await;
    seed(&h, "model/a.bin", OBJECT_LEN).await;
    seed(&h, "model/b.bin", OBJECT_LEN).await;
    // Below the admission floor, like the config.json beside a model's weights.
    seed(&h, "model/config.json", 1024).await;
    seed(&h, "elsewhere/c.bin", OBJECT_LEN).await;
    let dir = tempfile::tempdir().unwrap();
    let manifest = dir.path().join("keys.txt");
    std::fs::write(&manifest, format!("# again\n{}\n", uri("model/a.bin"))).unwrap();
    let args = WarmArgs {
        manifest: Some(manifest),
        ..warm_args(&[&uri("model/")])
    };

    let summary = run(&h.client, &args).await.unwrap();
    assert_eq!(summary.objects, 3, "a.bin once, b.bin, config.json");
    assert_eq!(summary.warmed_bytes, 2 * OBJECT_LEN);
    assert_eq!(
        summary.skipped.get(SkipReason::ObjectSize.as_str()),
        Some(&1),
        "{summary:?}"
    );
    assert_eq!(summary.failed, 0, "a skip is not a failure");
    assert_eq!(warm_outcome(&h, "skipped"), 1);
    wait_for_fills(&h.metrics, 2 * OBJECT_CHUNKS).await;
    assert_eq!(
        h.metrics.fills_completed.get(),
        2 * OBJECT_CHUNKS,
        "nothing outside the prefix, nothing below the floor"
    );
}

#[tokio::test]
async fn max_bytes_and_dry_run_read_nothing() {
    let h = harness().await;
    seed(&h, "model/weights.bin", OBJECT_LEN).await;

    let over = WarmArgs {
        max_bytes: Some(OBJECT_LEN - 1),
        ..warm_args(&[&uri("model/")])
    };
    let err = run(&h.client, &over).await.unwrap_err();
    assert!(err.to_string().contains("--max-bytes"), "{err:#}");

    let dry = WarmArgs {
        dry_run: true,
        ..warm_args(&[&uri("model/")])
    };
    let summary = run(&h.client, &dry).await.unwrap();
    assert_eq!((summary.objects, summary.bytes), (1, OBJECT_LEN));
    assert_eq!(summary.warmed_bytes, 0);

    assert_eq!(h.metrics.fills_completed.get(), 0);
    assert_eq!(warm_outcome(&h, "warmed"), 0);
}

#[tokio::test]
async fn a_missing_key_fails_the_warm_by_name() {
    let h = harness().await;
    let err = run(&h.client, &warm_args(&[&uri("model/absent.bin")]))
        .await
        .unwrap_err();
    assert!(format!("{err:#}").contains("model/absent.bin"), "{err:#}");
}

#[tokio::test]
async fn a_no_store_warm_is_skipped_and_fills_nothing() {
    let h = harness().await;
    seed(&h, "model/weights.bin", OBJECT_LEN).await;
    let got = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("model/weights.bin")
        .customize()
        .mutate_request(|r| {
            r.headers_mut().insert(WARM_HEADER, "1");
            r.headers_mut().insert("cache-control", "no-store");
        })
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes().len(), 0);
    assert_eq!(warm_outcome(&h, "skipped"), 1);
    assert_eq!(h.metrics.fills_completed.get(), 0);
}

#[tokio::test]
async fn a_warm_header_with_any_other_value_is_refused_not_ignored() {
    let h = harness().await;
    seed(&h, "model/weights.bin", OBJECT_LEN).await;
    let err = h
        .client
        .get_object()
        .bucket(BUCKET)
        .key("model/weights.bin")
        .customize()
        .mutate_request(|r| {
            r.headers_mut().insert(WARM_HEADER, "yes");
        })
        .send()
        .await
        .unwrap_err();
    let text = format!("{}", aws_sdk_s3::error::DisplayErrorContext(err));
    assert!(text.contains("InvalidArgument"), "{text}");
    assert_eq!(h.metrics.fills_completed.get(), 0);
}
