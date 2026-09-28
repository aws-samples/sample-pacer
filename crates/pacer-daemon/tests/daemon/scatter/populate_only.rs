//! Planning/30 § 4.1's T3 gate: the owner's `StoreChunk` branch for
//! `populate_only` offers — the shared primitive both ADR-0041's requester-
//! mode read-path populate and its write-tee (planning/30) are built on top
//! of. Landed ahead of both callers (planning/30 § 8 step 1: "merges alone;
//! nothing sends `populate_only` yet"), so this file is what exercises the
//! owner branch until the first real caller does.

use bytes::Bytes;
use pacer_transport::{PeerTransport, StoreOffer, StoreOutcome};

use super::probe::base64_crc32;
use super::{fleet, CHUNK_SIZE, ROOMY_STAGING};
use crate::common::BUCKET;

/// A `populate_only` offer stages the bytes and answers `staged`, without
/// ever calling `UploadPart` — proven by addressing an `upload_id` no
/// `CreateMultipartUpload` ever created. If the owner attempted a real
/// upload against it, S3 would refuse the part and the RPC would fail
/// instead of answering `staged`.
#[tokio::test]
async fn populate_only_stages_without_uploading() {
    let h = fleet(ROOMY_STAGING).await;
    let key = "scatter/populate-only-stage";
    let window = h.plan(key, CHUNK_SIZE).remove(0);
    let body = Bytes::from(vec![7u8; CHUNK_SIZE as usize]);
    let checksum = base64_crc32(&body);

    let outcome = h
        .probe
        .store_chunk(
            &window.home,
            StoreOffer {
                chunk_key: &window.chunk_key,
                upload_id: "populate-only:no-such-upload",
                bucket: BUCKET,
                key,
                part_number: window.part_number,
                body,
                checksum_crc32: &checksum,
                populate_only: true,
            },
        )
        .await
        .expect("a populate-only offer must not need a real multipart upload to succeed");
    assert_eq!(outcome, StoreOutcome::Staged);
}

/// Commit publishes a `populate_only` window exactly as it publishes a real
/// one — the handler branch only skips `UploadPart`, not staging.
#[tokio::test]
async fn populate_only_commit_publishes() {
    let h = fleet(ROOMY_STAGING).await;
    let key = "scatter/populate-only-commit";
    let window = h.plan(key, CHUNK_SIZE).remove(0);
    let body = Bytes::from(vec![9u8; CHUNK_SIZE as usize]);
    let checksum = base64_crc32(&body);
    let upload_id = "populate-only:commit";

    let outcome = h
        .probe
        .store_chunk(
            &window.home,
            StoreOffer {
                chunk_key: &window.chunk_key,
                upload_id,
                bucket: BUCKET,
                key,
                part_number: window.part_number,
                body,
                checksum_crc32: &checksum,
                populate_only: true,
            },
        )
        .await
        .unwrap();
    assert_eq!(outcome, StoreOutcome::Staged);

    let committed = h
        .probe
        .commit_upload(&window.home, upload_id, "populate-only-etag")
        .await
        .expect("commit must reach a real owner");
    assert_eq!(committed, 1, "exactly the one staged window must publish");
}

/// Discard drops a `populate_only` window exactly as it drops a real one —
/// proven by re-offering the same chunk key under a fresh upload id and
/// seeing it stage cleanly. A lingering reservation would refuse the second
/// offer as `RacingUpload` instead.
#[tokio::test]
async fn populate_only_discard_drops_it() {
    let h = fleet(ROOMY_STAGING).await;
    let key = "scatter/populate-only-discard";
    let window = h.plan(key, CHUNK_SIZE).remove(0);
    let body = Bytes::from(vec![3u8; CHUNK_SIZE as usize]);
    let checksum = base64_crc32(&body);
    let first_upload = "populate-only:discard-1";

    h.probe
        .store_chunk(
            &window.home,
            StoreOffer {
                chunk_key: &window.chunk_key,
                upload_id: first_upload,
                bucket: BUCKET,
                key,
                part_number: window.part_number,
                body: body.clone(),
                checksum_crc32: &checksum,
                populate_only: true,
            },
        )
        .await
        .unwrap();

    let discarded = h
        .probe
        .discard_upload(&window.home, first_upload)
        .await
        .expect("discard must reach a real owner");
    assert_eq!(discarded, 1);

    let second_upload = "populate-only:discard-2";
    let outcome = h
        .probe
        .store_chunk(
            &window.home,
            StoreOffer {
                chunk_key: &window.chunk_key,
                upload_id: second_upload,
                bucket: BUCKET,
                key,
                part_number: window.part_number,
                body,
                checksum_crc32: &checksum,
                populate_only: true,
            },
        )
        .await
        .expect("a discarded reservation must not linger and block a fresh offer");
    assert_eq!(outcome, StoreOutcome::Staged);
}
