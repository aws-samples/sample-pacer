//! Gates 3.1, 3.2, 3.6, 3.7, 3.8, 3.9 and 3.13 — **the bytes**.
//!
//! Every arm here answers one question: after this, does a reader get exactly what the
//! writer sent? The interesting cases are the ones where part of the design has already
//! gone wrong — only some owners were told, Complete failed, a window was damaged, the
//! coordinator died — and the answer still has to be yes, or "the object does not
//! exist", and never anything in between.
//!
//! All of them run against [`ROOMY_STAGING`], because a refusal is [`gate_budget`]'s
//! subject and a budget that refused here would be a confound.
//!
//! [`gate_budget`]: super::gate_budget

use std::sync::atomic::Ordering;

use aws_sdk_s3::primitives::ByteStream;
use pacer_transport::PeerTransport;
use rstest::rstest;

use super::probe::{base64_crc32, base64_of};
use super::{
    body, declined, fleet, scatter_report, windows, INTERLEAVE_LEN, MIN_INTERLEAVE_OWNERS,
    OBJECT_LEN, ROOMY_STAGING, SPREAD_OWNERS, WINDOWS,
};
use crate::common::BUCKET;

/// Gates 3.1 (in process), 3.9 and 3.13: a scattered PUT round-trips byte for byte
/// across at least [`SPREAD_OWNERS`] owners, answers with the composite ETag the
/// backend minted, and its metrics say the path engaged rather than degraded.
#[tokio::test]
async fn a_scattered_put_round_trips_across_four_owners() {
    let h = fleet(ROOMY_STAGING).await;
    let coordinator = 0;
    let key = h.key_reaching(
        "scatter/integrity",
        OBJECT_LEN,
        &h.nodes[coordinator].name,
        SPREAD_OWNERS,
    );
    let payload = body(1, OBJECT_LEN);

    let out = h
        .put_expecting_success(coordinator, &key, &payload, "a scattered PUT must succeed")
        .await;

    // Gate 3.9: a scattered write is a multipart upload, so its ETag is composite —
    // ADR-0032's one client-visible break, asserted deliberately rather than
    // observed in passing.
    let e_tag = out
        .e_tag()
        .expect("a scattered PUT answers with the ETag Complete minted")
        .trim_matches('"')
        .to_owned();
    assert!(
        e_tag.ends_with(&format!("-{WINDOWS}")),
        "expected a composite ETag over {WINDOWS} parts, got {e_tag}\n{}",
        scatter_report(&h.nodes[coordinator])
    );
    let head = h
        .backend
        .head_object()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.e_tag().unwrap().trim_matches('"'),
        e_tag,
        "the client's ETag must be the object's ETag"
    );

    // Gate 3.13: the metrics distinguish "engaged" from "silently degraded".
    let node = &h.nodes[coordinator];
    assert_eq!(node.metrics.scatter.scattered.get(), 1);
    assert_eq!(
        windows(node, "owner") + windows(node, "local"),
        WINDOWS,
        "every window must be accounted for in exactly one role"
    );
    assert!(
        windows(node, "owner") >= SPREAD_OWNERS as u64,
        "owners took {} windows; the scatter did not spread",
        windows(node, "owner")
    );
    assert!(
        node.metrics.scatter.owners_engaged.get() >= SPREAD_OWNERS as u64,
        "only {} owners engaged",
        node.metrics.scatter.owners_engaged.get()
    );
    assert_eq!(
        node.metrics.scatter.uncached_windows.get(),
        0,
        "with room to stage, every window must be cached"
    );

    // Gate 3.1: the bytes. Durable at the backend, and identical through every node.
    h.assert_readable_everywhere(&key, &payload, "a scattered write")
        .await;

    // The populate half actually happened: every window's home published its copy
    // and announced itself, which is what makes the next read a hit.
    for w in h.plan(&key, OBJECT_LEN) {
        h.wait_announced(&w.home, &w.chunk_key).await;
    }
}

/// Owners told before the sweep stops, per case of [`every_partial_commit_serves_correct_bytes`].
///
/// `usize::MAX` is "every owner"; the rest are prefixes of the owner list, clamped to
/// its length. Four cases rather than the `for` loop this used to be: each interleaving
/// is an independent claim about an independent upload, so a failure should name the
/// prefix that broke rather than the loop that contained it — and under nextest the four
/// run as four processes instead of one 13-second test.
#[rstest]
#[case::none_committed(0)]
#[case::first_owner_only(1)]
#[case::first_two_owners(2)]
#[case::every_owner(usize::MAX)]
#[tokio::test]
async fn every_partial_commit_serves_correct_bytes(#[case] commit_first: usize) {
    let h = fleet(ROOMY_STAGING).await;
    let payload = body(5, INTERLEAVE_LEN);
    let key = h.key_reaching(
        &format!("scatter/interleave-{commit_first}"),
        INTERLEAVE_LEN,
        "",
        MIN_INTERLEAVE_OWNERS,
    );
    let up = h.drive_by_hand(&key, &payload).await;
    let owners = up.owners();
    let committed: Vec<String> = owners
        .iter()
        .take(commit_first.min(owners.len()))
        .cloned()
        .collect();

    // Before any commit, the object is durable but nothing is published: this is
    // the state the staging fence exists to make safe.
    assert_eq!(
        h.read_backend(&key).await,
        payload,
        "Complete must be durable"
    );
    for owner in &committed {
        let node = h.node_id(owner);
        assert!(
            h.probe
                .commit_upload(&node, &up.upload_id, &up.e_tag)
                .await
                .unwrap()
                > 0,
            "{owner} committed nothing for {key}"
        );
    }

    for w in &up.windows {
        let idx = h.index_of(w.home.name());
        let bounds = h.chunk.chunk_bounds(w.index, INTERLEAVE_LEN).unwrap();
        let expected = payload.slice(bounds.start as usize..bounds.end as usize);
        let fills_before = h.nodes[idx].metrics.fills_completed.get();
        let hits_before = h.nodes[idx].metrics.cache_hits.get();
        let is_committed = committed.iter().any(|name| name == w.home.name());
        if is_committed {
            h.wait_announced(&w.home, &w.chunk_key).await;
            assert_eq!(
                h.read_window(idx, &key, w, INTERLEAVE_LEN).await,
                expected,
                "committed window {} of {key} served wrong bytes",
                w.index
            );
            assert!(
                h.nodes[idx].metrics.cache_hits.get() > hits_before,
                "committed window {} of {key} was not a cache hit",
                w.index
            );
            assert_eq!(
                h.nodes[idx].metrics.fills_completed.get(),
                fills_before,
                "committed window {} of {key} was read through instead of served",
                w.index
            );
        } else {
            assert!(
                !h.is_announced(&w.home, &w.chunk_key).await,
                "uncommitted window {} of {key} must not be announced",
                w.index
            );
            assert_eq!(
                h.read_window(idx, &key, w, INTERLEAVE_LEN).await,
                expected,
                "uncommitted window {} of {key} served wrong bytes",
                w.index
            );
            h.wait_for_fill(idx, fills_before).await;
        }
    }

    // Whatever the interleaving, a whole-object read through any node is exact.
    for idx in 0..h.nodes.len() {
        assert_eq!(
            h.read_whole(idx, &key).await,
            payload,
            "whole-object read through {} differs for {key}",
            h.nodes[idx].name
        );
    }

    // Drop what nobody committed, the way a coordinator's unwind does. The count
    // doubles as a check on DiscardUpload.
    for owner in owners.iter().filter(|o| !committed.contains(o)) {
        let held = up
            .windows
            .iter()
            .filter(|w| w.home.name() == owner.as_str())
            .count();
        assert_eq!(
            h.probe
                .discard_upload(&h.node_id(owner), &up.upload_id)
                .await
                .unwrap() as usize,
            held,
            "{owner} should have dropped its {held} staged window(s) of {key}"
        );
    }
}

/// The client-digest contract, which gate 3.13 is what exposed: a whole-object CRC32
/// from the client is **honoured**, because the coordinator already computes exactly
/// that digest to hand to Complete. Every current AWS SDK sends one by default, so
/// treating it as unscatterable meant the scatter declined every real client's PUT
/// while the metrics said only "declined: client_checksum".
///
/// Three arms: the right digest scatters, a wrong one fails the write with `BadDigest`
/// and leaves no object, and a digest the coordinator cannot reproduce still declines.
#[tokio::test]
async fn a_client_crc32_is_honoured_and_a_wrong_one_fails_the_write() {
    let h = fleet(ROOMY_STAGING).await;
    let node = &h.nodes[0];
    let payload = body(16, OBJECT_LEN);

    let good = h.key_reaching("scatter/crc-ok", OBJECT_LEN, &node.name, SPREAD_OWNERS);
    node.client
        .put_object()
        .bucket(BUCKET)
        .key(&good)
        .checksum_crc32(base64_crc32(&payload))
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect("a PUT carrying a correct whole-object CRC32 must scatter");
    assert_eq!(node.metrics.scatter.scattered.get(), 1);
    assert_eq!(
        declined(node, "client_checksum"),
        0,
        "a CRC32 is the one digest the coordinator computes anyway"
    );
    assert!(h.faults.full_object_checks.load(Ordering::Relaxed) >= 1);
    assert_eq!(h.read_backend(&good).await, payload);

    // A wrong digest is the client's error, and is caught before Complete, so the
    // object never exists even briefly.
    let bad = h.key_reaching("scatter/crc-bad", OBJECT_LEN, &node.name, SPREAD_OWNERS);
    let err = node
        .client
        .put_object()
        .bucket(BUCKET)
        .key(&bad)
        .checksum_crc32(base64_of(0))
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect_err("a PUT whose CRC32 disagrees with its body must fail");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("BadDigest"),
        "a client digest mismatch is a 400, not a 500"
    );
    assert!(
        h.is_absent(&bad).await,
        "a rejected digest must leave no object"
    );

    // A digest that cannot be reproduced from parts still stands the scatter aside.
    // The SDK computes this one, so the plain path's write still succeeds.
    let other = "scatter/crc32c.bin";
    node.client
        .put_object()
        .bucket(BUCKET)
        .key(other)
        .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32C)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        declined(node, "client_checksum"),
        1,
        "a CRC32C covers the whole body and no assembly can reproduce it:\n{}",
        scatter_report(node)
    );
    assert_eq!(h.read_backend(other).await, payload);
}

/// Gate 3.7: a coordinator that dies after Complete but before it can commit.
///
/// The object is durable and every read is correct through the miss path — which was
/// always going to be true, because a miss is legal — and the staged bytes its owners
/// are left holding come back when the TTL reaper sweeps. That reaper is the whole
/// point: without it one dead writer's reservations would refuse every later write on
/// those nodes until the daemon restarted, and a crashed coordinator would be
/// indistinguishable from a permanently saturated peer.
#[tokio::test]
async fn a_coordinator_that_dies_after_complete_leaves_reads_correct() {
    let h = fleet(ROOMY_STAGING).await;
    let payload = body(6, INTERLEAVE_LEN);
    let key = h.key_reaching("scatter/orphan", INTERLEAVE_LEN, "", MIN_INTERLEAVE_OWNERS);
    // Complete, and then nothing: no commit, no discard. The coordinator is gone.
    let up = h.drive_by_hand(&key, &payload).await;

    assert_eq!(
        h.read_backend(&key).await,
        payload,
        "Complete must be durable"
    );
    // Asserted BEFORE any read through a node: a home that fills on read-through
    // announces itself (ADR-0017 "home fills first, home-is-holder"), so after a
    // read the directory says nothing about whether a commit happened.
    for w in &up.windows {
        assert!(
            !h.is_announced(&w.home, &w.chunk_key).await,
            "window {} was published without a commit",
            w.index
        );
    }
    for idx in 0..h.nodes.len() {
        assert_eq!(
            h.read_whole(idx, &key).await,
            payload,
            "read through {} after an abandoned upload",
            h.nodes[idx].name
        );
    }

    // Each owner is holding exactly the windows it homes, and the reaper returns them.
    let deadline = std::time::Instant::now() + super::STAGING_TTL;
    for owner in up.owners() {
        let node = &h.nodes[h.index_of(&owner)];
        let held = up
            .windows
            .iter()
            .filter(|w| w.home.name() == owner.as_str())
            .count();
        assert_eq!(node.staging.pending_count(), held, "{owner} staged count");
        assert_eq!(
            node.staging.staged_bytes(),
            held * usize::try_from(super::CHUNK_SIZE).unwrap(),
            "{owner} staged bytes"
        );
        assert_eq!(
            node.staging.reap_at(deadline).len(),
            held,
            "the reaper must drop {owner}'s abandoned windows"
        );
        assert_eq!(
            node.staging.staged_bytes(),
            0,
            "{owner} must have its whole budget back"
        );
    }

    // Reaping cost warmth, never correctness.
    for idx in 0..h.nodes.len() {
        assert_eq!(
            h.read_whole(idx, &key).await,
            payload,
            "read through {} after the reaper swept",
            h.nodes[idx].name
        );
    }
}

/// Gate 3.6: Complete fails. The client's PUT fails, the multipart upload is aborted,
/// every owner's staged windows are dropped at once rather than left for the TTL, and
/// no chunk of an object that never existed is cached or announced anywhere.
#[tokio::test]
async fn a_failed_complete_fails_the_put_and_drops_every_staged_window() {
    let h = fleet(ROOMY_STAGING).await;
    let key = h.key_reaching(
        "scatter/complete-fails",
        OBJECT_LEN,
        &h.nodes[0].name,
        SPREAD_OWNERS,
    );
    let payload = body(7, OBJECT_LEN);
    h.faults.fail_complete.store(true, Ordering::Relaxed);

    let err = h.nodes[0]
        .client
        .put_object()
        .bucket(BUCKET)
        .key(&key)
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect_err("a write whose Complete fails must fail");
    assert_eq!(
        err.into_service_error().meta().code(),
        Some("InternalError"),
        "a backend failure is ours to own, not the client's"
    );

    assert!(
        !h.faults.aborts_for(&key).is_empty(),
        "the coordinator must abort the upload it could not complete"
    );
    for node in &h.nodes {
        assert_eq!(
            node.staging.pending_count(),
            0,
            "{} still holds staged windows of an aborted upload",
            node.name
        );
    }
    for w in h.plan(&key, OBJECT_LEN) {
        assert!(
            !h.is_announced(&w.home, &w.chunk_key).await,
            "window {} of an aborted upload was announced",
            w.index
        );
    }
    assert!(h.is_absent(&key).await, "the object must not exist");
    let err = h.nodes[0]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .expect_err("reading an object that was never written must 404");
    assert!(err.into_service_error().is_no_such_key());
}

/// Gate 3.8: both digest layers, end to end through the coordinator.
///
/// The control arm is the load-bearing one: it asserts the backend *was asked* to
/// check a per-part digest on every part and a whole-object digest at Complete, so
/// the two failure arms cannot pass because the coordinator quietly stopped sending
/// them. The failure arms then damage one window on either side of the per-part
/// check, and neither damaged assembly ever becomes an object.
#[tokio::test]
async fn a_corrupted_window_never_assembles() {
    /// The window to damage. Any interior one; 3 is not the first or the last, so it
    /// is neither the part the plan starts with nor the short tail.
    const DAMAGED_PART: i32 = 3;

    let h = fleet(ROOMY_STAGING).await;
    let payload = body(8, OBJECT_LEN);
    let coordinator = &h.nodes[0];

    let good = h.key_reaching(
        "scatter/digest-ok",
        OBJECT_LEN,
        &coordinator.name,
        SPREAD_OWNERS,
    );
    coordinator
        .client
        .put_object()
        .bucket(BUCKET)
        .key(&good)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .unwrap();
    assert_eq!(
        h.faults.parts_without_digest.load(Ordering::Relaxed),
        0,
        "every part must carry its own CRC32 (the coordinator→owner hop)"
    );
    assert!(
        h.faults.full_object_checks.load(Ordering::Relaxed) >= 1,
        "Complete must carry the whole-object CRC32 the split removed"
    );
    assert_eq!(h.read_backend(&good).await, payload);

    // Damaged on the way to the backend: the part's own digest catches it, so the
    // part is never stored and the window fails at whichever node uploads it.
    h.faults
        .corrupt_on_arrival
        .store(DAMAGED_PART, Ordering::Relaxed);
    let part_bad = h.key_reaching(
        "scatter/digest-part",
        OBJECT_LEN,
        &coordinator.name,
        SPREAD_OWNERS,
    );
    coordinator
        .client
        .put_object()
        .bucket(BUCKET)
        .key(&part_bad)
        .body(ByteStream::from(payload.clone()))
        .send()
        .await
        .expect_err("a part whose digest fails must fail the write");
    assert!(
        h.is_absent(&part_bad).await,
        "a write with a rejected part must leave no object"
    );
    h.faults.corrupt_on_arrival.store(0, Ordering::Relaxed);

    // Damaged after its own digest passed: every part is individually valid and only
    // the whole-object digest can tell, which is exactly why it is sent.
    h.faults
        .corrupt_after_check
        .store(DAMAGED_PART, Ordering::Relaxed);
    let whole_bad = h.key_reaching(
        "scatter/digest-whole",
        OBJECT_LEN,
        &coordinator.name,
        SPREAD_OWNERS,
    );
    coordinator
        .client
        .put_object()
        .bucket(BUCKET)
        .key(&whole_bad)
        .body(ByteStream::from(payload))
        .send()
        .await
        .expect_err("an assembly whose whole-object digest fails must fail the write");
    assert!(
        h.is_absent(&whole_bad).await,
        "a rejected assembly must leave no object"
    );
    assert!(
        !h.faults.aborts_for(&whole_bad).is_empty(),
        "the upload must be aborted, not left incomplete"
    );
    for node in &h.nodes {
        assert_eq!(
            node.staging.pending_count(),
            0,
            "{} still holds staged windows after a rejected assembly",
            node.name
        );
    }
}
