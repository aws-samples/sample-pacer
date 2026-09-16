//! Gates 3.4 and 3.5 — **the fleet under all-to-all load, and the absence of a cycle.**
//!
//! Both arms run at [`ONE_WINDOW_STAGING`], which is the point: every node coordinates
//! its own write while homing everyone else's windows, against a budget that guarantees
//! refusals. That is exactly the shape where an owner blocking for budget while its own
//! coordinator blocks on that owner would be a deadlock. Refusing rather than waiting is
//! what makes the cycle impossible, so the assertion is that the round *finishes*.
//!
//! The customer's 256-shard save is this shape at scale (ADR-0032 § 4), which is why a
//! deadlock here would be a deadlock in the one workload we can point at.
//!
//! What these arms do **not** prove is a latency bound: an in-process fleet on loopback
//! cannot say anything honest about tail latency, and the hardware arms are where that
//! question belongs.

use aws_sdk_s3::primitives::ByteStream;

use super::{
    body, fleet, scatter_report, CHUNK_SIZE, DEADLOCK_BUDGET, INTERLEAVE_LEN, ONE_WINDOW_STAGING,
};
use crate::common::BUCKET;

/// Gate 3.4 — the all-to-all, and the test that could not be written before the
/// coordinator existed.
#[tokio::test]
async fn an_all_to_all_round_of_scatters_never_deadlocks() {
    let h = fleet(ONE_WINDOW_STAGING).await;
    let payload = body(11, INTERLEAVE_LEN);
    let keys: Vec<String> = (0..h.nodes.len())
        .map(|i| format!("scatter/all-to-all-{i}.bin"))
        .collect();

    let writes = h.nodes.iter().enumerate().map(|(i, node)| {
        let (client, key, bytes) = (node.client.clone(), keys[i].clone(), payload.clone());
        async move {
            client
                .put_object()
                .bucket(BUCKET)
                .key(key)
                .body(ByteStream::from(bytes))
                .send()
                .await
        }
    });
    let results = tokio::time::timeout(DEADLOCK_BUDGET, futures::future::join_all(writes))
        .await
        .expect("an all-to-all round of scattered writes did not finish: deadlock");

    for (i, result) in results.iter().enumerate() {
        assert!(
            result.is_ok(),
            "write {i} failed: {:?}",
            result.as_ref().err()
        );
    }
    for (i, key) in keys.iter().enumerate() {
        assert_eq!(
            h.read_backend(key).await,
            payload,
            "object {i} of the all-to-all round differs"
        );
        assert_eq!(
            h.nodes[i].metrics.scatter.scattered.get(),
            1,
            "{} did not take the scatter path:\n{}",
            h.nodes[i].name,
            scatter_report(&h.nodes[i])
        );
    }
}

/// Gate 3.5: an owner taking windows must keep serving reads. The same all-to-all
/// round runs while one node reads a warm object whose chunks live on the nodes that
/// are busy receiving — so every read crosses the peer plane into a receiver.
///
/// What this proves is the absence of starvation and of a serve-path cycle, not a
/// latency bound.
#[tokio::test]
async fn owner_receive_does_not_starve_the_read_path() {
    let h = fleet(ONE_WINDOW_STAGING).await;

    // A read fixture, written straight to the backend so no scatter is involved, then
    // read once through the reader so its chunks are warm at their homes.
    let read_key = "scatter/served-while-writing.bin";
    let read_payload = body(12, CHUNK_SIZE);
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(read_key)
        .body(ByteStream::from(read_payload.clone()))
        .send()
        .await
        .unwrap();
    let reader = 0;
    assert_eq!(h.read_whole(reader, read_key).await, read_payload);

    let write_payload = body(13, INTERLEAVE_LEN);
    let writes: Vec<_> = h
        .nodes
        .iter()
        .enumerate()
        .map(|(i, node)| {
            let (client, bytes) = (node.client.clone(), write_payload.clone());
            let key = format!("scatter/while-serving-{i}.bin");
            async move {
                client
                    .put_object()
                    .bucket(BUCKET)
                    .key(key)
                    .body(ByteStream::from(bytes))
                    .send()
                    .await
            }
        })
        .collect();
    let round = tokio::spawn(futures::future::join_all(writes));

    let fetches_before = h.nodes[reader].metrics.peer_fetches.get();
    let mut served = 0_usize;
    while !round.is_finished() {
        assert_eq!(
            h.read_whole(reader, read_key).await,
            read_payload,
            "a read served during the write round returned wrong bytes"
        );
        served += 1;
    }
    let results = tokio::time::timeout(DEADLOCK_BUDGET, round)
        .await
        .expect("the write round did not finish")
        .expect("the write round panicked");
    for (i, result) in results.iter().enumerate() {
        assert!(
            result.is_ok(),
            "write {i} failed: {:?}",
            result.as_ref().err()
        );
    }
    assert!(served > 0, "no read completed while the writes ran");
    assert!(
        h.nodes[reader].metrics.peer_fetches.get() > fetches_before,
        "the reads never crossed the peer plane, so they never met a busy owner"
    );
}
