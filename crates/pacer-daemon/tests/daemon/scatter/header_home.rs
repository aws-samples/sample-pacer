//! Issue #19 — **a scattered object's header lands at `home(object_key)`, never
//! on the coordinator.**
//!
//! A header has to live where a later write's invalidation can find it: the
//! object key's R co-homes, or a directory-listed sharer. A scatter's
//! coordinator is whichever node the client's pod happened to reach — usually
//! neither — and headers are never announced to the directory, so a header
//! cached on the coordinator would survive every later write's invalidation
//! and keep describing a deleted or replaced object. ADR-0032 § 2 says the
//! header is written at `home(object_key)`; these arms pin that the code does
//! what the ADR says, on both ends of the RPC.

use pacer_transport::{HeaderOffer, PeerTransport};

use super::{body, fleet, Harness, OBJECT_LEN, REPLICATION_R, ROOMY_STAGING};
use crate::common::BUCKET;

/// A key under `prefix` whose *object key* (the plain `"{bucket}/{key}"`, no
/// chunk suffix) is homed on a node other than `exclude` — so a scatter through
/// `exclude` exercises the remote leg. Deterministic for the same reason
/// [`Harness::key_reaching`] is: the ring is fixed, so exhausting the
/// candidates means the hash changed, not that a key was unlucky.
fn key_homed_off(h: &Harness, prefix: &str, exclude: &str) -> (String, String) {
    /// Candidate keys tried before giving up. A five-node ring homes a key off
    /// any one node on most keys, so far more than needed.
    const CANDIDATES: usize = 64;
    for i in 0..CANDIDATES {
        let key = format!("{prefix}-{i}.bin");
        let home = object_home(h, &key);
        if home != exclude {
            return (key, home);
        }
    }
    panic!("no key under {prefix} is homed off {exclude} in {CANDIDATES} tries");
}

/// The name of the node the ring homes `key`'s object key on.
fn object_home(h: &Harness, key: &str) -> String {
    h.ring
        .homes(&format!("{BUCKET}/{key}"), REPLICATION_R)
        .first()
        .expect("a non-empty ring homes every key")
        .name()
        .to_owned()
}

/// Whether node `idx` holds a cached header for `key`'s object key.
async fn holds_header(h: &Harness, idx: usize, key: &str) -> bool {
    h.nodes[idx]
        .tier
        .cache()
        .get(&format!("{BUCKET}/{key}"))
        .await
        .expect("cache read")
        .is_some_and(|entry| entry.value().as_header().is_some())
}

/// The scattered header lands at its home and a DELETE through a third node
/// kills it — the issue's own scenario: scatter through a non-home, DELETE
/// through a node that is neither the coordinator nor the home, then GET
/// through the coordinator and expect 404, not the deleted object's body.
#[tokio::test]
async fn a_scattered_header_lands_at_its_home_and_dies_with_the_object() {
    let h = fleet(ROOMY_STAGING).await;
    let coordinator = 0;
    let (key, home) = key_homed_off(&h, "scatter/header-home", &h.nodes[coordinator].name);
    let home_idx = h.index_of(&home);
    // A third node, so the DELETE can find the header only through the ring —
    // never through its own cache or the coordinator's.
    let deleter = (0..h.nodes.len())
        .find(|&i| i != coordinator && i != home_idx)
        .expect("five nodes leave a third");

    let payload = body(21, OBJECT_LEN);
    h.put_expecting_success(coordinator, &key, &payload, "the scattered PUT")
        .await;
    assert_eq!(h.nodes[coordinator].metrics.scatter.scattered.get(), 1);

    // The header is at home(object_key) and nowhere near the coordinator —
    // asserted on the caches directly, because a misplaced header READS fine:
    // it only shows once an invalidation cannot find it.
    assert!(
        holds_header(&h, home_idx, &key).await,
        "the scatter must write the object header at {home}"
    );
    assert!(
        !holds_header(&h, coordinator, &key).await,
        "the coordinator must not keep a header no invalidation can reach"
    );
    // And the moved header still serves: whole-object read-back through the
    // coordinator, which now resolves the header per request.
    assert_eq!(h.read_whole(coordinator, &key).await, payload);

    h.nodes[deleter]
        .client
        .delete_object()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .expect("the DELETE");

    // The issue's check: a GET through the coordinator answers 404 — through
    // every node, in fact, since the header died at a home invalidation reaches.
    for idx in 0..h.nodes.len() {
        let err = h.nodes[idx]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(&key)
            .send()
            .await
            .expect_err("a deleted object must not serve")
            .into_service_error();
        assert!(
            err.is_no_such_key(),
            "GET through {} after a DELETE answered {err:?}, not 404",
            h.nodes[idx].name
        );
    }
}

/// The receiving end holds the same line: a home whose own ring says it does
/// not home the key declines the insert and answers `stored: false`, because
/// accepting would recreate exactly the unreachable copy the RPC exists to
/// avoid (a sender plans against a ring that may have moved).
#[tokio::test]
async fn a_node_declines_a_header_for_a_key_it_does_not_home() {
    let h = fleet(ROOMY_STAGING).await;
    let not_home = &h.nodes[0].name;
    let (key, _) = key_homed_off(&h, "scatter/header-decline", not_home);
    let object_key = format!("{BUCKET}/{key}");

    let stored = h
        .probe
        .store_header(
            &h.node_id(not_home),
            HeaderOffer {
                cache_key: &object_key,
                object_len: OBJECT_LEN,
                e_tag: Some("deadbeef-8"),
                content_type: None,
                last_modified_epoch_secs: None,
            },
        )
        .await
        .expect("the RPC itself succeeds; the decline is the answer");
    assert!(!stored, "a non-home must decline the header");
    assert!(
        !holds_header(&h, 0, &key).await,
        "a declined header must not have been cached anyway"
    );
}
