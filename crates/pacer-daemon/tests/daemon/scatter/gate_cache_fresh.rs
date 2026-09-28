//! Regression for issue #21 (ADR-0043) — a scattered PUT's freshness check must mean
//! *cache*-fresh, not merely backend-fresh.
//!
//! `key_already_exists` (`proxy/write.rs`) is one backend `HeadObject`: a clean 404
//! says the key is new to the backend, and ADR-0032 § 2 read that as "no holders to
//! invalidate at any R." It is not the same claim. A home can still be caching an
//! older chunk under that exact key if an earlier invalidation missed it — best-effort
//! invalidation never fails the write it rides on, so a home that was briefly
//! unreachable during a DELETE's fan-out keeps whatever it already had (distinct from
//! #20, already fixed by [0042](../../../../../docs/adr/0042-invalidation-measures-the-replaced-object.md):
//! that fix makes the DELETE *try* to reach every home, not guarantee it succeeds).
//! The next scatter of the same key sees a fresh backend key, skips invalidation
//! entirely, and — before ADR-0043 — never told a home that refused its offer to drop
//! what it already had either.
//!
//! The first test drives that exact sequence at the suite's default R = 1, where
//! "a chunk's home" and "a chunk's only home" are the same claim. The second
//! extends it to R = 2: `replication_r` (default 2 in production) lists more than
//! one home per chunk, but a scattered write only ever offers a window — or
//! commits it, or invalidates it — to `homes[0]`. `homes[1..]` are invisible to the
//! write path entirely; the only way one of them ever holds anything is filling
//! itself on an ordinary read, exactly as any home does. That is enough to leave a
//! stale copy behind even for a window `homes[0]` fully **accepts**, which the
//! first test cannot show since it never has a second home to leave one on.
//!
//! Both drive: PUT, cut off the home that should have received a DELETE's
//! invalidation so it misses it, restore reachability, re-PUT different bytes
//! through the scatter, then read the object back through every node.

use bytes::Bytes;
use pacer_daemon::staging::StageOutcome;
use pacer_ring::NodeId;

use super::{
    body, fleet, fleet_with_replication, Harness, Window, CHUNK_SIZE, MIN_SCATTER_BYTES,
    ONE_WINDOW_STAGING, ROOMY_STAGING,
};
use crate::common::BUCKET;

/// Candidate keys tried before giving up on finding two windows on distinct homes,
/// neither of them the coordinator's own node. Mirrors [`Harness::key_reaching`]'s
/// search, widened by the extra "distinct from each other" condition that one does
/// not need.
const CANDIDATES: usize = 64;

/// A [`MIN_SCATTER_BYTES`]-sized key whose two windows land on two distinct homes,
/// neither of them `coordinator`. Deterministic — the ring is fixed, so this tries the
/// same keys in the same order every run — which is what a budget aimed at one home
/// must not accidentally also starve the other.
fn two_distinct_homes(h: &Harness, coordinator: &str) -> (String, Vec<Window>) {
    for i in 0..CANDIDATES {
        let key = format!("scatter/regress21-{i}.bin");
        let windows = h.plan(&key, MIN_SCATTER_BYTES);
        if windows.len() == 2
            && windows[0].home.name() != coordinator
            && windows[0].home.name() != windows[1].home.name()
        {
            return (key, windows);
        }
    }
    panic!(
        "no key under scatter/regress21 reaches two distinct non-coordinator-first \
         homes in {CANDIDATES} tries"
    );
}

/// #21's regression: DELETE, then re-PUT different bytes through the scatter with one
/// home refusing, then GET through every node.
#[tokio::test]
async fn a_re_put_after_delete_invalidates_a_home_that_refused() {
    let h = fleet(ONE_WINDOW_STAGING).await;
    let coordinator = 0;
    let (key, windows) = two_distinct_homes(&h, &h.nodes[coordinator].name);
    let stale_home = windows[0].home.clone();
    let stale_chunk_key = windows[0].chunk_key.clone();

    // First write: each window fits its home's exactly-one-window budget, so both are
    // accepted and committed normally.
    let v1 = body(31, MIN_SCATTER_BYTES);
    h.put_expecting_success(coordinator, &key, &v1, "the first scatter must succeed")
        .await;
    h.wait_announced(&stale_home, &stale_chunk_key).await;
    h.wait_announced(&windows[1].home, &windows[1].chunk_key)
        .await;

    // Cut the stale home off from the ring — same technique
    // `daemon/cluster.rs::peer_down_falls_back_to_backend` uses to simulate a node
    // lost between membership epochs — so the DELETE below cannot invalidate it.
    let live_members = h.ring.load().members().to_vec();
    let unreachable: Vec<NodeId> = live_members
        .iter()
        .map(|member| {
            if member.name() == stale_home.name() {
                NodeId::new(member.name(), "127.0.0.1:1")
            } else {
                member.clone()
            }
        })
        .collect();
    h.ring.store(unreachable);

    h.nodes[coordinator]
        .client
        .delete_object()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .unwrap();
    assert!(
        h.is_absent(&key).await,
        "the DELETE must still reach the backend even though one home misses its invalidation"
    );
    assert!(
        h.nodes[coordinator].metrics.peer_fallbacks.get() > 0,
        "the DELETE's invalidation of the cut-off home must have actually failed, or this \
         test proves nothing about a missed invalidation"
    );

    // Restore reachability. The stale home is up and would answer an `Invalidate` now
    // — it only missed the one the DELETE sent while it was cut off, and it still
    // holds v1's chunk under `stale_chunk_key`.
    h.ring.store(live_members);

    // Force the coming re-PUT's offer to the stale home to be a real, observed
    // refusal rather than a network failure: pre-stage a decoy chunk that consumes
    // its whole one-window budget.
    let stale_idx = h.index_of(stale_home.name());
    let decoy_size = usize::try_from(CHUNK_SIZE).expect("chunk size fits a usize");
    let decoy = matches!(
        h.nodes[stale_idx].staging.try_stage(
            "scatter/regress21-decoy#0",
            "decoy-upload",
            Bytes::from(vec![0u8; decoy_size]),
        ),
        StageOutcome::Staged
    );
    assert!(
        decoy,
        "the decoy must consume the whole budget itself, not merely queue behind it"
    );

    // The re-PUT: the backend key is gone, so the scatter runs. Pre-ADR-0043 this
    // skips invalidation entirely; the stale home refuses (`BudgetExhausted`) and the
    // coordinator uploads and caches that window itself.
    let v2 = body(32, MIN_SCATTER_BYTES);
    h.put_expecting_success(
        coordinator,
        &key,
        &v2,
        "a re-PUT after DELETE must succeed even though one home refuses",
    )
    .await;

    // Without ADR-0043 the stale home still answers window 0 with v1's bytes: every
    // node's read of this chunk key resolves to that same home (`chunk_sources` reads
    // the ring alone, R = 1 here), so no node is spared.
    h.assert_readable_everywhere(&key, &v2, "a re-PUT after DELETE with one home refusing")
        .await;
}

/// A [`MIN_SCATTER_BYTES`]-sized key whose window 0 has exactly `r` co-homes, none
/// of them `coordinator`. Deterministic for the same reason [`two_distinct_homes`]
/// is — the ring is fixed, so this tries the same keys in the same order every run.
fn key_with_co_homes(h: &Harness, coordinator: &str, r: usize) -> (String, String, Vec<NodeId>) {
    for i in 0..CANDIDATES {
        let key = format!("scatter/regress21-r{r}-{i}.bin");
        let chunk_key0 = h.chunk.chunk_key(&format!("{BUCKET}/{key}"), 0);
        let homes = h.ring.homes(&chunk_key0, r);
        if homes.len() == r && homes.iter().all(|home| home.name() != coordinator) {
            return (key, chunk_key0, homes);
        }
    }
    panic!(
        "no key under scatter/regress21-r{r} reaches {r} distinct non-coordinator \
         co-homes for window 0 in {CANDIDATES} tries"
    );
}

/// Window 0's byte range within a [`MIN_SCATTER_BYTES`] object, and its length as a
/// `usize` — window 0 always starts at 0, so this needs no [`Harness`] to compute.
fn window0_range() -> (String, usize) {
    let len = usize::try_from(CHUNK_SIZE).expect("chunk size fits a usize");
    (format!("bytes=0-{}", len - 1), len)
}

/// Read window 0's byte range straight through node `idx`'s own client.
async fn read_window0(h: &Harness, idx: usize, key: &str, range: &str) -> Bytes {
    h.nodes[idx]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .range(range)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap()
        .into_bytes()
}

/// #21's regression, extended to `replication_r = 2`: an ACCEPTED window's
/// non-offered co-home can be left just as stale as a fallen-back one. Only
/// `homes[0]` is ever offered a window, told to commit, or (pre-ADR-0043)
/// invalidated — `homes[1]` hears nothing from the write path in either case, so
/// accepting the offer is not by itself enough to make every co-home correct.
///
/// PUT, read window 0 through its own co-home so it fills and caches v1 — the only
/// way a co-home not `homes[0]` ever holds anything — cut that co-home off the ring
/// so a DELETE's invalidation misses it, restore it, re-PUT different bytes through
/// the scatter with window 0's offer to `homes[0]` fully **accepted** (no refusal
/// anywhere), then read window 0 straight through the co-home, and every node.
#[tokio::test]
async fn an_accepted_windows_stale_co_home_is_invalidated_too() {
    let h = fleet_with_replication(ROOMY_STAGING, 2).await;
    let coordinator = 0;
    let (key, chunk_key0, homes) = key_with_co_homes(&h, &h.nodes[coordinator].name, 2);
    let offered_home = homes[0].clone();
    let co_home = homes[1].clone();
    let co_home_idx = h.index_of(co_home.name());
    let (range, window_len) = window0_range();

    // First write: window 0 is offered to `offered_home` and accepted (plenty of
    // budget). `co_home` is never contacted — nothing tells it anything about a
    // scattered write, accepted or not.
    let v1 = body(41, MIN_SCATTER_BYTES);
    h.put_expecting_success(coordinator, &key, &v1, "the first scatter must succeed")
        .await;
    h.wait_announced(&offered_home, &chunk_key0).await;

    // Read window 0 straight through the co-home's own client: it is one of window
    // 0's ranked homes, so it fills and caches v1 from the backend exactly as any
    // home does on an ordinary read-through miss — the only way it ever gets
    // anything for a key the write path never told it about.
    let seeded = read_window0(&h, co_home_idx, &key, &range).await;
    assert_eq!(
        seeded,
        v1.slice(0..window_len),
        "the co-home must have actually cached v1's bytes for this to be a real seed"
    );
    h.wait_announced(&co_home, &chunk_key0).await;

    // Cut the co-home off from the ring so the DELETE below cannot invalidate it —
    // same technique as the R = 1 regression above.
    let live_members = h.ring.load().members().to_vec();
    let unreachable: Vec<NodeId> = live_members
        .iter()
        .map(|member| {
            if member.name() == co_home.name() {
                NodeId::new(member.name(), "127.0.0.1:1")
            } else {
                member.clone()
            }
        })
        .collect();
    h.ring.store(unreachable);

    h.nodes[coordinator]
        .client
        .delete_object()
        .bucket(BUCKET)
        .key(&key)
        .send()
        .await
        .unwrap();
    assert!(
        h.is_absent(&key).await,
        "the DELETE must still reach the backend even though the co-home misses its \
         invalidation"
    );
    assert!(
        h.nodes[coordinator].metrics.peer_fallbacks.get() > 0,
        "the DELETE's invalidation of the cut-off co-home must have actually failed, or \
         this test proves nothing about a missed invalidation"
    );

    // Restore reachability. The co-home is up and would answer an `Invalidate` now
    // — it only missed the one the DELETE sent while it was cut off.
    h.ring.store(live_members);

    // The re-PUT: the backend key is gone, so the scatter runs. Window 0 is offered
    // to `offered_home` and ACCEPTED — no refusal anywhere, which is the point: an
    // accepted window is not enough by itself, because the offer and commit both go
    // only to `homes[0]`.
    let v2 = body(42, MIN_SCATTER_BYTES);
    h.put_expecting_success(coordinator, &key, &v2, "a re-PUT after DELETE must succeed")
        .await;

    // Without ADR-0043's R co-home rule, the co-home still answers window 0 with
    // v1's bytes — checked directly and deterministically: a home always serves
    // its own cached copy first, with no rotation involved.
    let through_co_home = read_window0(&h, co_home_idx, &key, &range).await;
    assert_eq!(
        through_co_home,
        v2.slice(0..window_len),
        "the co-home of an ACCEPTED window must not still serve v1's stale bytes"
    );
    h.assert_readable_everywhere(
        &key,
        &v2,
        "a re-PUT after DELETE with an accepted window's co-home left stale",
    )
    .await;
}
