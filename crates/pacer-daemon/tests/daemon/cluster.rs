//! Cluster correctness suite: N full daemon nodes (proxy + peer gRPC server +
//! shared ring) over ONE s3s-fs backend, exercised through an unmodified
//! aws-sdk-s3 client. The peer plane runs over real TCP loopback — the same
//! tonic client/server pair production uses. Most tests use the 2-node,
//! single-copy [`cluster`] (Phase 2 / ADR-0012); B3 replication tests use
//! [`cluster_with`] to size the cluster and set the replication factor R.
//!
//! client → node A (PacerProxy, ring says B homes the key)
//!            └─ GrpcTransport ──▶ node B (PacerPeer: cache hit / read-through)
//!                                   └─ aws-sdk-s3 ──▶ s3s-fs backend

// Tests are linear scenarios; splitting them to satisfy a line count would
// hurt readability (CLAUDE.md: size limits target production code).
#![allow(clippy::too_many_lines)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use aws_sdk_s3::config::retry::RetryConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{Delete, ObjectIdentifier};
use bytes::Bytes;
use pacer_cache::tier::ChunkTier;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::proxy::PacerProxy;
use pacer_ring::directory::Tier;
use pacer_ring::{NodeId, SharedRing};
use pacer_transport::grpc::GrpcTransport;
use pacer_transport::PeerTransport;
use s3s::dto;
use s3s::{s3_error, S3Request, S3Response, S3Result};

use crate::common::{
    self, backend_service, create_test_bucket, fs_backend_service, node_parts, sdk_client_for,
    seeded_body, serve_node, CacheSpec, NodeSpec, BUCKET, FILL_PARALLELISM,
};

const MIN_OBJECT_SIZE: u64 = 4 << 20;
const MAX_OBJECT_SIZE: Option<u64> = Some(64 << 20);
/// Cache chunk size for the cluster harness. Chosen LARGER than every test
/// object so each object is exactly ONE chunk — placement, peer fetch, and
/// invalidation are then deterministic on that single chunk's key (per-chunk
/// spreading is exercised in the single-node covering-set tests). ADR-0015's
/// contract is that a chunk key is just a ring key, so single-chunk objects
/// exercise the same code paths as multi-chunk ones.
const CHUNK_SIZE: u64 = 64 << 20;

/// The chunk-0 cache key for an object key — where a single-chunk object's
/// bytes are homed (the object key homes only the metadata header).
fn chunk0_key(object_cache_key: &str) -> String {
    pacer_cache::chunk::ChunkConfig::new(CHUNK_SIZE).chunk_key(object_cache_key, 0)
}

struct Node {
    /// Client pointing at this node's S3 front (placeholder creds).
    client: aws_sdk_s3::Client,
    metrics: Metrics,
    /// This node's chunk tier, so an arm can ask which nodes still hold a key.
    tier: ChunkTier,
    name: String,
    /// TCP connections this node's peer server has accepted, from anyone — read as a
    /// delta around the calls under test.
    accepted: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    _cache_dir: tempfile::TempDir,
}

struct ClusterHarness {
    nodes: Vec<Node>,
    /// Ground-truth client straight at the backend.
    backend: aws_sdk_s3::Client,
    ring: SharedRing,
    _backend_dir: tempfile::TempDir,
}

/// Two daemon nodes, replication factor 1 (ADR-0012 single-copy) — the Phase-2
/// / B2 default the existing suite asserts against. B3 top-R tests use
/// [`cluster_with`] with ≥3 nodes so co-homes are distinguishable from
/// non-homes.
async fn cluster() -> ClusterHarness {
    cluster_with(&["node-a", "node-b"], 1).await
}

/// `names.len()` daemon nodes on loopback peer ports over one shared backend,
/// each configured with replication factor `replication_r` (ADR-0016 layer 2).
///
/// The per-node assembly is [`common::node_parts`] plus [`common::serve_node`],
/// which `scatter/` shares; what is left here is the loop and the ring, because
/// a member's [`NodeId`] cannot be written until its peer port is known.
async fn cluster_with(names: &[&str], replication_r: usize) -> ClusterHarness {
    let backend_dir = tempfile::tempdir().unwrap();
    let (backend_service, backend_creds) = fs_backend_service(backend_dir.path());
    cluster_on(
        names,
        replication_r,
        backend_service,
        backend_creds,
        backend_dir,
    )
    .await
}

/// [`cluster_with`] over a backend the caller built — [`HeadGate`], for the arms that
/// need the backend's `HeadObject` to fail on demand. `backend_dir` is whatever the
/// backend stores its objects in, held for the harness's lifetime.
async fn cluster_on(
    names: &[&str],
    replication_r: usize,
    backend_service: s3s::service::S3Service,
    backend_creds: aws_sdk_s3::config::Credentials,
    backend_dir: tempfile::TempDir,
) -> ClusterHarness {
    let backend_client = sdk_client_for(backend_service.clone(), backend_creds.clone());
    create_test_bucket(&backend_client).await;

    let ring = SharedRing::default();
    let mut nodes = Vec::new();
    let mut members = Vec::new();
    for &name in names {
        let spec = NodeSpec {
            name,
            replication_r,
            min_object_size: MIN_OBJECT_SIZE,
            max_object_size: MAX_OBJECT_SIZE,
            chunk: pacer_cache::chunk::ChunkConfig::new(CHUNK_SIZE),
            cache: CacheSpec::default(),
        };
        let parts = node_parts(&spec, &ring, &backend_service, &backend_creds).await;
        let proxy = PacerProxy::new(
            parts.backend.clone(),
            parts.tier.clone(),
            parts.metrics.clone(),
            MIN_OBJECT_SIZE,
            MAX_OBJECT_SIZE,
            spec.chunk,
            FILL_PARALLELISM,
        )
        .with_cluster(parts.cluster.clone());
        // No staging area: this file's subject is the read path, and a peer with one
        // would be a scatter owner (ADR-0032), which `scatter/` is for.
        let served = serve_node(&spec, &parts, proxy, None).await;

        members.push(NodeId::new(name, served.peer_addr.to_string()));
        nodes.push(Node {
            client: served.client,
            metrics: parts.metrics,
            tier: parts.tier,
            name: name.to_owned(),
            accepted: served.accepted,
            _cache_dir: parts.cache_dir,
        });
    }
    ring.store(members);

    ClusterHarness {
        nodes,
        backend: backend_client,
        ring,
        _backend_dir: backend_dir,
    }
}

impl ClusterHarness {
    /// Indices (owner, other) for `key` in the current ring.
    fn owner_and_other(&self, cache_key: &str) -> (usize, usize) {
        let owner = self.ring.owner(cache_key).unwrap();
        let idx = self
            .nodes
            .iter()
            .position(|n| n.name == owner.name())
            .unwrap();
        (idx, 1 - idx)
    }

    /// Node index for a node name.
    fn index_of(&self, name: &str) -> usize {
        self.nodes.iter().position(|n| n.name == name).unwrap()
    }

    /// The top-`r` co-home node indices for `cache_key` (owner-first), plus the
    /// index of one node that is NOT a home — the (homes, non_home) split B3
    /// layer-2 tests need. Panics if every node is a home (caller must size the
    /// cluster so a non-home exists).
    fn homes_and_outsider(&self, cache_key: &str, r: usize) -> (Vec<usize>, usize) {
        let homes: Vec<usize> = self
            .ring
            .homes(cache_key, r)
            .iter()
            .map(|n| self.index_of(n.name()))
            .collect();
        let outsider = (0..self.nodes.len())
            .find(|i| !homes.contains(i))
            .expect("cluster has no non-home node for this key; add more nodes");
        (homes, outsider)
    }

    /// Names of the nodes whose tier holds `cache_key` right now.
    async fn holders(&self, cache_key: &str) -> Vec<String> {
        let mut holders = Vec::new();
        for node in &self.nodes {
            if matches!(node.tier.cache().get(cache_key).await, Ok(Some(_))) {
                holders.push(node.name.clone());
            }
        }
        holders
    }

    /// PUT `body` at `key` through node `via`, then GET it through node `reader` and wait
    /// for the fill — so `reader`'s side of the cluster holds the object's chunks.
    async fn put_and_warm(&self, key: &str, body: &Bytes, via: usize, reader: usize) {
        self.nodes[via]
            .client
            .put_object()
            .bucket(BUCKET)
            .key(key)
            .body(ByteStream::from(body.clone()))
            .send()
            .await
            .unwrap();
        let fills = self.nodes[reader].metrics.fills_completed.get();
        let got = self.nodes[reader]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(&got.body.collect().await.unwrap().into_bytes(), body);
        wait_for_fill(&self.nodes[reader].metrics, fills).await;
    }
}

fn big_body(seed: u8, len: usize) -> Bytes {
    seeded_body(seed, len)
}

/// Poll until `metrics` records a fill past `fills_before`.
///
/// A fill is a spawned tee, so a test that asserts on it the instant a GET body drains
/// is racing it. Aborts count: the question is whether the pipeline has *settled*.
async fn wait_for_fill(metrics: &Metrics, fills_before: u64) {
    common::poll_until("a chunk fill settles on this node", || async {
        metrics.fills_completed.get() > fills_before || metrics.fills_aborted.get() > fills_before
    })
    .await;
}

/// Poll until `home`'s directory shard lists `holder` for `chunk_key`.
///
/// Layer-1 admission announces the requester to the chunk's home fire-and-forget
/// (ADR-0017 remote-announce), so the announce is not ordered against the GET that
/// caused it and a test that looks once is racing a spawned task.
async fn wait_announced(home: &NodeId, chunk_key: &str, holder: &str) {
    let probe = GrpcTransport::new(0, "probe");
    common::poll_until(
        &format!(
            "{home_name} lists {holder} as a holder of {chunk_key}",
            home_name = home.name()
        ),
        || async {
            probe
                .lookup_sharers(home, chunk_key)
                .await
                .unwrap()
                .unwrap_or_default()
                .holders
                .iter()
                .any(|held| held.node == holder)
        },
    )
    .await;
}

/// A miss on the non-owner triggers owner read-through: the OWNER fills, the
/// requester serves the bytes and stores nothing (ADR-0012).
#[tokio::test]
async fn miss_via_peer_fills_owner_not_requester() {
    let h = cluster().await;
    let body = big_body(1, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/fill.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    let (owner, other) = h.owner_and_other(&chunk0_key(&format!("{BUCKET}/{key}")));
    let got = h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);

    // Requester (COLD chunk, single fetch < layer-1 threshold): miss resolved
    // via peer fetch, no local fill. ADR-0012 semantics are preserved for the
    // cold tail — layer 1 admits only once a chunk proves hot (next test).
    assert_eq!(h.nodes[other].metrics.cache_misses.get(), 1);
    assert_eq!(h.nodes[other].metrics.peer_fetches.get(), 1);
    assert_eq!(h.nodes[other].metrics.peer_fallbacks.get(), 0);
    // Owner: read through and filled exactly once.
    wait_for_fill(&h.nodes[owner].metrics, 0).await;
    assert_eq!(h.nodes[owner].metrics.peer_readthroughs.get(), 1);
    assert_eq!(h.nodes[owner].metrics.fills_completed.get(), 1);
    assert_eq!(h.nodes[other].metrics.fills_completed.get(), 0);

    // The read-through fill registered the owner in its directory shard
    // (ADR-0017): the pump_read_through path self-announces just like the
    // proxy's local fill does. One fetch has not admitted the requester, so the
    // owner is still the sole holder.
    let owner_node = h.ring.load().members()[owner].clone();
    let set = GrpcTransport::new(0, "probe")
        .lookup_sharers(&owner_node, &chunk0_key(&format!("{BUCKET}/{key}")))
        .await
        .unwrap()
        .expect("read-through fill must register the owner");
    assert_eq!(set.holders.len(), 1);
    assert_eq!(set.holders[0].node, owner_node.name());
}

/// Layer 1 (ADR-0016 requester-local admission): a peer-owned chunk fetched
/// enough times within the window (threshold 2 here) is admitted locally by the
/// requester, which then registers with the chunk's directory home so other
/// nodes can fetch from it too. Cold chunks (previous test) still store nothing.
#[tokio::test]
async fn hot_chunk_admits_locally_on_second_fetch() {
    let h = cluster().await;
    let body = big_body(11, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/hot.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let chunk_key = chunk0_key(&format!("{BUCKET}/{key}"));
    let (owner, other) = h.owner_and_other(&chunk_key);

    // Two reads through the non-owner. The 1st is cold (no admit); the 2nd
    // crosses the threshold and admits a local copy on the requester.
    for _ in 0..2 {
        h.nodes[other]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap();
    }
    // The requester admitted a local copy on the 2nd (hot) fetch. The insert
    // is synchronous with the chunk resolution, so by the time the GET body
    // drained the admission has already happened.
    assert_eq!(h.nodes[other].metrics.local_admits.get(), 1);
    assert_eq!(h.nodes[other].metrics.peer_fetches.get(), 2);

    // A third read is now a LOCAL hit — no peer fetch.
    h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    assert_eq!(
        h.nodes[other].metrics.peer_fetches.get(),
        2,
        "warm local copy serves the 3rd read"
    );
    assert!(h.nodes[other].metrics.cache_hits.get() >= 1);

    // The admission announced the requester to the chunk's home (ADR-0017
    // remote-announce), so the sharer set now lists both the owner and the
    // requester. The announce is fire-and-forget (spawned), so poll for it.
    let owner_node = h.ring.load().members()[owner].clone();
    let other_name = h.nodes[other].name.clone();
    wait_announced(&owner_node, &chunk_key, &other_name).await;
}

/// ADR-0048's warm skips layer-1 admission: two warms through a non-owner cross the same
/// threshold [`hot_chunk_admits_locally_on_second_fetch`] does with ordinary reads, but a
/// warm reads *for* the workload's nodes, not *as* one, so it must not leave a copy behind
/// on the node that happened to run it.
#[tokio::test]
async fn a_warm_through_a_non_owner_admits_no_local_copy() {
    let h = cluster().await;
    let body = big_body(11, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/warmed.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let chunk_key = chunk0_key(&format!("{BUCKET}/{key}"));
    let (owner, other) = h.owner_and_other(&chunk_key);

    for _ in 0..2 {
        h.nodes[other]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .customize()
            .mutate_request(|r| {
                r.headers_mut().insert(pacer_daemon::warm::WARM_HEADER, "1");
            })
            .send()
            .await
            .unwrap();
    }
    assert_eq!(
        h.nodes[other].metrics.local_admits.get(),
        0,
        "a warm must not leave a copy on the node that ran it"
    );
    assert_eq!(h.nodes[other].metrics.peer_fetches.get(), 2);
    assert_eq!(
        h.nodes[owner]
            .metrics
            .warm
            .requests
            .with_label_values(&["warmed"])
            .get(),
        0,
        "the owner served these as ordinary peer reads, not warms of its own"
    );
    wait_for_fill(&h.nodes[owner].metrics, 0).await;
    let owner_hits_before = h.nodes[owner].metrics.cache_hits.get();

    // A workload's own read through the OWNER now hits what the warm placed there.
    let got = h.nodes[owner]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(&got.body.collect().await.unwrap().into_bytes(), &body);
    assert_eq!(
        h.nodes[owner].metrics.cache_hits.get(),
        owner_hits_before + 1
    );
}

/// Top-R co-homes (ADR-0016 layer 2): with R=2 on a 3-node cluster, BOTH of a
/// chunk's two homes fill it on read-through and hold a cluster-wide copy,
/// while a non-home requester fetches from a home and stores nothing. This is
/// the multi-copy relaxation of ADR-0012's single-owner rule.
#[tokio::test]
async fn top_r_co_homes_each_fill_non_home_does_not() {
    let h = cluster_with(&["node-a", "node-b", "node-c"], 2).await;
    let body = big_body(9, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/top-r.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    let chunk_key = chunk0_key(&format!("{BUCKET}/{key}"));
    let (homes, outsider) = h.homes_and_outsider(&chunk_key, 2);
    assert_eq!(homes.len(), 2, "R=2 must designate two homes");

    // Each home, reading locally, fills its own cluster-wide copy (a home owns
    // the chunk, so it reads through the backend and stores it — not a peer
    // fetch). Two homes ⇒ two independent copies.
    for &home in &homes {
        let got = h.nodes[home]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
        wait_for_fill(&h.nodes[home].metrics, 0).await;
        assert_eq!(h.nodes[home].metrics.fills_completed.get(), 1);
        assert_eq!(
            h.nodes[home].metrics.peer_fetches.get(),
            0,
            "a home reads through, never peer-fetches its own key"
        );
        // Each home self-registers in its own directory shard.
        let home_node = h.ring.load().members()[home].clone();
        let set = GrpcTransport::new(0, "probe")
            .lookup_sharers(&home_node, &chunk_key)
            .await
            .unwrap()
            .expect("each home must register itself after filling");
        assert!(set.holders.iter().any(|held| held.node == home_node.name()));
    }

    // The non-home requester fetches from a home and stores nothing (layer 1
    // admission is off by default heat; this is the pure ADR-0012 non-home path
    // preserved for cold chunks).
    let got = h.nodes[outsider]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.nodes[outsider].metrics.peer_fetches.get(), 1);
    assert_eq!(h.nodes[outsider].metrics.peer_fallbacks.get(), 0);
    // A non-home requester stores nothing (the pure ADR-0012 path for a cold chunk).
    // Watched throughout the window rather than checked once after it, so a fill that
    // does happen fails at the moment it happens.
    common::no_fills_beyond(&h.nodes[outsider].metrics, 0).await;
    assert_eq!(h.nodes[outsider].metrics.fills_completed.get(), 0);
}

/// Invalidation fan-out (ADR-0016/0017): a write must drop the chunk at EVERY
/// holder — all R co-homes AND any layer-1 admitter the directory lists — so
/// read-after-write holds cluster-wide once copies are plural. Here R=2 on a
/// 3-node cluster: both homes fill, the third node admits a hot local copy,
/// then a write through any node must leave all three serving the new bytes.
#[tokio::test]
async fn write_invalidates_all_homes_and_admitters() {
    let h = cluster_with(&["node-a", "node-b", "node-c"], 2).await;
    let v1 = big_body(12, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/fanout.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(v1.clone()))
        .send()
        .await
        .unwrap();
    let chunk_key = chunk0_key(&format!("{BUCKET}/{key}"));
    let (homes, outsider) = h.homes_and_outsider(&chunk_key, 2);

    // Warm both homes (each fills its own copy) ...
    for &home in &homes {
        h.nodes[home]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap();
        wait_for_fill(&h.nodes[home].metrics, 0).await;
    }
    // ... and make the outsider admit a hot local copy (2 fetches ≥ threshold).
    for _ in 0..2 {
        h.nodes[outsider]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap()
            .body
            .collect()
            .await
            .unwrap();
    }
    assert_eq!(h.nodes[outsider].metrics.local_admits.get(), 1);

    // Wait for the admit-announce (fire-and-forget) to reach the home, so the
    // outsider's copy is discoverable ONLY via the directory sharer set — that
    // is exactly the fan-out path this test must exercise.
    let home_node = h.ring.load().members()[homes[0]].clone();
    let outsider_name = h.nodes[outsider].name.clone();
    wait_announced(&home_node, &chunk_key, &outsider_name).await;

    // Overwrite through a HOME (not the outsider): the outsider's stale copy
    // can now only be cleared by the directory-driven fan-out to admitters.
    let v2 = big_body(13, (MIN_OBJECT_SIZE + 2048) as usize);
    h.nodes[homes[0]]
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(v2.clone()))
        .send()
        .await
        .unwrap();

    // Read via ALL three nodes: fresh bytes everywhere (no stale home/admitter).
    for n in 0..h.nodes.len() {
        let got = h.nodes[n]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes(),
            v2,
            "stale read via node {n} after fan-out invalidation"
        );
    }
}

/// A GET on the owner itself is pure Phase 1: local fill, no peer traffic.
#[tokio::test]
async fn owner_read_is_local() {
    let h = cluster().await;
    let body = big_body(2, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/local.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    let (owner, other) = h.owner_and_other(&chunk0_key(&format!("{BUCKET}/{key}")));
    let got = h.nodes[owner]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    wait_for_fill(&h.nodes[owner].metrics, 0).await;
    assert_eq!(h.nodes[owner].metrics.fills_completed.get(), 1);
    assert_eq!(h.nodes[owner].metrics.peer_fetches.get(), 0);
    assert_eq!(h.nodes[other].metrics.peer_serves.get(), 0);

    // Warm hit stays local.
    h.nodes[owner]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    assert_eq!(h.nodes[owner].metrics.cache_hits.get(), 1);
}

/// Range reads across the peer plane, chunked (ADR-0015 supersedes ADR-0011):
/// a cold ranged read fetches its COVERING chunk from the owner, the owner
/// reads that chunk through and fills it, the requester serves the sliced bytes
/// and fills nothing. Subsequent ranges are served from the owner's cached
/// chunk with correct Content-Range.
#[tokio::test]
async fn range_reads_through_peers() {
    let h = cluster().await;
    let len = (MIN_OBJECT_SIZE + 4096) as usize; // one chunk at the harness size
    let body = big_body(3, len);
    let key = "peer/ranged.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let (owner, other) = h.owner_and_other(&chunk0_key(&format!("{BUCKET}/{key}")));

    // Cold ranged read via the non-owner: the covering chunk is fetched from
    // the owner, which reads it through and fills it. The requester serves the
    // sliced bytes and stores nothing (ADR-0012).
    let got = h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .range("bytes=100-4195")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.content_range(),
        Some(format!("bytes 100-4195/{len}").as_str())
    );
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(100..4196)
    );
    assert_eq!(h.nodes[other].metrics.peer_fetches.get(), 1);
    wait_for_fill(&h.nodes[owner].metrics, 0).await;
    assert_eq!(h.nodes[owner].metrics.peer_readthroughs.get(), 1);
    assert_eq!(h.nodes[owner].metrics.fills_completed.get(), 1);
    assert_eq!(h.nodes[other].metrics.fills_completed.get(), 0);

    // A second range via the peer is served from the owner's now-warm chunk.
    let got = h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .range("bytes=-1000")
        .send()
        .await
        .unwrap();
    assert_eq!(
        got.body.collect().await.unwrap().into_bytes(),
        body.slice(len - 1000..len)
    );
    assert_eq!(h.nodes[other].metrics.peer_fetches.get(), 2);
    assert_eq!(h.nodes[owner].metrics.peer_readthroughs.get(), 1); // unchanged

    // Unsatisfiable range → InvalidRange at the client (resolved against the
    // header length before any chunk fetch).
    let err = h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .range(format!("bytes={len}-"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.into_service_error().meta().code(), Some("InvalidRange"));
}

/// Write through ANY node invalidates the owner's copy (cluster
/// read-after-write, ADR-0007 + ADR-0012).
#[tokio::test]
async fn write_through_non_owner_invalidates_owner() {
    let h = cluster().await;
    let v1 = big_body(4, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/raw.bin";
    let (owner, other) = h.owner_and_other(&chunk0_key(&format!("{BUCKET}/{key}")));

    h.nodes[other]
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(v1.clone()))
        .send()
        .await
        .unwrap();

    // Warm the owner's cache.
    h.nodes[owner]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    wait_for_fill(&h.nodes[owner].metrics, 0).await;

    // Overwrite through the NON-owner; the owner's cached v1 must die with it.
    let v2 = big_body(5, (MIN_OBJECT_SIZE + 2048) as usize);
    h.nodes[other]
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(v2.clone()))
        .send()
        .await
        .unwrap();

    // Read via BOTH nodes: fresh bytes everywhere.
    for n in [owner, other] {
        let got = h.nodes[n]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes(),
            v2,
            "stale read via node {n}"
        );
    }

    // DELETE through the non-owner: both nodes then 404.
    h.nodes[other]
        .client
        .delete_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    for n in [owner, other] {
        let err = h.nodes[n]
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap_err();
        assert!(err.into_service_error().is_no_such_key());
    }
}

/// Kill the owner's peer server: reads through the other node must fall back
/// to the backend and still succeed (peer failure is never client-visible).
#[tokio::test]
async fn peer_down_falls_back_to_backend() {
    let h = cluster().await;
    let body = big_body(6, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/fallback.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let (owner, other) = h.owner_and_other(&chunk0_key(&format!("{BUCKET}/{key}")));

    // Point the owner's ring entry at a dead port (simulates node loss between
    // membership epochs — the hard case; a clean epoch would just re-home).
    let members: Vec<NodeId> = h
        .ring
        .load()
        .members()
        .iter()
        .map(|n| {
            if n.name() == h.nodes[owner].name {
                NodeId::new(n.name(), "127.0.0.1:1")
            } else {
                n.clone()
            }
        })
        .collect();
    h.ring.store(members);

    let got = h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);
    assert_eq!(h.nodes[other].metrics.peer_fallbacks.get(), 1);
    // Fallback reads never fill the requester (it doesn't own the key).
    common::no_fills_beyond(&h.nodes[other].metrics, 0).await;
    assert_eq!(h.nodes[other].metrics.fills_completed.get(), 0);
}

/// no-store travels with the peer read: the owner serves a warm object but a
/// cold one is NOT read through (requester goes straight to the backend).
#[tokio::test]
async fn no_store_never_fills_owner() {
    let h = cluster().await;
    let body = big_body(7, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/nostore.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();
    let (owner, other) = h.owner_and_other(&chunk0_key(&format!("{BUCKET}/{key}")));

    let got = h.nodes[other]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .customize()
        .mutate_request(|req| {
            req.headers_mut().insert("cache-control", "no-store");
        })
        .send()
        .await
        .unwrap();
    assert_eq!(got.body.collect().await.unwrap().into_bytes(), body);

    // `no-store` travels with the peer read: neither the owner (which was asked not to
    // read through) nor the requester may admit anything. Both are watched for the whole
    // window, so whichever one breaks the rule is named as it does.
    common::no_fills_beyond(&h.nodes[owner].metrics, 0).await;
    common::no_fills_beyond(&h.nodes[other].metrics, 0).await;
    assert_eq!(h.nodes[owner].metrics.fills_completed.get(), 0);
    assert_eq!(h.nodes[other].metrics.fills_completed.get(), 0);
}

/// `cluster.peerConnections` must produce that many TCP connections to a peer, and the
/// default must still produce exactly one.
///
/// The knob exists because one HTTP/2 connection is one TCP flow and one framing task, which
/// is what the save path's `StoreChunk` leg is capped by
/// (`pacer_transport::DEFAULT_PEER_CONNECTIONS`). Every one of the ceilings it addresses is a
/// property of the *connection count* — so a pool that dialled once and multiplexed, or one
/// whose cursor never moved off index 0, would answer every RPC correctly while measuring the
/// one-flow arm under a wide arm's name. Nothing above the socket can see that, which is why
/// this counts accepts at the listener rather than asserting on a metric.
///
/// A delta rather than an absolute: the harness's own nodes dial each other, and other tests'
/// probes share nothing but the cluster.
#[tokio::test]
async fn a_peer_pool_dials_one_connection_per_configured_flow() {
    /// Wide enough that a stuck cursor is unambiguous, small enough to keep loopback quiet.
    const POOL_WIDTH: usize = 4;

    let h = cluster().await;
    let target = h.ring.load().members()[0].clone();
    let accepted = std::sync::Arc::clone(
        &h.nodes
            .iter()
            .find(|n| n.name == target.name())
            .expect("target node in harness")
            .accepted,
    );
    let count = || accepted.load(std::sync::atomic::Ordering::Relaxed);

    let before = count();
    let wide = GrpcTransport::new(0, "probe-wide").with_connections_per_peer(POOL_WIDTH);
    // More calls than connections: the pool is dialled once and then reused, so a second lap
    // must add no sockets — otherwise the cache is not caching and every RPC pays a handshake.
    for _ in 0..POOL_WIDTH * 2 {
        wide.handshake(&target, pacer_proto::v1::RdmaCapabilities::default(), None)
            .await
            .expect("a pooled handshake");
    }
    assert_eq!(
        count() - before,
        POOL_WIDTH,
        "a pool of {POOL_WIDTH} must open {POOL_WIDTH} connections, once"
    );

    let before = count();
    let narrow = GrpcTransport::new(0, "probe-narrow");
    for _ in 0..POOL_WIDTH * 2 {
        narrow
            .handshake(&target, pacer_proto::v1::RdmaCapabilities::default(), None)
            .await
            .expect("a default handshake");
    }
    assert_eq!(
        count() - before,
        pacer_transport::DEFAULT_PEER_CONNECTIONS,
        "the default must stay the single cached channel it has always been"
    );
}

/// Ring handshake sanity: both directions agree on protocol version.
#[tokio::test]
async fn handshake_roundtrip() {
    let h = cluster().await;
    let transport = GrpcTransport::new(0, "probe");
    for member in h.ring.load().members() {
        let resp = transport
            .handshake(member, pacer_proto::v1::RdmaCapabilities::default(), None)
            .await
            .unwrap();
        assert_eq!(
            resp.protocol_version,
            pacer_transport::grpc::PROTOCOL_VERSION
        );
        assert_eq!(resp.node_id, member.name());
        // Phase 2: nobody advertises RDMA.
        let caps = resp.capabilities.unwrap();
        assert!(!caps.rdma_read && !caps.rdma_write && !caps.send_recv);
    }
}

/// Chunk directory (ADR-0017) over the real gRPC wire: Announce (admit/evict)
/// and LookupSharers round-trip through a live peer server exactly like the
/// pure `pacer_ring::directory` unit tests, but exercised end to end (proto
/// encode/decode, tier mapping, the `PacerPeer` handler) rather than in-process.
#[tokio::test]
async fn directory_announce_and_lookup_roundtrip_over_grpc() {
    let h = cluster().await;
    let home = h.ring.load().members()[0].clone();
    let transport = GrpcTransport::new(0, "probe");
    let key = "peer/directory-probe.bin";

    // The home node serves this key's directory shard; its metrics count the
    // RPCs the B4 Step-6 projection reads off (planning/15).
    let home_metrics = &h
        .nodes
        .iter()
        .find(|n| n.name == home.name())
        .expect("home node in harness")
        .metrics;
    let lookups = |m: &Metrics| {
        m.dir_rpc_seconds
            .with_label_values(&["lookup"])
            .get_sample_count()
    };
    let admits = |m: &Metrics| {
        m.dir_rpc_seconds
            .with_label_values(&["admit"])
            .get_sample_count()
    };
    let evicts = |m: &Metrics| {
        m.dir_rpc_seconds
            .with_label_values(&["evict"])
            .get_sample_count()
    };

    // A miss before any announce: no holders, not widely held.
    assert!(transport
        .lookup_sharers(&home, key)
        .await
        .unwrap()
        .is_none());
    assert_eq!(lookups(home_metrics), 1, "a miss still times a lookup");

    // Admit one holder, then look it up through the wire.
    transport
        .announce_admit(&home, key, "node-x", Tier::Dram, 1)
        .await
        .unwrap();
    assert_eq!(admits(home_metrics), 1);
    let set = transport
        .lookup_sharers(&home, key)
        .await
        .unwrap()
        .expect("admitted holder must be visible");
    assert_eq!(set.holders.len(), 1);
    assert_eq!(set.holders[0].node, "node-x");
    assert_eq!(set.holders[0].tier, Tier::Dram);
    assert_eq!(set.holders[0].generation, 1);
    assert!(!set.widely_held);

    // Evict it: the entry disappears (fully-evicted leaves no residue,
    // mirroring the pure directory test).
    transport
        .announce_evict(&home, key, "node-x", 1)
        .await
        .unwrap();
    assert!(transport
        .lookup_sharers(&home, key)
        .await
        .unwrap()
        .is_none());

    // Every directory op was timed under its own `op` label: 3 lookups
    // (pre-admit miss + post-admit hit + post-evict miss), 1 admit, 1 evict.
    // The `_count` children double as the per-shard RPC counters B4 Step-6
    // projects.
    assert_eq!(lookups(home_metrics), 3);
    assert_eq!(admits(home_metrics), 1);
    assert_eq!(evicts(home_metrics), 1);
}

/// A write's invalidation clears the home's directory entry, not just its
/// cache copy (ADR-0017): a sharer set must not outlive the write that
/// invalidated the underlying object.
#[tokio::test]
async fn invalidate_clears_the_directory_entry_too() {
    let h = cluster().await;
    let home = h.ring.load().members()[0].clone();
    let transport = GrpcTransport::new(0, "probe");
    let key = "peer/directory-invalidate-probe.bin";

    transport
        .announce_admit(&home, key, "node-x", Tier::Nvme, 1)
        .await
        .unwrap();
    assert!(transport
        .lookup_sharers(&home, key)
        .await
        .unwrap()
        .is_some());

    transport.invalidate(&home, key).await.unwrap();
    assert!(transport
        .lookup_sharers(&home, key)
        .await
        .unwrap()
        .is_none());
}

/// A real GET that fills the owner registers the owner as a holder in its own
/// directory shard (ADR-0017 "home fills first, home-is-holder"): after the
/// fill, a LookupSharers on the owner lists exactly the owner, at the DRAM
/// tier hint. This is the admit-on-fill path, driven end to end through the
/// S3 client — no manual Announce.
#[tokio::test]
async fn owner_fill_self_registers_in_the_directory() {
    let h = cluster().await;
    let body = big_body(8, (MIN_OBJECT_SIZE + 1024) as usize);
    let key = "peer/self-register.bin";
    h.backend
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(body.clone()))
        .send()
        .await
        .unwrap();

    let chunk_key = chunk0_key(&format!("{BUCKET}/{key}"));
    let (owner, _other) = h.owner_and_other(&chunk_key);
    let owner_node = h.ring.load().members()[owner].clone();

    // Nothing cached yet → the owner's shard has no entry for this chunk.
    let probe = GrpcTransport::new(0, "probe");
    assert!(probe
        .lookup_sharers(&owner_node, &chunk_key)
        .await
        .unwrap()
        .is_none());

    // A GET on the owner fills locally (Phase 1 path) and self-registers.
    h.nodes[owner]
        .client
        .get_object()
        .bucket(BUCKET)
        .key(key)
        .send()
        .await
        .unwrap()
        .body
        .collect()
        .await
        .unwrap();
    wait_for_fill(&h.nodes[owner].metrics, 0).await;

    let set = probe
        .lookup_sharers(&owner_node, &chunk_key)
        .await
        .unwrap()
        .expect("owner must register itself after filling");
    assert_eq!(set.holders.len(), 1);
    assert_eq!(set.holders[0].node, owner_node.name());
    assert_eq!(set.holders[0].tier, Tier::Dram);
    assert!(!set.widely_held);
}

/// An `s3s-fs` backend whose `HeadObject` answers `ServiceUnavailable` while `failing`
/// is raised — the throttle or 5xx that a write's invalidation used to read as "the key
/// does not exist". Every other call is delegated untouched.
struct HeadGate {
    inner: s3s_fs::FileSystem,
    failing: Arc<AtomicBool>,
}

#[async_trait::async_trait]
impl s3s::S3 for HeadGate {
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

    async fn get_object(
        &self,
        req: S3Request<dto::GetObjectInput>,
    ) -> S3Result<S3Response<dto::GetObjectOutput>> {
        self.inner.get_object(req).await
    }

    async fn head_object(
        &self,
        req: S3Request<dto::HeadObjectInput>,
    ) -> S3Result<S3Response<dto::HeadObjectOutput>> {
        if self.failing.load(Ordering::SeqCst) {
            return Err(s3_error!(ServiceUnavailable, "HeadGate is failing HEADs"));
        }
        self.inner.head_object(req).await
    }
}

/// [`cluster`] over a [`HeadGate`], with the switch that makes its HEADs fail.
async fn head_gated_cluster() -> (ClusterHarness, Arc<AtomicBool>) {
    let backend_dir = tempfile::tempdir().unwrap();
    let failing = Arc::new(AtomicBool::new(false));
    let (service, creds) = backend_service(HeadGate {
        inner: s3s_fs::FileSystem::new(backend_dir.path()).unwrap(),
        failing: Arc::clone(&failing),
    });
    let h = cluster_on(&["node-a", "node-b"], 1, service, creds, backend_dir).await;
    (h, failing)
}

/// How a test removes an object: the two DELETE operations take different paths.
#[derive(Clone, Copy, Debug)]
enum Removal {
    DeleteObject,
    DeleteObjects,
}

/// ADR-0042 (#20): a DELETE through a node that holds no header purges the object's
/// chunks everywhere.
///
/// The old code sized the purge with a HEAD sent *after* the delete, which can only
/// answer 404, so a node without the header dropped the header key and no chunk. Those
/// chunks are invisible to a GET (the header is gone and the backend says 404), which
/// is why `write_through_non_owner_invalidates_owner` passes regardless — they surface
/// only when the key is written again by a path that does not purge them (#21). So this
/// arm asserts the cache itself rather than a read.
#[rstest::rstest]
#[case::delete_object(Removal::DeleteObject)]
#[case::delete_objects(Removal::DeleteObjects)]
#[tokio::test]
async fn a_delete_through_a_headerless_node_purges_every_chunk(#[case] removal: Removal) {
    let h = cluster().await;
    let key = "peer/deleted.bin";
    let object = format!("{BUCKET}/{key}");
    let chunk = chunk0_key(&object);
    let (chunk_home, _) = h.owner_and_other(&chunk);
    let (header_home, deleter) = h.owner_and_other(&object);
    h.put_and_warm(
        key,
        &big_body(7, (MIN_OBJECT_SIZE + 1024) as usize),
        deleter,
        chunk_home,
    )
    .await;
    assert!(
        !h.holders(&chunk).await.is_empty(),
        "precondition: the chunk is cached somewhere"
    );
    assert!(
        !h.holders(&object).await.contains(&h.nodes[deleter].name),
        "precondition: the deleting node holds no header (only {} may)",
        h.nodes[header_home].name
    );

    let client = &h.nodes[deleter].client;
    match removal {
        Removal::DeleteObject => {
            client
                .delete_object()
                .bucket(BUCKET)
                .key(key)
                .send()
                .await
                .unwrap();
        }
        Removal::DeleteObjects => {
            let target = ObjectIdentifier::builder().key(key).build().unwrap();
            let batch = Delete::builder().objects(target).build().unwrap();
            client
                .delete_objects()
                .bucket(BUCKET)
                .delete(batch)
                .send()
                .await
                .unwrap();
        }
    }

    assert_eq!(
        h.holders(&chunk).await,
        Vec::<String>::new(),
        "{removal:?} through a headerless node must purge the chunk on every node"
    );
}

/// ADR-0042 (#20): an overwrite whose footprint cannot be measured is refused, never
/// left half-invalidated.
///
/// The old code treated a failed HEAD like a 404: the PUT reached the backend, the
/// header was purged, the chunk survived, and the next GET resolved the new object's
/// length and assembled the old one's bytes. Now the HEAD runs before the write, and a
/// failure refuses it, so the backend and every node still agree afterwards.
#[tokio::test]
async fn an_overwrite_whose_footprint_cannot_be_measured_is_refused() {
    let (h, head_fails) = head_gated_cluster().await;
    let key = "peer/overwritten.bin";
    let object = format!("{BUCKET}/{key}");
    let (chunk_home, _) = h.owner_and_other(&chunk0_key(&object));
    let (_, writer) = h.owner_and_other(&object);
    let len = (MIN_OBJECT_SIZE + 1024) as usize;
    h.put_and_warm(key, &big_body(8, len), writer, chunk_home)
        .await;

    // Same length, different bytes: a stale chunk would then be served under the new
    // object's length without any size mismatch to give it away.
    head_fails.store(true, Ordering::SeqCst);
    let no_retry = aws_sdk_s3::config::Builder::default().retry_config(RetryConfig::disabled());
    let overwrite = h.nodes[writer]
        .client
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(big_body(9, len)))
        .customize()
        .config_override(no_retry)
        .send()
        .await;
    head_fails.store(false, Ordering::SeqCst);

    let truth = h
        .backend
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
        .into_bytes();
    for node in &h.nodes {
        let got = node
            .client
            .get_object()
            .bucket(BUCKET)
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(
            got.body.collect().await.unwrap().into_bytes(),
            truth,
            "{} must serve what the backend holds",
            node.name
        );
    }
    let code = overwrite.map_err(|e| e.into_service_error().meta().code().map(str::to_owned));
    assert_eq!(
        code.err(),
        Some(Some("ServiceUnavailable".to_owned())),
        "the overwrite must be refused with a retryable 503"
    );
    assert_eq!(h.nodes[writer].metrics.writes_refused.get(), 1);
}
