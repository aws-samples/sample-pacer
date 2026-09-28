//! ADR-0041 step 4 (`planning/29-auth-modes.md` § 4): `requester` mode on a cluster.
//!
//! Two properties, each the one a reviewer should check first:
//!
//! * **A home never reads S3 for a requester** — enforced by the peer server itself,
//!   not by the requester's `no_fill`/`populate_only` flags, because the peer plane
//!   authenticates nobody. A requester-mode peer answers a `FetchBlob` miss with "not
//!   cached" and refuses a `StoreChunk` that asks it to upload.
//! * **A chunk nobody holds is read by the requesting node and pushed to its home**
//!   as a populate-only window, so the home becomes a holder without an identity. The
//!   push carries bytes only: the caller's signature never leaves the node it arrived
//!   on (a `StoreOffer` has no field that could carry one).
//!
//! client ⇢ TCP ⇢ RequesterFront[RequesterS3] on node A ⇢ held-signature reads ⇢ fake S3
//!                                               └──── StoreChunk{populate_only} ⇢ node B

use std::net::SocketAddr;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use aws_sdk_s3::primitives::ByteStream;
use bytes::Bytes;
use pacer_cache::chunk::ChunkConfig;
use pacer_daemon::authz::{Forwarder, RequesterFront};
use pacer_daemon::listen::ListenLimits;
use pacer_daemon::proxy::{PacerProxy, RequesterS3};
use pacer_daemon::shutdown::Shutdown;
use pacer_daemon::staging::StagingArea;
use pacer_ring::{NodeId, SharedRing};
use pacer_transport::grpc::GrpcTransport;
use pacer_transport::{PeerTransport, StoreOffer, StoreOutcome, StoreRefusal, TransportError};
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpListener, TcpStream};

use super::authz_requester_get::{
    authorization, read_response, spawn_fake_s3, CHUNK_SIZE, OBJECT_BYTES, OBJECT_PATH,
};
use crate::common::{
    self, create_test_bucket, fs_backend_service, node_parts, sdk_client_for, serve_peer,
    CacheSpec, NodeParts, NodeSpec, BUCKET,
};

/// Single-copy placement, so every chunk has exactly one home and "the home" is
/// unambiguous in every assertion here.
const REPLICATION_R: usize = 1;
/// Staging ample for every window these tests offer, so a refusal is never a budget.
const STAGING_BYTES: usize = 1 << 20;
/// Long enough that nothing is reaped inside a test.
const STAGING_TTL: Duration = Duration::from_secs(900);
/// In-flight chunk resolutions per GET on a requester node.
const FILL_PARALLELISM: usize = 4;
/// The two nodes of the end-to-end arm.
const NODES: [&str; 2] = ["node-a", "node-b"];

fn spec(name: &str) -> NodeSpec<'_> {
    NodeSpec {
        name,
        replication_r: REPLICATION_R,
        // No floor: the fixture object is 40 bytes.
        min_object_size: 0,
        max_object_size: None,
        chunk: ChunkConfig::new(CHUNK_SIZE),
        cache: CacheSpec::default(),
    }
}

/// One peer server over an s3s-fs backend holding `key`, with a staging area,
/// in the given mode. Returns the node and what must outlive the test.
async fn one_peer(requester_mode: bool, key: &str) -> (NodeId, NodeParts, tempfile::TempDir) {
    let backend_dir = tempfile::tempdir().expect("a temp dir for the backend");
    let (service, creds) = fs_backend_service(backend_dir.path());
    let truth = sdk_client_for(service.clone(), creds.clone());
    create_test_bucket(&truth).await;
    truth
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(ByteStream::from(Bytes::from_static(OBJECT_BYTES)))
        .send()
        .await
        .expect("seeding the backend");

    let ring = SharedRing::default();
    let spec = spec(NODES[0]);
    let parts = node_parts(&spec, &ring, &service, &creds).await;
    let staging = Arc::new(StagingArea::new(STAGING_BYTES, STAGING_TTL));
    let filling = pacer_daemon::proxy::FillRegistry::new();
    let (addr, _) = serve_peer(&spec, &parts, filling, Some(staging), requester_mode).await;
    let node = NodeId::new(NODES[0], addr.to_string());
    ring.store(vec![node.clone()]);
    (node, parts, backend_dir)
}

/// The first chunk key of `key` in [`BUCKET`], which the single node homes.
fn first_chunk_key(key: &str) -> String {
    ChunkConfig::new(CHUNK_SIZE).chunk_key(&format!("{BUCKET}/{key}"), 0)
}

fn offer<'a>(chunk_key: &'a str, key: &'a str, populate_only: bool) -> StoreOffer<'a> {
    StoreOffer {
        chunk_key,
        upload_id: "authz-cluster",
        bucket: BUCKET,
        key,
        part_number: 1,
        body: Bytes::from_static(&OBJECT_BYTES[..CHUNK_SIZE as usize]),
        checksum_crc32: "",
        populate_only,
    }
}

/// The server-side gate. The node-mode control proves the same call really does
/// read through when the gate is off, so a `NotCached` here cannot be a fixture
/// that simply had nothing to read.
#[tokio::test]
async fn a_requester_mode_home_never_reads_through_or_uploads() {
    let key = "authz/cluster-gate";
    let chunk_key = first_chunk_key(key);
    let probe = GrpcTransport::new(0, "probe");

    let (node_mode, _parts, _dir) = one_peer(false, key).await;
    assert!(
        probe
            .fetch_blob(&node_mode, &chunk_key, None, false)
            .await
            .is_ok(),
        "control: a node-mode home reads through on a miss"
    );

    let (requester, _parts, _dir) = one_peer(true, key).await;
    let miss = probe.fetch_blob(&requester, &chunk_key, None, false).await;
    assert!(
        matches!(miss, Err(TransportError::NotCached)),
        "a requester-mode home must not read S3 for anyone, even without no_fill"
    );
    assert_eq!(
        probe
            .store_chunk(&requester, offer(&chunk_key, key, false))
            .await
            .expect("a refusal is an answer, not a transport error"),
        StoreOutcome::Refused(StoreRefusal::NotAccepting),
        "a requester-mode home must refuse to upload a part with its own identity"
    );
    assert_eq!(
        probe
            .store_chunk(&requester, offer(&chunk_key, key, true))
            .await
            .unwrap(),
        StoreOutcome::Staged,
        "the populate-only offer is the one a requester-mode home takes"
    );
}

/// A requester-mode node: `RequesterFront[RequesterS3]` over TCP in front of a
/// clustered proxy, and a requester-mode peer server with a staging area.
struct RequesterNode {
    s3: SocketAddr,
    parts: NodeParts,
    _shutdown: Shutdown,
}

async fn requester_node(
    name: &str,
    ring: &SharedRing,
    upstream: SocketAddr,
    backend: (&s3s::service::S3Service, &aws_sdk_s3::config::Credentials),
) -> (RequesterNode, NodeId) {
    let spec = spec(name);
    let parts = node_parts(&spec, ring, backend.0, backend.1).await;
    // `parts.backend` is a throwaway s3s-fs this node's identity reaches. Nothing
    // in requester mode may read it, and the fake upstream's request count proves
    // nothing did: every byte a client receives comes from there.
    let proxy = PacerProxy::new(
        parts.backend.clone(),
        parts.tier.clone(),
        parts.metrics.clone(),
        0,
        None,
        spec.chunk,
        FILL_PARALLELISM,
    )
    .with_cluster(parts.cluster.clone());
    let staging = Arc::new(StagingArea::new(STAGING_BYTES, STAGING_TTL));
    let (peer_addr, _) = serve_peer(&spec, &parts, proxy.filling(), Some(staging), true).await;

    let forwarder = Arc::new(Forwarder::new());
    let domains = || s3s::host::MultiDomain::new([upstream.to_string()]).expect("a valid domain");
    let mut b =
        s3s::service::S3ServiceBuilder::new(RequesterS3::new(proxy, Arc::clone(&forwarder)));
    b.set_host(domains());
    let front = RequesterFront::new(
        b.build(),
        Arc::new(domains()),
        forwarder,
        parts.metrics.clone(),
    );
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind loopback");
    let s3 = listener.local_addr().expect("a bound port has an address");
    let shutdown = Shutdown::new();
    tokio::spawn(pacer_daemon::listen::serve_s3_on(
        listener,
        front,
        ListenLimits::default(),
        parts.metrics.clone(),
        shutdown.signal(),
    ));
    (
        RequesterNode {
            s3,
            parts,
            _shutdown: shutdown,
        },
        NodeId::new(name, peer_addr.to_string()),
    )
}

/// One whole-object GET of the fixture through `node`, as a client with `range`
/// left unsigned would send it.
async fn get_through(node: &RequesterNode, upstream: SocketAddr) -> Vec<u8> {
    let mut client = TcpStream::connect(node.s3)
        .await
        .expect("connect to the daemon");
    let request = format!(
        "GET http://{upstream}{OBJECT_PATH} HTTP/1.1\r\n\
         Host: {upstream}\r\n\
         Authorization: {auth}\r\n\
         Connection: close\r\n\
         \r\n",
        auth = authorization("host;x-amz-date"),
    );
    client
        .write_all(request.as_bytes())
        .await
        .expect("write request");
    let (status, body) = read_response(&mut client).await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    body
}

/// The end-to-end arm. A cold read through the node that homes the fewest chunks
/// (so at least two chunks are homed elsewhere and have to be pushed) leaves every
/// chunk held somewhere; a second read through the other node then costs S3
/// exactly one request — its own authorization probe — and no chunk read at all.
#[tokio::test]
async fn a_cold_read_populates_the_homes_it_does_not_own() {
    let (upstream, _upstream_shutdown, requests) = spawn_fake_s3().await;
    let backend_dir = tempfile::tempdir().expect("a temp dir for the throwaway backend");
    let (service, creds) = fs_backend_service(backend_dir.path());
    let ring = SharedRing::default();
    let mut nodes = Vec::new();
    let mut members = Vec::new();
    for name in NODES {
        let (node, id) = requester_node(name, &ring, upstream, (&service, &creds)).await;
        nodes.push(node);
        members.push(id);
    }
    ring.store(members);

    let chunk = ChunkConfig::new(CHUNK_SIZE);
    let chunk_count = chunk.chunk_count(OBJECT_BYTES.len() as u64);
    let homed_at = |name: &str| {
        (0..chunk_count)
            .filter(|&i| {
                ring.homes(&chunk.chunk_key("bkt/obj", i), REPLICATION_R)[0].name() == name
            })
            .count() as u64
    };
    let (reader, other) = if homed_at(NODES[0]) <= homed_at(NODES[1]) {
        (0, 1)
    } else {
        (1, 0)
    };
    let pushed = chunk_count - homed_at(NODES[reader]);
    assert!(
        pushed >= 2,
        "three chunks over two nodes leave the lighter one ≥ 2 to push"
    );

    assert_eq!(get_through(&nodes[reader], upstream).await, OBJECT_BYTES);
    let populate = nodes[reader].parts.metrics.authz.populate_windows.clone();
    common::poll_until("every pushed window committed at its home", || {
        let populate = populate.clone();
        async move { populate.with_label_values(&["committed"]).get() == pushed }
    })
    .await;

    let before = requests.load(Ordering::SeqCst);
    assert_eq!(get_through(&nodes[other], upstream).await, OBJECT_BYTES);
    assert_eq!(
        requests.load(Ordering::SeqCst) - before,
        1,
        "the second read must cost only its own probe: every chunk is held somewhere"
    );
}
