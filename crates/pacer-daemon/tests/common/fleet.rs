//! One node of a multi-node fleet, in the two halves the fleets differ between.
//!
//! `daemon/cluster.rs` and `daemon/scatter/` both stand up N full daemons — proxy, peer
//! gRPC server on loopback, shared ring, one shared backend — and the 429-line `fleet()`
//! and 139-line `cluster_with()` that did it were the same code twice, diverging at
//! exactly one point: whether the node's proxy carries a [`ScatterCoordinator`] and its
//! peer server shares that node's [`StagingArea`].
//!
//! So the assembly is split there. [`node_parts`] is everything a node needs *before*
//! its proxy exists; [`serve_node`] is everything after one is configured. What happens
//! in between is the caller's, and it is the only thing either fleet has to write.
//!
//! [`ScatterCoordinator`]: pacer_daemon::coordinate::ScatterCoordinator

use std::net::SocketAddr;
use std::sync::Arc;

use aws_sdk_s3::config::Credentials;
use pacer_cache::chunk::ChunkConfig;
use pacer_cache::tier::ChunkTier;
use pacer_daemon::metrics::Metrics;
use pacer_daemon::peer::{PacerPeer, PeerParts};
use pacer_daemon::proxy::{Cluster, PacerProxy};
use pacer_daemon::staging::StagingArea;
use pacer_ring::directory::SharedDirectory;
use pacer_ring::SharedRing;
use pacer_transport::grpc::GrpcTransport;
use s3s::service::S3Service;

use super::single::daemon_service;
use super::{
    build_cache, placeholder_credentials, sdk_client_for, CacheSpec, CHANNEL_CAPACITY,
    FILL_PARALLELISM, LOCAL_ADMISSION_RATIO, LOCAL_ADMISSION_THRESHOLD, LOCAL_ADMISSION_WINDOW,
    LOOPBACK_ANY_PORT,
};

/// What one node of a fleet is parameterised by. The fields a fleet varies per node
/// (its name) and the ones it holds constant across the fleet (everything else) in one
/// place, so a fleet builder is a loop over names rather than a loop over a config.
#[derive(Debug, Clone, Copy)]
pub struct NodeSpec<'a> {
    /// This node's name in the ring. Also its identity to its peers.
    pub name: &'a str,
    /// Copies per chunk (ADR-0012 / ADR-0016 layer 2).
    pub replication_r: usize,
    /// Read-path cache floor (ADR-0002).
    pub min_object_size: u64,
    /// Read-path cache ceiling, or `None`.
    pub max_object_size: Option<u64>,
    /// The cache's grid — and the fleet's window and part size where the scatter is on.
    pub chunk: ChunkConfig,
    /// foyer's capacities.
    pub cache: CacheSpec,
}

/// Everything a node needs before its proxy exists.
///
/// The `directory` is deliberately reachable from here as well as through
/// `cluster.directory`: a node's proxy and its peer server share ONE shard, because the
/// proxy's fills populate it and a peer `LookupSharers` reads it back (ADR-0017).
pub struct NodeParts {
    /// This node's registry.
    pub metrics: Metrics,
    /// This node's chunk tier.
    pub tier: ChunkTier,
    /// This node's directory shard, shared by its proxy and its peer server.
    pub directory: SharedDirectory,
    /// The cluster view this node's proxy and peer server run against.
    pub cluster: Cluster,
    /// This node's own client to the shared backend.
    pub backend: aws_sdk_s3::Client,
    /// This node's cache directory, held only so it outlives the node.
    pub cache_dir: tempfile::TempDir,
}

/// Assemble a node's cache, registry, directory shard and cluster view.
///
/// # Panics
///
/// If the temp dir or the cache cannot be brought up.
pub async fn node_parts(
    spec: &NodeSpec<'_>,
    ring: &SharedRing,
    backend: &S3Service,
    creds: &Credentials,
) -> NodeParts {
    let cache_dir = tempfile::tempdir().expect("a temp dir for the cache");
    let cache = build_cache(cache_dir.path(), spec.cache).await;
    let directory = SharedDirectory::new(pacer_ring::directory::DEFAULT_MAX_SHARERS_TRACKED);
    let cluster = Cluster {
        ring: ring.clone(),
        directory: directory.clone(),
        transport: Arc::new(GrpcTransport::new(0, spec.name)),
        local_node: spec.name.to_owned(),
        channel_capacity: CHANNEL_CAPACITY,
        replication_r: spec.replication_r,
        // Layer-1 admission (ADR-0016) at the production threshold, with a byte budget
        // large enough it never binds: these tests drive fetch *counts*, not the window,
        // so a wall-clock start is fine.
        admission: Arc::new(pacer_cache::admission::AdmissionGate::new(
            LOCAL_ADMISSION_THRESHOLD,
            LOCAL_ADMISSION_WINDOW,
            LOCAL_ADMISSION_RATIO,
            u64::MAX,
            std::time::Instant::now(),
        )),
        // In-process fleets run entirely over gRPC on loopback; ADR-0026's
        // client-memory delivery is what needs a concrete EFA handle, and it has no
        // in-process test (it needs real hardware — planning/19 Track C).
        #[cfg(feature = "efa")]
        efa: None,
    };
    NodeParts {
        metrics: Metrics::new().expect("a fresh registry"),
        // No slab in an in-process fleet: these nodes have no RDMA plane, so cached
        // chunks belong on the heap (ADR-0028's default).
        tier: ChunkTier::foyer(cache, Default::default()),
        directory,
        cluster,
        backend: sdk_client_for(backend.clone(), creds.clone()),
        cache_dir,
    }
}

/// A node that is up: an S3 front a client can address, and a peer port its ring entry
/// can name.
pub struct ServedNode {
    /// Client pointing at this node's S3 front (placeholder creds).
    pub client: aws_sdk_s3::Client,
    /// The loopback port this node's peer gRPC server is on.
    pub peer_addr: SocketAddr,
}

/// Put a configured proxy behind an S3 front and a peer gRPC server on a loopback port.
///
/// `staging` is the one thing the two fleets disagree about: the scatter fleet hands the
/// peer the *same* [`StagingArea`] its coordinator holds, because a coordinator's own
/// windows wait for the same Complete an owner's do (ADR-0032 § 4), and a second
/// `StagingArea` would publish a budget nothing consults.
///
/// # Panics
///
/// If loopback cannot be bound.
pub async fn serve_node(
    spec: &NodeSpec<'_>,
    parts: &NodeParts,
    proxy: PacerProxy,
    staging: Option<Arc<StagingArea>>,
) -> ServedNode {
    let mut peer = PacerPeer::new(PeerParts {
        tier: parts.tier.clone(),
        backend: parts.backend.clone(),
        chunk: spec.chunk,
        ring: parts.cluster.ring.clone(),
        directory: parts.directory.clone(),
        local_node: spec.name.to_owned(),
        replication_r: spec.replication_r,
        min_object_size: spec.min_object_size,
        max_object_size: spec.max_object_size,
        channel_capacity: CHANNEL_CAPACITY,
        metrics: parts.metrics.clone(),
        filling: proxy.filling(),
        fill_parallelism: FILL_PARALLELISM,
        // Heap, not the ADR-0028 slab: an in-process fleet has no RDMA plane, so
        // there is no slab to fill into (same reason `efa` below is `None`).
        fill: Default::default(),
        #[cfg(feature = "efa")]
        efa: None,
        #[cfg(feature = "efa")]
        rdma_runtime: tokio::runtime::Handle::current(),
    });
    if let Some(staging) = staging {
        peer = peer.with_staging(staging);
    }

    let listener = tokio::net::TcpListener::bind(LOOPBACK_ANY_PORT)
        .await
        .expect("binding the peer port");
    let peer_addr = listener.local_addr().expect("a bound port has an address");
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(peer.into_service())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );

    ServedNode {
        client: sdk_client_for(daemon_service(proxy), placeholder_credentials()),
        peer_addr,
    }
}
