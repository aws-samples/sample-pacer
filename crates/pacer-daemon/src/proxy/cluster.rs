//! The cluster tier's view of one key: who homes it, who holds it, and the
//! order in which to ask them.
//!
//! Implements the source side of ADR-0012 (peer fetch instead of the backend for
//! a key another node owns), ADR-0016 (layer 2's R co-homes, layer 1's
//! requester-local admission) and ADR-0017 (the directory shard's holder hints).
//!
//! **The invariant this module owns: one derivation of a chunk's source list.**
//! [`chunk_sources`] is a free function, not a method, because the read path and
//! the pre-flight query ([`crate::preflight`]) must not disagree — a pre-flight
//! that names holders the read would never ask is worse than no pre-flight at
//! all. [`is_home`] is shared for the same reason between the proxy's ownership
//! checks and the peer server's read-through gate.

use std::sync::Arc;

use pacer_cache::admission::AdmissionGate;
use pacer_ring::directory::{Holder, SharedDirectory, Tier};
use pacer_ring::SharedRing;
use pacer_transport::PeerTransport;
use tracing::trace;

/// Ring-aware peer fetch state (ADR-0012).
#[derive(Clone)]
pub struct Cluster {
    /// Live view of key ownership (updated by the membership watch).
    pub ring: SharedRing,
    /// This node's directory shard (ADR-0017), shared with the peer server.
    /// An owner-fill records itself here so the chunk's sharer set is
    /// populated — and since a chunk's home IS its owner (same hash), this is
    /// a local write, no RPC (ADR-0017 "home fills first, home-is-holder").
    pub directory: SharedDirectory,
    /// How peer blobs and invalidations move (gRPC in Phase 2).
    pub transport: Arc<dyn PeerTransport>,
    /// This node's stable name (matches NodeId::name in the ring).
    pub local_node: String,
    /// Buffered chunks when relaying a peer blob to the client (ADR-0013
    /// tunable; the tonic→client Sync-relay channel in `serve_peer_blob`).
    pub channel_capacity: usize,
    /// Replication factor R (ADR-0016 layer 2): the top-R ranked nodes co-home
    /// every chunk, so a chunk is "owned" (filled + directory-homed) by any of
    /// R nodes, not just the single rendezvous winner. `1` is ADR-0012.
    pub replication_r: usize,
    /// Requester-local admission gate (ADR-0016 layer 1): decides when a
    /// peer-owned chunk is hot enough to keep a local copy of. Shared per node.
    pub admission: Arc<AdmissionGate>,
    /// The concrete EFA transport, when this node's RDMA plane came up.
    ///
    /// The one place the read path needs more than the [`PeerTransport`] trait:
    /// ADR-0026's delivery registers a *client's* memory and offers it to
    /// holders, which is RDMA-specific by nature and has no meaning for the gRPC
    /// transport (it would be a copy either way). `None` — and the whole field on
    /// a non-`efa` build — simply means deliveries land via the daemon.
    #[cfg(feature = "efa")]
    pub efa: Option<Arc<pacer_transport::efa::EfaRdmaTransport>>,
}

/// Whether `cluster.local_node` is one of `cache_key`'s top-R co-homes
/// (ADR-0016 layer 2). An empty ring (no owner yet) counts as a home so a
/// not-yet-converged node still fills locally rather than looping on peers.
/// Shared by the proxy (header + chunk ownership) and the peer read-through
/// gate so both roles agree on "am I a home for this key".
pub(super) fn is_home(cluster: &Cluster, cache_key: &str) -> bool {
    let homes = cluster.ring.homes(cache_key, cluster.replication_r);
    trace!(
        cache_key,
        homes = ?homes.iter().map(|n| n.name()).collect::<Vec<_>>(),
        local_node = %cluster.local_node,
        replication_r = cluster.replication_r,
        "is_home computed"
    );
    homes.is_empty() || homes.iter().any(|n| n.name() == cluster.local_node)
}

/// Starting index into a chunk's `len` co-homes for a requester on
/// `local_node` (ADR-0016 layer 2 source selection). Deterministic per
/// (requester, chunk) so retries are stable, but varied across requesters so
/// the cluster-wide read load for one hot chunk spreads over all R homes
/// rather than hammering `ranked[0]`. Not a security boundary — a plain
/// `DefaultHasher` (build-local) is fine: nothing on the wire depends on it,
/// unlike the ring's stable `score`.
fn home_offset(local_node: &str, chunk_key: &str, len: usize) -> usize {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    local_node.hash(&mut h);
    chunk_key.hash(&mut h);
    (h.finish() % len as u64) as usize
}

/// Order directory holders by tier hint (ADR-0017): DRAM holders first (faster
/// to serve from), NVMe after. A stable sort preserves the directory's order
/// within a tier, so ties don't reshuffle across calls.
fn sorted_by_tier(mut holders: Vec<Holder>) -> Vec<Holder> {
    holders.sort_by_key(|h| match h.tier {
        Tier::Dram => 0u8,
        Tier::Nvme => 1,
    });
    holders
}

/// Merge directory `holders` into `nodes` (DRAM-hinted first), resolving each
/// holder's node name to a dialable [`NodeId`] via the ring and skipping this
/// node and any node already listed. Shared by the read path (source selection)
/// and the write path (invalidation fan-out) — both union the ring-computed
/// homes with the directory's layer-1 admitters.
pub(super) fn append_holders(
    cluster: &Cluster,
    holders: Vec<Holder>,
    nodes: &mut Vec<pacer_ring::NodeId>,
) {
    let ring = cluster.ring.load();
    for holder in sorted_by_tier(holders) {
        let known =
            holder.node == cluster.local_node || nodes.iter().any(|n| n.name() == holder.node);
        if known {
            continue;
        }
        if let Some(node) = ring.member(&holder.node) {
            nodes.push(node.clone());
        }
    }
}

/// Ordered peer sources to try for `chunk_key` (ADR-0016 layer 2): the R
/// co-homes, rotated from a per-requester offset so the cluster-wide readers
/// of one hot chunk fan out across the R homes instead of converging on
/// `ranked[0]`. Computed **entirely from the local ring** — no RPC — so it
/// is synchronous and never on a network round-trip.
///
/// A free function rather than a method because there are now **two** callers that
/// must not disagree: the read path (`FillCtx::deliver_from_peer` /
/// `FillCtx::fetch_from_peer`) and the pre-flight query
/// ([`crate::preflight`]), which predicts the holder set a client should build
/// address handles for. A pre-flight derived from a second copy of this rule
/// would prime addresses no writer uses, which is worse than not priming at all.
///
/// FUTURE (layer-1 widening, ADR-0017): the home's directory shard may list
/// layer-1 admitters — non-home nodes that cached this chunk once it proved
/// hot — beyond the R co-homes. Folding them in spreads a hot chunk's read
/// load onto those extra copies. This used to be done here with a blocking
/// `lookup_sharers` RPC to the home *before* the data fetch, i.e. every
/// peer chunk fetch paid two serial round-trips (directory lookup, then
/// fetch) even though the R homes alone are always a complete, valid source
/// list (they fill on read-through). On the B4 restore-storm hot path — CPU
/// and NIC idle, throughput latency-bound — that optional second RTT was
/// pure tax, so it was dropped from the blocking path (planning/15). To
/// restore layer-1 widening without the latency, consult the directory
/// **off** the fetch path: e.g. cache the sharer set per object across its
/// chunks, prefetch it asynchronously, or look it up only after the R homes
/// are detected hot — then merge via [`append_holders`].
pub(crate) fn chunk_sources(cluster: &Cluster, chunk_key: &str) -> Vec<pacer_ring::NodeId> {
    let mut sources = cluster.ring.homes(chunk_key, cluster.replication_r);
    if !sources.is_empty() {
        let offset = home_offset(&cluster.local_node, chunk_key, sources.len());
        sources.rotate_left(offset);
    }
    sources
}
