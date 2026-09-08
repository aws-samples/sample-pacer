//! Per-peer address-handle cache — the symmetric-AH requirement (ADR-0018
//! finding 6/ADR-0019): SRD is reliable, so every WRITE/READ generates
//! transport ACKs the target must send back to the initiator, which needs
//! the target to hold an AH for the initiator's GID *before* the op is
//! posted — even on the end that only ever receives, never sends. Both
//! directions therefore build and retain an AH for every peer they talk to.
//!
//! Mirrors `spike/efa/src/rdma.rs::address_handle_for`, cached instead of
//! built fresh per exchange (the spike was a single-shot process; the daemon
//! talks to the same peer set repeatedly for its whole lifetime).

use std::collections::HashMap;

use anyhow::{anyhow, Context, Result};
use ibverbs::{AddressHandle, AddressHandleAttribute, ProtectionDomain, QueuePairEndpoint};
use tokio::sync::Mutex;
use tracing::info;

/// GID table index to route from (matches [`super::context`]'s own selection
/// — both ends of an AH use the same local index).
const GID_INDEX: u8 = 0;
/// EFA exposes exactly one port per device.
const PORT_NUM: u8 = 1;
/// IP hop limit for the GRH (matches [`super::context::EfaContext::bring_up`]'s
/// choice — same-AZ peers only, per the spike's cross-AZ finding).
const HOP_LIMIT: u8 = 64;
/// GRH traffic class: no differentiated-services marking needed on an
/// intra-cluster fabric.
const TRAFFIC_CLASS: u8 = 0;

/// One cached peer: the address handle plus the endpoint it was built from.
/// The full `endpoint` is retained (not just `qp_num`) so a re-handshake can
/// detect whether the peer's addressing changed and the AH must be rebuilt.
struct CachedPeer {
    ah: AddressHandle,
    endpoint: QueuePairEndpoint,
}

/// Address handles this node has built, keyed by peer `node_id` (the same
/// identity carried on `HandshakeRequest`/`FetchBlobRequest.requester_node_id`).
///
/// Insert-once *while the endpoint is unchanged* — an AH is cheap and valid
/// for the endpoint's lifetime (ADR-0018: "AH insert is cheap and
/// per-peer-once"). But a peer's endpoint is NOT stable across its process
/// lifetime: a pod restart (same K8s node name → same `node_id`) brings up a
/// fresh SRD QP with a new `qp_num`, and the stale AH would then address a
/// dead QP — every WRITE to it failing until the entry is refreshed. So the
/// cache is keyed by `node_id` but **rebuilds the AH when the endpoint
/// differs from the cached one** (hardware finding, A1 2026-07-19: without
/// this, RDMA silently stops engaging with any peer after it restarts).
pub struct AhCache {
    handles: Mutex<HashMap<String, CachedPeer>>,
}

impl AhCache {
    /// An empty cache, populated lazily as peers are first addressed or first
    /// handshake with us.
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(HashMap::new()),
        }
    }

    /// Return the cached AH for `peer_node_id`, (re)building one from
    /// `endpoint` if this is the first contact OR the peer's endpoint has
    /// changed since the cached AH was built (a restart with a new `qp_num`
    /// or GID — see the type doc).
    ///
    /// # Errors
    ///
    /// [`ibv_create_ah`](ibverbs::ProtectionDomain::create_address_handle)
    /// failing — a malformed/zero GID, a bad `sgid_index`, or (per the
    /// spike's finding 9) the peer being unreachable at the EFA fabric level
    /// (different AZ, no cluster placement group).
    pub async fn get_or_insert(
        &self,
        peer_node_id: &str,
        endpoint: &QueuePairEndpoint,
        pd: &ProtectionDomain,
    ) -> Result<AhRef<'_>> {
        let mut handles = self.handles.lock().await;
        let stale = handles
            .get(peer_node_id)
            .is_none_or(|c| !endpoints_match(&c.endpoint, endpoint));
        if stale {
            let ah = build(pd, endpoint)
                .with_context(|| format!("building AH for peer {peer_node_id}"))?;
            let replaced = handles
                .insert(
                    peer_node_id.to_owned(),
                    CachedPeer {
                        ah,
                        endpoint: *endpoint,
                    },
                )
                .is_some();
            info!(
                peer = peer_node_id,
                qp_num = endpoint.qp_num,
                refreshed = replaced,
                "AH inserted"
            );
        }
        Ok(AhRef {
            handles,
            peer_node_id: peer_node_id.to_owned(),
        })
    }
}

/// Two SRD endpoints address the same remote QP iff both `qp_num` and GID
/// match (LID is unused on EFA's GID-routed path).
fn endpoints_match(a: &QueuePairEndpoint, b: &QueuePairEndpoint) -> bool {
    a.qp_num == b.qp_num && a.gid == b.gid
}

impl Default for AhCache {
    fn default() -> Self {
        Self::new()
    }
}

impl AhCache {
    /// Whether an AH is already cached for `peer_node_id` — the capability
    /// signal both [`super::EfaRdmaTransport`] (requester) and the holder's
    /// serve path use to decide whether to attempt RDMA at all: an AH is
    /// only ever inserted from a handshake that carried an `efa_endpoint`
    /// (ADR-0019 bidirectional exchange), so its presence means "this peer
    /// negotiated RDMA with us" with no separate capability map to keep in
    /// sync.
    pub async fn has(&self, peer_node_id: &str) -> bool {
        self.handles.lock().await.contains_key(peer_node_id)
    }

    /// The endpoint the handshake negotiated for `peer_node_id` on this rail, or `None` if
    /// this rail has never learned it.
    ///
    /// Read-only and **never builds a handle**, which is what makes it usable from a
    /// control-plane query: the pre-flight exchange (`pacer_daemon::preflight`) has to answer
    /// "what is this holder's address on rail *i*?" without the side effect
    /// [`Self::get_or_insert`] has. Its presence carries the same meaning [`Self::has`] does
    /// — an entry only ever arrives from a handshake that carried an `efa_endpoint` (ADR-0019
    /// bidirectional exchange) — so `Some` is also the statement "this peer negotiated RDMA
    /// with us on this rail".
    ///
    /// Returned by value rather than behind a guard: a [`QueuePairEndpoint`] is `Copy`, and a
    /// caller that wanted to hold this across an `.await` while assembling a document for
    /// several peers would otherwise serialise every delivery in the process behind this
    /// mutex.
    pub async fn endpoint_of(&self, peer_node_id: &str) -> Option<QueuePairEndpoint> {
        self.handles
            .lock()
            .await
            .get(peer_node_id)
            .map(|cached| cached.endpoint)
    }

    /// Drop the cached AH for `peer_node_id` (A2). Returns whether an entry
    /// was actually present (for the caller's log). Called on a WRITE
    /// *completion error* — the surest live signal that the peer's endpoint
    /// went stale (a pod restart gave it a new SRD `qp_num`, so the cached AH
    /// now addresses a dead QP and every WRITE to it fails). After eviction
    /// [`Self::has`]/[`Self::get_or_insert_cached_only`] report the peer as
    /// un-negotiated, so subsequent fetches skip RDMA and fall back to gRPC
    /// cleanly instead of re-attempting a doomed WRITE for up to the 30 s
    /// handshake sweep. A later handshake (the sweep, or a proactive
    /// re-handshake — see [`super::EfaRdmaTransport`]) rebuilds a fresh AH from
    /// the peer's new endpoint via [`Self::get_or_insert`].
    ///
    /// Safe to call while another task has a WRITE in flight that was posted
    /// against this AH: removing the entry drops the [`AddressHandle`] (its
    /// `ibv_destroy_ah` on `Drop`), but the in-flight WRITE already captured
    /// its addressing into the WQE at `ibv_post_send` time and does not
    /// dereference the AH again — see [`super::EfaRdmaTransport::serve_via_write`]'s
    /// invariant doc. Acquiring the lock here also cannot race the synchronous
    /// post itself: that runs while the poster holds an [`AhRef`] guard on this
    /// same mutex.
    pub async fn evict(&self, peer_node_id: &str) -> bool {
        self.handles.lock().await.remove(peer_node_id).is_some()
    }

    /// Look up an already-cached AH without attempting to build one — the
    /// holder's serve path calls this (never [`Self::get_or_insert`]):
    /// `FetchBlobRequest` carries no full `QueuePairEndpoint`, only the
    /// requester's node id, so there is nothing to build from if the
    /// handshake never taught us this peer.
    ///
    /// # Errors
    ///
    /// If `peer_node_id` was never inserted — the caller's cue to fall back
    /// to streaming rather than attempt a WRITE with no return-path AH
    /// (ADR-0018 finding 6).
    pub async fn get_or_insert_cached_only(&self, peer_node_id: &str) -> Result<AhRef<'_>> {
        let handles = self.handles.lock().await;
        if !handles.contains_key(peer_node_id) {
            return Err(anyhow!("no cached AH for peer {peer_node_id}"));
        }
        Ok(AhRef {
            handles,
            peer_node_id: peer_node_id.to_owned(),
        })
    }
}

/// A guard holding the cache's lock, exposing the just-verified-present
/// entry. Short-lived: callers read `qp_num`/pass the handle to a post and
/// drop this immediately, never across an `.await` that isn't the post
/// itself (posts are synchronous, see `EfaContext::post`).
pub struct AhRef<'a> {
    handles: tokio::sync::MutexGuard<'a, HashMap<String, CachedPeer>>,
    peer_node_id: String,
}

impl AhRef<'_> {
    /// The address handle and destination `qp_num` for this peer — the two
    /// pieces `SendBatch::to(ah, qp_num, qkey)` needs to address a send.
    ///
    /// # Panics
    ///
    /// Never in practice: an `AhRef` is only ever constructed after the entry is
    /// present, and it holds the cache's lock for its whole lifetime, so nothing
    /// can evict the entry out from under it. A panic here would mean that
    /// construction invariant was broken, not that a peer went away.
    pub fn handle_and_qp_num(&self) -> (&AddressHandle, u32) {
        // `get_or_insert` guarantees presence before constructing `AhRef`.
        let peer = self
            .handles
            .get(&self.peer_node_id)
            .expect("AhRef invariant: entry inserted before construction");
        (&peer.ah, peer.endpoint.qp_num)
    }
}

/// Build one address handle for `peer`'s SRD endpoint (GID-routed, per the
/// `efa_srd` reference and the spike's hardware-validated sequence).
fn build(pd: &ProtectionDomain, peer: &QueuePairEndpoint) -> Result<AddressHandle> {
    let gid = peer
        .gid
        .ok_or_else(|| anyhow!("peer endpoint carries no GID (EFA requires one)"))?;
    let mut attr = AddressHandleAttribute::new(PORT_NUM);
    attr.set_grh(gid, GID_INDEX, HOP_LIMIT, TRAFFIC_CLASS);
    pd.create_address_handle(&attr)
        .context("creating address handle for peer")
}
