//! gRPC peer transport (tonic over TCP; ENA Express does SRD under it with
//! zero code — ADR-0008).
//!
//! Channels are cached per peer address: tonic channels multiplex over one
//! HTTP/2 connection, and a DaemonSet peer set is small and stable. A cached
//! entry is dropped on any RPC transport error and re-dialed on next use, so
//! pod restarts (same node, new IP) heal via the membership epoch that
//! replaces the NodeId's addr.
//!
//! One peer's entry holds a *pool* of such channels, [`crate::DEFAULT_PEER_CONNECTIONS`]
//! wide by default (= the single cached channel this has always been), and calls are handed
//! them round-robin. Wider spreads a fan-out over that many TCP flows and that many HTTP/2
//! framing tasks, which is what the save path's `StoreChunk` leg appears to be capped by —
//! see [`crate::DEFAULT_PEER_CONNECTIONS`] for the measurement and the three ceilings it
//! cannot tell apart.

use std::collections::HashMap;
use std::sync::Arc;

use futures::StreamExt;
use pacer_proto::v1::{
    peer_client::PeerClient, AnnounceRequest, BlobChunk, CommitUploadRequest, DiscardUploadRequest,
    FetchBlobRequest, HandshakeRequest, InvalidateRequest, LookupSharersRequest, RdmaCapabilities,
    RefusalReason, StoreChunkRequest, StoreChunkResponse, Tier as WireTier,
};
use pacer_ring::directory::{Holder, SharerSet, Tier};
use pacer_ring::NodeId;
use tokio::sync::Mutex;
use tonic::transport::Channel;
use tracing::debug;

use crate::{
    BlobStream, ByteRange, PeerTransport, StoreOffer, StoreOutcome, StoreRefusal, TransportError,
};

/// [`Tier`] → wire enum for an `Announce` admit.
fn tier_to_wire(tier: Tier) -> WireTier {
    match tier {
        Tier::Dram => WireTier::Dram,
        Tier::Nvme => WireTier::Nvme,
    }
}

/// Wire enum → [`Tier`], defaulting unspecified/unknown to `Nvme` (the
/// conservative choice: it never makes a hint *look* faster than it is —
/// worst case a reader skips a DRAM holder it could have preferred).
fn tier_from_wire(tier: i32) -> Tier {
    match WireTier::try_from(tier) {
        Ok(WireTier::Dram) => Tier::Dram,
        _ => Tier::Nvme,
    }
}

/// Protocol version sent in handshakes. Bump on wire-visible changes.
pub const PROTOCOL_VERSION: u32 = 1;

/// Render an error plus its `source()` chain as `outer: cause: root`. tonic's
/// connect/transport errors Display as a bare "transport error"; the real
/// diagnosis lives one or more `source()` hops down.
fn source_chain(err: &dyn std::error::Error) -> String {
    let mut out = err.to_string();
    let mut src = err.source();
    while let Some(e) = src {
        out.push_str(": ");
        out.push_str(&e.to_string());
        src = e.source();
    }
    out
}

/// The connections held for one peer, and the cursor that spreads calls over them.
///
/// A `Vec` rather than one channel because a single HTTP/2 connection is a single TCP flow
/// and a single framing task ([`crate::DEFAULT_PEER_CONNECTIONS`]). The cursor lives here,
/// beside the channels it indexes, and is advanced under the map's own lock — the alternative
/// (an atomic) would buy nothing, since reaching it already means holding that lock.
struct PeerPool {
    /// Established channels, never empty: a pool is only inserted once every dial succeeded.
    clients: Vec<PeerClient<Channel>>,
    /// Calls handed out so far. Wraps; only its residue matters.
    handed_out: usize,
}

impl PeerPool {
    /// The next channel, round-robin.
    ///
    /// Round-robin rather than least-loaded because the transport does not know when a call
    /// ends — a channel is cloned out and used by a task this type never sees again — and
    /// because the point is to spread N concurrent windows over N flows, which even
    /// distribution achieves without tracking anything.
    fn next_client(&mut self) -> PeerClient<Channel> {
        let client = self.clients[self.handed_out % self.clients.len()].clone();
        self.handed_out = self.handed_out.wrapping_add(1);
        client
    }
}

/// [`PeerTransport`] over tonic/HTTP2 with a per-peer pool of cached channels.
pub struct GrpcTransport {
    /// Port peers listen on for the Peer service (same on every node —
    /// DaemonSet symmetry).
    peer_port: u16,
    /// Identity announced in handshakes (this node's name).
    local_node: String,
    /// Flow-control override for channels this transport dials — see [`crate::H2Windows`].
    /// Governs what this node advertises for what it RECEIVES, i.e. `FetchBlob` bodies.
    h2_windows: crate::H2Windows,
    /// Connections opened per peer — see [`crate::DEFAULT_PEER_CONNECTIONS`].
    connections_per_peer: usize,
    channels: Arc<Mutex<HashMap<String, PeerPool>>>,
}

impl GrpcTransport {
    /// A transport dialing `peer_port` and announcing itself as `local_node`.
    pub fn new(peer_port: u16, local_node: impl Into<String>) -> Self {
        Self {
            peer_port,
            local_node: local_node.into(),
            h2_windows: crate::H2Windows::default(),
            connections_per_peer: crate::DEFAULT_PEER_CONNECTIONS,
            channels: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Override the HTTP/2 receive windows for channels dialed after this call.
    ///
    /// A builder rather than a fourth constructor argument: every one of this constructor's
    /// call sites outside `main` is a test that does not care, and flow control is exactly
    /// the kind of setting that should be absent from those.
    ///
    /// Call before anything dials — a channel already in the cache keeps the settings it was
    /// built with, since HTTP/2 windows are negotiated at connection setup.
    #[must_use]
    pub fn with_h2_windows(mut self, windows: crate::H2Windows) -> Self {
        self.h2_windows = windows;
        self
    }

    /// Override how many connections each peer gets — see [`crate::DEFAULT_PEER_CONNECTIONS`].
    ///
    /// Clamped into `1..=`[`crate::MAX_PEER_CONNECTIONS`] rather than trusted: a zero here
    /// would panic on the first modulo, and the daemon's own configuration layer already
    /// refuses an out-of-range value with a message naming the knob. This is the second line
    /// of that defence, for the call sites that bypass configuration entirely (tests).
    ///
    /// Call before anything dials — a peer already in the cache keeps the pool it was built
    /// with, the same way [`Self::with_h2_windows`] cannot change a live connection's windows.
    #[must_use]
    pub fn with_connections_per_peer(mut self, connections: usize) -> Self {
        self.connections_per_peer = connections.clamp(1, crate::MAX_PEER_CONNECTIONS);
        self
    }

    /// Dial (or reuse) this peer's channel. `pub(crate)` so
    /// [`crate::efa::EfaRdmaTransport`] can share the same cached-channel
    /// dial path instead of duplicating it (it issues its own `FetchBlob`
    /// call, carrying `rdma_buffer`, before falling back to this transport's
    /// plain streaming path on any error).
    pub(crate) async fn client(
        &self,
        peer: &NodeId,
    ) -> Result<PeerClient<Channel>, TransportError> {
        let mut channels = self.channels.lock().await;
        if let Some(pool) = channels.get_mut(peer.addr()) {
            return Ok(pool.next_client());
        }
        // A bare IP dials the symmetric DaemonSet port; an addr that already
        // parses as ip:port wins (in-process tests, heterogeneous setups).
        let endpoint = if peer.addr().parse::<std::net::SocketAddr>().is_ok() {
            format!("http://{}", peer.addr())
        } else {
            format!("http://{}:{}", peer.addr(), self.peer_port)
        };
        let mut pool = PeerPool {
            clients: self.dial_pool(&endpoint).await?,
            handed_out: 0,
        };
        let client = pool.next_client();
        channels.insert(peer.addr().to_owned(), pool);
        Ok(client)
    }

    /// Open this peer's whole pool, or none of it.
    ///
    /// Dialed concurrently and joined: a pool of N built serially would make first contact
    /// N handshakes deep, and a `StoreChunk` fan-out reaches a cold peer from many tasks at
    /// once (the first holds the map's lock; the rest are already queued behind it).
    ///
    /// All-or-nothing on purpose. A partial pool would work — and would then quietly measure a
    /// narrower arm than the one that was configured, which is exactly the class of confound
    /// this knob exists to resolve. `try_join_all` returns the first dial error, and the peer
    /// stays absent from the cache, so the next call retries the whole pool.
    ///
    /// # Errors
    ///
    /// [`TransportError::PeerUnavailable`] from the first connection that could not be dialed.
    async fn dial_pool(&self, endpoint: &str) -> Result<Vec<PeerClient<Channel>>, TransportError> {
        let channels = futures::future::try_join_all(
            (0..self.connections_per_peer).map(|_| self.dial(endpoint)),
        )
        .await?;
        Ok(channels
            .into_iter()
            .map(|channel| {
                PeerClient::new(channel)
                    // A StoreChunk carries a whole chunk_size window, which is far past
                    // tonic's 4 MiB default in both directions (ADR-0032 § 2).
                    .max_encoding_message_size(crate::MAX_PEER_MESSAGE_BYTES)
                    .max_decoding_message_size(crate::MAX_PEER_MESSAGE_BYTES)
            })
            .collect())
    }

    /// Connect to `endpoint`, applying this transport's flow-control settings.
    ///
    /// Built through [`tonic::transport::Endpoint`] rather than `PeerClient::connect` because
    /// that convenience takes hyper's defaults and offers no seam for the HTTP/2 windows —
    /// see [`crate::H2Windows`] for what those defaults are and why they are worth naming.
    /// With no override set this is `connect`'s own behaviour, reached the long way.
    async fn dial(&self, endpoint: &str) -> Result<Channel, TransportError> {
        let mut builder =
            tonic::transport::Endpoint::from_shared(endpoint.to_owned()).map_err(|e| {
                TransportError::PeerUnavailable(format!("bad peer URI {endpoint}: {e}"))
            })?;
        if let Some(bytes) = self.h2_windows.stream_bytes {
            builder = builder.initial_stream_window_size(bytes);
        }
        if let Some(bytes) = self.h2_windows.connection_bytes {
            builder = builder.initial_connection_window_size(bytes);
        }
        builder.connect().await.map_err(|e| {
            // tonic's Display is famously just "transport error" — walk the
            // source chain so the real cause (DNS, connection refused, h2
            // handshake, TLS) is visible instead of swallowed.
            TransportError::PeerUnavailable(format!("dialing {endpoint}: {}", source_chain(&e)))
        })
    }

    /// Forget this peer's pool after a transport failure, so the next call re-dials it.
    ///
    /// The **whole** pool, on one connection's error. A pod restart or a node replacement
    /// invalidates every connection to that address at once, which is what these failures
    /// nearly always are; and keeping the survivors would make the pool silently narrow over a
    /// run, so an arm's width would depend on its error history. Re-dialling N connections
    /// costs one handshake each, once.
    async fn evict(&self, peer: &NodeId) {
        self.channels.lock().await.remove(peer.addr());
    }

    /// One-time capability handshake with a peer. `capabilities`/`efa_endpoint`
    /// are this node's own — `None`/default on every Phase 2 (non-EFA) daemon;
    /// `EfaRdmaTransport` (crate::efa) passes its real ones so both ends can
    /// AH-insert each other before either issues a WRITE (ADR-0018 finding 6).
    ///
    /// # Errors
    ///
    /// [`TransportError::PeerUnavailable`] when the peer cannot be dialed or
    /// rejects the RPC.
    pub async fn handshake(
        &self,
        peer: &NodeId,
        capabilities: RdmaCapabilities,
        efa_endpoint: Option<pacer_proto::v1::EfaEndpoint>,
    ) -> Result<pacer_proto::v1::HandshakeResponse, TransportError> {
        let mut client = self.client(peer).await?;
        let resp = client
            .handshake(HandshakeRequest {
                node_id: self.local_node.clone(),
                capabilities: Some(capabilities),
                protocol_version: PROTOCOL_VERSION,
                efa_endpoint,
            })
            .await
            .map_err(|s| {
                TransportError::PeerUnavailable(format!(
                    "handshake RPC: {} (source: {})",
                    s,
                    source_chain(&s)
                ))
            })?;
        let resp = resp.into_inner();
        debug!(peer = %peer.name(), version = resp.protocol_version, "peer handshake");
        Ok(resp)
    }
}

/// `(range_start, range_end, suffix_len)` triple `FetchBlobRequest` carries,
/// from the trait's [`ByteRange`]. Shared by [`GrpcTransport::fetch_blob`]
/// and `EfaRdmaTransport::fetch_blob` (crate::efa), which builds its own
/// request (carrying `rdma_buffer`/`requester_node_id` too) from the same
/// range encoding.
pub(crate) fn range_fields(range: Option<ByteRange>) -> (Option<u64>, Option<u64>, Option<u64>) {
    match range {
        None => (None, None, None),
        Some(ByteRange::From { start, end }) => (Some(start), end, None),
        Some(ByteRange::Suffix { len }) => (None, None, Some(len)),
    }
}

/// Dial `peer` and issue `request`, mapping a call failure to the right
/// [`TransportError`] variant (and evicting the cached channel on a
/// transport-level failure, so the next call re-dials instead of reusing a
/// corpse). Shared by [`GrpcTransport::fetch_blob`] and the EFA transport,
/// which posts its own request shape but wants identical error handling.
pub(crate) async fn call_fetch_blob(
    grpc: &GrpcTransport,
    peer: &NodeId,
    request: FetchBlobRequest,
) -> Result<tonic::Streaming<BlobChunk>, TransportError> {
    let mut client = grpc.client(peer).await?;
    match client.fetch_blob(request).await {
        Ok(resp) => Ok(resp.into_inner()),
        Err(status) => Err(match status.code() {
            tonic::Code::NotFound => TransportError::NotCached,
            tonic::Code::OutOfRange => TransportError::RangeNotSatisfiable,
            _ => {
                grpc.evict(peer).await;
                TransportError::PeerUnavailable(status.to_string())
            }
        }),
    }
}

/// Read the first message of a `FetchBlob` response stream and split it into
/// its `BlobMeta` and the (possibly-empty; see
/// [`pacer_proto::v1::BlobChunk::data`]) leading data. Every response starts
/// this way whether the body then continues as `BlobChunk`s or arrived by
/// RDMA WRITE, so callers branch on `meta.served_via_rdma` after this and
/// before deciding how to assemble the rest.
///
/// # Errors
///
/// The stream ending with no message, that message carrying no `meta`, or a
/// transport error on the first read.
pub(crate) async fn read_first_chunk(
    stream: &mut tonic::Streaming<BlobChunk>,
) -> Result<(pacer_proto::v1::BlobMeta, bytes::Bytes), TransportError> {
    let first = stream
        .next()
        .await
        .ok_or_else(|| TransportError::PeerUnavailable("empty blob stream".into()))?
        .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?;
    let meta = first
        .meta
        .ok_or_else(|| TransportError::PeerUnavailable("missing blob meta".into()))?;
    Ok((meta, first.data))
}

/// Decode a `FetchBlob` response stream into a [`BlobStream`] entirely from
/// `BlobChunk.data` (the gRPC streaming path — always what
/// [`GrpcTransport::fetch_blob`] does, and what the EFA transport falls back
/// to when `BlobMeta.served_via_rdma` comes back false).
///
/// # Errors
///
/// See [`read_first_chunk`]; propagates any transport error mid-stream too.
pub(crate) async fn decode_blob_stream(
    mut stream: tonic::Streaming<BlobChunk>,
) -> Result<BlobStream, TransportError> {
    let (meta, first_data) = read_first_chunk(&mut stream).await?;
    let head = futures::stream::iter([Ok(first_data)]);
    let tail = stream.map(|chunk| match chunk {
        Ok(c) => Ok(c.data),
        Err(s) => Err(TransportError::PeerUnavailable(s.to_string())),
    });
    Ok(BlobStream {
        len: meta.total_len,
        object_len: meta.object_len,
        body_start: meta.body_start,
        e_tag: meta.e_tag,
        content_type: meta.content_type,
        last_modified_epoch_secs: meta.last_modified_epoch_secs,
        chunks: Box::pin(head.chain(tail)),
    })
}

#[async_trait::async_trait]
impl PeerTransport for GrpcTransport {
    async fn fetch_blob(
        &self,
        peer: &NodeId,
        cache_key: &str,
        range: Option<ByteRange>,
        no_fill: bool,
    ) -> Result<BlobStream, TransportError> {
        let (range_start, range_end, suffix_len) = range_fields(range);
        let stream = call_fetch_blob(
            self,
            peer,
            FetchBlobRequest {
                cache_key: cache_key.to_owned(),
                range_start,
                range_end,
                suffix_len,
                no_fill,
                rdma_buffer: None,
                requester_node_id: None,
                // Both RDMA destinations are absent on the gRPC-only path by definition:
                // this transport has no plane to write with, and offering a client's window
                // to a holder that would write it is `efa::target`'s job.
                client_token: None,
            },
        )
        .await?;
        decode_blob_stream(stream).await
    }

    async fn invalidate(&self, peer: &NodeId, cache_key: &str) -> Result<(), TransportError> {
        let mut client = self.client(peer).await?;
        client
            .invalidate(InvalidateRequest {
                cache_key: cache_key.to_owned(),
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?;
        Ok(())
    }

    async fn announce_admit(
        &self,
        home: &NodeId,
        chunk_key: &str,
        node: &str,
        tier: Tier,
        generation: u64,
    ) -> Result<(), TransportError> {
        let mut client = self.client(home).await?;
        client
            .announce(AnnounceRequest {
                chunk_key: chunk_key.to_owned(),
                node: node.to_owned(),
                tier: Some(tier_to_wire(tier).into()),
                generation,
                evict: false,
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?;
        Ok(())
    }

    async fn announce_evict(
        &self,
        home: &NodeId,
        chunk_key: &str,
        node: &str,
        generation: u64,
    ) -> Result<(), TransportError> {
        let mut client = self.client(home).await?;
        client
            .announce(AnnounceRequest {
                chunk_key: chunk_key.to_owned(),
                node: node.to_owned(),
                tier: None,
                generation,
                evict: true,
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?;
        Ok(())
    }

    async fn lookup_sharers(
        &self,
        home: &NodeId,
        chunk_key: &str,
    ) -> Result<Option<SharerSet>, TransportError> {
        let mut client = self.client(home).await?;
        let resp = client
            .lookup_sharers(LookupSharersRequest {
                chunk_key: chunk_key.to_owned(),
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?
            .into_inner();
        if resp.sharers.is_empty() && !resp.widely_held {
            return Ok(None);
        }
        Ok(Some(SharerSet {
            holders: resp
                .sharers
                .into_iter()
                .map(|s| Holder {
                    node: s.node,
                    tier: tier_from_wire(s.tier),
                    generation: s.generation,
                })
                .collect(),
            widely_held: resp.widely_held,
        }))
    }

    async fn store_chunk(
        &self,
        owner: &NodeId,
        offer: StoreOffer<'_>,
    ) -> Result<StoreOutcome, TransportError> {
        let mut client = self.client(owner).await?;
        let resp = client
            .store_chunk(StoreChunkRequest {
                chunk_key: offer.chunk_key.to_owned(),
                upload_id: offer.upload_id.to_owned(),
                bucket: offer.bucket.to_owned(),
                key: offer.key.to_owned(),
                part_number: offer.part_number,
                data: offer.body.to_vec(),
                checksum_crc32: offer.checksum_crc32.to_owned(),
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?
            .into_inner();
        store_outcome_from_wire(resp)
    }

    async fn commit_upload(
        &self,
        owner: &NodeId,
        upload_id: &str,
        e_tag: &str,
    ) -> Result<u32, TransportError> {
        let mut client = self.client(owner).await?;
        let resp = client
            .commit_upload(CommitUploadRequest {
                upload_id: upload_id.to_owned(),
                e_tag: e_tag.to_owned(),
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?
            .into_inner();
        Ok(resp.committed)
    }

    async fn discard_upload(&self, owner: &NodeId, upload_id: &str) -> Result<u32, TransportError> {
        let mut client = self.client(owner).await?;
        let resp = client
            .discard_upload(DiscardUploadRequest {
                upload_id: upload_id.to_owned(),
            })
            .await
            .map_err(|s| TransportError::PeerUnavailable(s.to_string()))?
            .into_inner();
        Ok(resp.discarded)
    }
}

/// Read an owner's answer to a [`StoreOffer`].
///
/// A response with neither field set is a protocol violation rather than a
/// refusal, and is surfaced as an error so it lands in the transport-failure
/// metric instead of being silently counted as a busy peer — the two have very
/// different meanings for whether the scatter is working.
fn store_outcome_from_wire(resp: StoreChunkResponse) -> Result<StoreOutcome, TransportError> {
    if let Some(e_tag) = resp.e_tag {
        return Ok(StoreOutcome::Uploaded { e_tag });
    }
    let Some(refusal) = resp.refusal else {
        return Err(TransportError::Other(anyhow::anyhow!(
            "StoreChunk response carried neither an ETag nor a refusal"
        )));
    };
    let (staged, budget) = (refusal.staged_bytes, refusal.budget_bytes);
    Ok(StoreOutcome::Refused(
        match RefusalReason::try_from(refusal.reason) {
            Ok(RefusalReason::BudgetExhausted) => StoreRefusal::BudgetExhausted { staged, budget },
            Ok(RefusalReason::RacingUpload) => StoreRefusal::RacingUpload,
            Ok(RefusalReason::OversizedForBudget) => StoreRefusal::OversizedForBudget {
                // The owner reports its own view of the window's length in
                // `staged_bytes` for this reason, since nothing is staged.
                chunk_len: staged,
                budget,
            },
            Ok(RefusalReason::NotAccepting) => StoreRefusal::NotAccepting,
            // An unspecified or unrecognised reason is treated as permanent:
            // guessing "transient" would put the coordinator in a cooldown loop
            // against a peer that will never accept.
            Ok(RefusalReason::Unspecified) | Err(_) => StoreRefusal::Unknown,
        },
    ))
}

#[cfg(test)]
mod tests {
    //! `range_fields` is the one piece of `EfaRdmaTransport`'s request-
    //! building logic (crate::efa) that has no `ibverbs` dependency, so it's
    //! tested here rather than needing the finch/hardware environment the
    //! `efa` feature otherwise requires.
    use super::{range_fields, GrpcTransport, PeerPool};
    use crate::ByteRange;

    /// A pool wide enough that a wrap is visible and an off-by-one in the residue is not
    /// hidden by symmetry.
    const POOL_WIDTH: usize = 3;

    /// Lazy channels: [`PeerPool`]'s contract is the cursor, and `connect_lazy` gives real
    /// `PeerClient`s to index without a server to dial. It still builds a hyper connector, so
    /// its callers are `#[tokio::test]` — a plain `#[test]` panics on the absent reactor.
    fn lazy_pool(width: usize) -> PeerPool {
        let clients = (0..width)
            .map(|_| {
                pacer_proto::v1::peer_client::PeerClient::new(
                    tonic::transport::Endpoint::from_static("http://127.0.0.1:1").connect_lazy(),
                )
            })
            .collect();
        PeerPool {
            clients,
            handed_out: 0,
        }
    }

    #[test]
    fn none_range_is_all_none() {
        assert_eq!(range_fields(None), (None, None, None));
    }

    /// The cursor must walk the pool and wrap. A pool that always handed out index 0 would
    /// open N connections, spread nothing over them, and measure the one-flow arm under
    /// another arm's name — a silent null rather than a failure.
    #[tokio::test]
    async fn the_cursor_walks_every_connection_then_wraps() {
        let mut pool = lazy_pool(POOL_WIDTH);
        let mut handed = Vec::new();
        for _ in 0..POOL_WIDTH * 2 {
            pool.next_client();
            handed.push(pool.handed_out);
        }
        assert_eq!(handed, vec![1, 2, 3, 4, 5, 6]);
        assert_eq!(
            pool.handed_out % POOL_WIDTH,
            0,
            "six calls over three is two laps"
        );
    }

    /// A pool of one is what a single cached channel always was, so the default arm still
    /// hands the same channel back every time.
    #[tokio::test]
    async fn a_pool_of_one_is_the_channel_cache_it_replaced() {
        let mut pool = lazy_pool(1);
        for _ in 0..4 {
            pool.next_client();
        }
        assert_eq!(pool.clients.len(), 1);
    }

    /// Zero would panic on the first modulo and a huge value would open thousands of sockets,
    /// so the builder clamps instead of trusting its caller (configuration refuses first, with
    /// a message; this is the floor under the call sites that never go through it).
    #[test]
    fn the_builder_clamps_a_pool_width_it_cannot_honour() {
        let clamped = GrpcTransport::new(0, "probe").with_connections_per_peer(0);
        assert_eq!(clamped.connections_per_peer, 1);
        let capped = GrpcTransport::new(0, "probe")
            .with_connections_per_peer(crate::MAX_PEER_CONNECTIONS * 4);
        assert_eq!(capped.connections_per_peer, crate::MAX_PEER_CONNECTIONS);
    }

    #[test]
    fn from_range_maps_start_and_end() {
        assert_eq!(
            range_fields(Some(ByteRange::From {
                start: 10,
                end: Some(20)
            })),
            (Some(10), Some(20), None)
        );
    }

    #[test]
    fn from_range_open_end_maps_end_none() {
        assert_eq!(
            range_fields(Some(ByteRange::From {
                start: 10,
                end: None
            })),
            (Some(10), None, None)
        );
    }

    #[test]
    fn suffix_range_maps_len_only() {
        assert_eq!(
            range_fields(Some(ByteRange::Suffix { len: 5 })),
            (None, None, Some(5))
        );
    }
}
