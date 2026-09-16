//! Server side of the peer protocol (pacer.v1.Peer): capability handshake and
//! FetchBlob — the data path peers use to read this node's cache.
//!
//! The cache unit is a chunk (ADR-0015): a `FetchBlobRequest.cache_key` is a
//! chunk key `"{bucket}/{key}#{size}:{index}"`, and the handler is
//! key-agnostic — a hit returns the whole chunk's bytes (chunks are the unit,
//! so no server-side range slicing).
//!
//! Fill discipline (ADR-0012, per chunk): a miss for a chunk key this node
//! OWNS is read through to the backend with a ranged GET of exactly that
//! chunk's bounds and filled here (the owner is the single cluster-wide copy);
//! the bytes stream to the requester while the fill accumulates. A `no_fill`
//! read (or a chunk this node does not own) returns NOT_FOUND — the requester
//! falls back to a direct backend GET.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use pacer_cache::chunk::{CachedChunk, ChunkConfig};
use pacer_cache::tier::ChunkTier;
use pacer_cache::{object_key_parts, Promotion};
use pacer_proto::v1::peer_server::{Peer, PeerServer};
use pacer_proto::v1::{
    AnnounceRequest, AnnounceResponse, BlobChunk, BlobMeta, CommitUploadRequest,
    CommitUploadResponse, DiscardUploadRequest, DiscardUploadResponse, FetchBlobRequest,
    HandshakeRequest, HandshakeResponse, InvalidateRequest, InvalidateResponse,
    LookupSharersRequest, LookupSharersResponse, RdmaCapabilities, RefusalReason, Sharer,
    StoreChunkRequest, StoreChunkResponse, StoreRefusal as WireRefusal, Tier as WireTier,
};
use pacer_ring::directory::{SharedDirectory, Tier};
use pacer_ring::SharedRing;
use pacer_transport::StoreRefusal;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
use tracing::{debug, warn};

use crate::cachefill::ChunkFill;
use crate::metrics::Metrics;
use crate::proxy::FillGuard;
use crate::staging::{StageOutcome, StagingArea};

/// gRPC message payload per BlobChunk frame. Well under tonic's 4 MiB default
/// decode limit; slicing a cached `Bytes` is refcount-only, no copies. (A cache
/// chunk is much larger — 16 MiB default — so it spans several frames.)
const FRAME_SIZE: usize = 1 << 20;

/// Max concurrent holder serves — in-flight chunk-body materializations on the
/// FetchBlob path. Acquired BEFORE the cache read so no 16 MiB body is pulled
/// from NVMe that cannot be promptly staged, bounding resident serve memory to
/// `HOLDER_SERVE_SLOTS × chunk_size` (256 × 16 MiB = 4 GiB at the defaults).
///
/// Pinned to the EFA holder arena depth
/// ([`pacer_transport::efa::HOLDER_ARENA_RANGES`]): every RDMA serve leases one
/// holder range to stage its WRITE, so admitting more serves than the arena can
/// stage merely piles unstaged bodies in memory.
///
/// Raised from 64 with ADR-0024 (planning/19 D2). 64 was the old fixed pool's
/// depth, justified by "64 concurrent already saturates a 40 Gbps rail" — a bar
/// planning/18 has since replaced with a measured ~58 GiB/s transport ceiling,
/// where 64 in-flight 16 MiB serves is *not* enough to keep the wire busy. The
/// bound it encodes still holds, only against a bigger arena: this is
/// backpressure to the requesters, not a throughput cap.
///
/// Without any such gate (planning/15 B4, 8-node run 2026-08-12): a single-holder
/// fan-in of 7 requesters × 256 requester slots = 1792 concurrent FetchBlobs each
/// materialized a 16 MiB body ahead of the holder-side gate — ~28 GiB at once —
/// and OOM-killed the holder past its 44 GiB cgroup limit, even though staging
/// itself was capped at 64. (`readthrough_limit` bounds only the cold-fill path;
/// this bounds the WARM serve path that fan-in hit.) The memory this admits —
/// `slots × chunk_size` — is therefore a real commitment: raising `chunk_size`
/// raises it proportionally, on top of the arena's own pinned bytes.
const HOLDER_SERVE_SLOTS: usize = 256;

/// The serve-admission bound is only correct if it never exceeds the holder arena
/// it tracks (else bodies pile up unstaged — the very OOM it prevents). Enforce
/// that at compile time wherever the RDMA holder arena actually exists. It stays
/// checkable because the arena's depth is a range COUNT, not a byte budget —
/// bytes would depend on the runtime `chunk_size` and this assert would silently
/// stop meaning anything (see `HOLDER_ARENA_RANGES`).
#[cfg(feature = "efa")]
const _: () = assert!(HOLDER_SERVE_SLOTS <= pacer_transport::efa::HOLDER_ARENA_RANGES);

/// What a holder's one-sided WRITE attempt did for one `FetchBlob` response.
///
/// Replaced a bare `bool` when ADR-0030's remote half landed: a WRITE into the reading
/// **client's** own window also owes a digest of what it wrote, because the requester never
/// sees those bytes and cannot read the window back (ADR-0030 point 7). Keeping the two
/// together makes it impossible to report `served_via_rdma` without the checksum that path
/// requires.
struct WriteServe {
    /// Whether the bytes landed by RDMA. `false` means the caller must stream the body —
    /// always legal, never a failed read (ADR-0003).
    served: bool,
    /// CRC32 of the bytes written, for `BlobMeta.written_crc32`. `Some` only on the
    /// client-token path and only when the token asked for one; every other path leaves the
    /// digest to whoever owns the destination.
    crc32: Option<u32>,
}

impl WriteServe {
    /// "Nothing landed by RDMA" — the answer for every fallback, of which there are many and
    /// none is an error.
    fn streamed() -> Self {
        Self {
            served: false,
            crc32: None,
        }
    }
}

/// Server side of the peer protocol for this node.
pub struct PacerPeer {
    tier: ChunkTier,
    /// Direct backend client for owner read-through (same identity as the
    /// proxy's, ADR-0006).
    backend: aws_sdk_s3::Client,
    /// Chunking (ADR-0015): key parsing + bounds for a read-through range GET.
    chunk: ChunkConfig,
    ring: SharedRing,
    /// This node's directory shard (ADR-0017): sharer sets for the chunk keys
    /// the ring homes here. Also holds this node's OWN admit/evict
    /// announcements when it is a holder of a key homed elsewhere — see
    /// `directory` module docs on the split between shard-serving and being
    /// a holder oneself; a daemon has exactly one `SharedDirectory` playing
    /// both roles depending on which chunk key is in question.
    directory: SharedDirectory,
    local_node: String,
    /// Replication factor R (ADR-0016 layer 2): this node reads through and
    /// fills a chunk when it is one of the key's top-R co-homes, not only the
    /// single rendezvous winner. Matches the proxy's `Cluster::replication_r`.
    replication_r: usize,
    /// Objects at or below this size are never cached (ADR-0002 small-object
    /// bypass) — applied on the owner too, from the read-through's object length.
    min_object_size: u64,
    /// Optional whole-object admission cap; `None` = unbounded (ADR-0015: an
    /// object of any size is chunk-cached). Applied on the owner too, from the
    /// read-through's object length.
    max_object_size: Option<u64>,
    /// Chunks buffered between a producer task and the gRPC response stream
    /// (ADR-0013 tunable; small — the stream itself is the backpressure).
    channel_capacity: usize,
    metrics: Metrics,
    /// Shared with the proxy: one concurrent fill per chunk key node-wide,
    /// regardless of whether a client GET or a peer fetch triggered it.
    filling: Arc<Mutex<HashSet<String>>>,
    /// Caps how many peer read-through fills run concurrently on this holder,
    /// bounding their combined memory to `fill_parallelism × chunk_size` — the
    /// same bound the proxy's client read path already enforces via
    /// `stream::buffered` (see `proxy.rs`). Without it, a cold-cache fan-in
    /// storm (many requesters × their in-flight-request depth all missing at
    /// once) spawns one unbounded read-through per miss; on-hardware, fan-in 7
    /// with a cold cache drove the holder past its cgroup memory limit and
    /// OOM-killed it, resetting its serve counters mid-run (planning/15 B4).
    /// `serve_cached` hits are bounded separately by [`Self::serve_limit`], so
    /// this only throttles the cold-fill storm, never the warm serve rate.
    readthrough_limit: Arc<Semaphore>,
    /// Caps how many warm serves materialize a chunk body at once
    /// (see [`HOLDER_SERVE_SLOTS`]). Acquired before the cache read in
    /// `fetch_blob`, held for the whole serve, so resident serve memory stays
    /// bounded regardless of how deep the aggregate requester fan-in is. This is
    /// the bound the holder-arena lease could NOT provide: the arena caps RDMA
    /// staging, but the 16 MiB body is already resident by the time a serve
    /// reaches it.
    serve_limit: Arc<Semaphore>,
    /// `None` on a non-EFA node or when the startup capability probe failed
    /// (ADR-0018) — every RDMA branch below treats that identically to "peer
    /// didn't ask for RDMA": fall through to streaming.
    #[cfg(feature = "efa")]
    efa: Option<Arc<pacer_transport::efa::EfaRdmaTransport>>,
    /// Where a read-through fill's bytes go (ADR-0028) — the same value the
    /// proxy holds. This path is why [`ChunkFill`] is shared rather than a proxy
    /// detail: a COLD holder fills here, so a slab the read-through ignored
    /// would leave every subsequent serve staging a copy (planning/19 § C1b).
    fill: ChunkFill,
    /// Whether a local disk hit is promoted into the RAM tier — the same value the
    /// proxy holds, for the same reason [`Self::fill`] is shared: a holder that
    /// promoted while the proxy did not would make one node's RAM tier behave two
    /// ways depending on which path read it.
    promotion: Promotion,
    /// Handle of the dedicated RDMA serve-path runtime (see the daemon's
    /// `main`). The holder-side `serve_via_write` (stage copy + WRITE post +
    /// completion await) is bridged onto it rather than run inline on the tonic
    /// handler's task, so client-role S3-proxy work on the main runtime cannot
    /// starve or delay holder serves (planning/15 all-hammer contention).
    #[cfg(feature = "efa")]
    rdma_runtime: tokio::runtime::Handle,
    /// Windows this node has uploaded as parts of some coordinator's multipart
    /// upload but not yet made visible (ADR-0032 § 3). `None` when this node does
    /// not accept scattered writes at all — the feature is off or the backend is
    /// not a general-purpose bucket (§ 6) — which is what makes
    /// [`RefusalReason::NotAccepting`] distinguishable from a load refusal.
    staging: Option<Arc<StagingArea>>,
}

/// Everything [`PacerPeer::new`] cannot default: the handles and policy values a
/// peer server has to be told, gathered into one value.
///
/// A parameter struct rather than a parameter list because the list had reached
/// **sixteen** and was only compiling behind `#[allow(clippy::too_many_arguments)]`.
/// The alternative — pushing the optional ones onto builder methods, as
/// [`PacerPeer::with_staging`] and [`PacerPeer::with_promotion`] already are — does not
/// apply here: none of these is optional. Every one of them is a value the node must
/// hold the *same* copy of as the proxy does, and a builder that may be skipped is
/// exactly how a holder ends up disagreeing with its own read path (the failure mode
/// [`crate::cachefill`] documents for the slab, and [`PacerPeer::with_promotion`] for
/// promotion). Naming them in a struct literal at the call site also means a
/// same-typed pair — `min_object_size` and `max_object_size`, `replication_r` and
/// `channel_capacity` — cannot be silently transposed, which a positional list of
/// sixteen invites.
///
/// Field docs here say where a value comes from and what breaks if it is wrong; the
/// deeper rationale lives on the matching [`PacerPeer`] field.
pub struct PeerParts {
    /// The node's chunk cache — the SAME tier the proxy reads, not a second handle:
    /// a serve must see what a client GET just filled.
    pub tier: ChunkTier,
    /// Direct backend client for owner read-through, carrying this node's identity
    /// (ADR-0006) rather than any requester's.
    pub backend: aws_sdk_s3::Client,
    /// Chunk geometry (ADR-0015). Must match the fleet's: it decides both how a
    /// `cache_key` parses and the bounds of a read-through range GET, so a node that
    /// disagrees serves the wrong bytes for a key it accepted.
    pub chunk: ChunkConfig,
    /// Cluster ring, for deciding whether this node OWNS a missing chunk key and may
    /// therefore fill it (ADR-0012/0016).
    pub ring: SharedRing,
    /// This node's directory shard (ADR-0017), also holding its own holdings.
    pub directory: SharedDirectory,
    /// This node's ring identity. The name peers announce to and the name the
    /// ownership test compares against, so a mismatch makes the node own nothing.
    pub local_node: String,
    /// Replication factor R (ADR-0016 layer 2). Must equal the proxy's
    /// `Cluster::replication_r`, or the two halves of one node disagree about which
    /// keys it may fill.
    pub replication_r: usize,
    /// ADR-0002's small-object bypass, applied on the owner too.
    pub min_object_size: u64,
    /// Optional whole-object admission cap; `None` is unbounded (ADR-0015).
    pub max_object_size: Option<u64>,
    /// Chunks buffered between the producer task and the gRPC response stream
    /// (ADR-0013).
    pub channel_capacity: usize,
    /// The node's one metrics registry, so a serve and a client GET count into the
    /// same series.
    pub metrics: Metrics,
    /// The node-wide in-flight-fill guard, shared with the proxy: one concurrent fill
    /// per chunk key whichever path triggered it.
    pub filling: Arc<Mutex<HashSet<String>>>,
    /// Concurrent peer read-through fills allowed. Becomes a semaphore, clamped to at
    /// least one — a zero-permit semaphore would deadlock every read-through.
    pub fill_parallelism: usize,
    /// Where a read-through fill's bytes go (ADR-0028) — the same value the proxy
    /// holds, because a node where only one fill path uses the slab looks healthy and
    /// does nothing.
    pub fill: ChunkFill,
    /// This node's EFA plane, or `None` on a non-EFA node or a failed capability
    /// probe (ADR-0018) — treated exactly like "the peer didn't ask for RDMA".
    #[cfg(feature = "efa")]
    pub efa: Option<Arc<pacer_transport::efa::EfaRdmaTransport>>,
    /// The dedicated RDMA serve-path runtime holder serves are bridged onto, so
    /// client-role proxy work cannot delay them (planning/15 all-hammer contention).
    #[cfg(feature = "efa")]
    pub rdma_runtime: tokio::runtime::Handle,
}

impl PacerPeer {
    /// Assemble the peer server from the daemon's shared parts ([`PeerParts`]).
    #[must_use]
    pub fn new(parts: PeerParts) -> Self {
        Self {
            tier: parts.tier,
            backend: parts.backend,
            chunk: parts.chunk,
            ring: parts.ring,
            directory: parts.directory,
            local_node: parts.local_node,
            replication_r: parts.replication_r,
            min_object_size: parts.min_object_size,
            max_object_size: parts.max_object_size,
            channel_capacity: parts.channel_capacity,
            metrics: parts.metrics,
            fill: parts.fill,
            // foyer's own behaviour unless an operator opts out (see
            // `Self::with_promotion`).
            promotion: Promotion::default(),
            filling: parts.filling,
            // `fill_parallelism` resolves to a nonzero default in config, but a
            // Semaphore of 0 permits would deadlock every read-through, so clamp.
            readthrough_limit: Arc::new(Semaphore::new(parts.fill_parallelism.max(1))),
            serve_limit: Arc::new(Semaphore::new(HOLDER_SERVE_SLOTS)),
            #[cfg(feature = "efa")]
            efa: parts.efa,
            #[cfg(feature = "efa")]
            rdma_runtime: parts.rdma_runtime,
            staging: None,
        }
    }

    /// Upload one staged window as a part of the coordinator's multipart upload,
    /// signed with **this** node's credentials (ADR-0006 already re-signs, so a
    /// scattered part needs no new IAM — verified cross-session in
    /// `spike/mpu-scatter`).
    ///
    /// The per-part CRC32 the coordinator computed travels with it, so a window
    /// corrupted on the peer hop is rejected here by S3 rather than assembled
    /// into the object.
    ///
    /// # Why the payload is not signed
    ///
    /// The Rust SDK sets no payload override for `UploadPart`, so a body handed over
    /// in memory is signed as `SignableBody::Bytes`: a SHA-256 over the whole window,
    /// computed synchronously on the tokio worker that called `send()`, before the
    /// first byte moves (`aws-runtime` `auth/sigv4.rs`). That is ~8 ms of a worker
    /// thread per 16 MiB part and, at the rates ADR-0032 is for, whole cores of the
    /// runtime spent hashing bodies that S3 already integrity-checks by the CRC32
    /// above, over TLS. `UNSIGNED-PAYLOAD` is what every other AWS SDK and the CRT
    /// send S3 over HTTPS; `disable_payload_signing` is this SDK's own switch for it,
    /// and it changes exactly one header.
    ///
    /// # Errors
    ///
    /// Any backend failure, which the caller turns into a `Status` after
    /// releasing the staging reservation.
    async fn upload_scattered_part(
        &self,
        req: &StoreChunkRequest,
        body: Bytes,
    ) -> anyhow::Result<String> {
        let out = self
            .backend
            .upload_part()
            // Already resolved through the coordinator's bucket map: re-mapping
            // an alias here would be a silent wrong-target write.
            .bucket(&req.bucket)
            .key(&req.key)
            .upload_id(&req.upload_id)
            .part_number(req.part_number)
            .checksum_crc32(&req.checksum_crc32)
            .body(aws_sdk_s3::primitives::ByteStream::from(body))
            .customize()
            .disable_payload_signing()
            .send()
            .await?;
        out.e_tag().map(ToOwned::to_owned).ok_or_else(|| {
            anyhow::anyhow!("UploadPart returned no ETag for part {}", req.part_number)
        })
    }

    /// [`Self::upload_scattered_part`], charged to `served_upload`.
    ///
    /// **The phase that decides ADR-0032 Phase 5**: if it accounts for nearly all of the
    /// coordinator's `owner_rpc`, the remote leg is S3's per-part throughput and
    /// replacing gRPC `StoreChunk` with a one-sided RDMA WRITE buys that remainder and
    /// no more.
    ///
    /// A wrapper rather than a timer inside `upload_scattered_part` so that function
    /// stays what its name says — one `UploadPart` and its ETag — and the failure path
    /// is charged as well as the success one: an upload that errored still occupied the
    /// coordinator's slot for as long as it ran.
    async fn timed_scattered_part(
        &self,
        req: &StoreChunkRequest,
        body: Bytes,
    ) -> anyhow::Result<String> {
        let started = std::time::Instant::now();
        let uploaded = self.upload_scattered_part(req, body).await;
        self.metrics.scatter.observe_phase(
            crate::metrics::SCATTER_PHASE_SERVED_UPLOAD,
            started.elapsed(),
        );
        uploaded
    }

    /// Count one refused window, by reason.
    ///
    /// Counted **here**, where the taxonomy is decided, rather than at the
    /// coordinator: the coordinator's own fallback is already visible as
    /// `windows{role="local"}`, and what an operator cannot otherwise see is which
    /// node did the refusing and why. On a balanced all-ranks save this series is
    /// expected to be large — that is reject-fast working (ADR-0032 § 4), and
    /// without it a fleet that has silently stopped scattering looks exactly like
    /// one that never tried.
    fn note_refusal(&self, refusal: &StoreRefusal) {
        self.metrics
            .scatter
            .refusals
            .with_label_values(&[refusal_label(refusal)])
            .inc();
    }

    /// Accept scattered writes, staging them in `staging` (ADR-0032 § 2).
    ///
    /// Builder-style rather than another constructor argument because it is
    /// genuinely optional: without it the node answers every `StoreChunk` with
    /// [`RefusalReason::NotAccepting`], which is the correct behaviour when the
    /// scatter is off or the backend is not a general-purpose bucket (§ 6).
    #[must_use]
    pub fn with_staging(mut self, staging: Arc<StagingArea>) -> Self {
        self.staging = Some(staging);
        self
    }

    /// Set whether a serve's local disk hit promotes into the RAM tier.
    ///
    /// Builder-style rather than a [`PeerParts`] field, and it must be given
    /// the same value as [`crate::proxy::PacerProxy::with_promotion`]: a holder
    /// that promoted while the proxy did not would make one node's RAM tier behave
    /// two ways depending on which path read it.
    #[must_use]
    pub fn with_promotion(mut self, promotion: Promotion) -> Self {
        self.promotion = promotion;
        self
    }

    /// Wrap into the tonic service for `Server::add_service`.
    pub fn into_service(self) -> PeerServer<Self> {
        // A StoreChunk carries a whole chunk_size window, which is far past
        // tonic's 4 MiB default (ADR-0032 § 2); the client raises the same two
        // limits, and both must move together or one direction silently caps.
        PeerServer::new(self)
            .max_decoding_message_size(pacer_transport::MAX_PEER_MESSAGE_BYTES)
            .max_encoding_message_size(pacer_transport::MAX_PEER_MESSAGE_BYTES)
    }

    /// Serve a cached chunk. A chunk is the cache unit (ADR-0015): a whole
    /// `Bytes` body, no server-side range slicing (the requester fetches whole
    /// chunks and trims client-side). When the request offered an `rdma_buffer`
    /// and this node's EFA plane can reach the requester, WRITE the chunk there
    /// instead and stream back only the done signal (ADR-0018 point 3: the
    /// response IS the completion); otherwise stream the body as BlobChunk
    /// frames.
    // tonic::Status is ~180 bytes by design; every tonic server signature
    // carries it.
    #[allow(clippy::result_large_err)]
    async fn serve_cached(
        &self,
        chunk: &CachedChunk,
        req: &FetchBlobRequest,
    ) -> Result<ReceiverStream<Result<BlobChunk, Status>>, Status> {
        let body = chunk.body.clone();
        self.metrics.peer_serves.inc();
        self.metrics.bytes_to_peers.inc_by(body.len() as u64);
        let written = self.try_serve_via_write(req, &body).await;
        let served_via_rdma = written.served;
        // Chunk bodies carry no per-object metadata (it lives in the header
        // entry, fetched separately). The requester reads only the bytes.
        let meta = BlobMeta {
            total_len: body.len() as u64,
            e_tag: None,
            object_len: body.len() as u64,
            body_start: 0,
            content_type: None,
            last_modified_epoch_secs: None,
            served_via_rdma,
            written_crc32: written.crc32,
        };
        if served_via_rdma {
            // Bytes already landed on the requester; this response carries
            // only the done signal, no body (ADR-0018 point 3).
            self.metrics.peer_serves_rdma.inc();
            return Ok(done_only_stream(meta));
        }
        Ok(stream_bytes(body, meta, self.channel_capacity))
    }

    /// Attempt a one-sided WRITE for a request that offered somewhere to write.
    ///
    /// Two destinations, and the request names at most one of them (the proto makes them
    /// mutually exclusive):
    ///
    /// * `rdma_buffer` — the REQUESTER's memory, either a range of its own arena (ADR-0018)
    ///   or a client window it registered on its own behalf (ADR-0026/0027). One `RdmaBuffer`,
    ///   bound to the requester's rail by the by-index pairing rule.
    /// * `client_token` — the READING CLIENT's own registered memory (ADR-0030 point 4,
    ///   planning/19 C3). This node writes straight into the loader, so the requester never
    ///   sees these bytes at all — which is why this arm is the one that returns a CRC32.
    ///
    /// [`WriteServe::streamed`] for every reason to fall back to streaming instead — no
    /// destination on the request, this node has no EFA plane (non-EFA node or failed probe),
    /// the client declined the WRITE, or the WRITE itself failed. All logged, none propagated
    /// as an RPC error: the gRPC streaming path always still answers this same request, and a
    /// requester that offered a client token writes the streamed body into that client itself
    /// (the two-hop path this exists to skip).
    async fn try_serve_via_write(&self, req: &FetchBlobRequest, body: &Bytes) -> WriteServe {
        #[cfg(not(feature = "efa"))]
        {
            let _ = (req, body);
            WriteServe::streamed()
        }
        #[cfg(feature = "efa")]
        {
            // Both destinations at once is a requester bug, and picking one would be a guess
            // about which memory it means. Streaming is the answer that is right under either
            // reading: the requester handles `served_via_rdma == false` on both paths.
            if req.rdma_buffer.is_some() && req.client_token.is_some() {
                warn!(key = %req.cache_key, requester = ?req.requester_node_id,
                    "FetchBlob named both an rdma_buffer and a client_token; streaming rather than guessing which memory it meant");
                return WriteServe::streamed();
            }
            if let Some(token) = &req.client_token {
                return self.write_into_client_token(req, token, body).await;
            }
            let (Some(efa), Some(buffer), Some(requester)) =
                (&self.efa, &req.rdma_buffer, &req.requester_node_id)
            else {
                return WriteServe::streamed();
            };
            // Bridge the WRITE onto the dedicated RDMA runtime: this tonic
            // handler runs on the main runtime, so running the stage copy /
            // post / completion-await inline would put holder-serve CPU right
            // back in contention with client-role S3-proxy tasks — exactly what
            // the second runtime exists to prevent. `serve_via_write`'s own
            // body and signature are untouched; only the call site moves.
            // Owned clones (all cheap: `efa`/`body` are refcounts, the ids/
            // buffer are small) let the spawned future be `'static`.
            let efa = Arc::clone(efa);
            let requester_owned = requester.clone();
            let buffer = *buffer;
            let body = body.clone();
            let joined = self
                .rdma_runtime
                .spawn(async move { efa.serve_via_write(&requester_owned, &buffer, &body).await })
                .await;
            match joined {
                // No CRC32 on this arm: the requester owns the destination, so it digests for
                // itself — by reading the delivered window back where it can, which proves
                // what is IN the client's memory rather than what was sent.
                Ok(Ok(served)) => WriteServe {
                    served,
                    crc32: None,
                },
                Ok(Err(e)) => {
                    warn!(key = %req.cache_key, requester, error = %e, "RDMA WRITE failed; falling back to streaming this response");
                    WriteServe::streamed()
                }
                // The serve task panicked or was cancelled with the runtime
                // shutting down — treat as any other serve failure and fall
                // back to streaming rather than propagating an RPC error.
                Err(e) => {
                    warn!(key = %req.cache_key, requester, error = %e, "RDMA serve task did not complete; falling back to streaming this response");
                    WriteServe::streamed()
                }
            }
        }
    }

    /// WRITE this chunk into the reading **client's** own registered window (ADR-0030 point 4).
    ///
    /// The mechanism is [`pacer_transport::efa::EfaRdmaTransport::write_into_token`] unchanged
    /// — the same call the *requesting* node makes for a chunk it holds locally, run here
    /// instead. Nothing in it is specific to being the node the client is talking to: this
    /// holder announces itself to the client, builds its own address handles, spreads its
    /// WRITEs over the client's rails, and pays first contact once per client endpoint.
    ///
    /// The digest is the one thing this path owes that the others do not. The requester never
    /// sees these bytes, and a client-registered window cannot be read back (ADR-0030 point
    /// 7), so a CRC32 of what was written can only come from here — computed **concurrently
    /// with the WRITE**, because one is wire-bound and the other CPU-bound, and on the
    /// blocking pool for the same reason every other chunk-sized digest in this codebase is.
    ///
    /// Every failure is [`WriteServe::streamed`]: the body then goes back over gRPC and the
    /// requester writes it into the client from there, which is exactly the two-hop path this
    /// replaces. So the worst case of this whole feature is the behaviour that preceded it.
    #[cfg(feature = "efa")]
    async fn write_into_client_token(
        &self,
        req: &FetchBlobRequest,
        token: &pacer_proto::v1::ClientToken,
        body: &Bytes,
    ) -> WriteServe {
        use pacer_transport::efa::TokenWrite;
        use pacer_transport::token::TokenWindow;

        let Some(efa) = &self.efa else {
            return WriteServe::streamed();
        };
        let window = match TokenWindow::from_proto(token) {
            Ok(window) => window,
            Err(e) => {
                // A malformed token is the requester's bug, not a fabric condition — but it
                // must not fail the read either, since the streamed body still answers it.
                warn!(key = %req.cache_key, requester = ?req.requester_node_id, error = %e,
                    "FetchBlob carried an unusable client token; streaming this response");
                return WriteServe::streamed();
            }
        };
        // Both halves are started before either is awaited, so the digest rides along with
        // the wire round trip instead of following it.
        let (efa, write_body) = (Arc::clone(efa), body.clone());
        // Offset 0: `ClientToken.addr` is already this chunk's absolute destination — the
        // requester folded every offset in (see `TokenWindow::from_proto`).
        let writing = self
            .rdma_runtime
            .spawn(async move { efa.write_into_token(&window, 0, &write_body).await });
        let digest_body = body.clone();
        let digesting = token.checksum.then(|| {
            tokio::task::spawn_blocking(move || {
                crate::delivery::DeliveryDigest::of(&digest_body).value()
            })
        });
        let outcome = match writing.await {
            Ok(Ok(outcome)) => outcome,
            Ok(Err(e)) => {
                warn!(key = %req.cache_key, error = %format!("{e:#}"),
                    "WRITE into the client's registered window failed; streaming this response");
                return WriteServe::streamed();
            }
            Err(e) => {
                warn!(key = %req.cache_key, error = %e,
                    "client-token WRITE task did not complete; streaming this response");
                return WriteServe::streamed();
            }
        };
        if let TokenWrite::Declined(declined) = outcome {
            // Counted under the same series the requester's own declines use, because the
            // question a dashboard asks is "why is this client not taking WRITEs" and the
            // answer must not split by which node happened to be writing.
            self.metrics
                .delivery
                .declines
                .with_label_values(&[declined.label()])
                .inc();
            debug!(key = %req.cache_key, reason = declined.label(),
                "client declined this holder's WRITE; streaming this response");
            return WriteServe::streamed();
        }
        let crc32 = match digesting {
            None => None,
            Some(task) => match task.await {
                Ok(crc) => Some(crc),
                Err(e) => {
                    // The bytes ARE in the client's memory, but a delivery whose checksum was
                    // asked for and cannot be produced is unverifiable — and the requester
                    // refuses exactly that rather than folding a hole into the client's
                    // digest. Stream instead, so it delivers and digests them itself.
                    //
                    // That means the same bytes are written to the same offsets twice, which
                    // is safe rather than merely tolerable: the second WRITE carries the
                    // identical chunk body to the identical address and completes before the
                    // requester reports the window placed, so the client — which may not read
                    // its buffer until the 200 — cannot observe the difference. Reachable only
                    // if the blocking pool is shutting down or `crc32fast` panicked.
                    warn!(key = %req.cache_key, error = %e,
                        "digest of a client-token WRITE failed; streaming this response so the requester can digest it");
                    return WriteServe::streamed();
                }
            },
        };
        WriteServe {
            served: true,
            crc32,
        }
    }

    /// The RDMA capabilities this handshake response advertises: real ones
    /// if this node's EFA plane is up, the all-false default otherwise
    /// (non-EFA node, or the startup probe failed — ADR-0018's capability
    /// gate).
    fn rdma_capabilities(&self) -> RdmaCapabilities {
        #[cfg(not(feature = "efa"))]
        {
            RdmaCapabilities::default()
        }
        #[cfg(feature = "efa")]
        {
            self.efa
                .as_ref()
                .map(|_| pacer_transport::efa::advertised_capabilities())
                .unwrap_or_default()
        }
    }

    /// This node's SRD endpoint to carry in the handshake response, so the
    /// peer can AH-insert us before it ever needs to (ADR-0019
    /// bidirectionality) — `None` on a non-EFA node.
    fn efa_endpoint(&self) -> Option<pacer_proto::v1::EfaEndpoint> {
        #[cfg(not(feature = "efa"))]
        {
            None
        }
        #[cfg(feature = "efa")]
        {
            self.efa.as_ref().map(|efa| efa.endpoint_proto())
        }
    }

    /// Record a peer's `EfaEndpoint` (insert its AH), logging (not failing
    /// the handshake) on any error — a malformed endpoint or an
    /// EFA-unreachable peer just means this pair stays on gRPC.
    #[cfg(feature = "efa")]
    async fn learn_efa_peer(
        &self,
        peer_node_id: &str,
        endpoint: &pacer_proto::v1::EfaEndpoint,
        efa: &pacer_transport::efa::EfaRdmaTransport,
    ) {
        if let Err(e) = efa.learn_peer(peer_node_id, endpoint).await {
            warn!(peer = peer_node_id, error = %e, "failed to learn peer's EFA endpoint");
        }
    }

    /// Owner read-through of one chunk: a backend ranged GET of the chunk's
    /// bounds, streamed to the requester while the fill accumulates
    /// (serve-while-fill, per chunk). The chunk's byte range is `index*size ..`; S3
    /// clamps the end to the object, so the object length need not be known up
    /// front (it arrives in `Content-Range`).
    // Same reason as `serve_cached` above: `tonic::Status` is ~180 bytes by design and
    // every tonic server signature carries it.
    #[allow(clippy::result_large_err)]
    async fn read_through(
        &self,
        cache_key: &str,
    ) -> Result<ReceiverStream<Result<BlobChunk, Status>>, Status> {
        let (object_key, index) = parse_chunk_key(cache_key)
            .ok_or_else(|| Status::invalid_argument("malformed chunk key"))?;
        let (bucket, key) = object_key_parts(object_key)
            .ok_or_else(|| Status::invalid_argument("malformed cache key"))?;
        // Take a fill permit BEFORE the backend GET so the in-flight S3 body and
        // the fill accumulator are both counted against the bound; excess
        // concurrent misses await here (backpressure to the requester) rather
        // than each allocating a fresh read-through. Held for the whole pump
        // (moved into `PumpState`), released when the fill finishes. The
        // Semaphore is never closed, so `acquire_owned` only errs on a bug —
        // treat it as a transient unavailable rather than panicking the handler.
        let permit = Arc::clone(&self.readthrough_limit)
            .acquire_owned()
            .await
            .map_err(|_| Status::unavailable("read-through limiter closed"))?;
        let start = index * self.chunk.chunk_size();
        // HTTP Range is inclusive on both ends; the chunk spans `chunk_size`
        // bytes from `start` (S3 clamps a past-the-end `last` to the object).
        let last = start + self.chunk.chunk_size() - 1;
        let resp = self
            .backend
            .get_object()
            .bucket(bucket)
            .key(key)
            .range(format!("bytes={start}-{last}"))
            .send()
            .await
            .map_err(|e| {
                let svc = e.into_service_error();
                if svc.is_no_such_key() {
                    Status::not_found("no such key at backend")
                } else {
                    Status::unavailable(format!("backend GET failed: {svc}"))
                }
            })?;

        // Length of THIS chunk (the ranged body), not the whole object.
        let chunk_len = resp
            .content_length()
            .and_then(|l| u64::try_from(l).ok())
            .ok_or_else(|| Status::internal("backend GET without content length"))?;
        let object_len = content_range_total(resp.content_range()).unwrap_or(chunk_len);

        // Admit on the WHOLE-object size (small-object bypass, ADR-0002), and
        // only if no other fill of this chunk key is in flight — claimed through
        // the same [`FillGuard`] the proxy's fill sites use, so a pump whose future
        // is dropped mid-fill releases the key instead of stranding it (see
        // `PumpState::guard`).
        let guard = if pacer_cache::should_admit(
            Some(object_len),
            self.min_object_size,
            self.max_object_size,
        ) {
            FillGuard::for_fill(&self.filling, &self.metrics, cache_key)
        } else {
            None
        };
        let admit = guard.is_some();
        let meta = BlobMeta {
            total_len: chunk_len,
            e_tag: resp.e_tag().map(|e| e.trim_matches('"').to_owned()),
            object_len,
            body_start: start,
            content_type: resp.content_type().map(str::to_owned),
            last_modified_epoch_secs: resp.last_modified().map(|t| t.secs()),
            // A1 scoping: read-through streams straight from the backend
            // response as it arrives (see `pump_read_through` below) — there
            // is no whole body in hand yet to WRITE in one shot the way
            // `serve_cached`'s already-materialized `Bytes` allows. RDMA on
            // this path (bouncing the backend stream through a leased
            // buffer as it fills) is a possible follow-up, not A1 scope.
            served_via_rdma: false,
            // Nothing was written, so there is nothing to attest — and a requester that
            // asked a client token for a checksum treats a missing one as a protocol
            // violation only when `served_via_rdma` is set.
            written_crc32: None,
        };
        self.metrics.peer_readthroughs.inc();
        self.metrics.bytes_to_peers.inc_by(chunk_len);

        let (tx, rx) = mpsc::channel::<Result<BlobChunk, Status>>(self.channel_capacity);
        tokio::spawn(pump_read_through(PumpState {
            body: resp.body,
            tx,
            meta,
            chunk_len,
            buf: admit.then(|| BytesMut::with_capacity(chunk_len as usize)),
            tier: self.tier.clone(),
            metrics: self.metrics.clone(),
            fill: self.fill.clone(),
            guard,
            cache_key: cache_key.to_owned(),
            directory: self.directory.clone(),
            local_node: self.local_node.clone(),
            _permit: permit,
        }));
        Ok(ReceiverStream::new(rx))
    }
}

/// Everything [`pump_read_through`] needs, bundled so the spawn site stays flat.
struct PumpState {
    body: aws_sdk_s3::primitives::ByteStream,
    tx: mpsc::Sender<Result<BlobChunk, Status>>,
    /// Metadata for the first frame.
    meta: BlobMeta,
    /// Length of the chunk body being read through.
    chunk_len: u64,
    /// Fill accumulator; None = not admitted, stream-only.
    buf: Option<BytesMut>,
    tier: ChunkTier,
    metrics: Metrics,
    /// Where the accumulated fill's bytes go (ADR-0028), cloned from the
    /// service so this path cannot diverge from the proxy's.
    fill: ChunkFill,
    /// This fill's claim on the node-wide dedup set, held for the pump's whole
    /// life and released by `Drop` — `None` when nothing was admitted, in which
    /// case this pump only streams.
    ///
    /// It is the guard rather than the set itself because this future is spawned
    /// and can be dropped mid-`await` (runtime shutdown) or unwound through (a
    /// panic in `put_chunk`), and both skip the `filling.remove()` that used to sit
    /// at the end of the pump — stranding that chunk key for the daemon's lifetime,
    /// with nothing in `/metrics` to show it. The proxy's fill sites were fixed
    /// this way; this path claims the SAME set, so it inherited the bug and now
    /// inherits the fix, including `pacer_fill_inflight` finally counting a peer
    /// read-through.
    guard: Option<FillGuard>,
    cache_key: String,
    /// Directory shard to record this owner-fill in (ADR-0017); paired with
    /// `local_node`. Read-through only runs for keys this node owns, so this
    /// is the chunk's home recording itself — a local admit, no announce RPC.
    directory: SharedDirectory,
    local_node: String,
    /// Fill-concurrency permit (see [`PacerPeer::readthrough_limit`]), held for
    /// the pump's lifetime and released on drop when the fill completes or
    /// aborts — this is what bounds concurrent read-through memory.
    _permit: OwnedSemaphorePermit,
}

/// Split a chunk key `"{object_key}#{size}:{index}"` into its object key and
/// chunk index. The object key half never contains `#` (see
/// [`pacer_cache::chunk::ChunkConfig::chunk_key`]), so the last `#` is the
/// boundary. `None` if the key is not a well-formed chunk key.
fn parse_chunk_key(cache_key: &str) -> Option<(&str, u64)> {
    let (object_key, suffix) = cache_key.rsplit_once('#')?;
    let (_size, index) = suffix.split_once(':')?;
    Some((object_key, index.parse().ok()?))
}

/// Parse the `/{total}` denominator out of a `Content-Range: bytes a-b/total`
/// header into the whole-object length.
fn content_range_total(content_range: Option<&str>) -> Option<u64> {
    content_range?.rsplit_once('/')?.1.parse().ok()
}

/// Relay backend bytes to the requester while accumulating the fill.
///
/// A dropped requester closes the channel; when a fill is in flight the pump
/// keeps reading so it still completes (the backend bytes are already paid
/// for). Without a fill, a closed channel ends the pump.
async fn pump_read_through(mut st: PumpState) {
    let mut sent: u64 = 0;
    let mut first = Some(st.meta);
    let mut failed = false;
    loop {
        let chunk = match st.body.try_next().await {
            Ok(Some(chunk)) => chunk,
            Ok(None) => break,
            Err(e) => {
                // NOT retried here, on purpose: these bytes are already being
                // relayed to the requester, so resuming would mean splicing a
                // second GET's body onto a partial one. The error travels to the
                // requester instead, whose `fetch_from_peer` falls back to its own
                // backend read — and THAT one retries (`pacer_backend::retry`), so
                // a transient fault on this path still costs the client nothing
                // but a re-read.
                warn!(key = %st.cache_key, error = %e, "backend stream failed mid-read-through");
                let _ = st.tx.send(Err(Status::unavailable(e.to_string()))).await;
                failed = true;
                break;
            }
        };
        sent += chunk.len() as u64;
        if let Some(buf) = st.buf.as_mut() {
            buf.extend_from_slice(&chunk);
        }
        let msg = BlobChunk {
            data: chunk,
            meta: first.take(),
        };
        if st.tx.send(Ok(msg)).await.is_err() && st.buf.is_none() {
            failed = true;
            break;
        }
    }
    let Some(buf) = st.buf else {
        return;
    };
    if !failed && sent == st.chunk_len {
        // Through `fill`, NOT `buf.freeze()` directly: on a node with an ADR-0028
        // slab this is what puts the chunk in registered memory so the holder can
        // WRITE it without staging a copy. Getting this wrong is invisible in
        // every metric except `pacer_cache_slab_stores_total` — it was 0 with a
        // fully registered slab on the first hardware run, because a cold holder
        // fills HERE and this line did not consult the slab.
        let body = st.fill.cached_bytes(&buf.freeze(), &st.metrics);
        if let Err(e) = st
            .tier
            .put_chunk(&st.cache_key, CachedChunk::new(body))
            .await
        {
            warn!(key = %st.cache_key, error = %e, "chunk fill could not reach the disk tier");
        }
        // Record this node as a holder in its own directory shard (ADR-0017):
        // the fill lands in DRAM, so the tier hint is Dram (advisory — foyer
        // may demote it later; a stale hint costs a suboptimal source pick,
        // never a wrong serve).
        st.directory
            .admit_next(&st.cache_key, &st.local_node, Tier::Dram);
        st.metrics.fills_completed.inc();
        st.metrics.bytes_filled.inc_by(sent);
    } else {
        st.metrics.fills_aborted.inc();
        debug!(key = %st.cache_key, got = sent, expected = st.chunk_len, "read-through fill abandoned");
    }
    // Both branches completed the fill *on their own* — the second one failed, but
    // it observed the failure and counted `fills_aborted` for it. Only a pump that
    // never got here (its future dropped mid-poll) is what
    // `pacer_fill_abandoned_total` exists to make visible, so the guard is marked
    // complete on either outcome and the key is released by its `Drop`.
    if let Some(guard) = st.guard.as_mut() {
        guard.complete();
    }
}

/// Chunk a refcounted body into BlobChunk frames; the first carries the metadata.
fn stream_bytes(
    body: Bytes,
    meta: BlobMeta,
    channel_capacity: usize,
) -> ReceiverStream<Result<BlobChunk, Status>> {
    let (tx, rx) = mpsc::channel(channel_capacity);
    tokio::spawn(async move {
        let mut meta = Some(meta);
        let mut offset = 0;
        // An empty body still needs one frame to deliver the metadata.
        loop {
            let end = (offset + FRAME_SIZE).min(body.len());
            let msg = BlobChunk {
                data: body.slice(offset..end),
                meta: meta.take(),
            };
            if tx.send(Ok(msg)).await.is_err() {
                return; // requester went away
            }
            offset = end;
            if offset >= body.len() {
                return;
            }
        }
    });
    ReceiverStream::new(rx)
}

/// A one-message response carrying only `meta` (`served_via_rdma = true`,
/// empty `data`) — the ADR-0018 point 3 done signal for a request the WRITE
/// path already served.
fn done_only_stream(meta: BlobMeta) -> ReceiverStream<Result<BlobChunk, Status>> {
    let (tx, rx) = mpsc::channel(1);
    tokio::spawn(async move {
        let _ = tx
            .send(Ok(BlobChunk {
                data: Bytes::new(),
                meta: Some(meta),
            }))
            .await;
    });
    ReceiverStream::new(rx)
}

#[tonic::async_trait]
impl Peer for PacerPeer {
    async fn handshake(
        &self,
        request: Request<HandshakeRequest>,
    ) -> Result<Response<HandshakeResponse>, Status> {
        let peer = request.into_inner();
        debug!(peer = %peer.node_id, version = peer.protocol_version, "handshake");
        #[cfg(feature = "efa")]
        if let (Some(efa), Some(endpoint)) = (&self.efa, &peer.efa_endpoint) {
            self.learn_efa_peer(&peer.node_id, endpoint, efa).await;
        }
        Ok(Response::new(HandshakeResponse {
            node_id: self.local_node.clone(),
            capabilities: Some(self.rdma_capabilities()),
            protocol_version: pacer_transport::grpc::PROTOCOL_VERSION,
            efa_endpoint: self.efa_endpoint(),
        }))
    }

    type FetchBlobStream = ReceiverStream<Result<BlobChunk, Status>>;

    async fn fetch_blob(
        &self,
        request: Request<FetchBlobRequest>,
    ) -> Result<Response<Self::FetchBlobStream>, Status> {
        let req = request.into_inner();
        // Admission gate BEFORE the cache read: a hit materializes a whole
        // (16 MiB default) chunk body here and, on the RDMA path, holds it
        // through stage + WRITE + completion. Bounding in-flight serves to
        // `HOLDER_SERVE_SLOTS` caps resident serve memory at slots × chunk_size
        // no matter how deep the aggregate requester fan-in is — the bound the
        // holder-arena lease alone could not give, since the body is resident
        // before a serve ever reaches that gate (planning/15 B4). Excess
        // concurrent fetches wait here (correct backpressure — the admitted
        // count is sized to keep the wire busy). Held for the whole handler; the RDMA
        // serve awaits its WRITE inline, so the body is freed before release.
        let _serve_permit = self
            .serve_limit
            .acquire()
            .await
            .map_err(|_| Status::unavailable("serve limiter closed"))?;
        match self.tier.get_chunk(&req.cache_key).await {
            Ok(Some(chunk)) => {
                return Ok(Response::new(self.serve_cached(&chunk, &req).await?));
            }
            Ok(None) => {}
            Err(e) => warn!(key = %req.cache_key, error = %e, "cache read failed on peer path"),
        }

        // Miss. Read through only for fill-eligible chunk fetches of keys this
        // node co-homes; everything else is the requester's problem (direct
        // backend GET, no fill anywhere — ADR-0011/0012/0016). Chunk fetches
        // carry no range (the chunk IS the unit), so there is no ranged-miss
        // case anymore. A node in the top-R for the key reads through and fills
        // (layer 2: all R homes fill on read-through).
        let homes = self.ring.homes(&req.cache_key, self.replication_r);
        let owns = homes.iter().any(|n| n.name() == self.local_node);
        if req.no_fill || !owns {
            self.metrics.peer_misses.inc();
            return Err(Status::not_found("blob not cached"));
        }
        Ok(Response::new(self.read_through(&req.cache_key).await?))
    }

    /// Take one window of a scattered write: stage it, upload it as a part with
    /// this node's own credentials, and answer with the part's ETag (ADR-0032 § 2).
    ///
    /// Staging is reserved *before* the upload starts, so the budget covers the
    /// upload's in-flight bytes as well as the wait for commit — and a refusal
    /// comes back as a normal response, never a `Status`, because a busy owner and
    /// a broken one call for different counters even though the coordinator's
    /// action (upload it itself) is the same.
    ///
    /// # This handler times itself, and that is what makes Phase 5 decidable
    ///
    /// The coordinator's clock around `StoreChunk` covers this whole handler, so it
    /// cannot tell the network hop from the `UploadPart` inside it — and those two
    /// readings argue for opposite decisions about ADR-0032 Phase 5's RDMA WRITE. So the
    /// two steps are charged **here**, as `served_stage` and `served_upload`, and the
    /// hop is `owner_rpc − (served_stage + served_upload)`. Reported on this node's own
    /// `/metrics` rather than returned in `StoreChunkResponse` on purpose: an owner
    /// that did not report would make the subtraction blame the entire RPC on the wire,
    /// whereas an absent series is visibly a gap. See
    /// [`crate::metrics::ScatterMetrics::phase_seconds`].
    async fn store_chunk(
        &self,
        request: Request<StoreChunkRequest>,
    ) -> Result<Response<StoreChunkResponse>, Status> {
        let mut req = request.into_inner();
        let Some(staging) = self.staging.clone() else {
            self.note_refusal(&StoreRefusal::NotAccepting);
            return Ok(Response::new(refused(RefusalReason::NotAccepting, 0, 0)));
        };
        // Take the payload out so `req` stays usable as the (small) descriptor the
        // upload needs, without cloning a whole window.
        let body = Bytes::from(std::mem::take(&mut req.data));
        let chunk_len = body.len() as u64;
        // A mutex and a budget compare, so this belongs in the microsecond floor of the
        // bucket set — and it is measured precisely so that can be *checked*: a
        // `served_stage` climbing into milliseconds is a lock convoy across
        // `windowsInFlight × N` concurrent offers, which is the one term here whose
        // smallness is the finding rather than an assumption.
        let staged_at = std::time::Instant::now();
        let outcome = staging.try_stage(&req.chunk_key, &req.upload_id, body.clone());
        self.metrics.scatter.observe_phase(
            crate::metrics::SCATTER_PHASE_SERVED_STAGE,
            staged_at.elapsed(),
        );
        match outcome {
            StageOutcome::Refused(refusal) => {
                debug!(key = %req.chunk_key, ?refusal, "refused a scattered window");
                self.note_refusal(&refusal);
                Ok(Response::new(refusal_response(&refusal, chunk_len)))
            }
            // A re-offer of the same upload must still be uploaded, since the
            // coordinator is missing the ETag it needs for Complete — S3 takes a
            // repeated part number for the same bytes without complaint.
            StageOutcome::Staged | StageOutcome::AlreadyStaged => {
                match self.timed_scattered_part(&req, body).await {
                    Ok(e_tag) => Ok(Response::new(StoreChunkResponse {
                        e_tag: Some(e_tag),
                        refusal: None,
                    })),
                    Err(e) => {
                        // Release at once rather than letting the reservation sit
                        // until the TTL: this node just proved it cannot serve
                        // this window, and holding budget would refuse the next.
                        staging.release(&req.chunk_key);
                        warn!(key = %req.chunk_key, error = %e, "scattered part upload failed");
                        Err(Status::internal(format!("upload_part failed: {e}")))
                    }
                }
            }
        }
    }

    /// Publish an upload's staged chunks now that Complete has made the object
    /// exist (ADR-0032 § 3). Not awaited by the coordinator and needing no
    /// atomicity: this turns a guaranteed miss into a possible hit.
    async fn commit_upload(
        &self,
        request: Request<CommitUploadRequest>,
    ) -> Result<Response<CommitUploadResponse>, Status> {
        let req = request.into_inner();
        let Some(staging) = self.staging.clone() else {
            return Ok(Response::new(CommitUploadResponse { committed: 0 }));
        };
        let chunks = staging.commit(&req.upload_id);
        let committed = u32::try_from(chunks.len()).unwrap_or(u32::MAX);
        for (chunk_key, body) in chunks {
            // The witness is what makes a later version check a flag flip rather
            // than a cache migration (ADR-0032 § 7).
            if let Err(e) = self
                .tier
                .put_chunk(&chunk_key, CachedChunk::versioned(body, req.e_tag.clone()))
                .await
            {
                warn!(key = %chunk_key, error = %e, "staged chunk could not reach the disk tier");
            }
            // This node is the chunk's home, so recording the holder is a local
            // directory write with no RPC (ADR-0017 "home-is-holder").
            self.directory
                .admit_next(&chunk_key, &self.local_node, Tier::Dram);
        }
        debug!(upload = %req.upload_id, committed, "published staged chunks");
        Ok(Response::new(CommitUploadResponse { committed }))
    }

    /// Drop an upload's staged chunks — Complete failed or the upload was
    /// aborted, so they describe an object that will never exist.
    async fn discard_upload(
        &self,
        request: Request<DiscardUploadRequest>,
    ) -> Result<Response<DiscardUploadResponse>, Status> {
        let req = request.into_inner();
        let discarded = self.staging.as_ref().map_or(0, |s| {
            u32::try_from(s.discard(&req.upload_id)).unwrap_or(u32::MAX)
        });
        debug!(upload = %req.upload_id, discarded, "discarded staged chunks");
        Ok(Response::new(DiscardUploadResponse { discarded }))
    }

    async fn invalidate(
        &self,
        request: Request<InvalidateRequest>,
    ) -> Result<Response<InvalidateResponse>, Status> {
        let req = request.into_inner();
        self.tier.forget(&req.cache_key).await;
        // ADR-0017: a write's invalidation clears the home's directory entry
        // too, not just this node's own cache copy — the sharer set for a
        // rewritten key must not outlive the write.
        self.directory.remove(&req.cache_key);
        Ok(Response::new(InvalidateResponse {}))
    }

    async fn announce(
        &self,
        request: Request<AnnounceRequest>,
    ) -> Result<Response<AnnounceResponse>, Status> {
        let req = request.into_inner();
        if req.evict {
            // Time the in-lock op only (B4 Step-6 directory-CPU input,
            // planning/15); the timer guard stops when this scope ends.
            let _timer = self.metrics.dir_rpc_timer("evict").start_timer();
            self.directory
                .evict(&req.chunk_key, &req.node, req.generation);
        } else {
            let tier = match req.tier.and_then(|t| WireTier::try_from(t).ok()) {
                Some(WireTier::Dram) => Tier::Dram,
                // Nvme and unspecified/unknown both fall to the conservative
                // tier (never claims to be faster than it is — see
                // pacer_transport::grpc::tier_from_wire's identical rationale).
                _ => Tier::Nvme,
            };
            let _timer = self.metrics.dir_rpc_timer("admit").start_timer();
            self.directory
                .admit(&req.chunk_key, &req.node, tier, req.generation);
        }
        Ok(Response::new(AnnounceResponse {}))
    }

    async fn lookup_sharers(
        &self,
        request: Request<LookupSharersRequest>,
    ) -> Result<Response<LookupSharersResponse>, Status> {
        let req = request.into_inner();
        // Time the in-lock lookup only — the dominant directory RPC under a
        // restore storm and the B4 Step-6 hotspot projection input (planning/15).
        let set = {
            let _timer = self.metrics.dir_rpc_timer("lookup").start_timer();
            self.directory.lookup(&req.chunk_key).unwrap_or_default()
        };
        Ok(Response::new(LookupSharersResponse {
            sharers: set
                .holders
                .into_iter()
                .map(|h| Sharer {
                    node: h.node,
                    tier: match h.tier {
                        Tier::Dram => WireTier::Dram.into(),
                        Tier::Nvme => WireTier::Nvme.into(),
                    },
                    generation: h.generation,
                })
                .collect(),
            widely_held: set.widely_held,
        }))
    }
}

/// A `StoreChunk` answer carrying a refusal and no ETag.
///
/// Exactly one of the two response fields is ever set; a client that sees neither
/// treats it as a protocol violation rather than a refusal, so this helper is the
/// only place refusals are built.
fn refused(reason: RefusalReason, staged_bytes: u64, budget_bytes: u64) -> StoreChunkResponse {
    StoreChunkResponse {
        e_tag: None,
        refusal: Some(WireRefusal {
            reason: reason.into(),
            staged_bytes,
            budget_bytes,
        }),
    }
}

/// The metric label for a refusal. One arm per variant rather than a `Debug`
/// rendering, so a label a dashboard depends on cannot change because someone
/// renamed a variant — the same rule the proxy's decline labels follow.
fn refusal_label(refusal: &StoreRefusal) -> &'static str {
    match refusal {
        StoreRefusal::BudgetExhausted { .. } => "budget_exhausted",
        StoreRefusal::RacingUpload => "racing_upload",
        StoreRefusal::OversizedForBudget { .. } => "oversized_for_budget",
        StoreRefusal::NotAccepting => "not_accepting",
        StoreRefusal::Unknown => "unknown",
    }
}

/// Put a [`StoreRefusal`] on the wire.
///
/// `chunk_len` is the offered window's length, needed because
/// [`RefusalReason::OversizedForBudget`] reports it in the `staged_bytes` field —
/// nothing is staged in that case, so the field would otherwise carry a
/// meaningless zero and the coordinator's log would not say what was too big.
fn refusal_response(refusal: &StoreRefusal, chunk_len: u64) -> StoreChunkResponse {
    match refusal {
        StoreRefusal::BudgetExhausted { staged, budget } => {
            refused(RefusalReason::BudgetExhausted, *staged, *budget)
        }
        StoreRefusal::RacingUpload => refused(RefusalReason::RacingUpload, 0, 0),
        StoreRefusal::OversizedForBudget { budget, .. } => {
            refused(RefusalReason::OversizedForBudget, chunk_len, *budget)
        }
        StoreRefusal::NotAccepting => refused(RefusalReason::NotAccepting, 0, 0),
        // A local staging area never produces this; it exists for a peer whose
        // build knows a reason ours does not.
        StoreRefusal::Unknown => refused(RefusalReason::Unspecified, 0, 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A chunk key of the shape `read_through` claims, and the set + metrics it
    /// claims into — the two things `PacerPeer` shares with the proxy, without the
    /// backend client and cache tier a whole `PacerPeer` would need.
    const A_CHUNK_KEY: &str = "bucket/key#16777216:0";

    #[tokio::test]
    async fn a_cancelled_read_through_fill_releases_its_key_and_counts_abandoned() {
        // The peer server's read-through used to claim this set with a bare
        // `HashSet::insert` and release it with a `.remove()` at the end of the
        // pump — a line that a dropped future (runtime shutdown) or an unwinding
        // panic never reaches, leaving that chunk key unfillable for the daemon's
        // lifetime with nothing in `/metrics` to show it. Aborting a spawned task
        // drops its future the same way: mid-poll, with no chance to run anything
        // past the last `.await`.
        let filling: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let metrics = Metrics::new().expect("a fresh registry must accept every metric");
        let (filling_task, metrics_task) = (Arc::clone(&filling), metrics.clone());

        let pump = tokio::spawn(async move {
            let _guard = FillGuard::for_fill(&filling_task, &metrics_task, A_CHUNK_KEY)
                .expect("claiming a fresh key must succeed");
            // Stands in for a pump still relaying backend bytes — it never reaches
            // the `guard.complete()` at the end of `pump_read_through`.
            std::future::pending::<()>().await;
        });
        while filling.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            metrics.fill_inflight.get(),
            1,
            "a peer read-through fill must be visible in pacer_fill_inflight, which it was not \
             while this path claimed the set directly"
        );

        pump.abort();
        assert!(
            pump.await.is_err_and(|e| e.is_cancelled()),
            "the pump must have been cancelled, not have run to completion"
        );
        assert!(
            filling.lock().unwrap().is_empty(),
            "the key must be released even though the pump never reached its own release"
        );
        assert_eq!(metrics.fill_inflight.get(), 0);
        assert_eq!(
            metrics.fill_abandoned.get(),
            1,
            "a cancelled peer fill must count as abandoned"
        );
    }

    #[test]
    fn a_client_fill_already_in_flight_refuses_the_read_through_claim() {
        // The reason the set is shared at all (ADR-0016/0017): one fill per chunk
        // key node-wide, whichever path got there first. A holder that read the
        // chunk through anyway would pay a second backend GET for bytes already
        // being fetched.
        let filling: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let metrics = Metrics::new().expect("a fresh registry must accept every metric");

        let client_fill = FillGuard::for_fill(&filling, &metrics, A_CHUNK_KEY)
            .expect("claiming a fresh key must succeed");
        assert!(
            FillGuard::for_fill(&filling, &metrics, A_CHUNK_KEY).is_none(),
            "a read-through must not fill a key a client GET is already filling"
        );

        drop(client_fill);
        assert!(
            FillGuard::for_fill(&filling, &metrics, A_CHUNK_KEY).is_some(),
            "once the first fill releases the key, the next claim must succeed"
        );
    }
}
