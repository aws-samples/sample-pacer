//! Cross-node cache-read transport.
//!
//! Scope per ADR-0003: peer transports exist ONLY to fetch cached blobs from
//! other nodes. Writes, replication, control plane, and backend traffic never
//! go through this trait.

use bytes::Bytes;
use futures::stream::BoxStream;
use pacer_ring::directory::{SharerSet, Tier};
use pacer_ring::NodeId;

// `announce`, `client_registry` and `token` are documented in their own `//!` headers, and
// deliberately NOT with a `///` here as well: rustdoc concatenates an outer doc on the
// declaration with the module's inner doc and then resolves the whole thing in THIS module's
// scope, so every intra-doc link the module makes to its own items ceases to resolve —
// `-D warnings` turns that into a failed `cargo doc` naming a line no span points at.
pub mod announce;
// Outside the `efa` gate for the same reason as `announce` and `token`: the bound, the eviction
// order and the TTL are the policy the `efa` client-edge maps are built from, and none of it
// needs a device — so it compiles and is tested in every build.
pub mod client_registry;
pub mod grpc;
pub mod rdma_device;
pub mod token;

#[cfg(feature = "efa")]
pub mod efa;

/// Byte range within a cached whole object, mirroring HTTP Range semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ByteRange {
    /// `bytes=first-` / `bytes=first-last` (inclusive).
    From {
        /// First byte offset (inclusive).
        start: u64,
        /// Last byte offset (inclusive); None = to end of object.
        end: Option<u64>,
    },
    /// `bytes=-N`: the last N bytes.
    Suffix {
        /// Number of trailing bytes.
        len: u64,
    },
}

/// Streamed blob body from a peer.
pub struct BlobStream {
    /// Length of the (possibly range-sliced) returned body.
    pub len: u64,
    /// Length of the whole cached object (Content-Range denominator).
    pub object_len: u64,
    /// Offset of the returned body within the whole object.
    pub body_start: u64,
    /// Backend ETag of the cached object.
    pub e_tag: Option<String>,
    /// Backend Content-Type of the cached object.
    pub content_type: Option<String>,
    /// Seconds since epoch, from the backend's Last-Modified.
    pub last_modified_epoch_secs: Option<i64>,
    /// The body bytes, in order; refcounted slices, no copies.
    pub chunks: BoxStream<'static, Result<Bytes, TransportError>>,
}

/// Why a peer operation did not return a blob.
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    /// Peer does not have the blob cached and could not (or was asked not to)
    /// read through. Not an error for the caller: fall through to the backend.
    #[error("blob not cached on peer")]
    NotCached,
    /// The requested range is unsatisfiable against the object (416).
    #[error("range not satisfiable")]
    RangeNotSatisfiable,
    /// The peer could not be dialed or the RPC failed in transit.
    #[error("peer unavailable: {0}")]
    PeerUnavailable(String),
    /// Any other failure. Callers must treat this as "fall back": to gRPC if
    /// this was an RDMA transport, to the backend otherwise (ADR-0003:
    /// automatic fallback on ANY error).
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// The seam between the cache tier and the wire (see 05-architecture.md).
/// gRPC implements it in Phase 2; EFA-RDMA in Phase 3 behind `feature = "efa"`.
#[async_trait::async_trait]
pub trait PeerTransport: Send + Sync {
    /// Fetch a cached blob (or a range of it) from a peer. The ONLY data
    /// operation (ADR-0003 — the EFA transport replaces just this).
    /// `no_fill` forwards the client's `Cache-Control: no-store` to the owner
    /// (serve, but don't populate — ADR-0012).
    ///
    /// # Errors
    ///
    /// Every [`TransportError`] variant means "fall back", never "fail the
    /// client": [`TransportError::NotCached`] and
    /// [`TransportError::RangeNotSatisfiable`] are protocol answers; the rest
    /// are transport failures.
    async fn fetch_blob(
        &self,
        peer: &NodeId,
        cache_key: &str,
        range: Option<ByteRange>,
        no_fill: bool,
    ) -> Result<BlobStream, TransportError>;

    /// Drop a key from a peer's cache (cluster read-after-write, ADR-0012).
    /// Control-plane: always gRPC, even when blobs move over RDMA.
    ///
    /// # Errors
    ///
    /// Transport failures only; the caller logs and continues (the backend
    /// mutation already happened).
    async fn invalidate(&self, peer: &NodeId, cache_key: &str) -> Result<(), TransportError>;

    /// Tell `home` (the chunk key's directory shard, ADR-0017) that `node`
    /// now holds `chunk_key` at `tier`, admission `generation`. Control-plane,
    /// always gRPC (mirrors [`Self::invalidate`]).
    ///
    /// # Errors
    ///
    /// Transport failures only; callers treat an announce as fire-and-forget
    /// (ADR-0017 soft state) — log and continue, never fail the caller's read.
    async fn announce_admit(
        &self,
        home: &NodeId,
        chunk_key: &str,
        node: &str,
        tier: Tier,
        generation: u64,
    ) -> Result<(), TransportError>;

    /// Tell `home` that `node` no longer holds `chunk_key` as of `generation`.
    ///
    /// # Errors
    ///
    /// Same as [`Self::announce_admit`].
    async fn announce_evict(
        &self,
        home: &NodeId,
        chunk_key: &str,
        node: &str,
        generation: u64,
    ) -> Result<(), TransportError>;

    /// Ask `home` who currently holds `chunk_key` (ADR-0017 v1 RPC lookup;
    /// ADR-0020's one-sided RDMA READ is the v2 end state on EFA nodes). A
    /// `None` sharer set is the directory miss path: the caller performs or
    /// delegates the read-through.
    ///
    /// # Errors
    ///
    /// Transport failures only; callers fall back to computing placement
    /// themselves (e.g. treating the request as an unconditional miss).
    async fn lookup_sharers(
        &self,
        home: &NodeId,
        chunk_key: &str,
    ) -> Result<Option<SharerSet>, TransportError>;

    /// Offer one window of a scattered write to its chunk's home, which uploads
    /// it as a part of `upload_id` with its own node credentials and stages the
    /// bytes (ADR-0032 § 2).
    ///
    /// The second data operation after [`Self::fetch_blob`], and the one the
    /// RDMA leg will replace (`planning/24` Phase 5) — ADR-0018's verb with the
    /// trigger inverted, since here the *sender* holds the bytes.
    ///
    /// # Errors
    ///
    /// Transport failures only. A [`StoreOutcome::Refused`] is a successful RPC
    /// with a negative answer, not an error, because refusal is the expected
    /// steady state on a busy fleet and is counted separately from breakage.
    /// Either way the coordinator uploads the window itself.
    async fn store_chunk(
        &self,
        owner: &NodeId,
        offer: StoreOffer<'_>,
    ) -> Result<StoreOutcome, TransportError>;

    /// Tell an owner that `upload_id` completed as version `e_tag`, so its
    /// staged chunks may become visible. Returns how many did.
    ///
    /// # Errors
    ///
    /// Transport failures only, and the caller ignores them: an uncommitted
    /// chunk is a miss, never a wrong answer, so this is fire-and-forget in the
    /// same sense as an ADR-0017 announce (and unlike an invalidation, which
    /// must be awaited because a holder that misses it serves stale bytes).
    async fn commit_upload(
        &self,
        owner: &NodeId,
        upload_id: &str,
        e_tag: &str,
    ) -> Result<u32, TransportError>;

    /// Tell an owner to drop `upload_id`'s staged chunks — Complete failed or the
    /// upload was aborted. Returns how many went.
    ///
    /// # Errors
    ///
    /// Transport failures only. An unreachable owner keeps its staged bytes
    /// until its TTL reaps them, which is why that reaper exists.
    async fn discard_upload(&self, owner: &NodeId, upload_id: &str) -> Result<u32, TransportError>;
}

/// One window offered to its chunk's home.
///
/// A struct rather than eight positional parameters: the fields travel together
/// for one window and several are same-typed strings that would be easy to
/// transpose at a call site.
#[derive(Debug, Clone)]
pub struct StoreOffer<'a> {
    /// Cache key the window is stored under (`"{bucket}/{key}#{size}:{index}"`).
    pub chunk_key: &'a str,
    /// The coordinator's multipart upload, which groups an owner's staged chunks.
    pub upload_id: &'a str,
    /// The **backend** bucket, already resolved through the coordinator's bucket
    /// map. Sending the resolved name stops the owner re-mapping an
    /// already-mapped bucket into a wrong target.
    pub bucket: &'a str,
    /// Object key this part belongs to.
    pub key: &'a str,
    /// S3 part number — the chunk's 0-based grid index plus one.
    pub part_number: i32,
    /// The window's bytes.
    pub body: Bytes,
    /// Base64 CRC32 of `body` in S3's `ChecksumCRC32` form, covering the
    /// coordinator→owner hop.
    pub checksum_crc32: &'a str,
}

/// What an owner did with an offered window.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreOutcome {
    /// Uploaded as a part and staged. Carries the part ETag the coordinator
    /// needs for `CompleteMultipartUpload`.
    Uploaded {
        /// The part's ETag, as S3 returned it.
        e_tag: String,
    },
    /// Declined, immediately. The coordinator uploads this window itself.
    Refused(StoreRefusal),
}

/// Why an owner declined a window (ADR-0032 § 4).
///
/// Lives here rather than in the daemon because it is part of the peer contract:
/// the reason is what tells a coordinator whether to keep offering to this owner,
/// so both ends have to agree on the taxonomy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoreRefusal {
    /// Staging would exceed the owner's byte budget — load, so a cooldown clears
    /// it. The coordinator should stop offering to this owner for a while: by the
    /// time an owner can refuse it has already received the bytes, so re-offering
    /// every window wastes a transfer per window instead of one per owner.
    BudgetExhausted {
        /// Bytes the owner had staged when it refused.
        staged: u64,
        /// The owner's configured ceiling.
        budget: u64,
    },
    /// Another upload already has this chunk key staged — two writers racing one
    /// key, out of contract in v1.
    RacingUpload,
    /// The window exceeds the owner's whole budget: a misconfiguration, which no
    /// cooldown clears. Kept distinct from [`Self::BudgetExhausted`] so a
    /// coordinator does not wait out load that was never the problem.
    OversizedForBudget {
        /// The window's length.
        chunk_len: u64,
        /// The owner's configured ceiling.
        budget: u64,
    },
    /// The owner takes no scattered writes at all — feature off, or its backend
    /// is not a general-purpose bucket (ADR-0032 § 6).
    NotAccepting,
    /// A reason this build does not know, from a newer peer. Treated as
    /// permanent, because guessing "transient" would risk a cooldown loop.
    Unknown,
}

impl StoreRefusal {
    /// Whether offering to this owner again could succeed. `false` for a
    /// misconfiguration or a disabled peer, which no amount of waiting clears.
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::BudgetExhausted { .. } | Self::RacingUpload)
    }
}

/// Largest peer message this daemon will send or accept, in either direction.
///
/// Sized by `StoreChunk`, which is the only message carrying a whole
/// `chunk_size` window — every other one is metadata or a blob frame far below
/// tonic's 4 MiB default, which a 16 MiB window would otherwise blow straight
/// through.
///
/// 80 MiB clears the largest chunk size ever benched (64 MiB, planning/14) with
/// room for the request's small string fields. It is a *bound*, not a target: what
/// one node can be made to allocate is this times the peer server's concurrency,
/// and that product is exactly what the staging budget cannot cover, because
/// staging can only refuse a window after gRPC has already received it. A
/// `chunk_size` too large to fit here makes the scatter decline rather than fail —
/// see `pacer_daemon::scatter::worth_scattering`.
pub const MAX_PEER_MESSAGE_BYTES: usize = 80 << 20;

/// HTTP/2 receive-window sizes for the peer plane, in bytes.
///
/// # The gap this exists to close
///
/// [`MAX_PEER_MESSAGE_BYTES`] only *permits* a 16 MiB `StoreChunk`; it says nothing about how
/// fast one may cross. That is flow control, and the peer plane has been running on
/// hyper's defaults, which are **asymmetric by direction**:
///
/// | | connection | stream |
/// |---|---|---|
/// | what a hyper **server** advertises (so: what a `StoreChunk` SENDER gets) | 1 MiB | 1 MiB |
/// | what a hyper **client** advertises (so: what a `FetchBlob` body gets) | 5 MiB | 2 MiB |
///
/// A cached channel multiplexes every concurrent RPC to one peer onto ONE HTTP/2 connection,
/// so the connection row is a per-connection total shared by every in-flight window, not a
/// per-window allowance — and with [`DEFAULT_PEER_CONNECTIONS`] there is one such connection
/// per peer, which is the other half of this ceiling and its own knob.
/// `results/laguna-store-remotewrite.md` measured the save's
/// `StoreChunk` leg at **0.61 GiB/s per node** against `planning/08`'s **1.75 GiB/s** for
/// `FetchBlob` on this same transport — a 2.9x gap in the same direction as, and the same
/// order as, the 5x gap between the two connection windows above. That is a correspondence,
/// not a proof, and it is what these knobs exist to test.
///
/// # Why both default to `None`
///
/// `None` leaves hyper's own default in place, so merging this changes no behaviour and
/// costs no memory. A window is receive credit the receiver must be prepared to buffer:
/// raising it raises what one peer can make this node hold before the application reads it,
/// which is memory the ADR-0032 staging budget cannot refuse (it only sees a window after
/// gRPC has received it, the same limitation [`MAX_PEER_MESSAGE_BYTES`] documents). So the
/// value that wins an arm has to be published into `memory_budget` in the same change that
/// makes it a default — which is a separate commit, on evidence.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct H2Windows {
    /// Per-stream receive window. Sized to one whole message, a `StoreChunk` is never
    /// ack-limited mid-body.
    pub stream_bytes: Option<u32>,
    /// Per-connection receive window, shared by every concurrent RPC to that peer — so this
    /// is the one a fan-out of `StoreChunk`s to a single owner contends on.
    pub connection_bytes: Option<u32>,
}

impl H2Windows {
    /// Whether either window is set, i.e. whether anything overrides hyper's defaults.
    #[must_use]
    pub fn is_set(&self) -> bool {
        self.stream_bytes.is_some() || self.connection_bytes.is_some()
    }
}

/// HTTP/2 connections `GrpcTransport` opens per peer.
///
/// One, which is what a single cached channel has always given, so the default changes
/// nothing and is the control arm.
///
/// # Why one is a suspect at all
///
/// A tonic channel multiplexes every concurrent RPC to a peer onto ONE HTTP/2 connection, so
/// with a pool of one the whole peer plane between two nodes is a single TCP flow. That makes
/// three separate ceilings coincide on it, and `results/laguna-disk4096-and-control-rot.md`
/// shows the save sitting on one of them:
///
/// * **EC2 caps a single VPC flow** — 5 Gbps, or 10 Gbps between instances in one cluster
///   placement group. EFA does not carry gRPC, so the peer plane gets no rails.
/// * **One HTTP/2 connection is driven by one task**, so all framing and every copy for
///   16 MiB `StoreChunk` messages to one owner runs on one core however many vCPUs the node
///   has.
/// * **Flow-control credit is per connection** ([`H2Windows`]), so concurrent windows to one
///   owner share one allowance.
///
/// The measurement that makes this concrete: that arm shipped 276.6 GiB per node over the hop
/// in 457.563 s = **0.6045 GiB/s = 5.19 Gbps**, within 4 % of the 5 Gbps single-flow cap, and
/// its `owner_rpc` of 2.919 s is what Little's Law predicts for ~111 windows in flight against
/// a pipe that fixed (111 × 16 MiB ÷ 0.6045 GiB/s = 2.86 s). A rate that does not move when
/// concurrency doubles — `windowsInFlight` 64 → 128 was a null — is a fixed-rate pipe, not a
/// latency.
///
/// Raising this splits the fan-out over N flows and N framing tasks. It does *not* address the
/// third bullet on its own: credit is per connection, so N connections multiply the allowance
/// without widening any one of it. The two knobs are therefore separate arms and the
/// discriminator between the first two ceilings and the third.
pub const DEFAULT_PEER_CONNECTIONS: usize = 1;

/// Most connections one peer may be given.
///
/// Every connection is a TCP socket, an HTTP/2 state machine and its own receive-window
/// allowance on both ends, and a DaemonSet dials **every** other node — so the node-wide cost
/// is this times the peer count, twice (dialed and accepted). 32 covers a pool wide enough to
/// clear any per-flow cap by an order of magnitude while keeping a fat-fingered value from
/// quietly becoming thousands of sockets. Refused at configuration time rather than at first
/// contact, where it would surface as a peer that cannot be dialed.
pub const MAX_PEER_CONNECTIONS: usize = 32;
