//! Resolving one chunk, and the single flight that stops two resolutions doing
//! the same backend read (ADR-0040).
//!
//! [`FillCtx`] is everything a single chunk resolution needs, shared (via `Arc`)
//! across the read path's bounded look-ahead pipeline so each in-flight
//! resolution clones only a refcount. Its tier order is ADR-0016's: a local hit,
//! then — for a chunk this node does not home — a peer (layer 2's R co-homes,
//! ordered by [`super::cluster::chunk_sources`]), then a backend ranged GET of
//! exactly that chunk's bounds (ADR-0015). A fill records this node in the
//! directory (ADR-0017) and puts the bytes wherever [`ChunkFill`] says
//! (ADR-0028), and a backend read retries under [`pacer_backend::retry`] because
//! by then the client's `200` has already gone out.
//!
//! # The two invariants, and why they are not the same one
//!
//! **At most one fill per chunk key node-wide, and a claim that is always
//! released.** [`FillGuard`] holds the key in the shared [`FillRegistry`] and
//! frees it in `Drop` — which is why it exists at all: a manual `.remove()` is
//! skipped by exactly the two things that happen most (a client that disconnects
//! mid-stream, an early `?`), and the key then stayed claimed for the daemon's
//! lifetime with no metric to show it. The peer server's read-through claims
//! through this same type ([`crate::peer`]), so the two fill paths cannot
//! disagree about who holds a key.
//!
//! **At most one backend read per chunk key node-wide** (ADR-0040) is a
//! *different* claim, and until that ADR only the first one held. The guard used
//! to be taken *after* the backend read, from inside `maybe_fill`, so N clients
//! arriving on one cold chunk each issued their own ranged GET and then N-1 of
//! them found the key claimed and skipped the insert: the writes were deduped,
//! the reads were not. Now the home's read claims *first*
//! ([`FillCtx::fetch_owned`]), and a second arrival is handed the leader's bytes
//! rather than a second GET.
//!
//! The registry entry is a three-state [`FillState`] rather than a bare key
//! because a second arrival has to be able to tell three situations apart: bytes
//! are coming (wait), bytes are already here (take them), and nothing will ever
//! be published (fetch your own). The third is what keeps this change small: the
//! peer server's read-through and the layer-1 admit still claim exclusively, so
//! they behave exactly as they did and nothing waits on them.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use bytes::{Bytes, BytesMut};
use futures::StreamExt;
use pacer_backend::retry::{BackendReadError, BackendReadErrorKind, ChunkRead, RetryPolicy};
use pacer_cache::chunk::{CachedChunk, ChunkConfig};
use pacer_cache::tier::ChunkTier;
use pacer_cache::ReadDecision;
use pacer_ring::directory::Tier;
use pacer_transport::TransportError;
use prometheus::{IntCounter, IntGauge};
use s3s::{s3_error, S3Result};
use tokio::sync::broadcast;
use tracing::{trace, warn};

use crate::cachefill::ChunkFill;
use crate::metrics::Metrics;

use super::cluster::{chunk_sources, is_home, Cluster};
use super::PacerProxy;

/// `outcome` label values of `pacer_backend_read_failures_total`. Kept as consts
/// for the same reason as the `source` labels above: a dashboard depends on the
/// exact string.
///
/// Every allowed attempt hit a retryable fault — the backend was unhealthy for
/// the whole backoff window, not momentarily.
const READ_FAILURE_EXHAUSTED: &str = "exhausted";
/// A fault retrying cannot fix (a `4xx` other than throttling): this daemon's
/// credentials, or the request it asked for.
const READ_FAILURE_PERMANENT: &str = "permanent";

/// Everything a chunk resolution needs, shared (via `Arc`) across the bounded
/// look-ahead pipeline so each in-flight resolution clones only a refcount.
pub(super) struct FillCtx {
    pub(super) tier: ChunkTier,
    pub(super) backend: aws_sdk_s3::Client,
    pub(super) chunk: ChunkConfig,
    pub(super) cluster: Option<Cluster>,
    pub(super) metrics: Metrics,
    /// Where a cached chunk's bytes go (ADR-0028), cloned from the proxy so this
    /// path and the peer server's read-through cannot disagree.
    pub(super) fill: ChunkFill,
    /// Per-chunk-key fill claims, shared with the peer server (one fill per key
    /// node-wide).
    pub(super) filling: FillRegistry,
    /// Whether a home's missed chunk read joins an in-flight read of the same key
    /// instead of issuing its own (ADR-0040). Cloned from the proxy; `false` is the
    /// pre-ADR-0040 path, byte for byte.
    pub(super) coalesce: bool,
    /// How hard each chunk's backend read tries, cloned from the proxy.
    pub(super) read_retry: RetryPolicy,
    pub(super) object_key: String,
    pub(super) bucket: String,
    pub(super) key: String,
    pub(super) object_len: u64,
    /// `true` when a completed fetch of a missed chunk should be inserted
    /// (`CacheAndFill` + size-admitted); `false` bypasses the fill (`no-store`
    /// or a below-`min`/above-`max` object).
    pub(super) admit: bool,
    /// Forwarded to the owner on a peer fetch (`Cache-Control: no-store`).
    pub(super) no_fill: bool,
    /// Whether a remote chunk bound for a **client-registered** window is written by its
    /// HOLDER (planning/19 C3) instead of arriving here and being written on from here.
    ///
    /// Cloned from `delivery.remote_write` rather than reached through the proxy, for the
    /// same reason every other field here is: a chunk resolution holds an `Arc<FillCtx>` and
    /// nothing else. It is a *control-arm* switch, not a safety valve — see
    /// [`crate::delivery::DeliveryConfig::remote_write`].
    ///
    /// Absent without the RDMA plane, like the `cuda` handle on the proxy: a client token is
    /// not even reachable there (`open_client_memory` degrades it), so a build with no way to
    /// write client memory has no second path to choose between.
    #[cfg(feature = "efa")]
    pub(super) remote_write: bool,
}

impl PacerProxy {
    /// Everything one GET's chunk resolutions share, ready to be handed to each
    /// in-flight resolution as a refcount.
    ///
    /// Assembled here rather than at the read path's call site because every field
    /// but two is a copy of a proxy field, and those two — `admit` and `no_fill` —
    /// are this read's [`ReadDecision`] restated. Deriving them in one place is what
    /// keeps "a `no-store` read populates nothing" (ADR-0012) from being a claim two
    /// call sites have to agree on.
    pub(super) fn fill_ctx(
        &self,
        object_key: String,
        bucket: String,
        key: String,
        object_len: u64,
        decision: ReadDecision,
    ) -> Arc<FillCtx> {
        Arc::new(FillCtx {
            tier: self.tier.clone(),
            backend: self.backend.clone(),
            chunk: self.chunk,
            cluster: self.cluster.clone(),
            metrics: self.metrics.clone(),
            fill: self.fill.clone(),
            filling: self.filling.clone(),
            coalesce: self.fill_coalesce,
            read_retry: self.read_retry,
            object_key,
            bucket,
            key,
            object_len,
            admit: decision == ReadDecision::CacheAndFill,
            no_fill: decision == ReadDecision::CacheNoFill,
            #[cfg(feature = "efa")]
            remote_write: self.delivery.remote_write,
        })
    }
}

/// Published fills one waiter may fall behind before the value is lost.
///
/// One, because a leader sends exactly once: every waiter holds a receiver taken
/// before that send, and the value's whole lifetime is the one slot. `broadcast`
/// rejects a capacity of zero, and anything above one would only reserve room for
/// a second send that cannot happen.
const FILL_BROADCAST_CAPACITY: usize = 1;

/// What one claimed chunk key is doing, and therefore what a second arrival
/// should do about it.
///
/// The distinction that matters is between the last variant and the other two: a
/// claim that will never publish anything must not be waited on, or a request
/// would block on a fill whose bytes it is never going to see.
enum FillState {
    /// A leader is reading this chunk from the backend and will publish the bytes
    /// to this channel. Subscribe and wait.
    Fetching(broadcast::Sender<Bytes>),
    /// The leader has published and has not yet finished inserting. Its bytes are
    /// parked here so an arrival inside that window is served immediately instead
    /// of subscribing to a channel that has already been sent to — `broadcast`
    /// buffers nothing for a receiver that did not exist at send time, so without
    /// this the request would wait and then fall back for no reason.
    Filled(Bytes),
    /// Claimed to serialise a *write* only: the peer server's read-through
    /// ([`crate::peer`]) and the layer-1 admit of peer-fetched bytes
    /// ([`FillCtx::maybe_admit_local`]) both already hold their bytes, so there is
    /// no backend read to share. A second arrival fetches its own, exactly as it
    /// did before ADR-0040.
    Exclusive,
}

/// The node-wide record of which chunk keys are being filled right now, and by
/// whom (ADR-0040). Cheap to clone — every fill path holds one, and the proxy
/// hands the same registry to the peer server so the two cannot disagree.
///
/// `std::sync::Mutex`, not tokio's, and that is a property rather than a
/// preference: every critical section here is one hash lookup with no `.await`
/// inside it. A waiter receives its `broadcast::Receiver` from `claim_fill` and
/// does its waiting *after* the lock is released, so the lock is never held
/// across a suspension point.
///
/// Every method but [`Self::new`] is `pub(crate)` or private — this type is `pub`
/// only because `main` hands one from the proxy to the peer server — so the doc
/// above names `claim_fill` in plain backticks rather than linking it: a public
/// item may not intra-doc-link a private one (`rustdoc::private_intra_doc_links`,
/// fatal under the workspace's `-D warnings`).
#[derive(Clone, Default)]
pub struct FillRegistry {
    /// One entry per claimed key; absent means nobody is filling it.
    claims: Arc<Mutex<HashMap<String, FillState>>>,
}

/// What a would-be filler of one chunk key got when it asked
/// ([`FillRegistry::claim_fill`]).
pub(crate) enum FillClaim {
    /// Nobody else was filling this key. This caller leads: read the backend,
    /// publish, insert.
    Lead(FillGuard),
    /// A leader is mid-read. Await these bytes instead of issuing a second GET.
    Follow(broadcast::Receiver<Bytes>),
    /// A leader already published, and this caller landed before the claim was
    /// released. Take the bytes; there is nothing to wait for.
    Ready(Bytes),
    /// The key is claimed by a path that publishes nothing ([`FillState::Exclusive`]).
    /// Fetch your own bytes, exactly as every caller did before ADR-0040.
    Busy,
}

impl FillRegistry {
    /// An empty registry. One per daemon, shared by every fill path.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Claim `key` for a **publishing** fill, or report who already holds it.
    ///
    /// The whole of ADR-0040's single flight is this one lookup: whoever finds the
    /// key absent leads and everyone else is routed to the leader's bytes. Raises
    /// [`Metrics::fill_inflight`] on a successful claim, like the exclusive path,
    /// so the gauge keeps counting every claim however it was taken.
    pub(crate) fn claim_fill(&self, metrics: &Metrics, key: &str) -> FillClaim {
        let mut claims = self.claims.lock().unwrap();
        match claims.get(key) {
            Some(FillState::Fetching(tx)) => return FillClaim::Follow(tx.subscribe()),
            Some(FillState::Filled(bytes)) => return FillClaim::Ready(bytes.clone()),
            Some(FillState::Exclusive) => return FillClaim::Busy,
            None => {}
        }
        // The receiver `channel` hands back is dropped immediately: waiters get
        // their own through `subscribe`, and a leader with no waiters must not be
        // kept from finishing by a receiver nobody is polling.
        let (tx, _rx) = broadcast::channel(FILL_BROADCAST_CAPACITY);
        claims.insert(key.to_owned(), FillState::Fetching(tx));
        drop(claims);
        FillClaim::Lead(FillGuard::new(self, key, metrics))
    }

    /// Claim `key` for exclusion only — no bytes will be published, so a second
    /// arrival is told to fetch its own ([`FillClaim::Busy`]). `false` if the key
    /// is already claimed by anyone, on any path.
    ///
    /// This is the pre-ADR-0040 claim, unchanged in meaning, and it is what the
    /// peer server's read-through and the layer-1 admit still take: both already
    /// hold the bytes they are about to insert, so there is no read to share and
    /// making a waiter block on them would only couple one request's latency to
    /// another's for nothing.
    fn claim_exclusive(&self, key: &str) -> bool {
        let mut claims = self.claims.lock().unwrap();
        if claims.contains_key(key) {
            return false;
        }
        claims.insert(key.to_owned(), FillState::Exclusive);
        true
    }

    /// Hand `bytes` to every waiter on `key`, and leave them in the claim for
    /// anyone who arrives before it is released.
    ///
    /// A `send` with no live receiver is not a failure — it is the ordinary case
    /// of a fill nobody else asked for — so its `Err` is dropped. That is exactly
    /// why the bytes are also parked in the entry: `broadcast` keeps nothing for a
    /// receiver created after the send, and the window between this call and
    /// [`Self::release`] is real (a `put_chunk` wide).
    fn publish(&self, key: &str, bytes: &Bytes) {
        let mut claims = self.claims.lock().unwrap();
        let Some(state) = claims.get_mut(key) else {
            return;
        };
        // `&*state`: `send` needs only a shared borrow, which ends with this block
        // and so leaves the entry free to be replaced on the next line.
        if let FillState::Fetching(tx) = &*state {
            let _ = tx.send(bytes.clone());
        } else {
            return;
        }
        *state = FillState::Filled(bytes.clone());
    }

    /// Drop `key`'s claim. Reached from [`FillGuard::drop`] alone, which is what
    /// makes "always released" true on every path — including a future dropped
    /// mid-fill, where nothing else would have run.
    fn release(&self, key: &str) {
        self.claims.lock().unwrap().remove(key);
    }

    /// Whether `key` is claimed by anyone right now. Tests only — `peer.rs`'s as well
    /// as this module's, hence `pub(crate)`: the fill paths learn this from the claim
    /// they took, never by asking.
    #[cfg(test)]
    pub(crate) fn is_claimed(&self, key: &str) -> bool {
        self.claims.lock().unwrap().contains_key(key)
    }

    /// How many keys are claimed right now. Tests only, for the same reason.
    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.claims.lock().unwrap().len()
    }
}

/// Await the bytes a leader promised, or `None` if it never delivered them.
///
/// `None` covers every way a leader can fail to publish — its read errored, or
/// its future was dropped when the client that triggered it disconnected — because
/// both reach a waiter identically: [`FillGuard::drop`] releases the claim, the
/// last `Sender` goes with it, and the channel closes. The caller's answer to all
/// of them is the same and is exactly the pre-ADR-0040 behaviour: fetch your own.
///
/// `Lagged` cannot occur against a single send into a one-slot channel, and is
/// folded into the same fallback rather than given a branch that no run reaches.
async fn await_published_fill(
    mut waiter: broadcast::Receiver<Bytes>,
    waiters: &IntGauge,
) -> Option<Bytes> {
    let _parked = WaiterTicket::new(waiters);
    waiter.recv().await.ok()
}

/// Holds `pacer_fill_waiters` up for exactly as long as one request is parked on
/// another's fill — including when that request is cancelled mid-wait.
///
/// A `Drop` guard rather than an `inc()`/`dec()` pair around the `.await`, for the
/// same reason [`FillGuard`] is one: `chunked_body`'s pipeline drops a chunk
/// resolution's future outright when its client disconnects, so a `dec()` written
/// after the await is precisely the line that never runs. A gauge that can only go
/// up is worse than no gauge, because it reads as the incident it is meant to
/// detect.
struct WaiterTicket(IntGauge);

impl WaiterTicket {
    /// Raise the gauge, and hand back the ticket that lowers it.
    fn new(gauge: &IntGauge) -> Self {
        gauge.inc();
        Self(gauge.clone())
    }
}

impl Drop for WaiterTicket {
    fn drop(&mut self) {
        self.0.dec();
    }
}

/// RAII slot in the node-wide [`FillRegistry`] (shared by `maybe_admit_local`,
/// `maybe_fill`, [`FillCtx::fetch_owned`]'s leader and the peer server's own
/// read-through fill): while a guard for `key` is alive, a second claim on the
/// same key cannot lead, so at most one fill per key runs at a time
/// (ADR-0016/0017).
///
/// **Why this replaces a bare `HashSet::insert`/`.remove()` pair.**
/// `chunked_body`'s `stream::buffered` pipeline drops an in-flight chunk
/// resolution's future outright when the client disconnects
/// (`tx.send(..).await.is_err()` returns before any further `.await` runs),
/// and an early `return`/`?` elsewhere skips the same way. A manual `.remove()`
/// placed after the fill never runs on either path, so the key stayed in
/// `filling` forever — silently, with no metric — and that chunk could never
/// be filled again until the daemon restarted. `Drop` cannot be skipped by a
/// dropped future or an early return, so the key is always released.
pub(crate) struct FillGuard {
    /// The shared registry this guard holds one key in.
    registry: FillRegistry,
    /// The claimed key, owned so `Drop` needs no borrow.
    key: String,
    /// Set by [`Self::complete`] once the fill finished on its own (success or
    /// a logged, non-cancellation failure). `false` at drop means the guard's
    /// future was cut short instead, which increments
    /// [`Metrics::fill_abandoned`].
    completed: bool,
    /// [`Metrics::fill_inflight`], adjusted alongside the set.
    inflight: IntGauge,
    /// [`Metrics::fill_abandoned`], incremented on drop iff `!completed`.
    abandoned: IntCounter,
}

impl FillGuard {
    /// Wrap an already-taken claim on `key`, raising `inflight` by one.
    ///
    /// Private, and takes no decision: whoever calls it has just won the claim in
    /// the registry, and the only way to reach one of those is through
    /// [`FillRegistry::claim_fill`] or [`Self::for_fill`]. Splitting the claim from
    /// the guard is what lets the two claim *kinds* share one release path.
    fn new(registry: &FillRegistry, key: &str, metrics: &Metrics) -> Self {
        metrics.fill_inflight.inc();
        Self {
            registry: registry.clone(),
            key: key.to_owned(),
            completed: false,
            inflight: metrics.fill_inflight.clone(),
            abandoned: metrics.fill_abandoned.clone(),
        }
    }

    /// Claim `key` **exclusively** — no bytes published — or `None` if another
    /// fill for it is already in flight. The pre-ADR-0040 claim, and still the
    /// right one for a caller that already holds its bytes.
    ///
    /// The registry is node-wide and this claim is path-agnostic on purpose: a
    /// client GET's fill and the peer server's read-through fill of the same chunk
    /// key are the same work, and the second must skip rather than duplicate the
    /// insert (ADR-0016/0017). Both therefore claim through here, and both count
    /// into `pacer_fill_inflight` / `pacer_fill_abandoned_total`.
    #[must_use]
    pub(crate) fn for_fill(registry: &FillRegistry, metrics: &Metrics, key: &str) -> Option<Self> {
        if !registry.claim_exclusive(key) {
            return None;
        }
        Some(Self::new(registry, key, metrics))
    }

    /// Hand the leader's freshly-read bytes to everyone waiting on this key, and
    /// park them in the claim for anyone who arrives before it is released
    /// (ADR-0040). A no-op on an exclusive claim, which publishes nothing.
    ///
    /// Separate from [`Self::complete`] rather than folded into it, because the two
    /// answer different questions and the order matters: waiters are released as
    /// soon as the bytes exist, while `complete` means the *whole* fill — insert
    /// included — finished, and merging them would make
    /// [`Metrics::fill_abandoned`] stop counting a fill abandoned between the
    /// publish and the insert.
    pub(crate) fn publish(&self, bytes: &Bytes) {
        self.registry.publish(&self.key, bytes);
    }

    /// Mark the fill as having finished on its own rather than been cut short
    /// — suppresses [`Metrics::fill_abandoned`] when this guard drops.
    pub(crate) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for FillGuard {
    fn drop(&mut self) {
        // Releasing the entry also drops the last `broadcast::Sender` for it, which
        // is what wakes a waiter whose leader never published: the channel closes
        // and `await_published_fill` returns `None`. So a leader whose future was
        // dropped mid-read costs its followers one wake-up, not a hang.
        self.registry.release(&self.key);
        self.inflight.dec();
        if !self.completed {
            self.abandoned.inc();
        }
    }
}

impl FillCtx {
    /// Claim `chunk_key` exclusively, or `None` if another fill for it is already
    /// running. See [`FillGuard`] for why this replaces the bare `HashSet::insert`
    /// the fill sites used to do directly.
    #[must_use]
    fn try_begin_fill(&self, chunk_key: &str) -> Option<FillGuard> {
        FillGuard::for_fill(&self.filling, &self.metrics, chunk_key)
    }
}

impl FillCtx {
    /// Resolve one chunk's full bytes: local cache hit, else (cluster) a fetch
    /// from the chunk's owning peer, else a backend ranged GET of the chunk's
    /// bounds. A successful backend fetch of a missed chunk is inserted into
    /// the cache when admitted; a fill failure NEVER fails the read (the bytes
    /// are already in hand — log and serve them anyway).
    ///
    /// # Errors
    ///
    /// Only a backend fetch failure (no bytes to serve) errors; peer failures
    /// fall back to the backend and are never client-visible.
    pub(super) async fn resolve_chunk(&self, idx: u64) -> S3Result<Bytes> {
        let chunk_key = self.chunk.chunk_key(&self.object_key, idx);
        if let Ok(Some(c)) = self.tier.get_chunk(&chunk_key).await {
            self.metrics.cache_hits.inc();
            self.metrics.bytes_from_cache.inc_by(c.body.len() as u64);
            return Ok(c.body);
        }
        // A chunk this node co-homes (or single-node) is read through the
        // backend and filled here (ADR-0016 layer 2: all R homes fill).
        let owns = self.owns_chunk(&chunk_key);
        trace!(chunk_key = %chunk_key, owns, local_node = ?self.cluster.as_ref().map(|c| c.local_node.as_str()), "resolve_chunk ownership decision");
        if owns {
            return self.fetch_owned(idx, &chunk_key).await;
        }
        // Not a home: fetch from a peer. On success, layer 1 (ADR-0016) may
        // admit a local copy once the chunk proves hot. A peer failure falls
        // back to a no-fill backend GET.
        if let Some(bytes) = self.fetch_from_peer(&chunk_key).await {
            self.maybe_admit_local(&chunk_key, &bytes).await;
            return Ok(bytes);
        }
        self.fetch_from_backend(idx, &chunk_key, false).await
    }

    /// The home's own read of a missed chunk, under ADR-0040's single flight: one
    /// backend read per chunk key node-wide, with every other request for the same
    /// key served from it.
    ///
    /// Reached for a chunk this node co-homes and for **every** chunk on a
    /// single-node daemon, which is the shape the fan-in actually has — N ranks of
    /// one job loading one checkpoint through one daemon.
    ///
    /// The two non-leading fallbacks are deliberately the *same* call the whole
    /// path used to make, `self.admit` and all: a claim held by a non-publishing
    /// path, and a leader that delivered nothing, both leave this request exactly
    /// where it was before this ADR. So the mechanism can remove a backend read and
    /// cannot add one.
    ///
    /// # Errors
    ///
    /// Whatever [`Self::fetch_from_backend`] fails with — this adds no failure of
    /// its own, because a waiter that is let down falls back rather than erroring.
    pub(super) async fn fetch_owned(&self, idx: u64, chunk_key: &str) -> S3Result<Bytes> {
        if !self.coalesce {
            return self.fetch_from_backend(idx, chunk_key, self.admit).await;
        }
        match self.filling.claim_fill(&self.metrics, chunk_key) {
            FillClaim::Lead(guard) => self.fetch_leading(idx, chunk_key, guard).await,
            FillClaim::Ready(bytes) => {
                self.note_coalesced(&bytes);
                Ok(bytes)
            }
            FillClaim::Follow(waiter) => {
                match await_published_fill(waiter, &self.metrics.fill_coalesce.waiters).await {
                    Some(bytes) => {
                        self.note_coalesced(&bytes);
                        Ok(bytes)
                    }
                    None => {
                        self.metrics.fill_coalesce.fallbacks.inc();
                        self.fetch_from_backend(idx, chunk_key, self.admit).await
                    }
                }
            }
            FillClaim::Busy => self.fetch_from_backend(idx, chunk_key, self.admit).await,
        }
    }

    /// Lead one chunk's fill: read it from the backend, hand the bytes to every
    /// request that asked for the same chunk while the read was in flight, then
    /// insert.
    ///
    /// **Publish before the insert**, on purpose. A waiter needs the bytes, not a
    /// cache entry, and `put_chunk` is a device write that can be slow or fail —
    /// making waiters wait for it would spend the latency this exists to save, and
    /// a failed insert would strand them for nothing.
    ///
    /// **The published value is the backend's `Bytes`, never `cached_bytes`'
    /// output.** On an ADR-0028 node that output is a registered slab frame, and
    /// handing N waiters a refcount on one frame would pin it until the slowest of
    /// them finished streaming — the frame pool is bounded and sized for the
    /// resident set, not for readers in flight.
    ///
    /// `!admit` (a `no-store` read, ADR-0012) still leads and still publishes: it
    /// populates nothing, which is what that decision requires, and a read that is
    /// already served from the cache when the chunk happens to be there cannot
    /// object to being served from a read that is already in flight.
    ///
    /// # Errors
    ///
    /// The backend read's, unchanged — see [`Self::fetch_from_backend`].
    async fn fetch_leading(
        &self,
        idx: u64,
        chunk_key: &str,
        mut guard: FillGuard,
    ) -> S3Result<Bytes> {
        let body = self.read_backend_chunk(idx, chunk_key).await?;
        guard.publish(&body);
        if self.admit {
            self.insert_filled(chunk_key, &body).await;
        }
        guard.complete();
        Ok(body)
    }

    /// Count one read served from another request's in-flight fill (ADR-0040) —
    /// the pair of series that makes the mechanism visible at all.
    ///
    /// The byte counter is the one to read: it is backend traffic this node did
    /// **not** issue, which is the whole claim, and `fill_coalesced_total` alone
    /// cannot say it because chunks differ in size (ADR-0015 clamps the last one).
    fn note_coalesced(&self, bytes: &Bytes) {
        self.metrics.fill_coalesce.served.inc();
        self.metrics.fill_coalesce.bytes.inc_by(bytes.len() as u64);
    }

    /// Whether this node is one of `chunk_key`'s R co-homes (ADR-0016 layer 2)
    /// — always true single-node (no cluster) or when the ring has no owner
    /// yet. A home reads the chunk through and fills it; a non-home fetches
    /// from a home and (layer 1) may admit locally once the chunk proves hot.
    pub(super) fn owns_chunk(&self, chunk_key: &str) -> bool {
        match &self.cluster {
            None => true,
            Some(cluster) => is_home(cluster, chunk_key),
        }
    }

    /// Fetch a chunk from a peer that holds it, trying sources in preference
    /// order and advancing past any that fail or lack the chunk. `None` means
    /// "fall back to the backend": no source had it cached or was reachable.
    /// Only called for chunks this node does not home.
    ///
    /// Sources (ADR-0016 layer 2): the chunk's R co-homes, ordered from a
    /// per-requester offset (a hash of this node's name and the chunk key) so
    /// the cluster-wide readers of one hot chunk fan out across the R homes
    /// instead of converging on `ranked[0]`. A `NotCached` answer just advances
    /// to the next source.
    async fn fetch_from_peer(&self, chunk_key: &str) -> Option<Bytes> {
        let cluster = self.cluster.as_ref()?;
        let sources = chunk_sources(cluster, chunk_key);
        for source in &sources {
            match cluster
                .transport
                .fetch_blob(source, chunk_key, None, self.no_fill)
                .await
            {
                Ok(blob) => match collect_blob(blob).await {
                    Ok(bytes) => {
                        self.metrics.peer_fetches.inc();
                        self.metrics.bytes_from_peers.inc_by(bytes.len() as u64);
                        return Some(bytes);
                    }
                    Err(e) => {
                        self.metrics.peer_fallbacks.inc();
                        warn!(key = %chunk_key, source = %source.name(), error = %e,
                            "peer chunk stream failed; trying next source / backend");
                    }
                },
                // A source that hasn't filled this chunk yet: try the next one.
                Err(TransportError::NotCached) => {}
                Err(e) => {
                    self.metrics.peer_fallbacks.inc();
                    warn!(key = %chunk_key, source = %source.name(), error = %e,
                        "peer chunk fetch failed; trying next source / backend");
                }
            }
        }
        None
    }

    /// Feed a successful peer fetch to the layer-1 admission gate (ADR-0016);
    /// on admission, insert the chunk locally and announce this node as a new
    /// holder to the chunk's directory home (ADR-0017 remote-announce). A
    /// no-store read never admits (it must populate nothing, ADR-0012).
    async fn maybe_admit_local(&self, chunk_key: &str, data: &Bytes) {
        if self.no_fill {
            return;
        }
        let Some(cluster) = &self.cluster else {
            return;
        };
        let Some(generation) = cluster.admission.admit(chunk_key, data.len() as u64) else {
            return;
        };
        // Guarded insert: one fill per key node-wide (shared with owner fills
        // and the peer server). A closed slot means another fill is running —
        // skip, the bytes are already served. The guard's Drop releases the
        // slot even if this future is dropped before `guard.complete()` runs
        // (see FillGuard).
        let Some(mut guard) = self.try_begin_fill(chunk_key) else {
            return;
        };
        // Copy out of `data` before retaining it. On the RDMA zero-copy serve
        // path `data` may be a `Bytes` that OWNS a requester arena range (its
        // clone is just a refcount bump); retaining that clone in the cache
        // would pin the scarce range for the chunk's whole cache lifetime and
        // starve the requester arena. `cached_bytes` detaches the bytes — into a
        // slab frame if this node has one (ADR-0028), otherwise a fresh heap
        // allocation — so the range is freed as soon as the client stream drops
        // its `Bytes`. Note the slab does NOT reintroduce the hazard it looks
        // like it might: a frame is the cache's own memory, sized for the
        // resident set, not a range borrowed from the transport's fetch supply.
        if let Err(e) = self
            .tier
            .put_chunk(chunk_key, CachedChunk::new(self.cached_bytes(data)))
            .await
        {
            warn!(key = %chunk_key, error = %e, "chunk fill could not reach the disk tier");
        }
        self.metrics.local_admits.inc();
        self.metrics.bytes_filled.inc_by(data.len() as u64);
        guard.complete();
        // Announce to the chunk's home (a remote node — this node is not a
        // home). Fire-and-forget: a dropped announce costs one stale-entry
        // retry later, never a wrong serve (ADR-0017 soft state).
        self.spawn_admit_announce(cluster, chunk_key, generation);
    }

    /// Announce this node's layer-1 admission of `chunk_key` to the chunk's
    /// home (ADR-0017). Spawned so the client's read is never delayed by a
    /// control-plane RPC; a failure is logged and dropped (soft state).
    fn spawn_admit_announce(&self, cluster: &Cluster, chunk_key: &str, generation: u64) {
        let Some(home) = cluster
            .ring
            .homes(chunk_key, cluster.replication_r)
            .into_iter()
            .next()
        else {
            return;
        };
        let transport = Arc::clone(&cluster.transport);
        let node = cluster.local_node.clone();
        let key = chunk_key.to_owned();
        tokio::spawn(async move {
            if let Err(e) = transport
                .announce_admit(&home, &key, &node, Tier::Dram, generation)
                .await
            {
                warn!(key = %key, home = %home.name(), error = %e,
                    "admit announce failed; directory will repopulate on re-announce");
            }
        });
    }

    /// Backend ranged GET of chunk `idx`'s bounds, collected whole, **retrying a
    /// transient failure** (`pacer_backend::retry`). Inserts the chunk into the
    /// cache when `fill` is set and the per-chunk fill guard is free. `fill` is
    /// false for a peer-fallback fetch of a chunk this node does not own
    /// (ADR-0012: never store a peer-owned key).
    ///
    /// **Why the retry is here and not left to the SDK.** This is the last source
    /// in the tier order — a local hit, then a peer, then this — so an error
    /// returned here is the client's error. And it is the worst-shaped error the
    /// read path can produce: the response's `200` and `Content-Length` went out
    /// with the headers before chunk `idx` was ever requested, so a failure now
    /// truncates a body the client has already been promised in full. The SDK's
    /// own retry does not cover it, because the fault that showed up in
    /// production is a *body stream* dying after the response headers were
    /// accepted — see the [`pacer_backend::retry`] module header.
    ///
    /// # Errors
    ///
    /// The key not existing (`NoSuchKey`), or a read that failed for good: every
    /// attempt hitting a retryable fault, or one attempt hitting a fault retrying
    /// cannot fix. There are then no bytes to serve, so the read fails.
    pub(super) async fn fetch_from_backend(
        &self,
        idx: u64,
        chunk_key: &str,
        fill: bool,
    ) -> S3Result<Bytes> {
        let body = self.read_backend_chunk(idx, chunk_key).await?;
        if fill {
            self.maybe_fill(chunk_key, &body).await;
        }
        Ok(body)
    }

    /// The backend ranged GET alone — bounds, retry and its two counters — with no
    /// fill of any kind.
    ///
    /// Split from [`Self::fetch_from_backend`] for ADR-0040's leader, which holds
    /// the key's claim for the whole read and must therefore **not** go through
    /// [`Self::maybe_fill`]: that claims exclusively, would be refused by the
    /// leader's own live guard, and would silently skip the insert. A fill path that
    /// looks like it cached the chunk and did not is the failure this split exists
    /// to make unreachable.
    ///
    /// # Errors
    ///
    /// See [`Self::fetch_from_backend`] — this is where those errors are produced.
    async fn read_backend_chunk(&self, idx: u64, chunk_key: &str) -> S3Result<Bytes> {
        let bounds = self
            .chunk
            .chunk_bounds(idx, self.object_len)
            .ok_or_else(|| s3_error!(InternalError, "chunk index past object end"))?;
        let read = ChunkRead {
            bucket: &self.bucket,
            key: &self.key,
            range: bounds,
            // The chunk index decorrelates concurrent retries: `fill_parallelism`
            // chunks of one read fail together on a backend-wide fault, and
            // retrying all of them on the same schedule would rebuild the burst.
            jitter_index: idx,
        };
        let got = pacer_backend::retry::read_range(&self.backend, &read, &self.read_retry)
            .await
            .map_err(|e| self.note_read_failure(chunk_key, &e))?;
        self.note_read_retries(got.attempts);
        Ok(got.body)
    }

    /// Count the retried attempts a finished chunk read cost, successful or not.
    ///
    /// `attempts` counts the first try too, so the retries are one fewer — and a
    /// read that never issued a request (an empty range) reports zero attempts,
    /// hence the saturating subtraction rather than `- 1`.
    fn note_read_retries(&self, attempts: u32) {
        self.metrics
            .backend_read
            .retries
            .inc_by(u64::from(attempts.saturating_sub(1)));
    }

    /// Turn a terminal chunk-read failure into the client's error, counting it.
    ///
    /// A missing key is the backend's *answer* and stays a `404`, uncounted by
    /// `pacer_backend_read_failures_total` — the other two are this node failing
    /// to serve a chunk, which is what that series exists to alert on. The
    /// underlying error is logged rather than returned: it names our bucket and
    /// key, which are not the client's to see (the client addressed an alias).
    fn note_read_failure(&self, chunk_key: &str, err: &BackendReadError) -> s3s::S3Error {
        self.note_read_retries(err.attempts);
        let outcome = match &err.kind {
            BackendReadErrorKind::Missing => {
                return s3_error!(NoSuchKey, "The specified key does not exist.")
            }
            BackendReadErrorKind::Exhausted(_) => READ_FAILURE_EXHAUSTED,
            BackendReadErrorKind::Permanent(_) => READ_FAILURE_PERMANENT,
        };
        self.metrics
            .backend_read
            .failures
            .with_label_values(&[outcome])
            .inc();
        warn!(
            key = %chunk_key, outcome, attempts = err.attempts, error = %err.kind,
            "backend chunk read failed; the client's GET cannot be completed"
        );
        s3_error!(InternalError, "backend chunk read failed")
    }

    /// Where this proxy's cached chunk bytes go — [`ChunkFill`] decides
    /// (ADR-0028). Kept as a one-line method because both fill sites below read
    /// better for it, and because the policy must NOT be restated here: it is
    /// shared with the peer server's read-through fill, and the first hardware
    /// run of ADR-0028 measured nothing precisely because that path had its own
    /// answer (see [`crate::cachefill`]).
    fn cached_bytes(&self, data: &Bytes) -> Bytes {
        self.fill.cached_bytes(data, &self.metrics)
    }

    /// Insert a freshly-fetched chunk into the cache, guarded so only one fill
    /// per chunk key is in flight node-wide. A closed guard slot (another fill
    /// running) simply skips the insert — the bytes are still served.
    ///
    /// This path only fills chunks this node OWNS (see `resolve_chunk`:
    /// `fetch_from_backend(.., fill=true)` is reached exactly when
    /// `owns_chunk` held or single-node). So the fill is by the chunk's home,
    /// which is also its directory shard — the admit is recorded locally
    /// (ADR-0017 "home fills first"), no announce RPC.
    async fn maybe_fill(&self, chunk_key: &str, data: &Bytes) {
        let Some(mut guard) = self.try_begin_fill(chunk_key) else {
            return;
        };
        self.insert_filled(chunk_key, data).await;
        guard.complete();
    }

    /// Put a fetched chunk in the tier, count it, and record this node as a holder
    /// — everything [`Self::maybe_fill`] does once its claim is won.
    ///
    /// Reached by two callers holding two *kinds* of claim: `maybe_fill`'s exclusive
    /// one, and ADR-0040's leader, which took a publishing claim before its read.
    /// Neither may claim again from in here, which is why the claim is the caller's
    /// and this function takes none.
    async fn insert_filled(&self, chunk_key: &str, data: &Bytes) {
        // `cached_bytes`, not `data.clone()`: this is the OWNER's copy, the one
        // peers fetch from, so it is exactly the chunk whose serve ADR-0028
        // wants to post without staging. On a build or node with no slab this is
        // the same refcount bump it always was.
        if let Err(e) = self
            .tier
            .put_chunk(chunk_key, CachedChunk::new(self.cached_bytes(data)))
            .await
        {
            warn!(key = %chunk_key, error = %e, "chunk fill could not reach the disk tier");
        }
        self.metrics.fills_completed.inc();
        self.metrics.bytes_filled.inc_by(data.len() as u64);
        self.announce_local_admit(chunk_key);
    }

    /// Record this node as a holder of `chunk_key` in its own directory shard
    /// (ADR-0017). A fresh fill lands in the DRAM tier, so the hint is
    /// [`Tier::Dram`]; foyer may later demote it to NVMe, which only makes the
    /// hint stale (advisory — a wrong tier hint costs a suboptimal source
    /// pick, never a wrong serve). No-op single-node (no directory).
    fn announce_local_admit(&self, chunk_key: &str) {
        if let Some(cluster) = &self.cluster {
            cluster
                .directory
                .admit_next(chunk_key, &cluster.local_node, Tier::Dram);
        }
    }
}

/// Drain a peer [`BlobStream`] into a single `Bytes` (a chunk is fetched whole,
/// so its body fits the `fill_parallelism × chunk_size` memory bound).
///
/// A whole cached chunk arrives as exactly one stream item — over RDMA that one
/// `Bytes` owns the requester's registered pool slot (the zero-copy serve path,
/// planning/16 §5), so it is returned AS-IS: concatenating it through a
/// `BytesMut` would silently reintroduce the very copy-out that path removes.
/// Only a genuinely multi-item stream (a gRPC read-through split across
/// `BlobChunk` frames) is concatenated, exactly as before.
///
/// # Errors
///
/// A transport error mid-stream (the caller falls back to the backend).
pub(super) async fn collect_blob(blob: pacer_transport::BlobStream) -> anyhow::Result<Bytes> {
    let mut chunks = blob.chunks;
    let Some(first) = chunks.next().await else {
        return Ok(Bytes::new());
    };
    let first = first?;
    // Fast path: a single-item stream (every RDMA-served chunk, and any gRPC
    // chunk that fit one frame) is handed back without a copy, preserving the
    // slot-owning `Bytes` from the transport untouched.
    let Some(second) = chunks.next().await else {
        return Ok(first);
    };
    let mut buf = BytesMut::with_capacity(blob.len as usize);
    buf.extend_from_slice(&first);
    buf.extend_from_slice(&second?);
    while let Some(chunk) = chunks.next().await {
        buf.extend_from_slice(&chunk?);
    }
    Ok(buf.freeze())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a [`pacer_transport::BlobStream`] whose body is `parts` in order,
    /// with `len` = the total byte count (as the transport sets it).
    fn blob_stream(parts: Vec<Bytes>) -> pacer_transport::BlobStream {
        let len: u64 = parts.iter().map(|p| p.len() as u64).sum();
        pacer_transport::BlobStream {
            len,
            object_len: len,
            body_start: 0,
            e_tag: None,
            content_type: None,
            last_modified_epoch_secs: None,
            chunks: Box::pin(futures::stream::iter(parts.into_iter().map(Ok))),
        }
    }

    #[tokio::test]
    async fn collect_blob_passes_a_single_item_through_without_copying() {
        // The RDMA serve path yields exactly one Bytes that owns its pool slot;
        // collect_blob must hand back that same allocation, not a fresh copy —
        // otherwise the requester zero-copy win (planning/16 §5) evaporates.
        let original = Bytes::from(vec![7u8; 4096]);
        let ptr = original.as_ptr();
        let got = collect_blob(blob_stream(vec![original])).await.unwrap();
        assert_eq!(got.len(), 4096);
        assert_eq!(got.as_ptr(), ptr, "single-item stream must not be recopied");
    }

    #[tokio::test]
    async fn collect_blob_concatenates_a_multi_item_stream() {
        // A gRPC read-through arrives as several BlobChunk frames; those still
        // concatenate exactly as before.
        let got = collect_blob(blob_stream(vec![
            Bytes::from_static(b"abc"),
            Bytes::from_static(b"de"),
            Bytes::from_static(b"f"),
        ]))
        .await
        .unwrap();
        assert_eq!(&got[..], b"abcdef");
    }

    #[tokio::test]
    async fn collect_blob_handles_an_empty_stream() {
        let got = collect_blob(blob_stream(vec![])).await.unwrap();
        assert!(got.is_empty());
    }

    /// A chunk key of the shape the fill paths claim — one object, chunk 0.
    const A_CHUNK_KEY: &str = "bucket/obj#16777216:0";

    /// The daemon's real metrics, because [`FillGuard`] carries the registered
    /// `fill_inflight`/`fill_abandoned` pair rather than bare atomics: a fresh
    /// registry per test keeps the counters independent, which is all these need.
    fn fill_metrics() -> Metrics {
        Metrics::new().expect("metrics registry")
    }

    #[test]
    fn fill_guard_second_claim_of_a_live_key_is_refused() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let first = FillGuard::for_fill(&registry, &metrics, A_CHUNK_KEY)
            .expect("claiming a fresh key must succeed");
        assert_eq!(metrics.fill_inflight.get(), 1);
        assert!(
            FillGuard::for_fill(&registry, &metrics, A_CHUNK_KEY).is_none(),
            "a second claim of the same key while the first guard is alive must be refused"
        );

        drop(first);
        assert_eq!(registry.len(), 0);
        assert_eq!(metrics.fill_inflight.get(), 0);
        assert_eq!(
            metrics.fill_abandoned.get(),
            1,
            "dropping without complete() must count as abandoned"
        );
    }

    #[test]
    fn fill_guard_complete_suppresses_the_abandoned_counter() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let mut guard = FillGuard::for_fill(&registry, &metrics, A_CHUNK_KEY)
            .expect("claiming a fresh key must succeed");
        guard.complete();
        drop(guard);

        assert_eq!(registry.len(), 0);
        assert_eq!(
            metrics.fill_abandoned.get(),
            0,
            "a fill that completed normally must not count as abandoned"
        );
    }

    #[tokio::test]
    async fn fill_guard_drop_on_a_cancelled_future_frees_the_key_and_counts_abandoned() {
        // Reproduces `chunked_body`'s failure mode: the pipeline drops an
        // in-flight chunk resolution's future outright on client disconnect,
        // never reaching the code that would have removed the key. Aborting a
        // spawned task drops its future the same way — mid-poll, with no
        // chance to run anything past the last `.await` point.
        let registry = FillRegistry::new();
        let metrics = fill_metrics();
        let (registry_task, metrics_task) = (registry.clone(), metrics.clone());

        let handle = tokio::spawn(async move {
            let _guard = FillGuard::for_fill(&registry_task, &metrics_task, A_CHUNK_KEY)
                .expect("claiming a fresh key must succeed");
            // Never calls `complete()` — stands in for a fill whose backend
            // read or `put_chunk` is still in flight when the client goes away.
            std::future::pending::<()>().await;
        });

        // Wait for the spawned task to actually claim the slot before cancelling it.
        while registry.len() == 0 {
            tokio::task::yield_now().await;
        }

        handle.abort();
        let result = handle.await;
        assert!(
            result.is_err_and(|e| e.is_cancelled()),
            "the task must have been cancelled, not have run to completion"
        );

        assert!(
            !registry.is_claimed(A_CHUNK_KEY),
            "FillGuard::drop must free the key even when its future is dropped mid-poll"
        );
        assert_eq!(metrics.fill_inflight.get(), 0);
        assert_eq!(
            metrics.fill_abandoned.get(),
            1,
            "a cancelled fill must be counted as abandoned"
        );
    }

    /// ADR-0040's whole point, at the registry level: the second arrival on a key
    /// somebody is already reading gets *those* bytes, not a claim of its own.
    #[tokio::test]
    async fn a_second_arrival_is_handed_the_leaders_bytes() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let FillClaim::Lead(mut leader) = registry.claim_fill(&metrics, A_CHUNK_KEY) else {
            panic!("the first claim of a free key must lead");
        };
        let FillClaim::Follow(waiter) = registry.claim_fill(&metrics, A_CHUNK_KEY) else {
            panic!("a claim taken while a leader is fetching must follow it");
        };

        let filled = Bytes::from_static(b"the leader's chunk");
        leader.publish(&filled);
        leader.complete();

        assert_eq!(
            await_published_fill(waiter, &metrics.fill_coalesce.waiters).await,
            Some(filled),
            "the follower must receive exactly the bytes the leader published"
        );
        assert_eq!(
            metrics.fill_coalesce.waiters.get(),
            0,
            "the waiter gauge must come back down once the wait ends"
        );
    }

    /// The window this design exists to close: a request that arrives *after* the
    /// publish but before the insert finishes must take the parked bytes rather
    /// than subscribe to a channel that has already been sent to — `broadcast`
    /// buffers nothing for a receiver created after the fact, so the naive shape
    /// makes this caller wait for the full `put_chunk` and then fall back.
    #[tokio::test]
    async fn bytes_published_before_the_claim_is_released_are_taken_without_waiting() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let FillClaim::Lead(leader) = registry.claim_fill(&metrics, A_CHUNK_KEY) else {
            panic!("the first claim of a free key must lead");
        };
        let filled = Bytes::from_static(b"published, not yet inserted");
        leader.publish(&filled);

        match registry.claim_fill(&metrics, A_CHUNK_KEY) {
            FillClaim::Ready(bytes) => assert_eq!(bytes, filled),
            _ => panic!("an arrival after the publish must be served without waiting"),
        }
    }

    /// A leader that never publishes — its read failed, or its client went away and
    /// took the future with it — must *wake* its followers rather than strand them.
    /// They then fetch their own, which is what every request did before ADR-0040.
    #[tokio::test]
    async fn a_leader_that_publishes_nothing_wakes_its_follower_empty_handed() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let FillClaim::Lead(leader) = registry.claim_fill(&metrics, A_CHUNK_KEY) else {
            panic!("the first claim of a free key must lead");
        };
        let FillClaim::Follow(waiter) = registry.claim_fill(&metrics, A_CHUNK_KEY) else {
            panic!("a claim taken while a leader is fetching must follow it");
        };

        drop(leader);
        assert_eq!(
            await_published_fill(waiter, &metrics.fill_coalesce.waiters).await,
            None,
            "a dropped leader must close the channel instead of hanging its follower"
        );
        assert_eq!(
            metrics.fill_coalesce.waiters.get(),
            0,
            "and the gauge must come down on the empty-handed path too"
        );
        assert!(
            !registry.is_claimed(A_CHUNK_KEY),
            "and it must leave the key free for the follower's own fill"
        );
    }

    /// An exclusive claim publishes nothing, so a second arrival must be told to
    /// fetch its own rather than wait for bytes that will never come. This is what
    /// keeps the peer server's read-through and the layer-1 admit behaving exactly
    /// as they did.
    #[test]
    fn an_exclusive_claim_is_never_waited_on() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let _exclusive = FillGuard::for_fill(&registry, &metrics, A_CHUNK_KEY)
            .expect("claiming a fresh key must succeed");
        assert!(
            matches!(registry.claim_fill(&metrics, A_CHUNK_KEY), FillClaim::Busy),
            "a publishing claim must not wait on a holder that publishes nothing"
        );
    }

    /// Publishing on an exclusive claim is a no-op rather than a panic or a state
    /// change: `publish` is on [`FillGuard`], which both claim kinds hand out, and a
    /// future caller wiring it to the wrong one should lose the optimisation, not
    /// corrupt the entry.
    #[test]
    fn publishing_on_an_exclusive_claim_changes_nothing() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        let exclusive = FillGuard::for_fill(&registry, &metrics, A_CHUNK_KEY)
            .expect("claiming a fresh key must succeed");
        exclusive.publish(&Bytes::from_static(b"nobody asked for these"));
        assert!(
            matches!(registry.claim_fill(&metrics, A_CHUNK_KEY), FillClaim::Busy),
            "an exclusive claim must stay exclusive"
        );
    }

    /// The claim is per key, so two different chunks never serialise against each
    /// other — the property that keeps one GET's `fill_parallelism` chunks
    /// concurrent.
    #[test]
    fn claims_on_different_keys_are_independent() {
        let registry = FillRegistry::new();
        let metrics = fill_metrics();

        // Both claims are *bound*, not matched in place: a `FillClaim::Lead` holds the
        // guard, and a temporary would release the key at the end of its statement — which
        // is exactly what the occupancy assertion below would then fail to see.
        let _first = registry.claim_fill(&metrics, A_CHUNK_KEY);
        let second = registry.claim_fill(&metrics, "bucket/obj#16777216:1");
        assert!(
            matches!(second, FillClaim::Lead(_)),
            "a different chunk of the same object must lead its own fill"
        );
        assert_eq!(
            registry.len(),
            2,
            "two keys claimed at once is what keeps one GET's chunks concurrent"
        );
    }
}
