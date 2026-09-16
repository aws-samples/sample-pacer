//! Resolving one chunk, and the guard that stops two resolutions racing.
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
//! **The invariant this module owns: at most one fill per chunk key node-wide,
//! and a claim that is always released.** [`FillGuard`] holds the key in the
//! shared `filling` set and frees it in `Drop` — which is why it exists at all: a
//! manual `.remove()` is skipped by exactly the two things that happen most (a
//! client that disconnects mid-stream, an early `?`), and the key then stayed
//! claimed for the daemon's lifetime with no metric to show it. The peer server's
//! read-through claims through this same type ([`crate::peer`]), so the two fill
//! paths cannot disagree about who holds a key.

use std::collections::HashSet;
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
    /// Per-chunk-key fill guard, shared with the peer server (one fill per key
    /// node-wide).
    pub(super) filling: Arc<Mutex<HashSet<String>>>,
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
            filling: Arc::clone(&self.filling),
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

/// RAII slot in the node-wide fill-dedup set (`filling`, shared by
/// `maybe_admit_local`, `maybe_fill` and the peer server's own read-through
/// fill): while a guard for `key` is alive, [`FillCtx::try_begin_fill`] on the
/// same key returns `None`, so at most one fill per key runs at a time
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
    /// The shared set this guard holds one key in.
    filling: Arc<Mutex<HashSet<String>>>,
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
    /// Claim `key`'s slot in `filling`, or return `None` if another fill for
    /// it is already in flight — the set's whole purpose, one fill per key
    /// node-wide. Raises `inflight` by one on success.
    #[must_use]
    fn try_begin(
        filling: &Arc<Mutex<HashSet<String>>>,
        key: &str,
        inflight: &IntGauge,
        abandoned: &IntCounter,
    ) -> Option<Self> {
        if !filling.lock().unwrap().insert(key.to_owned()) {
            return None;
        }
        inflight.inc();
        Some(Self {
            filling: Arc::clone(filling),
            key: key.to_owned(),
            completed: false,
            inflight: inflight.clone(),
            abandoned: abandoned.clone(),
        })
    }

    /// Claim `key`'s slot with the two series every fill path shares — the form
    /// both callers use, so neither can wire its guard to a different pair.
    ///
    /// The `filling` set is node-wide and the claim is path-agnostic on purpose: a
    /// client GET's fill and the peer server's read-through fill of the same chunk
    /// key are the same work, and the second must skip rather than duplicate the
    /// backend read (ADR-0016/0017). Both therefore claim through here, and both
    /// count into `pacer_fill_inflight` / `pacer_fill_abandoned_total`.
    #[must_use]
    pub(crate) fn for_fill(
        filling: &Arc<Mutex<HashSet<String>>>,
        metrics: &Metrics,
        key: &str,
    ) -> Option<Self> {
        Self::try_begin(
            filling,
            key,
            &metrics.fill_inflight,
            &metrics.fill_abandoned,
        )
    }

    /// Mark the fill as having finished on its own rather than been cut short
    /// — suppresses [`Metrics::fill_abandoned`] when this guard drops.
    pub(crate) fn complete(&mut self) {
        self.completed = true;
    }
}

impl Drop for FillGuard {
    fn drop(&mut self) {
        self.filling.lock().unwrap().remove(&self.key);
        self.inflight.dec();
        if !self.completed {
            self.abandoned.inc();
        }
    }
}

impl FillCtx {
    /// Claim `chunk_key`'s slot in the node-wide fill-dedup set, or `None` if
    /// another fill for it is already running. See [`FillGuard`] for why this
    /// replaces the bare `HashSet::insert` the fill sites used to do directly.
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
            return self.fetch_from_backend(idx, &chunk_key, self.admit).await;
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
        if fill {
            self.maybe_fill(chunk_key, &got.body).await;
        }
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
        guard.complete();
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

    /// Fresh, unregistered gauge/counter pair for a [`FillGuard`] test — a
    /// bare [`Registry`](prometheus::Registry) is deliberately not involved,
    /// since these tests only need the atomics themselves.
    fn fill_metrics() -> (IntGauge, IntCounter) {
        (
            IntGauge::new("test_pacer_fill_inflight", "test").unwrap(),
            IntCounter::new("test_pacer_fill_abandoned_total", "test").unwrap(),
        )
    }

    #[test]
    fn fill_guard_second_claim_of_a_live_key_is_refused() {
        let filling: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let (inflight, abandoned) = fill_metrics();

        let first = FillGuard::try_begin(&filling, "obj#16777216:0", &inflight, &abandoned)
            .expect("claiming a fresh key must succeed");
        assert_eq!(inflight.get(), 1);
        assert!(
            FillGuard::try_begin(&filling, "obj#16777216:0", &inflight, &abandoned).is_none(),
            "a second claim of the same key while the first guard is alive must be refused"
        );

        drop(first);
        assert!(filling.lock().unwrap().is_empty());
        assert_eq!(inflight.get(), 0);
        assert_eq!(
            abandoned.get(),
            1,
            "dropping without complete() must count as abandoned"
        );
    }

    #[test]
    fn fill_guard_complete_suppresses_the_abandoned_counter() {
        let filling: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let (inflight, abandoned) = fill_metrics();

        let mut guard = FillGuard::try_begin(&filling, "obj#16777216:1", &inflight, &abandoned)
            .expect("claiming a fresh key must succeed");
        guard.complete();
        drop(guard);

        assert!(filling.lock().unwrap().is_empty());
        assert_eq!(
            abandoned.get(),
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
        let filling: Arc<Mutex<HashSet<String>>> = Arc::new(Mutex::new(HashSet::new()));
        let (inflight, abandoned) = fill_metrics();
        let filling_task = Arc::clone(&filling);
        let inflight_task = inflight.clone();
        let abandoned_task = abandoned.clone();

        let handle = tokio::spawn(async move {
            let _guard = FillGuard::try_begin(
                &filling_task,
                "obj#16777216:2",
                &inflight_task,
                &abandoned_task,
            )
            .expect("claiming a fresh key must succeed");
            // Never calls `complete()` — stands in for a fill whose backend
            // read or `put_chunk` is still in flight when the client goes away.
            std::future::pending::<()>().await;
        });

        // Wait for the spawned task to actually claim the slot before cancelling it.
        while filling.lock().unwrap().is_empty() {
            tokio::task::yield_now().await;
        }

        handle.abort();
        let result = handle.await;
        assert!(
            result.is_err_and(|e| e.is_cancelled()),
            "the task must have been cancelled, not have run to completion"
        );

        assert!(
            filling.lock().unwrap().is_empty(),
            "FillGuard::drop must free the key even when its future is dropped mid-poll"
        );
        assert_eq!(inflight.get(), 0);
        assert_eq!(
            abandoned.get(),
            1,
            "a cancelled fill must be counted as abandoned"
        );
    }
}
