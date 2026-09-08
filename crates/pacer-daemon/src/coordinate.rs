//! Coordinator side of a scattered write (ADR-0032 § 2, `planning/24-write-path.md`).
//!
//! The node a client's PUT arrives on opens one multipart upload, streams the body
//! onto the chunk grid, and offers each window to that chunk's home — which uploads
//! it as a part and stages it. When every part is in, the coordinator calls
//! Complete, and only then does anything become cache-visible.
//!
//! # The scatter is chosen before the first byte, and cannot be unchosen
//!
//! A body is a stream that can be read once. So the decision to scatter is made
//! from the object's length and the key's novelty *before* any of it is consumed
//! ([`crate::scatter::scatter_verdict`]), and after that there is no going back to
//! a plain `PutObject` — the bytes are gone. What remains is a **per-window**
//! fallback, which is enough for every failure that actually happens: an owner that
//! refuses or cannot be reached simply has its window uploaded by the coordinator
//! instead. Only a failure of the multipart upload itself (Create or Complete) is
//! fatal, and it fails the client's PUT exactly as a failed plain PUT would.
//!
//! # What bounds a coordinator's memory
//!
//! One semaphore, acquired by the body reader before it hands a window to a task
//! and released when that upload ends — so `windows_in_flight × chunk_size` is a
//! real ceiling and the read of the client's body genuinely stalls at it. The
//! ordering is subtle enough, and was wrong for long enough, that it is argued
//! where it happens: `ScatterCoordinator::dispatch`.
//!
//! # The coordinator stages its own windows too
//!
//! A window the coordinator uploads itself still cannot be cached until Complete —
//! same fence, same reason. So it goes through the *same* [`StagingArea`] as an
//! owner's would, under the same node-wide budget, and is published by the same
//! commit. Two consequences worth naming: the budget bounds this node's total
//! staged bytes whichever role they came from, and a coordinator past its budget
//! simply does not cache that window. Uploading still succeeds; the window is a
//! miss later. Never an error.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use futures::StreamExt;
use pacer_cache::chunk::{CachedChunk, ObjectHeader};
use pacer_cache::tier::ChunkTier;
use pacer_cache::CacheValue;
use pacer_ring::directory::Tier;
use pacer_ring::NodeId;
use pacer_transport::{StoreOffer, StoreOutcome};
use s3s::dto::StreamingBlob;
use tokio::sync::{AcquireError, Semaphore};
use tracing::{debug, warn};

use crate::proxy::Cluster;
use crate::scatter::{SaturationTracker, ScatterConfig, ScatterPlan, WindowSplitter};
use crate::staging::{StageOutcome, StagingArea};

/// What a scattered PUT is writing.
#[derive(Debug, Clone, Copy)]
pub struct ScatterTarget<'a> {
    /// The **backend** bucket, already resolved through the bucket map. Owners are
    /// handed this name, not the alias the client used.
    pub bucket: &'a str,
    /// Object key.
    pub key: &'a str,
    /// `"{bucket}/{key}"` — the cache key the header is stored under, and the base
    /// every chunk key derives from.
    pub object_key: &'a str,
    /// Object length, from `Content-Length`. Known before the body arrives, which is
    /// what lets the whole plan be computed up front.
    pub object_len: u64,
    /// Content type to replay on a cache hit, if the client sent one.
    pub content_type: Option<&'a str>,
    /// The whole-object CRC32 the client asked the backend to validate, in S3's
    /// base64 form, if it sent one.
    ///
    /// **Why this is honoured rather than a reason to decline.** Splitting the body
    /// means no single `UploadPart` sees the whole object — but the *coordinator*
    /// does, and it already folds exactly this digest to hand to Complete. So a
    /// client CRC32 is not a promise the scatter cannot keep: it is the same number,
    /// computed on the same bytes, and comparing them costs nothing. A mismatch
    /// fails the write with the error S3 itself would return
    /// ([`ScatterError::ClientDigestMismatch`]).
    ///
    /// This matters far more than it looks: every current AWS SDK computes a CRC32
    /// for uploads by default (`when_supported`), so treating one as
    /// unscatterable would make the scatter decline essentially every real client's
    /// PUT — the feature would be dead in production while looking configured. The
    /// digests we genuinely cannot reproduce from parts (MD5, CRC32C, SHA-1,
    /// SHA-256, CRC64NVME) still decline, in `proxy::try_scatter`.
    pub expected_crc32: Option<&'a str>,
}

/// Why a scattered write failed, split by what the client should be told.
#[derive(Debug, thiserror::Error)]
pub enum ScatterError {
    /// The body's digest disagreed with the CRC32 the client supplied, so the
    /// object was never assembled. The same answer S3 gives for a bad digest, and
    /// the reason it is a distinct variant: this is the client's error, not ours.
    #[error("client CRC32 {claimed} does not match the body's {computed}")]
    ClientDigestMismatch {
        /// What the client claimed the whole body hashes to.
        claimed: String,
        /// What the coordinator computed while splitting it.
        computed: String,
    },
    /// Anything else — Create, Complete, a window that failed on both the owner and
    /// this node, or a body shorter than `Content-Length` promised.
    #[error(transparent)]
    Failed(#[from] anyhow::Error),
}

/// A finished scatter, for the response and the caller's metrics.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScatterResult {
    /// The object's final ETag — composite (`"…-N"`), since a scattered write is
    /// always a multipart upload. ADR-0032's one client-visible break.
    pub e_tag: String,
    /// Windows an owner uploaded.
    pub scattered_windows: usize,
    /// Windows the coordinator uploaded itself, because the home refused, could not
    /// be reached, or *is* this node.
    pub local_windows: usize,
    /// Windows that could not be staged for caching, on either side. Uploaded and
    /// durable, simply not cached — each is a later miss.
    pub uncached_windows: usize,
    /// Distinct nodes that took at least one window. The scatter's whole premise is
    /// that this approaches the fleet size; a 1 here means it did not engage.
    pub distinct_owners: usize,
}

/// Everything a single window's upload needs, shared across the in-flight tasks.
struct Shared {
    backend: aws_sdk_s3::Client,
    cluster: Cluster,
    staging: Arc<StagingArea>,
    tracker: Arc<Mutex<SaturationTracker>>,
    cooldown: std::time::Duration,
    /// Where this window's slot-hold is charged — `metrics.scatter.phase_seconds`.
    ///
    /// Carried here rather than passed down, and that is not tidiness: [`upload_window`]
    /// and [`upload_here`] are already at the seven-argument limit, so a further
    /// parameter would have to displace one. `Shared` is what those two already take to
    /// reach the backend and the tracker, and the phase histogram is the same kind of
    /// per-node singleton.
    metrics: crate::metrics::Metrics,
}

/// The mutable state of one body's pass through the splitter.
///
/// Bundled rather than passed as six parameters: they are one thing — the progress of
/// this upload — and threading them separately is what pushed the dispatch call past
/// the argument and nesting limits.
struct Pipeline {
    /// Reassembles the body into exactly-`chunk_size` windows.
    splitter: WindowSplitter,
    /// In-flight window uploads.
    tasks: tokio::task::JoinSet<anyhow::Result<DonePart>>,
    /// Parts finished so far — also what an unwind needs to know who to tell.
    parts: Vec<DonePart>,
    /// Index of the next planned window, so a window is matched to its home and part
    /// number by position rather than by recomputing the grid.
    next: usize,
}

/// One window's outcome, collected to build the Complete call and the publish step.
struct DonePart {
    part_number: i32,
    e_tag: String,
    /// The owner that uploaded it, or `None` if the coordinator did. Drives which
    /// nodes get a commit and which chunks this node publishes itself.
    ///
    /// The whole [`NodeId`] rather than a name, so publishing does not have to look
    /// the peer back up in a ring that may have changed underneath it — the address
    /// this window was actually sent to is the one to commit against.
    owner: Option<NodeId>,
    /// Whether the bytes were staged anywhere. `false` means nobody will cache this
    /// window — durable in S3, a miss on read.
    staged: bool,
    /// CRC32 of exactly this window's bytes, carried back so the whole-object digest can
    /// be folded from the parts instead of hashed on the body reader's task.
    ///
    /// A [`crc32fast::Hasher`] and not a `u32`, because `Hasher::combine` needs the LENGTH
    /// of what it is folding in and only the hasher knows it. Order is recoverable from
    /// `part_number`, which [`ScatterCoordinator::complete`] already has to sort by, so
    /// nothing depends on the order these come back from the `JoinSet` in.
    digest: crc32fast::Hasher,
}

/// One window on its way to S3, with the digest of exactly those bytes beside them.
///
/// Bundled so the two cannot be separated: the S3 part checksum, the offer to an owner and
/// the whole-object fold all read this digest, and a digest computed over anything but
/// `body` would fail Complete rather than corrupt the object — but it would fail it after
/// the whole upload, which is an expensive way to learn about a mismatched pair. It also
/// keeps [`upload_window`] and [`upload_here`] under the seven-argument limit, which
/// passing the two separately would not.
struct Window {
    body: Bytes,
    digest: crc32fast::Hasher,
}

impl Window {
    /// This window's CRC32 in S3's `ChecksumCRC32` form.
    fn checksum(&self) -> String {
        base64_u32(self.digest.clone().finalize())
    }
}

/// The window slots a coordinator holds at once, and the high-water mark of how many
/// were ever held together.
///
/// The semaphore alone would be enough to *enforce* the bound; this type exists so the
/// bound can be **read on hardware**. `ebe07c07` moved the permit acquisition ahead of
/// the window's bytes, which is what makes `windows_in_flight × chunk_size` a real byte
/// ceiling — and nothing on a running daemon could confirm that, or say how close a
/// paid arm came to it, because a semaphore publishes nothing.
///
/// Owned as one `Arc` by the coordinator, so the metrics scrape reads the same slots
/// `ScatterCoordinator::dispatch` takes rather than a copy of a count.
pub struct WindowSlots {
    /// The permits themselves. An `Arc` because `acquire_owned` needs one, and the
    /// permit has to outlive this call to be moved into the upload's task.
    slots: Arc<Semaphore>,
    /// The ceiling, i.e. `scatter.windowsInFlight`.
    ///
    /// Kept alongside because [`Semaphore`] does not report the count it was built
    /// with, and every reading of the peak is judged against it: a peak *at* the limit
    /// means the ceiling was reached, and a peak *above* it would mean the ceiling is
    /// not one at all.
    limit: usize,
    /// High-water mark of [`Self::in_flight()`] since the process started.
    ///
    /// A peak rather than a scrape-time sample, for the same reason
    /// `DeliveryMetrics::inflight_chunks_peak` is one: a Prometheus scrape lands every
    /// 15-60 s (and this repo's Grafana has a 60 s rate floor), while a pipeline fills
    /// and drains inside one PUT — so a sampled gauge reports whatever one instant
    /// happened to hold, and the maximum is the entire quantity of interest.
    ///
    /// `fetch_max` rather than the get-compare-set the delivery peak uses, because
    /// there is a single-instruction primitive for exactly this and it cannot lose a
    /// concurrent raise. Never decreases, so it is read once at the end of a run
    /// rather than `rate()`d.
    peak: AtomicUsize,
}

impl WindowSlots {
    /// `limit` slots, none held.
    ///
    /// A zero would deadlock every scatter, so it is clamped rather than trusted
    /// (config resolves it to a nonzero default, but this is cheap).
    #[must_use]
    pub fn new(limit: usize) -> Self {
        let limit = limit.max(1);
        Self {
            slots: Arc::new(Semaphore::new(limit)),
            limit,
            peak: AtomicUsize::new(0),
        }
    }

    /// Take one slot, waiting until one is free, and raise [`Self::peak()`] to what is
    /// now in flight.
    ///
    /// The permit is returned owned so the caller can move it into the spawned upload:
    /// the slot is held for that upload's whole life and released when the task ends,
    /// failure included.
    ///
    /// # Errors
    ///
    /// Only if the semaphore has been closed, which nothing in this daemon does — it
    /// is surfaced rather than unwrapped so a future `close()` cannot turn into a panic
    /// on the write path.
    pub async fn acquire(&self) -> Result<tokio::sync::OwnedSemaphorePermit, AcquireError> {
        let permit = Arc::clone(&self.slots).acquire_owned().await?;
        // After the acquisition, so the count includes this window. A slot freed
        // concurrently can make the reading one low, which costs at most 1 against a
        // bound in the tens — the same benign undercount the delivery peak documents.
        self.peak.fetch_max(self.in_flight(), Ordering::Relaxed);
        Ok(permit)
    }

    /// Slots held right now — the instantaneous width of the coordinator's pipeline,
    /// and with `chunk_size` the bytes it is holding.
    #[must_use]
    pub fn in_flight(&self) -> usize {
        self.limit.saturating_sub(self.slots.available_permits())
    }

    /// The most slots ever held at once — a latched high-water mark, never a sample;
    /// see the `peak` field for why, and read it against [`Self::limit()`].
    #[must_use]
    pub fn peak(&self) -> usize {
        self.peak.load(Ordering::Relaxed)
    }

    /// The ceiling this node was configured with (`scatter.windowsInFlight`), so a
    /// reader of [`Self::peak()`] does not have to know the chart value.
    #[must_use]
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Drives scattered PUTs for this node.
pub struct ScatterCoordinator {
    shared: Arc<Shared>,
    tier: ChunkTier,
    chunk: pacer_cache::chunk::ChunkConfig,
    /// Bounds windows in flight, and with them the bytes this coordinator holds.
    ///
    /// A permit is taken in [`Self::dispatch`] — **before** the window is handed to
    /// a task — and released when that window's upload ends. Since `dispatch` is
    /// awaited by the single task that reads the body, a coordinator with
    /// `windows_in_flight` uploads outstanding stops pulling frames, and the client
    /// is backpressured through TCP. Read `dispatch`'s docs before moving that
    /// acquisition: it used to sit inside the spawned task, where it bounded
    /// concurrent uploads and nothing at all about memory.
    ///
    /// So held bytes are `windows_in_flight × chunk_size` plus two terms this
    /// coordinator does not choose: the under-one-window remainder
    /// [`WindowSplitter`] carries, and the one frame `body.next()` last yielded —
    /// whose size is the *sender's* framing, not ours. Over a network that is tens
    /// of KiB; an in-process caller may hand over the whole object in one frame,
    /// and no ordering here can unmake a buffer that already exists.
    ///
    /// A [`WindowSlots`] rather than a bare semaphore so that ceiling is **observable
    /// on hardware**: it carries the limit and the high-water mark the scrape publishes
    /// as `pacer_scatter_windows_in_flight`, `_peak` and `_limit`.
    windows: Arc<WindowSlots>,
}

impl ScatterCoordinator {
    /// Assemble a coordinator from the daemon's shared parts.
    ///
    /// `metrics` is here so every phase of a window's slot-hold is attributable —
    /// `metrics.scatter.phase_seconds`, the series
    /// `bench/ladder/results/w1-write-ceilings.md` names as the one measurement that
    /// decides ADR-0032 Phase 5.
    pub fn new(
        backend: aws_sdk_s3::Client,
        tier: ChunkTier,
        chunk: pacer_cache::chunk::ChunkConfig,
        cluster: Cluster,
        cfg: &ScatterConfig,
        staging: Arc<StagingArea>,
        metrics: crate::metrics::Metrics,
    ) -> Self {
        Self {
            shared: Arc::new(Shared {
                backend,
                cluster,
                staging,
                tracker: Arc::new(Mutex::new(SaturationTracker::new())),
                cooldown: cfg.saturated_cooldown,
                metrics,
            }),
            tier,
            chunk,
            windows: Arc::new(WindowSlots::new(cfg.windows_in_flight)),
        }
    }

    /// This coordinator's window slots, for the metrics scrape to read.
    ///
    /// Handed out rather than copied into a gauge at the call site: the peak has to be
    /// latched where a slot is taken (see [`WindowSlots`]), and the scrape then reads
    /// the same object instead of a second count that could drift from it.
    #[must_use]
    pub fn window_slots(&self) -> &Arc<WindowSlots> {
        &self.windows
    }

    /// Scatter one PUT and return its ETag.
    ///
    /// # Errors
    ///
    /// Create or Complete failing, a window failing on both the owner and this
    /// node, the client's body ending short of `object_len`, or the body's digest
    /// disagreeing with a CRC32 the client supplied. Every one aborts the multipart
    /// upload and discards whatever was staged, then fails the PUT — the body is
    /// consumed by then, so there is no plain-PUT retry to fall back to.
    pub async fn scatter(
        &self,
        target: ScatterTarget<'_>,
        body: StreamingBlob,
    ) -> Result<ScatterResult, ScatterError> {
        let upload_id = self.create_upload(&target).await?;
        let plan = ScatterPlan::build(
            &self.shared.cluster,
            &self.chunk,
            target.object_key,
            target.object_len,
        );
        debug!(
            key = target.key,
            windows = plan.windows.len(),
            distinct_homes = plan.distinct_homes(),
            "scattering a PUT"
        );
        match self.drive(&plan, &target, &upload_id, body).await {
            Ok((parts, crc32)) => {
                // Before Complete, because a digest the client already disagrees
                // with must not become a durable object even briefly.
                if let Some(claimed) = target.expected_crc32 {
                    let computed = base64_u32(crc32);
                    if claimed != computed {
                        self.unwind(&plan, &target, &upload_id).await;
                        return Err(ScatterError::ClientDigestMismatch {
                            claimed: claimed.to_owned(),
                            computed,
                        });
                    }
                }
                match self.complete(&target, &upload_id, &parts, crc32).await {
                    Ok(e_tag) => Ok(self.publish(&target, &upload_id, parts, e_tag).await),
                    Err(e) => {
                        self.unwind(&plan, &target, &upload_id).await;
                        Err(e.into())
                    }
                }
            }
            Err((e, _parts)) => {
                self.unwind(&plan, &target, &upload_id).await;
                Err(e.into())
            }
        }
    }

    /// Open the multipart upload, asking S3 for a whole-object CRC32 so the digest
    /// the coordinator accumulates is actually checked at Complete.
    ///
    /// `FULL_OBJECT` is the load-bearing part: splitting the body means no single
    /// `UploadPart` sees the whole thing, so without this the backend can only
    /// validate parts and never the assembly. Enforcement verified in
    /// `spike/mpu-scatter` (gate 0.2b).
    async fn create_upload(&self, target: &ScatterTarget<'_>) -> anyhow::Result<String> {
        let mut req = self
            .shared
            .backend
            .create_multipart_upload()
            .bucket(target.bucket)
            .key(target.key)
            .checksum_algorithm(aws_sdk_s3::types::ChecksumAlgorithm::Crc32)
            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject);
        if let Some(ct) = target.content_type {
            req = req.content_type(ct);
        }
        let out = req.send().await?;
        out.upload_id()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("CreateMultipartUpload returned no upload id"))
    }

    /// Stream the body, dispatch each window, and collect the parts.
    ///
    /// On failure returns the parts finished so far, so the caller can still abort
    /// the upload and tell every owner that took one to discard it.
    #[allow(clippy::type_complexity)]
    async fn drive(
        &self,
        plan: &ScatterPlan,
        target: &ScatterTarget<'_>,
        upload_id: &str,
        body: StreamingBlob,
    ) -> Result<(Vec<DonePart>, u32), (anyhow::Error, Vec<DonePart>)> {
        let mut pipe = Pipeline {
            splitter: WindowSplitter::new(
                usize::try_from(self.chunk.chunk_size()).unwrap_or(usize::MAX),
            ),
            tasks: tokio::task::JoinSet::new(),
            parts: Vec::with_capacity(plan.windows.len()),
            next: 0,
        };
        // Converting to the tuple here, once, is what spares every inner step from
        // having to carry the partial part list along just so a failure can unwind.
        match self
            .run_pipeline(plan, target, upload_id, body, &mut pipe)
            .await
        {
            Ok(()) => {
                let whole = whole_object_crc32(&pipe.parts);
                Ok((pipe.parts, whole))
            }
            Err(e) => Err((e, pipe.parts)),
        }
    }

    /// Read the body to its end, dispatching every window and joining the uploads.
    async fn run_pipeline(
        &self,
        plan: &ScatterPlan,
        target: &ScatterTarget<'_>,
        upload_id: &str,
        mut body: StreamingBlob,
        pipe: &mut Pipeline,
    ) -> anyhow::Result<()> {
        loop {
            match body.next().await {
                Some(Ok(bytes)) => {
                    pipe.splitter.push(bytes);
                    self.drain_windows(plan, target, upload_id, pipe).await?;
                }
                Some(Err(e)) => return Err(anyhow::anyhow!("reading PUT body: {e}")),
                None => break,
            }
        }
        // End of body: the remainder is the object's last chunk, the only window
        // allowed to be shorter than `chunk_size`.
        if let Some(tail) = pipe.splitter.take_tail() {
            self.dispatch(plan, target, upload_id, tail, pipe).await?;
        }
        while let Some(joined) = pipe.tasks.join_next().await {
            pipe.parts.push(joined_part(joined)?);
        }
        if pipe.parts.len() != plan.windows.len() {
            anyhow::bail!(
                "body yielded {} windows, Content-Length promised {}",
                pipe.parts.len(),
                plan.windows.len()
            );
        }
        Ok(())
    }

    /// Dispatch every full window the splitter now holds, collecting any upload that
    /// already finished so its permit frees without waiting for the whole body.
    ///
    /// Awaited by the body reader, and that is load-bearing: while the pipeline is
    /// full this does not return, so [`Self::run_pipeline`] does not pull another
    /// frame. See [`Self::dispatch`].
    async fn drain_windows(
        &self,
        plan: &ScatterPlan,
        target: &ScatterTarget<'_>,
        upload_id: &str,
        pipe: &mut Pipeline,
    ) -> anyhow::Result<()> {
        while let Some(window) = pipe.splitter.take_window() {
            self.dispatch(plan, target, upload_id, window, pipe).await?;
            while let Some(joined) = pipe.tasks.try_join_next() {
                pipe.parts.push(joined_part(joined)?);
            }
        }
        Ok(())
    }

    /// Take a slot in flight, fold one window into the running digest, and spawn its
    /// upload.
    ///
    /// The digest is folded here, in body order and before any concurrency, so it
    /// is the CRC32 of the object as the client sent it regardless of the order
    /// windows finish in. Awaiting the permit first does not weaken that: the body
    /// reader calls this one window at a time and awaits it, so the fold order is
    /// still the body's.
    ///
    /// # Why the permit is acquired here rather than in the task
    ///
    /// It used to be acquired *inside* the spawned upload, which made this function
    /// synchronous and made [`ScatterCoordinator::windows`] bound only the number of
    /// concurrent `UploadPart`s. Every window past the limit sat in a parked task
    /// **already holding a full `chunk_size` buffer**, so a coordinator's footprint
    /// was `concurrent PUTs × object bytes`, not `windows_in_flight × chunk_size`,
    /// and the body read it claimed to backpressure never blocked. That cost a
    /// hardware arm: at `windows_in_flight = 64` and five concurrent coordinators,
    /// three of five daemons were `OOMKilled` against a 53 GiB limit the chart had
    /// sized from `64 × 16 MiB = 1 GiB`
    /// (`bench/ladder/results/w1-write-scatter.md`, 2026-08-27). The old ordering
    /// was deliberate and commented as correct, which is why this one is spelled
    /// out at length.
    ///
    /// Two alternatives were considered and rejected:
    ///
    /// * **A bounded channel between the body reader and an upload pool.** Same
    ///   effect, but its capacity would be a second spelling of
    ///   `windows_in_flight`, and it needs a consumer whose lifetime and error path
    ///   are new machinery. The semaphore already exists and already carries that
    ///   meaning.
    /// * **Acquiring in [`Self::run_pipeline`], before the next `body.next()`.** A
    ///   frame is not a window: one frame may complete none or many, so a
    ///   permit-per-frame is neither an upper nor a lower bound on windows in
    ///   flight. It would over-admit a client sending 1 GiB frames and needlessly
    ///   stall one sending 4 KiB.
    ///
    /// One window's worth of slack remains by choice: the permit is taken after
    /// `take_window` has materialised the buffer, because the alternative is for
    /// this loop to re-derive "a full window is available" from
    /// `WindowSplitter::carried`, duplicating a condition `take_window` owns. Those
    /// bytes were resident in the splitter's carry either way, so the ordering moves
    /// an allocation, not a byte.
    async fn dispatch(
        &self,
        plan: &ScatterPlan,
        target: &ScatterTarget<'_>,
        upload_id: &str,
        window: Bytes,
        pipe: &mut Pipeline,
    ) -> anyhow::Result<()> {
        // Before the permit, so a body longer than `Content-Length` is answered now
        // rather than after parking behind other windows' uploads.
        let planned = plan
            .windows
            .get(pipe.next)
            .ok_or_else(|| anyhow::anyhow!("body is longer than Content-Length promised"))?
            .clone();
        // Raises the in-flight high-water mark as a side effect, which is what makes
        // this bound readable on a running daemon rather than merely argued here.
        let queued = std::time::Instant::now();
        let permit = self
            .windows
            .acquire()
            .await
            .map_err(|e| anyhow::anyhow!("the windows-in-flight semaphore closed: {e}"))?;
        // The QUEUEING term, and the only phase timed on the body reader's own task.
        // That is what makes it the client's cost rather than the daemon's: this
        // function is awaited by `run_pipeline`, so while it is here no `body.next()`
        // is issued and the client is stalled through TCP. The peak gauge above says
        // the slots were all taken; this says for how long the next window waited.
        self.shared
            .metrics
            .scatter
            .observe_phase(crate::metrics::SCATTER_PHASE_PERMIT_WAIT, queued.elapsed());
        pipe.next += 1;
        // NOT hashed here. This function is awaited by `run_pipeline`, so every microsecond
        // spent in it is a microsecond no `body.next()` is issued and the client is stalled
        // through TCP — and until 2026-09-08 it made TWO full passes over every window on
        // this task, one for the running whole-object digest and one for the part checksum.
        // At ~1 ms per 16 MiB pass that is a ceiling of a few GiB/s per node from CRC alone,
        // on the one task that must never be busy. Both are now the single pass
        // [`window_digest`] makes on the blocking pool inside the spawned upload task, and
        // the whole-object digest is folded from the parts in `drive`.
        let shared = Arc::clone(&self.shared);
        let (bucket, key, upload_id) = (
            target.bucket.to_owned(),
            target.key.to_owned(),
            upload_id.to_owned(),
        );
        pipe.tasks.spawn(async move {
            // Moved in rather than acquired here, and named so it is not dropped
            // early: the slot is held for the upload's whole life and returned when
            // this task ends — including when it ends by failing.
            let _permit = permit;
            upload_window(&shared, &planned, &bucket, &key, &upload_id, window).await
        });
        Ok(())
    }

    /// Assemble the object. **This is the durability point**, and it precedes the
    /// client's 200 — ADR-0032 § 1.
    async fn complete(
        &self,
        target: &ScatterTarget<'_>,
        upload_id: &str,
        parts: &[DonePart],
        crc32: u32,
    ) -> anyhow::Result<String> {
        let mut ordered: Vec<&DonePart> = parts.iter().collect();
        ordered.sort_unstable_by_key(|p| p.part_number);
        let completed: Vec<aws_sdk_s3::types::CompletedPart> = ordered
            .iter()
            .map(|p| {
                aws_sdk_s3::types::CompletedPart::builder()
                    .part_number(p.part_number)
                    .e_tag(&p.e_tag)
                    .build()
            })
            .collect();
        let started = std::time::Instant::now();
        let out = self
            .shared
            .backend
            .complete_multipart_upload()
            .bucket(target.bucket)
            .key(target.key)
            .upload_id(upload_id)
            .checksum_crc32(base64_u32(crc32))
            .checksum_type(aws_sdk_s3::types::ChecksumType::FullObject)
            .multipart_upload(
                aws_sdk_s3::types::CompletedMultipartUpload::builder()
                    .set_parts(Some(completed))
                    .build(),
            )
            .send()
            .await;
        // Before the `?`, so a Complete that FAILED is charged too: it held every
        // window's staged bytes for as long as it ran, and a phase that silently gets
        // cheaper on the failure path is the shape that makes a slow one invisible.
        // Observed once per PUT, unlike every other phase in this family — which is why
        // its `_count` is the PUT count and not the window count.
        self.shared
            .metrics
            .scatter
            .observe_phase(crate::metrics::SCATTER_PHASE_COMPLETE, started.elapsed());
        let out = out?;
        out.e_tag()
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow::anyhow!("CompleteMultipartUpload returned no ETag"))
    }

    /// Make everything visible: commit at each owner, commit this node's own staged
    /// windows, and write the object header.
    ///
    /// Deliberately after Complete and deliberately not awaited for correctness —
    /// each step turns a guaranteed miss into a possible hit, so a failure costs
    /// warmth and never a wrong answer (ADR-0032 § 3). Errors are logged, not
    /// propagated: the client's write already succeeded.
    async fn publish(
        &self,
        target: &ScatterTarget<'_>,
        upload_id: &str,
        parts: Vec<DonePart>,
        e_tag: String,
    ) -> ScatterResult {
        let scattered_windows = parts.iter().filter(|p| p.owner.is_some()).count();
        let uncached_windows = parts.iter().filter(|p| !p.staged).count();
        let local_windows = parts.len() - scattered_windows;
        let owners = distinct_owners(&parts);

        for owner in &owners {
            if let Err(e) = self
                .shared
                .cluster
                .transport
                .commit_upload(owner, upload_id, &e_tag)
                .await
            {
                warn!(owner = %owner.name(), error = %e,
                    "commit failed; those chunks stay uncached until their TTL");
            }
        }
        self.commit_local(upload_id, &e_tag).await;
        self.write_header(target, &e_tag).await;
        ScatterResult {
            e_tag,
            scattered_windows,
            local_windows,
            uncached_windows,
            distinct_owners: owners.len(),
        }
    }

    /// Publish the windows this node uploaded itself, announcing each copy so
    /// readers can find it.
    ///
    /// A window whose home *is* this node is announced as its home; one this node
    /// took because the home refused is announced as an ordinary sharer — which is
    /// exactly what ADR-0016 layer 1 admission already produces, so the directory
    /// and the read path need nothing new.
    async fn commit_local(&self, upload_id: &str, e_tag: &str) {
        for (chunk_key, body) in self.shared.staging.commit(upload_id) {
            if let Err(e) = self
                .tier
                .put_chunk(&chunk_key, CachedChunk::versioned(body, e_tag.to_owned()))
                .await
            {
                warn!(key = %chunk_key, error = %e, "committed chunk could not reach the disk tier");
            }
            self.shared.cluster.directory.admit_next(
                &chunk_key,
                &self.shared.cluster.local_node,
                Tier::Dram,
            );
        }
    }

    /// Store the object header at its home, carrying the composite ETag Complete
    /// just minted — which is why this cannot happen any earlier.
    async fn write_header(&self, target: &ScatterTarget<'_>, e_tag: &str) {
        let header = ObjectHeader::new(
            target.object_len,
            Some(e_tag.to_owned()),
            target.content_type.map(ToOwned::to_owned),
            None,
        );
        self.tier
            .cache()
            .insert(target.object_key.to_owned(), CacheValue::Header(header));
    }

    /// Abort the upload and drop everything staged for it, on either side.
    ///
    /// Addressed from the **plan**, not from the parts collected so far, and that
    /// distinction is a bug's worth of difference. A window's upload runs in its own
    /// task; when one task fails, the rest are dropped mid-flight, so an owner can
    /// have staged and uploaded a window whose result this coordinator never
    /// collected. Telling only the owners of collected parts leaves exactly those
    /// reservations held until their TTL — 15 minutes during which that node refuses
    /// offers for a write that already failed. Every home in the plan is told
    /// instead: `DiscardUpload` for an upload a node knows nothing about drops
    /// nothing and answers 0, so the extra messages cost a round trip on a path that
    /// has already failed.
    ///
    /// Best-effort beyond that: an owner we cannot reach keeps its staged bytes until
    /// its TTL reaps them, which is why that reaper exists. The bucket also needs an
    /// `AbortIncompleteMultipartUpload` lifecycle rule for the case where this node
    /// dies before getting here.
    async fn unwind(&self, plan: &ScatterPlan, target: &ScatterTarget<'_>, upload_id: &str) {
        self.shared.staging.discard(upload_id);
        for owner in self.offered_homes(plan) {
            if let Err(e) = self
                .shared
                .cluster
                .transport
                .discard_upload(&owner, upload_id)
                .await
            {
                warn!(owner = %owner.name(), error = %e,
                    "discard failed; staged bytes wait for the owner's TTL");
            }
        }
        if let Err(e) = self
            .shared
            .backend
            .abort_multipart_upload()
            .bucket(target.bucket)
            .key(target.key)
            .upload_id(upload_id)
            .send()
            .await
        {
            warn!(key = target.key, error = %e,
                "abort failed; the incomplete upload waits for the bucket lifecycle rule");
        }
    }

    /// Every distinct peer this plan's windows could have been offered to.
    ///
    /// The unwind's address list (see [`Self::unwind`]). This node is excluded
    /// because its own staged windows are dropped locally, not over the wire.
    fn offered_homes(&self, plan: &ScatterPlan) -> Vec<NodeId> {
        let mut homes: Vec<NodeId> = Vec::new();
        for home in plan.windows.iter().filter_map(|w| w.homes.first()) {
            let known = home.name() == self.shared.cluster.local_node
                || homes.iter().any(|seen| seen.name() == home.name());
            if !known {
                homes.push(home.clone());
            }
        }
        homes
    }
}

/// One window task's result, flattened: a panicked task and a failed upload both
/// become an error, and neither is dropped.
///
/// Both reap sites go through this. The opportunistic one in
/// [`ScatterCoordinator::drain_windows`] used to match `Some(Ok(Ok(part)))`, which
/// **consumed** a failed window's result and threw the error away — `try_join_next`
/// removes the task from the set whichever way it went. The write still failed, but it
/// failed as `body yielded N windows, Content-Length promised M`: a count mismatch
/// describing a body that was fine, with the actual `UploadPart` error logged nowhere.
fn joined_part(
    joined: Result<anyhow::Result<DonePart>, tokio::task::JoinError>,
) -> anyhow::Result<DonePart> {
    joined.map_err(|e| anyhow::anyhow!("window task panicked: {e}"))?
}

/// The owners that took at least one window, deduplicated by node name.
///
/// Commit and discard address a whole upload, so one message per owner is enough
/// however many of its windows it took.
fn distinct_owners(parts: &[DonePart]) -> Vec<NodeId> {
    let mut owners: Vec<NodeId> = Vec::new();
    for part in parts.iter().filter_map(|p| p.owner.as_ref()) {
        if !owners.iter().any(|seen| seen.name() == part.name()) {
            owners.push(part.clone());
        }
    }
    owners
}

/// Upload one window: offer it to its home, and do it here if that does not work.
///
/// Every non-success on the peer path falls through rather than propagating, which
/// is the invariant that makes the scatter an optimization: a refusal, an
/// unreachable owner and a peer error all end with the coordinator uploading the
/// part itself.
async fn upload_window(
    shared: &Shared,
    planned: &crate::scatter::PlannedWindow,
    bucket: &str,
    key: &str,
    upload_id: &str,
    body: Bytes,
) -> anyhow::Result<DonePart> {
    // The one pass over these bytes, here rather than on the body reader's task: this
    // function already runs in its own spawned task, concurrently with the reader, so the
    // CRC costs the client nothing. See `dispatch_window` for what it used to cost.
    let window = window_digest(body).await?;
    if let Some(home) = planned.homes.first() {
        if home.name() != shared.cluster.local_node && may_offer(shared, home.name()) {
            let checksum = window.checksum();
            let offer = StoreOffer {
                chunk_key: &planned.chunk_key,
                upload_id,
                bucket,
                key,
                part_number: planned.part_number,
                body: window.body.clone(),
                checksum_crc32: &checksum,
            };
            let offered = std::time::Instant::now();
            let answer = shared.cluster.transport.store_chunk(home, offer).await;
            observe_offer(shared, &answer, offered.elapsed());
            match answer {
                Ok(StoreOutcome::Uploaded { e_tag }) => {
                    return Ok(DonePart {
                        part_number: planned.part_number,
                        e_tag,
                        owner: Some(home.clone()),
                        staged: true,
                        digest: window.digest,
                    });
                }
                Ok(StoreOutcome::Refused(refusal)) => {
                    debug!(key = %planned.chunk_key, owner = %home.name(), ?refusal,
                        "owner refused; uploading this window here");
                    note_refusal(shared, home, &refusal);
                }
                Err(e) => {
                    warn!(key = %planned.chunk_key, owner = %home.name(), error = %e,
                        "offer failed; uploading this window here");
                }
            }
        }
    }
    upload_here(shared, planned, bucket, key, upload_id, window).await
}

/// Charge one `StoreChunk` round trip to the phase its **outcome** names.
///
/// Three phases rather than one, and the split is the whole point:
///
/// * `owner_rpc` — taken. Covers the wire *and* the owner's own `UploadPart`, so it is
///   not a network measurement however much its name invites reading as one.
/// * `owner_refused` — refused. An owner gates on `try_stage` before it uploads
///   anything, but by then it has already received the window, so this **is** the hop
///   on its own: the same bytes over the same wire with no S3 in it. On an arm with
///   refusals it needs no subtraction to be a wire figure.
/// * `owner_failed` — a transport error, whose duration is a timeout. Kept out of
///   `owner_rpc` because one timeout folded into a few hundred offers moves their mean
///   by more than the quantity being measured.
///
/// Timed by the caller and labelled here, because the label is only known once the
/// await has returned — which is exactly what a drop guard could not do.
fn observe_offer(
    shared: &Shared,
    answer: &Result<StoreOutcome, pacer_transport::TransportError>,
    elapsed: std::time::Duration,
) {
    let phase = match answer {
        Ok(StoreOutcome::Uploaded { .. }) => crate::metrics::SCATTER_PHASE_OWNER_RPC,
        Ok(StoreOutcome::Refused(_)) => crate::metrics::SCATTER_PHASE_OWNER_REFUSED,
        Err(_) => crate::metrics::SCATTER_PHASE_OWNER_FAILED,
    };
    shared.metrics.scatter.observe_phase(phase, elapsed);
}

/// Whether this coordinator should offer to `owner` right now.
fn may_offer(shared: &Shared, owner: &str) -> bool {
    shared
        .tracker
        .lock()
        .expect("saturation tracker lock poisoned")
        .may_offer(owner, std::time::Instant::now())
}

/// Remember a refusal so the next window skips this owner.
fn note_refusal(shared: &Shared, home: &NodeId, refusal: &pacer_transport::StoreRefusal) {
    shared
        .tracker
        .lock()
        .expect("saturation tracker lock poisoned")
        .refused(
            home.name(),
            refusal,
            std::time::Instant::now(),
            shared.cooldown,
        );
}

/// Upload a window as a part from this node, staging it for the later commit.
///
/// Staging is attempted but not required: past the node's budget the window is
/// uploaded and simply not cached (`staged: false`), which is a later miss and never
/// an error. The same fence applies here as to an owner — nothing may be published
/// before Complete — so the bytes have to be held until then or dropped.
async fn upload_here(
    shared: &Shared,
    planned: &crate::scatter::PlannedWindow,
    bucket: &str,
    key: &str,
    upload_id: &str,
    window: Window,
) -> anyhow::Result<DonePart> {
    let body = window.body;
    // A drop guard rather than two `observe` calls, for the reason [`ScopedStage`]
    // exists: this function returns early on a failed `UploadPart`, and a phase charged
    // only on success gets *cheaper* under exactly the conditions worth measuring.
    //
    // Spans the staging attempt as well as the upload, deliberately — that makes it the
    // same two steps `served_stage + served_upload` cover on an owner, so
    // `owner_rpc − local_upload` is a single-node estimate of the wire term beside the
    // cross-node one.
    let _phase = crate::metrics::ScopedStage::new(
        &shared.metrics.scatter.phase_seconds,
        crate::metrics::SCATTER_PHASE_LOCAL_UPLOAD,
    );
    let staged = matches!(
        shared
            .staging
            .try_stage(&planned.chunk_key, upload_id, body.clone()),
        StageOutcome::Staged | StageOutcome::AlreadyStaged
    );
    let out = shared
        .backend
        .upload_part()
        .bucket(bucket)
        .key(key)
        .upload_id(upload_id)
        .part_number(planned.part_number)
        .checksum_crc32(base64_u32(window.digest.clone().finalize()))
        .body(aws_sdk_s3::primitives::ByteStream::from(body))
        .send()
        .await;
    let out = match out {
        Ok(out) => out,
        Err(e) => {
            // Release at once: this node has just proved it cannot serve the
            // window, and holding budget would refuse the next one.
            if staged {
                shared.staging.release(&planned.chunk_key);
            }
            return Err(e.into());
        }
    };
    let e_tag = out
        .e_tag()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow::anyhow!("UploadPart returned no ETag"))?;
    Ok(DonePart {
        part_number: planned.part_number,
        e_tag,
        owner: None,
        staged,
        digest: window.digest,
    })
}

/// Hash one window on the blocking pool, returning it paired with its digest.
///
/// **The only place a scattered window's bytes are read for a CRC**, and it is on the
/// blocking pool because it is a CPU-bound pass over up to `chunk_size` bytes — the one kind
/// of work a tokio worker must not do inline. It is the chunk-sized digest that
/// `planning/26-dcp-throughput-plan.md` § 2.2 named as the last synchronous pass in this
/// path; every other digest of this size in the repo was already off the runtime.
///
/// # Errors
///
/// The blocking task being cancelled, which happens only if the runtime is shutting down —
/// in which case the upload this window belongs to is going to fail anyway, and saying so
/// here is better than a digest silently computed over nothing.
async fn window_digest(body: Bytes) -> anyhow::Result<Window> {
    tokio::task::spawn_blocking(move || {
        let mut digest = crc32fast::Hasher::new();
        digest.update(&body);
        Window { body, digest }
    })
    .await
    .map_err(|e| anyhow::anyhow!("hashing a scatter window was cancelled: {e}"))
}

/// The whole object's CRC32, folded from its parts in **part order**.
///
/// `ChecksumType::FullObject` means S3 checks this against the assembled object at Complete,
/// so the order is not a detail: CRC32 combination is position-dependent, and folding the
/// parts as they happened to finish would produce a digest for a permutation of the object
/// and fail every Complete. `part_number` is the body order by construction (the splitter
/// numbers windows as it emits them), and it is the same key
/// [`ScatterCoordinator::complete`] sorts by.
///
/// `Hasher::combine` is what makes this equal to a single pass over the whole body — proved
/// against one in `the_full_object_digest_is_the_concatenation_in_body_order`, which is the
/// test that would catch a fold that lost the order or the lengths.
fn whole_object_crc32(parts: &[DonePart]) -> u32 {
    let mut ordered: Vec<&DonePart> = parts.iter().collect();
    ordered.sort_unstable_by_key(|p| p.part_number);
    let mut whole = crc32fast::Hasher::new();
    for part in ordered {
        whole.combine(&part.digest);
    }
    whole.finalize()
}

/// Base64 of a big-endian `u32`, which is how S3 carries a CRC32.
///
/// Hand-rolled rather than pulling in an encoder, the same way `delivery` hand-rolls
/// its decoder: the input is always exactly four bytes, so this is one fixed group
/// plus a partial one, and the general case never arises. Pinned by a test against
/// the value S3 itself reported in `spike/mpu-scatter`.
fn base64_u32(value: u32) -> String {
    /// Standard base64 alphabet (RFC 4648), which is what S3 expects — not the
    /// URL-safe variant.
    const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    /// Bits a base64 character carries.
    const BITS_PER_CHAR: u32 = 6;
    let bytes = value.to_be_bytes();
    // 4 bytes = 32 bits = five full 6-bit characters plus 2 leftover bits, which
    // become a sixth character, then one padding group.
    let packed = u64::from(value) << (BITS_PER_CHAR - (u32::BITS % BITS_PER_CHAR));
    let mut out = String::with_capacity(8);
    for i in 0..6 {
        let shift = 30 - i * BITS_PER_CHAR;
        let index = ((packed >> shift) & 0x3f) as usize;
        out.push(ALPHABET[index] as char);
    }
    debug_assert_eq!(bytes.len(), 4, "the shift arithmetic assumes four bytes");
    out.push_str("==");
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `DonePart` carrying nothing but what [`whole_object_crc32`] reads: its number and
    /// the digest of its own bytes.
    fn part_of(part_number: i32, body: &[u8]) -> DonePart {
        let mut digest = crc32fast::Hasher::new();
        digest.update(body);
        DonePart {
            part_number,
            e_tag: String::new(),
            owner: None,
            staged: true,
            digest,
        }
    }

    /// The digest handed to Complete must be the CRC32 of the whole body as the client sent
    /// it, whatever order windows finish in.
    ///
    /// Since 2026-09-08 that is a `Hasher::combine` fold over the parts rather than a running
    /// pass on the body reader's task, which moves TWO risks into this test: that combining
    /// per-window digests really equals one pass over the concatenation (it needs each
    /// window's LENGTH, which is why a `u32` per part would not do), and that the fold is by
    /// `part_number` and not by completion order — parts arrive from a `JoinSet` in whatever
    /// order S3 answers, and CRC32 combination is position-dependent, so a fold in arrival
    /// order would hand S3 the digest of a permutation and fail every Complete. (The base64
    /// encoding is pinned separately, against a value S3 itself accepted, in
    /// [`the_checksum_encoding_matches_what_s3_reported`].)
    #[test]
    fn the_full_object_digest_is_the_concatenation_in_body_order() {
        let windows: Vec<Bytes> = vec![
            Bytes::from_static(b"first-"),
            Bytes::from_static(b"second-"),
            Bytes::from_static(b"third"),
        ];
        let whole: Vec<u8> = windows.iter().flat_map(|w| w.to_vec()).collect();
        let mut once = crc32fast::Hasher::new();
        once.update(&whole);
        let whole_crc = once.finalize();

        // Deliberately built in the WRONG order — 3, 1, 2 — because that is what a `JoinSet`
        // can hand back and the function's whole contract is that it does not matter.
        let shuffled = vec![
            part_of(3, &windows[2]),
            part_of(1, &windows[0]),
            part_of(2, &windows[1]),
        ];
        assert_eq!(whole_object_crc32(&shuffled), whole_crc);

        // And the fold is really order-SENSITIVE, so the assertion above is evidence that
        // the sort ran rather than that any fold would have passed.
        let mislabelled = vec![
            part_of(1, &windows[2]),
            part_of(2, &windows[0]),
            part_of(3, &windows[1]),
        ];
        assert_ne!(
            whole_object_crc32(&mislabelled),
            whole_crc,
            "a permuted fold matched the whole body, so this test cannot detect a lost sort"
        );
    }

    /// One window's `checksum()` covers exactly its own bytes, and reading it twice gives the
    /// same answer.
    ///
    /// The second half is the real risk: `crc32fast::Hasher::finalize` takes `self`, so
    /// `checksum` has to clone. The same hasher is folded into the whole-object CRC after the
    /// part checksum is read, and a `checksum` that consumed it would leave the fold reading a
    /// reset hasher — a Complete that fails on every scattered PUT.
    #[tokio::test]
    async fn a_windows_checksum_covers_only_that_window_and_survives_being_read() {
        let body = Bytes::from_static(b"second-");
        let window = window_digest(body.clone()).await.expect("hashing a window");
        let mut expected = crc32fast::Hasher::new();
        expected.update(&body);
        let want = base64_u32(expected.finalize());
        assert_eq!(window.checksum(), want);
        assert_eq!(window.checksum(), want, "checksum() consumed the digest");
        assert_eq!(
            window.body, body,
            "the window must hand back the bytes it hashed"
        );
    }

    /// Slots for [`the_windows_in_flight_peak_latches_when_slots_come_back`], more than
    /// the two it takes so a peak below the limit is distinguishable from the limit.
    const TEST_SLOTS: usize = 4;

    /// The in-flight peak must LATCH: it rises as slots are taken and does not fall
    /// back when they are returned.
    ///
    /// The failure mode this exists to catch is a high-water mark that quietly behaves
    /// like a sample. Every slot is released by the time a PUT finishes, so a sampled
    /// gauge reads 0 on almost every scrape and an arm would conclude that a pipeline
    /// which actually ran full never filled at all — on the one arm the series exists
    /// for. Asserted on [`WindowSlots`] directly, so it is exact rather than a race
    /// with an upload: `dispatch`'s use of it is pinned separately by
    /// `tests/daemon/scatter/gate_pipeline.rs::a_full_pipeline_stops_the_body_read`.
    #[tokio::test]
    async fn the_windows_in_flight_peak_latches_when_slots_come_back() {
        let slots = WindowSlots::new(TEST_SLOTS);
        assert_eq!(slots.limit(), TEST_SLOTS);
        assert_eq!(slots.in_flight(), 0);
        assert_eq!(slots.peak(), 0, "nothing dispatched, nothing peaked");

        let first = slots.acquire().await.expect("a free slot");
        let second = slots.acquire().await.expect("a free slot");
        assert_eq!(slots.in_flight(), 2);
        assert_eq!(slots.peak(), 2, "the peak follows the pipeline up");

        drop(first);
        drop(second);
        assert_eq!(slots.in_flight(), 0, "a returned slot is free again");
        assert_eq!(
            slots.peak(),
            2,
            "the peak must survive the uploads finishing — a peak that drains with the \
             pipeline is a sample wearing a peak's name, and reads 0 on every scrape \
             taken between two PUTs"
        );

        let third = slots.acquire().await.expect("a free slot");
        assert_eq!(
            slots.peak(),
            2,
            "a shallower later pipeline must not lower the mark"
        );
        drop(third);
    }

    /// A zero `windowsInFlight` would deadlock every scatter, so it is clamped.
    #[test]
    fn a_zero_window_limit_is_clamped_to_one_slot() {
        assert_eq!(WindowSlots::new(0).limit(), 1);
    }

    /// S3 wants base64 of the big-endian four bytes. Pinned against the exact value
    /// `spike/mpu-scatter` observed S3 report for a 27 MiB probe object, so an
    /// endianness slip cannot pass.
    #[test]
    fn the_checksum_encoding_matches_what_s3_reported() {
        assert_eq!(base64_u32(0x8b1d_7a0b), "ix16Cw==");
    }
}
