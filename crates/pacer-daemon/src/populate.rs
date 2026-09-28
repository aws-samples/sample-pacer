//! `auth.mode: requester`'s write tee (ADR-0041 § "requester" point 2, ADR-0032's populate
//! half): the bytes of a client's `PutObject` or `UploadPart` are cloned onto the chunk grid
//! as they stream through, staged at their homes, and committed under the object's ETag once
//! S3 says the object exists. The daemon never uploads — it has no identity to upload with.
//!
//! Everything here is warmth, never correctness. A window that is refused, dropped for want
//! of a slot, or in doubt for any reason is simply not populated; a read of it is a miss. And
//! every committed window carries the object's ETag as its version witness, which a
//! requester-mode read checks against its own authorization request before serving it.
//!
//! The pieces with no I/O — where a part's bytes land on the grid ([`PartPlacer`]), how an
//! `aws-chunked` body decodes ([`AwsChunkedDecoder`]) — come first and are tested
//! exhaustively on their own. [`WriteTee`] and [`TeeBody`] are the runtime.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::Bytes;
use pacer_cache::chunk::{CachedChunk, ChunkConfig};
use pacer_cache::tier::ChunkTier;
use pacer_ring::directory::Tier;
use pacer_ring::NodeId;
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::warn;

use crate::metrics::Metrics;
use crate::proxy::Cluster;
use crate::scatter::WindowSplitter;
use crate::staging::{StageOutcome, StagingArea};

/// `outcome` labels of `pacer_populate_windows_total` the tee adds to the read path's
/// `committed`/`refused`/`failed`. A window dropped because no slot was free — the tee never
/// makes the writer wait.
const OUTCOME_SKIPPED: &str = "skipped";
/// A window staged for an upload that was then discarded: the write failed, was aborted, or
/// its placement did not verify.
const OUTCOME_DISCARDED: &str = "discarded";
/// See `proxy::fill`'s constants of the same names; one series, one vocabulary.
const OUTCOME_COMMITTED: &str = "committed";
const OUTCOME_REFUSED: &str = "refused";
const OUTCOME_FAILED: &str = "failed";

/// How long a commit or discard waits for window offers still in flight to their homes.
/// An offer is one `StoreChunk` round trip, milliseconds in the same region; past this the
/// window is simply left to its owner's staging TTL.
const OFFER_SETTLE: Duration = Duration::from_secs(30);

/// The write tee's node-wide state (see the module header). Cheap to clone.
#[derive(Clone)]
pub struct WriteTee {
    shared: Arc<Shared>,
}

struct Shared {
    staging: Arc<StagingArea>,
    cluster: Option<Cluster>,
    tier: ChunkTier,
    chunk: ChunkConfig,
    /// Windows held between leaving a body and being staged: the tee's memory bound.
    slots: Arc<Semaphore>,
    /// Bodies being teed at once, each holding at most one partial window.
    bodies: Arc<Semaphore>,
    records: Mutex<HashMap<String, Record>>,
    /// Open uploads tracked at once; past it a new upload is forwarded untouched.
    max_records: usize,
    /// A record older than this is an upload that never completed; it is dropped, and its
    /// staged windows are left to the staging TTL, which is this same value.
    ttl: Duration,
    metrics: Metrics,
    sequence: AtomicU64,
}

/// One upload being teed: a multipart upload between Create and Complete, or one PUT.
struct Record {
    bucket: String,
    key: String,
    /// `None` for a PUT, whose single body always starts at chunk 0.
    placer: Option<PartPlacer>,
    /// Peers a window was offered to, so commit and discard reach every one.
    homes: Vec<NodeId>,
    inflight: Arc<InFlight>,
    opened: Instant,
    tainted: bool,
}

/// The object one body writes: where its windows' chunk keys and offers come from.
struct Target {
    bucket: String,
    key: String,
    object_key: String,
}

#[derive(Default)]
struct InFlight {
    count: AtomicUsize,
    idle: Notify,
}

impl InFlight {
    fn start(&self) {
        self.count.fetch_add(1, Ordering::SeqCst);
    }

    fn finish(&self) {
        if self.count.fetch_sub(1, Ordering::SeqCst) == 1 {
            self.idle.notify_waiters();
        }
    }

    async fn settled(&self) {
        let wait = async {
            while self.count.load(Ordering::SeqCst) > 0 {
                let notified = self.idle.notified();
                if self.count.load(Ordering::SeqCst) == 0 {
                    break;
                }
                notified.await;
            }
        };
        let _ = tokio::time::timeout(OFFER_SETTLE, wait).await;
    }
}

/// How one body announces itself to the tee.
pub struct BodyShape {
    /// The object's bytes in this body — `x-amz-decoded-content-length` for an `aws-chunked`
    /// body, `Content-Length` otherwise.
    pub len: u64,
    /// Whether the body is `aws-chunked` and must be decoded before it is the object.
    pub aws_chunked: bool,
}

impl WriteTee {
    /// A tee over `staging` — the same area the peer server stages its homes' windows in —
    /// holding at most `windows_in_flight` windows and as many bodies at once.
    #[must_use]
    pub fn new(
        staging: Arc<StagingArea>,
        cluster: Option<Cluster>,
        tier: ChunkTier,
        chunk: ChunkConfig,
        windows_in_flight: usize,
        ttl: Duration,
        metrics: Metrics,
    ) -> Self {
        let limit = windows_in_flight.max(1);
        Self {
            shared: Arc::new(Shared {
                staging,
                cluster,
                tier,
                chunk,
                slots: Arc::new(Semaphore::new(limit)),
                bodies: Arc::new(Semaphore::new(limit)),
                records: Mutex::new(HashMap::new()),
                max_records: limit,
                ttl,
                metrics,
                sequence: AtomicU64::new(0),
            }),
        }
    }

    /// Start tracking multipart upload `upload_id` of `bucket`/`key`, after S3 created it.
    /// Returns `false` when the tee is full, in which case its parts are not teed.
    pub fn open_upload(&self, bucket: &str, key: &str, upload_id: &str) -> bool {
        let placer = Some(PartPlacer::new(self.shared.chunk.chunk_size()));
        self.shared.open(mpu_id(upload_id), bucket, key, placer)
    }

    /// Tee part `part_number` of `upload_id`, or `None` when this part populates nothing —
    /// the upload is not tracked or is for another object, the part lands off the chunk
    /// grid, or the tee has no room for another body.
    #[must_use]
    pub fn begin_part(
        &self,
        target: (&str, &str, &str),
        part_number: i32,
        shape: &BodyShape,
    ) -> Option<PartTee> {
        let (bucket, key, upload_id) = target;
        let id = mpu_id(upload_id);
        let first_chunk = {
            let mut records = self.shared.lock();
            let record = records.get_mut(&id)?;
            if record.bucket != bucket || record.key != key {
                return None;
            }
            record.placer.as_mut()?.observe(part_number, shape.len)
        };
        // A part stages only the full windows it contains: its short tail is a chunk only
        // if this is the upload's final part, which nothing says until Complete.
        self.shared.body(id, first_chunk?, shape, false)
    }

    /// Tee one `PutObject` body. Returns the id to [`Self::commit`] or [`Self::discard`] it
    /// under once S3 answers, and the tee to wrap the body in.
    #[must_use]
    pub fn begin_put(
        &self,
        bucket: &str,
        key: &str,
        shape: &BodyShape,
    ) -> Option<(String, PartTee)> {
        let n = self.shared.sequence.fetch_add(1, Ordering::Relaxed);
        let id = format!("put:{bucket}/{key}:{n}");
        if !self.shared.open(id.clone(), bucket, key, None) {
            return None;
        }
        // A PUT's body is the whole object, so its tail is the object's final chunk.
        let Some(tee) = self.shared.body(id.clone(), 0, shape, true) else {
            self.shared.lock().remove(&id);
            return None;
        };
        Some((id, tee))
    }

    /// Make `upload_id`'s staging untrustworthy — a part failed at S3 — so its Complete
    /// discards rather than commits.
    pub fn taint_upload(&self, upload_id: &str) {
        self.shared.taint(&mpu_id(upload_id));
    }

    /// Complete for `upload_id` succeeded as `e_tag`, naming `ordered_parts`: commit if the
    /// placement verifies, discard otherwise.
    pub async fn complete(&self, upload_id: &str, ordered_parts: &[i32], e_tag: &str) {
        let id = mpu_id(upload_id);
        {
            let mut records = self.shared.lock();
            if let Some(record) = records.get_mut(&id) {
                let verified = record
                    .placer
                    .as_ref()
                    .is_some_and(|p| p.verify(ordered_parts));
                record.tainted |= !verified;
            }
        }
        self.shared.settle(&id, Some(e_tag)).await;
    }

    /// The write behind `id` succeeded as `e_tag`: make its windows visible.
    pub async fn commit(&self, id: &str, e_tag: &str) {
        self.shared.settle(id, Some(e_tag)).await;
    }

    /// The write behind `id` (a PUT id, or an upload id's) failed or was aborted.
    pub async fn discard(&self, id: &str) {
        self.shared.settle(id, None).await;
    }

    /// [`Self::discard`] for multipart upload `upload_id`.
    pub async fn discard_upload(&self, upload_id: &str) {
        self.discard(&mpu_id(upload_id)).await;
    }
}

/// The staging id of multipart upload `upload_id`.
fn mpu_id(upload_id: &str) -> String {
    format!("mpu:{upload_id}")
}

impl Shared {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Record>> {
        self.records
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn open(&self, id: String, bucket: &str, key: &str, placer: Option<PartPlacer>) -> bool {
        let mut records = self.lock();
        let ttl = self.ttl;
        records.retain(|_, r| r.opened.elapsed() < ttl);
        if records.len() >= self.max_records {
            return false;
        }
        records.insert(
            id,
            Record {
                bucket: bucket.to_owned(),
                key: key.to_owned(),
                placer,
                homes: Vec::new(),
                inflight: Arc::default(),
                opened: Instant::now(),
                tainted: false,
            },
        );
        true
    }

    fn body(
        self: &Arc<Self>,
        id: String,
        first_chunk: u64,
        shape: &BodyShape,
        keep_tail: bool,
    ) -> Option<PartTee> {
        let permit = Arc::clone(&self.bodies).try_acquire_owned().ok()?;
        let target = {
            let records = self.lock();
            let record = records.get(&id)?;
            Arc::new(Target {
                object_key: pacer_cache::object_key(&record.bucket, &record.key),
                bucket: record.bucket.clone(),
                key: record.key.clone(),
            })
        };
        Some(PartTee {
            shared: Arc::clone(self),
            id,
            target,
            next_chunk: first_chunk,
            splitter: WindowSplitter::new(usize::try_from(self.chunk.chunk_size()).ok()?),
            decoder: shape.aws_chunked.then(AwsChunkedDecoder::new),
            expected: shape.len,
            seen: 0,
            done: false,
            failed: false,
            keep_tail,
            _permit: permit,
        })
    }

    fn taint(&self, id: &str) {
        if let Some(record) = self.lock().get_mut(id) {
            record.tainted = true;
        }
    }

    fn count(&self, outcome: &str, n: u64) {
        self.metrics
            .authz
            .populate_windows
            .with_label_values(&[outcome])
            .inc_by(n);
    }

    /// Stage window `index` of `object_key` for upload `id` at its home: here, through the
    /// shared staging area, or at a peer as a populate-only offer.
    fn dispatch(self: &Arc<Self>, id: &str, target: &Arc<Target>, index: u64, body: Bytes) {
        let Ok(permit) = Arc::clone(&self.slots).try_acquire_owned() else {
            self.count(OUTCOME_SKIPPED, 1);
            return;
        };
        let chunk_key = self.chunk.chunk_key(&target.object_key, index);
        let home = self.cluster.as_ref().and_then(|c| {
            c.ring
                .homes(&chunk_key, c.replication_r)
                .into_iter()
                .next()
                .filter(|h| h.name() != c.local_node)
        });
        let Some(home) = home else {
            match self.staging.try_stage(&chunk_key, id, body) {
                StageOutcome::Staged | StageOutcome::AlreadyStaged => {}
                StageOutcome::Refused(_) => self.count(OUTCOME_REFUSED, 1),
            }
            return;
        };
        let inflight = {
            let mut records = self.lock();
            let Some(record) = records.get_mut(id) else {
                return;
            };
            if !record.homes.iter().any(|h| h.name() == home.name()) {
                record.homes.push(home.clone());
            }
            Arc::clone(&record.inflight)
        };
        inflight.start();
        let shared = Arc::clone(self);
        let (id, target) = (id.to_owned(), Arc::clone(target));
        tokio::spawn(async move {
            shared
                .offer(&home, &id, &target, &chunk_key, index, body)
                .await;
            inflight.finish();
            drop(permit);
        });
    }

    async fn offer(
        &self,
        home: &NodeId,
        id: &str,
        target: &Target,
        chunk_key: &str,
        index: u64,
        body: Bytes,
    ) {
        let Some(cluster) = &self.cluster else {
            return;
        };
        let offer = pacer_transport::StoreOffer {
            chunk_key,
            upload_id: id,
            bucket: &target.bucket,
            key: &target.key,
            part_number: i32::try_from(index + 1).unwrap_or(i32::MAX),
            body,
            checksum_crc32: "",
            populate_only: true,
        };
        match cluster.transport.store_chunk(home, offer).await {
            Ok(pacer_transport::StoreOutcome::Staged) => {}
            Ok(pacer_transport::StoreOutcome::Refused(_)) => self.count(OUTCOME_REFUSED, 1),
            Ok(pacer_transport::StoreOutcome::Uploaded { .. }) | Err(_) => {
                self.count(OUTCOME_FAILED, 1);
            }
        }
    }

    /// Finish upload `id`: commit its windows under `e_tag`, or discard them when `e_tag` is
    /// `None` or the record was tainted. Waits for offers still in flight first, so a window
    /// that reaches its home after the commit is not stranded until its TTL.
    async fn settle(&self, id: &str, e_tag: Option<&str>) {
        let Some(record) = self.lock().remove(id) else {
            return;
        };
        record.inflight.settled().await;
        match e_tag.filter(|_| !record.tainted) {
            Some(e_tag) => self.commit(id, &record.homes, e_tag).await,
            None => self.discard(id, &record.homes).await,
        }
    }

    async fn commit(&self, id: &str, homes: &[NodeId], e_tag: &str) {
        let mut committed = 0u64;
        for (chunk_key, body) in self.staging.commit(id) {
            if let Err(e) = self
                .tier
                .put_chunk(&chunk_key, CachedChunk::versioned(body, e_tag.to_owned()))
                .await
            {
                warn!(key = %chunk_key, error = %e, "teed chunk could not reach the disk tier");
                continue;
            }
            if let Some(c) = &self.cluster {
                c.directory
                    .admit_next(&chunk_key, &c.local_node, Tier::Dram);
            }
            committed += 1;
        }
        if let Some(cluster) = &self.cluster {
            for home in homes {
                match cluster.transport.commit_upload(home, id, e_tag).await {
                    Ok(n) => committed += u64::from(n),
                    Err(e) => warn!(home = %home.name(), error = %e,
                        "tee commit failed; those windows wait for the home's staging TTL"),
                }
            }
        }
        self.count(OUTCOME_COMMITTED, committed);
    }

    async fn discard(&self, id: &str, homes: &[NodeId]) {
        let mut discarded = self.staging.discard(id) as u64;
        if let Some(cluster) = &self.cluster {
            for home in homes {
                if let Ok(n) = cluster.transport.discard_upload(home, id).await {
                    discarded += u64::from(n);
                }
            }
        }
        self.count(OUTCOME_DISCARDED, discarded);
    }
}

/// The tee for one body: decodes it if it is `aws-chunked`, cuts it onto the chunk grid and
/// dispatches each window. Owned by the [`TeeBody`] the body is wrapped in.
pub struct PartTee {
    shared: Arc<Shared>,
    id: String,
    target: Arc<Target>,
    next_chunk: u64,
    splitter: WindowSplitter,
    decoder: Option<AwsChunkedDecoder>,
    expected: u64,
    seen: u64,
    done: bool,
    failed: bool,
    /// Whether the short window left at the body's end is a chunk: yes for a PUT (the body
    /// is the whole object), no for a multipart part (see [`WriteTee::begin_part`]).
    keep_tail: bool,
    _permit: OwnedSemaphorePermit,
}

impl PartTee {
    fn feed(&mut self, data: &Bytes) {
        if self.failed || self.done {
            return;
        }
        let mut pieces = Vec::new();
        match &mut self.decoder {
            Some(decoder) => {
                if decoder.feed(data, &mut pieces).is_err() {
                    return self.fail();
                }
            }
            None => pieces.push(data.clone()),
        }
        for piece in pieces {
            self.seen += piece.len() as u64;
            self.splitter.push(piece);
        }
        if self.seen > self.expected {
            return self.fail();
        }
        while let Some(window) = self.splitter.take_window() {
            self.emit(window);
        }
    }

    fn finish(&mut self) {
        if self.done {
            return;
        }
        self.done = true;
        let decoded = self
            .decoder
            .as_ref()
            .is_none_or(AwsChunkedDecoder::is_complete);
        if !decoded || self.seen != self.expected {
            return self.fail();
        }
        if !self.failed && self.keep_tail {
            if let Some(tail) = self.splitter.take_tail() {
                self.emit(tail);
            }
        }
    }

    fn fail(&mut self) {
        self.failed = true;
        self.done = true;
        self.shared.taint(&self.id);
    }

    fn emit(&mut self, window: Bytes) {
        self.shared
            .dispatch(&self.id, &self.target, self.next_chunk, window);
        self.next_chunk += 1;
    }
}

impl Drop for PartTee {
    /// A body dropped before its end — the client went away, or the forward failed — is a
    /// write S3 did not receive whole; whatever it staged must not be committed.
    fn drop(&mut self) {
        if !self.done {
            self.fail();
        }
    }
}

/// A request body wrapped so its data frames reach a [`PartTee`] as they stream out.
/// Frames are yielded unchanged and the tee only ever clones a `Bytes` handle, so the
/// forwarded request is exactly the one the client sent.
pub struct TeeBody<B> {
    inner: B,
    tee: Option<PartTee>,
}

impl<B> TeeBody<B> {
    /// Wrap `inner`, teeing it through `tee` when there is one.
    pub fn new(inner: B, tee: Option<PartTee>) -> Self {
        Self { inner, tee }
    }
}

impl<B> http_body::Body for TeeBody<B>
where
    B: http_body::Body<Data = Bytes> + Unpin,
{
    type Data = Bytes;
    type Error = B::Error;

    fn poll_frame(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<http_body::Frame<Bytes>, Self::Error>>> {
        let this = self.get_mut();
        let polled = Pin::new(&mut this.inner).poll_frame(cx);
        if let Some(tee) = this.tee.as_mut() {
            match &polled {
                Poll::Ready(Some(Ok(frame))) => {
                    if let Some(data) = frame.data_ref() {
                        tee.feed(data);
                    }
                    // hyper stops polling a body of known length once it reports its end,
                    // so the final `None` below may never be seen.
                    if this.inner.is_end_stream() {
                        tee.finish();
                    }
                }
                Poll::Ready(Some(Err(_))) => tee.fail(),
                Poll::Ready(None) => tee.finish(),
                Poll::Pending => {}
            }
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> http_body::SizeHint {
        self.inner.size_hint()
    }
}

/// Where the parts of one multipart upload land on the chunk grid (planning/30 § 3.2).
///
/// A part does not carry its byte offset: offset(N) is the sum of the lengths of parts
/// `1..N`, which arrive concurrently and in any order. So a part is placed under a
/// hypothesis — every part is as long as the longest seen so far, `P` — and the hypothesis is
/// **verified** at Complete, when the ordered part list makes the true offsets arithmetic. Any
/// disagreement discards the whole upload's staging: warmth lost, never a wrong byte.
#[derive(Debug)]
pub struct PartPlacer {
    chunk_size: u64,
    /// `P`: the longest part observed so far.
    hypothesis: u64,
    /// Each observed part's length and the offset it was placed at.
    parts: BTreeMap<i32, Observed>,
    /// Set once anything makes the staged windows untrustworthy: a part seen twice (a retry
    /// may carry different bytes, and S3 keeps the last), a body that failed or ended short.
    tainted: bool,
}

#[derive(Debug, Clone, Copy)]
struct Observed {
    len: u64,
    assumed_offset: u64,
}

impl PartPlacer {
    /// A placer for an upload on a `chunk_size` grid.
    #[must_use]
    pub fn new(chunk_size: u64) -> Self {
        Self {
            chunk_size,
            hypothesis: 0,
            parts: BTreeMap::new(),
            tainted: false,
        }
    }

    /// Record part `part_number` of `len` bytes and say which chunk its first byte starts —
    /// `None` when this part must populate nothing: it lands off the chunk grid (a part size
    /// that is not a multiple of the chunk size), or it has been seen before.
    pub fn observe(&mut self, part_number: i32, len: u64) -> Option<u64> {
        if part_number < 1 || self.parts.contains_key(&part_number) {
            self.tainted = true;
            return None;
        }
        self.hypothesis = self.hypothesis.max(len);
        let index = u64::try_from(part_number - 1).ok()?;
        let assumed_offset = index.checked_mul(self.hypothesis)?;
        self.parts.insert(
            part_number,
            Observed {
                len,
                assumed_offset,
            },
        );
        (assumed_offset % self.chunk_size == 0).then_some(assumed_offset / self.chunk_size)
    }

    /// Mark the upload's staging untrustworthy — a part's body failed or its upload was
    /// refused — so [`Self::verify`] discards it.
    pub fn taint(&mut self) {
        self.tainted = true;
    }

    /// Whether the windows staged for this upload may be committed, given the part list
    /// Complete named, in order. True only if nothing tainted the upload, the parts observed
    /// are exactly the parts listed, and every part was placed at its true offset.
    #[must_use]
    pub fn verify(&self, ordered_parts: &[i32]) -> bool {
        if self.tainted || ordered_parts.len() != self.parts.len() {
            return false;
        }
        let mut offset = 0u64;
        let mut previous = 0;
        for &part in ordered_parts {
            let Some(observed) = self.parts.get(&part) else {
                return false;
            };
            if part <= previous || observed.assumed_offset != offset {
                return false;
            }
            previous = part;
            offset += observed.len;
        }
        true
    }
}

/// Longest `aws-chunked` chunk-size line accepted, bytes. The line is a hex size plus an
/// optional `;chunk-signature=<64 hex>` extension — under 100 bytes in practice. A line past
/// this is not the encoding this decoder knows, and it gives up rather than buffer it.
const MAX_CHUNK_LINE: usize = 256;

/// A streaming decoder for the `aws-chunked` content encoding SigV4 uses for streaming and
/// trailing-checksum uploads, yielding the object bytes as zero-copy slices of the input.
///
/// A body sent this way is **not** the object: each chunk is framed by a hex size line,
/// optionally signed, and the body may end in checksum trailers. Teeing it undecoded would
/// cache the framing as object content. Anything this decoder does not recognise fails the
/// decode, and a failed decode populates nothing.
#[derive(Debug)]
pub struct AwsChunkedDecoder {
    state: DecodeState,
    line: Vec<u8>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DecodeState {
    /// Reading a chunk-size line up to its CRLF.
    SizeLine,
    /// Inside a chunk's data, with this many bytes left.
    Data(u64),
    /// Expecting the CRLF that ends a chunk's data; `true` once the CR has been seen.
    DataEnd(bool),
    /// After the zero-length chunk: trailers, which carry no object bytes.
    Trailers,
    /// The input did not follow the encoding.
    Failed,
}

/// The decode failed: the body is not `aws-chunked` as this decoder understands it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("body is not valid aws-chunked encoding")]
pub struct DecodeError;

impl Default for AwsChunkedDecoder {
    fn default() -> Self {
        Self::new()
    }
}

impl AwsChunkedDecoder {
    /// A decoder at the start of a body.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: DecodeState::SizeLine,
            line: Vec::new(),
        }
    }

    /// Decode the next frame of the body, appending the object bytes it carries to `out`.
    ///
    /// # Errors
    ///
    /// [`DecodeError`] once the input stops following the encoding; every later call fails.
    pub fn feed(&mut self, input: &Bytes, out: &mut Vec<Bytes>) -> Result<(), DecodeError> {
        let mut at = 0;
        while at < input.len() {
            at = self.step(input, at, out)?;
        }
        Ok(())
    }

    /// Whether the body ended where the encoding says it may: after the zero-length chunk.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.state == DecodeState::Trailers
    }

    /// Consume input from `at`, returning where the next step starts.
    fn step(
        &mut self,
        input: &Bytes,
        at: usize,
        out: &mut Vec<Bytes>,
    ) -> Result<usize, DecodeError> {
        match self.state {
            DecodeState::Failed => Err(DecodeError),
            DecodeState::Trailers => Ok(input.len()),
            DecodeState::Data(left) => {
                let take =
                    usize::try_from(left).map_or(input.len() - at, |l| l.min(input.len() - at));
                out.push(input.slice(at..at + take));
                let left = left - take as u64;
                self.state = if left == 0 {
                    DecodeState::DataEnd(false)
                } else {
                    DecodeState::Data(left)
                };
                Ok(at + take)
            }
            DecodeState::DataEnd(seen_cr) => {
                let expected = if seen_cr { b'\n' } else { b'\r' };
                if input[at] != expected {
                    return self.fail();
                }
                self.state = if seen_cr {
                    DecodeState::SizeLine
                } else {
                    DecodeState::DataEnd(true)
                };
                Ok(at + 1)
            }
            DecodeState::SizeLine => self.size_line(input, at),
        }
    }

    /// Accumulate a chunk-size line and, at its LF, parse it.
    fn size_line(&mut self, input: &Bytes, at: usize) -> Result<usize, DecodeError> {
        let Some(lf) = input[at..].iter().position(|&b| b == b'\n') else {
            self.line.extend_from_slice(&input[at..]);
            return if self.line.len() > MAX_CHUNK_LINE {
                self.fail()
            } else {
                Ok(input.len())
            };
        };
        self.line.extend_from_slice(&input[at..at + lf]);
        let Some(line) = self.line.strip_suffix(b"\r") else {
            return self.fail();
        };
        let size = line.split(|&b| b == b';').next().unwrap_or_default();
        let parsed = std::str::from_utf8(size)
            .ok()
            .and_then(|s| u64::from_str_radix(s.trim(), 16).ok());
        let Some(size) = parsed else {
            return self.fail();
        };
        self.line.clear();
        self.state = if size == 0 {
            DecodeState::Trailers
        } else {
            DecodeState::Data(size)
        };
        Ok(at + lf + 1)
    }

    fn fail<T>(&mut self) -> Result<T, DecodeError> {
        self.state = DecodeState::Failed;
        Err(DecodeError)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHUNK: u64 = 16;

    /// Planning/30 § 7's T1 table: uniform parts in order and out of order; a short last
    /// part arriving first; non-uniform parts; a part size off the grid; a part four chunks
    /// long.
    #[test]
    fn uniform_parts_place_and_verify_in_any_order() {
        for order in [[1, 2, 3], [3, 1, 2], [2, 3, 1]] {
            let mut p = PartPlacer::new(CHUNK);
            for part in order {
                let first = p.observe(part, CHUNK);
                assert_eq!(
                    first,
                    Some(u64::try_from(part - 1).unwrap()),
                    "order {order:?}"
                );
            }
            assert!(p.verify(&[1, 2, 3]), "order {order:?}");
        }
    }

    #[test]
    fn a_short_last_part_arriving_last_verifies() {
        let mut p = PartPlacer::new(CHUNK);
        p.observe(1, CHUNK);
        p.observe(2, CHUNK);
        assert_eq!(p.observe(3, 5), Some(2));
        assert!(p.verify(&[1, 2, 3]));
    }

    #[test]
    fn a_short_last_part_arriving_first_is_discarded() {
        // Off the grid: placed at 2 x 5 = 10, so it populates nothing, and verify still says no.
        let mut p = PartPlacer::new(CHUNK);
        assert_eq!(p.observe(3, 5), None);
        p.observe(1, CHUNK);
        p.observe(2, CHUNK);
        assert!(!p.verify(&[1, 2, 3]));

        // The dangerous case: the wrong hypothesis lands ON the grid (2 x 8 = 16, chunk 1)
        // and stages bytes at the wrong chunk. Only verify can catch it, and it must.
        let mut p = PartPlacer::new(CHUNK);
        assert_eq!(
            p.observe(3, CHUNK / 2),
            Some(1),
            "wrongly placed at chunk 1"
        );
        p.observe(1, CHUNK);
        p.observe(2, CHUNK);
        assert!(!p.verify(&[1, 2, 3]), "part 3's true offset is 32, not 16");
    }

    #[test]
    fn non_uniform_parts_are_discarded() {
        let mut p = PartPlacer::new(CHUNK);
        p.observe(1, 2 * CHUNK);
        p.observe(2, CHUNK);
        p.observe(3, 2 * CHUNK);
        assert!(!p.verify(&[1, 2, 3]));
    }

    #[test]
    fn a_part_size_off_the_grid_populates_nothing_and_errs_nowhere() {
        let mut p = PartPlacer::new(CHUNK);
        assert_eq!(
            p.observe(1, CHUNK / 2),
            Some(0),
            "part 1 always starts at 0"
        );
        assert_eq!(
            p.observe(2, CHUNK / 2),
            None,
            "8 bytes in: not a chunk boundary"
        );
        assert_eq!(
            p.observe(3, CHUNK / 2),
            Some(1),
            "16 bytes in happens to be one"
        );
        assert!(
            p.verify(&[1, 2, 3]),
            "placement was right, so nothing to discard"
        );
    }

    #[test]
    fn a_part_of_four_chunks_places_four_chunks_apart() {
        let mut p = PartPlacer::new(CHUNK);
        assert_eq!(p.observe(1, 4 * CHUNK), Some(0));
        assert_eq!(p.observe(2, 4 * CHUNK), Some(4));
        assert!(p.verify(&[1, 2]));
    }

    #[test]
    fn a_part_seen_twice_or_a_taint_discards_the_upload() {
        let mut p = PartPlacer::new(CHUNK);
        p.observe(1, CHUNK);
        assert_eq!(
            p.observe(1, CHUNK),
            None,
            "a retry may carry different bytes"
        );
        assert!(!p.verify(&[1]));

        let mut p = PartPlacer::new(CHUNK);
        p.observe(1, CHUNK);
        p.taint();
        assert!(!p.verify(&[1]));
    }

    #[test]
    fn a_complete_that_names_other_parts_is_discarded() {
        let mut p = PartPlacer::new(CHUNK);
        p.observe(1, CHUNK);
        p.observe(2, CHUNK);
        assert!(
            !p.verify(&[1]),
            "part 2 was staged but is not in the object"
        );
        assert!(!p.verify(&[1, 2, 3]), "part 3 was never seen");
        assert!(!p.verify(&[2, 1]), "S3 requires ascending order");
    }

    fn decode_all(frames: &[&[u8]]) -> (Result<(), DecodeError>, Vec<u8>, bool) {
        let mut d = AwsChunkedDecoder::new();
        let mut out = Vec::new();
        let mut result = Ok(());
        for f in frames {
            result = d.feed(&Bytes::copy_from_slice(f), &mut out);
            if result.is_err() {
                break;
            }
        }
        (result, out.concat(), d.is_complete())
    }

    #[test]
    fn a_signed_chunked_body_with_trailers_decodes_to_the_object() {
        let body = b"5;chunk-signature=abc\r\nhello\r\n6;chunk-signature=def\r\n world\r\n\
                     0;chunk-signature=0123\r\nx-amz-checksum-crc32:AAAAAA==\r\n\r\n";
        let (result, bytes, complete) = decode_all(&[body]);
        assert_eq!(result, Ok(()));
        assert_eq!(bytes, b"hello world");
        assert!(complete);
    }

    #[test]
    fn decoding_is_independent_of_how_the_body_is_framed() {
        let body: &[u8] = b"a\r\n0123456789\r\n3\r\nabc\r\n0\r\n\r\n";
        for split in 1..body.len() {
            let (result, bytes, complete) = decode_all(&[&body[..split], &body[split..]]);
            assert_eq!(result, Ok(()), "split at {split}");
            assert_eq!(bytes, b"0123456789abc", "split at {split}");
            assert!(complete, "split at {split}");
        }
    }

    #[test]
    fn a_malformed_or_truncated_body_never_passes() {
        for bad in [&b"zz\r\nhello\r\n"[..], b"5\r\nhelloXX", b"5\nhello\r\n"] {
            assert_eq!(
                decode_all(&[bad]).0,
                Err(DecodeError),
                "{:?}",
                String::from_utf8_lossy(bad)
            );
        }
        let (result, _, complete) = decode_all(&[b"5\r\nhel"]);
        assert_eq!(result, Ok(()));
        assert!(
            !complete,
            "a body that stops mid-chunk is not a complete decode"
        );
        let long_line = vec![b'1'; MAX_CHUNK_LINE + 1];
        assert_eq!(decode_all(&[&long_line]).0, Err(DecodeError));
    }
}
