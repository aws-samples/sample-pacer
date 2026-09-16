//! Write-path scatter: splitting a PUT body onto the chunk grid, and deciding
//! where each window goes (ADR-0032, `planning/24-write-path.md`).
//!
//! This module is the *plan*, not the execution. It answers two questions with no
//! I/O and no S3 involved, so both are unit-testable on their own:
//!
//! 1. **Where does window `i` belong, and as which S3 part?** — [`ScatterPlan`].
//! 2. **How does an arbitrarily-framed body become exactly-`chunk_size`
//!    windows?** — [`WindowSplitter`].
//! 3. **Is this owner worth offering to right now?** — [`SaturationTracker`].
//!
//! # Why the split has to be exact
//!
//! A window is simultaneously an S3 *part* and a cache *chunk*, and those two
//! roles disagree about what is allowed to vary. S3 requires every part but the
//! last to be at least 5 MiB; the cache requires a chunk to be exactly the bytes
//! `ChunkConfig::chunk_bounds` says, because that is what a reader will ask for by
//! key. Emitting a short window mid-object would satisfy neither — it would land
//! in the cache under a key whose bounds it does not fill, and a later read would
//! serve a short body as if it were complete. So the splitter only ever emits a
//! full `chunk_size` window until [`WindowSplitter::take_tail`] is called at end
//! of body.
//!
//! # Part numbering
//!
//! Chunk indices are 0-based (`ChunkConfig::chunk_key`), S3 part numbers are
//! 1-based, so part `n` carries chunk `n - 1`. Because the plan covers the object
//! contiguously the numbers come out `1..=N` with no gaps, which is what S3
//! Express requires and what general-purpose buckets accept — the scatter never
//! produces the sparse set only Standard would tolerate.

use bytes::{Bytes, BytesMut};
use pacer_cache::chunk::ChunkConfig;
use pacer_ring::NodeId;
use pacer_transport::MAX_PEER_MESSAGE_BYTES;

use crate::proxy::Cluster;

/// One window's destination: which chunk it is, which S3 part carries it, and
/// which nodes may hold it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedWindow {
    /// 0-based chunk index within the object (the ADR-0015 chunk grid).
    pub index: u64,
    /// S3 part number for this window — always `index + 1`, see the module docs.
    pub part_number: i32,
    /// Cache key this window is stored under, from `ChunkConfig::chunk_key`.
    pub chunk_key: String,
    /// The chunk's homes, best first: the rendezvous home plus its `R - 1`
    /// co-homes (ADR-0016 layer 2). The first entry is the node the upload is
    /// offered to; the rest are where the chunk is also cached, and are the
    /// retry order if the first refuses.
    pub homes: Vec<NodeId>,
}

/// Byte range this window spans in the object, derived rather than stored so it
/// cannot disagree with the chunk grid.
impl PlannedWindow {
    /// The object byte range this window covers, given the object's length.
    ///
    /// Returns `None` only if `index` is past the end of an object that long,
    /// which a plan built by [`ScatterPlan::build`] never produces.
    pub fn bounds(&self, chunk: &ChunkConfig, object_len: u64) -> Option<std::ops::Range<u64>> {
        chunk.chunk_bounds(self.index, object_len)
    }
}

/// The full set of windows a PUT decomposes into, in body order.
#[derive(Debug, Clone)]
pub struct ScatterPlan {
    /// Windows in ascending index order, covering `[0, object_len)` exactly.
    pub windows: Vec<PlannedWindow>,
    /// The object length the plan was built for, needed to size the tail window.
    pub object_len: u64,
}

impl ScatterPlan {
    /// Plan the scatter of `object_key`'s `object_len` bytes across the ring.
    ///
    /// Computed before the first body byte arrives: a `PutObject` always carries
    /// `Content-Length` (or `x-amz-decoded-content-length` for aws-chunked), so
    /// the coordinator knows the window count and every destination up front and
    /// never has to buffer the object to discover them.
    pub fn build(
        cluster: &Cluster,
        chunk: &ChunkConfig,
        object_key: &str,
        object_len: u64,
    ) -> Self {
        let count = chunk.chunk_count(object_len);
        let windows = (0..count)
            .map(|index| {
                let chunk_key = chunk.chunk_key(object_key, index);
                PlannedWindow {
                    index,
                    // The cast is safe for any object S3 accepts: part numbers
                    // stop at MAX_S3_PARTS, which `worth_scattering` enforces
                    // before a plan is ever built.
                    part_number: (index + 1) as i32,
                    homes: cluster.ring.homes(&chunk_key, cluster.replication_r),
                    chunk_key,
                }
            })
            .collect();
        Self {
            windows,
            object_len,
        }
    }

    /// How many distinct nodes the plan's first-choice homes cover.
    ///
    /// The scatter's whole value is that this approaches the fleet size even for
    /// an object count far below it (ADR-0032: a 4.66 GiB shard is ~298 chunks at
    /// the 16 MiB default, so a handful of shards still reach every node). Worth
    /// a metric, because a plan that concentrates is a plan that will not help.
    pub fn distinct_homes(&self) -> usize {
        let mut names: Vec<&str> = self
            .windows
            .iter()
            .filter_map(|w| w.homes.first().map(NodeId::name))
            .collect();
        names.sort_unstable();
        names.dedup();
        names.len()
    }
}

/// Default node-wide staging budget (ADR-0032 § 4).
///
/// # What this actually bounds
///
/// Not "how much load a node takes" — **how much concurrently-uploading object
/// data the scatter can absorb**. A staged window cannot be published until its
/// upload's `CompleteMultipartUpload`, and Complete waits for *every* window of
/// that object, so residency is the length of the whole upload rather than of one
/// part. Peak staged bytes on a node are therefore all the windows it homes for
/// every object in flight at once:
///
/// ```text
/// staging_bytes  ≥  (bytes of concurrently-uploading objects) ÷ N
/// ```
///
/// 2 GiB across 32 nodes admits 64 GiB of concurrent upload. One 6.55 GiB shard at
/// a time needs only 210 MiB per node and fits easily — it is *save concurrency*
/// that consumes this, not object size.
///
/// # The measured workload does not fit, and that is the designed outcome
///
/// A real 1800 GB / 256-file checkpoint saved by all ranks at once needs
/// 1676 GiB ÷ 32 ≈ **52 GiB per node**, 26× this. So the first ~128 windows stage
/// and everything after is refused, and the write degrades to reject-fast's
/// fallback: the coordinator uploads those windows itself and caches them locally
/// with a sharer announce.
///
/// That degradation is fine, and arguably better here. The chunks end up on
/// whoever wrote them instead of at their homes, and on a resume rank *R* reads
/// the shard rank *R* wrote — a local hit if the pod lands on the same node, a
/// peer fetch otherwise. Correctness is untouched either way: a non-home copy is
/// exactly what ADR-0016 layer 1 admission already produces, and the directory
/// finds it.
///
/// Raising this to fit such a save is **not** the answer — 52 GiB of staged
/// `Bytes` per node is not a reasonable ask on top of the cache. The answer, if a
/// workload ever wants the scatter at that scale, is to stage into the **disk
/// tier**, which foyer already has and which absorbs 52 GiB without noticing.
///
/// Must exceed `chunk_size`, or every offer is refused as
/// `OversizedForBudget` — reported distinctly precisely because a budget below one
/// window is a misconfiguration rather than load.
pub const DEFAULT_STAGING_BYTES: u64 = 2 << 30;

/// Default lifetime of a staged chunk before the reaper drops it.
///
/// Must exceed the longest plausible gap between an owner's `UploadPart` and the
/// coordinator's Complete, which is however long the *rest* of the object takes —
/// minutes for a multi-hundred-GiB object at a fleet's aggregate write rate. 15
/// minutes clears that comfortably while still returning a dead coordinator's
/// budget inside a single benchmark arm, so a crashed writer never looks like a
/// permanently saturated node.
pub const DEFAULT_STAGING_TTL_SECS: u64 = 900;

/// Default windows a coordinator keeps in flight.
///
/// This is *instantaneous* concurrency, not a limit on how many nodes take part:
/// an object's windows all go to their own homes over the upload's life, however
/// many there are (a 156 GiB object is ~10,000 windows across the whole fleet). It
/// bounds how many owners are uploading for one coordinator at any moment, and the
/// coordinator's own memory to `this × chunk_size` — 256 MiB at the defaults.
///
/// The memory half of that is only true because `ScatterCoordinator::dispatch`
/// takes the semaphore permit **before** the window's bytes, and it is node-wide
/// rather than per-PUT: one coordinator, one semaphore, whatever the concurrent-PUT
/// count. Read that function's docs (`crate::coordinate`) before moving the
/// acquisition — it sat inside the spawned upload until 2026-08-31, where it bounded
/// concurrent uploads and nothing at all about memory.
///
/// **So it wants to be at least the fleet size**, ideally a small multiple, or a
/// large fleet is left partly idle on one object. 16 suits the eight-node fleets
/// the ladder benches (two concurrent parts per node); a 64-node cluster should
/// raise it, at 16 MiB per window of coordinator memory.
///
/// Deliberately NOT `PACER_FILL_PARALLELISM`: that bounds an *ordered* body
/// stream, where reading past the reorder window is wasted. Windows are
/// independent destinations, so depth here buys concurrency directly.
pub const DEFAULT_WINDOWS_IN_FLIGHT: usize = 16;

/// Default smallest object worth scattering.
///
/// Below this the extra round trips and the composite-ETag change (ADR-0032's one
/// client-visible break) buy nothing. 128 MiB is also `N × chunk_size` for an
/// eight-node fleet at the default chunk size — the point below which an object
/// has fewer windows than nodes and so could not reach the whole fleet anyway.
pub const DEFAULT_MIN_SCATTER_BYTES: u64 = 128 << 20;

/// Default cooldown after an owner refuses for load.
///
/// Long enough that a saturated owner has actually drained: a 16 MiB
/// `UploadPart` at a per-node write rate of hundreds of MiB/s is well under a
/// second, so several complete inside this window. Short enough that one
/// transient burst does not exclude an owner for the rest of an object. Only
/// transient refusals use it — a misconfigured or non-accepting peer is
/// remembered without a timer, since no waiting clears those.
pub const DEFAULT_SATURATED_COOLDOWN_SECS: u64 = 5;

/// How this node participates in scattered writes (ADR-0032).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScatterConfig {
    /// Whether a PUT is scattered at all. **Resolved from the backend when nobody
    /// says** (`crate::config::SCATTER_ENABLED`, ADR-0032 § 6 as amended 2026-09-01):
    /// on for a general-purpose bucket, off for an Express directory bucket, and an
    /// explicit setting wins either way. By the time it reaches this struct the
    /// three-state knob is already one bool, and `true` here with an Express backend
    /// is unreachable — startup refuses that combination rather than downgrading it.
    pub enabled: bool,
    /// Node-wide ceiling on staged bytes ([`DEFAULT_STAGING_BYTES`]).
    pub staging_bytes: u64,
    /// How long a staged chunk may sit ([`DEFAULT_STAGING_TTL_SECS`]).
    pub staging_ttl: std::time::Duration,
    /// Windows a coordinator keeps in flight ([`DEFAULT_WINDOWS_IN_FLIGHT`]).
    pub windows_in_flight: usize,
    /// Smallest object to scatter ([`DEFAULT_MIN_SCATTER_BYTES`]).
    pub min_object_bytes: u64,
    /// Cooldown after a transient refusal ([`DEFAULT_SATURATED_COOLDOWN_SECS`]).
    pub saturated_cooldown: std::time::Duration,
}

impl Default for ScatterConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            staging_bytes: DEFAULT_STAGING_BYTES,
            staging_ttl: std::time::Duration::from_secs(DEFAULT_STAGING_TTL_SECS),
            windows_in_flight: DEFAULT_WINDOWS_IN_FLIGHT,
            min_object_bytes: DEFAULT_MIN_SCATTER_BYTES,
            saturated_cooldown: std::time::Duration::from_secs(DEFAULT_SATURATED_COOLDOWN_SECS),
        }
    }
}

/// S3's hard ceiling on parts per multipart upload.
///
/// One window is one part, so this and `chunk_size` cap the largest scatterable
/// object at `MAX_S3_PARTS × chunk_size` — **156 GiB at the 16 MiB default**. A
/// bigger object declines to scatter (see [`ScatterVerdict::TooManyParts`]) rather
/// than being uploaded on a coarser grid, since the grid is what makes a window a
/// cache chunk and widening it would store entries under keys no reader computes.
///
/// Measured headroom, so nobody re-derives this from first principles: every
/// checkpoint shard observed is between 0.92 and **18.08 GiB**
/// (`spike/mpu-scatter/shard-skew.py`, 12 models), and a real 1800 GB training
/// checkpoint sharded to 256 files is ~6.55 GiB per object. The ceiling is 8.6×
/// the largest shard that exists, so this branch is unreachable in practice and is
/// a guard, not a limit to design around.
pub const MAX_S3_PARTS: u64 = 10_000;

/// S3's minimum size for every part but the last. A `chunk_size` below this makes
/// every interior window an invalid part, so the scatter cannot be used at all —
/// checked once at startup rather than per request.
pub const MIN_S3_PART_SIZE: u64 = 5 << 20;

/// Why a PUT is or is not scattered.
///
/// A reason rather than a bool because every decline falls back to the same
/// behaviour — proxy the PUT, populate locally — so without the reason a metric
/// can only say the scatter is not engaging, not *why*. And the reasons are not
/// alike: one is the design working as intended, two are misconfigurations, and
/// one is a known limit that silently excludes the objects the design most wants.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ScatterVerdict {
    /// Scatter it.
    Scatter,
    /// Below `min_object_bytes`. Working as intended: the round trips and the
    /// composite-ETag change would not pay for themselves, and a single-window
    /// object would become a one-part multipart upload for nothing.
    TooSmall,
    /// Needs more than [`MAX_S3_PARTS`] windows. Unreachable for any observed
    /// checkpoint (see that constant for the measured headroom) — kept as a guard,
    /// and distinguishable so that if it ever *does* fire it reads as "an object
    /// larger than anything we have seen" rather than as ordinary load.
    TooManyParts {
        /// Windows the object would need.
        needed: u64,
        /// Largest object this `chunk_size` can scatter, in bytes.
        ceiling: u64,
    },
    /// `chunk_size` is below [`MIN_S3_PART_SIZE`], so every interior window would
    /// be an invalid part. A misconfiguration: no object can ever scatter.
    ChunkBelowPartMinimum,
    /// `chunk_size` exceeds [`MAX_PEER_MESSAGE_BYTES`], so no window could be
    /// offered to its home. Also a misconfiguration, declined here so it shows up
    /// as a fallback rather than as an RPC error per request.
    ChunkAbovePeerMessageLimit,
}

impl ScatterVerdict {
    /// Whether the scatter path runs.
    pub fn is_scatter(&self) -> bool {
        matches!(self, Self::Scatter)
    }

    /// Whether this verdict means the node is misconfigured — no object will ever
    /// scatter until an operator changes something, as opposed to this particular
    /// object being unsuitable.
    pub fn is_misconfiguration(&self) -> bool {
        matches!(
            self,
            Self::ChunkBelowPartMinimum | Self::ChunkAbovePeerMessageLimit
        )
    }
}

/// Decide whether an object of `object_len` bytes is scattered, and if not, why.
///
/// Every decline falls back to the unscattered path, which still populates locally
/// (ADR-0032 § 4) — so this is never a correctness gate, only a routing one.
pub fn scatter_verdict(
    chunk: &ChunkConfig,
    object_len: u64,
    min_object_size: u64,
) -> ScatterVerdict {
    if chunk.chunk_size() < MIN_S3_PART_SIZE {
        return ScatterVerdict::ChunkBelowPartMinimum;
    }
    if chunk.chunk_size() > MAX_PEER_MESSAGE_BYTES as u64 {
        return ScatterVerdict::ChunkAbovePeerMessageLimit;
    }
    if object_len < min_object_size {
        return ScatterVerdict::TooSmall;
    }
    let needed = chunk.chunk_count(object_len);
    if needed > MAX_S3_PARTS {
        return ScatterVerdict::TooManyParts {
            needed,
            ceiling: MAX_S3_PARTS * chunk.chunk_size(),
        };
    }
    ScatterVerdict::Scatter
}

/// Remembers which owners have refused, so a coordinator stops offering to them.
///
/// # Why a coordinator must remember at all
///
/// An owner can only refuse *after* gRPC has delivered the window — the bytes are
/// already across the wire and in its memory by the time the staging budget is
/// consulted (ADR-0032 § 4). Re-offering every window to a saturated peer
/// therefore wastes one whole window transfer per window. Remembering costs one
/// map lookup and turns that into one wasted transfer per peer.
///
/// # Why two kinds of memory
///
/// A refusal for *load* clears on its own, so it is remembered with a deadline. A
/// refusal because the peer is misconfigured or not accepting scattered writes at
/// all never clears, so it is remembered without one — a deadline there would
/// re-probe a peer that will refuse every time, forever.
///
/// Not shared between coordinators or persisted: it is a per-daemon hint whose
/// worst failure is a wasted offer, and a stale entry costs at most one part the
/// coordinator uploads itself.
#[derive(Debug, Default)]
pub struct SaturationTracker {
    /// Node name → when it may be offered to again. `None` means never.
    blocked: std::collections::HashMap<String, Option<std::time::Instant>>,
}

impl SaturationTracker {
    /// An empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `owner` should be offered a window at `now`.
    ///
    /// Expired entries are dropped as they are found, so the map self-cleans on the
    /// path that reads it rather than needing a sweep of its own.
    pub fn may_offer(&mut self, owner: &str, now: std::time::Instant) -> bool {
        match self.blocked.get(owner) {
            None => true,
            Some(None) => false,
            Some(Some(until)) if *until > now => false,
            Some(Some(_)) => {
                self.blocked.remove(owner);
                true
            }
        }
    }

    /// Record a refusal. Transient ones clear after `cooldown`; the rest never do.
    pub fn refused(
        &mut self,
        owner: &str,
        refusal: &pacer_transport::StoreRefusal,
        now: std::time::Instant,
        cooldown: std::time::Duration,
    ) {
        let until = refusal.is_transient().then(|| now + cooldown);
        self.blocked.insert(owner.to_owned(), until);
    }

    /// Forget a peer entirely — used when membership replaces it, since a restarted
    /// pod on the same node is a fresh daemon with a fresh budget and a fresh
    /// configuration, and inheriting the old one's block would be wrong in both
    /// directions.
    pub fn forget(&mut self, owner: &str) {
        self.blocked.remove(owner);
    }

    /// How many peers are currently blocked, for metrics. Counts entries as
    /// recorded, including any whose deadline has passed but that no offer has
    /// looked at yet.
    pub fn blocked_count(&self) -> usize {
        self.blocked.len()
    }
}

/// Reassembles an arbitrarily-framed body into exactly-`chunk_size` windows.
///
/// The source (`hyper` behind `s3s`) frames a body however the network did, so a
/// window boundary almost never lines up with a source buffer boundary. This
/// carries the remainder across and hands out full windows only — see the module
/// docs for why a short window mid-object would be a correctness bug rather than
/// an inefficiency.
///
/// Holds at most one window's worth of bytes beyond what it has emitted. That is
/// this type's whole contribution to a coordinator's footprint — a **residual**, not
/// the bound. What keeps a coordinator's memory at `windows_in_flight × chunk_size`
/// is the semaphore `ScatterCoordinator::dispatch` acquires before handing a window
/// on (`crate::coordinate`); this splitter would happily carry a remainder for an
/// unbounded number of dispatched windows if nothing else stopped the body read.
#[derive(Debug)]
pub struct WindowSplitter {
    /// Target window size, in bytes.
    chunk_size: usize,
    /// Source buffers received but not yet emitted, in arrival order.
    carry: std::collections::VecDeque<Bytes>,
    /// Bytes held in `carry`, tracked rather than summed per call.
    carried: usize,
}

impl WindowSplitter {
    /// A splitter emitting `chunk_size`-byte windows.
    ///
    /// # Panics
    ///
    /// Panics if `chunk_size` is zero, which would make every window empty and
    /// loop forever. A zero chunk size is already rejected by
    /// `ChunkConfig::new`, so this is a defence against a future caller
    /// constructing one directly.
    pub fn new(chunk_size: usize) -> Self {
        assert!(chunk_size > 0, "chunk_size must be positive");
        Self {
            chunk_size,
            carry: std::collections::VecDeque::new(),
            carried: 0,
        }
    }

    /// Accept one source buffer. Empty buffers are dropped rather than queued —
    /// a stream may legitimately yield them, and they would otherwise sit in
    /// `carry` forever making `take_window` scan past them every call.
    pub fn push(&mut self, bytes: Bytes) {
        if bytes.is_empty() {
            return;
        }
        self.carried += bytes.len();
        self.carry.push_back(bytes);
    }

    /// Bytes held but not yet emitted.
    pub fn carried(&self) -> usize {
        self.carried
    }

    /// Emit one full window, or `None` if fewer than `chunk_size` bytes are held.
    ///
    /// Zero-copy when the head buffer alone covers a window — the common case,
    /// since a client sending multi-MiB writes gives us buffers far larger than
    /// one window. Otherwise one copy into a fresh window-sized buffer, which is
    /// also the buffer the cache insert and the CRC32 will use, so it is not a
    /// copy the scatter adds on top of others.
    pub fn take_window(&mut self) -> Option<Bytes> {
        if self.carried < self.chunk_size {
            return None;
        }
        self.carried -= self.chunk_size;
        // Popping first and putting back on the slow path keeps this free of an
        // "unreachable" unwrap: the head is either big enough to slice, or it
        // goes back and `drain_exact` stitches from the front as usual.
        if let Some(mut head) = self.carry.pop_front() {
            if head.len() >= self.chunk_size {
                let window = head.split_to(self.chunk_size);
                if !head.is_empty() {
                    self.carry.push_front(head);
                }
                return Some(window);
            }
            self.carry.push_front(head);
        }
        Some(self.drain_exact(self.chunk_size))
    }

    /// Emit whatever is left as a final, possibly short window — the object's
    /// last chunk, and the only window allowed to be under `chunk_size` (S3
    /// exempts the final part from its minimum, verified in
    /// `spike/mpu-scatter`).
    ///
    /// `None` when the object's length is an exact multiple of `chunk_size` and
    /// every window has already been emitted. Calling this before end of body
    /// would produce a short interior chunk, so it is the caller's contract to
    /// call it once, last.
    pub fn take_tail(&mut self) -> Option<Bytes> {
        if self.carried == 0 {
            return None;
        }
        let len = self.carried;
        self.carried = 0;
        Some(self.drain_exact(len))
    }

    /// Concatenate exactly `len` bytes off the front of `carry`.
    ///
    /// `len` is always ≤ the bytes held, because both callers check first; the
    /// loop therefore cannot run dry, and the `expect` documents that rather
    /// than defending against it.
    fn drain_exact(&mut self, len: usize) -> Bytes {
        let mut out = BytesMut::with_capacity(len);
        while out.len() < len {
            let need = len - out.len();
            let mut head = self
                .carry
                .pop_front()
                .expect("drain_exact called for more bytes than are carried");
            if head.len() <= need {
                out.extend_from_slice(&head);
            } else {
                out.extend_from_slice(&head.split_to(need));
                self.carry.push_front(head);
            }
        }
        out.freeze()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Small enough to make boundary math legible; the real default is 16 MiB.
    const WINDOW: usize = 8;

    fn bytes(range: std::ops::Range<u8>) -> Bytes {
        Bytes::from(range.collect::<Vec<u8>>())
    }

    /// One source buffer larger than a window splits without copying, and the
    /// remainder stays available for the next window.
    #[test]
    fn a_large_source_buffer_yields_windows_without_loss() {
        let mut s = WindowSplitter::new(WINDOW);
        s.push(bytes(0..20));
        assert_eq!(s.take_window().unwrap(), bytes(0..8));
        assert_eq!(s.take_window().unwrap(), bytes(8..16));
        assert!(s.take_window().is_none(), "4 bytes left is not a window");
        assert_eq!(s.take_tail().unwrap(), bytes(16..20));
        assert!(s.take_tail().is_none());
    }

    /// The boundary-crossing case: no source buffer alone fills a window, so the
    /// splitter must stitch across several and keep byte order.
    #[test]
    fn windows_stitch_across_many_small_buffers() {
        let mut s = WindowSplitter::new(WINDOW);
        for i in 0..10u8 {
            s.push(Bytes::from(vec![i]));
        }
        assert_eq!(s.take_window().unwrap(), bytes(0..8));
        assert!(s.take_window().is_none());
        assert_eq!(s.take_tail().unwrap(), bytes(8..10));
    }

    /// An exact multiple of the window size emits whole windows and no tail —
    /// the case where a spurious empty final part would be sent to S3.
    #[test]
    fn an_exact_multiple_has_no_tail() {
        let mut s = WindowSplitter::new(WINDOW);
        s.push(bytes(0..16));
        assert_eq!(s.take_window().unwrap(), bytes(0..8));
        assert_eq!(s.take_window().unwrap(), bytes(8..16));
        assert!(s.take_window().is_none());
        assert!(
            s.take_tail().is_none(),
            "an exact multiple must not emit an empty final part"
        );
    }

    /// Empty source buffers are legal on a stream and must not accumulate or
    /// stall the head-length fast path.
    #[test]
    fn empty_source_buffers_are_ignored() {
        let mut s = WindowSplitter::new(WINDOW);
        s.push(Bytes::new());
        s.push(bytes(0..4));
        s.push(Bytes::new());
        s.push(bytes(4..8));
        assert_eq!(s.carried(), 8);
        assert_eq!(s.take_window().unwrap(), bytes(0..8));
    }

    /// Held bytes never exceed one window beyond what was emitted, which is what
    /// bounds a coordinator's memory to `in_flight × chunk_size`.
    #[test]
    fn carried_tracks_exactly_what_is_held() {
        let mut s = WindowSplitter::new(WINDOW);
        s.push(bytes(0..12));
        assert_eq!(s.carried(), 12);
        s.take_window().unwrap();
        assert_eq!(s.carried(), 4);
        s.take_tail().unwrap();
        assert_eq!(s.carried(), 0);
    }

    /// A body arriving in irregular frames reassembles byte-for-byte — the
    /// property that makes a window safe to serve later as a cache chunk.
    #[test]
    fn irregular_framing_reassembles_the_original_body() {
        let original: Vec<u8> = (0..=250).collect();
        let mut s = WindowSplitter::new(WINDOW);
        let mut out = Vec::new();
        let mut offset = 0;
        for frame in [1usize, 7, 3, 64, 2, 100, 74] {
            let end = (offset + frame).min(original.len());
            s.push(Bytes::copy_from_slice(&original[offset..end]));
            offset = end;
            while let Some(w) = s.take_window() {
                assert_eq!(w.len(), WINDOW, "interior windows are always full");
                out.extend_from_slice(&w);
            }
        }
        if let Some(tail) = s.take_tail() {
            out.extend_from_slice(&tail);
        }
        assert_eq!(out, original);
    }

    /// A load refusal clears on its own; a misconfigured peer never does. Getting
    /// that backwards either re-probes a peer that will always refuse, or excludes
    /// a peer that has since drained.
    #[test]
    fn a_load_refusal_expires_and_a_permanent_one_does_not() {
        use pacer_transport::StoreRefusal;
        /// Long enough that the "not yet" assertions cannot flake.
        const COOLDOWN: std::time::Duration = std::time::Duration::from_secs(5);
        let mut t = SaturationTracker::new();
        let t0 = std::time::Instant::now();
        assert!(t.may_offer("node-a", t0), "an unknown peer is offerable");

        t.refused(
            "node-a",
            &StoreRefusal::BudgetExhausted {
                staged: 1,
                budget: 1,
            },
            t0,
            COOLDOWN,
        );
        assert!(!t.may_offer("node-a", t0));
        assert!(!t.may_offer("node-a", t0 + COOLDOWN / 2));
        assert!(
            t.may_offer("node-a", t0 + COOLDOWN),
            "a load refusal must clear"
        );
        assert_eq!(
            t.blocked_count(),
            0,
            "the expired entry is dropped as it is read, not left to accumulate"
        );

        t.refused("node-b", &StoreRefusal::NotAccepting, t0, COOLDOWN);
        assert!(!t.may_offer("node-b", t0 + COOLDOWN * 1000));
        t.refused("node-c", &StoreRefusal::Unknown, t0, COOLDOWN);
        assert!(
            !t.may_offer("node-c", t0 + COOLDOWN * 1000),
            "an unrecognised reason is treated as permanent, never re-probed"
        );
    }

    /// A restarted pod on the same node is a fresh daemon with a fresh budget and
    /// possibly a fresh configuration, so membership replacing a peer must clear
    /// its block in both directions.
    #[test]
    fn forgetting_a_peer_unblocks_it() {
        let mut t = SaturationTracker::new();
        let t0 = std::time::Instant::now();
        t.refused(
            "node-a",
            &pacer_transport::StoreRefusal::NotAccepting,
            t0,
            std::time::Duration::from_secs(1),
        );
        assert!(!t.may_offer("node-a", t0));
        t.forget("node-a");
        assert!(t.may_offer("node-a", t0));
    }

    /// Default chunk size, so the verdict tests read against the real grid.
    const DEFAULT_CHUNK: u64 = 16 << 20;
    /// Default scatter floor.
    const MIN_OBJECT: u64 = 128 << 20;

    /// Each decline is reported as its own reason, because they route the same way
    /// (proxy + populate locally) and a metric that only says "not scattering"
    /// cannot tell a tuned floor from a misconfigured chunk size.
    #[test]
    fn every_decline_names_its_own_reason() {
        let usable = ChunkConfig::new(DEFAULT_CHUNK);
        assert_eq!(
            scatter_verdict(&usable, 1 << 30, MIN_OBJECT),
            ScatterVerdict::Scatter
        );
        assert_eq!(
            scatter_verdict(&usable, MIN_OBJECT - 1, MIN_OBJECT),
            ScatterVerdict::TooSmall,
            "below the floor the ETag change buys nothing"
        );
        assert_eq!(
            scatter_verdict(&ChunkConfig::new(MIN_S3_PART_SIZE - 1), 1 << 30, MIN_OBJECT),
            ScatterVerdict::ChunkBelowPartMinimum
        );
        assert_eq!(
            scatter_verdict(
                &ChunkConfig::new(MAX_PEER_MESSAGE_BYTES as u64 + 1),
                1 << 30,
                MIN_OBJECT
            ),
            ScatterVerdict::ChunkAbovePeerMessageLimit
        );
    }

    /// A chunk size that can never carry a part is an operator problem, not an
    /// unsuitable object — worth separating so an alert can distinguish "fix your
    /// config" from "this object was small".
    #[test]
    fn only_the_chunk_size_verdicts_are_misconfigurations() {
        let usable = ChunkConfig::new(DEFAULT_CHUNK);
        assert!(!scatter_verdict(&usable, MIN_OBJECT - 1, MIN_OBJECT).is_misconfiguration());
        assert!(
            scatter_verdict(&ChunkConfig::new(MIN_S3_PART_SIZE - 1), 1 << 30, MIN_OBJECT)
                .is_misconfiguration()
        );
    }

    /// The part ceiling is inclusive (S3's limit is a maximum part number, not a
    /// count to stay under), and the object that would exceed it is far larger than
    /// anything measured: the biggest observed shard is 18.08 GiB and a real
    /// 1800 GB / 256-file training checkpoint is ~6.55 GiB per object, against a
    /// 156 GiB ceiling. This asserts the guard exists, not that it matters.
    #[test]
    fn the_part_ceiling_is_inclusive_and_far_above_any_real_shard() {
        let chunk = ChunkConfig::new(DEFAULT_CHUNK);
        let ceiling = MAX_S3_PARTS * DEFAULT_CHUNK;
        assert!(scatter_verdict(&chunk, ceiling, MIN_OBJECT).is_scatter());
        assert_eq!(
            scatter_verdict(&chunk, ceiling + 1, MIN_OBJECT),
            ScatterVerdict::TooManyParts {
                needed: MAX_S3_PARTS + 1,
                ceiling,
            }
        );
        /// Largest checkpoint shard in the reference corpus, Kimi-K2-Instruct.
        const LARGEST_OBSERVED_SHARD: u64 = 18_082 << 20;
        /// A real 1800 GB training checkpoint over 256 files.
        const CUSTOMER_SHARD: u64 = 6_707 << 20;
        for observed in [LARGEST_OBSERVED_SHARD, CUSTOMER_SHARD] {
            assert!(
                scatter_verdict(&chunk, observed, MIN_OBJECT).is_scatter(),
                "every shard size actually seen must scatter"
            );
        }
    }
}
