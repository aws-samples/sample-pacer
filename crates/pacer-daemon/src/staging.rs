//! Owner-side staging for a scattered write: chunks uploaded to S3 but not yet
//! visible to readers (ADR-0032 § 3/§ 4, `planning/24-write-path.md`).
//!
//! # Why staging exists at all
//!
//! In a scattered write the coordinator opens one multipart upload and each
//! chunk's home uploads its own part. Between an owner's `UploadPart` and the
//! coordinator's `CompleteMultipartUpload` **the object does not exist in S3**. If
//! the owner made its chunk servable at that moment, a reader on a third node
//! could fetch bytes successfully while a `HEAD` of the same key 404s — and if
//! Complete later failed, that node would hold cached bytes for an object that
//! never existed, with no header whose absence would force a re-read.
//!
//! So a staged chunk is present but invisible: not in the cache, not announced to
//! the directory. Commit binds the visibility flip to S3's own atomic Complete,
//! which is the design's single fence. Everything after that fence is unordered
//! and idempotent, and a *missed* commit costs only warmth — the chunk simply is
//! not cached and the next read falls through to an object that now exists.
//!
//! # Why nothing here ever waits
//!
//! [`StagingArea::try_stage_at`] returns a refusal rather than blocking, and that
//! is load-bearing three times over (ADR-0032 § 4):
//!
//! * **Deadlock.** When every node is both coordinating its own write and homing
//!   others' chunks, an owner that blocks waiting for budget while its own
//!   coordinator blocks waiting for that owner is a cycle. Refusing breaks it by
//!   construction.
//! * **Load awareness.** A node already saturating its egress refuses, so the
//!   scatter self-limits to nodes with spare capacity instead of shuffling bytes
//!   between equally busy peers — which is exactly the balanced-shard case where
//!   scattering would otherwise *lose* (same per-node bytes to S3, plus a
//!   shuffle).
//! * **Degradation.** Every refusal sends that window back to the coordinator,
//!   which uploads it and caches it locally. With every owner refusing, the whole
//!   design collapses to today's PUT plus a local populate — strictly better than
//!   `main`, never worse.
//!
//! # What the budget actually bounds
//!
//! Reservation happens on RPC arrival, *before* the owner starts its `UploadPart`,
//! so the budget covers both the upload's in-flight bytes and the staged wait for
//! commit. It does **not** bound what gRPC already buffered to deliver the
//! request — by the time this module can refuse, the bytes are in RAM. That cost
//! is bounded by the server's concurrency limit, and by the coordinator being
//! expected to stop offering to an owner that refused, so the wasted transfer
//! happens once per saturated owner rather than once per window.
//!
//! The residency is the part that surprises: a window is held until its upload's
//! Complete, and Complete waits for *every* window of that object, so a window
//! staged first is held for the whole upload rather than for one part. Peak staged
//! bytes are therefore `(bytes of concurrently-uploading objects) ÷ N`, which for a
//! save where every rank writes at once is the whole checkpoint over the fleet. See
//! `crate::scatter::DEFAULT_STAGING_BYTES` for the worked example and for why a
//! save that does not fit is *meant* to fall back rather than be accommodated with
//! a bigger budget.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use bytes::Bytes;
use pacer_transport::StoreRefusal;

/// One chunk held between its `UploadPart` and the upload's commit.
#[derive(Debug)]
struct Staged {
    /// The chunk's bytes — the same buffer the part was uploaded from, held by
    /// refcount rather than copied.
    body: Bytes,
    /// Multipart upload this chunk belongs to. Commit and discard address a whole
    /// upload, so this is what groups them.
    upload_id: String,
    /// When the chunk was staged, for [`StagingArea::reap_at`].
    staged_at: Instant,
}

/// What [`StagingArea::try_stage_at`] decided.
///
/// The refusal carries [`StoreRefusal`] — the transport's type, not a local one.
/// The reason is what tells a coordinator whether to keep offering to this owner,
/// so it is genuinely part of the peer contract and both ends must agree on the
/// taxonomy; a parallel daemon-side enum plus a mapping layer would be two places
/// for that taxonomy to drift.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StageOutcome {
    /// Reserved and held. The owner may now upload its part.
    Staged,
    /// Already held for this same upload — an idempotent retry of a `StoreChunk`
    /// whose response was lost. The owner must re-upload the part to answer with
    /// its ETag, but must not double-charge the budget.
    AlreadyStaged,
    /// Refused, immediately. See [`StoreRefusal`].
    Refused(StoreRefusal),
}

/// Chunks an owner has uploaded but not yet made visible.
///
/// Sized by a byte budget rather than an entry count, because a budget is what
/// bounds memory and what the refusal decision is actually about. That also keeps
/// the map small — at most `budget ÷ chunk_size` entries — which is why
/// [`Self::commit`] and [`Self::discard`] may scan it rather than maintaining a
/// secondary index by upload.
#[derive(Debug)]
pub struct StagingArea {
    /// Guarded together so the byte total can never disagree with the map. The
    /// critical sections contain no `await` and no I/O, so a std mutex is right.
    state: Mutex<State>,
    /// Ceiling on staged bytes — the reject-fast threshold.
    budget_bytes: usize,
    /// How long a staged chunk may sit before [`Self::reap_at`] drops it. Covers
    /// the case ADR-0032 names: a coordinator that dies after some owners staged
    /// but before it could commit or discard.
    ttl: Duration,
}

/// The map and its byte total, which must move together.
#[derive(Debug, Default)]
struct State {
    /// Chunk key → staged chunk.
    pending: HashMap<String, Staged>,
    /// Sum of `pending`'s body lengths, maintained on every insert and removal
    /// rather than summed per call.
    staged_bytes: usize,
    /// The largest [`Self::staged_bytes`] ever reached — see
    /// [`StagingArea::staged_bytes_peak`] for why the peak and not a sample.
    staged_bytes_peak: usize,
}

impl StagingArea {
    /// A staging area holding at most `budget_bytes`, reaping after `ttl`.
    pub fn new(budget_bytes: usize, ttl: Duration) -> Self {
        Self {
            state: Mutex::new(State::default()),
            budget_bytes,
            ttl,
        }
    }

    /// Offer a chunk for staging. Never waits; see the module docs.
    ///
    /// [`Instant::now`] variant of [`Self::try_stage_at`].
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned, which means another thread panicked
    /// while holding it — the staged map is then untrustworthy.
    pub fn try_stage(&self, chunk_key: &str, upload_id: &str, body: Bytes) -> StageOutcome {
        self.try_stage_at(chunk_key, upload_id, body, Instant::now())
    }

    /// Offer a chunk for staging, stamping it `now`.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn try_stage_at(
        &self,
        chunk_key: &str,
        upload_id: &str,
        body: Bytes,
        now: Instant,
    ) -> StageOutcome {
        let chunk_len = body.len();
        let budget = self.budget_bytes as u64;
        if chunk_len > self.budget_bytes {
            return StageOutcome::Refused(StoreRefusal::OversizedForBudget {
                chunk_len: chunk_len as u64,
                budget,
            });
        }
        let mut st = self.lock();
        if let Some(existing) = st.pending.get(chunk_key) {
            return if existing.upload_id == upload_id {
                StageOutcome::AlreadyStaged
            } else {
                StageOutcome::Refused(StoreRefusal::RacingUpload)
            };
        }
        if st.staged_bytes + chunk_len > self.budget_bytes {
            return StageOutcome::Refused(StoreRefusal::BudgetExhausted {
                staged: st.staged_bytes as u64,
                budget,
            });
        }
        st.staged_bytes += chunk_len;
        // The one place staged bytes rise, so the one place the high-water mark can
        // move. Latched here rather than sampled by the metrics scrape for the reason
        // on [`Self::staged_bytes_peak`]: a residency that peaks and drains between two
        // scrapes is invisible to a sample, and that peak is the whole quantity of
        // interest.
        st.staged_bytes_peak = st.staged_bytes_peak.max(st.staged_bytes);
        st.pending.insert(
            chunk_key.to_owned(),
            Staged {
                body,
                upload_id: upload_id.to_owned(),
                staged_at: now,
            },
        );
        StageOutcome::Staged
    }

    /// Release one chunk without committing it — the owner's own `UploadPart`
    /// failed, so the reservation must not linger until the TTL.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn release(&self, chunk_key: &str) -> bool {
        self.lock().remove(chunk_key).is_some()
    }

    /// Make every chunk of `upload_id` committable, returning them as
    /// `(chunk_key, body)` for the caller to insert as
    /// `CachedChunk::versioned(body, e_tag)` and announce.
    ///
    /// Removing them here is what frees the budget, so the caller must insert
    /// what it is handed — a dropped return value loses the warmth (never
    /// correctness, since an uncached chunk is a miss) and the ADR accepts that.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn commit(&self, upload_id: &str) -> Vec<(String, Bytes)> {
        let mut st = self.lock();
        let keys = st.keys_of(upload_id);
        keys.into_iter()
            .filter_map(|k| st.remove(&k).map(|s| (k, s.body)))
            .collect()
    }

    /// Drop every chunk of `upload_id` — the upload was aborted, so its staged
    /// bytes describe an object that will never exist. Returns how many went.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn discard(&self, upload_id: &str) -> usize {
        let mut st = self.lock();
        let keys = st.keys_of(upload_id);
        keys.iter().filter(|k| st.remove(k).is_some()).count()
    }

    /// Drop chunks staged longer than the TTL, returning their keys.
    ///
    /// The backstop for a coordinator that died between an owner's `UploadPart`
    /// and its commit or discard: nothing else will ever resolve those entries,
    /// and they hold budget that would otherwise refuse every later write.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn reap_at(&self, now: Instant) -> Vec<String> {
        let mut st = self.lock();
        let expired: Vec<String> = st
            .pending
            .iter()
            .filter(|(_, s)| now.duration_since(s.staged_at) >= self.ttl)
            .map(|(k, _)| k.clone())
            .collect();
        for key in &expired {
            st.remove(key);
        }
        expired
    }

    /// Bytes currently staged, for metrics and for tests.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn staged_bytes(&self) -> usize {
        self.lock().staged_bytes
    }

    /// The most bytes this node has ever held staged at once, since the process
    /// started. **Read it against [`Self::budget_bytes`]** — a peak with no ceiling
    /// beside it is unjudgeable.
    ///
    /// A high-water mark rather than a scrape-time sample, for the same reason
    /// `DeliveryMetrics::inflight_chunks_peak` is one: Prometheus scrapes every
    /// 15-60 s and this repo's Grafana has a 60 s rate floor, while a window's
    /// residency is one object's upload — so an instantaneous gauge reports whatever
    /// one instant happened to hold and misses the peak between two scrapes, which is
    /// exactly the number being asked for.
    ///
    /// **What it discriminates.** `refusals{reason="budget_exhausted"}` says refusals
    /// happened; only this says whether the budget was at its ceiling when they did. A
    /// peak that reaches the budget means staging is *residency*-bound — a window is
    /// held until its object's Complete, and Complete waits for every window, so peak
    /// staged ≈ (concurrently-uploading object bytes) ÷ N, a quantity with no
    /// coordinator read-ahead term in it, which no ordering fix can move (ADR-0032
    /// § 4; the levers are Phase 5's reservation, a per-part commit, or staging to
    /// disk). A peak well *below* the budget with refusals still present means the
    /// cause is something else entirely — [`StoreRefusal::RacingUpload`],
    /// [`Self::reap_at`], or ownership skew.
    ///
    /// Never decreases, so it is read once at the end of a run rather than `rate()`d.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn staged_bytes_peak(&self) -> usize {
        self.lock().staged_bytes_peak
    }

    /// Chunks currently staged, for metrics and for tests.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (see [`Self::try_stage`]).
    pub fn pending_count(&self) -> usize {
        self.lock().pending.len()
    }

    /// The configured ceiling, so a coordinator's refusal handling can log what
    /// it was up against — and so the scrape can publish it beside
    /// [`Self::staged_bytes_peak`], which is meaningless without it.
    pub fn budget_bytes(&self) -> usize {
        self.budget_bytes
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().expect("staging area lock poisoned")
    }
}

impl State {
    /// Remove one entry, keeping the byte total in step. The single release path,
    /// so the budget can never be freed twice for one chunk.
    fn remove(&mut self, chunk_key: &str) -> Option<Staged> {
        let staged = self.pending.remove(chunk_key)?;
        self.staged_bytes -= staged.body.len();
        Some(staged)
    }

    /// Keys belonging to one upload. A scan, which is cheap because the budget
    /// bounds `pending` to `budget ÷ chunk_size` entries (128 at a 2 GiB budget
    /// and 16 MiB chunks) — an index by upload would be more state to keep
    /// consistent for no measurable gain.
    fn keys_of(&self, upload_id: &str) -> Vec<String> {
        self.pending
            .iter()
            .filter(|(_, s)| s.upload_id == upload_id)
            .map(|(k, _)| k.clone())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Chunk size for the tests, small enough to make budget arithmetic legible.
    const CHUNK: usize = 100;
    /// Room for exactly three chunks, so the fourth is the interesting one.
    const BUDGET: usize = 3 * CHUNK;
    /// Long enough that nothing expires unless a test advances the clock itself.
    const TTL: Duration = Duration::from_secs(900);

    fn area() -> StagingArea {
        StagingArea::new(BUDGET, TTL)
    }

    fn chunk() -> Bytes {
        Bytes::from(vec![7u8; CHUNK])
    }

    #[test]
    fn staging_holds_bytes_and_commit_hands_them_back() {
        let a = area();
        assert_eq!(a.try_stage("k#0", "up-1", chunk()), StageOutcome::Staged);
        assert_eq!(a.staged_bytes(), CHUNK);
        let committed = a.commit("up-1");
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, "k#0");
        assert_eq!(committed[0].1.len(), CHUNK);
        assert_eq!(a.staged_bytes(), 0, "commit must free the budget");
    }

    /// The load-bearing rule: past the budget an offer is refused, not queued.
    #[test]
    fn a_full_budget_refuses_instead_of_waiting() {
        let a = area();
        for i in 0..3 {
            assert_eq!(
                a.try_stage(&format!("k#{i}"), "up-1", chunk()),
                StageOutcome::Staged
            );
        }
        assert_eq!(
            a.try_stage("k#3", "up-1", chunk()),
            StageOutcome::Refused(StoreRefusal::BudgetExhausted {
                staged: BUDGET as u64,
                budget: BUDGET as u64,
            })
        );
        assert_eq!(a.pending_count(), 3, "a refusal stores nothing");
    }

    /// The peak must LATCH: it rises with residency and does not fall back when the
    /// commit frees the budget.
    ///
    /// The failure mode this exists to catch is a high-water mark that quietly behaves
    /// like a sample — which would read 0 on every scrape taken between two writes and
    /// so report a node that never staged anything, on the one arm the series exists
    /// for. So all three phases are asserted: it rises, it survives the commit that
    /// takes the live total to 0, and a later, shallower residency does not lower it.
    #[test]
    fn the_staged_bytes_peak_latches_and_does_not_fall_back() {
        let a = area();
        for i in 0..3 {
            a.try_stage(&format!("k#{i}"), "up-1", chunk());
        }
        assert_eq!(a.staged_bytes(), BUDGET);
        assert_eq!(
            a.staged_bytes_peak(),
            BUDGET,
            "the peak follows the residency up"
        );
        // A refusal must not raise it: the bytes were never held.
        assert!(matches!(
            a.try_stage("k#3", "up-1", chunk()),
            StageOutcome::Refused(_)
        ));
        assert_eq!(a.staged_bytes_peak(), BUDGET, "a refusal stages nothing");

        a.commit("up-1");
        assert_eq!(a.staged_bytes(), 0, "commit frees the budget");
        assert_eq!(
            a.staged_bytes_peak(),
            BUDGET,
            "the peak must survive the commit — a peak that drains with the residency \
             is a sample wearing a peak's name, and would report a node that never staged"
        );

        a.try_stage("k#4", "up-2", chunk());
        assert_eq!(a.staged_bytes(), CHUNK);
        assert_eq!(
            a.staged_bytes_peak(),
            BUDGET,
            "a shallower later residency must not lower the mark"
        );
    }

    /// Committing an upload frees room for the next one — the refusal is about
    /// load, not a permanent ceiling.
    #[test]
    fn budget_recovers_after_a_commit() {
        let a = area();
        for i in 0..3 {
            a.try_stage(&format!("k#{i}"), "up-1", chunk());
        }
        assert!(matches!(
            a.try_stage("k#3", "up-2", chunk()),
            StageOutcome::Refused(_)
        ));
        a.commit("up-1");
        assert_eq!(a.try_stage("k#3", "up-2", chunk()), StageOutcome::Staged);
    }

    /// A chunk bigger than the whole budget is a misconfiguration, not load:
    /// distinguishable so a coordinator does not sit in a cooldown loop that can
    /// never clear.
    #[test]
    fn a_chunk_larger_than_the_budget_is_not_transient() {
        let a = StagingArea::new(CHUNK - 1, TTL);
        let outcome = a.try_stage("k#0", "up-1", chunk());
        let StageOutcome::Refused(refusal) = outcome else {
            panic!("expected a refusal");
        };
        assert_eq!(
            refusal,
            StoreRefusal::OversizedForBudget {
                chunk_len: CHUNK as u64,
                budget: (CHUNK - 1) as u64,
            }
        );
        assert!(!refusal.is_transient());
        assert!(StoreRefusal::RacingUpload.is_transient());
    }

    /// A lost `StoreChunk` response must be retryable without double-charging.
    #[test]
    fn re_offering_the_same_upload_is_idempotent() {
        let a = area();
        assert_eq!(a.try_stage("k#0", "up-1", chunk()), StageOutcome::Staged);
        assert_eq!(
            a.try_stage("k#0", "up-1", chunk()),
            StageOutcome::AlreadyStaged
        );
        assert_eq!(a.staged_bytes(), CHUNK, "a retry must not charge twice");
    }

    /// Two uploads racing one key: refuse, so neither version is cached under an
    /// ambiguous ETag (ADR-0032 puts this out of contract for v1).
    #[test]
    fn a_second_upload_of_the_same_key_is_refused() {
        let a = area();
        a.try_stage("k#0", "up-1", chunk());
        assert_eq!(
            a.try_stage("k#0", "up-2", chunk()),
            StageOutcome::Refused(StoreRefusal::RacingUpload)
        );
    }

    /// A failed `UploadPart` must release immediately rather than hold budget
    /// until the TTL.
    #[test]
    fn release_frees_a_failed_part_at_once() {
        let a = area();
        a.try_stage("k#0", "up-1", chunk());
        assert!(a.release("k#0"));
        assert_eq!(a.staged_bytes(), 0);
        assert!(!a.release("k#0"), "releasing twice must not underflow");
    }

    /// Discard drops one upload's chunks and leaves another's alone.
    #[test]
    fn discard_is_scoped_to_one_upload() {
        let a = area();
        a.try_stage("k#0", "up-1", chunk());
        a.try_stage("k#1", "up-2", chunk());
        assert_eq!(a.discard("up-1"), 1);
        assert_eq!(a.pending_count(), 1);
        assert_eq!(a.staged_bytes(), CHUNK);
        assert_eq!(a.commit("up-2").len(), 1);
    }

    /// Commit is scoped the same way — an owner holding windows of several
    /// concurrent objects must not publish one object's chunks under another's
    /// ETag.
    #[test]
    fn commit_is_scoped_to_one_upload() {
        let a = area();
        a.try_stage("k#0", "up-1", chunk());
        a.try_stage("k#1", "up-2", chunk());
        let committed = a.commit("up-1");
        assert_eq!(committed.len(), 1);
        assert_eq!(committed[0].0, "k#0");
        assert_eq!(a.pending_count(), 1);
    }

    /// The abandoned-coordinator backstop: entries past the TTL are dropped and
    /// their budget returned, or a dead coordinator would refuse every later
    /// write on this node forever.
    #[test]
    fn reaping_frees_chunks_a_dead_coordinator_abandoned() {
        let a = area();
        let t0 = Instant::now();
        a.try_stage_at("k#0", "up-1", chunk(), t0);
        a.try_stage_at("k#1", "up-1", chunk(), t0 + TTL);
        assert!(a.reap_at(t0 + TTL / 2).is_empty(), "nothing expired yet");
        let reaped = a.reap_at(t0 + TTL);
        assert_eq!(reaped, vec!["k#0".to_owned()]);
        assert_eq!(a.staged_bytes(), CHUNK, "only the expired entry freed");
        assert_eq!(a.pending_count(), 1);
    }

    /// Offers per thread in [`concurrent_offers_keep_the_budget_exact`], more
    /// than fits so the refusal path runs concurrently with the staging path.
    const OFFERS_PER_THREAD: usize = 16;

    /// One thread's share of the concurrent offers, returning how many were
    /// accepted. Extracted from the test so the loop does not nest a conditional
    /// inside a closure inside a loop.
    fn offer_many(area: &StagingArea, thread: usize) -> usize {
        (0..OFFERS_PER_THREAD)
            .filter(|i| {
                area.try_stage(&format!("k#{thread}-{i}"), "up-1", chunk()) == StageOutcome::Staged
            })
            .count()
    }

    /// Concurrent offers must leave the byte total exactly consistent with the
    /// map — the invariant that makes the budget meaningful under real load.
    #[test]
    fn concurrent_offers_keep_the_budget_exact() {
        /// Enough threads to interleave the staged and refused paths.
        const THREADS: usize = 8;
        let a = std::sync::Arc::new(StagingArea::new(BUDGET, TTL));
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let a = std::sync::Arc::clone(&a);
                std::thread::spawn(move || offer_many(&a, t))
            })
            .collect();
        let staged: usize = handles.into_iter().map(|h| h.join().unwrap()).sum();
        assert_eq!(
            staged * CHUNK,
            a.staged_bytes(),
            "every Staged outcome must be reflected in the byte total, exactly once"
        );
        assert_eq!(a.pending_count(), staged);
        assert!(
            a.staged_bytes() <= BUDGET,
            "the budget must never be exceeded, whatever the interleaving"
        );
    }
}
