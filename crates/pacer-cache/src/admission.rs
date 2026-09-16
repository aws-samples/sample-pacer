//! Requester-local admission gate (ADR-0016 layer 1): decide when a
//! *peer-owned* chunk is hot enough to keep a local copy of.
//!
//! ADR-0012 streams peer-owned chunks through without storing them, so
//! aggregate capacity ≈ Σ node capacities for the cold tail. Layer 1 relaxes
//! that only for chunks that prove hot: a requester admits a peer-owned chunk
//! after the **K-th** fetch within a window (TinyLFU-style frequency gating,
//! not admit-on-first), turning every admitting node into extra serving
//! capacity (it then registers with the directory, ADR-0017).
//!
//! Two bounds, both fixed-memory and self-correcting so a restore storm can
//! neither exhaust memory nor lock the gate:
//!
//! - **Frequency** — a count-min sketch estimates per-chunk fetch counts in
//!   fixed memory (`SKETCH_DEPTH` × `SKETCH_WIDTH` `u16` counters). Admit once
//!   the estimate reaches the threshold.
//! - **Capacity** — a per-window byte budget (`fraction × cache capacity`)
//!   caps how many peer-copy bytes this node admits per window, so storm heat
//!   cannot evict the node's own homed chunks wholesale.
//!
//! Both age via a **tumbling window**: the first call after `window` elapses
//! zeroes the sketch and the admitted-byte tally and starts a fresh window.
//! This approximates "within a window" (a true sliding window would need
//! per-key timestamps, unbounded) and makes the byte budget a per-window
//! *admission rate* rather than a live occupancy cap — deliberate, because
//! foyer's eviction callbacks fire on DRAM→NVMe demotion, not true eviction
//! (see ADR-0017 B2 notes), so a live occupancy counter cannot be kept
//! accurate. Rate-limiting admission per window plus foyer's own LRU
//! reclamation converges occupancy without a reliable eviction signal; B4's
//! storm benchmark revisits whether this bound binds.
//!
//! **Generation persistence (R4, ADR-0017 amendment).** The gate vends a
//! monotonic `generation` to each admitting node; the directory home folds it
//! to reconcile out-of-order announcements, keeping the entry with the *higher*
//! generation and dropping a lower one as stale. A purely in-memory counter
//! restarts at 0 every process start, so a restarted pod on the same node —
//! same `node_id` — would re-vend generations *below* the home's recorded
//! high-water mark and stay invisible in the sharer set until the counter
//! re-climbed. [`AdmissionGate::with_generation_store`] closes that gap by
//! persisting a reserved-ahead high-water mark to the node's state dir and
//! resuming strictly above it. The gate additionally keeps a fixed-capacity
//! record of the keys it admitted (with their generations) so the daemon's
//! periodic re-announce loop (ADR-0017 `reannounce_interval`) can re-register
//! them without a second bookkeeping structure — see
//! [`AdmissionGate::admitted_holdings`].

use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Count-min sketch rows. Four independent hashes make the per-row collision
/// probabilities multiply, so the min estimate over-counts only when all four
/// rows collide — rare at [`SKETCH_WIDTH`]. Small: the sketch is queried on
/// every non-home peer fetch, and over-counting only admits a slightly colder
/// chunk (bounded by the capacity budget), never a wrong serve.
const SKETCH_DEPTH: usize = 4;
/// Counters per row. 2048 × 4 rows × 2 bytes = 16 KiB per node — the "fixed,
/// small" frequency sketch ADR-0016 budgets for. Wide enough that distinct hot
/// chunks in one window rarely all-collide; a false positive costs one early
/// admission, capped by the byte budget.
const SKETCH_WIDTH: usize = 2048;

/// Per-row hash seeds (build-local; nothing here crosses the wire, unlike the
/// ring's stable `score`). Distinct primes so the rows hash independently.
const ROW_SEEDS: [u64; SKETCH_DEPTH] = [
    0x9e37_79b9_7f4a_7c15,
    0xc2b2_ae3d_27d4_eb4f,
    0x1656_67b1_9e37_79f9,
    0xff51_afd7_ed55_8ccd,
];

/// Fixed-memory frequency estimator with tumbling-window aging. Not public:
/// the gate owns it and never leaks counter internals.
struct CountMinSketch {
    rows: [[u16; SKETCH_WIDTH]; SKETCH_DEPTH],
}

impl CountMinSketch {
    fn new() -> Self {
        Self {
            rows: [[0; SKETCH_WIDTH]; SKETCH_DEPTH],
        }
    }

    /// Column in row `r` for `key`.
    fn col(r: usize, key: &str) -> usize {
        let mut h = std::collections::hash_map::DefaultHasher::new();
        ROW_SEEDS[r].hash(&mut h);
        key.hash(&mut h);
        (h.finish() % SKETCH_WIDTH as u64) as usize
    }

    /// Increment `key`'s counters (saturating) and return the new estimate —
    /// the min across rows, the count-min guarantee (never underestimates).
    fn increment(&mut self, key: &str) -> u16 {
        let mut est = u16::MAX;
        for r in 0..SKETCH_DEPTH {
            let c = Self::col(r, key);
            self.rows[r][c] = self.rows[r][c].saturating_add(1);
            est = est.min(self.rows[r][c]);
        }
        est
    }

    /// Zero every counter (window rollover).
    fn clear(&mut self) {
        self.rows = [[0; SKETCH_WIDTH]; SKETCH_DEPTH];
    }
}

/// Generations reserved-ahead per persistence write (R4, ADR-0017 amendment).
/// The gate persists a high-water mark this far above the last vended
/// generation *before* returning it, so a crash between persists can never let
/// a restarted process re-vend a generation the previous one already announced
/// (the directory fold keeps the higher generation, so a regressed one would
/// leave the restarted holder invisible until its counter re-climbed). Chosen
/// as a trade-off: one small `fsync` per this many admissions (admissions are
/// rare — frequency- and byte-budget-gated) against ≤ this many generations
/// "wasted" per restart, negligible against the `u64` generation space.
const GENERATION_RESERVE_BLOCK: u64 = 1024;

/// Distinct layer-1-admitted chunk keys the gate remembers for the periodic
/// re-announce loop (ADR-0017 `reannounce_interval`). Sized to cover the
/// resident peer-copy working set — bounded by the per-window byte budget
/// (`fraction × capacity`) over the chunk size — with headroom (a 25 %-of-100
/// GiB budget at the 16 MiB default chunk holds ≈ 1600 copies). Beyond it the
/// oldest tracked key is dropped from re-announce: soft state, so it simply
/// re-registers on its next hot read-through, and the bound keeps this record
/// fixed-memory however many distinct chunks the node admits over its lifetime.
const REANNOUNCE_TRACKED_KEYS: usize = 4096;

/// Durable high-water mark for the admission generation (R4). A tiny text file
/// in the node's cache/state dir holding the highest generation the gate has
/// *reserved* (see [`GENERATION_RESERVE_BLOCK`]); reloaded at startup so a
/// restarted process vends strictly above any generation the previous process
/// could have announced. Best-effort by design — a read/write failure degrades
/// to the soft-state envelope (a restarted holder may be briefly invisible),
/// never a daemon-fatal error.
struct GenerationStore {
    /// Absolute path to the high-water-mark file (node-local, survives pod
    /// restarts on the same node).
    path: PathBuf,
}

impl GenerationStore {
    /// The persisted high-water mark, or `0` when the file is absent or
    /// unreadable (a fresh node, or a corrupt/partial file — both mean "no
    /// prior generation to beat").
    fn load(path: &Path) -> u64 {
        std::fs::read_to_string(path)
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    }

    /// Persist `hwm` durably; a filesystem failure is logged and swallowed (see
    /// the type-level soft-state note).
    fn persist(&self, hwm: u64) {
        if let Err(e) = self.persist_inner(hwm) {
            tracing::warn!(
                error = %e,
                path = %self.path.display(),
                "persisting admission generation failed; a restart may briefly \
                 regress the generation (holder re-announces heal it)"
            );
        }
    }

    /// Atomic replace via a sibling temp file, so a crash mid-write leaves
    /// either the old value or the new one, never a torn read.
    ///
    /// # Errors
    ///
    /// Any filesystem failure creating, writing, syncing, or renaming the file.
    fn persist_inner(&self, hwm: u64) -> std::io::Result<()> {
        use std::io::Write;
        let tmp = self.path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(hwm.to_string().as_bytes())?;
        f.sync_all()?;
        std::fs::rename(&tmp, &self.path)
    }
}

/// Fixed-capacity FIFO record of `chunk_key → generation` for the re-announce
/// loop. Insert updates a known key's generation in place (a re-admit after a
/// window rollover carries a fresh, higher generation); a new key past capacity
/// evicts the oldest inserted one. Not public — the gate owns it and exposes
/// only a snapshot via [`AdmissionGate::admitted_holdings`].
struct AdmittedKeys {
    /// Maximum distinct keys retained ([`REANNOUNCE_TRACKED_KEYS`]).
    cap: usize,
    /// Insertion order, for oldest-first eviction at capacity.
    order: VecDeque<String>,
    /// The generation each tracked key was last admitted at.
    gens: HashMap<String, u64>,
}

impl AdmittedKeys {
    /// A record retaining at most `cap` distinct keys.
    fn new(cap: usize) -> Self {
        Self {
            cap,
            order: VecDeque::new(),
            gens: HashMap::new(),
        }
    }

    /// Record that `key` was admitted at `generation`: update in place if
    /// already tracked, else insert (evicting the oldest tracked key first when
    /// at capacity).
    fn insert(&mut self, key: &str, generation: u64) {
        if let Some(g) = self.gens.get_mut(key) {
            *g = generation;
            return;
        }
        if self.order.len() >= self.cap {
            if let Some(old) = self.order.pop_front() {
                self.gens.remove(&old);
            }
        }
        self.order.push_back(key.to_owned());
        self.gens.insert(key.to_owned(), generation);
    }

    /// A snapshot of every tracked `(chunk_key, generation)`.
    fn snapshot(&self) -> Vec<(String, u64)> {
        self.gens.iter().map(|(k, &g)| (k.clone(), g)).collect()
    }
}

/// Mutable gate state, guarded by one mutex. Contention is only on non-home
/// peer fetches (local hits short-circuit before the gate), so a mutex over
/// this small state is cheaper than sharding.
struct State {
    sketch: CountMinSketch,
    /// Start of the current tumbling window.
    window_start: Instant,
    /// Peer-copy bytes admitted so far this window (reset on rollover).
    admitted_bytes: u64,
    /// Monotonic admission generation vended to announcers (ADR-0017). A single
    /// per-node counter is enough: generations only need to be monotonic per
    /// `(node, key)` to fold correctly, and a global monotonic counter satisfies
    /// that (it merely skips values across keys, which the fold tolerates).
    /// Persisted across restarts (R4) via [`State::reserved_through`] +
    /// [`AdmissionGate::generation_store`].
    next_generation: u64,
    /// Highest generation durably reserved so far (R4). `next_generation` climbs
    /// up to this without a new persist; crossing it reserves and persists the
    /// next [`GENERATION_RESERVE_BLOCK`] before the generation is vended.
    reserved_through: u64,
    /// Bounded record of layer-1-admitted keys → their admission generation,
    /// the input to the periodic re-announce loop (ADR-0017).
    admitted: AdmittedKeys,
}

/// The requester-local admission gate (ADR-0016 layer 1). Cheap to share
/// behind an `Arc`; every method takes `&self`.
pub struct AdmissionGate {
    /// Fetches of a chunk within a window before it is admitted locally.
    threshold: u16,
    /// Tumbling-window length.
    window: Duration,
    /// Max peer-copy bytes admitted per window.
    byte_budget: u64,
    /// Durable generation high-water mark (R4); `None` disables persistence
    /// (single-node, tests). Set via [`Self::with_generation_store`].
    generation_store: Option<GenerationStore>,
    state: Mutex<State>,
}

impl AdmissionGate {
    /// A gate admitting a peer-owned chunk on the `threshold`-th fetch within
    /// `window`, capping admitted bytes per window at `fraction × capacity`
    /// (ADR-0016 knobs). `at`/[`Instant::now`] anchors the first window.
    ///
    /// A `threshold` of 0 or 1 admits on the first fetch (cache-everything);
    /// callers pass the resolved `local_admission_threshold` (defaulted/clamped
    /// in config). `fraction` is clamped to `[0.0, 1.0]`.
    pub fn new(
        threshold: u32,
        window: Duration,
        fraction: f64,
        capacity: u64,
        at: Instant,
    ) -> Self {
        let byte_budget = (capacity as f64 * fraction.clamp(0.0, 1.0)) as u64;
        Self {
            threshold: threshold.clamp(1, u16::MAX as u32) as u16,
            window,
            byte_budget,
            generation_store: None,
            state: Mutex::new(State {
                sketch: CountMinSketch::new(),
                window_start: at,
                admitted_bytes: 0,
                next_generation: 0,
                reserved_through: 0,
                admitted: AdmittedKeys::new(REANNOUNCE_TRACKED_KEYS),
            }),
        }
    }

    /// Persist the admission generation to `path` (R4, ADR-0017 amendment):
    /// reload the high-water mark left by a previous process and resume vending
    /// strictly above it, so a pod restart on the same node never re-vends a
    /// generation an earlier process already announced (which the directory
    /// fold would drop as stale, silently dropping the restarted holder from the
    /// sharer set until its counter re-climbed past the old mark). Best-effort:
    /// an unreadable file resumes from `0`, an unwritable one degrades to the
    /// pre-R4 in-memory behavior (logged).
    ///
    /// `path` must live in the node's cache/state dir (node-local storage that
    /// survives a pod restart on the same node). Call once, right after
    /// [`Self::new`], before the gate is shared.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned — impossible on a freshly built gate
    /// that has not yet been shared, so this is a build-time invariant.
    #[must_use]
    pub fn with_generation_store(mut self, path: PathBuf) -> Self {
        let hwm = GenerationStore::load(&path);
        // Resume above every generation the previous process could have vended:
        // a persist always precedes the vend (both under the lock), so the last
        // persisted reservation is an upper bound on prior generations. Seeding
        // `reserved_through` too makes the first admit re-reserve + persist.
        let mut st = self.state.lock().expect("admission gate lock poisoned");
        st.next_generation = hwm;
        st.reserved_through = hwm;
        drop(st);
        self.generation_store = Some(GenerationStore { path });
        self
    }

    /// Record a fetch of `chunk_key` (a peer-owned chunk this node just pulled)
    /// and decide whether to admit a local copy of `chunk_len` bytes.
    ///
    /// Returns `Some(generation)` to admit — the caller inserts the chunk and
    /// announces `(node, generation)` to the chunk's directory home
    /// (ADR-0017) — or `None` to keep streaming through without storing.
    /// [`Instant::now`] variant of [`Self::admit_at`].
    pub fn admit(&self, chunk_key: &str, chunk_len: u64) -> Option<u64> {
        self.admit_at(chunk_key, chunk_len, Instant::now())
    }

    /// [`Self::admit`] with an explicit clock, for deterministic tests.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (a prior caller panicked while holding
    /// it — treated as fatal, mirroring the ring/directory locks).
    pub fn admit_at(&self, chunk_key: &str, chunk_len: u64, now: Instant) -> Option<u64> {
        let mut st = self.state.lock().expect("admission gate lock poisoned");
        // Tumbling-window rollover: the first call past the window boundary
        // ages the whole gate (fresh heat estimate, fresh byte budget).
        if now.duration_since(st.window_start) >= self.window {
            st.sketch.clear();
            st.admitted_bytes = 0;
            st.window_start = now;
        }
        let estimate = st.sketch.increment(chunk_key);
        if estimate < self.threshold {
            return None;
        }
        // Hot enough — but only if it fits this window's peer-copy byte budget,
        // so admission cannot run away with the node's capacity in one storm.
        if st.admitted_bytes.saturating_add(chunk_len) > self.byte_budget {
            return None;
        }
        st.admitted_bytes += chunk_len;
        let generation = self.next_generation(&mut st);
        // Remember this admission so the re-announce loop can re-register it
        // (ADR-0017); bounded, so this never grows without limit.
        st.admitted.insert(chunk_key, generation);
        Some(generation)
    }

    /// Vend the next monotonic generation, reserving and persisting the next
    /// [`GENERATION_RESERVE_BLOCK`] durably *before* returning it whenever the
    /// current reservation is exhausted (R4). Persisting under the caller's
    /// held lock serializes writes and guarantees the reserved value reaches
    /// disk before the generation can be announced, so a restart never regresses
    /// below an announced generation.
    fn next_generation(&self, st: &mut State) -> u64 {
        st.next_generation += 1;
        let generation = st.next_generation;
        if generation > st.reserved_through {
            st.reserved_through = generation.saturating_add(GENERATION_RESERVE_BLOCK - 1);
            if let Some(store) = &self.generation_store {
                store.persist(st.reserved_through);
            }
        }
        generation
    }

    /// Snapshot of the layer-1-admitted chunk keys this gate still tracks, each
    /// with the generation it was admitted at — the input to the periodic
    /// re-announce loop (ADR-0017 `reannounce_interval`), which re-registers
    /// each with its directory home carrying this (persisted-monotonic, R4)
    /// generation so the home's fold accepts it. Bounded to
    /// `REANNOUNCE_TRACKED_KEYS` distinct keys.
    ///
    /// # Panics
    ///
    /// If the internal lock is poisoned (mirrors [`Self::admit_at`]).
    pub fn admitted_holdings(&self) -> Vec<(String, u64)> {
        self.state
            .lock()
            .expect("admission gate lock poisoned")
            .admitted
            .snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "bucket/obj#16777216:0";
    const WINDOW: Duration = Duration::from_secs(60);
    /// Big enough that the byte budget never binds in frequency-only tests.
    const BIG_CAPACITY: u64 = 1 << 40;

    fn gate(threshold: u32, fraction: f64, capacity: u64) -> (AdmissionGate, Instant) {
        let t0 = Instant::now();
        (
            AdmissionGate::new(threshold, WINDOW, fraction, capacity, t0),
            t0,
        )
    }

    #[test]
    fn admits_on_the_kth_fetch_not_before() {
        let (g, t0) = gate(3, 1.0, BIG_CAPACITY);
        assert!(g.admit_at(KEY, 100, t0).is_none(), "1st fetch: too cold");
        assert!(g.admit_at(KEY, 100, t0).is_none(), "2nd fetch: too cold");
        assert!(g.admit_at(KEY, 100, t0).is_some(), "3rd fetch: admit");
    }

    #[test]
    fn distinct_keys_count_independently() {
        let (g, t0) = gate(2, 1.0, BIG_CAPACITY);
        assert!(g.admit_at(KEY, 100, t0).is_none());
        // A different key on its first fetch is still cold.
        assert!(g.admit_at("bucket/other#16777216:0", 100, t0).is_none());
        // KEY's second fetch crosses the threshold.
        assert!(g.admit_at(KEY, 100, t0).is_some());
    }

    #[test]
    fn generations_increase_across_admissions() {
        let (g, t0) = gate(1, 1.0, BIG_CAPACITY);
        let g1 = g.admit_at(KEY, 100, t0).unwrap();
        let g2 = g.admit_at("k2", 100, t0).unwrap();
        assert!(g2 > g1, "generations must be monotonic across keys");
    }

    #[test]
    fn window_rollover_resets_heat() {
        let (g, t0) = gate(2, 1.0, BIG_CAPACITY);
        assert!(g.admit_at(KEY, 100, t0).is_none()); // count 1
                                                     // Past the window: the sketch is aged, so the next fetch is "1st" again.
        let t1 = t0 + WINDOW + Duration::from_secs(1);
        assert!(
            g.admit_at(KEY, 100, t1).is_none(),
            "heat aged out; count restarts"
        );
        assert!(
            g.admit_at(KEY, 100, t1).is_some(),
            "now the 2nd in the new window"
        );
    }

    #[test]
    fn byte_budget_caps_admissions_per_window() {
        // 25% of a 1000-byte capacity = 250-byte budget.
        let (g, t0) = gate(1, 0.25, 1000);
        // Each admit is 100 bytes; the 3rd would exceed 250 and is refused.
        assert!(g.admit_at("k1", 100, t0).is_some()); // 100
        assert!(g.admit_at("k2", 100, t0).is_some()); // 200
        assert!(
            g.admit_at("k3", 100, t0).is_none(),
            "over budget (300 > 250)"
        );
        // A fresh window restores the budget.
        let t1 = t0 + WINDOW + Duration::from_secs(1);
        assert!(g.admit_at("k4", 100, t1).is_some());
    }

    #[test]
    fn threshold_is_floored_at_one() {
        // Threshold 0 must not admit-on-nothing; it behaves as admit-on-first.
        let (g, t0) = gate(0, 1.0, BIG_CAPACITY);
        assert!(g.admit_at(KEY, 100, t0).is_some());
    }

    #[test]
    fn zero_fraction_admits_nothing() {
        let (g, t0) = gate(1, 0.0, BIG_CAPACITY);
        assert!(
            g.admit_at(KEY, 100, t0).is_none(),
            "no byte budget → never admit"
        );
    }

    // ---- R4: generation persistence across a process restart ----

    #[test]
    fn generation_survives_a_restart_on_the_same_node() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("admission-generation");
        let t0 = Instant::now();
        // First process: admit a couple of chunks, remember the last generation.
        let last = {
            let g = AdmissionGate::new(1, WINDOW, 1.0, BIG_CAPACITY, t0)
                .with_generation_store(path.clone());
            let a = g.admit_at(KEY, 100, t0).unwrap();
            let b = g.admit_at("k2", 100, t0).unwrap();
            assert!(b > a, "monotonic within a run");
            b
        };
        // "Restart": a brand-new gate pointed at the same persisted state must
        // vend strictly above the pre-restart high-water mark (the R4 fix — a
        // fresh 0-based counter would regress and be dropped as stale).
        let g2 = AdmissionGate::new(1, WINDOW, 1.0, BIG_CAPACITY, t0).with_generation_store(path);
        let resumed = g2.admit_at(KEY, 100, t0).unwrap();
        assert!(
            resumed > last,
            "restarted gate must vend above the prior generation: {resumed} <= {last}"
        );
    }

    #[test]
    fn restart_resumes_above_the_reserved_block_not_just_the_last_vended() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("admission-generation");
        let t0 = Instant::now();
        // First process vends exactly generation 1 but reserves a whole block
        // ahead on disk before returning it.
        let first = {
            let g = AdmissionGate::new(1, WINDOW, 1.0, BIG_CAPACITY, t0)
                .with_generation_store(path.clone());
            g.admit_at(KEY, 100, t0).unwrap()
        };
        assert_eq!(first, 1);
        // A crash loses the in-memory counter; the reserved mark on disk
        // survives, so the next process resumes ABOVE the whole reserved block —
        // this is what makes monotonicity crash-safe, not just clean-restart-safe.
        let g2 = AdmissionGate::new(1, WINDOW, 1.0, BIG_CAPACITY, t0).with_generation_store(path);
        let resumed = g2.admit_at(KEY, 100, t0).unwrap();
        assert!(
            resumed > GENERATION_RESERVE_BLOCK,
            "must resume past the reserved block ({GENERATION_RESERVE_BLOCK}), got {resumed}"
        );
    }

    #[test]
    fn absent_or_corrupt_store_resumes_from_zero_without_erroring() {
        let dir = tempfile::tempdir().unwrap();
        // A garbage file must not panic or fail the gate — R4 is best-effort.
        let path = dir.path().join("admission-generation");
        std::fs::write(&path, b"not-a-number").unwrap();
        let t0 = Instant::now();
        let g = AdmissionGate::new(1, WINDOW, 1.0, BIG_CAPACITY, t0).with_generation_store(path);
        assert_eq!(g.admit_at(KEY, 100, t0), Some(1), "corrupt → resume from 0");
    }

    #[test]
    fn admitted_holdings_report_generations_and_stay_bounded() {
        let (g, t0) = gate(1, 1.0, BIG_CAPACITY);
        let g1 = g.admit_at("a", 100, t0).unwrap();
        let g2 = g.admit_at("b", 100, t0).unwrap();
        let holdings: HashMap<String, u64> = g.admitted_holdings().into_iter().collect();
        assert_eq!(holdings.get("a"), Some(&g1));
        assert_eq!(holdings.get("b"), Some(&g2));

        // Re-admitting a known key updates its generation in place, not a dup.
        let g1b = g.admit_at("a", 100, t0).unwrap();
        let holdings: HashMap<String, u64> = g.admitted_holdings().into_iter().collect();
        assert_eq!(
            holdings.get("a"),
            Some(&g1b),
            "generation refreshed in place"
        );

        // The record is fixed-memory: admit well past capacity and the oldest
        // keys drop out (soft state — they re-register on their next hot read).
        for i in 0..(REANNOUNCE_TRACKED_KEYS + 100) {
            g.admit_at(&format!("k{i}"), 100, t0).unwrap();
        }
        assert_eq!(
            g.admitted_holdings().len(),
            REANNOUNCE_TRACKED_KEYS,
            "tracked-key record is bounded"
        );
    }
}
