//! Chunk directory (ADR-0017): the sharded, soft-state map from a chunk key to
//! its **sharer set** — who currently holds it. The shard for a key *is* the
//! node [`crate::Ring::owner`] computes for that key (ADR-0014's rendezvous
//! hash, unchanged); this module is the per-node bookkeeping a home keeps for
//! the keys it shards, with no cache or transport dependency, so it unit-tests
//! in isolation the way `pacer_cache::chunk` does for B1.
//!
//! Two things a home tracks per chunk key, straight from the ADR:
//!
//! - **The sharer set itself**: `node → (tier, generation)`. `tier` is a
//!   source-selection latency hint only (never a byte address — the holder
//!   still drives the transfer, ADR-0018). `generation` is a per-holder
//!   admission counter: it lets an `admit`/`evict` pair that arrives
//!   out of order (the home has no ordering guarantee across gRPC calls) fold
//!   correctly instead of a stale message undoing a fresher one.
//! - **The `max_sharers_tracked` cap**: beyond it the entry flips to
//!   "widely held" and stops recording new holders, bounding entry size (and
//!   therefore invalidation fan-out) for a storm-hot chunk.
//!
//! This is deliberately **not** wired to any transport yet — `admit`/`evict`
//! are called directly in tests here; the RPC that carries them cluster-wide
//! is a later step (mirrors B1d-1: storage layer first, wire path after).

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};

/// Default cap on distinct holders tracked per chunk key (ADR-0017 knob).
/// Must be ≥ `replication_r` (ADR-0016) so every top-R home fits before the
/// entry goes "widely held". Benchmark-tuned; kept small so a storm-hot
/// entry's size stays metadata-cheap regardless of read fan-in.
pub const DEFAULT_MAX_SHARERS_TRACKED: usize = 16;

/// Storage tier a holder reports for its copy — a source-selection latency
/// hint only (a requester prefers a `Dram` holder over an `Nvme` one). Never a
/// byte address: byte-level location stays private to the holder (ADR-0017/18).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum Tier {
    /// In-memory tier — the faster of the two to serve from.
    Dram,
    /// On-disk (NVMe) tier.
    Nvme,
}

/// One holder's current registration for a chunk key, as returned by
/// [`Directory::lookup`]. `node` is the stable node name (matches
/// [`crate::NodeId::name`]), not a dialable address — the caller resolves it
/// through the ring/membership the same way it resolves an owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    /// Stable node identity of the holder.
    pub node: String,
    /// Latency hint for source selection (see [`Tier`]).
    pub tier: Tier,
    /// This holder's admission generation (see module docs).
    pub generation: u64,
}

/// The sharer set for one chunk key, as returned by [`Directory::lookup`].
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SharerSet {
    /// Known holders, in no particular order. Empty does not imply nobody
    /// holds the chunk when `widely_held` is set — the home stopped counting.
    pub holders: Vec<Holder>,
    /// Set once distinct-holder count has ever reached `max_sharers_tracked`
    /// (ADR-0017): the home no longer records new holders for this entry.
    /// Readers should pick among the already-known holders or fall back.
    pub widely_held: bool,
}

impl SharerSet {
    /// True when no holder is known and the entry was never widely held — the
    /// caller should treat this exactly like a directory miss (empty sharer
    /// set → the caller performs/delegates the S3 read-through, ADR-0017).
    pub fn is_empty(&self) -> bool {
        self.holders.is_empty() && !self.widely_held
    }
}

/// One chunk key's home-side bookkeeping. Kept internal: callers only ever see
/// the reader-facing [`SharerSet`] (the module-doc "split that reconciles v1
/// and v2" — per-holder tier/generation is home-private, not reader state).
#[derive(Debug, Clone, Default)]
struct Entry {
    /// `node name → (tier, generation)`.
    holders: HashMap<String, (Tier, u64)>,
    widely_held: bool,
}

/// A node's directory shard: the chunk keys this node is currently the home
/// for, each with its sharer set. Soft state, no persistence (ADR-0017) — a
/// fresh `Directory` after a restart is a correct empty shard, not a bug; it
/// repopulates from read-through and holder re-announcements.
#[derive(Debug, Clone)]
pub struct Directory {
    max_sharers_tracked: usize,
    entries: HashMap<String, Entry>,
}

impl Default for Directory {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_SHARERS_TRACKED)
    }
}

impl Directory {
    /// A directory shard tracking at most `max_sharers_tracked` holders per
    /// entry before it flips to "widely held" (see module docs).
    pub fn new(max_sharers_tracked: usize) -> Self {
        Self {
            max_sharers_tracked,
            entries: HashMap::new(),
        }
    }

    /// Record that `node` now holds `chunk_key` at `tier`, admission
    /// `generation`. Idempotent and safe under reordering: an admit whose
    /// generation is older than what's already recorded for that node is a
    /// stale/duplicate message and is dropped rather than clobbering a fresher
    /// registration. Once the entry is "widely held" (cap reached), new
    /// holders are no longer recorded — the flag alone is the signal readers
    /// need (ADR-0017: "stops listing").
    pub fn admit(&mut self, chunk_key: &str, node: &str, tier: Tier, generation: u64) {
        let entry = self.entries.entry(chunk_key.to_owned()).or_default();
        if let Some((existing_tier, existing_gen)) = entry.holders.get_mut(node) {
            if generation >= *existing_gen {
                *existing_tier = tier;
                *existing_gen = generation;
            }
            return;
        }
        if entry.widely_held || entry.holders.len() >= self.max_sharers_tracked {
            entry.widely_held = true;
            return;
        }
        entry.holders.insert(node.to_owned(), (tier, generation));
    }

    /// Self-admit convenience for the common case (ADR-0017: "the home
    /// performs the read-through, registers itself"): allocates the next
    /// generation for `(chunk_key, node)` — one past whatever is currently
    /// recorded for that pair, or `1` if none — and admits with it. Saves
    /// every fill site from keeping its own generation counter; a caller that
    /// announces a *remote* holder's admission still uses [`Self::admit`]
    /// directly with the generation that peer reported.
    pub fn admit_next(&mut self, chunk_key: &str, node: &str, tier: Tier) -> u64 {
        let next_gen = self
            .entries
            .get(chunk_key)
            .and_then(|e| e.holders.get(node))
            .map_or(1, |&(_, g)| g + 1);
        self.admit(chunk_key, node, tier, next_gen);
        next_gen
    }

    /// Record that `node` no longer holds `chunk_key` at admission
    /// `generation`. Only removes when `generation` is at least as new as what
    /// is recorded — an evict carrying an older generation than the current
    /// registration is a stale message for an admission that has since been
    /// superseded by a re-admit, and folding it would incorrectly drop a live
    /// holder (this is exactly "distinguish a re-admitted chunk from a stale
    /// registration" from ADR-0017). The entry itself is dropped once its
    /// holder set is empty and it was never widely held, so a fully-evicted
    /// chunk leaves no residue in the shard.
    pub fn evict(&mut self, chunk_key: &str, node: &str, generation: u64) {
        let Some(entry) = self.entries.get_mut(chunk_key) else {
            return;
        };
        if let Some((_, existing_gen)) = entry.holders.get(node) {
            if generation >= *existing_gen {
                entry.holders.remove(node);
            }
        }
        if entry.holders.is_empty() && !entry.widely_held {
            self.entries.remove(chunk_key);
        }
    }

    /// The current sharer set for `chunk_key`, or `None` if this shard has no
    /// record of it (equivalent to an empty, never-widely-held set — the
    /// ADR-0017 "empty sharer set" miss path). Cheap: clones out of the lock
    /// held by [`SharedDirectory`], same pattern as [`crate::SharedRing::owner`].
    pub fn lookup(&self, chunk_key: &str) -> Option<SharerSet> {
        let entry = self.entries.get(chunk_key)?;
        let holders = entry
            .holders
            .iter()
            .map(|(node, &(tier, generation))| Holder {
                node: node.clone(),
                tier,
                generation,
            })
            .collect();
        Some(SharerSet {
            holders,
            widely_held: entry.widely_held,
        })
    }

    /// Every chunk key this shard records `node` as a holder of, with that
    /// holder's tier hint and generation. The re-announce healer (ADR-0017 soft
    /// state) calls this with the local node to enumerate the chunks it holds
    /// and homes, so it can re-assert them to each key's home before organic
    /// re-reads would. Read-only; clones out of the [`SharedDirectory`] lock the
    /// same way [`Self::lookup`] does. Bounded by the shard's entry count.
    pub fn holdings_of(&self, node: &str) -> Vec<(String, Tier, u64)> {
        self.entries
            .iter()
            .filter_map(|(key, entry)| {
                entry
                    .holders
                    .get(node)
                    .map(|&(tier, generation)| (key.clone(), tier, generation))
            })
            .collect()
    }

    /// Drop `chunk_key`'s entry wholesale — the invalidation path (ADR-0017):
    /// a writer's invalidation fans out to the listed sharers and clears the
    /// home's record so a later lookup starts from empty, not stale holders.
    pub fn remove(&mut self, chunk_key: &str) {
        self.entries.remove(chunk_key);
    }

    /// Number of chunk keys this shard currently has an entry for
    /// (observability; not everything homed here has an entry — only keys
    /// that have seen at least one admit).
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// True when this shard has no entries at all — the state right after
    /// construction or a full re-home (ADR-0017: a home crash loses its shard
    /// and starts empty).
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Concurrently readable, mutable directory shard. Lookups (reads) are
/// expected to be far more frequent than admit/evict (writes) once the
/// storm's fills settle — ADR-0017: "update traffic is proportional to
/// fills/evictions, not reads" — hence `RwLock` over the same `Mutex`-vs-lock
/// tradeoff [`crate::SharedRing`] makes for membership epochs.
#[derive(Debug, Clone, Default)]
pub struct SharedDirectory {
    inner: Arc<RwLock<Directory>>,
}

impl SharedDirectory {
    /// A shared handle over a directory shard tracking at most
    /// `max_sharers_tracked` holders per entry.
    pub fn new(max_sharers_tracked: usize) -> Self {
        Self {
            inner: Arc::new(RwLock::new(Directory::new(max_sharers_tracked))),
        }
    }

    /// See [`Directory::admit`].
    ///
    /// # Panics
    ///
    /// If the directory lock is poisoned (a writer panicked — daemon-fatal).
    pub fn admit(&self, chunk_key: &str, node: &str, tier: Tier, generation: u64) {
        self.inner
            .write()
            .expect("directory lock poisoned")
            .admit(chunk_key, node, tier, generation);
    }

    /// See [`Directory::admit_next`].
    ///
    /// # Panics
    ///
    /// If the directory lock is poisoned (a writer panicked — daemon-fatal).
    pub fn admit_next(&self, chunk_key: &str, node: &str, tier: Tier) -> u64 {
        self.inner
            .write()
            .expect("directory lock poisoned")
            .admit_next(chunk_key, node, tier)
    }

    /// See [`Directory::evict`].
    ///
    /// # Panics
    ///
    /// If the directory lock is poisoned (a writer panicked — daemon-fatal).
    pub fn evict(&self, chunk_key: &str, node: &str, generation: u64) {
        self.inner
            .write()
            .expect("directory lock poisoned")
            .evict(chunk_key, node, generation);
    }

    /// See [`Directory::lookup`].
    ///
    /// # Panics
    ///
    /// If the directory lock is poisoned (a writer panicked — daemon-fatal).
    pub fn lookup(&self, chunk_key: &str) -> Option<SharerSet> {
        self.inner
            .read()
            .expect("directory lock poisoned")
            .lookup(chunk_key)
    }

    /// See [`Directory::holdings_of`].
    ///
    /// # Panics
    ///
    /// If the directory lock is poisoned (a writer panicked — daemon-fatal).
    pub fn holdings_of(&self, node: &str) -> Vec<(String, Tier, u64)> {
        self.inner
            .read()
            .expect("directory lock poisoned")
            .holdings_of(node)
    }

    /// See [`Directory::remove`].
    ///
    /// # Panics
    ///
    /// If the directory lock is poisoned (a writer panicked — daemon-fatal).
    pub fn remove(&self, chunk_key: &str) {
        self.inner
            .write()
            .expect("directory lock poisoned")
            .remove(chunk_key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY: &str = "bucket/obj#16777216:0";

    #[test]
    fn miss_on_unknown_key_is_none() {
        let dir = Directory::default();
        assert!(dir.lookup(KEY).is_none());
    }

    #[test]
    fn admit_then_lookup_returns_the_holder() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        let set = dir.lookup(KEY).unwrap();
        assert_eq!(
            set.holders,
            vec![Holder {
                node: "node-a".into(),
                tier: Tier::Dram,
                generation: 1
            }]
        );
        assert!(!set.widely_held);
        assert!(!set.is_empty());
    }

    #[test]
    fn evict_removes_the_holder_and_drops_the_entry() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        dir.evict(KEY, "node-a", 1);
        assert!(
            dir.lookup(KEY).is_none(),
            "fully-evicted entry leaves no residue"
        );
        assert_eq!(dir.len(), 0);
    }

    #[test]
    fn multiple_holders_are_all_listed() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        dir.admit(KEY, "node-b", Tier::Nvme, 1);
        let mut nodes: Vec<_> = dir
            .lookup(KEY)
            .unwrap()
            .holders
            .into_iter()
            .map(|h| h.node)
            .collect();
        nodes.sort();
        assert_eq!(nodes, vec!["node-a".to_owned(), "node-b".to_owned()]);
    }

    #[test]
    fn admit_next_allocates_increasing_generations_for_repeat_fills() {
        let mut dir = Directory::default();
        assert_eq!(dir.admit_next(KEY, "node-a", Tier::Dram), 1);
        // Re-admit of the SAME node (e.g. a refill after an evict) advances.
        assert_eq!(dir.admit_next(KEY, "node-a", Tier::Nvme), 2);
        let set = dir.lookup(KEY).unwrap();
        assert_eq!(
            (set.holders[0].tier, set.holders[0].generation),
            (Tier::Nvme, 2)
        );
    }

    #[test]
    fn admit_next_starts_at_one_for_a_different_node_on_the_same_key() {
        let mut dir = Directory::default();
        dir.admit_next(KEY, "node-a", Tier::Dram);
        // A second, distinct holder starts its OWN generation sequence at 1.
        assert_eq!(dir.admit_next(KEY, "node-b", Tier::Dram), 1);
    }

    #[test]
    fn stale_out_of_order_admit_does_not_clobber_a_fresher_one() {
        // Re-admit at generation 2 (e.g. re-fetched after a local evict), then
        // a delayed/duplicated generation-1 admit arrives out of order.
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 2);
        dir.admit(KEY, "node-a", Tier::Nvme, 1);
        let set = dir.lookup(KEY).unwrap();
        assert_eq!(set.holders[0].generation, 2);
        assert_eq!(
            set.holders[0].tier,
            Tier::Dram,
            "stale admit must not overwrite the newer tier"
        );
    }

    #[test]
    fn stale_evict_does_not_undo_a_newer_admit() {
        // The re-admit's generation-2 evict is announced, but reordering
        // delivers the OLDER generation-1 evict after the generation-2 admit
        // already landed — the stale evict must not remove the live holder.
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        dir.admit(KEY, "node-a", Tier::Dram, 2); // re-admit (e.g. after a refill)
        dir.evict(KEY, "node-a", 1); // stale: targets the superseded generation
        let set = dir.lookup(KEY).unwrap();
        assert_eq!(set.holders[0].generation, 2, "node-a must still be listed");
    }

    #[test]
    fn evict_at_or_above_the_current_generation_removes_it() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 3);
        dir.evict(KEY, "node-a", 3);
        assert!(dir.lookup(KEY).is_none());
    }

    #[test]
    fn evicting_one_of_two_holders_keeps_the_other() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        dir.admit(KEY, "node-b", Tier::Nvme, 1);
        dir.evict(KEY, "node-a", 1);
        let set = dir.lookup(KEY).unwrap();
        assert_eq!(set.holders.len(), 1);
        assert_eq!(set.holders[0].node, "node-b");
    }

    #[test]
    fn cap_flips_to_widely_held_and_stops_tracking_new_holders() {
        let mut dir = Directory::new(2);
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        dir.admit(KEY, "node-b", Tier::Dram, 1);
        // Cap reached (2); a third distinct holder flips the sentinel instead
        // of growing the tracked set.
        dir.admit(KEY, "node-c", Tier::Dram, 1);
        let set = dir.lookup(KEY).unwrap();
        assert!(set.widely_held);
        assert_eq!(
            set.holders.len(),
            2,
            "the cap bounds tracked holders, not just a flag"
        );
        assert!(
            !set.is_empty(),
            "widely-held is not the same as an empty miss"
        );
    }

    #[test]
    fn widely_held_entry_still_folds_updates_for_already_tracked_holders() {
        let mut dir = Directory::new(1);
        dir.admit(KEY, "node-a", Tier::Nvme, 1);
        dir.admit(KEY, "node-b", Tier::Dram, 1); // flips widely_held, not tracked
        dir.admit(KEY, "node-a", Tier::Dram, 2); // re-admit of an ALREADY tracked holder
        let set = dir.lookup(KEY).unwrap();
        let a = set.holders.iter().find(|h| h.node == "node-a").unwrap();
        assert_eq!((a.tier, a.generation), (Tier::Dram, 2));
    }

    #[test]
    fn evict_on_an_unknown_key_is_a_harmless_no_op() {
        let mut dir = Directory::default();
        dir.evict(KEY, "node-a", 1); // no admit ever happened
        assert!(dir.lookup(KEY).is_none());
    }

    #[test]
    fn remove_clears_the_whole_entry_for_invalidation() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        dir.admit(KEY, "node-b", Tier::Nvme, 1);
        dir.remove(KEY);
        assert!(dir.lookup(KEY).is_none());
    }

    #[test]
    fn shared_directory_admit_is_visible_through_lookup() {
        let shared = SharedDirectory::new(DEFAULT_MAX_SHARERS_TRACKED);
        shared.admit(KEY, "node-a", Tier::Dram, 1);
        assert_eq!(shared.lookup(KEY).unwrap().holders.len(), 1);
        shared.evict(KEY, "node-a", 1);
        assert!(shared.lookup(KEY).is_none());
    }

    #[test]
    fn holdings_of_lists_only_this_nodes_keys_with_tier_and_generation() {
        const OTHER: &str = "bucket/obj#16777216:1";
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 5);
        dir.admit(KEY, "node-b", Tier::Nvme, 1);
        dir.admit(OTHER, "node-b", Tier::Nvme, 2);
        // node-a holds only KEY; the (tier, generation) it recorded come back so
        // the healer re-announces at the current generation, not a fresh 1.
        let a = dir.holdings_of("node-a");
        assert_eq!(a, vec![(KEY.to_owned(), Tier::Dram, 5)]);
        // node-b holds both keys.
        let mut b: Vec<_> = dir
            .holdings_of("node-b")
            .into_iter()
            .map(|(k, ..)| k)
            .collect();
        b.sort();
        assert_eq!(b, vec![KEY.to_owned(), OTHER.to_owned()]);
        // A node that holds nothing gets an empty list, not a panic.
        assert!(dir.holdings_of("node-c").is_empty());
    }

    #[test]
    fn shared_directory_holdings_of_reflects_admits() {
        let shared = SharedDirectory::new(DEFAULT_MAX_SHARERS_TRACKED);
        shared.admit(KEY, "node-a", Tier::Dram, 3);
        assert_eq!(
            shared.holdings_of("node-a"),
            vec![(KEY.to_owned(), Tier::Dram, 3)]
        );
    }

    #[test]
    fn different_keys_are_independent() {
        let mut dir = Directory::default();
        dir.admit(KEY, "node-a", Tier::Dram, 1);
        assert!(dir.lookup("bucket/other#16777216:0").is_none());
        assert_eq!(dir.lookup(KEY).unwrap().holders.len(), 1);
    }
}
