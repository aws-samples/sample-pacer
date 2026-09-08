//! Cache-key ownership: which node should hold a given object?
//!
//! Rendezvous (highest-random-weight) hashing rather than a token ring:
//! membership churn only moves keys owned by the departed node, no virtual
//! nodes to tune, and ownership is computable statelessly from the member set.
//! Membership arrives via a Kubernetes EndpointSlice watch ([`membership`]) or
//! a static list; both publish into a [`SharedRing`].
//!
//! [`directory`] adds the sharer-set bookkeeping a node keeps for the chunk
//! keys this ring homes on it (ADR-0017): "who currently holds this chunk?",
//! sharded by the same [`Ring::owner`] function — a chunk's home *is* its
//! directory shard, no separate placement.

use std::sync::{Arc, RwLock};

use serde::{Deserialize, Serialize};
use xxhash_rust::xxh3::Xxh3;

pub mod directory;
pub mod membership;

/// A cluster member. `name` is the stable identity (Kubernetes node name);
/// `addr` is the pod IP peers dial.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId {
    name: String,
    addr: String,
}

impl NodeId {
    /// A member with stable identity `name` reachable at `addr`.
    pub fn new(name: impl Into<String>, addr: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            addr: addr.into(),
        }
    }

    /// Stable identity (Kubernetes node name) — the ownership hash input.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Dialable address (pod IP); may change across pod restarts.
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

/// Rendezvous hash ring over the current member set.
#[derive(Debug, Default, Clone)]
pub struct Ring {
    members: Vec<NodeId>,
}

impl Ring {
    /// A ring over the given member set.
    pub fn new(members: Vec<NodeId>) -> Self {
        Self { members }
    }

    /// The current member set (unordered).
    pub fn members(&self) -> &[NodeId] {
        &self.members
    }

    /// The member with stable identity `name`, or `None` if not in this epoch.
    /// Used to resolve a directory holder's node name (ADR-0017) back to a
    /// dialable [`NodeId`].
    pub fn member(&self, name: &str) -> Option<&NodeId> {
        self.members.iter().find(|n| n.name() == name)
    }

    /// True when no members are known (every key is un-owned).
    pub fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Replace the member set (called on each membership epoch).
    pub fn set_members(&mut self, members: Vec<NodeId>) {
        self.members = members;
    }

    /// The node that owns `key`, or None if the ring is empty. Ownership is
    /// by stable node name, so pod restarts (new IP, same node) don't reshuffle.
    pub fn owner(&self, key: &str) -> Option<&NodeId> {
        self.members
            .iter()
            .max_by_key(|node| score(node.name(), key))
    }

    /// Members ranked by preference for `key` (owner first). Used for
    /// "try the owner, then the next-best peer" fetch strategies.
    pub fn ranked(&self, key: &str) -> Vec<&NodeId> {
        let mut ranked: Vec<&NodeId> = self.members.iter().collect();
        ranked.sort_by_key(|node| std::cmp::Reverse(score(node.name(), key)));
        ranked
    }
}

/// Concurrently readable, swappable ring. The membership task writes epochs;
/// every request path reads a snapshot.
#[derive(Debug, Clone, Default)]
pub struct SharedRing {
    inner: Arc<RwLock<Ring>>,
}

impl SharedRing {
    /// A shared handle starting at the given epoch.
    pub fn new(ring: Ring) -> Self {
        Self {
            inner: Arc::new(RwLock::new(ring)),
        }
    }

    /// Snapshot of the current ring (member vecs are node-count-sized; the
    /// clone keeps lock hold time to a copy).
    ///
    /// # Panics
    ///
    /// If the ring lock is poisoned (a writer panicked — daemon-fatal).
    pub fn load(&self) -> Ring {
        self.inner.read().expect("ring lock poisoned").clone()
    }

    /// Publish a new membership epoch.
    ///
    /// # Panics
    ///
    /// If the ring lock is poisoned (a writer panicked — daemon-fatal).
    pub fn store(&self, members: Vec<NodeId>) {
        self.inner
            .write()
            .expect("ring lock poisoned")
            .set_members(members);
    }

    /// Owner of `key` in the current epoch (cloned out of the lock).
    ///
    /// # Panics
    ///
    /// If the ring lock is poisoned (a writer panicked — daemon-fatal).
    pub fn owner(&self, key: &str) -> Option<NodeId> {
        self.inner
            .read()
            .expect("ring lock poisoned")
            .owner(key)
            .cloned()
    }

    /// The top-`r` co-homes for `key` (owner first), cloned out of the lock —
    /// the replication set per ADR-0016 layer 2. Fewer than `r` members ⇒ every
    /// member is a home. `r == 0` yields an empty set (defensive; callers clamp
    /// `replication_r` to ≥ 1).
    ///
    /// # Panics
    ///
    /// If the ring lock is poisoned (a writer panicked — daemon-fatal).
    pub fn homes(&self, key: &str, r: usize) -> Vec<NodeId> {
        self.inner
            .read()
            .expect("ring lock poisoned")
            .ranked(key)
            .into_iter()
            .take(r)
            .cloned()
            .collect()
    }
}

/// xxh3 seed ("pacer" leetspoken). Part of the wire-stable ownership function:
/// changing it reshuffles EVERY key cluster-wide (see `score_is_pinned`).
const SCORE_SEED: u64 = 0x10_7a;
/// Separator between node name and key in the hash input. 0xff is never valid
/// mid-UTF-8, so it removes concatenation ambiguity ("ab"+"c" vs "a"+"bc").
const SCORE_SEPARATOR: [u8; 1] = [0xff];

// xxh3 with a fixed seed: a NAMED stable hash, deterministic across builds,
// architectures, and Rust versions — required the moment two daemon versions
// coexist during a rolling update (DefaultHasher, used before Phase 2, is only
// stable within one build).
fn score(node_name: &str, key: &str) -> u64 {
    let mut h = Xxh3::with_seed(SCORE_SEED);
    h.update(node_name.as_bytes());
    h.update(&SCORE_SEPARATOR);
    h.update(key.as_bytes());
    h.digest()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ring(n: usize) -> Ring {
        Ring::new(
            (0..n)
                .map(|i| NodeId::new(format!("node-{i}"), format!("10.0.0.{i}")))
                .collect(),
        )
    }

    #[test]
    fn empty_ring_has_no_owner() {
        assert!(ring(0).owner("bucket/key").is_none());
    }

    #[test]
    fn owner_is_deterministic() {
        let r = ring(5);
        assert_eq!(r.owner("bucket/a"), r.owner("bucket/a"));
    }

    #[test]
    fn score_is_pinned() {
        // Golden value: if this fails, the hash function changed and a rolling
        // update would reshuffle EVERY key (full cache re-warm cluster-wide).
        // Changing it requires an ADR note, not a test update.
        assert_eq!(score("node-0", "bucket/obj"), PINNED_SCORE);
    }
    const PINNED_SCORE: u64 = 0x68f6_9726_a4b6_cca7;

    #[test]
    fn owner_ignores_addr_changes() {
        let a = Ring::new(vec![
            NodeId::new("node-0", "10.0.0.1"),
            NodeId::new("node-1", "10.0.0.2"),
        ]);
        let b = Ring::new(vec![
            NodeId::new("node-0", "10.9.9.9"), // same node, new pod IP
            NodeId::new("node-1", "10.0.0.2"),
        ]);
        for key in ["x", "y", "bucket/some/deep/key"] {
            assert_eq!(a.owner(key).unwrap().name(), b.owner(key).unwrap().name());
        }
    }

    #[test]
    fn removal_only_moves_departed_nodes_keys() {
        let full = ring(5);
        let mut smaller = full.clone();
        let departed = "node-3";
        smaller.set_members(
            full.members()
                .iter()
                .filter(|n| n.name() != departed)
                .cloned()
                .collect(),
        );

        for i in 0..1000 {
            let key = format!("bucket/obj-{i}");
            let before = full.owner(&key).unwrap();
            let after = smaller.owner(&key).unwrap();
            if before.name() != departed {
                // Keys not owned by the departed node must not move.
                assert_eq!(before.name(), after.name(), "key {key} moved");
            }
        }
    }

    #[test]
    fn distribution_is_roughly_uniform() {
        let r = ring(4);
        let mut counts = std::collections::HashMap::new();
        for i in 0..10_000 {
            let owner = r.owner(&format!("bucket/obj-{i}")).unwrap();
            *counts.entry(owner.name().to_owned()).or_insert(0u32) += 1;
        }
        for (_, c) in counts {
            // 2500 expected per node; allow ±20%.
            assert!((2000..=3000).contains(&c), "skewed: {c}");
        }
    }

    #[test]
    fn ranked_starts_with_owner_and_covers_all() {
        let r = ring(5);
        let ranked = r.ranked("bucket/a");
        assert_eq!(ranked.len(), 5);
        assert_eq!(ranked[0], r.owner("bucket/a").unwrap());
    }

    #[test]
    fn homes_takes_top_r_owner_first() {
        let shared = SharedRing::new(ring(5));
        let key = "bucket/a";
        let homes = shared.homes(key, 2);
        assert_eq!(homes.len(), 2);
        assert_eq!(homes[0], shared.owner(key).unwrap());
        // The R homes are exactly the top-R of the full ranking.
        let full = ring(5);
        let ranked = full.ranked(key);
        assert_eq!(homes[0].name(), ranked[0].name());
        assert_eq!(homes[1].name(), ranked[1].name());
    }

    #[test]
    fn homes_clamps_to_member_count() {
        let shared = SharedRing::new(ring(2));
        // Asking for more homes than members yields all members, no panic.
        assert_eq!(shared.homes("k", 5).len(), 2);
        // r == 0 is empty; r on an empty ring is empty.
        assert!(shared.homes("k", 0).is_empty());
        assert!(SharedRing::new(ring(0)).homes("k", 2).is_empty());
    }

    #[test]
    fn shared_ring_swaps_epochs() {
        let shared = SharedRing::new(ring(2));
        assert!(shared.owner("k").is_some());
        shared.store(vec![]);
        assert!(shared.owner("k").is_none());
    }
}
