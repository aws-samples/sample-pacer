//! Which cached object headers this process has confirmed against the backend
//! (ADR-0049).
//!
//! A header can reach the cache two ways: a `HeadObject` this process issued, or the
//! disk tier recovering one a previous process wrote. Nothing in the header says which,
//! and the second kind may describe a version the backend no longer has — an overwrite
//! made while this node was down was never sent to it, and an invalidation it did
//! receive only updated an in-memory index the restart threw away (#64). So a cached
//! header is used only once its object is in this set; anything else costs one
//! `HeadObject` first.
//!
//! The set starts empty in every process, which is the whole mechanism: it is how a
//! restart forgets what it trusted without anything having to be written to disk.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

/// Objects the set holds before it starts over.
///
/// Clearing rather than evicting one at a time: the only cost of forgetting an object
/// is one more `HeadObject` on its next read, so an exact LRU would buy nothing that a
/// reset does not, at the price of bookkeeping on every GET. 65 536 object keys of a
/// few hundred bytes each is a few tens of MiB at worst — far below the RAM tier — and
/// well above the object count of the checkpoint and model sets this cache is for, so
/// in practice the reset never happens.
const CONFIRMED_CAPACITY: usize = 1 << 16;

/// The object keys whose cached header this process has confirmed is current. Cheap to
/// clone: every clone shares one set.
#[derive(Clone, Default)]
pub(crate) struct Revalidated {
    confirmed: Arc<Mutex<HashSet<String>>>,
}

impl Revalidated {
    /// Whether `object_key`'s cached header may be used without asking the backend.
    pub(crate) fn is_confirmed(&self, object_key: &str) -> bool {
        self.lock().contains(object_key)
    }

    /// Record that the backend just reported `object_key`'s current version — by a
    /// `HeadObject` this process issued, whichever way it came out.
    pub(crate) fn confirm(&self, object_key: &str) {
        let mut confirmed = self.lock();
        if confirmed.len() >= CONFIRMED_CAPACITY {
            confirmed.clear();
        }
        confirmed.insert(object_key.to_owned());
    }

    /// Lock the set. One hash operation is all any holder does with it, so a poisoned
    /// lock means a panic inside `HashSet` itself and is not recoverable.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.confirmed
            .lock()
            .expect("the revalidation set lock is never held across a panic-capable call")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_fresh_set_confirms_nothing() {
        assert!(!Revalidated::default().is_confirmed("bucket/key"));
    }

    #[test]
    fn a_confirmed_key_stays_confirmed_and_others_do_not() {
        let set = Revalidated::default();
        set.confirm("bucket/a");
        assert!(set.is_confirmed("bucket/a"));
        assert!(!set.is_confirmed("bucket/b"));
    }

    #[test]
    fn clones_share_one_set() {
        let set = Revalidated::default();
        set.clone().confirm("bucket/a");
        assert!(set.is_confirmed("bucket/a"));
    }

    /// Full means start over, keeping the key that triggered it — never a set that
    /// grows without bound against a workload that touches each key once.
    #[test]
    fn a_full_set_starts_over_with_the_new_key() {
        let set = Revalidated::default();
        for n in 0..CONFIRMED_CAPACITY {
            set.confirm(&format!("bucket/{n}"));
        }
        set.confirm("bucket/next");
        assert!(set.is_confirmed("bucket/next"));
        assert!(!set.is_confirmed("bucket/0"));
        assert_eq!(set.lock().len(), 1);
    }
}
