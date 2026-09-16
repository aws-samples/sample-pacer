//! The address handles this client holds for writers, **bounded** — one entry per writer
//! GID, least-recently-used first out.
//!
//! ## What an entry is for
//!
//! On EFA the *receiver* must hold an address handle for the sender before the sender can
//! write to it: SRD is reliable, so the receiving NIC has to send transport ACKs, and to send
//! them it needs a routing entry for the initiator. With no entry the WRITE completes
//! `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER` and **nothing lands** — measured
//! cross-node (planning/09 finding 10) and, decisively, between two endpoints on one device
//! (`bench/ladder/results/c2-loopback-gate.md`). A handle is bound to a protection domain,
//! hence to one process and one rail, which is why the daemon cannot build it on a client's
//! behalf and why this cache is per rail.
//!
//! ## Why the key is the GID alone
//!
//! `ibv_create_ah` takes a destination GID; the destination **QPN travels per work request**
//! (`endpoint::address_handle` documents this from the verbs side). So one handle
//! serves every queue pair on a writer's device, deduplication is on the GID, and the QPN in
//! an announce is carried only for the log line. A useful consequence: a daemon that restarts
//! comes back with the **same** GIDs and a fresh QPN, so the handles a client already holds
//! survive the restart. (This is a deliberate change from the original pump, which keyed on
//! `(gid, qpn)` and therefore built a second handle for the same device after every daemon
//! restart. That cost one `ibv_ah` per restart and was harmless; under a bound it is not, so
//! the key is now the thing the handle is actually built from.)
//!
//! ## Why there is a bound at all, and why it is a *concurrency* bound
//!
//! The pump used to keep every handle it ever built, forever, and could: it only ever learned
//! writers that had already announced themselves, so the set was "holders this window was
//! actually written by". ADR-0030's pre-flight exchange changes that — a client now asks the
//! daemon which nodes hold an object's chunks and primes handles for them *before* issuing
//! the read. On a 1000-node fleet at 32 rails a naive prefetch of every named holder is
//! ~32 000 device objects at roughly a millisecond of firmware admin command each, which is
//! not viable and would be slower than the race it removes.
//!
//! So the bound tracks **concurrency, not cluster size** — see [`DEFAULT_MAX_HANDLES`] for
//! the arithmetic. The load-bearing invariant is:
//!
//! > **The bound is at least the number of handles that can be in use at one instant.**
//!
//! It has to be, because eviction destroys a handle (`ibv_destroy_ah` on drop), and a handle
//! destroyed while a WRITE from that writer is in flight puts the client back in exactly the
//! `UNKNOWN_PEER` state this cache exists to prevent. With the bound at or above the
//! concurrent working set, the least-recently-used entry is by construction *not* a writer
//! with a window in flight, and eviction is safe. That is why [`HandleCache::evicted`] is not
//! merely a thrash indicator: **a non-zero eviction count is the alarm that the invariant may
//! have been violated**, and it is reported (and warned once) rather than left to be
//! inferred.
//!
//! ## Why this module is not behind the `efa` feature
//!
//! The policy above — the key, the bound, the eviction order, the counters — is where the
//! bugs are, and none of it needs a device. [`HandleCache`] is therefore generic over the
//! handle type so it compiles and is tested in every build (`cargo test --workspace`, no
//! `--features efa`), exactly as [`pacer_transport::announce`] is. The `efa` build
//! instantiates it at `HandleCache<ibverbs::AddressHandle>`.

use std::collections::HashMap;

use pacer_transport::announce::{self, AnnouncedRail};
use tracing::warn;

/// Writers that can be posting into one client window at the same instant.
///
/// The daemon resolves a request's windows through `stream::buffered(delivery.parallelism)`,
/// and each window is written by at most one node — so the number of *distinct writer nodes*
/// with bytes in flight into this client at any instant is bounded by that fan-out. 64 is the
/// shipped `DEFAULT_DELIVERY_PARALLELISM`. It is read here as a constant rather than
/// discovered, because a client cannot see the daemon's config; an operator who raises the
/// daemon's fan-out raises this with [`MAX_HANDLES_ENV`].
const CONCURRENT_WRITERS: usize = 64;

/// Rails one writer can post from, i.e. distinct GIDs per writer node.
///
/// 32 — a p5.48xlarge, the widest writer this repo runs. Same number and same reasoning as
/// `endpoint::P5_RAILS`, which sizes the announce receive ring against the same worst case.
const RAILS_PER_WRITER: usize = 32;

/// Default ceiling on handles held per rail: the full concurrent working set.
///
/// `64 × 32 = 2048`, i.e. every one of the daemon's concurrently resolving windows coming
/// from a *different* node, each of which has posted from all 32 of its rails. That is a
/// worst case, not an expectation, and the margin over anything measured is what makes it
/// safe to evict below it: the largest shape this repo runs is 8 nodes × 32 rails = 256
/// distinct writer GIDs, and the largest handle count ever observed on a loader is 1024
/// (4 GPUs × 16 windows, `bench/ladder/results/c5-multirail.md`). 2048 clears the measured
/// maximum by 2× and the realistic fleet by 8×.
///
/// The cost of the ceiling being *reached* is 2048 `ibv_ah` device objects per rail; the cost
/// of building that many from cold is ~2 s of firmware admin commands, which is why a
/// pre-flight primes only the holders the daemon actually named for one object and this bound
/// is the backstop rather than the working number.
pub const DEFAULT_MAX_HANDLES: usize = CONCURRENT_WRITERS * RAILS_PER_WRITER;

/// Override for [`DEFAULT_MAX_HANDLES`], for a fleet whose daemons run a wider
/// `delivery.parallelism` or whose writers have more rails than a p5.
///
/// Defined here rather than in a shared config module for the same reason
/// `PACER_CLIENT_ANNOUNCE_RECV_SLOTS` is: this crate is `dlopen`ed into someone else's
/// process and has no config file to read.
const MAX_HANDLES_ENV: &str = "PACER_CLIENT_MAX_ADDRESS_HANDLES";

/// Floor for the override: one full writer must fit, or first contact with a single 32-rail
/// daemon would evict its own earlier rails mid-request — the one case that is guaranteed to
/// break rather than merely thrash.
const MIN_MAX_HANDLES: usize = RAILS_PER_WRITER;

/// Ceiling for the override. Well above any plausible fan-out × rail count, and low enough
/// that a mistyped value cannot ask the device for a number of handles that would take
/// minutes of firmware commands to build.
const MAX_MAX_HANDLES: usize = 1 << 16;

/// Compile-time invariants on the sizing above — build failures rather than tests, because
/// each is a property of the constants alone (the same discipline `endpoint.rs` applies to the
/// announce ring).
const _: () = {
    // The bound must clear one full first-contact burst from the widest writer, or a single
    // daemon's 32 rails could evict each other.
    assert!(
        DEFAULT_MAX_HANDLES >= RAILS_PER_WRITER,
        "the bound must hold every rail of one writer"
    );
    // And it must clear the largest handle count ever measured on a loader (1024), or the
    // shipped default would evict on a shape this repo has already run.
    assert!(
        DEFAULT_MAX_HANDLES > 1024,
        "1024 handles was measured on a 4-GPU loader; the default must clear it"
    );
    // The bounds have to admit the default, or an unset environment would be clamped.
    assert!(MIN_MAX_HANDLES <= DEFAULT_MAX_HANDLES && DEFAULT_MAX_HANDLES <= MAX_MAX_HANDLES);
};

/// How many handles one rail may hold, resolved once per process.
///
/// Cached for the same reason `endpoint::recv_slots` is: every rail's cache is sized from it,
/// and two different answers within one process would mean two rails of one window disagreeing
/// about their own ceiling.
pub fn max_handles() -> usize {
    static RESOLVED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| parse_max_handles(std::env::var(MAX_HANDLES_ENV).ok().as_deref()))
}

/// The decision [`max_handles`] caches, as a pure function of the environment's value.
///
/// Separate so it is testable: `max_handles` resolves once per process, so a test driving it
/// through the environment could only ever assert one case, and which one would depend on test
/// ordering.
fn parse_max_handles(raw: Option<&str>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_MAX_HANDLES;
    };
    match raw.trim().parse::<usize>() {
        Ok(n) if (MIN_MAX_HANDLES..=MAX_MAX_HANDLES).contains(&n) => n,
        // An out-of-range number and a non-number are the same case: the operator's intent
        // cannot be honoured, and a loader that mistyped an env var should still load.
        _ => {
            warn!(
                value = raw,
                default = DEFAULT_MAX_HANDLES,
                min = MIN_MAX_HANDLES,
                max = MAX_MAX_HANDLES,
                "{MAX_HANDLES_ENV} is not a handle count in range; using the default"
            );
            DEFAULT_MAX_HANDLES
        }
    }
}

/// What one install pass did, per rail entry it was handed.
///
/// The split between [`Self::created`] and [`Self::already_held`] is the whole observability
/// story of the pre-flight: on the *announce* path `already_held` means the pre-flight got
/// there first (the race was structurally removed for this writer), and `created` means the
/// announce is what taught this client — which is the repair path doing its job, and the
/// number that goes to zero when priming is working.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Installed {
    /// Handles built by this pass — one `ibv_create_ah` each.
    pub created: usize,
    /// Rails whose GID was already held, so nothing was built.
    pub already_held: usize,
    /// Rails whose handle could not be built. Logged, never fatal: it means *that* writer's
    /// WRITEs will not be acknowledged, which is its problem to report, while failing here
    /// would break every other writer too.
    pub failed: usize,
    /// Entries evicted to make room during this pass. **Should be zero** — see the module
    /// header on why a non-zero value is a correctness alarm and not a tuning hint.
    pub evicted: usize,
}

/// One cached handle plus when it was last named, for the least-recently-used order.
struct Entry<H> {
    /// The handle, held **only to keep it alive** — nothing ever reads it, which is why it is
    /// `_`-prefixed in the idiom this repo uses for a value owned purely to keep something else
    /// valid (`efa::announce::RailPayload::_bytes`).
    ///
    /// A client posts no work request, so it never passes a handle to anything: an address
    /// handle's entire effect is that the *NIC* can find the writer's routing entry while the
    /// handle exists. Dropping it runs `ibv_destroy_ah` and takes that entry away, so ownership
    /// — and therefore the eviction order above it — is the whole of what this type does.
    _handle: H,
    /// Value of [`HandleCache::clock`] at the last insert or touch. A monotonic counter
    /// rather than a timestamp: nothing here needs wall-clock meaning, and a counter cannot
    /// tie two entries or go backwards under a clock adjustment.
    used: u64,
}

/// Address handles for writer GIDs on one rail's protection domain, bounded and LRU.
///
/// Generic over the handle so the policy compiles and is tested without a device; the `efa`
/// build uses `HandleCache<ibverbs::AddressHandle>`, whose `Drop` is `ibv_destroy_ah`.
pub struct HandleCache<H> {
    entries: HashMap<[u8; 16], Entry<H>>,
    /// Ceiling on [`Self::len`]. Never zero: [`Self::with_bound`] clamps it, because a cache
    /// of zero would destroy every handle the instant it was built.
    bound: usize,
    /// Monotonic tick stamped on an entry each time it is inserted or named.
    clock: u64,
    /// Cumulative handles built, over the cache's whole life.
    created: u64,
    /// Cumulative entries evicted. The alarm — see the module header.
    evicted: u64,
    /// Whether the first eviction has been warned about, so a cache that is permanently over
    /// its bound does not bury the delivery's own diagnostics under one line per handle.
    warned: bool,
}

impl<H> Default for HandleCache<H> {
    fn default() -> Self {
        Self::new()
    }
}

impl<H> HandleCache<H> {
    /// A cache bounded by [`max_handles`] — what a client rail gets.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(max_handles())
    }

    /// A cache with an explicit bound, clamped to at least one entry.
    ///
    /// The clamp is not defensive tidiness: a bound of zero would evict every handle
    /// immediately after building it, so every WRITE would find no address handle and the
    /// client would look broken in a way that reads as a fabric fault. Public so a test can
    /// drive eviction at a bound of 2 rather than 2048.
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            entries: HashMap::new(),
            bound: bound.max(1),
            clock: 0,
            created: 0,
            evicted: 0,
            warned: false,
        }
    }

    /// Handles held right now.
    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether nothing is held yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The ceiling this cache was built with.
    #[must_use]
    pub fn bound(&self) -> usize {
        self.bound
    }

    /// Cumulative handles built (`ibv_create_ah` calls), by either path.
    #[must_use]
    pub fn created(&self) -> u64 {
        self.created
    }

    /// Cumulative entries evicted. **Expected to stay zero**; see the module header.
    #[must_use]
    pub fn evicted(&self) -> u64 {
        self.evicted
    }

    /// Whether this rail already holds a handle for `gid`, without touching the LRU order.
    ///
    /// Read-only on purpose: it answers "am I primed for this writer?", and a query that
    /// promoted its subject would make the eviction order depend on who asked.
    #[must_use]
    pub fn holds(&self, gid: &[u8; 16]) -> bool {
        self.entries.contains_key(gid)
    }

    /// Build a handle for every GID in `rails` that is not already held, deduplicating on the
    /// GID and evicting least-recently-used entries if the bound is reached.
    ///
    /// **The one implementation of "install these writers"**, shared by the announce pump and
    /// by the pre-flight priming path. They differ only in where the rail list came from — the
    /// wire format is the same ([`pacer_transport::announce`]), so the handle set they produce
    /// is identical by construction rather than by two matching loops.
    ///
    /// `build` is called at most once per distinct GID. A `build` failure is counted and the
    /// pass continues, because one unaddressable writer must not cost the others their handles.
    pub fn install_rails<F, E>(&mut self, rails: &[AnnouncedRail], mut build: F) -> Installed
    where
        F: FnMut([u8; 16]) -> Result<H, E>,
        E: std::fmt::Display,
    {
        let mut done = Installed::default();
        for rail in rails {
            if let Some(entry) = self.entries.get_mut(&rail.gid) {
                self.clock += 1;
                entry.used = self.clock;
                done.already_held += 1;
                continue;
            }
            match build(rail.gid) {
                Ok(handle) => {
                    done.evicted += self.make_room();
                    self.clock += 1;
                    self.entries.insert(
                        rail.gid,
                        Entry {
                            _handle: handle,
                            used: self.clock,
                        },
                    );
                    self.created += 1;
                    done.created += 1;
                }
                // Named loudly: this is the case where a writer's bytes will silently fail to
                // land, and the QPN is what identifies it in the daemon's own log.
                Err(e) => {
                    done.failed += 1;
                    warn!(
                        qpn = rail.qpn,
                        writer_rail = rail.rail,
                        error = %e,
                        "could not build an address handle for a writer; its WRITEs will not \
                         be acknowledged"
                    );
                }
            }
        }
        done
    }

    /// Decode one [`pacer_transport::announce`] message and install every rail it names.
    ///
    /// `None` means the bytes were not an announce — logged and dropped rather than returned as
    /// an error, because the caller is a pump that must survive a malformed message from any
    /// writer. The distinction matters to the caller: `None` must not count as an announce
    /// received.
    pub fn install_announce<F, E>(&mut self, message: &[u8], build: F) -> Option<Installed>
    where
        F: FnMut([u8; 16]) -> Result<H, E>,
        E: std::fmt::Display,
    {
        match announce::decode(message) {
            Ok(rails) => Some(self.install_rails(&rails, build)),
            Err(e) => {
                warn!(bytes = message.len(), error = %format!("{e:#}"), "not an announce");
                None
            }
        }
    }

    /// Evict least-recently-used entries until one more will fit, returning how many went.
    ///
    /// `O(len)` per eviction, deliberately: eviction is the *abnormal* path (it means the
    /// bound was smaller than the concurrent working set), a scan of at most a few thousand
    /// map entries is nanoseconds, and it buys not carrying a second index that could
    /// disagree with the map about what is held. The alternative — an intrusive order list —
    /// would make the common path, which is a single hash lookup, more expensive to keep
    /// correct.
    fn make_room(&mut self) -> usize {
        let mut gone = 0;
        while self.entries.len() >= self.bound {
            let Some(victim) = self
                .entries
                .iter()
                .min_by_key(|(_, entry)| entry.used)
                .map(|(gid, _)| *gid)
            else {
                // Unreachable: a non-empty map has a minimum, and `bound >= 1` means an empty
                // map never enters the loop. Breaking rather than panicking keeps a delivery
                // degraded instead of aborting a loader that had already loaded.
                break;
            };
            // Dropping the entry runs `ibv_destroy_ah`. Safe only because the bound is at
            // least the concurrent working set — see the module header's invariant.
            self.entries.remove(&victim);
            self.evicted += 1;
            gone += 1;
        }
        if gone > 0 && !self.warned {
            self.warned = true;
            warn!(
                bound = self.bound,
                "evicting client address handles: the handle set outgrew its bound, so a \
                 writer whose WRITE is in flight can lose the handle its ACKs need and the \
                 WRITE will complete UNKNOWN_PEER. Raise {MAX_HANDLES_ENV} above the \
                 daemon's delivery.parallelism x its rail count"
            );
        }
        gone
    }
}

#[cfg(test)]
mod tests {
    use super::{
        parse_max_handles, HandleCache, DEFAULT_MAX_HANDLES, MAX_MAX_HANDLES, MIN_MAX_HANDLES,
    };
    use pacer_transport::announce::{self, AnnouncedRail};
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    /// A stand-in handle that reports its own destruction, so eviction can be observed as the
    /// thing it really is — `ibv_destroy_ah` — rather than only as a shrinking length.
    struct Tracked {
        destroyed: Arc<AtomicUsize>,
    }

    impl Drop for Tracked {
        fn drop(&mut self) {
            self.destroyed.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// A rail entry whose GID differs in more than its last byte, so a cache keying on a
    /// prefix cannot pass by accident.
    fn rail(i: u8) -> AnnouncedRail {
        let mut gid = [0u8; 16];
        for (j, b) in gid.iter_mut().enumerate() {
            *b = i.wrapping_mul(17).wrapping_add(j as u8);
        }
        AnnouncedRail {
            gid,
            qpn: 16_384 + u32::from(i),
            rail: u16::from(i),
        }
    }

    /// A cache of `u32` stand-ins, built from the GID's first byte.
    fn cache(bound: usize) -> HandleCache<u32> {
        HandleCache::with_bound(bound)
    }

    /// Handles key on the GID, so two rails of one writer that share a device — or the same
    /// device coming back under a fresh queue-pair number after a daemon restart — resolve to
    /// ONE handle. That is not an optimisation: `ibv_create_ah` takes only a GID, and the
    /// destination QPN travels per work request, so a second handle for the same GID would be
    /// a duplicate device object that addresses exactly what the first one does.
    #[test]
    fn dedups_on_the_gid_not_on_the_queue_pair() {
        let mut c = cache(16);
        let one = rail(1);
        let same_device_new_qp = AnnouncedRail { qpn: 49_153, ..one };
        let installed = c.install_rails(&[one, same_device_new_qp, rail(2)], |gid| {
            Ok::<_, Infallible>(u32::from(gid[0]))
        });
        assert_eq!(installed.created, 2, "two distinct GIDs, two handles");
        assert_eq!(
            installed.already_held, 1,
            "the recycled queue pair must reuse the device's handle"
        );
        assert_eq!(c.len(), 2);
        assert_eq!(c.created(), 2);
        assert_eq!(c.evicted(), 0);
    }

    /// A second pass over the same writers builds nothing and reports every rail as already
    /// held — which on the announce path is exactly the "the pre-flight got there first"
    /// signal an operator reads.
    #[test]
    fn a_second_pass_builds_nothing_and_reports_it() {
        let mut c = cache(16);
        let rails: Vec<_> = (0..4).map(rail).collect();
        let first = c.install_rails(&rails, |gid| Ok::<_, Infallible>(u32::from(gid[0])));
        let second = c.install_rails(&rails, |_| -> Result<u32, Infallible> {
            panic!("a held GID must not be rebuilt")
        });
        assert_eq!(first.created, 4);
        assert_eq!(first.already_held, 0);
        assert_eq!(second.created, 0);
        assert_eq!(second.already_held, 4);
    }

    /// An empty rail list is a no-op rather than an error: a peer the handshake has not
    /// negotiated yet contributes no endpoints to a pre-flight answer, and that is a
    /// legitimate answer.
    #[test]
    fn an_empty_rail_list_installs_nothing() {
        let mut c = cache(16);
        let installed = c.install_rails(&[], |_| -> Result<u32, Infallible> {
            panic!("nothing to build")
        });
        assert_eq!(installed, super::Installed::default());
        assert!(c.is_empty());
    }

    /// Malformed input installs nothing and is distinguishable from an empty install, because
    /// the caller must not count it as an announce received.
    #[test]
    fn a_message_that_is_not_an_announce_installs_nothing() {
        let mut c = cache(16);
        let good = announce::encode(&[rail(0), rail(1)]).unwrap();
        for bad in [
            Vec::new(),                         // no header
            vec![0x01],                         // half a header
            vec![0x02, 0x01],                   // a version this build does not know
            vec![0x01, 0x00],                   // zero rails
            good[..good.len() - 1].to_vec(),    // truncated
            [good.as_slice(), &[0u8]].concat(), // trailing bytes: a layout disagreement
        ] {
            assert!(
                c.install_announce(&bad, |_| -> Result<u32, Infallible> {
                    panic!("a malformed announce must build nothing")
                })
                .is_none(),
                "{bad:?} decoded"
            );
        }
        assert!(c.is_empty());
    }

    /// The golden vector the C++ decoder in ADR-0031's NIXL plugin is checked against also
    /// installs through THIS path, so the fixed bytes cover the client's own install loop and
    /// not only the wire format's round trip.
    #[test]
    fn the_golden_vector_installs_one_handle() {
        let mut c = cache(16);
        let installed = c
            .install_announce(&announce::GOLDEN_ONE_RAIL, |gid| {
                Ok::<_, Infallible>(u32::from(gid[0]))
            })
            .expect("the golden vector must decode");
        assert_eq!(installed.created, 1);
        assert_eq!(c.len(), 1);
    }

    /// At the bound the least-recently-used entry goes, its handle is really destroyed, and
    /// the eviction is counted — the counter being the alarm that the bound was smaller than
    /// the concurrent working set.
    #[test]
    fn evicts_the_least_recently_used_and_counts_it() {
        let destroyed = Arc::new(AtomicUsize::new(0));
        let mut c: HandleCache<Tracked> = HandleCache::with_bound(2);
        let build = |_gid: [u8; 16]| {
            Ok::<_, Infallible>(Tracked {
                destroyed: Arc::clone(&destroyed),
            })
        };
        c.install_rails(&[rail(0), rail(1)], build);
        assert_eq!(c.len(), 2);
        assert_eq!(c.evicted(), 0);
        assert_eq!(destroyed.load(Ordering::Relaxed), 0);

        // Naming rail 0 again makes rail 1 the least recently used, so rail 1 is what goes.
        c.install_rails(&[rail(0)], build);
        let installed = c.install_rails(&[rail(2)], build);

        assert_eq!(installed.evicted, 1);
        assert_eq!(c.evicted(), 1);
        assert_eq!(c.len(), 2, "the bound is never exceeded");
        assert_eq!(
            destroyed.load(Ordering::Relaxed),
            1,
            "eviction must DESTROY the handle, which is the hazard the bound exists to make safe"
        );
        assert!(c.holds(&rail(0).gid), "the touched entry survived");
        assert!(c.holds(&rail(2).gid), "the new entry is in");
        assert!(!c.holds(&rail(1).gid), "the least recently used went");
    }

    /// A prefetch far larger than the bound — the 1000-node case the bound exists for — never
    /// exceeds it, holds the LAST `bound` writers named, and reports every eviction.
    #[test]
    fn a_prefetch_beyond_the_bound_stays_bounded() {
        const BOUND: usize = 8;
        const NAMED: u8 = 64;
        let mut c = cache(BOUND);
        let rails: Vec<_> = (0..NAMED).map(rail).collect();
        let installed = c.install_rails(&rails, |gid| Ok::<_, Infallible>(u32::from(gid[0])));
        assert_eq!(installed.created, usize::from(NAMED));
        assert_eq!(c.len(), BOUND);
        assert_eq!(
            installed.evicted as u64,
            u64::from(NAMED) - BOUND as u64,
            "everything past the bound must be accounted for"
        );
        assert_eq!(c.evicted(), installed.evicted as u64);
        // Least-recently-used means the survivors are the tail of the list.
        for i in (NAMED - BOUND as u8)..NAMED {
            assert!(c.holds(&rail(i).gid), "rail {i} should have survived");
        }
        assert!(!c.holds(&rail(0).gid), "the first named must have gone");
    }

    /// A `holds` query must not reorder anything: if it promoted its subject, the eviction
    /// order would depend on who asked rather than on who was written to.
    #[test]
    fn a_holds_query_does_not_touch_the_order() {
        let mut c = cache(2);
        let build = |gid: [u8; 16]| Ok::<_, Infallible>(u32::from(gid[0]));
        c.install_rails(&[rail(0), rail(1)], build);
        assert!(c.holds(&rail(0).gid));
        c.install_rails(&[rail(2)], build);
        assert!(!c.holds(&rail(0).gid), "the query must not have saved it");
    }

    /// A handle that cannot be built is counted, is not cached, and does not stop the pass —
    /// one unaddressable writer must not cost every other writer its handle.
    #[test]
    fn a_failed_build_is_counted_and_the_pass_continues() {
        let mut c = cache(16);
        let installed = c.install_rails(&[rail(0), rail(1), rail(2)], |gid| {
            if gid[0] == rail(1).gid[0] {
                Err("no route to that GID")
            } else {
                Ok(u32::from(gid[0]))
            }
        });
        assert_eq!(installed.created, 2);
        assert_eq!(installed.failed, 1);
        assert_eq!(c.len(), 2);
        assert!(!c.holds(&rail(1).gid));
    }

    /// A bound of zero would destroy every handle the instant it was built, so it is clamped
    /// rather than accepted — the same reasoning that clamps the announce ring's floor to 1.
    #[test]
    fn a_zero_bound_is_clamped_to_one() {
        let mut c = cache(0);
        assert_eq!(c.bound(), 1);
        c.install_rails(&[rail(0)], |gid| Ok::<_, Infallible>(u32::from(gid[0])));
        assert_eq!(c.len(), 1, "one handle must survive its own insertion");
    }

    /// The override is honoured in range and falls back outside it, including the values a
    /// YAML env entry produces (trailing newline) and the one that would break rather than
    /// degrade (zero).
    #[test]
    fn the_bound_override_is_honoured_or_falls_back() {
        assert_eq!(parse_max_handles(None), DEFAULT_MAX_HANDLES);
        assert_eq!(parse_max_handles(Some("4096")), 4096);
        assert_eq!(parse_max_handles(Some(" 4096 \n")), 4096);
        assert_eq!(parse_max_handles(Some("32")), MIN_MAX_HANDLES);
        assert_eq!(parse_max_handles(Some("65536")), MAX_MAX_HANDLES);
        for bad in ["0", "1", "31", "-1", "wat", "", "  ", "65537", "2048.5"] {
            assert_eq!(
                parse_max_handles(Some(bad)),
                DEFAULT_MAX_HANDLES,
                "{bad:?} should have fallen back"
            );
        }
    }
}
