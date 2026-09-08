//! Posting an [announce](crate::announce) — the writer's half of ADR-0030 point 2.
//!
//! A client can only receive a one-sided WRITE from a peer its own NIC can ACK, which
//! means an address-vector entry for that peer. It cannot be told about holders in
//! advance (the daemon owns placement), so the *writer* installs itself: one two-sided
//! SEND carrying every rail it might post from, immediately before its first WRITE to
//! that client. Measured on hardware, including the two facts that shape this code:
//!
//! * **a SEND does reach a target that has not inserted the sender** — the receive path
//!   resolves the sender from the packet, only the RDMA path needs the AV entry
//!   (`bench/ladder/results/c2-announce-gate.md`, L1.2);
//! * ⚠ **a SEND to a target with no receive posted does not fail, it HANGS** — no
//!   completion arrives at all, because a reliable transport keeps retrying (L1's
//!   `L1-INFO` line). That is why [`ANNOUNCE_TIMEOUT`] exists and is short: a client
//!   that has not posted a receive must cost a delivery its fallback, not a stalled
//!   serve holding a send-queue slot.
//!
//! ## What this is not
//!
//! It is not the daemon-to-daemon path. Peers already exchange endpoints over the
//! `Handshake` RPC and insert each other before any WRITE (`super::address::AhCache`,
//! with the compare-and-rebuild rule A1 finding 11 forced); a ring member never needs to
//! be announced to. This exists solely for the *client* edge, whose members are not in
//! the ring and do not handshake.
//!
//! ## Owed
//!
//! Two things this module cannot self-verify. First, its caller: the delivery path that
//! parses a client's per-rail token (ADR-0030 point 1) does not exist yet, so nothing in
//! the daemon calls [`Announcer::ensure_announced`] today. Second, hardware coverage —
//! the *mechanism* is proven in `spike/efa` (L1.1-L1.3) but this implementation of it has
//! only been compile-verified; the first two-node delivery arm should assert it rather
//! than assume it. The wire format itself is unit-tested in [`crate::announce`].
//!
//! ## One caution for whoever wires it up
//!
//! An announce consumes a send-queue slot on the rail it posts from, and today a full
//! send queue is unreachable *by construction* rather than handled — `MAX_SEND_WR` is
//! `HOLDER_ARENA_RANGES` and the serve gate is compile-time asserted below it. An
//! announce is posted under neither gate, so it is one of the posters ADR-0030 point 8
//! says the invariant has to be re-established across.

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use ibverbs::{AddressHandle, MemoryRegion, ProtectionDomain};

use crate::announce::{encode, AnnouncedRail};
use crate::client_registry::{
    ClientEndpoint, ClientRegistry, ClientRegistryCounters, ClientRegistryStats,
};

use super::buffers::efa_access_flags;
use super::context::{EfaContext, QKEY};
use super::write::next_wr_id;

/// How long to wait for an announce's send completion before giving up on the client.
///
/// The wire round trip is microseconds, so this is not a latency budget — it is the
/// deadline on the one failure mode hardware showed: a client that has posted no receive
/// produces **no completion at all**, and a reliable transport retries indefinitely. A
/// serve must not wait out that retry, so this is short enough that the delivery falls
/// back promptly and long enough that a merely busy client is not misjudged.
pub const ANNOUNCE_TIMEOUT: Duration = Duration::from_millis(250);

/// One rail's registered copy of this node's announce message.
///
/// The payload is identical on every rail (it names them all); what differs is the
/// protection domain it is registered on, because a send's local buffer must belong to
/// the PD of the queue pair posting it.
struct RailPayload {
    /// Declared **before** `bytes` so the MR is deregistered before the memory it
    /// describes is freed — `register_from_raw`'s requirement, kept by field order
    /// exactly as `super::arena` does it.
    mr: MemoryRegion<()>,
    /// Owns the announce message for the process lifetime. Never read through this
    /// field — the NIC reads it through `mr` — so it is `_`-prefixed in the idiom the
    /// rest of this repo uses for a value held purely to keep something else valid. A
    /// `Vec`'s heap buffer does not move when the `Vec` itself does, so the MR's pointer
    /// stays valid across moves of this struct.
    _bytes: Vec<u8>,
}

impl RailPayload {
    /// Register `message` on `pd`.
    ///
    /// # Errors
    ///
    /// `ibv_reg_mr` failing (out of `RLIMIT_MEMLOCK`, or an access bundle EFA rejects).
    fn new(pd: &ProtectionDomain, message: &[u8]) -> Result<Self> {
        let bytes = message.to_vec();
        // SAFETY: `bytes` is a live, writable allocation of exactly `bytes.len()` bytes
        // whose heap buffer this struct owns and keeps alive until after the MR is
        // deregistered (field order above), which is what `register_from_raw` requires.
        // The `unsafe` is inherent to the verbs FFI, not a soundness gap.
        // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
        let mr = unsafe {
            pd.register_from_raw(bytes.as_ptr().cast_mut(), bytes.len(), efa_access_flags())
        }
        .with_context(|| format!("registering a {}-byte announce payload", bytes.len()))?;
        Ok(Self { mr, _bytes: bytes })
    }
}

/// This node's announce: the message, one registered copy per rail, and who has already
/// been sent it.
///
/// Built once and shared; `ensure_announced` is the only entry point and is idempotent
/// per client endpoint, so a caller may invoke it before every WRITE without thinking
/// about whether it is the first.
pub struct Announcer {
    /// Rail index → that rail's registered payload. Indexed by the same rail numbering
    /// the transport uses everywhere else.
    payloads: Vec<RailPayload>,
    /// Clients already announced to, **bounded**. A [`ClientRegistry`] behind a `Mutex` rather
    /// than a lock-free map: the entry is written once per client endpoint, so contention is
    /// negligible and the simpler structure is the honest choice.
    ///
    /// This was an unbounded `HashSet` — one entry per client endpoint ever seen, freed only with
    /// the transport. It is now capped with least-recently-used eviction and an idle TTL off the
    /// same two numbers `super::client_write`'s two registries use, because all three key on one
    /// population ([`ClientEndpoint`]). The TTL matters here beyond memory: a record this set
    /// keeps is what makes [`Self::ensure_announced`] short-circuit, so a stale one *suppresses*
    /// the announce a recycled queue pair needs — the exact 2026-08-25 failure. Evidence
    /// ([`Self::forget`]) removes it when a WRITE reports `UNKNOWN_PEER`; idleness removes it when
    /// nothing ever reports anything.
    ///
    /// The value is `()`: membership is the whole record, and nothing here owns a device object,
    /// so eviction costs one re-announce (a single SEND) and nothing else.
    announced: Mutex<ClientRegistry<()>>,
    /// Outside the mutex, so a metrics scrape reads the entry count and the eviction counters
    /// with no lock — see [`ClientRegistryCounters`].
    counters: Arc<ClientRegistryCounters>,
}

/// The rails of the node owning `contexts`, as the announce entries a receiver has to hold
/// address handles for.
///
/// Extracted from [`Announcer::new`] because there is now a **second** consumer that must
/// name exactly the same endpoints: the pre-flight exchange
/// (`pacer_daemon::preflight`) answers a client with the same `{gid, qpn, rail}` set an
/// announce would carry, so that a client primed from the pre-flight and a client taught by
/// an announce build handles for the same GIDs *by construction* rather than by review. Two
/// derivations of "this node's endpoints" is precisely the drift that would make the
/// pre-flight worse than useless — it would prime addresses nobody writes from.
///
/// Rail order is transport rail order, and the `rail` field is that index: it is what pairs
/// a WRITE with the rail that sent it in a log line, and the by-index rule
/// (`EfaRdmaTransport::rails`) is what makes the same index meaningful to a peer.
///
/// # Errors
///
/// A rail whose endpoint carries no GID (EFA always provides one, so this is a bring-up
/// failure rather than a configuration).
pub(super) fn announced_rails(contexts: &[&EfaContext]) -> Result<Vec<AnnouncedRail>> {
    contexts
        .iter()
        .enumerate()
        .map(|(i, ctx)| {
            let ep = &ctx.local_endpoint;
            let gid = ep
                .gid
                .ok_or_else(|| anyhow!("rail {i} has no GID; EFA requires one"))?;
            Ok(AnnouncedRail {
                gid: <[u8; 16]>::from(gid),
                qpn: ep.qp_num,
                // Rail count is bounded by the device count, far below u16::MAX.
                rail: i as u16,
            })
        })
        .collect()
}

impl Announcer {
    /// Encode this node's rails once and register the message on every rail's PD.
    ///
    /// `rails` must be in transport rail order — `ensure_announced`'s `rail` argument
    /// indexes `contexts`, and the two are paired positionally.
    ///
    /// # Errors
    ///
    /// A rail whose endpoint carries no GID (EFA always provides one, so this is a
    /// bring-up failure rather than a configuration), the message exceeding the format's
    /// rail limit, or any rail's registration failing.
    pub fn new(contexts: &[&EfaContext]) -> Result<Self> {
        let rails = announced_rails(contexts)?;
        let message = encode(&rails)?;
        let payloads = contexts
            .iter()
            .enumerate()
            .map(|(i, ctx)| {
                RailPayload::new(ctx.pd(), &message)
                    .with_context(|| format!("rail {i}: registering the announce payload"))
            })
            .collect::<Result<Vec<_>>>()?;
        let announced = ClientRegistry::new();
        let counters = announced.counters();
        Ok(Self {
            payloads,
            announced: Mutex::new(announced),
            counters,
        })
    }

    /// How many client endpoints have been announced to — for a metrics hook, and for a
    /// test that wants to assert the message went out exactly once per client.
    #[must_use]
    pub fn announced_clients(&self) -> usize {
        self.stats().entries
    }

    /// Endpoints announced to right now and what this set has shed, by reason — read without
    /// taking the set's lock (see [`ClientRegistryCounters`]).
    #[must_use]
    pub fn stats(&self) -> ClientRegistryStats {
        self.counters.snapshot()
    }

    /// Drop every announce record idle past the TTL, returning how many went. See
    /// [`super::EfaRdmaTransport::sweep_expired_clients`].
    pub fn sweep_expired(&self) -> usize {
        self.announced().sweep_expired()
    }

    /// The set, recovering rather than propagating a poisoned lock: every critical section is a
    /// hash lookup, so poisoning can only come from a panic elsewhere, and refusing to announce
    /// forever afterwards would degrade every delivery on the node.
    fn announced(&self) -> MutexGuard<'_, ClientRegistry<()>> {
        self.announced
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Make sure `client` holds an address handle for this node, sending the announce if it has
    /// not been sent before.
    ///
    /// Call it before the first WRITE to a client, on the rail that WRITE will use — or
    /// before every WRITE; the second call for a client is a hash lookup, and it also refreshes
    /// the record's idle deadline, so a client being written to is never expired.
    ///
    /// `ah` must be the handle built from `client`'s GID, and is taken by `Arc` so it can outlive
    /// a SEND whose completion never arrives — the handle is evictable now, and the post hands a
    /// clone to the completion pump on exactly those paths (`Self::post_announce`).
    ///
    /// # Errors
    ///
    /// `rail` being out of range, the post failing, the completion carrying a failure
    /// status, or [`ANNOUNCE_TIMEOUT`] elapsing — which on this path most likely means
    /// **the client has no receive posted**, since that produces no completion at all.
    /// The caller should treat any error as "this client cannot be delivered to right
    /// now" and fall back to a body response rather than retrying in place.
    pub async fn ensure_announced(
        &self,
        ctx: &EfaContext,
        rail: usize,
        ah: &Arc<AddressHandle>,
        client: ClientEndpoint,
    ) -> Result<()> {
        if self.announced().get(&client).is_some() {
            return Ok(());
        }
        let payload = self.payloads.get(rail).ok_or_else(|| {
            anyhow!(
                "rail {rail} is beyond the {} this node has",
                self.payloads.len()
            )
        })?;
        self.post_announce(ctx, payload, ah, client.qpn).await?;
        // Recorded only after the completion: a failed or timed-out announce must be
        // retried by the next delivery, not remembered as done.
        self.announced().insert(client, ());
        Ok(())
    }

    /// Forget that this endpoint was announced to, so the next delivery announces again.
    ///
    /// Called with the only evidence that can exist: a WRITE to it completed `UNKNOWN_PEER`,
    /// which says the target holds no address handle for us and therefore never processed an
    /// announce — whatever this record claims (see [`ClientEndpoint`] on why the key cannot
    /// tell a recycled queue pair from the one it was created for). The idle TTL is the other
    /// half of the same job, for the stale record no WRITE ever reports on.
    ///
    /// Returns whether anything was removed, so the caller can distinguish "the record was
    /// stale and is now gone" from "we had never announced, and something else is wrong" —
    /// the second is not a case re-announcing fixes.
    pub(super) fn forget(&self, client: ClientEndpoint) -> bool {
        self.announced().remove(&client).is_some()
    }

    /// Post the announce as one signaled two-sided SEND and await its completion.
    ///
    /// ## Why the address handle is refcounted and handed to the pump
    ///
    /// A client AH is evictable now (`super::client_write::ClientAhCache`'s bound) and
    /// `AddressHandle::drop` is `ibv_destroy_ah`. On the two paths below where no completion
    /// arrived, the SEND may still be outstanding — a reliable transport retries indefinitely
    /// against a client with no receive posted, which is the whole reason [`ANNOUNCE_TIMEOUT`]
    /// exists — so releasing the handle here could destroy a device object the NIC is still
    /// using. The `Arc` clone therefore goes to the completion pump exactly as a WRITE's source
    /// buffer does, and is released when the CQE arrives or the queue pair is destroyed. The
    /// payload MR needs no such treatment: it lives for this `Announcer`'s lifetime.
    ///
    /// # Errors
    ///
    /// The post failing, the completion failing, or [`ANNOUNCE_TIMEOUT`] elapsing (see
    /// [`Self::ensure_announced`]).
    async fn post_announce(
        &self,
        ctx: &EfaContext,
        payload: &RailPayload,
        ah: &Arc<AddressHandle>,
        client_qpn: u32,
    ) -> Result<()> {
        let wr_id = next_wr_id();
        let pump = ctx.pump();
        let rx = pump.register(wr_id);
        let posted = ctx
            .post(|qp| {
                let mut batch = qp.start_send();
                batch
                    .to(ah, client_qpn, QKEY)
                    .signaled()
                    .send(wr_id, &[payload.mr.slice(..)]);
                // SAFETY: the payload's MR and the memory behind it live for this
                // `Announcer`'s lifetime (they are never mutated after registration), so
                // they outlive this send regardless of when its completion arrives —
                // including the timeout path below, where the NIC may still be reading.
                // The `unsafe` is inherent to the verbs FFI.
                // nosemgrep: rust.lang.security.unsafe-usage.unsafe-usage
                unsafe { batch.submit() }
            })
            .await;
        if let Err(e) = posted {
            pump.cancel(wr_id);
            return Err(anyhow::Error::new(e).context("posting the announce SEND"));
        }
        match tokio::time::timeout(ANNOUNCE_TIMEOUT, rx).await {
            Ok(Ok(result)) => result.context("the announce SEND completed with a failure"),
            Ok(Err(_)) => {
                // No completion was dispatched to us and none can be, so the WQE's fate is
                // unknowable from here: the handle stays alive in the pump (see this function's
                // doc). `orphan` also stands in for the `cancel` this used to do — the waiter is
                // already gone, which is why we are on this arm.
                pump.orphan(wr_id, Box::new(Arc::clone(ah)));
                Err(anyhow!("the completion pump dropped the announce waiter"))
            }
            Err(_) => {
                // Our deadline, not the device's. `orphan_if_unreaped` removes the waiter and
                // keeps the handle alive in one critical section — or declines, because a
                // completion raced in and the NIC is provably done.
                pump.orphan_if_unreaped(wr_id, Box::new(Arc::clone(ah)));
                Err(anyhow!(
                    "no completion for the announce within {ANNOUNCE_TIMEOUT:?} — the client \
                     most likely has no receive posted, which produces no completion at all \
                     rather than an error"
                ))
            }
        }
    }
}
