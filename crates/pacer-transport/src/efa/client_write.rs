//! Writing into a client-registered window (ADR-0030): address it, announce ourselves to
//! it, WRITE into it.
//!
//! This is [`EfaRdmaTransport::serve_via_write`]'s shape with the destination swapped.
//! There, a *peer* leased a range of its own arena and offered `(addr, rkey, len)` on the
//! `FetchBlob` call; here the **client** registered its own memory and named it in a header
//! ([`crate::token`]). The source side is identical — an ADR-0028 cache frame written in
//! place, or one staging copy — because nothing about a one-sided WRITE cares whose memory
//! it lands in.
//!
//! Three things differ, each measured rather than assumed:
//!
//! 1. **The requester is not a ring member**, so [`super::AhCache`] — keyed by `node_id`,
//!    populated by the peer handshake — cannot address it. [`ClientAhCache`] keys on the
//!    endpoint itself.
//! 2. **The client must hold an address handle for *us* before the WRITE**, or it completes
//!    `UNKNOWN_PEER` and nothing lands — true even between two endpoints on one device
//!    (`bench/ladder/results/c2-loopback-gate.md`). So the announce runs first, once per
//!    client endpoint (`bench/ladder/results/c2-announce-gate.md`).
//! 3. **A local hit is this same WRITE.** With no CUDA in the daemon there is no `memcpy`
//!    into a client's GPU window, so the daemon writes to itself — the loopback case L0
//!    proved, at 11.039 GiB/s on one rail.
//!
//! ## The remote half uses this same code, on the holder
//!
//! Nothing here is specific to the node the client is talking to. When a chunk lives on a
//! *peer*, the requester puts the client's token on the `FetchBlob` call
//! (`FetchBlobRequest.client_token`, `super::target::fetch_chunk_into_token`) and the
//! **holder** runs [`EfaRdmaTransport::write_into_token`] against it — its own announce, its
//! own address-handle cache, its own first-contact gate, its own rails. That is planning/19's
//! C3: N holders writing one client buffer at once, which is the only shape in which a single
//! read can exceed one node's fabric.
//!
//! Two consequences worth stating, because they are what the extra hop bought:
//!
//! * **Every holder pays first contact once per client endpoint.** The gate and the ladder
//!   below are per-transport, so a fleet of N holders serving one loader runs N announces per
//!   client rail rather than one. It is paid once each, and the alternative was routing every
//!   remote byte through the requester.
//! * **The digest moves with the bytes.** The delivered window cannot be read back (the
//!   daemon has no mapping of it), so the holder reports a CRC32 of what it wrote
//!   (`BlobMeta.written_crc32`) and the requester folds it in offset order like any other
//!   window's — ADR-0030 point 7's source-side digest, one node further out.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use anyhow::{anyhow, Result};
use bytes::Bytes;
use ibverbs::{
    AddressHandle, AddressHandleAttribute, Gid, LocalMemorySlice, RemoteMemorySlice, WcError,
    WcStatus,
};
use tokio::sync::Mutex;

use crate::client_registry::{ClientEndpoint, ClientRegistry, ClientRegistryCounters};
use crate::token::{TokenRail, TokenWindow};

use super::buffers::{RdmaBuffers, RdmaLease};
use super::context::{self, EfaContext};
use super::{write, Announcer, EfaRdmaTransport};

/// GID index and hop limit for a client address handle — the values every other AH in this
/// transport is built with (`super::address`), so a client is addressed exactly as a peer
/// is.
const GID_INDEX: u8 = 0;
const HOP_LIMIT: u8 = 64;

/// EFA's vendor error for `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER`: the target has no
/// address handle for this sender, so its NIC cannot acknowledge the WRITE and nothing lands.
/// Named here rather than inline because the whole retry below hangs off recognising it, and
/// it is the same 14 the loopback gate saw when a target held no handle
/// (`bench/ladder/results/c2-loopback-gate.md`).
const EFA_VENDOR_UNKNOWN_PEER: u32 = 14;

/// How many times a WRITE is posted while the client has not installed this writer.
///
/// Seven, which with the doubling backoff below spans ~126 ms — and that number comes from
/// what the client actually has to do, measured 2026-08-24. One announce names EVERY rail the
/// writer may post from (ADR-0030 point 1), so on decoding it the client calls
/// `ibv_create_ah` **once per writer rail** — 32 of them on a p5 — and `ibv_create_ah` is a
/// firmware admin command costing on the order of a millisecond. The daemon meanwhile fans its
/// first WRITEs across all 32 of its rails at once, so a WRITE from rail 31 can arrive tens of
/// milliseconds before the client's loop reaches rail 31's handle.
///
/// Three attempts (~6 ms) covered only the first few handles, and seven (~126 ms) still lost on
/// a 4-GPU loader whose 16 windows had 512 handles to build. Ten spans ~2 s, which covers a
/// whole fleet's loop; pairing (see `healthy_rail_at`) is what keeps that loop small in the
/// first place, and this is the belt to its braces. It is paid on FIRST CONTACT per endpoint
/// only — the gate below makes sure of that.
pub(super) const UNKNOWN_PEER_ATTEMPTS: usize = 10;

/// First wait between those attempts, doubling: 2, 4, 8, … 64 ms, ~126 ms in total. Starts
/// small because the common case is a handle that is milliseconds away, and doubles rather
/// than sleeping a flat 126 ms because a chunk that would otherwise have degraded the request
/// should pay only as much latency as it needs.
const UNKNOWN_PEER_BACKOFF: std::time::Duration = std::time::Duration::from_millis(2);

/// Address handles for client endpoints on one rail's protection domain, **bounded**.
///
/// Keyed by [`ClientEndpoint`] — `(gid, qpn)` — rather than by any name the client chose: the
/// *endpoint* is the identity that matters, and a client that restarts returns with the same GID
/// and a fresh queue pair, which must not resolve to the dead one's handle. The peer-side version
/// of that mistake is A1 finding 11 — RDMA silently stopped engaging with any peer after it
/// restarted — and keying on the endpoint makes it impossible here rather than caught by a
/// refresh rule.
///
/// `ibv_create_ah` is a firmware admin command, so the cache exists to keep it off the
/// per-chunk path: a checkpoint restore is thousands of chunks to the same few endpoints.
///
/// ## The bound (R4, was ADR-0030's open lifetime item)
///
/// This held one entry per client endpoint *ever seen*, freed only with the transport — which on
/// a long-lived multi-tenant daemon is one leaked `ibv_ah` per departed process, forever. It is
/// now a [`ClientRegistry`]: at most [`crate::client_registry::max_clients`] endpoints,
/// least-recently-used first out, and an endpoint idle for
/// [`crate::client_registry::client_ttl`] is dropped whether or not the cap is near. [`Announcer`]
/// and `ClientReady` are bounded the same way, off the same two numbers, because all three key
/// on the same population.
///
/// ## SAFETY invariant: an evicted handle outlives the work requests using it
///
/// `AddressHandle::drop` is `ibv_destroy_ah`, so eviction destroys a device object a posted WRITE
/// may still need — and this type is where that is made impossible rather than argued about. The
/// registry holds `Arc<AddressHandle>`, and every poster **clones the `Arc` into the work
/// request's source guard** (`write::SourceGuard`), the same type-erased keep-alive that already
/// protects the WRITE's source buffer (ADR-0028's "a completion that times out must not free the
/// frame"). So on every exit path — completion reaped, the software completion deadline elapsed,
/// waiter dropped — the handle is released exactly when the source bytes are: at the completion,
/// or by the completion pump when the CQE finally arrives or the queue pair is destroyed. Evicting
/// drops the *registry's* reference only.
///
/// Two further properties make the case belt-and-braces rather than load-bearing: the capacity
/// victim is the least recently *used* entry, and an endpoint with a WRITE in flight was used
/// microseconds ago; and the TTL only expires an endpoint nothing has addressed for minutes.
pub struct ClientAhCache {
    handles: Mutex<ClientRegistry<Arc<AddressHandle>>>,
    /// The registry's counters, kept OUTSIDE the mutex so a metrics scrape reads them with no
    /// lock at all. The previous `len` had to `try_lock` and report `0` on contention; that is
    /// tolerable for a gauge and not for a cumulative eviction count, which a Prometheus counter
    /// reset would read as a process restart.
    counters: Arc<ClientRegistryCounters>,
}

impl Default for ClientAhCache {
    fn default() -> Self {
        let handles = ClientRegistry::new();
        let counters = handles.counters();
        Self {
            handles: Mutex::new(handles),
            counters,
        }
    }
}

impl ClientAhCache {
    /// The address handle for `endpoint` on `ctx`'s PD, built once and then reused.
    ///
    /// Returns the handle by reference count rather than behind the cache's lock guard, which is
    /// what makes the bound safe (see the type's SAFETY invariant) and also takes the cache's
    /// mutex off the post path entirely: the guard the peer path has to drop before awaiting a
    /// completion (`serve_via_write`'s `drop(ah_ref)`, a measured ~2× loss when held) does not
    /// exist here.
    ///
    /// Looking an endpoint up refreshes its recency and its idle deadline, so an endpoint being
    /// written to is never the eviction victim.
    ///
    /// # Errors
    ///
    /// `ibv_create_ah` failing — an unreachable or malformed client GID.
    async fn handle_for(
        &self,
        ctx: &EfaContext,
        endpoint: ClientEndpoint,
    ) -> Result<Arc<AddressHandle>> {
        let mut handles = self.handles.lock().await;
        let ah = handles.get_or_try_insert_with(endpoint, || build_client_ah(ctx, endpoint))?;
        Ok(Arc::clone(ah))
    }

    /// How many client endpoints this rail has handles for, and what it has shed — the gauge a
    /// cap is sized against and the counters that say whether it is big enough. Read without
    /// taking the cache's lock.
    #[must_use]
    pub fn stats(&self) -> crate::client_registry::ClientRegistryStats {
        self.counters.snapshot()
    }

    /// How many client endpoints this rail has handles for.
    #[must_use]
    pub fn len(&self) -> usize {
        self.stats().entries
    }

    /// Whether nothing has been addressed yet. Present because clippy asks for it beside
    /// [`ClientAhCache::len`].
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Destroy the handles of every endpoint idle past the TTL, returning how many went. See
    /// [`EfaRdmaTransport::sweep_expired_clients`] for why a caller other than an insert exists.
    pub async fn sweep_expired(&self) -> usize {
        self.handles.lock().await.sweep_expired()
    }
}

/// One `ibv_create_ah` for a client endpoint, refcounted so the registry's copy and every
/// in-flight work request's copy are the same device object.
///
/// # Errors
///
/// `ibv_create_ah` failing — an unreachable or malformed client GID.
fn build_client_ah(ctx: &EfaContext, endpoint: ClientEndpoint) -> Result<Arc<AddressHandle>> {
    let mut attr = AddressHandleAttribute::new(context::PORT_NUM);
    attr.set_grh(Gid::from(endpoint.gid), GID_INDEX, HOP_LIMIT, 0);
    ctx.pd()
        .create_address_handle(&attr)
        .map(Arc::new)
        .map_err(|e| {
            anyhow!(
                "creating an address handle for client qp {}: {e}",
                endpoint.qpn
            )
        })
}

/// Which client endpoints this rail has already written to successfully, **bounded**.
///
/// **Why a gate and not just a retry.** The un-installed-writer race (see
/// [`EfaRdmaTransport::post_into_client`]) resolves once per endpoint, when the client's pump
/// finishes building its address handles. A bare per-chunk retry therefore has every chunk
/// rediscover the same fact: measured 2026-08-24, a checkpoint's first batch burned 8030
/// retries and still declined 4015 chunks, and because each attempt re-stages the body into an
/// arena range, most of that cost was 16 MiB memcpys rather than waiting.
///
/// So first contact is serialized per endpoint: one chunk runs the ladder while the others
/// await this gate, and once it opens nothing pays anything again. The proof is checked under a
/// `std` mutex — never held across an `.await` — so the steady state is one hash lookup.
///
/// ## The bound, and one entry instead of two maps
///
/// This was two unbounded maps keyed identically, a `HashSet` of proven endpoints and a
/// `HashMap` of gates, both growing for the process lifetime. They are now one bounded
/// [`ClientRegistry`] of [`ClientGate`] — same key, same population, one eviction decision —
/// sharing the capacity and idle TTL [`ClientAhCache`] and [`Announcer`] use.
///
/// The TTL earns its place here beyond memory. Proof of liveness expires on **evidence** today
/// ([`Self::forget`], called on an `UNKNOWN_PEER` completion); the TTL expires it on
/// **idleness**, which covers the recycled queue pair that no evidence ever reached — a fresh
/// process handed `qpn=49153` long after the last WRITE to it now finds no proof and re-runs
/// first contact, instead of taking the un-retried fast path a dead process left open.
///
/// Nothing here owns a device object, so eviction is free: the values are a `bool` and an
/// uncontended mutex.
pub struct ClientReady {
    endpoints: std::sync::Mutex<ClientRegistry<ClientGate>>,
    /// Outside the mutex, for the same reason as [`ClientAhCache::counters`].
    counters: Arc<ClientRegistryCounters>,
}

/// What this rail knows about one client endpoint's first contact.
#[derive(Default)]
struct ClientGate {
    /// Whether a WRITE to this endpoint has completed on this rail.
    proven: bool,
    /// The lock one chunk holds while proving it.
    gate: FirstContact,
}

/// The lock one chunk holds while proving an endpoint will accept a WRITE.
type FirstContact = Arc<tokio::sync::Mutex<()>>;

impl Default for ClientReady {
    fn default() -> Self {
        let endpoints = ClientRegistry::new();
        let counters = endpoints.counters();
        Self {
            endpoints: std::sync::Mutex::new(endpoints),
            counters,
        }
    }
}

impl ClientReady {
    /// A gate map with explicit limits, so a test can drive expiry in milliseconds instead of the
    /// five minutes an operator gets. Test-only: production always takes the resolved numbers, or
    /// two rails of one node would disagree about their own ceiling.
    #[cfg(test)]
    fn with_limits(capacity: usize, ttl: std::time::Duration) -> Self {
        let endpoints = ClientRegistry::with_limits(capacity, ttl);
        let counters = endpoints.counters();
        Self {
            endpoints: std::sync::Mutex::new(endpoints),
            counters,
        }
    }

    /// Whether this endpoint has already accepted a WRITE on this rail.
    ///
    /// Refreshes the endpoint's recency and idle deadline: this runs once per chunk, so it is
    /// also the signal that says the endpoint is live and must not be evicted.
    fn is_proven(&self, endpoint: ClientEndpoint) -> bool {
        self.lock()
            .get_mut(&endpoint)
            .is_some_and(|entry| entry.proven)
    }

    /// Record that it has, so no later chunk waits on the gate.
    fn mark_proven(&self, endpoint: ClientEndpoint) {
        self.lock().get_or_insert_default(endpoint).proven = true;
    }

    /// Forget the proof, because proof of liveness EXPIRES: a client that restarts returns with
    /// the same device GID and may be handed the same queue-pair numbers, so an endpoint this
    /// rail proved minutes ago can be a process that has installed nothing. Measured 2026-08-24
    /// — a fresh loader inherited `qpn=1`/`49153` from a finished one, took the un-retried fast
    /// path, and its GET failed with a 500 while the retry counter stayed at zero.
    ///
    /// Clears the flag rather than removing the entry, so a chunk already waiting on this
    /// endpoint's gate keeps waiting on the same one — removing it would hand the next arrival a
    /// *different* mutex and let two chunks run the ladder at once.
    fn forget(&self, endpoint: ClientEndpoint) {
        if let Some(entry) = self.lock().get_mut(&endpoint) {
            entry.proven = false;
        }
    }

    /// The per-endpoint first-contact gate. Cloned out so the caller can await it without
    /// holding the map's lock.
    fn gate(&self, endpoint: ClientEndpoint) -> FirstContact {
        Arc::clone(&self.lock().get_or_insert_default(endpoint).gate)
    }

    /// Endpoints this rail is tracking first contact for, and what it has shed. Read without
    /// taking the lock.
    #[must_use]
    pub fn stats(&self) -> crate::client_registry::ClientRegistryStats {
        self.counters.snapshot()
    }

    /// Drop every endpoint idle past the TTL, returning how many went. See
    /// [`EfaRdmaTransport::sweep_expired_clients`].
    pub fn sweep_expired(&self) -> usize {
        self.lock().sweep_expired()
    }

    /// The registry, recovering rather than propagating a poisoned lock.
    ///
    /// Every critical section here is a hash lookup and a field write, so the only way this lock
    /// can be poisoned is a panic elsewhere in the process — and answering "not proven" forever
    /// afterwards would silently degrade every delivery to the retry ladder. Recovering keeps the
    /// map's contents, which are still exactly what was recorded.
    fn lock(&self) -> std::sync::MutexGuard<'_, ClientRegistry<ClientGate>> {
        self.endpoints
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

/// What a WRITE into a client-registered window did.
///
/// Replaced a bare `bool` because the caller's correct response differs by *reason*: some
/// declines resolve in milliseconds and are worth re-attempting for this one chunk, and some
/// are true of every chunk in the request so that re-attempting them is pure waste. A `bool`
/// forced one policy on both, and the policy it forced was to throw away the whole GET's
/// acceleration on the first decline of either kind (planning/19 § Track C, "one declined
/// chunk degrades the whole GET to a body").
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenWrite {
    /// The bytes are in the client's memory. A reaped send completion on a reliable
    /// transport is that proof.
    Wrote,
    /// RDMA was not used for this chunk, benignly. The caller delivers the same bytes
    /// another way — or, for a client-registered window where there is no other way,
    /// decides between re-attempting and degrading on [`TokenDecline::is_transient`].
    Declined(TokenDecline),
}

/// Why a WRITE into a client-registered window did not happen.
///
/// The split that matters is [`Self::is_transient`], not the individual variants: everything
/// here is a benign "not right now", but only some of them can become "yes" while the same
/// request is still running.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TokenDecline {
    /// No rail on this node has a live completion plane, so there is nothing to post from.
    /// Node-wide and terminal — the transport has flipped RDMA capability off and every
    /// chunk of every request will get this answer until a pump is rebuilt.
    NoHealthyRail,
    /// The token named no rail this node can address, or the requested sub-window is not
    /// inside what the client registered. A property of the descriptor, identical on every
    /// re-attempt.
    NotAddressable,
    /// The body is larger than a staging range on the chosen rail, i.e. the two ends'
    /// chunk geometry disagrees. A property of the configuration, identical on every
    /// re-attempt.
    BodyExceedsStaging,
    /// The client could not be announced to right now — most often because it has no
    /// receive posted, which surfaces as the announce timeout rather than an error.
    /// **Transient**: the client's pump reposts, and this is the shape a depth ceiling on
    /// announce receive slots produces.
    NotAnnounceable,
    /// The client never installed this writer within the WRITE retry ladder (ten posts over
    /// ~2 s, plus a re-announce). **Transient**: it resolves once the client's pump finishes
    /// building address handles, which at 8 GPUs is up to 1024 of them.
    WriterNotInstalled,
}

impl TokenDecline {
    /// Whether re-attempting this window can plausibly succeed while the same request is
    /// still in flight.
    ///
    /// The two transient variants are both *the client has not caught up yet*, and both were
    /// measured resolving in milliseconds (`bench/ladder/results/c5-multirail.md`,
    /// `c5-multigpu-remeasure.md`). The three others are properties of the node, the
    /// descriptor or the configuration: re-attempting them costs a staging copy and a
    /// wire round trip to learn the same thing again.
    #[must_use]
    pub fn is_transient(self) -> bool {
        matches!(self, Self::NotAnnounceable | Self::WriterNotInstalled)
    }

    /// A stable label for the metric dimension, so a decline is countable by reason rather
    /// than only visible in a log line.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::NoHealthyRail => "no_healthy_rail",
            Self::NotAddressable => "not_addressable",
            Self::BodyExceedsStaging => "body_exceeds_staging",
            Self::NotAnnounceable => "not_announceable",
            Self::WriterNotInstalled => "writer_not_installed",
        }
    }
}

impl EfaRdmaTransport {
    /// WRITE `body` into `window` at `dst_at`, from this node.
    ///
    /// See [`TokenWrite`] for the outcomes and [`TokenDecline::is_transient`] for the one
    /// distinction a caller has to act on.
    ///
    /// # Errors
    ///
    /// A genuine transport failure only: the post rejected, or the WRITE completing with a
    /// failure status. Those say the fabric or the descriptor is wrong — not that this
    /// request should take another path.
    pub async fn write_into_token(
        &self,
        window: &TokenWindow,
        dst_at: usize,
        body: &Bytes,
    ) -> Result<TokenWrite> {
        // `pick_healthy_rail` (target.rs) is the same choice ADR-0026's delivery already
        // makes, and reusing it keeps one definition of "healthy": any rail can reach any
        // client rail — L0.6 measured a WRITE from one rail landing in a window registered
        // on another — so the SOURCE rail is load balancing and the client's own preference
        // order governs the destination only.
        // Round-robin the SOURCE rail over every healthy one, and the destination independently
        // over the token's. Both spreads are load-bearing and were measured 2026-08-24 by
        // getting them wrong: pairing a destination to one source rail to shrink the
        // address-handle count put all 8440 writes of a 70B checkpoint on a SINGLE rail and cost
        // 4.3 GiB/s against 9.9 — one queue pair plateaus near 6 GiB/s (planning/18's multi-QP
        // sweep), so the source side needs width whatever the destination does. The handle count
        // that pairing was protecting is handled where it belongs instead: the first-contact gate
        // makes one chunk per endpoint wait, and the ladder covers the client's build.
        let Ok(rail_idx) = self.pick_healthy_rail() else {
            return Ok(TokenWrite::Declined(TokenDecline::NoHealthyRail));
        };
        let Some((client, remote)) = self.address_client(window, dst_at, body.len()) else {
            return Ok(TokenWrite::Declined(TokenDecline::NotAddressable));
        };
        let rail = &self.rails[rail_idx];
        if body.len() > rail.holder_arena.slot_bytes() {
            // Same benign-fallback reasoning as `serve_via_write`: a body that cannot be
            // staged means the two ends' chunk geometry already disagreed.
            return Ok(TokenWrite::Declined(TokenDecline::BodyExceedsStaging));
        }
        if !self.announce_to(rail_idx, client).await {
            return Ok(TokenWrite::Declined(TokenDecline::NotAnnounceable));
        }
        self.post_into_client(rail_idx, client, remote, body).await
    }

    /// Pick the client rail to address, and the remote descriptor for this chunk.
    ///
    /// **Round-robin over every rail the token names, chunk by chunk.** A client rail is a
    /// destination NIC, and one of them is a hard ~11 GiB/s ceiling however many chunks are in
    /// flight — measured, not inferred: C5 put 16 shards of a checkpoint in flight at once and
    /// stopped at ~10 GiB/s aggregate (`bench/ladder/results/c5-safetensors-8b.md`). Spreading
    /// the WRITEs is the only thing that lifts it, and it costs nothing on the source side,
    /// since any rail can reach any client rail (L0.6).
    ///
    /// The counter is this transport's own rather than the source rail's index, deliberately:
    /// a node with ONE healthy rail would otherwise always pick source 0 and therefore always
    /// destination 0, striping nothing on exactly the cheap single-interface shapes (r8gd has
    /// one EFA device) where the client is most likely to be the bottleneck.
    ///
    /// What this gives up is the *preference* reading of point 1: with several rails named, the
    /// order no longer selects, it only orders. That is the client's call to make — it publishes
    /// the set it wants written to, and for an HBM window that set is the rails on the GPU's own
    /// PCIe switch (`WindowSpec::rail_count`). A writer that picks exactly one still honours
    /// preference, because it takes the first.
    ///
    /// `None` when the sub-window does not fit what the client registered — refused rather than
    /// clamped, because a clamped delivery is a short read the client has no way to notice.
    fn address_client(
        &self,
        window: &TokenWindow,
        dst_at: usize,
        len: usize,
    ) -> Option<(TokenRail, RemoteMemorySlice)> {
        let rails = window.rails();
        // `TokenWindow::new` refuses an empty rail list, so this holds by construction — but a
        // modulo by zero is a panic on the data path, and refusing costs nothing.
        if rails.is_empty() {
            return None;
        }
        let ticket = self.next_client_rail.fetch_add(1, Ordering::Relaxed);
        let client = *rails.get(client_rail_index(ticket, rails.len()))?;
        let (addr, rkey, len) = window.slice_on(&client, dst_at, len)?;
        Some((client, RemoteMemorySlice { addr, len, rkey }))
    }

    /// Make sure this client can acknowledge our WRITEs, building the announcer on first
    /// use. `false` means it cannot right now — deliver another way.
    async fn announce_to(&self, rail_idx: usize, client: TokenRail) -> bool {
        let rail = &self.rails[rail_idx];
        let contexts: Vec<&EfaContext> = self.rails.iter().map(|r| r.ctx.as_ref()).collect();
        let announcer = match self
            .announcer
            .get_or_try_init(|| async { Announcer::new(&contexts) })
            .await
        {
            Ok(announcer) => announcer,
            Err(e) => {
                tracing::warn!(error = %format!("{e:#}"), "could not build this node's announce");
                return false;
            }
        };
        let endpoint = ClientEndpoint::from(&client);
        let ah = match rail.client_ah.handle_for(&rail.ctx, endpoint).await {
            Ok(ah) => ah,
            Err(e) => {
                tracing::debug!(error = %format!("{e:#}"), qpn = client.qpn, "client GID not addressable");
                return false;
            }
        };
        // The handle is held by reference count for the whole announce, not behind the cache's
        // lock: the SEND's completion is awaited under [`super::ANNOUNCE_TIMEOUT`], and holding a
        // cache lock across that would serialise every first contact in the process behind it.
        // The `Arc` is what keeps the handle alive across the await — and past it, since
        // `post_announce` hands a clone to the completion pump on the paths where the SEND may
        // still be outstanding.
        let announced = announcer
            .ensure_announced(&rail.ctx, rail_idx, &ah, endpoint)
            .await;
        if let Err(e) = announced {
            // Most likely the client has posted no receive: that produces no completion at
            // all, so it surfaces as the timeout rather than as an error (measured, L1).
            tracing::debug!(error = %format!("{e:#}"), qpn = client.qpn, "client not announceable; delivering another way");
            return false;
        }
        true
    }

    /// Drop the record that this client was announced to, then announce again.
    ///
    /// The recovery for a recycled queue-pair number (see [`Announcer::forget`]): the endpoint
    /// answers `UNKNOWN_PEER` because it never processed an announce, while our own record
    /// says we sent one to whatever process held these numbers before. `false` means it cannot
    /// be announced to right now, which the caller treats as "deliver another way".
    async fn reannounce_to(&self, rail_idx: usize, client: TokenRail) -> bool {
        let stale = self
            .announcer
            .get()
            .is_some_and(|announcer| announcer.forget(ClientEndpoint::from(&client)));
        tracing::debug!(
            qpn = client.qpn,
            stale,
            "endpoint holds no handle for us; re-announcing"
        );
        self.announce_to(rail_idx, client).await
    }

    /// Post the WRITE into the client, retrying while the client has not installed us yet.
    ///
    /// **Why a retry exists at all.** `announce_to` proves the announce SEND completed, which
    /// proves it reached the client's queue pair — not that the client has decoded it and built
    /// an address handle for us. That happens on the client's own pump thread, so a first WRITE
    /// to a freshly announced endpoint can beat it and complete `REM_OP_ERR` / vendor 14
    /// (`UNKNOWN_PEER`), which is not a fault: the same WRITE succeeds a millisecond later.
    ///
    /// Measured 2026-08-24 (`bench/ladder/results/c5-multirail.md`): with shards loading
    /// concurrently into a CPU-busy client — verification pinning every core — that race made a
    /// GET fail with a 500. Sequentially it never fired, which is why C5's first arms were
    /// green.
    ///
    /// After [`UNKNOWN_PEER_ATTEMPTS`] it degrades to a
    /// [`TokenDecline::WriterNotInstalled`] rather than propagating. A client that cannot be
    /// addressed is exactly the benign fallback this path already has for "not
    /// announceable"; failing the read instead turns a transient fabric state into a
    /// customer's error.
    ///
    /// # Errors
    ///
    /// The post failing, or a completion whose status is anything OTHER than an un-installed
    /// peer — those say the descriptor or the fabric is wrong, not that this should be retried.
    async fn post_into_client(
        &self,
        rail_idx: usize,
        client: TokenRail,
        remote: RemoteMemorySlice,
        body: &Bytes,
    ) -> Result<TokenWrite> {
        let rail = &self.rails[rail_idx];
        let endpoint = ClientEndpoint::from(&client);
        // Steady state: this endpoint has taken a WRITE before, so post once and let a failure
        // be a failure. Only first contact can be racing an address handle that does not exist
        // yet, and pretending otherwise would retry real faults.
        if rail.client_ready.is_proven(endpoint) {
            match self
                .attempt_into_client(rail_idx, client, remote, body)
                .await
            {
                Ok(()) => return Ok(TokenWrite::Wrote),
                // A proven endpoint that answers UNKNOWN_PEER is a DIFFERENT process wearing
                // the same address (see `ClientReady::forget`). Drop the proof and fall through
                // to first contact rather than failing: the ladder is what establishes whether
                // anyone is home.
                Err(e) if is_unknown_peer(&e) => {
                    rail.client_ready.forget(endpoint);
                }
                Err(e) => return Err(e),
            }
        }
        // First contact for this endpoint on this rail: exactly one chunk runs the ladder, the
        // rest wait here and then take the fast path above's single attempt.
        let gate = rail.client_ready.gate(endpoint);
        let _first = gate.lock().await;
        if rail.client_ready.is_proven(endpoint) {
            self.attempt_into_client(rail_idx, client, remote, body)
                .await?;
            return Ok(TokenWrite::Wrote);
        }
        let mut backoff = UNKNOWN_PEER_BACKOFF;
        for attempt in 1..=UNKNOWN_PEER_ATTEMPTS {
            match self
                .attempt_into_client(rail_idx, client, remote, body)
                .await
            {
                Ok(()) => {
                    rail.client_ready.mark_proven(endpoint);
                    return Ok(TokenWrite::Wrote);
                }
                Err(e) if !is_unknown_peer(&e) => return Err(e),
                // FIRST failure: re-announce before retrying, because the most likely reason
                // an endpoint holds no handle for us is that it never got an announce — and
                // no number of WRITE retries fixes that. `Announcer` remembers endpoints by
                // `(gid, qpn)` and a queue-pair number is RECYCLED, so a fresh client process
                // inherits a record left by a previous one and `ensure_announced`
                // short-circuits. Measured 2026-08-25 (70B, 8 GPUs): every decline named the
                // same `qpn=49153`, ten attempts each, and the arm degraded to the body path.
                //
                // Done once rather than on every attempt: one SEND is enough to make the
                // client build its handles, and the remaining attempts are what waits for it.
                Err(_) if attempt == 1 => {
                    self.unknown_peer_retries.fetch_add(1, Ordering::Relaxed);
                    if !self.reannounce_to(rail_idx, client).await {
                        // Not announceable at all now — the same benign fallback as an
                        // endpoint that never answered: deliver this chunk another way.
                        return Ok(TokenWrite::Declined(TokenDecline::NotAnnounceable));
                    }
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
                Err(e) if attempt == UNKNOWN_PEER_ATTEMPTS => {
                    self.unknown_peer_declines.fetch_add(1, Ordering::Relaxed);
                    tracing::warn!(
                        error = %format!("{e:#}"),
                        qpn = client.qpn,
                        attempts = UNKNOWN_PEER_ATTEMPTS,
                        "client never installed this writer; delivering this chunk another way"
                    );
                    return Ok(TokenWrite::Declined(TokenDecline::WriterNotInstalled));
                }
                Err(_) => {
                    self.unknown_peer_retries.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(backoff).await;
                    backoff *= 2;
                }
            }
        }
        // Unreachable: the loop returns on every branch of its last iteration.
        Ok(TokenWrite::Declined(TokenDecline::WriterNotInstalled))
    }

    /// One WRITE: stage the body if it is not already in a registered frame, post, await.
    ///
    /// # Errors
    ///
    /// The post failing, or the completion carrying a failure status — including the
    /// un-installed-peer status its caller retries.
    async fn attempt_into_client(
        &self,
        rail_idx: usize,
        client: TokenRail,
        remote: RemoteMemorySlice,
        body: &Bytes,
    ) -> Result<()> {
        let rail = &self.rails[rail_idx];
        let _window = rail.enter_window().await;
        // Addressed before anything is staged: an unaddressable client should not cost a 16 MiB
        // `copy_from_slice` to discover, and the handle has to be in hand to travel with the
        // source guard below.
        let ah = rail
            .client_ah
            .handle_for(&rail.ctx, ClientEndpoint::from(&client))
            .await?;
        // Identical to the holder path: a slab frame is already registered on this rail, so
        // the WRITE reads it in place; otherwise stage once into an arena range. Either way
        // `source` owns the memory until the completion proves the NIC is done reading it
        // (ADR-0028's "a completion that times out must not free the frame").
        //
        // **The address handle rides along in that same guard.** Unlike the peer path — where an
        // AH lives in the cache until a handshake replaces it, so invariant R7 only had to argue
        // that a *mid-flight* destruction would be benign — a client AH is now evictable
        // ([`ClientAhCache`]'s bound), and `AddressHandle::drop` is `ibv_destroy_ah`. Pairing the
        // `Arc` clone with the source bytes gives both the same release point on every exit path:
        // at the reaped completion, or in the completion pump when the CQE finally arrives or the
        // queue pair is destroyed. Eviction therefore cannot destroy a handle a work request is
        // still using, rather than being safe only because a provider does not re-read it.
        let source: write::SourceGuard;
        let local: LocalMemorySlice = match self
            .cache_slab
            .as_ref()
            .and_then(|slab| slab.local_slice(rail_idx, body))
        {
            Some(in_place) => {
                source = Box::new((body.clone(), Arc::clone(&ah)));
                in_place
            }
            None => {
                let mut lease = rail.holder_arena.lease().await;
                lease.with_bytes_mut(|dst| dst[..body.len()].copy_from_slice(body));
                let slice = lease.local_slice(0..body.len());
                source = Box::new((lease, Arc::clone(&ah)));
                slice
            }
        };
        let posted =
            write::post_write(&rail.ctx, &ah, client.qpn, context::QKEY, local, remote).await?;
        self.post_batch_nanos
            .fetch_add(posted.post_batch_nanos(), Ordering::Relaxed);
        // **The delivery path joins the per-rail depth and timing accounting here**, which
        // until now covered only peer serves — so `pacer_rdma_rail{metric="writes_in_flight"}`
        // and `pacer_rdma_write_completion_wait_seconds_total` sat at zero through a whole
        // checkpoint delivery (measured in Prometheus: 176 consecutive samples flat at 0 while
        // `pacer_delivery_chunks_total` advanced, 2026-08-24). That is the same blind spot the
        // per-rail WRITE COUNTS had one commit earlier, and it hides the question a
        // multi-GPU arm asks: 18.5 GiB/s sustained against a 26.7 peak is either a shallow
        // pipe or a long hold, and depth × hold is what tells them apart.
        //
        // Incremented after a successful post (a failed post never reached the wire) and
        // decremented on every exit path, exactly as `serve_via_write` does it.
        rail.writes_in_flight.fetch_add(1, Ordering::Relaxed);
        let wait_start = std::time::Instant::now();
        let completion = write::await_write_completion(posted, source).await;
        self.write_completion_wait_nanos
            .fetch_add(wait_start.elapsed().as_nanos() as u64, Ordering::Relaxed);
        rail.writes_in_flight.fetch_sub(1, Ordering::Relaxed);
        completion?;
        // The same per-rail accounting the PEER path keeps (`serve_via_write`), which until now
        // covered only that path: a delivery moved bytes on a rail without any of the
        // `pacer_rdma_rail{metric="writes_total"}` series noticing, so "did these WRITEs spread
        // over the rails at all?" had no answer. It is exactly the question a multi-GPU arm asks
        // (2026-08-24), and per-rail totals are also what the EFA hardware counters get compared
        // against. Successful writes only, for the same reason as there: a failed one is
        // delivered another way, and counting its bytes here would make the two disagree.
        rail.writes_total.fetch_add(1, Ordering::Relaxed);
        rail.write_bytes_total
            .fetch_add(body.len() as u64, Ordering::Relaxed);
        Ok(())
    }
}

/// Whether `error` is a completion saying the target has not installed this writer.
///
/// Structural, not a string match: the typed `WcError` is preserved through the completion
/// pump precisely so this decision can be made on `status` + `vendor_err` (see
/// `completion.rs`'s `drain_and_dispatch`).
///
/// Used by two decisions, which is why it is `pub(super)`: whether to retry a client WRITE
/// (here), and how much of the peer AH cache a holder-side completion failure justifies
/// evicting (`super::holder_eviction_scope`). Both hinge on the same fact — that the target
/// holds no queue pair for us — so they must not drift onto two definitions of it.
pub(super) fn is_unknown_peer(error: &anyhow::Error) -> bool {
    error.downcast_ref::<WcError>().is_some_and(|wc| {
        wc.status == WcStatus::RemoteOperationError && wc.vendor_err == EFA_VENDOR_UNKNOWN_PEER
    })
}

/// A stable, bounded metric label for why a delivery WRITE **failed** — the error
/// counterpart of [`TokenDecline::label`], and for the same reason: a failure that is only
/// visible in a log line is a failure nobody is alerted on.
///
/// Derived from the typed `WcError` the completion pump preserves, exactly as
/// `is_unknown_peer` is (private, hence unlinked), so this cannot drift onto a parse of an
/// error message — see that function's own note on why the type survives the trip. Failures
/// carrying no completion at all — a rejected post, a completion that never arrived, a pump
/// that dropped the waiter — share one deliberately coarse bucket; see the label's own doc.
///
/// **Cardinality is bounded by the `WcStatus` enum** (plus one bucket for no completion and
/// one for a status a future `ibverbs` adds), which is what makes it usable as a Prometheus
/// label at all. The vendor error code, the client's queue-pair number and its GID are all
/// excluded on purpose: the first is an unenumerated firmware value and the other two are
/// per-client-process, so any of the three would let one workload mint series without limit.
/// All three remain in the log line beside the increment.
#[must_use]
pub fn write_failure_reason(error: &anyhow::Error) -> &'static str {
    super::completion::failure_label(error)
}

/// Which of `count` client rails the WRITE holding cursor value `ticket` addresses.
///
/// A free function so the rule is testable without a fabric: the transport it belongs to needs
/// real devices to exist, and what matters here is arithmetic — every rail gets an equal share
/// and a full cycle touches each exactly once. Anything cleverer (hashing the offset, weighting
/// by observed completion time) would be a change to *this* function and its test.
fn client_rail_index(ticket: u64, count: usize) -> usize {
    ticket as usize % count
}

#[cfg(test)]
mod decline_tests {
    use super::TokenDecline;

    /// The classification, asserted variant by variant rather than by a rule, because it is
    /// the whole decision the caller makes and getting one wrong is silent: a transient
    /// variant misfiled as terminal throws away a request's acceleration, and a terminal one
    /// misfiled as transient buys a staging copy and a wire round trip per retry to learn
    /// what the first attempt already knew.
    #[test]
    fn only_the_client_has_not_caught_up_yet_is_transient() {
        // Both of these are "the client's pump has not got there yet", and both were
        // measured resolving in milliseconds.
        assert!(TokenDecline::NotAnnounceable.is_transient());
        assert!(TokenDecline::WriterNotInstalled.is_transient());
        // Node-wide: no rail has a live completion plane, so no chunk of any request can be
        // written until a pump is rebuilt.
        assert!(!TokenDecline::NoHealthyRail.is_transient());
        // Properties of the descriptor and of the configuration respectively — identical on
        // every re-attempt by construction.
        assert!(!TokenDecline::NotAddressable.is_transient());
        assert!(!TokenDecline::BodyExceedsStaging.is_transient());
    }

    /// Labels are a metric dimension, so they must be distinct and stable. A duplicate would
    /// silently merge two reasons into one series and make the resulting count unreadable.
    #[test]
    fn every_decline_has_its_own_label() {
        let all = [
            TokenDecline::NoHealthyRail,
            TokenDecline::NotAddressable,
            TokenDecline::BodyExceedsStaging,
            TokenDecline::NotAnnounceable,
            TokenDecline::WriterNotInstalled,
        ];
        let mut labels: Vec<&str> = all.iter().map(|d| d.label()).collect();
        labels.sort_unstable();
        let count = labels.len();
        labels.dedup();
        assert_eq!(labels.len(), count, "duplicate decline label: {labels:?}");
        assert!(
            labels.iter().all(|l| !l.is_empty()),
            "an empty label is not a usable metric dimension"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::client_rail_index;

    /// A full cycle covers every rail exactly once — the property that makes a striped
    /// delivery spread evenly rather than favour a rail.
    #[test]
    fn a_cycle_covers_every_rail_once() {
        for count in 1..=8_usize {
            let mut seen = vec![0_usize; count];
            for ticket in 0..count as u64 {
                seen[client_rail_index(ticket, count)] += 1;
            }
            assert!(
                seen.iter().all(|hits| *hits == 1),
                "rails {count}: uneven cycle {seen:?}"
            );
        }
    }

    /// Many WRITEs over few rails stay balanced to within one, which is what "round-robin"
    /// has to mean for a checkpoint's thousands of chunks.
    #[test]
    fn a_long_run_stays_balanced() {
        const RAILS: usize = 4;
        const WRITES: u64 = 10_000;
        let mut seen = vec![0_u64; RAILS];
        for ticket in 0..WRITES {
            seen[client_rail_index(ticket, RAILS)] += 1;
        }
        let (low, high) = (
            *seen.iter().min().expect("RAILS > 0"),
            *seen.iter().max().expect("RAILS > 0"),
        );
        assert!(high - low <= 1, "unbalanced across {RAILS} rails: {seen:?}");
    }

    /// The cursor wrapping must not skew the spread: a `u64` counter wraps eventually, and a
    /// rail count that does not divide `u64::MAX + 1` would then jump. With a power-of-two
    /// count it cannot, and this pins the case that would surface years into a daemon's life.
    #[test]
    fn wrapping_the_cursor_keeps_the_cycle() {
        const RAILS: usize = 4;
        let before = client_rail_index(u64::MAX, RAILS);
        let after = client_rail_index(u64::MAX.wrapping_add(1), RAILS);
        assert_eq!(before, RAILS - 1);
        assert_eq!(after, 0);
    }
}

/// The bounded client-edge maps as this module wires them (R4).
///
/// These need no device — a gate holds a `bool` and a mutex, and an empty AH cache builds nothing
/// — but they live behind the `efa` feature because the module does, so `make dev-build-test-efa`
/// is what runs them. The cap/TTL/LRU *policy* is tested device-free in
/// [`crate::client_registry`]; what is asserted here is the wiring on top of it.
#[cfg(test)]
mod bound_tests {
    use super::{ClientAhCache, ClientReady};
    use crate::client_registry::{ClientEndpoint, EvictionReason};
    use std::sync::Arc;
    use std::time::Duration;

    /// A TTL nothing expires under, for the cases that are not about expiry.
    const NO_EXPIRY: Duration = Duration::from_secs(3600);

    fn endpoint(qpn: u32) -> ClientEndpoint {
        ClientEndpoint::new([0xfe; 16], qpn)
    }

    /// Proof is recorded and retracted per endpoint, and **retraction keeps the gate**: a chunk
    /// already waiting on this endpoint's first contact must keep waiting on the same mutex, or
    /// two chunks would run the retry ladder at once — each re-staging a 16 MiB body, which is the
    /// cost the gate exists to avoid.
    #[test]
    fn forgetting_a_proof_keeps_the_endpoints_gate() {
        let ready = ClientReady::with_limits(64, NO_EXPIRY);
        let ep = endpoint(49_153);
        let gate_before = ready.gate(ep);
        assert!(!ready.is_proven(ep), "a fresh endpoint is not proven");
        ready.mark_proven(ep);
        assert!(ready.is_proven(ep));

        ready.forget(ep);
        assert!(!ready.is_proven(ep), "the proof is retracted");
        assert!(
            Arc::ptr_eq(&gate_before, &ready.gate(ep)),
            "the gate must be the same mutex, not a fresh one"
        );
        let stats = ready.stats();
        assert_eq!(stats.entries, 1, "forget retracts, it does not evict");
        assert_eq!(stats.evicted(EvictionReason::Explicit), 0);
    }

    /// Two windows of one rank share a device GID under different queue-pair numbers, so they are
    /// two endpoints with independent proofs and independent gates. Collapsing them would let one
    /// window's success wave the other one past first contact.
    #[test]
    fn one_gid_under_two_queue_pairs_gates_independently() {
        let ready = ClientReady::with_limits(64, NO_EXPIRY);
        let (first, second) = (endpoint(1), endpoint(49_153));
        ready.mark_proven(first);
        assert!(ready.is_proven(first));
        assert!(!ready.is_proven(second), "a sibling window is not proven");
        assert!(!Arc::ptr_eq(&ready.gate(first), &ready.gate(second)));
        assert_eq!(ready.stats().entries, 2);
    }

    /// An endpoint idle past the TTL loses its proof, which is the half of the recycled-queue-pair
    /// defence that needs no evidence: a fresh process handed a dead one's `qpn` finds nothing
    /// proven and runs the ladder, instead of taking the un-retried fast path and failing the GET.
    #[test]
    fn an_idle_endpoint_loses_its_proof() {
        let ready = ClientReady::with_limits(64, Duration::from_millis(20));
        let ep = endpoint(49_153);
        ready.mark_proven(ep);
        assert!(ready.is_proven(ep));
        std::thread::sleep(Duration::from_millis(50));
        assert!(
            !ready.is_proven(ep),
            "a stale proof must not survive its idle TTL"
        );
        assert_eq!(ready.stats().evicted(EvictionReason::Ttl), 1);
    }

    /// The gate map is capped: a node that saw many short-lived clients holds at most the cap,
    /// and the evictions are filed as capacity ones so a dashboard can tell them from idle reaping.
    #[test]
    fn the_gate_map_is_capped() {
        const CAP: usize = 4;
        let ready = ClientReady::with_limits(CAP, NO_EXPIRY);
        for qpn in 0..32 {
            ready.mark_proven(endpoint(qpn));
        }
        let stats = ready.stats();
        assert_eq!(stats.entries, CAP, "the cap is never exceeded");
        assert_eq!(stats.evicted(EvictionReason::Cap), 32 - CAP as u64);
        assert!(ready.is_proven(endpoint(31)), "the newest survived");
        assert!(!ready.is_proven(endpoint(0)), "the oldest went");
    }

    /// An AH cache that has addressed nothing reports zeroes without touching a device — and
    /// reports them without taking its own lock, which is what lets a scrape run beside a
    /// delivery.
    #[test]
    fn an_empty_ah_cache_reports_zeroes() {
        let cache = ClientAhCache::default();
        assert!(cache.is_empty());
        assert_eq!(cache.len(), 0);
        let stats = cache.stats();
        assert_eq!(stats.entries, 0);
        for reason in EvictionReason::all() {
            assert_eq!(stats.evicted(reason), 0, "{}", reason.label());
        }
    }
}

#[cfg(test)]
mod classify_tests {
    use super::{is_unknown_peer, EFA_VENDOR_UNKNOWN_PEER};
    use ibverbs::{WcError, WcStatus};

    /// The classifier has to see through the context the completion pump attaches, or the
    /// retryable case reads as a fault — which is exactly how this race became a customer's
    /// 500 even after the retry existed (measured 2026-08-24: vendor 14 in the daemon's log,
    /// zero retries on the counter).
    #[test]
    fn an_unknown_peer_completion_is_recognised_through_context() {
        let wrapped = anyhow::Error::new(WcError {
            status: WcStatus::RemoteOperationError,
            vendor_err: EFA_VENDOR_UNKNOWN_PEER,
        })
        .context("work request 42");
        assert!(
            is_unknown_peer(&wrapped),
            "wrapped completion not recognised: {wrapped:#}"
        );
    }

    /// And every other status stays a fault, so a real fabric error is never retried into a
    /// body fallback.
    #[test]
    fn other_statuses_are_not_retryable() {
        for (status, vendor) in [
            (WcStatus::RemoteOperationError, 0),
            (WcStatus::RetryExceeded, EFA_VENDOR_UNKNOWN_PEER),
        ] {
            let wrapped = anyhow::Error::new(WcError {
                status,
                vendor_err: vendor,
            })
            .context("work request 42");
            assert!(!is_unknown_peer(&wrapped), "{status:?}/{vendor} retried");
        }
    }
}
