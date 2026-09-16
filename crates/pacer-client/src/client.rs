//! A live delivery client: one registered window, N rails, an announce pump on each.
//!
//! This is the object ADR-0030 implies but nothing in the repo had: the counterparty that
//! *owns* the memory a daemon writes into. Its whole API is four things a loader needs —
//! a token to send, a pointer to read, a way to verify, and an honest answer about whether
//! announces are still being reaped.
//!
//! ## Lifetime is the model's, not the request's
//!
//! ADR-0026 made the window's lifetime the request's, because the daemon pinned it per
//! request. Under a token the client registers once and the daemon registers nothing
//! (`proxy.rs`'s `registration_for` returns `None` for a token target), so a window may
//! back a whole model load — which is what makes zero-copy safetensors views possible
//! (planning/19 C5's first "consequence to settle", settled by construction here).
//!
//! ## What one client is NOT
//!
//! Not a connection: SRD is connectionless, nothing is dialled, and the daemon is reached
//! by the ordinary signed GET. Not thread-per-request: one client serves any number of
//! concurrent GETs into disjoint sub-windows, since a token names an offset. And not a
//! member of the ring — a delivery client is deliberately not required to be one
//! (ADR-0030 point 6), so this links no gRPC and joins nothing.

use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use ibverbs::ProtectionDomain;
use pacer_transport::announce;
use pacer_transport::token::TokenRail;
use tracing::info;

use crate::endpoint::{self, address_handle, Endpoint};
use crate::handles::HandleCache;
use crate::pump::{Pump, SharedHandles};
use crate::token;
use crate::window::{self, Pages, Window};

/// What kind of window to open, and where.
#[derive(Clone, Copy, Debug)]
pub struct WindowSpec {
    /// Window size in bytes. Rounded up to the backing page size (host) or to the GPU page
    /// size (device), and the realized length is what [`Client::len`] reports.
    pub bytes: usize,
    /// CUDA device ordinal for an HBM window, or `None` for host memory.
    ///
    /// The ordinal is this process's own — narrow the view with `CUDA_VISIBLE_DEVICES`, and
    /// on a CDI cluster never with `NVIDIA_VISIBLE_DEVICES=<uuid>` (which fails container
    /// creation).
    pub gpu: Option<i32>,
    /// The FIRST EFA rail to register on.
    ///
    /// **The caller's choice, and it matters:** every rail named here is a destination the
    /// daemon may WRITE to. For an HBM window they must be rails on the GPU's own PCIe switch
    /// — Track H measured 6.95× for getting that affinity wrong, landing *below* host memory.
    /// Resolving it automatically (from `cuDeviceGetPCIBusId` against the rail's sysfs parent)
    /// is the obvious follow-up and is deliberately not guessed here.
    pub rail: usize,
    /// How many CONSECUTIVE rails to register on, starting at [`Self::rail`].
    ///
    /// One is the old behaviour and still the right answer for a small window. More than one
    /// is what lifts a delivery off a single rail's ceiling: C5 measured ~10 GiB/s aggregate
    /// with 16 shards in flight to one client rail, which is that rail's ~11 GiB/s line rate
    /// (`bench/ladder/results/c5-safetensors-8b.md`), so no amount of further client
    /// concurrency helps and only more destinations can.
    ///
    /// Consecutive rather than an arbitrary set, because on the hardware this exists for that
    /// IS the useful set: a p5's rails are grouped four to a PCIe switch with one GPU each
    /// (planning/22), so `rail = 4 * gpu, rail_count = 4` names exactly the rails affine to
    /// that GPU. A non-contiguous set would buy expressiveness nothing needs yet.
    pub rail_count: usize,
    /// Backing page size for a host window; ignored for a device one.
    pub pages: Pages,
    /// Byte the window is filled with before it is published.
    ///
    /// A sentinel is what makes "nothing arrived" distinguishable from "the wrong bytes
    /// arrived". Note what it cannot prove: the fill is ONE byte value, so a delivered body
    /// containing it anywhere reads as unchanged there — a count of changed bytes is a weak
    /// witness and only a digest settles what landed (measured: a 32 MiB body of
    /// `bytes(range(256))` repeated hides exactly 1/256 of itself).
    pub sentinel: u8,
}

impl Default for WindowSpec {
    fn default() -> Self {
        Self {
            // A window has no sensible default size; the caller always sets one, and zero is
            // rejected by `Client::open` rather than silently becoming a page.
            bytes: 0,
            gpu: None,
            rail: 0,
            // One rail: the conservative default, and what every caller wanted before
            // striping existed.
            rail_count: 1,
            pages: Pages::Base,
            sentinel: 0xAB,
        }
    }
}

/// What every rail's announce pump has done, summed — see [`Client::pump`].
#[derive(Clone, Copy, Debug)]
pub struct PumpTotals {
    /// Announces decoded across all rails.
    pub announces: u64,
    /// Address handles built across all rails.
    pub handles: u64,
    /// Whether EVERY pump is still reaping. False means some rail can no longer answer.
    pub healthy: bool,
    /// How many rails are pumping, which is how many destinations the token names.
    pub rails: usize,
    /// Whether ANY rail's announce receive ring was ever fully drained in one pass — i.e.
    /// whether the delivery depth ceiling was reached.
    ///
    /// **The field to check when a delivery was declined `not_announceable`.** `true` says the
    /// announce burst outran the ring and names the fix
    /// (`PACER_CLIENT_ANNOUNCE_RECV_SLOTS`); `false` says the client kept up and the cause is
    /// on the writer's side. An OR across rails, unlike `healthy`'s AND: one saturated rail is
    /// enough to decline a delivery, so it is enough to report.
    pub ring_saturated: bool,
    /// Announce receives posted per rail. The bound `ring_saturated` compares against, carried
    /// so a caller can log the ceiling it hit without reading the environment itself.
    pub recv_slots: u64,
}

/// What one [`Client::prime`] call did, summed over this window's rails.
///
/// The point of reporting it rather than returning `()` is that "the pre-flight ran" and "the
/// pre-flight installed anything" are different facts, and only the second one removes the
/// race. A caller that sees `created == 0 && already_held == 0` was handed endpoints for zero
/// writers — a daemon with no RDMA plane, or a holder set the handshake has not negotiated —
/// and should expect the announce path to do the work.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrimedEndpoints {
    /// Rails in the message, times the client rails they were installed on.
    pub offered: usize,
    /// Handles built — `ibv_create_ah` calls actually paid.
    pub created: usize,
    /// Rails whose GID this client already held, on some rail.
    pub already_held: usize,
    /// Rails whose handle could not be built. Non-fatal: that writer's WRITEs will not be
    /// acknowledged, and the daemon reports it as a decline.
    pub failed: usize,
}

/// The address handles this client holds, summed over its rails — [`Client::handle_totals`].
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HandleTotals {
    /// Handles held right now.
    pub held: u64,
    /// Handles ever built, by either the pre-flight or an announce.
    pub created: u64,
    /// Announced rails whose handle was already held — the pre-flight got there first.
    pub prearmed: u64,
    /// Handles evicted at the bound. **Expected to be zero**; a non-zero value says the bound
    /// was smaller than the concurrent working set, which is a correctness alarm and not a
    /// tuning hint (see [`crate::handles`]).
    pub evicted: u64,
}

/// A registered window with a live announce pump per rail.
///
/// Field order is drop order: the pumps stop (and their threads are joined) **before** the
/// handle caches release their address handles and before the window's registrations and
/// memory go away, so no NIC is left able to write into pages this process has released, and
/// no `ibv_destroy_ah` races a pump that is still installing.
pub struct Client {
    pumps: Vec<Pump>,
    /// Per-rail handle caches, shared with the pumps: [`Client::prime`] installs into the same
    /// cache an arriving announce would, on the same rail. One per rail and not one per
    /// window, because an address handle is bound to a **protection domain** — a handle built
    /// on rail 0 cannot acknowledge a WRITE arriving at rail 1, so priming means building the
    /// same writer's handle once per rail this window is registered on.
    handles: Vec<SharedHandles>,
    /// The protection domains those handles are built on, cloned before the pumps took the
    /// endpoints. `ProtectionDomain` is `Arc`-backed inside the ibverbs crate, so this is a
    /// refcount and refers to exactly the domain the window is registered on — which it must,
    /// or a primed handle would live in a domain no WRITE arrives in.
    pds: Vec<ProtectionDomain>,
    window: Window,
    /// The rails this window is reachable on, in the client's own preference order — which is
    /// what a writer that picks ONE reads, and what a writer that stripes spreads over.
    rails: Vec<TokenRail>,
    /// `MemoryRegion::remote().addr`, captured once. Shared by every rail: host registrations
    /// of one mapping all report the same virtual address, and every dma-buf export here uses
    /// `iova == 0`. For a dma-buf window this is therefore the dma-buf offset, not the device
    /// pointer.
    base_addr: u64,
}

impl Client {
    /// Bring up `spec.rail_count` rails, register one window on all of them, and start
    /// answering announces on each.
    ///
    /// Ordered deliberately: the rails come up first (their protection domains are what the
    /// window registers on), the window second, and the pumps last — because a pump takes its
    /// rail's queues by value, and because publishing a token before receives are posted would
    /// invite a SEND that hangs its sender.
    ///
    /// # Errors
    ///
    /// A zero size or a zero rail count; no EFA device at some rail in the range, or one this
    /// pod may not open (a pod is allocated N devices by the plugin, so asking for more rails
    /// than were allocated fails here rather than at the first WRITE); no CUDA driver or no
    /// such device for a GPU window; the allocation, the dma-buf export or any of the
    /// registrations failing; or a receive ring not posting.
    pub fn open(spec: &WindowSpec) -> Result<Self> {
        window::check_size(spec.bytes)?;
        anyhow::ensure!(
            spec.rail_count >= 1,
            "a window must be registered on at least one rail"
        );
        // Every rail first, because the window registers on all of their protection domains at
        // once and a rail that cannot come up must fail before any memory is pinned.
        let endpoints: Vec<Endpoint> = (spec.rail..spec.rail + spec.rail_count)
            .map(endpoint::bring_up)
            .collect::<Result<_>>()?;
        let pds: Vec<&_> = endpoints.iter().map(|e| &e.pd).collect();
        let window = match spec.gpu {
            Some(ordinal) => Window::gpu(&pds, spec.bytes, ordinal, spec.sentinel)?,
            None => {
                let mut window = Window::host(&pds, spec.bytes, spec.pages)?;
                window.fill(spec.sentinel)?;
                window
            }
        };
        let rails: Vec<TokenRail> = endpoints
            .iter()
            .enumerate()
            .map(|(index, endpoint)| rail_of(endpoint, &window, index))
            .collect::<Result<_>>()?;
        // The pairing `rail_of` relies on, checked rather than assumed: rail i's token entry
        // must carry rail i's rkey, and a mismatch would be a protection error on hardware and
        // silence everywhere else.
        anyhow::ensure!(
            window.rail_count() == rails.len(),
            "registered {} rails but built {} token entries",
            window.rail_count(),
            rails.len()
        );
        let (base_addr, _) = window.descriptor(0);
        info!(
            bytes = window.len(),
            gpu = ?spec.gpu,
            rails = spec.rail_count,
            first_rail = spec.rail,
            devices = %endpoints.iter().map(|e| e.device.as_str()).collect::<Vec<_>>().join(","),
            pages = window.pages(),
            base_addr = format!("{base_addr:#x}"),
            "delivery window registered on this client's own protection domains"
        );
        // The protection domains and the handle caches are taken BEFORE the endpoints move
        // into the pumps: `prime` builds handles on these domains while the pumps hold the
        // queues, and both halves must be the same cache or the pre-flight would prime a set
        // the pump cannot see.
        let pds: Vec<ProtectionDomain> = endpoints.iter().map(|e| e.pd.clone()).collect();
        let handles: Vec<SharedHandles> = (0..endpoints.len())
            .map(|_| Arc::new(Mutex::new(HandleCache::new())))
            .collect();
        // Pumps last, and one per rail: each rail has its own queue pair, so each needs its
        // own posted receives — an announce arriving at a rail with none HANGS its writer
        // (`bench/ladder/results/c2-announce-gate.md`), which would strand exactly the rails
        // striping just added.
        let pumps = endpoints
            .into_iter()
            .zip(handles.iter().map(Arc::clone))
            .map(|(endpoint, cache)| Pump::spawn(endpoint, cache))
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            pumps,
            handles,
            pds,
            window,
            rails,
            base_addr,
        })
    }

    /// Build address handles for the writers `announce` names — **before** the GET that
    /// authorises anyone to write.
    ///
    /// This is the client half of ADR-0030's pre-flight exchange. `announce` is one
    /// [`pacer_transport::announce`] message, byte-identical to what that writer would SEND on
    /// first contact: the daemon's pre-flight route answers with one such message per node that
    /// holds the object's chunks (including its own), so the client installs from *the same*
    /// wire format either way and there is one decoder, one dedup rule and one golden vector
    /// covering both paths.
    ///
    /// Why it removes the race rather than shrinking it: `announce_to` proves the announce SEND
    /// reached the client's queue pair, not that the client decoded it and ran
    /// `ibv_create_ah` — that happens later, on the pump thread, and a WRITE can beat it and
    /// complete `UNKNOWN_PEER`. Calling this first makes the ordering *structural*: every
    /// handle exists before the request that sets any writer in motion, so no writer can be
    /// early. Nothing here talks to a device other than this client's own, and nothing waits on
    /// a peer — it is `ibv_create_ah` in a loop.
    ///
    /// Idempotent and cheap to repeat: a GID already held costs a hash lookup, so a loader may
    /// prime before every read without tracking what it has primed.
    ///
    /// # Errors
    ///
    /// Only a message that is not an announce (wrong version, bad length — see
    /// [`pacer_transport::announce::decode`]). That is the *caller's* bug rather than a remote
    /// writer's, which is why it is an error here and merely a dropped message in the pump. A
    /// handle that cannot be built is counted in [`PrimedEndpoints::failed`], never an error:
    /// one unaddressable writer must not cost the others their handles.
    pub fn prime(&self, announce: &[u8]) -> Result<PrimedEndpoints> {
        // Decoded once for every rail rather than once per rail: the rails differ only in which
        // protection domain the handle lands on.
        let rails = announce::decode(announce).context(
            "the pre-flight named endpoints this build cannot decode; delivery will fall back \
             to the announce path",
        )?;
        let mut total = PrimedEndpoints::default();
        for (pd, cache) in self.pds.iter().zip(self.handles.iter()) {
            let mut cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let installed = cache.install_rails(&rails, |gid| address_handle(pd, gid));
            total.offered += rails.len();
            total.created += installed.created;
            total.already_held += installed.already_held;
            total.failed += installed.failed;
        }
        Ok(total)
    }

    /// The address handles this window holds, summed over its rails.
    ///
    /// [`HandleTotals::prearmed`] against [`HandleTotals::created`] is how a delivery arm says
    /// whether the pre-flight did anything, and `evicted` is the alarm that its bound was too
    /// small — see [`crate::handles`].
    pub fn handle_totals(&self) -> HandleTotals {
        let (mut held, mut created, mut evicted) = (0u64, 0u64, 0u64);
        for cache in &self.handles {
            let cache = cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            held += cache.len() as u64;
            created += cache.created();
            evicted += cache.evicted();
        }
        HandleTotals {
            held,
            created,
            prearmed: self.pumps.iter().map(|p| p.state().prearmed()).sum(),
            evicted,
        }
    }

    /// The `x-pacer-target` value naming `[offset, offset + len)` of this window.
    ///
    /// `checksum` asks the daemon to digest what it sent. Leave it off for a rate arm: the
    /// pass is O(delivered bytes) on both sides, and this side's half is a full device→host
    /// copy of everything just delivered.
    ///
    /// # Errors
    ///
    /// A window the token grammar would reject (zero length), or a range past the end of
    /// what was registered — refused here rather than clamped, because a clamped delivery is
    /// a short read the client has no way to notice.
    pub fn token(&self, offset: u64, len: u64, checksum: bool) -> Result<String> {
        let end = offset
            .checked_add(len)
            .context("token window offset + len overflows")?;
        anyhow::ensure!(
            end <= self.window.len() as u64,
            "window {offset}+{len} does not fit the {} bytes registered",
            self.window.len(),
        );
        token::render(self.base_addr, offset, len, checksum, &self.rails)
    }

    /// Registered length in bytes, after page rounding.
    pub fn len(&self) -> usize {
        self.window.len()
    }

    /// Whether the window is empty — never in practice; present for clippy beside
    /// [`Client::len`].
    pub fn is_empty(&self) -> bool {
        self.window.is_empty()
    }

    /// The GPU virtual address of a device window, or `None` for a host one. What a torch
    /// tensor in this process is built over; never what goes in a token.
    pub fn device_ptr(&self) -> Option<u64> {
        self.window.device_ptr()
    }

    /// The host pointer of a host window, or `None` for a device one.
    pub fn host_ptr(&self) -> Option<*mut u8> {
        self.window.host_ptr()
    }

    /// Backing page size, realized rather than requested.
    pub fn pages(&self) -> usize {
        self.window.pages()
    }

    /// Observable pump state across every rail: announces decoded, handles built, and
    /// whether all of them are still reaping.
    ///
    /// Summed rather than per rail because that is the question a caller has — "is this window
    /// being announced to, and can it still answer?" — and because a striped delivery spreads
    /// announces over rails by a rule the client does not control. `healthy` is an AND: one
    /// dead pump means WRITEs to that rail stop landing, which is a broken window and not a
    /// slower one.
    pub fn pump(&self) -> PumpTotals {
        PumpTotals {
            announces: self.pumps.iter().map(|p| p.state().announces()).sum(),
            handles: self.pumps.iter().map(|p| p.state().handles()).sum(),
            healthy: self.pumps.iter().all(|p| p.state().healthy()),
            rails: self.pumps.len(),
            // ANY, not ALL: a delivery is declined if the rail it targeted ran out of
            // receives, so one saturated rail is the whole answer.
            ring_saturated: self.pumps.iter().any(|p| p.state().ring_saturated()),
            recv_slots: self.pumps.first().map_or(0, |p| p.state().recv_slots()),
        }
    }

    /// Copy delivered bytes out to host memory — for verification, and the only way to read
    /// a device window at all.
    ///
    /// # Errors
    ///
    /// A range past the end of the window, or the device→host copy failing.
    pub fn copy_out(&self, offset: usize, dst: &mut [u8]) -> Result<()> {
        self.window.copy_out(offset, dst)
    }

    /// Copy bytes into the window — the host→device leg a *control* arm pays after reading a
    /// response body, so that the two arms differ only in how the bytes crossed.
    ///
    /// # Errors
    ///
    /// A range past the end of the window, or the host→device copy failing.
    pub fn copy_in(&mut self, offset: usize, src: &[u8]) -> Result<()> {
        self.window.copy_in(offset, src)
    }

    /// Refill the whole window with `byte` — how a second arm starts from a known state
    /// without paying another registration.
    ///
    /// # Errors
    ///
    /// The device memset failing.
    pub fn fill(&mut self, byte: u8) -> Result<()> {
        self.window.fill(byte)
    }
}

/// This window's one rail entry: the endpoint's `gid`/`qpn` and the registration's `rkey`.
///
/// # Errors
///
/// The rail carrying no GID, which EFA requires for SRD addressing — and which would make a
/// token nobody can address.
fn rail_of(endpoint: &Endpoint, window: &Window, index: usize) -> Result<TokenRail> {
    let gid = endpoint
        .local
        .gid
        .context("this rail has no GID; EFA requires one to address SRD sends")?;
    // `index` is the endpoint's position in the rail list, which is also its registration's:
    // `Window::host`/`gpu` register in the order the protection domains were handed to them.
    // Pairing an rkey with the wrong rail would produce a token whose WRITEs fail with a
    // protection error on hardware and nowhere else.
    let (_, rkey) = window.descriptor(index);
    Ok(TokenRail {
        gid: <[u8; 16]>::from(gid),
        qpn: endpoint.local.qp_num,
        rkey,
    })
}
