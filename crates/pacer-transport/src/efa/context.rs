//! Device bring-up and the startup capability probe (ADR-0018 knob:
//! "capability probe at startup gates the whole plane"; ADR-0021: efadv/
//! ibverbs, not libfabric).
//!
//! Mirrors `spike/efa/src/rdma.rs`'s `probe`/`bring_up` — those were the
//! hardware-validated shape (planning/09, S1-S7); this is the same sequence
//! kept alive for the daemon's process lifetime instead of a one-shot CLI run.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Context as _, Result};
use ibverbs::{ProtectionDomain, QueuePair, QueuePairEndpoint, Srd};

use tracing::info;

use super::affinity::{self, RailPlacement};
use super::completion::CompletionPump;
use super::RailPlacementPolicy;
use crate::rdma_device;

/// Whether a rail can be raised on `dev` — i.e. whether it is an EFA device.
///
/// The rule itself lives in [`crate::rdma_device`], outside the `efa` feature gate, because
/// `pacer-client` has to apply the SAME one: a rail index that means a different device on
/// each side of ADR-0030 sends the daemon's WRITE to a rail the client never registered.
/// This is only the `ibverbs`-typed adapter for it.
fn is_efa_device(dev: &ibverbs::Device) -> bool {
    dev.name()
        .is_none_or(|name| rdma_device::is_efa(&name.to_string_lossy()))
}

/// Resolve every enumerated rail's placement, honouring `policy`: the NUMA-local
/// plan from sysfs, or all-unpinned when the operator turned placement off
/// (`PACER_RDMA_AFFINITY=0`). Logs where the rails landed either way — a result is
/// not interpretable without knowing which of the two ran (planning/19 D5).
fn plan_rail_placements(
    dev_names: &[Option<String>],
    policy: RailPlacementPolicy,
) -> Vec<RailPlacement> {
    let placements = match policy {
        RailPlacementPolicy::NumaLocal => affinity::plan_placements(dev_names),
        RailPlacementPolicy::Unpinned => {
            vec![RailPlacement::unpinned(); dev_names.len()]
        }
    };
    affinity::log_placements(dev_names, &placements);
    placements
}

/// GID table index to source from. Index 0 is the device's first GID; the
/// `efa_srd` reference and the spike both use 0, and the spike's cross-node
/// run confirmed it resolves correctly on real EFA nodes.
const GID_INDEX: u32 = 0;
/// SRD Q_Key, re-exported from the place that owns it.
///
/// The value is [`crate::token::SRD_QKEY`], because a *client* has to activate its own QP
/// with the same number or this node's WRITEs are dropped by its NIC — which makes it part
/// of ADR-0030's cross-language contract rather than a private bring-up detail. Kept as a
/// `pub(super)` alias so [`super::write::post_write`]'s callers (the holder's serve path)
/// still address a send with the same name this context activated its QP on.
pub(super) const QKEY: u32 = crate::token::SRD_QKEY;
/// EFA exposes exactly one port per device.
pub(super) const PORT_NUM: u8 = 1;
/// CQ depth PER QP on the rail: twice [`MAX_SEND_WR`], so a full send queue's
/// worth of completions can land before the completion pump
/// (efa/completion.rs) drains it and a burst still has headroom. Derived rather
/// than written as a literal because the two must move together — a CQ shallower
/// than the send queue it serves overflows, which is a lost completion (a fetch
/// that waits out `COMPLETION_TIMEOUT`), not backpressure. The rail's single CQ
/// is sized [`CQ_DEPTH`] × `qps_per_rail` so every QP keeps that headroom even
/// when all of them complete at once (see [`EfaContext::bring_up_on`]).
pub(super) const CQ_DEPTH: u32 = 2 * MAX_SEND_WR;
/// Default SRD QPs per rail when `PACER_EFA_QPS_PER_RAIL` is unset — one, i.e.
/// the pre-multi-QP behavior, so the knob is opt-in and an unchanged config
/// brings up exactly one QP per rail as before. **Leave it at one:** the premise
/// for raising it (2/4/8 to fill a rail one SRD QP cannot saturate) was measured
/// false on p5 — one QP reaches 97.7 Gbps and every `qps_per_rail` ∈ {1,2,4,8}
/// is byte-identical (planning/18 § RESULT 2, ADR-0024 § Status). The knob stays
/// a published contract, not a lever. See `qps` on [`EfaContext`].
pub const DEFAULT_QPS_PER_RAIL: usize = 1;
/// Send-queue depth for the SRD QP, set explicitly to the node-wide holder
/// arena depth ([`super::HOLDER_ARENA_RANGES`]) so the queue can hold one work
/// request per range this node could possibly have staged — the true ceiling on
/// in-flight WRITEs it can post. Only the HOLDER posts (the requester just reads
/// back what landed, `mod.rs`), and one WRITE is one WR, so a holder that landed
/// every one of its ranges on a SINGLE rail still fits: sizing against the
/// node-wide count rather than the per-rail share is deliberate, since the
/// per-rail split depends on how many rails came up.
///
/// Set explicitly because EFA does NOT honour a bare request: the provider
/// rounds `max_send_wr` up to a power of two and enforces its own minimum
/// (rdma-core `efa_qp_create` → `roundup_pow_of_two(max(max_send_wr,
/// min_sq_wr))`, `providers/efa/verbs.c`). Leaving it at the ibverbs default
/// of 1 silently yields a depth of 32 that nobody chose (planning/16 §2) — so
/// the daemon's real inflight bound was `min(pool 64, SQ 32, CQ 128) = 32`,
/// an undeclared cap below the buffer supply. Benign at 40 Gbps (§2), but
/// exactly the class of undeclared cap planning/19 track D exists to remove, so
/// it stays derived from the buffer depth rather than written out.
const MAX_SEND_WR: u32 = super::HOLDER_ARENA_RANGES as u32;

/// One node's persistent EFA endpoint: device context, protection domain,
/// `qps_per_rail` SRD queue pairs (SRD is connectionless — any QP serves every
/// peer, addressed per-send by address handle; multiple QPs exist only to
/// parallelize this node's OWN sends across a rail a single QP cannot fill),
/// and the completion queue/channel the `completion::CompletionPump` (internal)
/// drains. Long-lived for the daemon's process lifetime; built once at startup,
/// shared (`Arc`) by the requester and holder paths.
pub struct EfaContext {
    _ctx: ibverbs::Context,
    /// Protection domain the per-rail arenas register into ([`super::HostArena`])
    /// and address handles are created from ([`super::address::AhCache`]).
    pub(super) pd: ProtectionDomain,
    /// The SRD queue pairs this rail posts WRITEs through — `qps_per_rail` of
    /// them (config `PACER_EFA_QPS_PER_RAIL`), all on the SAME protection
    /// domain and completion queue so the one [`CompletionPump`] reaps every
    /// QP's completions. [`EfaContext::post`] round-robins outbound WRITEs across
    /// them.
    ///
    /// NOTE: the original justification for `> 1` — "a single SRD QP tops out well
    /// below a 100 Gbps rail's line rate, the provider serializes one QP's
    /// doorbell/DMA pipeline" — was **measured false** on p5.48xlarge (planning/18
    /// § RESULT 2): one QP reaches full 97.7 Gbps line rate and QP count makes
    /// byte-identical difference. Round-robin is kept (it is free and spreads send
    /// queues), but do not expect throughput from raising the count.
    ///
    /// SRD is connectionless and a one-sided WRITE places data by
    /// rkey+addr, so it is the SENDER's QP count that scales throughput — the
    /// peer still targets a single advertised endpoint (`local_endpoint`), so
    /// no extra endpoints cross the handshake and the wire protocol is
    /// unchanged. `&mut` for every post (`start_send`); callers serialize
    /// per-QP through [`EfaContext::post`].
    qps: Vec<tokio::sync::Mutex<QueuePair<Srd>>>,
    /// Round-robin cursor selecting which of `qps` the next [`EfaContext::post`]
    /// locks — an even spread of concurrent WRITEs across the rail's QPs is the
    /// whole point of having more than one.
    next_qp: AtomicU64,
    /// This node's own endpoint (the FIRST QP's — `qps[0]`), exchanged with
    /// peers over the gRPC handshake (ADR-0019) so they can build a return-path
    /// address handle (ADR-0018 finding 6, symmetric AHs). One endpoint per
    /// rail regardless of `qps_per_rail`: the WRITE's data placement is by
    /// rkey+addr, so peers need only ONE valid destination QP on this rail.
    pub local_endpoint: QueuePairEndpoint,
    /// The CQ→tokio bridge draining this context's completion queue (S7).
    /// Owned here so its background task's lifetime is tied to the context's.
    pub(super) pump: CompletionPump,
    /// Cumulative completion-pump deaths (A1). The pump's [`CompletionPump::spawn`]
    /// drain loop bumps this once if it ever exits on a terminal CQ/fd error.
    /// Lives here (not on [`super::EfaRdmaTransport`]) because the pump is
    /// spawned during [`Self::bring_up`], before the transport is built; the
    /// transport reads it through the shared `Arc` for its `cq_error_count`
    /// accessor. Monotonic; surfaced by the daemon at scrape.
    pub(super) cq_errors: Arc<AtomicU64>,
    /// RDMA-plane health (A1), initialized `true`. Set `false` by the pump's
    /// drain loop on its death so [`super::EfaRdmaTransport`]'s fetch/serve
    /// paths skip RDMA up front and fall back to gRPC at zero per-op cost,
    /// instead of each WRITE waiting out [`COMPLETION_TIMEOUT`] for a
    /// completion no longer being reaped. Shared `Arc` for the same
    /// pump-predates-transport reason as `cq_errors`.
    pub(super) rdma_healthy: Arc<AtomicBool>,
    /// Where this rail belongs (planning/19 D5 step 0): the CPU its completion
    /// reaper is pinned to, and therefore the NUMA node its registered memory
    /// should be first-touched on. Resolved once at bring-up and kept here
    /// because the ARENAS are built later, by the transport, and must land on the
    /// same node as the NIC that will DMA them — see [`Self::placement`].
    placement: RailPlacement,
}

impl EfaContext {
    /// Bring up the device: open it, select the GID, create a completion
    /// channel + CQ on it, create and activate the SRD queue pair. Fails
    /// clearly (rather than panicking) so the caller can fall back to
    /// gRPC-only for this node (ADR-0018's capability-probe gate).
    ///
    /// The completion reaper gets its own pinned OS thread (see
    /// `completion.rs`), so this takes no runtime handle: there is no
    /// shared runtime left for it to compete on.
    ///
    /// # Errors
    ///
    /// Any step of device open, GID selection, or SRD QP creation/activation
    /// failing — most commonly no EFA device attached, or the device plugin
    /// not having mounted `/dev/infiniband` into this pod.
    pub fn bring_up() -> Result<Self> {
        let ctx = open_device()?;
        // The probe/single-rail convenience path brings up exactly one QP; the
        // multi-QP fan-out is a [`Self::bring_up_rails`] concern (the daemon's
        // production path). One QP is all a capability probe needs. A lone probe
        // rail has no NUMA peer to contend with, so it stays unplaced.
        Self::bring_up_on(ctx, 0, RailPlacement::unpinned(), DEFAULT_QPS_PER_RAIL)
    }

    /// A5 multi-rail: bring up one [`EfaContext`] per EFA device (rail), in
    /// device order, capped at `max_rails` (`0` = all), each with
    /// `qps_per_rail` SRD QPs (`0` is clamped to
    /// [`DEFAULT_QPS_PER_RAIL`] — always at least one QP). A device that will not
    /// open, or fails bring-up before any rail is up, is SKIPPED: enumeration
    /// shows every interface on the host while the device cgroup admits only the
    /// units this pod was allocated, so an unusable device 0 says nothing about
    /// device 1 (`open_device` carries the measurement). Only *every* device failing is the
    /// capability-probe failure (same contract as [`Self::bring_up`]); a later
    /// rail failing once others are up stops enumeration with a warning and the
    /// transport runs on the rails that did come up — a p5 with one sick rail
    /// should degrade to 31 rails, not to gRPC.
    ///
    /// `policy` decides whether each rail's reaper is pinned to a CPU on its own
    /// NIC's NUMA node — and, through the placement it keeps, whether the transport
    /// registers that rail's arenas from the same node (planning/19 D5 step 0).
    /// [`super::RailPlacementPolicy::Unpinned`] reproduces the pre-D5 behaviour,
    /// which is what makes an A/B possible on one image.
    ///
    /// # Errors
    ///
    /// No device present, or rail 0 failing any bring-up step — the caller
    /// falls back to gRPC-only exactly as with [`Self::bring_up`].
    pub fn bring_up_rails(
        max_rails: usize,
        qps_per_rail: usize,
        policy: super::RailPlacementPolicy,
    ) -> Result<Vec<Self>> {
        let devices = ibverbs::devices().context(
            "listing RDMA devices (is the efa kernel module loaded and /dev/infiniband mounted?)",
        )?;
        let cap = if max_rails == 0 {
            usize::MAX
        } else {
            max_rails
        };
        // A knob of 0 (or an operator typo) must never yield a QP-less rail:
        // clamp to the default so every rail posts through at least one QP.
        let qps_per_rail = qps_per_rail.max(DEFAULT_QPS_PER_RAIL);
        // Placement is planned for ALL enumerated devices up front, because the
        // per-node round-robin has to see the whole set to spread rails across
        // distinct cores; a rail that later fails bring-up just leaves its planned
        // CPU unused.
        // FILTERED BEFORE `take(cap)`, which is the whole point: a p6-b200.48xlarge
        // exposes TWO `mlx5_core` devices alongside its eight EFA ones, and
        // `ibv_get_device_list` hands back `ibp115s0f0`/`ibp116s0f0` FIRST. Taking the
        // first `cap` devices therefore spent the whole rail budget on hardware that cannot
        // create an SRD QP: zero working rails at `max_rails=1`, six of eight at 8
        // (measured on the node, 2026-09-06). A p5.48xlarge never showed it because every
        // device there is EFA. (This used to say libibverbs "sorts by name"; it does not —
        // the measured order was ascending PCI bus, and a name sort would have put
        // `rdmap113s0` ahead of `rdmap79s0`. See `crate::rdma_device`: do not predict an
        // index from the order, read the device name back.)
        //
        // Only pods that OPEN every device see this: one holding a device-plugin
        // allocation is handed just its EFA uverbs nodes, while a privileged daemon
        // (ADR-0030 point 9, the supported production mechanism) sees all of them.
        let eligible: Vec<_> = devices
            .iter()
            .filter(|dev| is_efa_device(dev))
            .take(cap)
            .collect();
        let dev_names: Vec<Option<String>> = eligible
            .iter()
            .map(|dev| dev.name().map(|n| n.to_string_lossy().into_owned()))
            .collect();
        let placements = plan_rail_placements(&dev_names, policy);
        let mut rails = Vec::new();
        // The first failure seen while no rail is up yet. Kept because a node where NO
        // device works has to say which kind of broken it is: a denied open is an
        // allocation/device-cgroup problem (fix the pod spec), a failed bring-up is a
        // fabric one (fix the node) — and "no RDMA device found" fits neither.
        let mut first_failure: Option<anyhow::Error> = None;
        for (i, dev) in eligible.iter().enumerate() {
            let ctx = match dev.open().context("opening the RDMA device context") {
                Ok(ctx) => ctx,
                // A device that will not OPEN is skipped rather than fatal, because
                // enumeration and access answer to different authorities: every pod sees
                // every interface through /sys/class/infiniband*, while the kubelet's
                // device cgroup admits only the ones a device plugin ALLOCATED to this
                // pod. So a denied open usually means "not mine", and which units are
                // mine is the plugin's choice — a one-unit allocation on a p5 was granted
                // `uverbs2`, so demanding device 0 fell back to gRPC on a node with 32
                // healthy rails (2026-08-24). A sick rail is skipped on the same terms;
                // `rails.is_empty()` below is what turns "none of them" into the
                // capability-probe failure.
                Err(e) => {
                    tracing::warn!(device = i, error = %format!("{e:#}"), "cannot open this RDMA device — not allocated to this pod, or unhealthy; skipping it");
                    first_failure = first_failure.or(Some(e));
                    continue;
                }
            };
            let placement = placements
                .get(i)
                .copied()
                .unwrap_or_else(RailPlacement::unpinned);
            match Self::bring_up_on(ctx, i, placement, qps_per_rail) {
                Ok(rail) => rails.push(rail),
                // Nothing is up yet, so this is a candidate for the capability-probe
                // failure — but only if every remaining device fails too, so keep going.
                Err(e) if rails.is_empty() => {
                    tracing::warn!(device = i, error = %format!("{e:#}"), "EFA device failed bring-up with no rail up yet; trying the next one");
                    first_failure = first_failure.or(Some(e));
                    continue;
                }
                Err(e) => {
                    tracing::warn!(rail = i, error = %e, "EFA rail failed bring-up; running on the rails below it");
                    break;
                }
            }
        }
        if rails.is_empty() {
            return Err(first_failure.unwrap_or_else(|| {
                anyhow!("no RDMA device found — no EFA attached, or the device plugin did not mount uverbs into this pod")
            }));
        }
        info!(rails = rails.len(), qps_per_rail, "EFA rails up");
        Ok(rails)
    }

    /// The shared tail of [`Self::bring_up`]/[`Self::bring_up_rails`]: bring
    /// up one already-opened device with `qps_per_rail` SRD QPs sharing one PD
    /// and one CQ (so the single [`CompletionPump`] reaps them all).
    ///
    /// `qps_per_rail` must be ≥ 1 (the callers guarantee it). `rail`/`placement`
    /// name and place this rail's completion reaper thread, and `placement` is
    /// retained on the context so the transport can register this rail's arenas
    /// from the same NUMA node ([`Self::placement`]).
    fn bring_up_on(
        ctx: ibverbs::Context,
        rail: usize,
        placement: RailPlacement,
        qps_per_rail: usize,
    ) -> Result<Self> {
        select_gid(&ctx)?;
        let channel = ctx
            .create_comp_channel()
            .context("creating completion channel")?;
        // One CQ for the whole rail, sized so every QP keeps CQ_DEPTH headroom:
        // all `qps_per_rail` QPs post into and complete on this one queue, and
        // the pump dispatches by (globally unique) wr_id regardless of which QP
        // produced the completion.
        let cq_depth = CQ_DEPTH.saturating_mul(qps_per_rail as u32);
        let cq = ctx
            .create_cq(cq_depth)
            .set_comp_channel(&channel)
            .build()
            .context("creating CQ on the completion channel")?;
        let pd = ctx.alloc_pd().context("allocating protection domain")?;
        // All QPs share this one PD (rkeys/AHs are PD-scoped, so a WRITE from
        // any of them uses the same AH cache and buffer pools) and this one CQ.
        let (qps, local_endpoint) = build_srd_qps(&pd, &cq, qps_per_rail)?;
        // Shared health handles (A1): created here so the pump's drain loop can
        // signal its own death into them; the transport reads the same `Arc`s.
        // `rdma_healthy` starts `true` — a freshly built context has a live
        // pump until proven otherwise.
        let cq_errors = Arc::new(AtomicU64::new(0));
        let rdma_healthy = Arc::new(AtomicBool::new(true));
        // Ownership of `cq`/`channel` moves into the pump's dedicated reaper
        // thread; `EfaContext` keeps only the `CompletionPump` handle, which is
        // what `write.rs`'s waiters register against. The QPs, already built on
        // this CQ, do not hold a Rust borrow of it (they reference it by handle),
        // so moving it into the pump is sound.
        let pump = CompletionPump::spawn(
            cq,
            channel,
            rail,
            placement,
            Arc::clone(&cq_errors),
            Arc::clone(&rdma_healthy),
        );
        info!(
            qp_num = local_endpoint.qp_num,
            qps_per_rail,
            has_gid = local_endpoint.gid.is_some(),
            numa = ?placement.numa_node,
            "EFA context up"
        );
        Ok(Self {
            _ctx: ctx,
            pd,
            qps,
            next_qp: AtomicU64::new(0),
            local_endpoint,
            pump,
            cq_errors,
            rdma_healthy,
            placement,
        })
    }

    /// The startup capability probe (ADR-0018/ADR-0021): can this node create
    /// and activate an SRD queue pair with one-sided send-ops at all? A
    /// passing probe on an EFA device *is* the "hardware, not emulated"
    /// result (ADR-0021: efadv has no software-emulation fallback on EFA
    /// v2+, unlike libfabric) — there is no separate emulation check to run.
    ///
    /// Cheap and side-effect-free on success (the probe QP is dropped
    /// immediately); on failure the node advertises gRPC-only to peers. Needs no
    /// ambient tokio runtime since D5 gave reapers their own threads — the probe's
    /// transient reaper brings up and tears down its own current-thread runtime.
    pub fn probe() -> bool {
        match Self::bring_up() {
            Ok(_ctx) => true,
            Err(e) => {
                // The whole chain (`{e:#}`), not just the outer context: this one line is
                // the only account of why a node has no fabric, and "opening the RDMA
                // device context" without its errno cost a paid p5 an hour of bisection
                // (2026-08-24 — the errno was EPERM, i.e. the device cgroup).
                info!(error = %format!("{e:#}"), "EFA capability probe failed; this node speaks gRPC only");
                false
            }
        }
    }

    /// Run `f` against ONE of the rail's queue pairs with exclusive access,
    /// chosen round-robin so concurrent fetch/serve tasks spread their WRITEs
    /// across every QP instead of contending on one send queue — that spread
    /// is what lets more than one QP fill a rail a single QP cannot. Only the
    /// chosen QP is locked; the others stay free for other posters.
    /// `start_send`'s batch borrows the QP mutably only for the (synchronous,
    /// non-blocking) duration of building and submitting the work request — the
    /// lock is never held across an `.await`.
    pub(super) async fn post<R>(&self, f: impl FnOnce(&mut QueuePair<Srd>) -> R) -> R {
        // Relaxed: the cursor only needs to hand out distinct-ish indices for
        // spread, not to order anything against other memory.
        let idx = self.next_qp.fetch_add(1, Ordering::Relaxed) as usize % self.qps.len();
        let mut qp = self.qps[idx].lock().await;
        f(&mut qp)
    }

    /// Where this rail's memory and reaper belong (planning/19 D5 step 0). The
    /// transport pins a thread here while registering this rail's arenas, so the
    /// NIC that will DMA them reads and writes node-local memory.
    pub(super) fn placement(&self) -> RailPlacement {
        self.placement
    }

    /// This context's protection domain, for callers building address
    /// handles or registering buffers directly ([`super::address::AhCache`],
    /// [`super::HostArena`]).
    pub(super) fn pd(&self) -> &ProtectionDomain {
        &self.pd
    }

    /// The completion pump [`super::write::post_write`] registers wr_id
    /// waiters against.
    pub(super) fn pump(&self) -> &CompletionPump {
        &self.pump
    }
}

/// How long a completion wait may block before the transport gives up and
/// reports the peer unreachable (matches the gRPC control plane's own
/// timeout order of magnitude, ADR-0019 `rpc_timeout`, scaled up for a data
/// transfer rather than a control message).
pub(super) const COMPLETION_TIMEOUT: Duration = Duration::from_secs(5);

/// Open the first RDMA device this pod can actually open, or explain the failure
/// in EFA terms (mirrors the spike's `open_device`, which this daemon-lifetime
/// path replaces).
///
/// The first *enumerated* device is not necessarily one this pod may use, and
/// this function gates the whole RDMA plane through [`EfaContext::probe`], so
/// getting it wrong costs the node its fabric: enumeration walks
/// `/sys/class/infiniband*`, which shows every interface on the host, while
/// opening goes through `/dev/infiniband/uverbsN`, which the kubelet's device
/// cgroup admits only for units a device plugin allocated to this pod. Measured
/// 2026-08-24 on a p5: a one-unit allocation was granted `uverbs2` and every
/// other device returned EPERM, so taking device 0 reported "gRPC only" on a
/// node with 32 healthy rails.
fn open_device() -> Result<ibverbs::Context> {
    let devices = ibverbs::devices().context(
        "listing RDMA devices (is the efa kernel module loaded and /dev/infiniband mounted?)",
    )?;
    let mut first_failure: Option<anyhow::Error> = None;
    // Same filter as `bring_up_rails`, for the same reason: this returns the FIRST device
    // that opens, and on a p6-b200 the first two enumerated are `mlx5_core`.
    for dev in devices.iter().filter(|dev| is_efa_device(dev)) {
        match dev.open().context("opening the RDMA device context") {
            Ok(ctx) => return Ok(ctx),
            Err(e) => first_failure = first_failure.or(Some(e)),
        }
    }
    Err(first_failure.unwrap_or_else(|| {
        anyhow!("no RDMA device found — no EFA attached, or the device plugin did not mount uverbs into this pod")
    }))
}

/// Build and activate `qps_per_rail` SRD QPs on the shared protection domain
/// `pd` and completion queue `cq`, returning them alongside the FIRST QP's
/// endpoint (the one this rail advertises — see [`EfaContext::local_endpoint`]).
/// All QPs share `pd`/`cq`; the caller then moves `cq` into the completion pump.
///
/// # Errors
///
/// Any per-QP `create_srd_qp`/`build`/`endpoint`/`activate` step failing (the
/// first-rail contract in [`EfaContext::bring_up_rails`] turns a rail-0 failure
/// into gRPC-only fallback).
fn build_srd_qps(
    pd: &ProtectionDomain,
    cq: &ibverbs::CompletionQueue,
    qps_per_rail: usize,
) -> Result<(Vec<tokio::sync::Mutex<QueuePair<Srd>>>, QueuePairEndpoint)> {
    let mut qps = Vec::with_capacity(qps_per_rail);
    let mut local_endpoint = None;
    for _ in 0..qps_per_rail {
        // set_gid_index takes &mut self; the builder must be a mutable
        // binding, not a temporary in a `?`-chain (spike compile finding 1).
        let mut builder = pd
            .create_srd_qp(cq, cq, PORT_NUM)
            .context("create_srd_qp")?;
        builder.set_gid_index(GID_INDEX);
        // Explicit, not inherited: EFA silently rounds this (see MAX_SEND_WR).
        builder.set_max_send_wr(MAX_SEND_WR);
        let prepared = builder
            .build()
            .context("building SRD QP (efadv_create_qp_ex)")?;
        // Advertise only the FIRST QP's endpoint — peers need one valid
        // destination QP on this rail; a one-sided WRITE lands by rkey+addr
        // regardless of which of our QPs (or the peer's) it targets.
        if local_endpoint.is_none() {
            local_endpoint = Some(prepared.endpoint().context("reading local SRD endpoint")?);
        }
        let qp = prepared.activate(QKEY).context("activating SRD QP")?;
        qps.push(tokio::sync::Mutex::new(qp));
    }
    let local_endpoint =
        local_endpoint.expect("qps_per_rail >= 1 guarantees at least one endpoint");
    Ok((qps, local_endpoint))
}

/// Confirm [`GID_INDEX`] exists on [`PORT_NUM`] before anything tries to use
/// it — EFA requires a GID to address SRD sends at all.
fn select_gid(ctx: &ibverbs::Context) -> Result<()> {
    let gids = ctx.gid_table().context("querying the GID table")?;
    gids.iter()
        .find(|e| e.port_num == PORT_NUM && e.gid_index == GID_INDEX)
        .map(|_| ())
        .ok_or_else(|| anyhow!("no GID at index {GID_INDEX} on port {PORT_NUM}"))
}
