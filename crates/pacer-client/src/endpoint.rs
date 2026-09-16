//! One brought-up SRD rail: the device, its protection domain, and the queues a client
//! needs to be *written into*.
//!
//! Mirrors `spike/efa/src/loopback.rs`'s `bring_up` — the sequence hardware validated for
//! exactly this role — with two differences that follow from being a library rather than a
//! probe:
//!
//! * **The send queue is vestigial.** A delivery client never posts a send: the daemon
//!   WRITEs, and the client's NIC answers in hardware. The queue exists because a QP has
//!   one.
//! * **The receive queue is not.** A SEND arriving at a QP with no receive posted does not
//!   fail — it **hangs the sender** (`bench/ladder/results/c2-announce-gate.md`), so the
//!   announce that installs the writer would stall the very delivery it precedes. Keeping
//!   the ring posted is [`crate::pump`]'s whole job; this module only sizes it.
//!
//!   That size is a **delivery depth ceiling**, and it used to be an accidental one — see
//!   [`DEFAULT_RECV_SLOTS`] for the measurement that found it and the arithmetic the default
//!   now comes from. EFA fixes `max_recv_wr` at QP creation, so it cannot be grown later
//!   under pressure: getting it right here is the only opportunity.
//!
//! One rail, chosen by index, because the daemon writes to the token's **first** rail only
//! (`client_write.rs::address_client`) — registering on 32 would pay 32 registrations for
//! one destination. Striping across a client's rails is a daemon-side change (ADR-0030
//! point 4, planning/19 C3); when it lands, this is what gets called N times.

use anyhow::{anyhow, Context, Result};
use ibverbs::{
    AddressHandle, AddressHandleAttribute, CompletionChannel, CompletionQueue, ProtectionDomain,
    QueuePair, QueuePairEndpoint, Srd,
};
use pacer_transport::token::SRD_QKEY;
use tracing::{info, warn};

/// GID table index to source from. Index 0 is the device's first GID; the `efa_srd`
/// reference, the daemon (`efa::address`) and the spike all use it, and a client that
/// routed from a different index would be addressable at an address the daemon never
/// resolves.
const GID_INDEX: u32 = 0;
/// EFA exposes exactly one port per device.
const PORT_NUM: u8 = 1;
/// IP hop limit for the GRH — same-AZ peers only, matching the daemon's own choice.
const HOP_LIMIT: u8 = 64;
/// GRH traffic class: no differentiated-services marking on an intra-cluster fabric.
const TRAFFIC_CLASS: u8 = 0;
/// Receives kept posted for announces, per endpoint — **the delivery depth ceiling**, so the
/// number is derived rather than picked.
///
/// A SEND arriving at an endpoint with no receive posted does not fail, it *hangs the
/// sender*: the daemon's announce then blows its 250 ms deadline, the WRITE is declined
/// `not_announceable`, and the delivery degrades to a body. The ring is a **burst tolerance**
/// — the pump refills each slot the moment its completion is reaped — so what has to fit is
/// the largest number of announces that can be in flight to ONE endpoint at ONE time.
///
/// That burst is set by the writer's rail count, not by anything the client chooses.
/// `Announcer::ensure_announced` dedupes per client endpoint but records only *after* the
/// completion, and the daemon's first-contact gate (`ClientReady`) serialises per **rail** —
/// so a 32-rail p5 daemon can have up to 32 announces outstanding to one endpoint before any
/// of them is recorded as done.
///
/// **8 was below that, and it was measured failing**: 2 GPUs (8 endpoints) at delivery depth
/// 30 was DECLINED — "no completion for the announce within 250 ms; the client most likely has
/// no receive posted", 229 retries — while the same depth spread over 32 endpoints was fine
/// (planning/19, "Depth is bounded by ANNOUNCE RECEIVE SLOTS"). The ceiling was
/// `slots × endpoints`, which made the fabric width silently set the delivery depth.
///
/// 64 is 2× a p5's 32 rails: one full first-contact burst from the largest shape this repo
/// runs, plus room for a second holder announcing concurrently — which is the case a striped
/// read produces and the reason this was ever more than 1. The cost is
/// `64 × ANNOUNCE_MAX_BYTES` = 90 KB of registered memory per rail, against a window measured
/// in GiB.
const DEFAULT_RECV_SLOTS: usize = 64;

/// Override for [`DEFAULT_RECV_SLOTS`], for a writer shape this crate has not seen. Raising it
/// costs 1410 B of pinned memory per slot per rail and nothing else.
const RECV_SLOTS_ENV: &str = "PACER_CLIENT_ANNOUNCE_RECV_SLOTS";

/// Floor for the override. One posted receive is the minimum that is not simply broken — a
/// ring of zero hangs the first writer that ever announces, which is a hang and not a
/// degradation.
const MIN_RECV_SLOTS: usize = 1;

/// Ceiling for the override. Well above any plausible rail count × holder count, and low
/// enough that a mistyped value cannot try to pin gigabytes or exceed what the device will
/// accept as `max_recv_wr` (an over-large request fails QP creation, i.e. fails the whole
/// window open).
const MAX_RECV_SLOTS: usize = 4096;

/// Send-queue depth. One, because this endpoint never posts a send; EFA rounds any request
/// up to its own minimum anyway (`efa_qp_create`), so asking for less would not save
/// anything.
const SQ_DEPTH: u32 = 1;

/// Completion-queue depth per posted receive: the ring plus its refills in flight, with
/// headroom. A CQ shallower than the queues it serves overflows, and a lost completion is an
/// announce the client never answers.
const CQ_DEPTH_PER_SLOT: u32 = 4;

/// EFA devices on a p5.48xlarge — the widest *writer* this repo runs, and therefore the
/// largest first-contact announce burst one client endpoint can face.
const P5_RAILS: usize = 32;

/// Compile-time invariants on the sizing above. Build failures rather than tests, because each
/// is a property of the constants alone: a test could only report at run time what the compiler
/// can refuse outright, and the failure mode being prevented (a ring too small for the fabric)
/// is one that only shows up on hardware.
const _: () = {
    // The default must CLEAR a full 32-rail burst with headroom, not merely meet it — the
    // headroom is for a second holder announcing concurrently, which a striped read produces.
    assert!(
        DEFAULT_RECV_SLOTS >= P5_RAILS * 2,
        "the announce ring must leave headroom above one full 32-rail first-contact burst"
    );
    // The measured-bad value must never be what ships again.
    assert!(
        DEFAULT_RECV_SLOTS > 8,
        "8 slots is the ceiling measured DECLINING a 2-GPU delivery at depth 30"
    );
    // A CQ shallower than the queue it serves overflows, and a lost completion is a slot never
    // re-posted — a ring that shrinks permanently, i.e. the ceiling getting worse over a run.
    assert!(CQ_DEPTH_PER_SLOT >= 2, "the CQ must outsize the recv ring");
    // The bounds have to admit the default, or an unset environment would be clamped.
    assert!(MIN_RECV_SLOTS <= DEFAULT_RECV_SLOTS && DEFAULT_RECV_SLOTS <= MAX_RECV_SLOTS);
    // A ring of zero does not degrade a delivery, it HANGS the first writer that announces.
    assert!(MIN_RECV_SLOTS >= 1, "zero receives hangs the writer");
};

/// How many announce receives to keep posted, resolved once per process.
///
/// Cached because it is read on every rail's bring-up and the pump indexes its inbox by slot:
/// two different answers within one process would mean a QP built for one ring size and an
/// inbox carved for another. Caching removes that possibility rather than documenting it —
/// and the value is additionally carried on [`Endpoint::recv_slots`], so the pump uses
/// exactly what its own QP was built with.
///
/// An unparseable or out-of-range value warns and falls back to the default: a loader that
/// mistyped an env var should still load, slowly, rather than fail to start.
pub fn recv_slots() -> usize {
    static RESOLVED: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *RESOLVED.get_or_init(|| parse_recv_slots(std::env::var(RECV_SLOTS_ENV).ok().as_deref()))
}

/// The decision [`recv_slots`] caches, as a pure function of the environment's value.
///
/// Separate so it is testable: `recv_slots` resolves once per process, so a test that drove it
/// through the environment could only ever assert one of these cases, and which one would
/// depend on test ordering.
fn parse_recv_slots(raw: Option<&str>) -> usize {
    let Some(raw) = raw else {
        return DEFAULT_RECV_SLOTS;
    };
    match raw.trim().parse::<usize>() {
        Ok(n) if (MIN_RECV_SLOTS..=MAX_RECV_SLOTS).contains(&n) => {
            info!(
                slots = n,
                default = DEFAULT_RECV_SLOTS,
                "announce receive ring overridden by {RECV_SLOTS_ENV}"
            );
            n
        }
        // Includes an out-of-range number as well as a non-number: both mean the operator's
        // intent cannot be honoured, and a loader that mistyped an env var should still load
        // — slowly if need be — rather than fail to start.
        _ => {
            warn!(
                value = raw,
                default = DEFAULT_RECV_SLOTS,
                min = MIN_RECV_SLOTS,
                max = MAX_RECV_SLOTS,
                "{RECV_SLOTS_ENV} is not a slot count in range; using the default"
            );
            DEFAULT_RECV_SLOTS
        }
    }
}

/// A client rail: everything hardware-side that one window needs.
///
/// The protection domain is [`Clone`] (it is `Arc`-backed) and every field is `Send`, which
/// is what lets the queues move onto the pump thread while the window's registration stays
/// with the caller — with no leaking and no self-referential struct, because a
/// `MemoryRegion` holds its own reference to the PD.
pub struct Endpoint {
    /// Where the window is registered and the announce address handles are created.
    pub pd: ProtectionDomain,
    /// Drained by the pump; every announce arrives here.
    pub cq: CompletionQueue,
    /// The CQ's event channel, so waiting for an announce costs no CPU.
    pub channel: CompletionChannel,
    /// Holds the receive ring. `mut` on every `post_recv`, hence owned rather than shared.
    pub qp: QueuePair<Srd>,
    /// This rail's own endpoint — the `gid`/`qpn` half of the token.
    pub local: QueuePairEndpoint,
    /// The device this rail opened, for the log line that explains a wrong-rail result.
    pub device: String,
    /// Announce receives this QP was built to hold ([`recv_slots`]).
    ///
    /// Carried on the endpoint rather than read from the environment again by the pump: the
    /// QP's `max_recv_wr` and the inbox's slot arithmetic have to agree exactly, and passing
    /// the number that was actually used makes disagreeing impossible instead of unlikely.
    pub recv_slots: usize,
}

/// Whether a rail can be raised on `dev` — i.e. whether it is an EFA device.
///
/// The rule lives in [`pacer_transport::rdma_device`] and the daemon's `efa::context`
/// applies the identical one, which is the point: `rail` below is an index into the
/// FILTERED list, so "rail 3" names the same device on both sides of ADR-0030. It did not
/// used to. See that module for the p6-b200 topology that made the raw index wrong.
pub(crate) fn carries_rail(dev: &ibverbs::Device) -> bool {
    dev.name()
        .is_none_or(|name| pacer_transport::rdma_device::is_efa(&name.to_string_lossy()))
}

/// Bring up rail `rail`: open the device, create the channel/CQ/PD, build and activate an
/// SRD QP with a receive queue.
///
/// `rail` indexes the node's **EFA** devices in libibverbs order, skipping any RDMA device
/// on another driver ([`carries_rail`]). On a p6-b200 that skip is load-bearing: two
/// `mlx5_core` interfaces sort ahead of the eight EFA rails, so an unfiltered index 0 named
/// a device that cannot create an SRD QP and every rail-to-GPU affinity map was off by two.
///
/// # Errors
///
/// No EFA device at that index (fewer than the caller assumed — or a pod that did not
/// request `vpc.amazonaws.com/efa`, so it may enumerate devices it cannot open), the device
/// refusing to open (`EPERM` means the kubelet's device cgroup did not admit this unit), no
/// GID at [`GID_INDEX`], or any queue-creation step failing.
pub fn bring_up(rail: usize) -> Result<Endpoint> {
    let list = ibverbs::devices().context(
        "listing RDMA devices (is the efa kernel module loaded and /dev/infiniband mounted?)",
    )?;
    let dev = list
        .iter()
        .filter(|dev| carries_rail(dev))
        .nth(rail)
        .ok_or_else(|| {
            anyhow!(
                "no EFA device at rail {rail} ({} EFA of {} RDMA devices enumerated) — does \
                 this pod request `vpc.amazonaws.com/efa`?",
                list.iter().filter(|dev| carries_rail(dev)).count(),
                list.iter().count(),
            )
        })?;
    let device = dev.name().map_or_else(
        || format!("rail{rail}"),
        |n| n.to_string_lossy().into_owned(),
    );
    let ctx = dev.open().with_context(|| {
        format!(
            "opening EFA device {device} — EPERM here means the device cgroup did not admit \
             this unit (enumeration shows every interface on the host; only an ALLOCATED \
             unit can be opened)"
        )
    })?;
    let gids = ctx.gid_table().context("querying the GID table")?;
    gids.iter()
        .find(|e| e.port_num == PORT_NUM && e.gid_index == GID_INDEX)
        .ok_or_else(|| anyhow!("no GID at index {GID_INDEX} on port {PORT_NUM}"))?;
    let slots = recv_slots();
    let cq_depth = u32::try_from(slots)
        .unwrap_or(u32::MAX)
        .saturating_mul(CQ_DEPTH_PER_SLOT);
    let channel = ctx
        .create_comp_channel()
        .context("creating completion channel")?;
    let cq = ctx
        .create_cq(cq_depth)
        .set_comp_channel(&channel)
        .build()
        .context("creating CQ on the completion channel")?;
    let pd = ctx.alloc_pd().context("allocating protection domain")?;
    // `set_gid_index` takes `&mut self`, so the builder must be a mutable binding rather
    // than a temporary in a `?`-chain.
    let mut builder = pd
        .create_srd_qp(&cq, &cq, PORT_NUM)
        .context("create_srd_qp")?;
    builder.set_gid_index(GID_INDEX);
    builder.set_max_send_wr(SQ_DEPTH);
    // Explicit: the ring is what keeps an announcing writer from hanging, and EFA fixes a
    // QP's queues at creation — a receive cannot be added later, which is precisely why the
    // size has to be right up front rather than grown under pressure.
    builder.set_max_recv_wr(u32::try_from(slots).unwrap_or(u32::MAX));
    let prepared = builder.build().context("building the SRD QP")?;
    let local = prepared
        .endpoint()
        .context("reading the local SRD endpoint")?;
    let qp = prepared
        .activate(SRD_QKEY)
        .context("activating the SRD QP")?;
    info!(
        rail,
        %device,
        qp_num = local.qp_num,
        recv_slots = slots,
        "client rail up"
    );
    Ok(Endpoint {
        pd,
        cq,
        channel,
        qp,
        local,
        device,
        recv_slots: slots,
    })
}

/// Build a GID-routed address handle for an announced writer, so this client's NIC can
/// acknowledge its WRITEs.
///
/// The load-bearing call of the whole client half: with no handle for the writer, a WRITE
/// completes `UNKNOWN_PEER` on the sender and **nothing lands** — measured cross-node
/// (planning/09 finding 10) and, decisively, between two endpoints on one device, where
/// being the same device exempts nothing.
///
/// Takes only the GID: an EFA address handle routes to a *device*, and the destination QPN
/// is named per-send by whoever posts. So one handle per announced rail is what the client
/// owes, and the `qpn` in the announce is the writer's own — never used here.
///
/// # Errors
///
/// `ibv_create_ah` failing.
pub fn address_handle(pd: &ProtectionDomain, gid: [u8; 16]) -> Result<AddressHandle> {
    let mut attr = AddressHandleAttribute::new(PORT_NUM);
    attr.set_grh(
        ibverbs::Gid::from(gid),
        GID_INDEX as u8,
        HOP_LIMIT,
        TRAFFIC_CLASS,
    );
    pd.create_address_handle(&attr)
        .context("creating an address handle for an announced writer")
}

#[cfg(test)]
mod tests {
    use super::{parse_recv_slots, DEFAULT_RECV_SLOTS, MAX_RECV_SLOTS, MIN_RECV_SLOTS};
    /// An absent variable is the shipping default, and a valid one is honoured exactly.
    #[test]
    fn an_override_in_range_is_honoured() {
        assert_eq!(parse_recv_slots(None), DEFAULT_RECV_SLOTS);
        assert_eq!(parse_recv_slots(Some("128")), 128);
        // Whitespace is what a YAML env value picks up, so trimming is required rather than
        // tidy: `"64\n"` failing to parse would silently halve a tuned deployment.
        assert_eq!(parse_recv_slots(Some(" 128 \n")), 128);
        assert_eq!(parse_recv_slots(Some("1")), MIN_RECV_SLOTS);
        assert_eq!(parse_recv_slots(Some("4096")), MAX_RECV_SLOTS);
    }

    /// Every unusable value falls back to the default rather than to zero or to a panic. Zero
    /// is the one that matters: a ring of zero does not degrade a delivery, it HANGS the first
    /// writer that announces, which is why it is clamped out rather than accepted.
    #[test]
    fn an_unusable_override_falls_back_rather_than_breaking() {
        for raw in [
            "0",
            "-1",
            "wat",
            "",
            "  ",
            "4097",
            "99999999999999999999",
            "8.5",
        ] {
            assert_eq!(
                parse_recv_slots(Some(raw)),
                DEFAULT_RECV_SLOTS,
                "{raw:?} should have fallen back to the default"
            );
        }
    }
}
