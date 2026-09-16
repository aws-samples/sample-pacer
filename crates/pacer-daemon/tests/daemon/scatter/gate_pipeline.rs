//! **What a window's slot costs, and what holding one bounds.**
//!
//! Not a gate — the instrument. Both arms exist because a paid benchmark arm read a
//! number off this machinery and could not say what it meant.
//!
//! * [`a_windows_slot_hold_is_attributable_across_both_sides_of_the_wire`] pins the
//!   population match and the nesting that make the fleet-wide subtraction
//!   `owner_rpc − (served_stage + served_upload)` a *wire* measurement rather than an
//!   artefact of two different sample sets.
//! * [`a_full_pipeline_stops_the_body_read`] pins the one claim `windows_in_flight`
//!   makes about **memory** rather than about concurrency, which the code asserted in a
//!   comment and did not provide.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use pacer_daemon::coordinate::ScatterTarget;
use s3s::dto;

use super::{
    body, fleet, phase, scatter_report, CHUNK_SIZE, OBJECT_LEN, ROOMY_STAGING, SPREAD_OWNERS,
    WINDOWS, WINDOWS_AT_A_FULL_PIPELINE, WINDOWS_IN_FLIGHT,
};
use crate::common::{poll_until, BUCKET};

/// **A window's slot-hold is attributable, and its two halves are measured on
/// DIFFERENT nodes** — the instrument
/// `bench/ladder/results/w1-write-ceilings.md` § *What this arm could not settle*
/// item 3 asks for.
///
/// That arm named the coordinator's window semaphore as the write path's wall
/// (`windows_in_flight_peak` pinned at `_limit` at client concurrency 1, and still
/// pinned after the ceiling was raised 4×) and could not say what occupies a slot. The
/// `StoreChunk` RPC covers the owner's own `UploadPart`, so a coordinator-side timer
/// cannot split network hop from peer-side S3 — and those two readings argue for
/// opposite decisions about ADR-0032 Phase 5's RDMA WRITE.
///
/// What this pins is the property the ladder's arithmetic rests on, which no
/// single-process test of the histogram could reach:
///
/// * **the population match.** Every window an owner took is timed exactly once as
///   `owner_rpc` on the coordinator and once as `served_upload` on the owner, so
///   `owner_rpc − (served_stage + served_upload)` summed over the fleet is the hop and
///   not an artefact of two different sample sets.
/// * **the nesting.** The owner's clocks run inside the interval the coordinator's
///   covers, so the subtraction cannot come out negative. A negative wire term would be
///   nonsense that a reader might well round to zero and quietly believe.
/// * **the coordinator does not double-count itself.** A window whose home *is* the
///   coordinator goes through `upload_here`, never a `StoreChunk` to itself, so it is
///   `local_upload` and contributes no `served_*` — otherwise the fleet sums would
///   over-count and understate the hop.
#[tokio::test]
async fn a_windows_slot_hold_is_attributable_across_both_sides_of_the_wire() {
    let h = fleet(ROOMY_STAGING).await;
    let coordinator = 0;
    let key = h.key_reaching(
        "scatter/phases",
        OBJECT_LEN,
        &h.nodes[coordinator].name,
        SPREAD_OWNERS,
    );

    h.put_expecting_success(
        coordinator,
        &key,
        &body(11, OBJECT_LEN),
        "the arm needs a successful scatter to time",
    )
    .await;

    let coord = &h.nodes[coordinator];
    let (waits, wait_secs) = phase(coord, pacer_daemon::metrics::SCATTER_PHASE_PERMIT_WAIT);
    assert_eq!(
        waits,
        WINDOWS,
        "every window takes a slot, so every window is charged a queueing sample — and \
         with {WINDOWS} windows over {WINDOWS_IN_FLIGHT} slots some of them waited:\n{}",
        scatter_report(coord)
    );
    assert!(
        wait_secs > 0.0,
        "a pipeline narrower than the object must show a positive wait somewhere"
    );
    assert_eq!(
        phase(coord, pacer_daemon::metrics::SCATTER_PHASE_COMPLETE).0,
        1,
        "Complete is per PUT, not per window — its _count is the PUT count"
    );

    let (rpcs, rpc_secs) = phase(coord, pacer_daemon::metrics::SCATTER_PHASE_OWNER_RPC);
    let locals = phase(coord, pacer_daemon::metrics::SCATTER_PHASE_LOCAL_UPLOAD).0;
    assert_eq!(
        rpcs + locals,
        WINDOWS,
        "every window is charged to exactly one of the two upload paths"
    );
    assert!(
        rpcs > 0,
        "this key was placed to reach {SPREAD_OWNERS} owners"
    );
    assert_eq!(
        phase(coord, pacer_daemon::metrics::SCATTER_PHASE_SERVED_UPLOAD).0,
        0,
        "a window the coordinator homes goes through upload_here, never a StoreChunk to \
         itself, so the coordinator must record no served_upload for its own object"
    );

    // The owners' side of the same windows, summed over every node but the coordinator.
    let mut served = 0;
    let mut served_secs = 0.0;
    for node in &h.nodes[1..] {
        let (stage_n, stage_s) = phase(node, pacer_daemon::metrics::SCATTER_PHASE_SERVED_STAGE);
        let (upload_n, upload_s) = phase(node, pacer_daemon::metrics::SCATTER_PHASE_SERVED_UPLOAD);
        assert_eq!(
            stage_n, upload_n,
            "an owner that staged a window uploads it, so the two counts move together"
        );
        served += upload_n;
        served_secs += stage_s + upload_s;
    }
    assert_eq!(
        served, rpcs,
        "each of the {rpcs} remote windows must be timed once on the coordinator and \
         once on its owner; the fleet-wide subtraction is invalid otherwise"
    );
    assert!(
        served_secs <= rpc_secs,
        "the owner's clocks run INSIDE the RPC the coordinator times, so \
         {served_secs}s of served work cannot exceed {rpc_secs}s of RPC — a negative \
         wire term is nonsense a reader would round to zero and believe"
    );
}

/// Frame `index` of `payload`, exactly one window long — so a frame *is* a window
/// and the count of frames pulled is the count of windows the coordinator has taken
/// in from the client.
fn frame_of(payload: &Bytes, index: u64) -> Bytes {
    let size = usize::try_from(CHUNK_SIZE).unwrap();
    let start = usize::try_from(index).unwrap() * size;
    payload.slice(start..start + size)
}

/// A body that hands over one window per poll, counts what it handed over, and
/// **panics rather than hand over more than `limit`**.
///
/// The panic is the assertion. A coordinator that reads ahead of its uploads does so
/// by pulling a frame it is not entitled to, from inside the future the test is
/// polling — so the failure lands as "the body read is not bounded" at the moment it
/// happens, instead of being inferred afterwards from a high-water mark.
fn bounded_body(
    payload: Bytes,
    frames: u64,
    limit: usize,
    pulled: Arc<AtomicUsize>,
) -> dto::StreamingBlob {
    let stream = futures::stream::unfold(0u64, move |index| {
        let pulled = Arc::clone(&pulled);
        let payload = payload.clone();
        async move {
            if index == frames {
                return None;
            }
            let taken = pulled.fetch_add(1, Ordering::SeqCst) + 1;
            assert!(
                taken <= limit,
                "the coordinator took in window {taken} with every upload held and \
                 only {WINDOWS_IN_FLIGHT} slots in flight: the body read is not \
                 backpressured, so this coordinator's memory is the client's object \
                 and not windows_in_flight × chunk_size"
            );
            let frame = frame_of(&payload, index);
            Some((Ok::<Bytes, std::io::Error>(frame), index + 1))
        }
    });
    dto::StreamingBlob::wrap(stream)
}

/// `windows_in_flight` must bound **buffered bytes**, not just concurrent uploads:
/// with every `UploadPart` held, the coordinator has to stop reading the client's
/// body rather than take the rest of the object into memory.
///
/// The test the original ordering would have failed. The permit used to be acquired
/// *inside* the spawned upload, so the body reader never awaited one: every window
/// past the limit sat in a parked task already holding a full `chunk_size` buffer,
/// and a coordinator's footprint was the object rather than
/// `windows_in_flight × chunk_size`. On hardware that read as three of five daemons
/// `OOMKilled` at a limit the chart had sized from the smaller number
/// (`bench/ladder/results/w1-write-scatter.md`, 2026-08-27).
///
/// Driven straight at the coordinator rather than through `put_object`, because the
/// S3 front end reframes a client's body and this test's whole instrument is *when*
/// a frame is handed over. Both bounds are asserted, and the pair is the point: the
/// upper one alone would also pass if the coordinator had stalled at the first
/// window and never filled its pipeline at all.
#[tokio::test]
async fn a_full_pipeline_stops_the_body_read() {
    let h = fleet(ROOMY_STAGING).await;
    let coordinator = 0;
    let key = h.key_reaching(
        "scatter/backpressure",
        OBJECT_LEN,
        &h.nodes[coordinator].name,
        SPREAD_OWNERS,
    );
    let object_key = format!("{BUCKET}/{key}");
    let pulled = Arc::new(AtomicUsize::new(0));
    let stream = bounded_body(
        body(11, OBJECT_LEN),
        WINDOWS,
        WINDOWS_AT_A_FULL_PIPELINE,
        Arc::clone(&pulled),
    );
    h.faults.hold_all_uploads();

    let scatter = h.nodes[coordinator].coordinator.scatter(
        ScatterTarget {
            bucket: BUCKET,
            key: &key,
            object_key: &object_key,
            object_len: OBJECT_LEN,
            content_type: None,
            expected_crc32: None,
        },
        stream,
    );
    // Runs concurrently with the scatter and is what ends the test: the scatter
    // itself never can, since no held upload ever returns a slot.
    let pipeline_fills = poll_until("every window slot is held by a parked upload", || async {
        h.faults.uploads_parked() == WINDOWS_IN_FLIGHT
    });
    tokio::select! {
        outcome = scatter => panic!(
            "no window can finish while every upload is held, so the scatter must not \
             have finished either: {outcome:?}"
        ),
        () = pipeline_fills => {}
    }

    assert_eq!(
        pulled.load(Ordering::SeqCst),
        WINDOWS_AT_A_FULL_PIPELINE,
        "a full pipeline holds exactly its windows in flight plus the one window \
         waiting for a slot"
    );
    // And the same fact as the daemon PUBLISHES it, since that is what a paid arm reads.
    // The peak is what makes `windowsInFlight` checkable on hardware, so a peak that did
    // not follow a pipeline this test just proved was full would make the series useless
    // exactly where it is needed.
    let slots = h.nodes[coordinator].coordinator.window_slots();
    assert_eq!(slots.limit(), WINDOWS_IN_FLIGHT);
    assert_eq!(
        slots.in_flight(),
        WINDOWS_IN_FLIGHT,
        "every slot is held: each parked upload holds one"
    );
    assert_eq!(
        slots.peak(),
        WINDOWS_IN_FLIGHT,
        "the published peak must reach the limit on a pipeline this test just held full, \
         and must never exceed it"
    );
}
