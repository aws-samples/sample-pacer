//! Gate 3.3 — **what a full staging budget costs, and what it must never cost.**
//!
//! Two arms, and the pair is the gate. `StagingArea::try_stage_at` refuses rather than
//! waits, and the two refusals it can give are not the same kind of thing:
//!
//! * [`ONE_WINDOW_STAGING`] makes every owner refuse *after its first* window —
//!   `BudgetExhausted`, the transient refusal reject-fast is built around. On the one
//!   save shape we have real numbers for this is the normal path, not the sad one, so
//!   the refusals must be **visible** and the coordinator must absorb the windows.
//! * [`SUB_WINDOW_STAGING`] is below one window, so every offer is
//!   `OversizedForBudget` — a misconfiguration no waiting clears. This one must degrade
//!   all the way to today's plain PUT, which is the floor the ADR claims the design
//!   never falls below.
//!
//! In both, the write succeeds and every byte is readable through every node. A full
//! budget costs *warmth*, never the write.

use super::{
    body, fleet, phase, refusals, scatter_report, windows, OBJECT_LEN, ONE_WINDOW_STAGING,
    SPREAD_OWNERS, SUB_WINDOW_STAGING, WINDOWS,
};

/// Gate 3.3, the transient half: a budget with room for one window per node, so every
/// owner refuses after its first. The write must still succeed — the coordinator
/// absorbs each refused window — and the refusals must be *visible*.
#[tokio::test]
async fn reject_fast_absorbs_the_windows_owners_refuse() {
    let h = fleet(ONE_WINDOW_STAGING).await;
    let coordinator = 0;
    let key = h.key_reaching(
        "scatter/reject-fast",
        OBJECT_LEN,
        &h.nodes[coordinator].name,
        SPREAD_OWNERS,
    );
    let payload = body(9, OBJECT_LEN);

    h.put_expecting_success(
        coordinator,
        &key,
        &payload,
        "a write no owner can fully stage must still succeed",
    )
    .await;

    let node = &h.nodes[coordinator];
    assert_eq!(node.metrics.scatter.scattered.get(), 1);
    assert_eq!(
        windows(node, "owner") + windows(node, "local"),
        WINDOWS,
        "every window must be accounted for"
    );
    assert!(
        windows(node, "local") > 0,
        "the coordinator must have absorbed the refused windows itself"
    );
    let refused: u64 = h
        .nodes
        .iter()
        .map(|n| refusals(n, "budget_exhausted"))
        .sum();
    assert!(
        refused > 0,
        "reject-fast must show up as a counted refusal, not as silence:\n{}",
        scatter_report(node)
    );
    // A refused offer is timed as its OWN phase, and that separation is what makes
    // `owner_refused` a wire measurement: the owner gates on `try_stage` before it
    // uploads anything, so a refusal is the window's bytes over the wire with no S3 in
    // it at all. Folded into `owner_rpc` it would instead pull that mean down and make
    // the hop look cheaper than it is.
    let refused_offers = phase(node, pacer_daemon::metrics::SCATTER_PHASE_OWNER_REFUSED);
    assert!(
        refused_offers.0 > 0,
        "a refused offer must be charged to owner_refused, not to owner_rpc:\n{}",
        scatter_report(node)
    );
    assert_eq!(
        phase(node, pacer_daemon::metrics::SCATTER_PHASE_OWNER_FAILED).0,
        0,
        "nothing here is a transport failure; a refusal is a successful RPC with a \
         negative answer, and conflating the two would put a timeout in a wire mean"
    );
    h.assert_readable_everywhere(&key, &payload, "a reject-fast write")
        .await;
}

/// Gate 3.3, the non-transient half: a budget below one window is a misconfiguration
/// no waiting clears, so it must degrade all the way to "today's PUT" — every window
/// uploaded by the coordinator, nothing cached anywhere, and the write still correct.
/// This is the floor the ADR claims the design never falls below.
#[tokio::test]
async fn a_budget_below_one_window_degrades_to_a_plain_upload() {
    let h = fleet(SUB_WINDOW_STAGING).await;
    let coordinator = 0;
    let key = h.key_reaching(
        "scatter/no-budget",
        OBJECT_LEN,
        &h.nodes[coordinator].name,
        SPREAD_OWNERS,
    );
    let payload = body(10, OBJECT_LEN);

    h.put_expecting_success(
        coordinator,
        &key,
        &payload,
        "a write nobody can stage must still succeed",
    )
    .await;

    let node = &h.nodes[coordinator];
    assert_eq!(
        windows(node, "owner"),
        0,
        "no owner can stage a window larger than its whole budget"
    );
    assert_eq!(windows(node, "local"), WINDOWS);
    assert_eq!(node.metrics.scatter.owners_engaged.get(), 0);
    assert_eq!(
        node.metrics.scatter.uncached_windows.get(),
        WINDOWS,
        "the coordinator's own budget is just as short, so nothing is cached"
    );
    let refused: u64 = h
        .nodes
        .iter()
        .map(|n| refusals(n, "oversized_for_budget"))
        .sum();
    assert!(
        refused > 0,
        "a misconfigured budget must be reported as its own reason:\n{}",
        scatter_report(node)
    );
    h.assert_readable_everywhere(&key, &payload, "a write nobody could stage")
        .await;
}
