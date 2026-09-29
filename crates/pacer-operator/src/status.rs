//! A ring's observed state (ADR-0047 § 5): three conditions, the DaemonSet's rollout
//! counts, and the ring's membership as the daemons themselves see it.
//!
//! Everything here is a pure function of objects already read, so the whole of what a
//! status says is testable without an API server.
//!
//! - **`Rendered`** — the chart accepted `spec.values`. `False/ValuesRefused` carries the
//!   chart's own message; the ring's author has to act.
//! - **`Applied`** — every rendered object was written and every stale one pruned. When a
//!   render fails nothing is applied, and the objects from the last good render are left
//!   running: a typo in a value must not take a warm cache down.
//! - **`Ready`** — the DaemonSet has converged on the current template, every daemon is
//!   ready, and every ready daemon is a ring member. Its `reason` is the one-word answer
//!   `kubectl get cacherings` prints.

use std::collections::BTreeSet;

use k8s_openapi::api::apps::v1::DaemonSet;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use k8s_openapi::apimachinery::pkg::apis::meta::v1::{Condition, Time};
use k8s_openapi::jiff::Timestamp;

use crate::crd::{CacheRingStatus, InventoryRef};

/// Condition types, as they appear in `status.conditions`.
pub const RENDERED: &str = "Rendered";
/// See [`RENDERED`].
pub const APPLIED: &str = "Applied";
/// See [`RENDERED`].
pub const READY: &str = "Ready";

/// How far the last reconcile got.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// The chart or the guard refused the render; the message says why.
    Refused(String),
    /// Rendering could not run at all.
    RenderFailed(String),
    /// Rendered, but writing or pruning an object failed.
    ApplyFailed(String),
    /// Rendered and applied; `inventory` is what was written.
    Applied {
        /// Every object applied.
        inventory: Vec<InventoryRef>,
    },
}

/// What the reconciler read about the ring's workloads after applying.
#[derive(Debug, Default)]
pub struct Observed<'a> {
    /// The ring's DaemonSet, if it exists yet.
    pub daemonset: Option<&'a DaemonSet>,
    /// EndpointSlices of the ring's headless peer Service; `None` when the chart renders
    /// no peer Service at all.
    pub peer_slices: Option<&'a [EndpointSlice]>,
}

/// Build the next status from the previous one, this reconcile's outcome and what was
/// observed. `generation` is the ring's `metadata.generation`.
#[must_use]
pub fn build(
    previous: Option<&CacheRingStatus>,
    generation: Option<i64>,
    outcome: &Outcome,
    seen: &Observed<'_>,
) -> CacheRingStatus {
    let (desired, updated, ready, rollout_current) = rollout(seen.daemonset);
    let members = seen.peer_slices.map(member_nodes);
    let inventory = match outcome {
        Outcome::Applied { inventory } => inventory.clone(),
        // Nothing was (fully) applied, so what exists is still what the last good apply
        // wrote; keep it, or the next successful apply could not prune it.
        _ => previous.map(|p| p.inventory.clone()).unwrap_or_default(),
    };
    let gen = generation.unwrap_or_default();
    let prior = previous
        .map(|p| p.conditions.as_slice())
        .unwrap_or_default();
    let conditions = vec![
        condition(prior, RENDERED, rendered(outcome), gen),
        condition(prior, APPLIED, applied(outcome), gen),
        condition(
            prior,
            READY,
            readiness(
                outcome,
                desired,
                updated,
                ready,
                rollout_current,
                members.as_ref(),
            ),
            gen,
        ),
    ];
    CacheRingStatus {
        observed_generation: generation,
        conditions,
        desired,
        updated,
        ready,
        members: members
            .as_ref()
            .map(|m| i32::try_from(m.len()).unwrap_or(i32::MAX)),
        member_nodes: members.map(|m| m.into_iter().collect()).unwrap_or_default(),
        inventory,
    }
}

/// Whether `status` says the ring is Ready — the one state the reconciler may leave for a
/// whole resync before looking again.
#[must_use]
pub fn is_ready(status: &CacheRingStatus) -> bool {
    status
        .conditions
        .iter()
        .any(|c| c.type_ == READY && c.status == "True")
}

/// `(status, reason, message)` of one condition.
type Verdict = (bool, &'static str, String);

fn rendered(outcome: &Outcome) -> Verdict {
    match outcome {
        Outcome::Refused(m) => (false, "ValuesRefused", m.clone()),
        Outcome::RenderFailed(m) => (false, "RenderFailed", m.clone()),
        _ => (true, "Rendered", String::new()),
    }
}

fn applied(outcome: &Outcome) -> Verdict {
    match outcome {
        Outcome::Applied { inventory } => (true, "Applied", format!("{} objects", inventory.len())),
        Outcome::ApplyFailed(m) => (false, "ApplyFailed", m.clone()),
        _ => (
            false,
            "NotRendered",
            "objects from the last successful render are left in place".into(),
        ),
    }
}

fn readiness(
    outcome: &Outcome,
    desired: i32,
    updated: i32,
    ready: i32,
    rollout_current: bool,
    members: Option<&BTreeSet<String>>,
) -> Verdict {
    if !matches!(outcome, Outcome::Applied { .. }) {
        return (
            false,
            "NotApplied",
            "see the Rendered and Applied conditions".into(),
        );
    }
    if desired == 0 {
        return (
            false,
            "NoNodes",
            "no node matches the ring's nodeSelector and tolerations".into(),
        );
    }
    if !rollout_current || updated < desired {
        return (
            false,
            "RollingOut",
            format!("{updated}/{desired} daemons on the current template"),
        );
    }
    if ready < desired {
        return (
            false,
            "DaemonsNotReady",
            format!("{ready}/{desired} daemons ready"),
        );
    }
    if let Some(m) = members {
        if m.len() < usize::try_from(ready).unwrap_or_default() {
            return (
                false,
                "MembersMissing",
                format!("{}/{ready} ready daemons are ring members", m.len()),
            );
        }
    }
    (
        true,
        "Converged",
        format!("{ready}/{desired} daemons ready"),
    )
}

/// `(desired, updated, ready, controller has seen the current spec)`.
fn rollout(ds: Option<&DaemonSet>) -> (i32, i32, i32, bool) {
    let Some(ds) = ds else {
        return (0, 0, 0, false);
    };
    let Some(s) = ds.status.as_ref() else {
        return (0, 0, 0, false);
    };
    let current =
        s.observed_generation.is_some() && s.observed_generation >= ds.metadata.generation;
    (
        s.desired_number_scheduled,
        s.updated_number_scheduled.unwrap_or_default(),
        s.number_ready,
        current,
    )
}

/// Node names of ready endpoints across all slices — the same rule the daemons' own
/// membership watch applies (`pacer_ring::membership`): an endpoint counts only when its
/// `ready` condition is explicitly true and it names a node.
fn member_nodes(slices: &[EndpointSlice]) -> BTreeSet<String> {
    slices
        .iter()
        .flat_map(|s| &s.endpoints)
        .filter(|ep| {
            ep.conditions
                .as_ref()
                .and_then(|c| c.ready)
                .unwrap_or(false)
        })
        .filter_map(|ep| ep.node_name.clone())
        .collect()
}

/// A condition, keeping `lastTransitionTime` from `prior` when its status did not change.
fn condition(
    prior: &[Condition],
    type_: &str,
    (ok, reason, message): Verdict,
    generation: i64,
) -> Condition {
    let status = if ok { "True" } else { "False" };
    let last_transition_time = prior
        .iter()
        .find(|c| c.type_ == type_ && c.status == status)
        .map_or_else(
            || Time(Timestamp::now()),
            |c| c.last_transition_time.clone(),
        );
    Condition {
        type_: type_.to_owned(),
        status: status.to_owned(),
        reason: reason.to_owned(),
        message,
        observed_generation: Some(generation),
        last_transition_time,
    }
}

#[cfg(test)]
mod tests {
    use k8s_openapi::api::apps::v1::DaemonSetStatus;
    use k8s_openapi::api::discovery::v1::{Endpoint, EndpointConditions};
    use kube::api::ObjectMeta;

    use super::*;

    fn ds(generation: i64, observed: i64, desired: i32, updated: i32, ready: i32) -> DaemonSet {
        DaemonSet {
            metadata: ObjectMeta {
                generation: Some(generation),
                ..Default::default()
            },
            status: Some(DaemonSetStatus {
                observed_generation: Some(observed),
                desired_number_scheduled: desired,
                updated_number_scheduled: Some(updated),
                number_ready: ready,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    fn slice(nodes: &[(&str, Option<bool>)]) -> EndpointSlice {
        EndpointSlice {
            endpoints: nodes
                .iter()
                .map(|(n, ready)| Endpoint {
                    node_name: Some((*n).to_owned()),
                    conditions: Some(EndpointConditions {
                        ready: *ready,
                        ..Default::default()
                    }),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn applied() -> Outcome {
        Outcome::Applied { inventory: vec![] }
    }

    fn ready_reason(s: &CacheRingStatus) -> &str {
        &s.conditions
            .iter()
            .find(|c| c.type_ == READY)
            .unwrap()
            .reason
    }

    #[test]
    fn converged_ring_is_ready() {
        let d = ds(2, 2, 2, 2, 2);
        let slices = [slice(&[("n1", Some(true)), ("n2", Some(true))])];
        let seen = Observed {
            daemonset: Some(&d),
            peer_slices: Some(&slices),
        };
        let s = build(None, Some(2), &applied(), &seen);
        assert_eq!(ready_reason(&s), "Converged");
        assert_eq!((s.desired, s.ready, s.members), (2, 2, Some(2)));
        assert_eq!(s.member_nodes, ["n1", "n2"]);
    }

    #[test]
    fn stale_observed_generation_is_rolling_out() {
        let d = ds(3, 2, 2, 2, 2);
        let s = build(
            None,
            Some(3),
            &applied(),
            &Observed {
                daemonset: Some(&d),
                peer_slices: None,
            },
        );
        assert_eq!(ready_reason(&s), "RollingOut");
    }

    #[test]
    fn unready_and_unset_endpoints_are_not_members() {
        let d = ds(1, 1, 3, 3, 3);
        let slices = [slice(&[
            ("n1", Some(true)),
            ("n2", Some(false)),
            ("n3", None),
        ])];
        let s = build(
            None,
            Some(1),
            &applied(),
            &Observed {
                daemonset: Some(&d),
                peer_slices: Some(&slices),
            },
        );
        assert_eq!(s.members, Some(1));
        assert_eq!(ready_reason(&s), "MembersMissing");
    }

    #[test]
    fn no_matching_nodes_is_reported_as_such() {
        let d = ds(1, 1, 0, 0, 0);
        let s = build(
            None,
            Some(1),
            &applied(),
            &Observed {
                daemonset: Some(&d),
                peer_slices: None,
            },
        );
        assert_eq!(ready_reason(&s), "NoNodes");
        assert_eq!(s.members, None);
    }

    #[test]
    fn a_refused_render_keeps_the_previous_inventory() {
        let kept = InventoryRef {
            api_version: "v1".into(),
            kind: "ConfigMap".into(),
            name: "c".into(),
        };
        let previous = CacheRingStatus {
            inventory: vec![kept.clone()],
            ..Default::default()
        };
        let s = build(
            Some(&previous),
            Some(2),
            &Outcome::Refused("bad".into()),
            &Observed::default(),
        );
        assert_eq!(s.inventory, [kept]);
        let rendered = s.conditions.iter().find(|c| c.type_ == RENDERED).unwrap();
        assert_eq!(
            (rendered.status.as_str(), rendered.reason.as_str()),
            ("False", "ValuesRefused")
        );
        assert_eq!(ready_reason(&s), "NotApplied");
    }

    #[test]
    fn only_a_converged_ring_is_ready() {
        let converged = ds(1, 1, 1, 1, 1);
        let rolling = ds(2, 1, 1, 1, 1);
        let ready = |d: &DaemonSet| {
            is_ready(&build(
                None,
                Some(1),
                &applied(),
                &Observed {
                    daemonset: Some(d),
                    peer_slices: None,
                },
            ))
        };
        assert!(ready(&converged));
        assert!(!ready(&rolling));
        assert!(!is_ready(&build(
            None,
            Some(1),
            &Outcome::Refused("x".into()),
            &Observed::default()
        )));
    }

    #[test]
    fn transition_time_survives_an_unchanged_status() {
        let d = ds(1, 1, 1, 1, 1);
        let seen = Observed {
            daemonset: Some(&d),
            peer_slices: None,
        };
        let first = build(None, Some(1), &applied(), &seen);
        let second = build(Some(&first), Some(1), &applied(), &seen);
        assert_eq!(first.conditions, second.conditions);
    }
}
