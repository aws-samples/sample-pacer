//! The operator ↔ chart contract, checked against the real `deploy/helm/pacer` chart.
//!
//! These run `helm`, which the workspace's default test environment does not have, so they
//! are `#[ignore]`d and run explicitly where helm is installed:
//!
//! ```text
//! cargo test -p pacer-operator --test render -- --ignored
//! ```
//!
//! They fail — they do not skip — when helm is missing, so a CI step that asks for them
//! cannot pass without having run them.

use std::path::PathBuf;

use kube::api::{DynamicObject, ObjectMeta};
use pacer_operator::crd::CacheRing;
use pacer_operator::guard::{self, GuardError, ALLOWED_KINDS};
use pacer_operator::render::{HelmRenderer, RenderError, Renderer};
use serde_json::{json, Value};

const NEEDS_HELM: &str = "needs helm on PATH (or PACER_OPERATOR_HELM)";

fn renderer() -> HelmRenderer {
    let chart = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/helm/pacer");
    let helm = std::env::var(pacer_operator::config::ENV_HELM).unwrap_or_else(|_| "helm".into());
    HelmRenderer {
        helm: helm.into(),
        chart,
    }
}

fn ring() -> CacheRing {
    let mut ring = CacheRing::new("r1", Default::default());
    ring.metadata = ObjectMeta {
        name: Some("r1".into()),
        namespace: Some("cache".into()),
        uid: Some("u-1".into()),
        ..Default::default()
    };
    ring
}

async fn render(values: Value) -> Result<Vec<DynamicObject>, RenderError> {
    let Value::Object(values) = values else {
        panic!("values must be an object")
    };
    renderer().render("r1", "cache", &values).await
}

fn kinds(objects: &[DynamicObject]) -> Vec<&str> {
    objects
        .iter()
        .map(|o| o.types.as_ref().unwrap().kind.as_str())
        .collect()
}

#[tokio::test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
async fn default_values_pass_the_guard() {
    let objects = render(json!({})).await.expect(NEEDS_HELM);
    let prepared = guard::prepare(&ring(), objects).expect("every default kind is allowed");
    assert!(prepared
        .iter()
        .all(|o| o.metadata.namespace.as_deref() == Some("cache")));
}

#[tokio::test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
async fn a_ring_renders_one_daemonset_and_one_headless_peer_service() {
    // `reconcile::observe` finds the ring's DaemonSet and its membership Service by kind
    // and by `clusterIP: None`. If the chart ever renders a second of either, status would
    // silently describe whichever came first.
    let objects = render(json!({})).await.expect(NEEDS_HELM);
    let k = kinds(&objects);
    assert_eq!(k.iter().filter(|k| **k == "DaemonSet").count(), 1);
    let headless = objects
        .iter()
        .filter(|o| o.types.as_ref().unwrap().kind == "Service")
        .filter(|o| o.data["spec"]["clusterIP"] == "None")
        .count();
    assert_eq!(headless, 1);
}

#[tokio::test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
async fn optional_templates_stay_inside_the_allowlist() {
    let objects = render(json!({
        "efa": { "enabled": true },
        "monitoring": { "prometheusRule": { "enabled": true } },
    }))
    .await
    .expect(NEEDS_HELM);
    assert!(kinds(&objects).contains(&"PrometheusRule"));
    guard::prepare(&ring(), objects).expect("optional templates are allowed");
}

#[tokio::test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
async fn karpenter_objects_are_refused() {
    let objects = render(json!({
        "karpenter": { "enabled": true, "zoneId": "use1-az4", "clusterName": "c", "role": "r" },
    }))
    .await
    .expect(NEEDS_HELM);
    let refused = guard::prepare(&ring(), objects).unwrap_err();
    assert!(matches!(refused, GuardError::Kind { .. }), "{refused}");
}

#[tokio::test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
async fn an_unknown_value_is_refused_with_the_chart_message() {
    let err = render(json!({ "bogus": 1 })).await.unwrap_err();
    let RenderError::Refused(message) = err else {
        panic!("expected a refusal, got {err}")
    };
    assert!(message.contains("bogus"), "{message}");
}

#[tokio::test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
async fn the_allowlist_has_no_dead_entries() {
    // Every allowed kind is one some configuration of the chart renders; an entry nothing
    // renders is permission the operator's RBAC must grant for no reason.
    let mut seen: Vec<(String, String)> = Vec::new();
    for values in [
        json!({}),
        json!({ "monitoring": { "prometheusRule": { "enabled": true } } }),
    ] {
        for o in render(values).await.expect(NEEDS_HELM) {
            let t = o.types.unwrap();
            seen.push((t.api_version, t.kind));
        }
    }
    for (api_version, kind) in ALLOWED_KINDS {
        let found = seen.iter().any(|(a, k)| a == api_version && k == kind);
        assert!(found, "{api_version} {kind} is allowed but never rendered");
    }
}
