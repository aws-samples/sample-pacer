//! The `CacheRing` custom resource (ADR-0047).
//!
//! A ring is one release of the shipped `pacer` chart: its name is the release name, its
//! namespace the release namespace, and [`CacheRingSpec::values`] the chart's values,
//! passed through untouched. The spec is deliberately thin — every knob a ring has is a
//! chart value, validated by the chart's own `values.schema.json` at render time — so the
//! operator and a plain `helm install` can never disagree about what a value means.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::Condition;
use kube::CustomResource;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// API group of every resource this operator serves. The same `pacer.io` prefix the chart
/// already uses for its labels (`pacer.io/nodepool`, `pacer.io/session`), so a cluster sees
/// one PACER namespace for keys, not two.
pub const GROUP: &str = "pacer.io";

/// Desired state of one cache ring.
#[derive(CustomResource, Deserialize, Serialize, Clone, Debug, Default, JsonSchema)]
#[kube(
    group = "pacer.io",
    version = "v1alpha1",
    kind = "CacheRing",
    namespaced,
    doc = "One PACER cache ring: a release of the pacer chart, reconciled by pacer-operator (ADR-0047).",
    status = "CacheRingStatus",
    shortname = "ring",
    printcolumn = r#"{"name":"Desired","type":"integer","jsonPath":".status.desired"}"#,
    printcolumn = r#"{"name":"Ready","type":"integer","jsonPath":".status.ready"}"#,
    printcolumn = r#"{"name":"Members","type":"integer","jsonPath":".status.members"}"#,
    printcolumn = r#"{"name":"Status","type":"string","jsonPath":".status.conditions[?(@.type==\"Ready\")].reason"}"#,
    printcolumn = r#"{"name":"Age","type":"date","jsonPath":".metadata.creationTimestamp"}"#
)]
pub struct CacheRingSpec {
    /// Values for the `pacer` chart, exactly as `helm install -f` would take them. Validated
    /// against the chart's `values.schema.json` when the ring is rendered, and reported as
    /// the `Rendered` condition; the API server does not check them at admission.
    #[serde(default)]
    #[schemars(schema_with = "free_form_object")]
    pub values: serde_json::Map<String, serde_json::Value>,
}

/// Observed state of one cache ring.
#[derive(Deserialize, Serialize, Clone, Debug, Default, PartialEq, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct CacheRingStatus {
    /// The `metadata.generation` this status describes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed_generation: Option<i64>,
    /// `Rendered`, `Applied` and `Ready`, as defined in ADR-0047 § 5.
    #[serde(default)]
    pub conditions: Vec<Condition>,
    /// Nodes the ring's DaemonSet should run a daemon on.
    #[serde(default)]
    pub desired: i32,
    /// Daemons running the current pod template.
    #[serde(default)]
    pub updated: i32,
    /// Daemons passing their readiness probe.
    #[serde(default)]
    pub ready: i32,
    /// Ready endpoints of the ring's headless peer Service — the membership the daemons
    /// themselves hash keys over. Absent when the chart renders no peer Service
    /// (`cluster.enabled: false`: independent nodes, no ring).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub members: Option<i32>,
    /// Node names behind `members`, sorted.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub member_nodes: Vec<String>,
    /// Every object the last successful apply wrote. The next reconcile deletes whatever is
    /// here and no longer rendered, which is how a value that turns a template off (a
    /// NetworkPolicy, a PodDisruptionBudget) removes the object it used to produce.
    #[serde(default)]
    pub inventory: Vec<InventoryRef>,
}

/// One object the operator applied on a ring's behalf. Always in the ring's namespace
/// (the guard refuses anything else), so no namespace field.
#[derive(
    Deserialize, Serialize, Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub struct InventoryRef {
    /// `group/version`, or `version` for the core group — as in a manifest.
    pub api_version: String,
    /// Object kind.
    pub kind: String,
    /// Object name.
    pub name: String,
}

/// `spec.values` is the chart's whole values tree, whose schema the chart owns. An object
/// that keeps unknown fields, rather than a schema copied from `values.schema.json` that
/// would drift from it the first time a chart value is added.
fn free_form_object(_: &mut schemars::SchemaGenerator) -> schemars::Schema {
    schemars::json_schema!({
        "type": "object",
        "x-kubernetes-preserve-unknown-fields": true,
    })
}

#[cfg(test)]
mod tests {
    use kube::CustomResourceExt;

    use super::*;

    #[test]
    fn values_keep_unknown_fields() {
        let crd = serde_json::to_value(CacheRing::crd()).unwrap();
        let values = &crd["spec"]["versions"][0]["schema"]["openAPIV3Schema"]["properties"]["spec"]
            ["properties"]["values"];
        assert_eq!(values["x-kubernetes-preserve-unknown-fields"], true);
        assert_eq!(values["type"], "object");
    }

    #[test]
    fn crd_is_namespaced_in_the_pacer_group() {
        let crd = CacheRing::crd();
        assert_eq!(crd.spec.group, GROUP);
        assert_eq!(crd.spec.scope, "Namespaced");
        assert_eq!(crd.spec.names.plural, "cacherings");
    }
}
