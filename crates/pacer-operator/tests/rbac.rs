//! Asserts the operator chart's own RBAC (`deploy/helm/pacer-operator/templates/rbac.yaml`)
//! grants every kind in [`pacer_operator::guard::ALLOWED_KINDS`] full CRUD, so the two
//! cannot drift apart (referenced from that template's header comment).
//!
//! Needs `helm`; run with `cargo test -p pacer-operator --test rbac -- --ignored`.

use std::path::PathBuf;
use std::process::Command;

use pacer_operator::guard::ALLOWED_KINDS;
use serde::Deserialize;
use serde_json::Value;

const NEEDS_HELM: &str = "needs helm on PATH (or PACER_OPERATOR_HELM)";

/// The verbs `converge::apply`/`converge::prune` need: read, write and delete an object.
const REQUIRED_VERBS: &[&str] = &[
    "get", "list", "watch", "create", "update", "patch", "delete",
];

/// `(apiVersion, kind) -> (apiGroup, resource)`, the mapping RBAC rules are written in.
fn to_rbac(api_version: &str, kind: &str) -> (String, String) {
    let group = api_version
        .split_once('/')
        .map_or("", |(g, _)| g)
        .to_owned();
    let lower = kind.to_lowercase();
    // Kubernetes plurals follow English spelling, not a blanket "+s": NetworkPolicy is
    // "networkpolicies", not "networkpolicys". Every other ALLOWED_KINDS entry is regular.
    let resource = if let Some(stem) = lower.strip_suffix('y') {
        format!("{stem}ies")
    } else {
        format!("{lower}s")
    };
    (group, resource)
}

#[test]
#[ignore = "needs helm on PATH (or PACER_OPERATOR_HELM)"]
fn rbac_covers_every_allowed_kind() {
    let chart = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../deploy/helm/pacer-operator");
    let helm = std::env::var(pacer_operator::config::ENV_HELM).unwrap_or_else(|_| "helm".into());
    let out = Command::new(&helm)
        .args([
            "template",
            "op",
            chart.to_str().unwrap(),
            "--set",
            "watchNamespace=cache",
            "--show-only",
        ])
        .arg("templates/rbac.yaml")
        .output()
        .expect(NEEDS_HELM);
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let text = String::from_utf8(out.stdout).unwrap();

    let mut rules: Vec<(Vec<String>, Vec<String>, Vec<String>)> = Vec::new();
    for doc in serde_yaml_ng::Deserializer::from_str(&text) {
        let value = Value::deserialize(doc).unwrap();
        if value["kind"] != "Role" && value["kind"] != "ClusterRole" {
            continue;
        }
        for rule in value["rules"].as_array().unwrap() {
            let strs = |k: &str| {
                rule[k]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_str().unwrap().to_owned())
                    .collect()
            };
            rules.push((strs("apiGroups"), strs("resources"), strs("verbs")));
        }
    }

    for (api_version, kind) in ALLOWED_KINDS {
        let (group, resource) = to_rbac(api_version, kind);
        let rule = rules.iter().find(|(groups, resources, _)| {
            groups.contains(&group) && resources.contains(&resource)
        });
        let Some((_, _, verbs)) = rule else {
            panic!(
                "no RBAC rule for {api_version} {kind} (group {group:?}, resource {resource:?})"
            );
        };
        for verb in REQUIRED_VERBS {
            assert!(
                verbs.iter().any(|v| v == verb),
                "{api_version} {kind} rule is missing verb {verb}: {verbs:?}"
            );
        }
    }
}
