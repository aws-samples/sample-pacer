//! What the operator will write on a ring's behalf (ADR-0047 § 4).
//!
//! The operator's identity can create DaemonSets and RBAC in every namespace it watches,
//! and a ring's author controls the values that shape what gets rendered. The chart only
//! ever renders a fixed set of namespaced objects, but the operator does not rely on that:
//! every rendered object passes through [`prepare`], which refuses anything outside the
//! ring's namespace or outside [`ALLOWED_KINDS`], and stamps each object with the ring's
//! ownership before it is applied.

use k8s_openapi::apimachinery::pkg::apis::meta::v1::OwnerReference;
use kube::api::DynamicObject;
use kube::Resource;

use crate::crd::{CacheRing, InventoryRef};

/// Label on every object the operator applies, naming the ring that owns it. It is how
/// the controller maps an EndpointSlice back to its ring: the EndpointSlice controller
/// copies a Service's labels onto its slices.
pub const RING_LABEL: &str = "pacer.io/ring";

/// Field manager for every server-side apply, and the `managed-by` label value.
pub const MANAGER: &str = "pacer-operator";

/// Standard label naming the tool that manages an object. The chart sets it to `Helm`
/// (`.Release.Service`); an operator-managed ring is not a Helm release, and saying so
/// stops `helm` tooling from claiming the objects.
const MANAGED_BY_LABEL: &str = "app.kubernetes.io/managed-by";

/// `(apiVersion, kind)` pairs the chart renders and the operator will apply. Everything
/// here is namespaced. Karpenter's NodePool and EC2NodeClass are deliberately absent:
/// they are cluster-scoped and name an IAM role, which is not a decision a namespaced
/// ring's author gets to make (ADR-0047 § 6).
pub const ALLOWED_KINDS: &[(&str, &str)] = &[
    ("v1", "ConfigMap"),
    ("v1", "Service"),
    ("v1", "ServiceAccount"),
    ("apps/v1", "DaemonSet"),
    ("rbac.authorization.k8s.io/v1", "Role"),
    ("rbac.authorization.k8s.io/v1", "RoleBinding"),
    ("networking.k8s.io/v1", "NetworkPolicy"),
    ("policy/v1", "PodDisruptionBudget"),
    ("monitoring.coreos.com/v1", "PrometheusRule"),
];

/// Why a rendered object was refused.
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum GuardError {
    /// The object's kind is not one the operator applies.
    #[error("{api_version} {kind} {name} is not a kind the operator applies")]
    Kind {
        /// Refused object's apiVersion.
        api_version: String,
        /// Refused object's kind.
        kind: String,
        /// Refused object's name.
        name: String,
    },
    /// The object names a namespace other than the ring's.
    #[error("{kind} {name} targets namespace {namespace}, not the ring's")]
    Namespace {
        /// Refused object's kind.
        kind: String,
        /// Refused object's name.
        name: String,
        /// The namespace it asked for.
        namespace: String,
    },
    /// A RoleBinding that binds a ClusterRole rather than a Role in the ring's namespace.
    #[error("RoleBinding {0} binds a ClusterRole; only a namespaced Role is allowed")]
    ClusterRoleBinding(String),
    /// The object has no name, or the ring has no namespace/uid yet.
    #[error("missing {0}")]
    Missing(&'static str),
}

/// Check every object in `rendered` and stamp it with `ring`'s namespace, owner reference
/// and labels. All-or-nothing: one refused object refuses the render, so a ring is never
/// half-applied because of a value.
///
/// # Errors
///
/// The first [`GuardError`] found.
pub fn prepare(
    ring: &CacheRing,
    rendered: Vec<DynamicObject>,
) -> Result<Vec<DynamicObject>, GuardError> {
    let namespace = ring
        .meta()
        .namespace
        .clone()
        .ok_or(GuardError::Missing("ring namespace"))?;
    let ring_name = ring
        .meta()
        .name
        .clone()
        .ok_or(GuardError::Missing("ring name"))?;
    let owner = ring
        .controller_owner_ref(&())
        .ok_or(GuardError::Missing("ring uid"))?;
    rendered
        .into_iter()
        .map(|object| stamp(object, &namespace, &ring_name, &owner))
        .collect()
}

fn stamp(
    mut object: DynamicObject,
    namespace: &str,
    ring_name: &str,
    owner: &OwnerReference,
) -> Result<DynamicObject, GuardError> {
    let r = inventory_ref(&object)?;
    if !ALLOWED_KINDS.contains(&(r.api_version.as_str(), r.kind.as_str())) {
        return Err(GuardError::Kind {
            api_version: r.api_version,
            kind: r.kind,
            name: r.name,
        });
    }
    match object.metadata.namespace.as_deref() {
        None => object.metadata.namespace = Some(namespace.to_owned()),
        Some(ns) if ns == namespace => {}
        Some(ns) => {
            return Err(GuardError::Namespace {
                kind: r.kind,
                name: r.name,
                namespace: ns.to_owned(),
            })
        }
    }
    if r.kind == "RoleBinding" && object.data["roleRef"]["kind"] != "Role" {
        return Err(GuardError::ClusterRoleBinding(r.name));
    }
    object.metadata.owner_references = Some(vec![owner.clone()]);
    let labels = object.metadata.labels.get_or_insert_with(Default::default);
    labels.insert(RING_LABEL.to_owned(), ring_name.to_owned());
    labels.insert(MANAGED_BY_LABEL.to_owned(), MANAGER.to_owned());
    Ok(object)
}

/// The inventory entry for `object`.
///
/// # Errors
///
/// [`GuardError::Missing`] when the object has no type or no name.
pub fn inventory_ref(object: &DynamicObject) -> Result<InventoryRef, GuardError> {
    let types = object
        .types
        .as_ref()
        .ok_or(GuardError::Missing("apiVersion/kind"))?;
    let name = object
        .metadata
        .name
        .clone()
        .ok_or(GuardError::Missing("object name"))?;
    Ok(InventoryRef {
        api_version: types.api_version.clone(),
        kind: types.kind.clone(),
        name,
    })
}

#[cfg(test)]
mod tests {
    use kube::api::ObjectMeta;

    use super::*;
    use crate::render::parse_manifests;

    fn ring() -> CacheRing {
        let mut ring = CacheRing::new("a", Default::default());
        ring.metadata = ObjectMeta {
            name: Some("a".into()),
            namespace: Some("cache".into()),
            uid: Some("u-1".into()),
            ..Default::default()
        };
        ring
    }

    fn one(yaml: &str) -> Vec<DynamicObject> {
        parse_manifests(yaml).unwrap()
    }

    #[test]
    fn stamps_namespace_owner_and_labels() {
        let out = prepare(
            &ring(),
            one("apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n"),
        )
        .unwrap();
        let meta = &out[0].metadata;
        assert_eq!(meta.namespace.as_deref(), Some("cache"));
        let owner = &meta.owner_references.as_ref().unwrap()[0];
        assert_eq!(
            (owner.kind.as_str(), owner.uid.as_str(), owner.controller),
            ("CacheRing", "u-1", Some(true))
        );
        let labels = meta.labels.as_ref().unwrap();
        assert_eq!(labels[RING_LABEL], "a");
        assert_eq!(labels[MANAGED_BY_LABEL], MANAGER);
    }

    #[test]
    fn refuses_a_kind_outside_the_allowlist() {
        let yaml = "apiVersion: karpenter.sh/v1\nkind: NodePool\nmetadata:\n  name: n\n";
        assert!(matches!(
            prepare(&ring(), one(yaml)),
            Err(GuardError::Kind { .. })
        ));
    }

    #[test]
    fn refuses_another_namespace() {
        let yaml =
            "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n  namespace: kube-system\n";
        assert!(matches!(
            prepare(&ring(), one(yaml)),
            Err(GuardError::Namespace { .. })
        ));
    }

    #[test]
    fn refuses_a_rolebinding_to_a_clusterrole() {
        let yaml = "apiVersion: rbac.authorization.k8s.io/v1\nkind: RoleBinding\nmetadata:\n  name: b\n\
                    roleRef:\n  apiGroup: rbac.authorization.k8s.io\n  kind: ClusterRole\n  name: cluster-admin\n";
        assert_eq!(
            prepare(&ring(), one(yaml)),
            Err(GuardError::ClusterRoleBinding("b".into()))
        );
    }

    #[test]
    fn one_refusal_refuses_the_whole_render() {
        let yaml = "apiVersion: v1\nkind: ConfigMap\nmetadata:\n  name: c\n---\n\
                    apiVersion: v1\nkind: Secret\nmetadata:\n  name: s\n";
        assert!(prepare(&ring(), one(yaml)).is_err());
    }
}
