//! The reconcile loop (ADR-0047 § 2–3): render → guard → apply → prune → observe → status.
//!
//! Every pass re-renders and re-applies the whole ring. Server-side apply makes that
//! idempotent, and it means drift — someone `kubectl edit`ing the ring's DaemonSet — is
//! undone on the next pass rather than living on until the next values change.

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use k8s_openapi::api::apps::v1::DaemonSet;
use k8s_openapi::api::core::v1::Service;
use k8s_openapi::api::discovery::v1::EndpointSlice;
use kube::api::{
    Api, ApiResource, DeleteParams, DynamicObject, GroupVersionKind, ListParams, Patch, PatchParams,
};
use kube::runtime::controller::{Action, Controller};
use kube::runtime::reflector::ObjectRef;
use kube::runtime::watcher;
use kube::{Client, Resource, ResourceExt};
use tokio::sync::Mutex;
use tracing::{info, warn};

use crate::crd::{CacheRing, CacheRingStatus, InventoryRef};
use crate::guard::{self, MANAGER, RING_LABEL};
use crate::render::{RenderError, Renderer};
use crate::status::{self, Observed, Outcome};

/// Re-reconcile a CONVERGED ring this often even with no event. Events cover every change
/// the operator knows to watch; this is the backstop for one it does not (a node label
/// edit that changes which nodes match, a missed watch event).
const RESYNC: Duration = Duration::from_secs(300);

/// Re-reconcile this soon when the ring is not yet Ready — a failed apply or render, or a
/// ring still rolling out. Short, because the usual cause is transient (an API server
/// blip, a CRD installed a moment after the ring, a daemon between probes), and because a
/// pass that observed a passing state and then saw no further event would otherwise leave
/// that state in status for a whole RESYNC: on the first cluster runs a ring took 361 s to
/// report Converged after a change that needed no rollout at all, against 6 s on a rerun.
const RETRY: Duration = Duration::from_secs(30);

/// Label the EndpointSlice controller puts on every slice, naming its Service.
const SERVICE_NAME_LABEL: &str = "kubernetes.io/service-name";

/// Why a reconcile pass failed outright. Render and apply failures are not errors here:
/// they are reported in the ring's status and retried. This is for failing to read the
/// ring's workloads or to write its status at all.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A Kubernetes API call failed.
    #[error(transparent)]
    Kube(#[from] kube::Error),
    /// The ring has no namespace (it is a namespaced resource, so the API server always
    /// sets one; seen only with a hand-built object).
    #[error("CacheRing has no namespace")]
    NoNamespace,
}

/// State shared by every reconcile pass.
pub struct Context<R> {
    /// Kubernetes client.
    pub client: Client,
    /// Turns a ring into manifests.
    pub renderer: R,
    /// Discovered API resources by kind, so each kind is looked up once per process.
    resources: Mutex<HashMap<GroupVersionKind, ApiResource>>,
}

impl<R> Context<R> {
    /// A context over `client` rendering with `renderer`.
    pub fn new(client: Client, renderer: R) -> Self {
        Self {
            client,
            renderer,
            resources: Mutex::default(),
        }
    }

    async fn resource(&self, gvk: &GroupVersionKind) -> Result<ApiResource, kube::Error> {
        let mut cache = self.resources.lock().await;
        if let Some(ar) = cache.get(gvk) {
            return Ok(ar.clone());
        }
        let (ar, _) = kube::discovery::pinned_kind(&self.client, gvk).await?;
        cache.insert(gvk.clone(), ar.clone());
        Ok(ar)
    }

    async fn dynamic(
        &self,
        namespace: &str,
        r: &InventoryRef,
    ) -> Result<Api<DynamicObject>, kube::Error> {
        let ar = self.resource(&gvk(r)).await?;
        Ok(Api::namespaced_with(self.client.clone(), namespace, &ar))
    }
}

/// Run the controller until a shutdown signal. Watches rings, the DaemonSets they own,
/// and their peer Services' EndpointSlices — in `namespace`, or everywhere when `None`.
pub async fn run<R: Renderer + 'static>(client: Client, renderer: R, namespace: Option<&str>) {
    let rings: Api<CacheRing> = scoped(&client, namespace);
    let daemonsets: Api<DaemonSet> = scoped(&client, namespace);
    let slices: Api<EndpointSlice> = scoped(&client, namespace);
    let ours = watcher::Config::default().labels(RING_LABEL);
    Controller::new(rings, watcher::Config::default())
        .owns(daemonsets, ours.clone())
        // A slice is owned by its Service, not by the ring, so `owns` cannot map it. The
        // EndpointSlice controller copies the Service's labels onto its slices, and the
        // guard stamped the Service with the ring's name.
        .watches(slices, ours, |slice: EndpointSlice| {
            let ring = slice.labels().get(RING_LABEL)?.clone();
            Some(ObjectRef::new(&ring).within(slice.namespace().as_deref()?))
        })
        .shutdown_on_signal()
        .run(
            reconcile,
            error_policy,
            Arc::new(Context::new(client, renderer)),
        )
        .for_each(|result| async move {
            match result {
                // A request for a ring deleted since it was queued — every delete produces
                // a few, from the owned objects' own deletion events. Not a failure.
                Err(kube::runtime::controller::Error::ObjectNotFound(r)) => {
                    tracing::debug!(object = %r, "ring gone before its reconcile ran");
                }
                Err(e) => warn!(error = %e, "reconcile failed"),
                Ok(_) => {}
            }
        })
        .await;
}

fn scoped<K>(client: &Client, namespace: Option<&str>) -> Api<K>
where
    K: Resource<Scope = k8s_openapi::NamespaceResourceScope>,
    <K as Resource>::DynamicType: Default,
{
    match namespace {
        Some(ns) => Api::namespaced(client.clone(), ns),
        None => Api::all(client.clone()),
    }
}

/// One reconcile pass over `ring`.
///
/// # Errors
///
/// [`Error`] when the ring's workloads cannot be read or its status cannot be written.
pub async fn reconcile<R: Renderer>(
    ring: Arc<CacheRing>,
    ctx: Arc<Context<R>>,
) -> Result<Action, Error> {
    let namespace = ring.namespace().ok_or(Error::NoNamespace)?;
    let previous = ring.status.as_ref();
    let outcome = converge(&ring, &ctx, &namespace).await;
    let inventory = match &outcome {
        Outcome::Applied { inventory } => inventory.as_slice(),
        _ => previous.map(|p| p.inventory.as_slice()).unwrap_or_default(),
    };
    let (daemonset, peer_slices) = observe(&ctx.client, &namespace, inventory).await?;
    let seen = Observed {
        daemonset: daemonset.as_ref(),
        peer_slices: peer_slices.as_deref(),
    };
    let next = status::build(previous, ring.metadata.generation, &outcome, &seen);
    if previous != Some(&next) {
        log_transition(&ring, &next);
        write_status(&ctx.client, &ring, &namespace, &next).await?;
    }
    Ok(Action::requeue(if status::is_ready(&next) {
        RESYNC
    } else {
        RETRY
    }))
}

/// One line per status change, naming every condition's reason — the line that would
/// have said, on the run where a ring sat unconverged for minutes, what it was waiting on.
fn log_transition(ring: &CacheRing, s: &CacheRingStatus) {
    let reason = |t: &str| {
        s.conditions
            .iter()
            .find(|c| c.type_ == t)
            .map_or("", |c| c.reason.as_str())
    };
    info!(
        ring = %ring.name_any(),
        generation = ring.metadata.generation,
        rendered = reason(status::RENDERED),
        applied = reason(status::APPLIED),
        ready = reason(status::READY),
        desired = s.desired,
        ready_daemons = s.ready,
        members = s.members,
        "status"
    );
}

/// Requeue a pass that failed outright.
pub fn error_policy<R>(ring: Arc<CacheRing>, err: &Error, _: Arc<Context<R>>) -> Action {
    warn!(ring = %ring.name_any(), error = %err, "reconcile error; retrying");
    Action::requeue(RETRY)
}

/// Render, guard, apply and prune; the outcome says how far it got.
async fn converge<R: Renderer>(ring: &CacheRing, ctx: &Context<R>, namespace: &str) -> Outcome {
    let name = ring.name_any();
    let rendered = match ctx
        .renderer
        .render(&name, namespace, &ring.spec.values)
        .await
    {
        Ok(objects) => objects,
        Err(RenderError::Refused(m)) => return Outcome::Refused(m),
        Err(RenderError::Failed(m)) => return Outcome::RenderFailed(m),
    };
    let objects = match guard::prepare(ring, rendered) {
        Ok(objects) => objects,
        Err(e) => return Outcome::Refused(e.to_string()),
    };
    let mut inventory = Vec::with_capacity(objects.len());
    for object in &objects {
        match apply(ctx, namespace, object).await {
            Ok(r) => inventory.push(r),
            Err(e) => return Outcome::ApplyFailed(e),
        }
    }
    let previous = ring
        .status
        .as_ref()
        .map(|s| s.inventory.as_slice())
        .unwrap_or_default();
    for stale in stale(previous, &inventory) {
        if let Err(e) = prune(ctx, ring, namespace, stale).await {
            return Outcome::ApplyFailed(e);
        }
    }
    info!(ring = %name, namespace, objects = inventory.len(), "applied");
    Outcome::Applied { inventory }
}

async fn apply<R>(
    ctx: &Context<R>,
    namespace: &str,
    object: &DynamicObject,
) -> Result<InventoryRef, String> {
    let r = guard::inventory_ref(object).map_err(|e| e.to_string())?;
    let api = ctx
        .dynamic(namespace, &r)
        .await
        .map_err(|e| format!("{} {}: {e}", r.kind, r.name))?;
    api.patch(
        &r.name,
        &PatchParams::apply(MANAGER).force(),
        &Patch::Apply(object),
    )
    .await
    .map_err(|e| format!("apply {} {}: {e}", r.kind, r.name))?;
    Ok(r)
}

/// Entries of `previous` that are not in `current`.
#[must_use]
pub fn stale<'a>(previous: &'a [InventoryRef], current: &[InventoryRef]) -> Vec<&'a InventoryRef> {
    let current: BTreeSet<&InventoryRef> = current.iter().collect();
    previous.iter().filter(|r| !current.contains(r)).collect()
}

/// Delete `r`, but only if it is still this ring's: an object of that name recreated by
/// someone else since the last apply is not the operator's to remove.
async fn prune<R>(
    ctx: &Context<R>,
    ring: &CacheRing,
    namespace: &str,
    r: &InventoryRef,
) -> Result<(), String> {
    let describe = |e: kube::Error| format!("prune {} {}: {e}", r.kind, r.name);
    let api = ctx.dynamic(namespace, r).await.map_err(describe)?;
    let Some(object) = api.get_opt(&r.name).await.map_err(describe)? else {
        return Ok(());
    };
    let owned = object
        .owner_references()
        .iter()
        .any(|o| Some(&o.uid) == ring.meta().uid.as_ref());
    if owned {
        api.delete(&r.name, &DeleteParams::background())
            .await
            .map_err(describe)?;
        info!(ring = %ring.name_any(), kind = %r.kind, name = %r.name, "pruned");
    }
    Ok(())
}

/// The ring's DaemonSet, and the EndpointSlices of its headless peer Service (`None` when
/// the inventory has no headless Service: a `cluster.enabled: false` ring).
async fn observe(
    client: &Client,
    namespace: &str,
    inventory: &[InventoryRef],
) -> Result<(Option<DaemonSet>, Option<Vec<EndpointSlice>>), Error> {
    let daemonset = match named(inventory, "DaemonSet").next() {
        Some(name) => {
            Api::<DaemonSet>::namespaced(client.clone(), namespace)
                .get_opt(name)
                .await?
        }
        None => None,
    };
    let services: Api<Service> = Api::namespaced(client.clone(), namespace);
    for name in named(inventory, "Service") {
        let headless = services
            .get_opt(name)
            .await?
            .and_then(|s| s.spec?.cluster_ip)
            .is_some_and(|ip| ip == "None");
        if headless {
            let slices: Api<EndpointSlice> = Api::namespaced(client.clone(), namespace);
            let lp = ListParams::default().labels(&format!("{SERVICE_NAME_LABEL}={name}"));
            return Ok((daemonset, Some(slices.list(&lp).await?.items)));
        }
    }
    Ok((daemonset, None))
}

async fn write_status(
    client: &Client,
    ring: &CacheRing,
    namespace: &str,
    status: &CacheRingStatus,
) -> Result<(), Error> {
    let api: Api<CacheRing> = Api::namespaced(client.clone(), namespace);
    let patch = serde_json::json!({
        "apiVersion": CacheRing::api_version(&()),
        "kind": CacheRing::kind(&()),
        "status": status,
    });
    api.patch_status(
        &ring.name_any(),
        &PatchParams::apply(MANAGER).force(),
        &Patch::Apply(patch),
    )
    .await?;
    Ok(())
}

/// Names of the `kind` entries in `inventory`.
fn named<'a>(inventory: &'a [InventoryRef], kind: &'a str) -> impl Iterator<Item = &'a str> {
    inventory
        .iter()
        .filter(move |r| r.kind == kind)
        .map(|r| r.name.as_str())
}

fn gvk(r: &InventoryRef) -> GroupVersionKind {
    let (group, version) = r
        .api_version
        .split_once('/')
        .unwrap_or(("", &r.api_version));
    GroupVersionKind::gvk(group, version, &r.kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(kind: &str, name: &str) -> InventoryRef {
        InventoryRef {
            api_version: "v1".into(),
            kind: kind.into(),
            name: name.into(),
        }
    }

    #[test]
    fn stale_is_previous_minus_current() {
        let previous = [r("ConfigMap", "a"), r("Service", "b"), r("ConfigMap", "c")];
        let current = [
            r("ConfigMap", "a"),
            r("ConfigMap", "c"),
            r("ConfigMap", "d"),
        ];
        assert_eq!(stale(&previous, &current), [&r("Service", "b")]);
    }

    #[test]
    fn same_name_different_kind_is_stale() {
        assert_eq!(stale(&[r("ConfigMap", "x")], &[r("Service", "x")]).len(), 1);
    }

    #[test]
    fn core_group_parses_to_empty_group() {
        let g = gvk(&r("ConfigMap", "a"));
        assert_eq!((g.group.as_str(), g.version.as_str()), ("", "v1"));
        let g = gvk(&InventoryRef {
            api_version: "apps/v1".into(),
            kind: "DaemonSet".into(),
            name: "d".into(),
        });
        assert_eq!((g.group.as_str(), g.version.as_str()), ("apps", "v1"));
    }
}
