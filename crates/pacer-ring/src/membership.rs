//! Membership sources. Each publishes member epochs into a
//! [`SharedRing`](crate::SharedRing).
//!
//! Production: a Kubernetes EndpointSlice watch on the daemon's own headless
//! peer Service (feature `k8s`). The API server is the single source of truth
//! kubelet already maintains — no gossip protocol to operate. Readiness is the
//! membership signal: a node that fails its readiness probe leaves the ring
//! and its keys re-home (rendezvous: only its keys move).
//!
//! Tests/dev: a static peer list (`name=addr,…`).

use crate::NodeId;

/// Parse `PACER_PEERS`-style static membership: "node-a=10.0.0.1,node-b=10.0.0.2".
pub fn parse_static_peers(s: &str) -> Vec<NodeId> {
    s.split(',')
        .filter_map(|pair| {
            let (name, addr) = pair.split_once('=')?;
            let (name, addr) = (name.trim(), addr.trim());
            (!name.is_empty() && !addr.is_empty()).then(|| NodeId::new(name, addr))
        })
        .collect()
}

#[cfg(feature = "k8s")]
pub use k8s::watch_endpoint_slices;

#[cfg(feature = "k8s")]
mod k8s {
    use std::collections::BTreeMap;

    use futures::TryStreamExt;
    use k8s_openapi::api::discovery::v1::EndpointSlice;
    use kube::runtime::watcher::{self, Event};
    use kube::runtime::WatchStreamExt;
    use kube::{Api, Client};
    use tracing::{info, warn};

    use crate::{NodeId, SharedRing};

    /// Watch the EndpointSlices of `service` in `namespace` and publish every
    /// membership change into `ring`. Runs forever (watcher restarts with
    /// backoff on API errors); returns only on a non-retryable setup error.
    ///
    /// RBAC: needs `list`+`watch` on `discovery.k8s.io/endpointslices` in the
    /// release namespace (chart ships the Role).
    ///
    /// # Errors
    ///
    /// Only non-retryable setup failures (kube client construction, exhausted
    /// watch backoff); transient API errors are retried internally.
    pub async fn watch_endpoint_slices(
        ring: SharedRing,
        namespace: String,
        service: String,
    ) -> anyhow::Result<()> {
        let client = Client::try_default().await?;
        let api: Api<EndpointSlice> = Api::namespaced(client, &namespace);
        let cfg =
            watcher::Config::default().labels(&format!("kubernetes.io/service-name={service}"));

        // A Service may span several EndpointSlices; membership is the union.
        // BTreeMap keyed by slice name keeps republishing deterministic.
        let mut slices: BTreeMap<String, Vec<NodeId>> = BTreeMap::new();
        // During a re-list (Init…InitDone) events describe a fresh world;
        // buffer them so a half-received re-list never publishes a shrunken ring.
        let mut pending: Option<BTreeMap<String, Vec<NodeId>>> = None;

        let mut stream = std::pin::pin!(watcher::watcher(api, cfg).default_backoff());
        while let Some(event) = stream.try_next().await? {
            match event {
                Event::Init => pending = Some(BTreeMap::new()),
                Event::InitApply(es) => {
                    if let Some(p) = pending.as_mut() {
                        p.insert(slice_name(&es), ready_members(&es));
                    }
                }
                Event::InitDone => {
                    if let Some(p) = pending.take() {
                        slices = p;
                        publish(&ring, &slices);
                    }
                }
                Event::Apply(es) => {
                    slices.insert(slice_name(&es), ready_members(&es));
                    publish(&ring, &slices);
                }
                Event::Delete(es) => {
                    slices.remove(&slice_name(&es));
                    publish(&ring, &slices);
                }
            }
        }
        warn!("endpointslice watch stream ended");
        Ok(())
    }

    fn slice_name(es: &EndpointSlice) -> String {
        es.metadata.name.clone().unwrap_or_default()
    }

    /// Ready endpoints only: readiness is the liveness signal for the ring.
    /// `node_name` is the stable identity (DaemonSet: one pod per node).
    fn ready_members(es: &EndpointSlice) -> Vec<NodeId> {
        es.endpoints
            .iter()
            .filter(|ep| {
                ep.conditions
                    .as_ref()
                    .and_then(|c| c.ready)
                    .unwrap_or(false)
            })
            .filter_map(|ep| {
                let name = ep.node_name.as_deref()?;
                let addr = ep.addresses.first()?;
                Some(NodeId::new(name, addr.clone()))
            })
            .collect()
    }

    fn publish(ring: &SharedRing, slices: &BTreeMap<String, Vec<NodeId>>) {
        let mut members: Vec<NodeId> = slices.values().flatten().cloned().collect();
        members.sort_by(|a, b| a.name().cmp(b.name()));
        members.dedup_by(|a, b| a.name() == b.name());
        info!(members = members.len(), "membership epoch");
        ring.store(members);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_static_peers() {
        let peers = parse_static_peers("a=10.0.0.1, b=10.0.0.2,,bad,=x,y=");
        assert_eq!(
            peers,
            vec![NodeId::new("a", "10.0.0.1"), NodeId::new("b", "10.0.0.2")]
        );
        assert!(parse_static_peers("").is_empty());
    }
}
