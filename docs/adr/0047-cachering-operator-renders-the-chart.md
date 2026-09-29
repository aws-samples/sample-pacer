# ADR-0047: A `CacheRing` operator that renders the shipped chart

Date: 2026-09-29 · Status: **Accepted.** Implements
[#40](https://github.com/aws-samples/sample-pacer/issues/40) (design),
[#41](https://github.com/aws-samples/sample-pacer/issues/41) (resource and reconciler) and
[#42](https://github.com/aws-samples/sample-pacer/issues/42) (status). No measurement: this
records an API and an ownership model, not a performance trade.

## Context

A cache ring is the set of daemons that hash keys over one membership: one DaemonSet, whose
headless peer Service's EndpointSlices are the membership source every daemon watches
(`pacer_ring::membership`, [0014](0014-ring-ownership-wire-contract.md)). Today the only way to
make one is a Helm release of [`deploy/helm/pacer`](../../deploy/helm/pacer), one release per
node pool, with values tuned by hand.

That works for one ring. It stops working as a way to *operate* rings:

- Nothing in the cluster describes a ring. There is no object to list, and no single place
  that says whether a ring converged after a values change, how many daemons it wanted, or
  how many of them are actually members. Answering takes a DaemonSet, a Service, its
  EndpointSlices and some arithmetic.
- The chart's refusals happen at `helm install` time, on the machine running helm, and
  vanish with that terminal. A ring whose values stop being valid has nowhere to say so.
- Rings are provisioned by whoever holds cluster credentials and a checkout of the values.
  Declaring one should be a Kubernetes object that GitOps tooling can own.

The chart is also not small. Around 1,100 lines of `_helpers.tpl` and `_validate.tpl`
**derive** values a ring needs and nobody should type (the container memory limit from the
cache tiers and staging budgets — [0035](0035-daemon-resources-and-qos-class.md); the
registered slab from `memCapacity` — [0028](0028-cache-ram-tier-is-the-registered-arena.md);
the disk-tier default — [0038](0038-chunk-store-is-the-default-disk-tier.md); hugepage
resources) and **refuse** combinations that would boot a broken daemon. Every one of those
exists because a deployment once went wrong without it, and each is pinned by a
`helm unittest` case.

## Decision

### 1. The operator renders with the chart, not instead of it

The operator runs `helm template <ring> <chart> --namespace <ns> --values -` with the ring's
values on stdin, and applies the result. It does not re-implement a single template.

This is sound only because the chart is a **pure function** of (chart, release name,
namespace, values): it uses no `lookup` and no `.Capabilities`, so `helm template` renders
exactly what `helm install` would. That property is now load-bearing, and
`crates/pacer-operator/tests/render.rs` runs the real chart to hold it together with the
rest of the operator ↔ chart contract.

The chart is baked into the operator image at the version the operator was released with,
so an operator release and a chart release are the same thing, and a ring cannot be
rendered by a chart the operator was never tested against.

### 2. The API: a thin, namespaced `CacheRing`

```yaml
apiVersion: pacer.io/v1alpha1
kind: CacheRing
metadata: { name: training, namespace: cache }
spec:
  values:            # the pacer chart's values, as `helm install -f` would take them
    nodeSelector: { pacer.io/nodepool: cache }
    config: { memCapacity: 64GiB }
```

- **Group `pacer.io`**, the prefix the chart already uses for its labels, so a cluster has one
  PACER key namespace rather than two. **`v1alpha1`**: the spec is expected to change.
- **Namespaced.** A ring's objects all live in its namespace, which is where Kubernetes RBAC
  and Pod Security admission already draw their boundaries (§ 4).
- **Name = release name, namespace = release namespace**, so an operator-managed ring's
  objects have exactly the names a `helm install <name>` of the same values would give them.
- **`spec.values` is the whole spec.** Every knob a ring has is a chart value, validated by
  the chart's own `values.schema.json` at render time. A second, typed spec would be a
  second definition of the same knobs that had to be kept in step with the first; the day
  the operator needs to *reason* about a value (a node selector, for
  [#46](https://github.com/aws-samples/sample-pacer/issues/46)), that value gets a typed field.

### 3. Ownership, drift and pruning

- Every rendered object is applied with **server-side apply** under the field manager
  `pacer-operator`, with the ring as its **controller owner reference**. Deleting a ring
  garbage-collects everything it made; no finalizer is needed, because nothing the operator
  creates is outside the ring's namespace (§ 4).
- **Every pass re-renders and re-applies the whole ring**, not only on a spec change. Apply is
  idempotent, and a `kubectl edit` of the ring's DaemonSet is undone on the next pass instead
  of living until the next values change.
- The objects written by the last successful apply are recorded in `status.inventory`. The
  next successful apply deletes each entry it no longer renders — how turning
  `networkPolicy.enabled` off removes the NetworkPolicy — but only if the object still names
  this ring as its owner, so an object recreated by someone else under the same name is left
  alone.
- **A failed render applies nothing and prunes nothing.** The last good objects keep running.
  A typo in a value must not take a warm cache down.

### 4. The trust boundary

Creating a `CacheRing` creates node-level pods: every ring mounts a hostPath cache directory,
and an EFA ring may run privileged containers (`efa.privileged`, [0030](0030-delivery-registration-belongs-to-the-memory-owner.md)).
So:

- **Permission to create a `CacheRing` in a namespace is equivalent to permission to run
  those pods in it.** Grant it only to whoever may already run node-level workloads there. The
  operator does not escalate past this: pods are created by the DaemonSet controller, so the
  namespace's Pod Security admission level applies to a ring's pods exactly as it would to a
  `helm install` — a namespace that forbids privileged pods forbids an EFA ring.
- The operator **refuses** any rendered object that is outside the ring's namespace, of a kind
  outside a fixed allowlist (the kinds the chart renders — `guard::ALLOWED_KINDS`), or a
  RoleBinding to a ClusterRole. The chart cannot currently produce any of those from values;
  the check makes that a property the operator enforces, not one it inherits. One refused
  object refuses the render, so a ring is never half-applied.
- The operator's own RBAC is the union of what those kinds need, namespace-scoped when it
  watches one namespace.

### 5. Status

`status` reports the DaemonSet's `desired`/`updated`/`ready` counts and the ring's
**members**: the ready endpoints of its headless peer Service, counted by the same rule the
daemons' membership watch uses (an endpoint is a member only when `ready` is explicitly true
and it names a node). So status shows the ring the daemons themselves hash over, not an
approximation of it.

Three conditions, each with `observedGeneration`:

| Condition | True means | False reasons |
|---|---|---|
| `Rendered` | the chart accepted `spec.values` | `ValuesRefused` (chart or guard message), `RenderFailed` |
| `Applied` | every object written, every stale one pruned | `ApplyFailed`, `NotRendered` |
| `Ready` | DaemonSet converged on the current template, every daemon ready, every ready daemon a member | `NotApplied`, `NoNodes`, `RollingOut`, `DaemonsNotReady`, `MembersMissing` |

`Ready`'s reason is what `kubectl get cacherings` prints. `MembersMissing` is the one only an
operator can report: daemons that pass their probe but that the ring does not count.

The controller watches rings, the DaemonSets they own, and the EndpointSlices of their
Services (mapped back through the `pacer.io/ring` label, which the EndpointSlice controller
copies from the Service), so a membership change reaches status without waiting for the
periodic resync.

### 6. Not in this version

- **Node provisioning.** The chart's Karpenter NodePool and EC2NodeClass are cluster-scoped and
  name an IAM role for the nodes they launch — not a choice a namespaced ring's author should
  make, and not an object a namespaced owner can garbage-collect. The guard refuses them; rings
  run on nodes that exist. [#45](https://github.com/aws-samples/sample-pacer/issues/45) designs
  an administrator-owned class for it.
- **Overlap between rings.** Two rings selecting the same nodes are kept apart by the chart's
  cross-release anti-affinity, which leaves the second ring's daemons Pending without saying
  why. [#46](https://github.com/aws-samples/sample-pacer/issues/46).
- **Admission-time validation** of `spec.values`; today an invalid value is accepted by the API
  server and reported as `Rendered=False`. [#47](https://github.com/aws-samples/sample-pacer/issues/47).
- **Leader election.** The operator runs as a single replica with a `Recreate` strategy; two
  replicas would both apply, harmlessly but wastefully.

## Alternatives considered

- **Port the chart to Rust** and build objects with typed `k8s-openapi` structs. Typed and
  subprocess-free, but it forks every derivation and refusal in § Context into a second
  implementation that must agree with the first forever, while `helm install` remains a
  supported path. Rejected until the operator is the *only* supported path; then the
  derivations move into Rust and the chart becomes a thin installer, as its own decision.
- **An operator framework's Helm operator** (spec = values, reconcile = `helm upgrade`). The
  same idea, but it keeps Helm release state in Secrets, adds a Go toolchain to a Rust
  workspace, and gives up the typed status and the object guard.
- **No operator; document multi-release Helm.** Leaves every problem in § Context in place.

## Consequences

- The chart's purity (§ 1) is now a contract. A template that adds `lookup` or `.Capabilities`
  breaks operator-managed rings, and the render tests are where that shows up.
- A new kind in the chart is refused by the operator until it is added to the allowlist and
  the operator's RBAC — deliberately, and a render test fails if the two disagree.
- The operator image carries a `helm` binary, which is now part of its supply chain and is
  pinned and checksum-verified at build.
- `spec.values` inherits a Kubernetes client quirk that a values file does not have. A
  `key: null` (how a values file deletes a chart default) is stored by the API server and
  honoured by the render, but client-side `kubectl apply` treats null as "remove this
  field" and never sends it, so the default silently stays. Found on the operator's first
  cluster run, where a ring kept the chart's default node selector and matched no node.
  Documented in `docs/helm/operator.md`: use server-side apply. A typed field for the
  values that are commonly deleted is the lasting fix if this bites users.
- Every reconcile forks `helm template` once. It renders a ring in well under a second, which
  is negligible at the number of rings a cluster has.
