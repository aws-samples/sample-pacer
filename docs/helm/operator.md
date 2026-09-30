# The operator (`deploy/helm/pacer-operator`)

Design and rationale: [ADR-0047](../adr/0047-cachering-operator-renders-the-chart.md). This
page is the how-to; the ADR is the why.

## Install

```bash
helm install pacer-operator oci://ghcr.io/aws-samples/sample-pacer/charts/pacer-operator \
  --version 0.1.0 --namespace pacer-system --create-namespace \
  --set watchNamespace=cache
```

Each release publishes the operator chart and image with the same version as the `pacer`
chart, and the operator renders rings with exactly that `pacer` chart, so a ring's
daemons default to the daemon image of the same release. From a checkout, install
`deploy/helm/pacer-operator` instead and point `image.repository`/`image.tag` at an
image you built with `docker build -f Dockerfile.operator .`.

`watchNamespace` also sets the scope of the operator's own RBAC: set, it gets a Role in
that namespace only; empty, it watches every namespace through a ClusterRole. Prefer a
namespace unless one operator really serves rings everywhere (ADR-0047 § 4).

Installing the CRD is part of `helm install`: Helm installs
everything under `crds/` before any template, and never touches it again once installed —
Helm will not delete or upgrade a CRD across releases, run `kubectl apply -f
deploy/helm/pacer-operator/crds/` by hand for that.

## Create a ring

```yaml
apiVersion: pacer.io/v1alpha1
kind: CacheRing
metadata:
  name: training
  namespace: cache
spec:
  values:                      # exactly deploy/helm/pacer's values.yaml keys
    nodeSelector: { pacer.io/nodepool: cache }
    config: { memCapacity: 64GiB }
```

Anything you would pass to `helm install -f` for the `pacer` chart goes under `spec.values`
unchanged — the operator does not reshape it (ADR-0047 § 2).

⚠ **Removing a chart default needs server-side apply.** In a values file, `key: null`
deletes a default — for example `nodeSelector: { pacer.io/nodepool: null }` to drop the
chart's cache-pool selector. The API server stores that null and the operator passes it to
the chart, but **client-side `kubectl apply` reads null as "delete this field from the
object" and never sends it**, so the ring silently keeps the default. Use

```bash
kubectl apply --server-side -f ring.yaml     # or kubectl create
```

The symptom of the other path is a ring stuck at `NoNodes`, with the default selector
still on its DaemonSet.

## Read its status

```bash
kubectl get cacherings -n cache
NAME       DESIRED   READY   MEMBERS   STATUS       AGE
training   4         4       4         Converged    3m

kubectl describe cachering training -n cache   # Rendered/Applied/Ready conditions, inventory
```

`STATUS` is the `Ready` condition's reason (ADR-0047 § 5): `Converged` is the only "done"
state; every other value says what is still missing —`RollingOut`, `DaemonsNotReady`,
`MembersMissing`, `NoNodes`, or `NotApplied`/`ValuesRefused` when the ring never rendered at
all (check `kubectl describe` for the chart's own refusal message in that case).

## Delete a ring

```bash
kubectl delete cachering training -n cache
```

Every object the operator applied for that ring carries an owner reference to it
(ADR-0047 § 3) and is garbage-collected with it — no separate cleanup.

## What it will not do

- **Provision nodes.** A ring runs on nodes that already exist; the chart's Karpenter
  objects are refused (ADR-0047 § 6, tracked as
  [#45](https://github.com/aws-samples/sample-pacer/issues/45)).
- **Run two rings on the same nodes.** Neither the chart nor the operator's first version
  detects the overlap; the second ring's DaemonSet pods stay `Pending`
  ([#46](https://github.com/aws-samples/sample-pacer/issues/46)).
- **Reject a bad value before you apply it.** `spec.values` has no structural schema of its
  own yet; an invalid value is accepted by the API server and reported as
  `Rendered=False` at the next reconcile ([#47](https://github.com/aws-samples/sample-pacer/issues/47)).

## Testing a change to the operator

Unit tests and the render tests (`cargo test -p pacer-operator`, plus `-- --ignored` with
helm installed) check the pure logic and the operator's contract with the chart. They
cannot see what happens against a real API server, which is where the operator's first
real defect was: it crash-looped on start while every one of them passed.

[`scripts/e2e/operator-e2e.sh`](../../scripts/e2e/operator-e2e.sh) is the end-to-end test:
it installs the operator, creates a ring, and checks convergence, a refused value, a
rollout, pruning, drift repair, membership changes and garbage collection against the live
cluster. [`scripts/e2e/operator-kind.sh`](../../scripts/e2e/operator-kind.sh) runs it on a
throwaway three-node kind cluster, which is what CI's `operator-e2e` job does for every
change to the operator, the chart or the images. To run the same thing locally:

```bash
docker build -t pacer:e2e .
docker build -f Dockerfile.operator -t pacer-operator:e2e .
scripts/e2e/operator-kind.sh          # KIND_KEEP=1 leaves the cluster up to inspect
```

On any other cluster, run `operator-e2e.sh` directly with your own images and values; its
header lists what it needs.
