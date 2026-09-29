# The operator (`deploy/helm/pacer-operator`)

Design and rationale: [ADR-0047](../adr/0047-cachering-operator-renders-the-chart.md). This
page is the how-to; the ADR is the why.

## Install

```bash
helm install pacer-operator deploy/helm/pacer-operator \
  --namespace pacer-system --create-namespace \
  --set watchNamespace=cache
```

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
