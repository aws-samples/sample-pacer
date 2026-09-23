> Design notes for the `networkPolicy`, `ports` and `service` keys in
> [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file
> keeps one short comment per key; the reasoning and the failure history live here.

# Who may reach the daemon — ADR-0006's authorization boundary

[ADR-0006](../adr/0006-strip-and-resign-auth.md) re-signs every request with
the NODE's IAM identity, so **reaching `:9000` IS the authorization to spend it**. That
makes this block the client-side security model, not a hardening extra — and it is why the
policy ships ON (`networkPolicy.enabled: true`).

**It enforces nothing unless your CNI enforces NetworkPolicy.** Every cluster's API server
accepts the object; only a policy-enforcing CNI acts on it. On EKS with the AWS VPC CNI the
addon ships enforcement DISABLED, so the default outcome is a policy that renders, applies,
reads correctly in `kubectl get netpol` — and changes nothing. Confirm enforcement before
treating this as a control; the repository [`README.md`](../../README.md) §
"Enforcement is not automatic" has the check (ask the **addon configuration**, not the
`aws-node` DaemonSet template).

What it cannot express: "same node" (NetworkPolicy has no node-topology selector, and under
the VPC CNI pod IPs come from shared subnets so there is no per-node `ipBlock` either), and
anything about the EFA/RDMA plane (device-to-device, never seen by the CNI).

## The three ports

| port | values key | what listens |
|---|---|---|
| `ports.s3` (9000) | `networkPolicy.clients` | the S3 API clients point at |
| `ports.admin` (9090) | `networkPolicy.probeCidrs` + `networkPolicy.metrics` | probes + `/metrics` |
| `ports.peer` (9100) | not configurable | the peer gRPC data/control plane (cluster tier) |

`service.internalTrafficPolicy: Local` is hard node-locality: kube-proxy only routes to
endpoints on the same node.

## `clients` — and why the shipped default is cluster-wide

**The shipped default is cluster-wide, and it is the thing to tighten.** It matches the
reachability an install had before this policy existed, deliberately: a chart-default that
denied would stop serving every already-deployed client the moment it was upgraded into,
and a cache that has silently stopped being reachable reads as a performance regression.
Cross-namespace is the default because clients normally live in a workload namespace while
the daemon lives in its own, and a bare `podSelector` would match only the release
namespace.

`clients` is a list of `NetworkPolicyPeer`, so `podSelector` / `namespaceSelector` /
`ipBlock` all work. Tighten it to the pods that should hold the node's S3 identity:

```yaml
clients:
  - namespaceSelector:
      matchLabels: {kubernetes.io/metadata.name: my-workload}
    podSelector:
      matchLabels: {app: my-trainer}
```

Both selectors in ONE list entry means **AND** — two entries would mean OR, i.e. every pod
in that namespace plus that app in every namespace.

`pacer.networkPolicyClientsWideOpen` is the helper that recognises the wide-open shape, and
[`templates/NOTES.txt`](../../deploy/helm/pacer/templates/NOTES.txt) says so out loud after
an install, because the difference between "we installed the policy" and "we scoped the
node's IAM identity" is invisible otherwise. Wide-open means an entry with no label
selection AND no `ipBlock`; each sub-test needs its own emptiness check rather than plain
truthiness, since `namespaceSelector: {}` — the default, and precisely the wide-open case —
is an EMPTY map, which Go templates treat as false.

## `probeCidrs` — the only entry that can admit the kubelet

Addresses allowed to reach `:9090`, as CIDRs. The kubelet's liveness/readiness/startup
probes come from the NODE's address and are not a pod, so **no selector can match them**:
this is the only entry that can admit them, and denying them CrashLoops the DaemonSet with
nothing pointing at the policy. Default is permissive (`0.0.0.0/0`) for exactly that
reason — narrow it to your node subnets (e.g. `["10.0.0.0/16"]`) rather than emptying it.

## `metrics`

Pods allowed to reach `:9090` in addition to `probeCidrs` — a Prometheus, or the
curl-in-a-pod some bench arms use to read counters. Same `NetworkPolicyPeer` shape as
`clients`. `/metrics` is unauthenticated and discloses key and topology detail
(`threat-model.md` T-008), so this is worth narrowing to the
scraper's namespace. It **may** be empty.

## `extraIngress`

Extra ingress rules, appended verbatim. A NetworkPolicy's rules are a **UNION**, so
anything here can only widen reachability — there is no entry that tightens a rule above.
For a service mesh sidecar's own ports, or an operator's own probe.

## `:9100` is not configurable

The peer plane admits this release's own daemon pods and nothing else. It carries no
credential of any kind — `requester_node_id` is self-asserted and `Invalidate`/`Announce`
are unauthenticated (`threat-model.md` T-002/T-003) — so widening
it hands out cache contents and directory writes. Use `extraIngress` if you genuinely need
to, and read those two rows first.

## The two shapes the chart refuses to render

An ingress rule is only emitted for a port when its peer list is non-empty, so an EMPTY
list is not "no restriction on this port" — it is "no rule admits this port", i.e. **deny**.
That inversion is invisible in the rendered object (the port simply isn't mentioned) and
surfaces as connection timeouts from every client, or as a CrashLooping DaemonSet when it is
the probe path. `pacer.validateNetworkPolicy` fails at render time with the alternative
spelled out instead:

* **empty `clients`** — denies `:9000`. If you meant to deny it, set
  `networkPolicy.enabled=false` and write your own policy; the chart will not render a deny
  it cannot distinguish from a mistake.
* **empty `probeCidrs`** — denies the kubelet's probes, so the container fails its
  `startupProbe` and CrashLoops forever.

## See also

* [`README.md`](../../README.md) § "Securing access — reachability *is* authorization".
* `threat-model.md` — M-001, T-002, T-003, T-008.
* [ADR-0006](../adr/0006-strip-and-resign-auth.md).
