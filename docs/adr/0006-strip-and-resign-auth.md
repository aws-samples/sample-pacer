# ADR-0006: Client auth = strip-and-re-sign with the node's IAM identity

Date: 2026-07-10 · Status: Accepted

## Context

SigV4 signs the Host header, so a proxy on a different hostname breaks client signatures
(SignatureDoesNotMatch). Three patterns exist (planning/02 §4): pure pass-through (impossible for a
renamed endpoint), verify-then-re-sign (proxy must hold every client's secret — gaul/s3proxy model),
strip-and-re-sign (proxy signs fresh with its own identity — awslabs/aws-sigv4-proxy model).
The property to preserve across whichever is chosen: the daemon holds no user secrets, and the
pod↔daemon hop is protected by segmentation rather than by a credential.

## Decision

**Strip-and-re-sign.** Clients point SDKs at the node endpoint with placeholder credentials;
the daemon re-signs outbound requests with its own identity (**EKS Pod Identity**,
`s3express:CreateSession` scoped to the bucket ARN — the one decided mechanism, see
planning gotchas). Pod↔daemon hop protected by network
segmentation (NetworkPolicy).

## Consequences

- Daemon holds no user secrets — the "untrusted daemon" property is preserved.
- **The network boundary IS the authorization boundary — state it as such.**
  Because the daemon re-signs with a privileged node identity, any pod that can
  reach the listener gets full `s3express` access to the bucket as the node
  (a deliberate confused-deputy, same as aws-sigv4-proxy). Reachability is scoped
  by a shipped **NetworkPolicy** (in the Helm chart, not left to the operator —
  `networkPolicy` in values.yaml), which restricts which pods may reach the
  listener. On a shared multi-tenant GPU node this is the whole security model; a
  multi-tenant deployment that needs per-caller authz must move to
  verify-then-re-sign (below).

  **Amended 2026-09-06, when the policy was actually written.** Two claims made
  above before it existed do not survive contact with the API:

  * **"same-node" is not expressible.** This decision originally scoped
    reachability "two ways", the second being a policy restricting pods *on that
    node*. NetworkPolicy has no node-topology selector, and under the VPC CNI pod
    IPs come from shared subnets, so there is no per-node `ipBlock` either.
    `internalTrafficPolicy: Local` (ADR-0001) constrains Service-VIP *routing* to
    the local node — it is not an authorization boundary, because an admitted pod
    on any node can dial a daemon pod IP directly. The boundary the chart draws is
    "these pods", cluster-wide.
  * **The policy binds only where the CNI enforces it.** Every API server accepts
    the object; only a policy-enforcing CNI acts on it, and the AWS VPC CNI ships
    enforcement disabled. So "a shipped NetworkPolicy" is a necessary and not a
    sufficient condition for this ADR's security model — which is why the chart
    announces the check at install time rather than assuming it (threat-model.md
    T-004/M-020).

  Neither changes the decision; both change what may be claimed for it.
  The proxy is opt-in — a pod can always use the real endpoint directly — so this
  governs only the accelerated path.
- All node traffic acts as ONE IAM identity → per-caller authorization is out of scope for now. Multi-tenancy later = verify-then-re-sign via `s3s::auth::S3Auth` (the trait is already in the stack); ADR to be revisited then.
- Simplest possible client config; no credential distribution problem.
- Express detail: authorization happens once per CreateSession (5-min amortized) on AWS's side — per-request IAM evaluation isn't in the hot path anyway.
