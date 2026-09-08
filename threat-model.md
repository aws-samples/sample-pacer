# PACER (s3-ai-cache) — Threat Model

*Methodology: STRIDE, structured after the AWS Threat Model template and produced with the
[AWS Threat Modeling MCP server](https://github.com/awslabs/threat-modeling-mcp-server) 9-phase
process (business context → architecture → threat actors → trust boundaries → asset flows →
STRIDE identification → mitigation planning → residual risk).*

A threat model answers four questions: (1) What are we building? (2) What can go wrong?
(3) What are we going to do about it? (4) Did we do a good job?

---

## Introduction

### Purpose

PACER is a per-node read-through cache for Amazon S3, deployed as a Kubernetes DaemonSet in
front of high-fan-out, read-heavy object workloads (for example loading model weights and
checkpoints onto many nodes at once). The problem it solves: when a large fleet reads the same
objects from S3 repeatedly, cost and latency are dominated by redundant GETs over the network.
PACER caches objects on node-local RAM+NVMe and serves cache hits to co-located client pods,
and to peer nodes over a fast intra-cluster data plane (gRPC, with an optional EFA/RDMA
one-sided transport), while writes pass straight through to S3 for read-after-write correctness.

This document models the security posture of PACER as an open-source, self-hosted component.
It is **not** a hosted service; the operator deploys and runs it inside their own cluster and
owns the surrounding controls (network policy, IAM scoping, node isolation).

### Project / Asset Overview

- **Single runtime binary — `pacer-daemon`** — one pod per node (DaemonSet). It bundles the
  S3-compatible front end (proxy), the RAM+NVMe cache, the peer replication server, and the
  admin/metrics endpoint. There is no separate control-plane, CLI, or orchestrator process.
- **Cache tier**: a hybrid RAM+NVMe cache (RAM by default 1 GiB, NVMe by default 100 GiB on a
  node-local hostPath).
- **Backend client**: the AWS S3 SDK, using the node's IAM identity (EKS Pod Identity).
  Requests are re-signed by the daemon (SigV4) — the daemon holds no per-client secrets.
- **Membership / directory**: watches Kubernetes EndpointSlices to discover peers and maintains
  a soft-state chunk directory of who caches what.
- **Peer transport**: gRPC (control + data fallback) and an optional EFA/RDMA one-sided transport
  for the bulk data plane.
- **Build & deploy**: multi-arch (amd64+arm64) container built natively per-arch onto a
  distroless nonroot base; published as an OCI image and Helm chart. Supply-chain gates
  (`cargo-deny`, `clippy -D warnings`, `cargo fmt`, `cargo doc`, and a Trivy CRITICAL/HIGH scan)
  run in CI before any image manifest is stitched.

### Assumptions

| ID | Assumption | Comments |
|----|------------|----------|
| A-01 | PACER runs inside a single trusted Kubernetes cluster; the cluster network is a shared trust domain for peer traffic. | The peer gRPC and RDMA planes are unauthenticated by design; their security rests on cluster network reachability, not on in-band credentials. |
| A-02 | The operator runs a CNI that **enforces** NetworkPolicy, has enforcement switched on, and has narrowed `networkPolicy.clients` from its shipped cluster-wide default. Node↔node controls (security groups) remain the operator's. | PACER's client-side authorization model is "network reachability = authorization." The chart now ships the policy (M-001), so the assumption has moved from *supplying* it to *enforcing* it — a distinction with teeth: a NetworkPolicy is accepted by every API server and acted on only by a policy-enforcing CNI, and the AWS VPC CNI ships enforcement disabled. An unenforced policy leaves reachability exactly as open as no policy while appearing mitigated. See T-001 and T-004. |
| A-03 | Client pods sharing a node are not mutually hostile at the S3-access level. | All client traffic on a node is served under one node IAM identity; PACER does not isolate one client pod's S3 access from another's. Multi-tenant nodes are out of scope for per-caller authZ. |
| A-04 | S3 backend traffic uses TLS 1.2+ and SigV4 (AWS SDK defaults). | The daemon→S3 hop is the only encrypted-and-authenticated plane. |
| A-05 | Dev mode (`devMode.enabled`) is never enabled in a real deployment. | Dev mode adds a shell, a hot-swap supervisor, and a host `/etc/pki` mount — a large added attack surface documented as test-only. |
| A-06 | Nodes use EKS Pod Identity, not long-lived static credentials. | No static AWS keys are mounted or baked into images. |

### References

- Code repo: this repository (`s3-pacer`) — see [README.md](README.md) and
  [deploy/helm/pacer/](deploy/helm/pacer/).
- Architecture: [deploy/helm/pacer/templates/](deploy/helm/pacer/templates/),
  [crates/](crates/) (`pacer-daemon`, `pacer-cache`, `pacer-backend`, `pacer-ring`,
  `pacer-transport`, `pacer-proto`).
- CI/CD pipeline reference: ci/README.md.
- Threat-model tooling: AWS Threat Modeling MCP server (STRIDE, 9-phase).

---

## Solution Architecture

### Architecture Diagram

```
                         AWS account boundary
                    ┌───────────────────────────────┐
   ┌──────────┐     │  S3 (Express One Zone same-AZ  │
   │  S3 SDK   │     │  and/or Standard regional)     │
   │ (weights) │     └───────────────▲───────────────┘
   └────┬──────┘                     │ TLS + SigV4 (node IAM identity)
        │                            │ [Flow 2]
        │ S3 REST, plaintext         │
        │ placeholder creds          │
        │ [Flow 1]                   │
   ┌────▼───────────────────────────┴───────────────────────────────┐
   │  NODE A (Kubernetes)                                            │
   │  ┌──────────────┐   same-node only          ┌────────────────┐ │
   │  │ client pod   │──(internalTrafficPolicy)──▶│ pacer-daemon    │ │
   │  └──────────────┘   :9000 S3 proxy           │  (nonroot,      │ │
   │                                              │   distroless)   │ │
   │  hostPath NVMe  ◀────cache RAM+NVMe──────────│  :9090 admin    │ │
   │  /mnt/.../pacer-cache                        │  :9100 peer gRPC│ │
   └──────────────────────────────────────────────┴───────▲────────┘ │
   └────────────────────────────────────────────────────── │ ────────┘
              peer gRPC :9100 (plaintext, unauthenticated)  │ [Flow 3]
              + EFA/RDMA one-sided WRITE (unauthenticated)   │ [Flow 4]
   ┌────────────────────────────────────────────────────── ▼ ────────┐
   │  NODE B ... pacer-daemon (peer)                                  │
   └──────────────────────────────────────────────────────────────────┘

   Kubernetes API ◀── watch EndpointSlices (list/watch, namespace-scoped) [Flow 5]
```

All three daemon listeners bind `0.0.0.0`. Only the outbound S3 hop is encrypted and
authenticated. Peer gRPC and the RDMA data plane are plaintext and unauthenticated; the
S3 proxy uses a **placeholder** SigV4 credential (a well-known, non-secret key pair used only
to reject malformed requests) — effective authorization on the front end is *network
reachability*.

### Main Functionality / Use Cases

1. **Same-node read-through cache**: a client pod issues normal S3 GETs to the local daemon;
   on a miss the daemon fetches from S3, streams to the client while populating the cache;
   subsequent reads on that node (and its peers) are served from cache.
2. **Peer serving**: on a local miss, the daemon consults the chunk directory and fetches the
   object from a peer that already holds it (over RDMA when available, else gRPC), avoiding a
   redundant S3 GET.
3. **Write pass-through**: PUT/DELETE/multipart bypass the cache and go straight to S3 for
   read-after-write correctness.
4. **Bucket aliasing**: clients address buckets by an operator-configured **alias**; the daemon
   rewrites the alias to the real backend bucket name. Real S3 Express bucket names
   (`*--x-s3`) are never exposed to clients (exposing them changes SDK behavior and breaks the
   proxy).

### Assets / Dependencies

| Asset | Usage | Data type | Comments |
|-------|-------|-----------|----------|
| Cached object bytes (model weights/checkpoints) | Served from RAM+NVMe on hits | Confidential (operator data) | Held in node RAM and on a hostPath NVMe volume shared with kubelet/containerd; blast radius of the cache dir is scoped to a subdirectory. |
| Real backend bucket names (`*--x-s3`) | Rewritten from client-facing aliases | Internal | Must never be exposed to clients (bucket-aliasing invariant). |
| Node IAM identity (EKS Pod Identity) | Signs S3 requests | Credential (Restricted) | Least-privilege: policy scoped to `s3express:CreateSession` on the bucket ARN. The daemon is a deliberate confused deputy for any pod that can reach it. |
| Placeholder S3 credential (`pacer`/`pacer`) | Front-end request shape validation | Public (not a secret) | Documented as non-secret; not an authorization control. |
| Pinned/registered RDMA memory (default 2 pools × 64 × 64 MiB = 8 GiB) | One-sided RDMA WRITE targets | Restricted | Registered remotely-writable via `rkey`; addressed by peers over the unauthenticated handshake. |
| Chunk directory / membership soft-state | Routes fetches to holders | Internal | Peers self-assert node IDs; directory entries are accepted without verification. |
| Container images (OCI) + Helm chart | Deploy artifact | Internal/Public | Distroless nonroot; Trivy-gated in CI; published to a public registry. |
| CI identity | ECR push / release | Credential | Pod-identity role; no static secrets in CI config. |

---

## Threats & Mitigations

### Threat Actors

| ID | Actor | Capability | Relevance to PACER |
|----|-------|-----------|--------------------|
| TA1 | Co-located / in-cluster workload (a pod on the same node or elsewhere in the cluster) | Medium | **Highest priority.** Can reach the daemon's listeners over the pod network. This is the primary realistic adversary given "network reachability = authorization." |
| TA2 | Compromised peer daemon (a node/pod that has joined or can impersonate the peer plane) | Medium–High | Peer plane is unauthenticated; a node that can reach :9100 / the RDMA fabric is fully trusted. |
| TA3 | Cluster operator / privileged K8s user | High | Owns the node, hostPath, and IAM scoping. Largely trusted; relevant for accidental misconfiguration (dev mode, over-broad IAM, missing NetworkPolicy). |
| TA4 | Supply-chain attacker (dependency, base image, or CI) | Medium | Relevant to the build/publish path. |
| TA5 | External / internet attacker | Low | Not directly relevant: no daemon listener is internet-exposed by design; reachable only through a cluster foothold (then becomes TA1/TA2). |

Excluded / de-scoped: nation-state and organized-crime actors are not separately modeled — their
techniques against this component reduce to the TA1/TA2/TA4 vectors above. External attackers
(TA5) are in scope only as a precondition (they must first become an in-cluster actor).

### Threat & Mitigation Detail

STRIDE threats, prioritized. Grammar: *[source] with [prerequisite] can [action] resulting in
[impact]*. IDs map to mitigations in Appendix B.

| # | Priority | Threat | STRIDE | Affected assets | Mitigations | Decision |
|---|----------|--------|--------|-----------------|-------------|----------|
| T-001 | **High** | An in-cluster pod (TA1) with network reachability to the S3 proxy (:9000) can issue arbitrary S3 operations, which the daemon re-signs with the node IAM identity — obtaining S3 access it was never granted (confused deputy). A NetworkPolicy now ships to constrain reachability, but its default admits the whole cluster and it binds only where the CNI enforces it. | Elevation of Privilege | Node IAM identity, cached bytes | M-001, M-002, M-009 | Mitigate |
| T-002 | **High** | A pod that can reach the peer gRPC plane (:9100, TA1/TA2) can call `FetchBlob`/`LookupSharers` with a self-asserted `requester_node_id` and exfiltrate any cached object bytes; there is no authentication, token, or mTLS. The shipped policy restricts :9100 to the release's own daemon pods, which is a reachability bound, not authentication — any compromised daemon pod still passes it. | Information Disclosure / Spoofing | Cached bytes, chunk directory | M-001, M-002, M-003, M-009 | Mitigate |
| T-003 | **High** | A peer (TA2) can call `Invalidate`/`Announce` on the unauthenticated peer plane to evict live entries or inject false directory state, poisoning cache routing and forcing S3 refetch storms or serving of attacker-chosen holders. | Tampering / Spoofing | Chunk directory, cache | M-001, M-002, M-003, M-004 | Mitigate |
| T-004 | **High** | The chart now ships the NetworkPolicy ADR-0006 makes the linchpin (M-001), so the threat has moved from absence to **inert presence**: the object is accepted by any API server and enforced only by a policy-enforcing CNI — the AWS VPC CNI ships its agent with `--enable-network-policy=false` — and its `networkPolicy.clients` default admits every pod in the cluster. Either condition leaves :9000 reachable cluster-wide while `kubectl get netpol` shows a policy in place, so the gap now also carries **false assurance**: it can retire the follow-up that would have closed it. | Elevation of Privilege | All | M-001, M-002, M-020 | Mitigate |
| T-005 | Medium | A peer holding a blob writes it via one-sided RDMA into the requester's registered buffer; the RDMA plane has no authentication or encryption, so a malicious peer on the fabric could target or read remotely-registered memory (`rkey`) beyond the intended transfer. | Tampering / Information Disclosure | Pinned RDMA memory, cached bytes | M-003, M-005 | Mitigate |
| T-006 | Medium | Object bytes cross node↔node (gRPC and RDMA) in plaintext; anyone able to observe the intra-cluster fabric (TA1/TA2) can read cached content in transit. | Information Disclosure | Cached bytes | M-003, M-006 | Mitigate |
| T-007 | Medium | Cached objects are keyed with a replayed backend ETag but the serve path performs no content-integrity verification, so a tampered peer/cache entry (T-003/T-005) would be served to a client as authentic object bytes. | Tampering | Cached bytes | M-004, M-007 | Mitigate |
| T-008 | Medium | Any pod reaching the admin endpoint (:9090) reads `/metrics` with no authentication, disclosing operational detail (keys/hit patterns/topology) useful for targeting. `/healthz`/`/readyz` are also unauthenticated. The shipped policy scopes :9090 to `networkPolicy.metrics` plus `probeCidrs`, but the latter defaults to `0.0.0.0/0` because kubelet probes arrive from the node address and match no selector — so out of the box this port is the least constrained of the three. | Information Disclosure | Operational metadata | M-001, M-002, M-008 | Mitigate |
| T-009 | Medium | An in-cluster caller (TA1) floods the S3 proxy or peer plane (large objects, high fan-in) to exhaust the cache, pinned RDMA pool slots, NVMe, or node memory — degrading service for co-located workloads or triggering OOMKill. | Denial of Service | Cache, RDMA pool, node memory | M-010, M-011 | Mitigate |
| T-010 | Medium | A caller passes (or the operator misconfigures) a real `*--x-s3` bucket name to a client, flipping SDK behavior and breaking the proxy or leaking real backend bucket identity. | Information Disclosure / Tampering | Real bucket names | M-012 | Mitigate |
| T-011 | Medium | Enabling dev mode in a real deployment adds a shell, a binary hot-swap supervisor, and a host `/etc/pki` mount to the pod, greatly enlarging the attack surface and enabling code substitution. | Elevation of Privilege / Tampering | Daemon binary, node | M-013 | Mitigate |
| T-012 | Low | A poisoned dependency, git-pinned crate, or base image (TA4) introduces malicious code into the daemon. | Tampering | Image, daemon | M-014, M-015 | Mitigate |
| T-013 | Low | The one-way OSS publish leaks internal identifiers (private design prose, real account IDs) into the public mirror. | Information Disclosure | Repo metadata | M-016 | Mitigate |
| T-014 | Low | No audit trail ties a specific S3 operation back to the originating client pod (all traffic is one node identity), so misuse via the daemon cannot be attributed. | Repudiation | Access logs | M-008, M-017 | Mitigate / Accept |
| T-015 | Low | The init container `chown`s the hostPath cache dir as root; a path or symlink issue on the shared NVMe device could affect other node consumers (kubelet/containerd/logs). | Tampering / Elevation of Privilege | Node NVMe | M-018 | Mitigate |

**Did we do a good job? (Question 4)** — Coverage spans all six STRIDE categories against every
listener, the peer/RDMA data plane, the cache store, and the build/publish path. The dominant
finding is architectural and deliberate: PACER's authorization model is *network reachability*,
and that model is only sound when the operator (or the chart) supplies network controls. The
chart now ships the NetworkPolicy ADR-0006 treats as the linchpin (**M-001**), which closes
the absence but not the exposure: the remaining gap is **enforcement and default width**
(**T-004**) — a policy binds only on a CNI configured to enforce it, and `networkPolicy.clients`
ships cluster-wide so that upgrades cannot silently stop serving. Those are operator
preconditions the chart can only announce (**M-020**), not guarantee, which makes them the
top item to verify per deployment rather than a code remediation. The next material *code*
remediation is peer-plane authentication (**M-003**) with directory-message origin validation
(**M-004**): the policy bounds who can reach :9100, and only authentication can bound who can
be believed on it.

---

## APPENDIX A — Interfaces

| Listener | Port | Protocol | Bind | AuthN | AuthZ | TLS | Callable from | Notes |
|----------|------|----------|------|-------|-------|-----|---------------|-------|
| S3 proxy | 9000 | HTTP/1.1 S3 REST | `0.0.0.0` | Placeholder SigV4 (non-secret) | Network reachability | No | `networkPolicy.clients` — **cluster-wide by default**. `internalTrafficPolicy: Local` constrains Service-VIP routing to the local node but not pod-IP dialing | Effective authZ is network-level only, and only where the CNI enforces policy. |
| Peer gRPC | 9100 | gRPC / HTTP2 | `0.0.0.0` | **None** | **None** | No | This release's own daemon pods, by shipped policy (not configurable); dialed by pod IP | `requester_node_id` self-asserted; `FetchBlob`/`Invalidate`/`Announce`/`LookupSharers`/`Handshake`. The tightest of the three by default — but reachability only, so a compromised daemon pod is still believed. |
| Admin / metrics | 9090 | HTTP | `0.0.0.0` | **None** | **None** | No | `networkPolicy.probeCidrs` (**`0.0.0.0/0`** by default — kubelet probes come from the node address and match no selector) + `networkPolicy.metrics` | `/healthz`, `/readyz`, `/metrics`. Loosest of the three by default. |
| EFA/RDMA data plane | (device, not IP) | One-sided RDMA WRITE over SRD | EFA NIC | **None** | **None** | No (unencrypted) | Negotiated peers | Addressing via symmetric AH exchanged on the unauthenticated gRPC handshake. |
| S3 backend (outbound) | 443 | HTTPS | — | SigV4 (node IAM) | IAM policy | **Yes** | AWS S3 | The only encrypted+authenticated plane. |

## APPENDIX B — Mitigations

| # | Mitigation | Type | Threats | Status |
|---|-----------|------|---------|--------|
| M-001 | **Ship a NetworkPolicy in the Helm chart** (`deploy/helm/pacer/templates/networkpolicy.yaml`, `networkPolicy.enabled: true`) scoping :9100 to the release's own daemon pods, :9090 to `probeCidrs` + `networkPolicy.metrics`, and :9000 to `networkPolicy.clients`. | Preventive | T-001, T-002, T-003, T-004, T-008 | **In place and empirically verified** on an enforcing cluster (VPC CNI v1.23.0 + policy agent v1.4.1, 2026-09-06): against four throwaway pods, the agent programmed a `PolicyEndpoint` from the rendered object and all ten probes matched expectation — an admitted client reached :9000 while a same-namespace pod differing only by label timed out, and the port scoping held in both directions (the :9000-admitted client was denied on :9100, the peer-labelled pod denied on :9000). A **control run with no policy applied** reached every port first, so the denials are the policy and not an unrelated failure. That is a test of enforcement and selector semantics, not of the shipped *defaults*, which admit the cluster on :9000 by design. **With two documented limits.** (a) `networkPolicy.clients` **defaults to cluster-wide** (`namespaceSelector: {}`) so that upgrading into a running install cannot silently stop serving it — :9000 is only genuinely scoped once an operator narrows it; :9100 is tight by default. (b) It restricts *reachability*, not identity, and does not cover the EFA/RDMA plane (M-006) — see the two limits below. Ingress-only: an egress rule set would have to enumerate S3, kube-dns, the API server and every peer, and a wrong entry turns a policy denial into an unexplained slow GET. |
| M-002 | Operator-supplied network controls (NetworkPolicy / security groups) scoping every daemon listener; document reachability as the authZ boundary. | Preventive | T-001, T-002, T-003, T-004, T-008 | Partial, and now narrower in scope since M-001 ships the pod-level policy: `internalTrafficPolicy: Local` on the S3 Service constrains Service-VIP routing to the local node (**not** a same-node authorization boundary — an admitted pod on any node can dial a daemon pod IP directly, and NetworkPolicy has no node-topology selector, so "same node" is not expressible at all under the VPC CNI's shared-subnet pod IPs). Node↔node and RDMA-fabric scoping remain the operator's, via security groups. |
| M-003 | Authenticate and (optionally) encrypt the peer plane — mTLS or a shared cluster token on gRPC, with the RDMA handshake bound to an authenticated identity. | Preventive | T-002, T-003, T-005, T-006 | Not implemented — peer plane is plaintext/unauthenticated by design. Candidate hardening. |
| M-004 | Validate directory/invalidate messages against membership (only accept `Announce`/`Invalidate` from peers currently in the EndpointSlice set). | Preventive | T-003, T-007 | Membership is watched; message-origin validation is a hardening candidate. |
| M-005 | Bound RDMA registrations to per-transfer regions and least-privilege `rkey` lifetime; no atomics registered (EFA has none). | Preventive | T-005 | Registrations are pool-scoped with `LOCAL_WRITE|REMOTE_READ|REMOTE_WRITE`; tightening lifetime is a candidate. |
| M-006 | Rely on placement-group / security-group isolation of the RDMA fabric; consider encrypted transport where the fabric is not exclusively PACER's. | Deterrent/Preventive | T-006 | In place operationally (self-referencing SG / placement group); not cryptographic. |
| M-007 | Verify cached content integrity on serve (e.g. checksum/ETag validation) before returning peer- or cache-sourced bytes to a client. | Detective/Preventive | T-007 | Not implemented — only ETag replay today. Hardening candidate. |
| M-008 | Restrict `/metrics` and health endpoints via network policy; avoid emitting sensitive keys in metric labels. | Preventive | T-008, T-014 | Partial (network-level only), and the weakest of the three ports: the shipped policy scopes :9090 to `networkPolicy.metrics`, but `probeCidrs` must also admit the kubelet, whose probes come from the node address and match no pod/namespace selector. It therefore defaults to `0.0.0.0/0` — emptying it fails the startupProbe and CrashLoops the DaemonSet with nothing naming the policy, so the chart refuses an empty value and the guidance is to narrow it to the node subnets. |
| M-009 | Least-privilege node IAM (single `s3express:CreateSession` action scoped to the bucket ARN) caps the blast radius of a confused-deputy call. | Preventive | T-001, T-002 | **In place.** |
| M-010 | Serve-admission semaphore bounding concurrent holder serves to the RDMA pool slot count; `fetch_parallelism`, `rpc_timeout`, `rpc_retries` bound peer fan-in. | Preventive | T-009 | **In place.** |
| M-011 | Container memory limit sized to include pinned RDMA pools to avoid OOMKill; NVMe/RAM cache capacities are configurable ceilings. | Preventive | T-009 | **In place.** |
| M-012 | Bucket aliasing: clients address aliases only; daemon rewrites to real backend names; real `*--x-s3` names never exposed. | Preventive | T-010 | **In place** (config `bucketMap`); operator must configure correctly. |
| M-013 | Dev mode disabled by default and documented "never enable in a real deployment." | Deterrent | T-011 | **In place** (default off). |
| M-014 | `cargo-deny` (RustSec advisories fail CI, permissive-license allowlist, deny unknown registry/git, deny wildcards); AWS SDK legacy TLS stack disabled. | Preventive/Detective | T-012 | **In place.** |
| M-015 | Distroless nonroot base (no shell/RUN), COPY-only runtime image, Trivy CRITICAL/HIGH scan gate before manifest stitch, native per-arch build (no QEMU). | Preventive | T-012 | **In place.** |
| M-016 | OSS leak gate (hard-fail on internal codenames; warn on non-placeholder account IDs) + path-exclusion publish + squashed one-way mirror history. | Detective/Preventive | T-013 | **In place.** |
| M-017 | Emit per-request logs at the proxy for operational traceability (best-effort attribution within the single-identity model). | Detective | T-014 | Partial; per-caller attribution is out of scope (A-03). |
| M-018 | hostPath scoped to a dedicated subdirectory (not the NVMe array root) to contain blast radius; init `chown` limited to that path. | Preventive | T-015 | **In place.** |
| M-019 | Every GitHub Actions `uses:` pinned to an immutable **commit** SHA, with a comment naming the exact release; `scripts/ci/verify-action-pins` re-resolves each pin upstream and fails on a moved tag, a vague comment, or a tag-object pin. | Preventive/Detective | T-012 | **In place.** A fork should re-run the verifier after any bump — it needs only `git` and github.com, no Dependabot or Renovate. |
| M-020 | **Counter M-001's false assurance at install time.** `helm install`/`upgrade` prints the CNI-enforcement check (`templates/NOTES.txt`) and, when `networkPolicy.clients` still admits the whole cluster, says so explicitly rather than reporting success; render-time `fail`s (`pacer.validateNetworkPolicy`) reject an empty `clients` or `probeCidrs`, since an empty peer list silently means *deny* — an inversion that surfaces as client timeouts or a CrashLoop, not as a config error. README states enforcement as a **requirement**, with the authoritative addon query, the live-pod confirmation, and the command to fix it. | Detective/Deterrent | T-004 | **In place.** Detective only: nothing in the chart can make a non-enforcing CNI enforce, so this converts a silent gap into a loud one and no further. Note the check itself has a trap worth keeping in the docs — the `aws-node` **DaemonSet template can disagree with its own running pods**, so reading the template reports "disabled" on a cluster that is enforcing (observed on this project's own test cluster), and `NETWORK_POLICY_ENFORCING_MODE` is not the switch though it reads like one. Ask the addon, or a live pod. |

### Residual risk

- **Accepted by design:** per-caller authorization and cross-client isolation on a shared node
  (T-014, A-03) — all node traffic acts as one IAM identity; mitigated only by least-privilege
  IAM (M-009) and operator network controls. Peer-plane plaintext/unauthenticated transport
  (T-002/T-003/T-006) is an explicit architectural choice premised on a trusted cluster network
  (A-01); it remains a real residual risk anywhere that premise is weak and is the strongest
  candidate for future hardening (M-003).
- **Shipped, but conditional on the operator (T-001/T-004):** **M-001** now ships the
  NetworkPolicy the design assumes, so the model no longer depends on the operator *writing*
  one. It still depends on two things the chart cannot do for them: a CNI that **enforces**
  NetworkPolicy with enforcement switched on (the AWS VPC CNI ships it off, and an unenforced
  policy is indistinguishable from a mitigated one at the API server), and narrowing
  `networkPolicy.clients` from its cluster-wide default. Until both hold, :9000 is reachable
  by any in-cluster pod and spends the node's IAM identity. **M-020** makes each condition
  loud at install time — it cannot make either true. `:9100` is the one port scoped tightly
  by default; `:9090` is the loosest, since kubelet probes force a permissive `probeCidrs`.
- **Hardening candidates (not yet implemented):** peer-plane authN/encryption (M-003),
  directory-message origin validation (M-004), serve-path content-integrity verification (M-007).
- **For an adopter who forks this repo (T-012):** the action pins (M-019) are the one
  supply-chain control that decays on its own — upstream ships releases whether or not
  anyone here bumps them, and a fork inherits whatever SHAs were current at fork time.
  Re-run `scripts/ci/verify-action-pins` after any bump, and enable Dependabot (or an
  equivalent) if you would rather be told an upgrade exists than discover it from a
  scan finding. A pin left alone is *safe* but *stale*: it cannot be retargeted at
  malicious code, and it also cannot pick up the fix for a vulnerability in the action.
