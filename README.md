# S3 PACER

**S3 Peer-Accelerated Cache with EFA Replication** — a node-local, S3-compatible caching
proxy for EKS, built in Rust, deployed as a DaemonSet, backed by Amazon S3 — by default an
**S3 Express One Zone** directory bucket in the same AZ as the nodepool.

Every node runs a node-local, S3-compatible caching proxy. Unmodified S3 clients
(boto3, AWS CLI, `s5cmd`, PyTorch dataloaders) point at the node endpoint; hot objects are
served from local NVMe/RAM at line rate, misses stream from S3 while filling the cache, and
— in a cluster — a miss can be served from a peer node that already holds the object,
over **EFA RDMA** (with automatic **gRPC/TCP fallback**) instead of re-fetching from S3.

Writes are never cached: they proxy straight through to the backing bucket, which yields
read-after-write consistency with no invalidation protocol.

```
Pod (boto3/CLI/s5cmd/dataloader) ──HTTP/S3──► pacer-daemon (node-local, :9000)
                                               │  hit:  RAM tier, then NVMe — chunked,
                                               │        RAM chunks live in a registered
                                               │        hugepage slab, so a peer's WRITE
                                               │        is posted straight out of cache
                                               │  miss: rendezvous ring — which nodes
                                               │        home this chunk?
                                               ├──► peer ── one-sided EFA RDMA WRITE ─┐
                                               │            gRPC/TCP control + data   ┘ :9100
                                               ▼
                                       Amazon S3 — Express One Zone (same-AZ directory
                                       bucket) by default, or Standard
                                               ▲
              PUT / DELETE / multipart ────────┘  (never cached → read-after-write)
```

Opt-in, per request: the bytes land in memory the **client** owns and the response carries
no body at all — so the socket copy, and the daemon's copy-out, both leave the data path.

```
GET /bucket/key
  x-pacer-target: shm:/loader-7;offset=0x40000;len=16777216
        ── or ──  nic:0x7f2a40000000;len=1073741824;rails=<gid>/<qpn>/<rkey>[,…]
                  ↑ memory the client registered on its OWN NIC: host RAM or GPU HBM,
                    named per rail, so the daemon needs no CUDA and never maps it

200 OK
  Content-Length: 0            ← the object is already in the client's window
  x-pacer-delivered: 16777216  ← presence IS the completion signal
  x-pacer-checksum: crc32=…    ← over the delivered bytes (`checksum=none` opts out)

  remote chunk ──one-sided EFA RDMA WRITE──►  the client's window
  local hit    ──single copy───────────────►  the client's window
```

## Why it exists

GPU time is gated on how fast bytes reach the node, and the S3 read path has two hard
ceilings: the backend itself, and the instance NIC that every remote byte must cross.
PACER exists to serve reads past both:

- **Warm reads at memory speed, not NIC speed.** A hot object is served from node-local
  RAM/NVMe without touching the network — measured at several times the instance's NIC
  line rate, with single-digit-millisecond TTFB — and the NIC stays free for training
  traffic.
- **Read bandwidth that scales with the cluster.** A local miss is served by the peer
  that already holds the object, over one-sided EFA RDMA WRITE at NIC line rate with the
  holder's CPU off the data path (automatic gRPC fallback). Every node serves
  concurrently, so a restore storm — the whole cluster pulling the same checkpoint at
  once — fans out across peers instead of funneling through S3: a real Llama-3.1-8B
  restore pulled its checkpoint **6.1× faster** than reading S3 directly, and a
  Llama-3.1-70B one **5.2× faster** (internal benchmark, 2026-08-21: 2-node cluster of
  1-GPU nodes against S3 Standard, warm cache; 14.96 GiB checkpoint at 5.41 vs 0.88 GiB/s,
  131.42 GiB at 4.81 vs 0.93 GiB/s. Byte-integrity asserted — every shard's digest matched
  a read that bypassed the cache entirely).

  End-to-end restore wall-clock improves less than the fetch does, and that is worth
  stating plainly: 1.5× on the 70B, because roughly 60% of a restore is the loader's own
  CPU-bound `safetensors` deserialize, which no cache can shorten. The cache's win is the
  segment it owns. On a **cold** cache it is S3 parity by construction, since it is reading
  S3 — the gain is entirely on warm and peer reads.

Cost falls out as a side effect, not the goal: every byte served from RAM, NVMe, or a
peer is a byte not re-fetched from S3 — no per-GB retrieval charge on epoch re-reads,
and peer traffic stays on the free intra-AZ fabric.

## Status

**Beta / pre-1.0.** Tagged releases (`vX.Y.Z`) publish a multi-arch (amd64 + arm64,
EFA-capable) image and an OCI Helm chart to GHCR — see
[Releases](https://github.com/aws-samples/sample-s3-pacer/releases). If no release is
visible yet, install from source (below).

This project is provided as-is, without warranty of any kind. It has not undergone the
testing, hardening, and operational review a production deployment requires — evaluate
it in a non-production environment and review the capability table below before relying
on it for production traffic.

| Capability | State |
|---|---|
| Single-node cache (foyer RAM+NVMe, serve-while-fill, write-through, strip-and-re-sign auth) | Shipped |
| Cluster tier over gRPC (rendezvous-hash ring, EndpointSlice membership, owner read-through, cross-node read-after-write) | Shipped |
| Chunked caching + sharded chunk directory + multi-copy replication | Shipped |
| EFA-RDMA peer transport (one-sided WRITE, gRPC fallback), behind the `efa` build feature | Built + hardware-validated; **beta**, off by default, hardware-gated |
| Real-checkpoint GPU restore benchmark (8B + 70B, Express and Standard, byte-integrity asserted) | Measured on hardware |
| Delivery into client-supplied host memory (`x-pacer-target: shm:`, header-only 200) | Built + measured; **beta**, off by default |
| Delivery into memory the client registered on its own NIC (`nic:` token), including GPU HBM | Proven end-to-end on one p5 — an S3 object written into a client-registered H100 window, both arms byte-verified; **beta** |
| Restore-storm benchmark at scale; multi-rail aggregation for a single reader | In flight / planned |
| Pre-stage Job; S3-Standard→Express two-tier read-through; Append/Rename passthrough | Planned — **not built** |

`s3s` (the S3 front-end crate) is pre-1.0; it is the main dependency risk of the stack.

## Requirements

- An **EKS cluster** (Kubernetes ≥ 1.26 — `internalTrafficPolicy: Local` GA).
- A **CNI that enforces NetworkPolicy**, and confirmation that it is switched on. The chart
  ships the policy that scopes who may spend the node's IAM identity, but nothing enforces
  it on a cluster whose CNI does not — and on EKS the VPC CNI's policy agent ships
  disabled. See [Securing access](#securing-access--reachability-is-authorization); this is
  a prerequisite for the security model, not a hardening step.
- An **S3 Express One Zone directory bucket** whose AZ **ID** matches the cache nodepool's
  AZ, plus a Gateway VPC endpoint `com.amazonaws.<region>.s3express` on the cluster VPC
  (PrivateLink is not supported for Express).
- **Node IAM via EKS Pod Identity** granting `s3express:CreateSession` on the bucket ARN,
  associated with the chart's ServiceAccount. The daemon holds the node identity and
  re-signs client requests outbound; it stores no per-client secrets.
- The **cache nodepool** should be single-AZ (pinned by AZ ID), with instance-store NVMe
  (RAID0) for the disk tier, and Nitro v4+ if you want the EFA path. The chart can render a
  Karpenter EC2NodeClass/NodePool that encodes these invariants (`karpenter.enabled`), or
  you can bring your own nodepool and label it to match `nodeSelector`
  (default `pacer.io/nodepool: cache`).

## Quick start

**From a tagged release** — the chart defaults to the matching GHCR image, so this is
the whole install (substitute the released version):

```bash
# Point Helm at a bucket alias + your S3 Express zonal endpoint and region.
helm install pacer oci://ghcr.io/aws-samples/sample-s3-pacer/charts/pacer --version 0.1.0 \
  --namespace pacer --create-namespace \
  --set 'config.bucketMap.cache=<your-bucket>--use1-az4--x-s3' \
  --set config.s3Endpoint=https://s3express-use1-az4.us-east-1.amazonaws.com \
  --set config.awsRegion=us-east-1

# Then grant the daemon its backend identity with an EKS Pod Identity association on the
# ServiceAccount the chart created (no chart value, no annotation — the association is
# cluster-side, and the Pod Identity Agent add-on must be installed).
aws eks create-pod-identity-association \
  --cluster-name <cluster> --namespace pacer --service-account pacer \
  --role-arn arn:aws:iam::<acct>:role/pacer-daemon
```

**From source** — the Helm chart is self-contained in this repo:

```bash
helm install pacer deploy/helm/pacer \
  --namespace pacer --create-namespace \
  --set 'config.bucketMap.cache=<your-bucket>--use1-az4--x-s3' \
  --set config.s3Endpoint=https://s3express-use1-az4.us-east-1.amazonaws.com \
  --set config.awsRegion=us-east-1
```

If you build and push the image + chart to your own registry, the same release installs
from the OCI chart (substitute your account/region):

```bash
helm install pacer \
  oci://<account>.dkr.ecr.<region>.amazonaws.com/s3-pacer/charts/pacer \
  --namespace pacer --create-namespace \
  --set 'config.bucketMap.cache=<your-bucket>--use1-az4--x-s3' \
  --set config.s3Endpoint=https://s3express-use1-az4.us-east-1.amazonaws.com \
  --set config.awsRegion=us-east-1
```

Every `--set` above maps to a documented key in
[deploy/helm/pacer/values.yaml](deploy/helm/pacer/values.yaml), which carries one short
comment per key (what it is, its unit, its default) and is validated by
[values.schema.json](deploy/helm/pacer/values.schema.json), so a typo fails the render
rather than being silently ignored. **The design notes — why each default is what it is,
the measurements behind it, and the failure it exists to prevent — are in
[docs/helm/](docs/helm/README.md).** Start there for cache sizing (and read
[docs/helm/memory-model.md](docs/helm/memory-model.md) before raising
`config.memCapacity`), chunk size, replication factor, EFA, delivery, the write scatter and
Karpenter. If a daemon is being OOMKilled, go straight to
[docs/runbooks/daemon-oom.md](docs/runbooks/daemon-oom.md).

### Enabling the EFA RDMA peer transport (optional, beta)

RDMA is **off in the base chart** because requesting an EFA device makes the pod
unschedulable on non-EFA nodes. On a Nitro v4+ EFA nodepool with the
`aws-efa-k8s-device-plugin` installed, set `efa.enabled: true` — the block is commented
at the end of [values-example.yaml](deploy/helm/pacer/values-example.yaml), with its
prerequisites, ready to uncomment. Note the image must be built `--features efa` (the
published release image is; a plain `Dockerfile` build is not).
The daemon probes for EFA at startup and speaks gRPC-only where it is absent, and
every RDMA error falls back to gRPC per-peer — RDMA is never a hard dependency. Node
prerequisites: the device plugin, a cluster placement group, and a self-referencing
security group.

Two knobs matter on a multi-rail node (p5-class, 32 EFA devices), both defaulting to
sensible values you can leave alone:

| value | what it does |
|---|---|
| `cluster.rdmaArenaBytes` | Registered bytes this node pins to receive peers' WRITEs, node-wide (default 4Gi). Concurrency is `bytes ÷ config.chunkSize`. Raising it must raise `efa.pinnedPoolReservation` in step — the pod's memory limit has to cover pinned bytes the cache's own accounting cannot see. |
| `cluster.rdmaAffinity` | Pin each rail's completion reaper — and register its buffers — on that rail's own NIC NUMA node (default on). Worth **+65 %** on a 2 × p5.48xlarge measurement; `false` reproduces the un-placed behaviour and exists as the control arm. |

The startup log reports what was actually registered — rail count, ranges per rail,
page size, pinned bytes, and how many rails landed node-locally — because a hugepage
request that silently fell back to 4 KiB pages, or placement that could not read the
node's topology, would otherwise be invisible in a throughput number.

### Delivering into a client's own memory (optional, opt-in)

For a client whose bottleneck is copying the response body out of a socket — a
checkpoint loader pulling multi-GiB shards — PACER can deliver an object's bytes
**into memory the client owns** and answer with a header-only 200, removing the
HTTP/TCP hop entirely.

**The wire contract is one request header and two response headers.** A GET carrying
`x-pacer-target` names a window the client owns; the daemon writes the object's bytes
there and answers `200` with `Content-Length: 0`, `x-pacer-delivered: <bytes>` and
`x-pacer-checksum: crc32=<hex>` over what it wrote, so the client verifies without
re-reading. The **absence** of `x-pacer-delivered` is the client's signal to read the
body instead, which is what makes the quota fallback below safe rather than fatal.

Every descriptor is `<scheme>:<handle>;<key>=<value>;…`, sharing `len` (required),
`offset` (default 0, decimal or `0x`-prefixed) and `checksum` (`crc32`, or `none` to
skip a full extra pass over a large window). Two schemes:

| scheme | the handle is | who registers the memory |
|---|---|---|
| `shm:/<name>` | a POSIX shared-memory segment (`shm_open` name) | the daemon maps and pins it |
| `nic:<address>` | the client's own virtual address, plus `rails=<gid>/<qpn>/<rkey>[,…]` | **the client**, on its own NIC |

`nic:` is the one that reaches **GPU memory**, and the reason is a constraint rather
than a preference: on EFA, dma-buf is the only way to register device memory, and only
the owning process can export a dma-buf for its own allocation — so a daemon handed an
IPC handle can never register it. Inverting that (the client registers, the daemon only
WRITEs) means the daemon needs no CUDA at all, and the rail list is per-rail because an
rkey is scoped to the protection domain that issued it. A writer picks the entry whose
rail is PCIe-local to the GPU holding the tensor.

**The client library that drives this is not published yet** — its API is still changing
and will land in its own release. The daemon side is complete, and the contract above is
all a loader needs to speak it.

Measured on one p5 against the same client's own stock GETs in the same session, 4 GiB
objects: **3.7× with the integrity check on, 5.0× with it off** — but **0.97× at 16 MiB**,
where per-request costs dominate. It is worth enabling for large reads (checkpoint
shards), not for small ones. It is off by default (`delivery.enabled`) because the daemon
maps and pins memory a client named. Enabling it does **not** change anything for other clients: the
behaviour is gated strictly on the request header, so a stock boto3/CLI GET against
the same endpoint still gets a byte-identical object body. Both pods must share a
tmpfs — `delivery.sharedShm` mounts the node's `/dev/shm` into the daemon via
`hostPath` (an `emptyDir` is per-pod and cannot be shared) and the client pod needs
the identical mount — and `delivery.pinnedReservation` must cover the pinned client
pages the cache's own accounting cannot see. A target over either quota is served as an
ordinary body instead of failing, so a client that asks for more than the node will pin
still gets its bytes — it just gets them the usual way, and the absent
`x-pacer-delivered` header is how it can tell.

## Securing access — reachability *is* authorization

The daemon strips the caller's signature and re-signs outbound with the **node's** IAM
identity. There is no per-caller credential anywhere in the request path, so **whatever
can reach `:9000` holds that identity's S3 access** — a deliberate confused deputy, the
same shape as `aws-sigv4-proxy`. The peer plane on `:9100` is likewise unauthenticated:
`Announce` and `Invalidate` take no credential and `requester_node_id` is self-asserted.

The chart therefore ships a **NetworkPolicy, enabled by default**
([`networkPolicy` in values.yaml](deploy/helm/pacer/values.yaml), design note in
[docs/helm/network-policy.md](docs/helm/network-policy.md)): `:9100` is restricted to
the release's own daemon pods, `:9090` to the kubelet's probe addresses plus whatever
`networkPolicy.metrics` names, and `:9000` to `networkPolicy.clients`.

### Enforcement is not automatic — check it

**A NetworkPolicy object is accepted by every cluster's API server. Only a
policy-enforcing CNI acts on one.** With the AWS VPC CNI, enforcement lives in a separate
agent and is **opt-in**, so an unconfigured cluster gives you a policy that installs
cleanly, reads correctly in `kubectl get netpol`, and constrains nothing at all. That is
worse than shipping no policy, because it looks like the box is ticked.

The **addon configuration is authoritative** — ask it, not the cluster:

```bash
aws eks describe-addon --cluster-name <cluster> --region <region> \
  --addon-name vpc-cni --query addon.configurationValues
# want: {"enableNetworkPolicy":"true", ...}
```

If it is absent or `"false"`, set it and let the addon roll `aws-node`:

```bash
aws eks update-addon --cluster-name <cluster> --region <region> --addon-name vpc-cni \
  --configuration-values '{"enableNetworkPolicy":"true"}'
```

To confirm what is actually running, read a **live pod** — not the `aws-node` DaemonSet
template, which can disagree with its own pods and is the read most likely to tell you
"disabled" on a cluster that is enforcing:

```bash
kubectl -n kube-system get pods -l k8s-app=aws-node \
  -o jsonpath='{.items[0].spec.containers[?(@.name=="aws-eks-nodeagent")].args}'
# want: --enable-network-policy=true
```

(`NETWORK_POLICY_ENFORCING_MODE` on the `aws-node` container is **not** this switch, and
reads as though it were.) Calico and Cilium enforce by default. Whatever the CNI, confirm
empirically before treating the policy as a control — a `curl` to `:9000` from a pod the
policy does not admit should hang or be refused, not return a bucket listing.

### Then narrow `networkPolicy.clients`

The shipped default is `namespaceSelector: {}` — **every pod in the cluster**. That is
chosen so upgrading into an existing install cannot silently stop serving traffic, not
because it is a useful boundary; enforcing it changes nothing on `:9000`. Name the pods
that should be allowed to spend the node's IAM identity:

```yaml
networkPolicy:
  clients:
    - namespaceSelector:
        matchLabels: {kubernetes.io/metadata.name: my-workload}
      podSelector:
        matchLabels: {app: my-trainer}
  # Probes arrive from the node's own address and match no selector — narrow, don't empty.
  probeCidrs: ["10.0.0.0/16"]
  metrics:
    - namespaceSelector:
        matchLabels: {kubernetes.io/metadata.name: monitoring}
```

Two limits worth knowing before you rely on it:

- **It cannot express "same node."** NetworkPolicy has no node-topology selector, and
  under the VPC CNI pod IPs come from shared subnets, so there is no per-node `ipBlock`
  either. `internalTrafficPolicy: Local` constrains Service-VIP routing only — an admitted
  pod on any node can still dial a daemon pod IP directly. The boundary is "these pods",
  not "these pods, here".
- **It does not cover the EFA/RDMA data plane.** One-sided RDMA over SRD goes
  device-to-device and never reaches the CNI's iptables/eBPF hooks. Scope that with the
  EFA security group and placement group.

Both, plus what remains unmitigated, are tracked in [threat-model.md](threat-model.md).

## Configuring clients

Point any S3 SDK at the node-local Service and sign with the placeholder credentials
(default `pacer`/`pacer` — not a real secret: the daemon only checks this signature to
reject malformed requests, then re-signs each call outbound under its own IAM identity —
which is why the pod↔daemon hop is the authorization boundary; see
[Securing access](#securing-access--reachability-is-authorization)):

```python
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="http://pacer.pacer.svc.cluster.local:9000",  # <release>.<namespace>.svc
    aws_access_key_id="pacer", aws_secret_access_key="pacer",
    region_name="us-east-1",
    config=boto3.session.Config(s3={"addressing_style": "path"}),  # REQUIRED — see below
)
s3.download_file("cache", "checkpoints/model.safetensors", "/tmp/model.safetensors")
```

### CRITICAL: use bucket aliases and path-style addressing

Clients **must not** address a directory bucket by its real `*--x-s3` name. That name
matches the S3 Express naming pattern, which flips every AWS SDK into Express-specific
behavior — zonal DNS, `CreateSession`, and the `s3express` signing scope — all aimed past
the proxy at S3 directly, which the node-local daemon cannot honor (and `s3s` rejects the
`s3express` credential scope). Two rules, both required:

1. **Address buckets by their alias.** Configure `config.bucketMap` in the chart
   (`alias -> real bucket name`, e.g. `cache: <your-bucket>--use1-az4--x-s3`; the env-var form
   is `PACER_BUCKET_MAP=cache=<your-bucket>--use1-az4--x-s3`). Clients then use the *alias*
   (`cache`) as the bucket name; the daemon rewrites it to the real same-AZ directory
   bucket. Never expose a `*--x-s3` name to a client.
2. **Force path-style addressing** (`addressing_style: "path"` in boto3, `--path-style` in
   AWS-CLI-family tools), or use an endpoint that is bucket-subdomain-resolvable.
   Virtual-host addressing puts the bucket in the hostname, which does not resolve against
   the node endpoint.

Both rules are enforced by the daemon's config contract
([config.rs](crates/pacer-daemon/src/config.rs) `bucket_map`).

`AWS_REGION` must be set in-cluster (the IMDS metadata hop limit is 1, so the SDK cannot
auto-discover it from inside a pod) — the chart sets it from `config.awsRegion`.

## Observability

The admin endpoint (`:9090` by default, `ports.admin`) serves `/healthz`, `/readyz`, and
`/metrics` (Prometheus text format — daemon counters plus foyer internals). A ready-to-import
Grafana dashboard, built only from metrics the daemon actually exports, is in
[deploy/grafana/](deploy/grafana/README.md).

## Local development

The quickest way to a working toolchain is a **devcontainer** — "Reopen in Container"
and you have the same image CI builds from, so a green check here means a green check
there. `.devcontainer/` covers the workspace as it builds by default;
`.devcontainer/efa/` adds the EFA userspace, which is what `--features efa` needs to
compile at all. See [CONTRIBUTING.md](CONTRIBUTING.md#development-environment) for what
each one can and cannot reproduce.

Common steps are wrapped in the [Makefile](Makefile) — `make help` lists everything:

```bash
make build      # debug build of the workspace
make test       # all unit/integration tests
make smoke      # boot the debug daemon, probe /healthz like kubelet
make ci         # the full CI gate locally: lint, test, doc, deny, smoke
make image      # self-contained local docker image
```

To iterate against real cache nodes without building images (cross-compile on the Mac,
hot-swap the binary in running pods), see scripts/dev/README.md:

```bash
make dev-up N=1 && make dev-deploy
make push       # edit → push → test, ~40 s per cycle
make dev-down
```

## Repository map

| Path | Contents |
|---|---|
| [crates/pacer-daemon](crates/pacer-daemon) | The daemon: s3s front, proxy (cached GETs, write-through), strip-and-re-sign auth, admin/metrics endpoint |
| [crates/pacer-cache](crates/pacer-cache) | foyer HybridCache wrapper + cache policy (>4 MiB whole objects, LRU, chunking, range slicing) |
| [crates/pacer-backend](crates/pacer-backend) | aws-sdk-s3 backend client (S3 Express session auth, endpoint config) |
| [crates/pacer-ring](crates/pacer-ring) | Rendezvous hashing (seeded xxh3) + membership (EndpointSlice watch or static list) + chunk directory |
| [crates/pacer-transport](crates/pacer-transport) | `PeerTransport` trait; gRPC impl; EFA-RDMA behind the `efa` feature |
| [crates/pacer-proto](crates/pacer-proto) | Peer protobufs: capability handshake, blob fetch, invalidate, directory RPCs |
| [deploy/helm/pacer](deploy/helm/pacer) | Helm chart: DaemonSet, node-local Service, ServiceAccount (Pod Identity), optional Karpenter nodepool, EFA overlay |
| [deploy/grafana](deploy/grafana) | Grafana dashboard (JSON model) + import notes |
| scripts/dev/ | Local dev loop (cross-compile + hot-swap binary into running pods) |
| [ci/](ci) | CI build scripts: EFA image staging, functional smoke test |

## Roadmap

Beyond the shipped cache and cluster tiers, planned but **not yet built** work includes: a
pre-stage warm-up Job that fills the cache before training pods start, an optional
two-tier read-through (S3 Standard as source of truth → Express fast tier → NVMe) to
mitigate single-AZ blast radius, and directory-bucket Append/RenameObject passthrough.
Do not assume these exist yet.

## CI

CI runs on GitHub Actions ([.github/workflows/ci.yaml](.github/workflows/ci.yaml)):

- **rust** — `cargo fmt --check`, `clippy -D warnings` (default features), `cargo test`;
- **helm** — `helm lint` + `helm template` (base, Karpenter, and EFA overlays);
- **image** — `docker build` of the release image (no push).

The `efa` feature links libibverbs/libfabric and only builds inside the EFA build image
(`ci/Dockerfile.efa-builder`); `--all-features` clippy is validated there and via
`make lint` on an EFA-capable host, not on stock CI runners.

Dependency updates are proposed by Dependabot
([.github/dependabot.yml](.github/dependabot.yml)): the Rust workspace and `Cargo.lock`
weekly, the workflows' commit-pinned actions weekly, and the Dockerfile base images
monthly — each grouped into one pull request rather than one per dependency.

Releases are cut by pushing a `vX.Y.Z` tag
([.github/workflows/release.yaml](.github/workflows/release.yaml)): per-arch native
builds (`--features efa`, tested in the EFA build image, incl. the native-arm64 test
pass), a Trivy scan gate, then the multi-arch image manifest and the OCI Helm chart are
published to GHCR.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

## Security

If you discover a potential security issue in this project, please notify AWS/Amazon
Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/)
rather than opening a public GitHub issue — see
[CONTRIBUTING.md](CONTRIBUTING.md#security-issue-notifications).

The trust boundaries this daemon assumes, and what each one does and does not protect
against, are written up in [threat-model.md](threat-model.md); the controls an operator
has to set are in [Securing access](#securing-access--reachability-is-authorization)
above.

## License

Licensed under [MIT-0](LICENSE) (MIT No Attribution). Copyright Amazon.com, Inc. or its
affiliates.
