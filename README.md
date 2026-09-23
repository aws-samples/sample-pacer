# PACER

> **Disclaimer** — an [AWS Samples](https://github.com/aws-samples) project: a sample of a
> pattern, **not an AWS supported offering**, with no service-level agreement, and not
> intended for production use as-is. Review and harden it for your environment first —
> start with [Securing access](#securing-access), and see [LICENSE](LICENSE) for the terms
> it is provided under.

**Peer-Accelerated Cache with EFA Replication** — a node-local, S3-compatible caching proxy
for EKS. Rust, deployed as a DaemonSet, backed by Amazon S3.

One daemon per node. Unmodified S3 clients — boto3, the AWS CLI, `s5cmd`, a PyTorch
dataloader — point at the node's own endpoint. Objects on the node are served from RAM or
NVMe; a miss another node holds is served **from that peer** over one-sided EFA RDMA, with
gRPC/TCP fallback; anything nobody holds streams from the bucket and fills the cache on the
way past.

```
Pod (boto3 / CLI / s5cmd / PyTorch dataloader)
  |
  |  HTTP/S3, path-style, to the node's own endpoint
  v
pacer-daemon (node-local, :9000)
  |
  +- GET, on this node        -> RAM slab, or NVMe
  |
  +- GET, a peer holds it     -> that peer, by one-sided EFA RDMA
  |    (the rendezvous ring      WRITE straight out of its slab
  |     picks the home)          (:9100); gRPC/TCP fallback per peer
  |
  +- GET, nobody holds it     -> Amazon S3, filling the cache on
  |                              the way past
  |
  +- PUT / DELETE / multipart -> S3 write-through; on Standard a
                                 large PUT is scattered across the
                                 chunks' own homes and cached there
```

Two things set it apart from a generic read cache, both because the response body — not the
storage — is the wall:

- A cooperating client can hand the daemon **memory it already owns** (host RAM, shared
  memory, or GPU HBM registered on its own NIC) and get a header-only `200`: no body, no
  socket copy.
- A large `PUT` to Standard is **scattered** — each part uploaded by the node owning that
  chunk, and cached there — so writing a checkpoint populates the cache for whoever reads it.

Read bandwidth scales with the cluster rather than the bucket, the holder's CPU stays off the
data path, and a byte served from a peer is a byte not re-fetched.

## Measured

**3.6× on the plain HTTP path with an unmodified client** — 26.97 GiB/s against 7.52 GiB/s
reading the same objects from regional S3, two `p5.48xlarge`, same load generator on both
arms. Warm-cache throughput, and the arm quoted is the one where bytes come **from a peer**.
Method, arms, caveats and a reproduction script: [docs/benchmarks/](docs/benchmarks/README.md).

**A 131.4 GiB `Llama-3.3-70B-Instruct` checkpoint into eight GPUs in ≈6.3 s ± 20 %** at TP=8 —
20.8 GiB/s, zero body bytes, output token-identical to the stock path. The checkpoint is
re-laid-out ahead of time into one contiguous slab per rank (8 × 16.429 GiB) and RDMA-written
into HBM the engine's own process registered. The ± is the spread *inside* a single load: its
eight ranks finish across 5.243–6.307 s, and a load ends when its slowest does. The layout is
the lever, not the fabric — on the ordinary path ~18 s of a 33 s load sits in vLLM's
per-parameter `weight_loader`, 729 tensors per rank at 0.9 GiB/s, dispatch overhead on strided
slices that no transport can shorten. One configuration, 2026-09-12; the exporter that writes
this layout and the loader that reads it are not published.

Cold, PACER is S3 parity by construction — it is reading S3.

## Status

**Beta, v0.1.0.** A `vX.Y.Z` tag publishes a multi-arch (amd64 + arm64, EFA-capable) image
and an OCI Helm chart to GHCR — see
[Releases](https://github.com/aws-samples/sample-pacer/releases); if none is visible yet,
install from source.

| Capability | State |
|---|---|
| Node cache: RAM + NVMe tiers, serve-while-fill, write-through, strip-and-re-sign auth | Shipped |
| Cluster tier over gRPC: rendezvous ring, EndpointSlice membership, cross-node read-after-write | Shipped |
| Chunked caching (16 MiB default), sharded chunk directory, multi-copy replication | Shipped |
| Two disk tiers — an `O_DIRECT` chunk store and a foyer hybrid — selected automatically | Shipped |
| Write scatter | Shipped, **default-on for Standard** |
| `If-Match` GETs served from cache — what lets a caching S3 client compose in front | Shipped 2026-09-15; composed throughput **not** re-measured |
| EFA-RDMA peer transport (one-sided WRITE, per-peer gRPC fallback), `efa` build feature | Hardware-validated; **beta**. The daemon auto-detects it and degrades to gRPC on its own; what stays off in the base chart is the pod's claim on a device, which no single DaemonSet spec can make safely across a mixed pool |
| Delivery into client shared memory (`shm:`) | Shipped, **on by default**; **beta** |
| Delivery into memory the client registered on its own NIC (`nic:`), including GPU HBM | Shipped, **on by default** — but it rides the RDMA transport, which the base chart leaves off (row above); **beta**, byte-verified on H100 and B200. Rust half is `crates/pacer-client`; the Python loader shim is unpublished |
| Fleet-scale checkpoint restore and save | Restore shipped and measured; **save** is materially slower — its ceiling is per-node egress, not the cache |
| Per-rank checkpoint re-layout — the ≈6.3 s result above | Measured; the exporter and the loader that reads it are **not published**, and a layout is specific to one TP width |
| Pre-stage warm-up Job; Standard→Express two-tier read-through; Append/Rename passthrough | Planned — **not built** |

## Requirements

- **EKS** ≥ 1.26 (GA `internalTrafficPolicy: Local`).
- **A CNI that enforces NetworkPolicy, with enforcement on** — the EKS VPC CNI ships it
  *disabled*. A prerequisite for the security model, not a hardening step: see
  [Securing access](#securing-access).
- **A bucket** — by default an S3 Express One Zone directory bucket whose AZ *ID* matches
  the nodepool's, plus a `com.amazonaws.<region>.s3express` Gateway VPC endpoint (PrivateLink
  is not supported for Express). Standard works via `config.backendType: standard`, and is
  the only shape write scatter applies to.
- **EKS Pod Identity** on the chart's ServiceAccount, granting the bucket's operations
  (`s3express:CreateSession` for Express). The daemon re-signs outbound and stores no
  per-client secrets.
- **A cache nodepool**, ideally single-AZ (pinned by AZ ID), with instance-store NVMe
  (RAID0), and Nitro v4+ for EFA. `karpenter.enabled` renders one, or label your own to match
  `nodeSelector` (default `pacer.io/nodepool: cache`).

## Quick start

```bash
# from a tagged release: the chart defaults to the matching GHCR image
helm install pacer oci://ghcr.io/aws-samples/sample-pacer/charts/pacer --version 0.1.0 \
  --namespace pacer --create-namespace \
  --set 'config.bucketMap.cache=<your-bucket>--use1-az4--x-s3' \
  --set config.s3Endpoint=https://s3express-use1-az4.us-east-1.amazonaws.com \
  --set config.awsRegion=us-east-1

# or from source, same flags: the chart is self-contained here
helm install pacer deploy/helm/pacer --namespace pacer --create-namespace ...

# then grant the daemon its identity: cluster-side, no chart value, and the
# EKS Pod Identity Agent add-on must be installed
aws eks create-pod-identity-association --cluster-name <cluster> \
  --namespace pacer --service-account pacer \
  --role-arn arn:aws:iam::<acct>:role/pacer-daemon
```

Every key is documented in [values.yaml](deploy/helm/pacer/values.yaml) and schema-validated,
so a typo fails the render instead of being ignored;
[values-example.yaml](deploy/helm/pacer/values-example.yaml) is a complete worked config.
**Why** each default is what it is, with the measurement behind it, is in
[docs/helm/](docs/helm/README.md). Read [memory-model.md](docs/helm/memory-model.md) before
raising `config.memCapacity` — every OOMKill this project has had came from a term nobody
added up; if one is happening now, [docs/runbooks/daemon-oom.md](docs/runbooks/daemon-oom.md).

### Recommended: reserve hugepages on the cache nodes

Boot the cache nodes with a hugepage reservation (kernel cmdline, or Karpenter `userData`),
then set `efa.hugepages` to a quantity and `efa.hugepageSizeMib` to `2` or `1024`. The daemon
registers its slab out of them and logs what it actually got at startup — rails placed, page
size, pinned bytes — which is the only way to see that a reservation was really honoured.

Two knock-on effects, both wanted: the ADR-0028 registered RAM tier turns on, and the disk
tier switches to the `O_DIRECT` chunk store (below). The render fails with the exact figure if
the quantity does not cover the RDMA arenas, 256 chunks and the slab.

Leave `efa.hugepages` empty and the daemon still runs — arenas map on 4 KiB pages, which costs
registration time and TLB footprint rather than bandwidth. Note the request is what selects a
node: set it, and a node that did not pre-reserve at boot will not be chosen at all.

### Which disk tier you get

Automatic. Where a registered slab exists — wherever `efa.hugepages` is set — you get the
**chunk store**: one `O_DIRECT` `pread` per chunk body, into a frame the NIC can send from.
Without a slab you get the **foyer** hybrid cache, because the store's read would silently
fall back to buffered I/O at roughly half the rate; the daemon refuses that combination
rather than serve it quietly. `config.diskTier: foyer` opts out.

### EFA RDMA peer transport (optional, beta)

**The daemon half already auto-detects.** It probes at startup, speaks gRPC-only where EFA is
absent, and falls back per peer on any RDMA error, so RDMA is never a hard dependency of the
read path. What cannot be a universal default is the **pod's** access to the device, because
both routes to it depend on the node's shape:

- `efa.enabled: true` requests a `vpc.amazonaws.com/efa` unit. That allocation is what earns
  the device-cgroup rule `ibv_open_device` needs — and it leaves the pod Pending on any node
  that has no unit to give.
- `efa.shareHostDevices: true` requests no unit and hostPath-mounts `/dev/infiniband`
  instead, so it takes nothing from the training job that wants the rails. But a device the
  pod did not allocate is outside its cgroup, and every rail then opens `EPERM` — which is why
  that mode needs `efa.privileged: true`. What that grants is device-cgroup allow-all rather
  than capabilities (`CapEff` is all zeros for the daemon's unprivileged uid), and it is the
  supported production setting on a cluster whose nodes you own.

So the base chart cannot flip this on without either taking a device from the workload or
granting itself a privilege, **and a chart must never silently escalate its own privileges**.
Off is a decision, not an omission — and the half-configured case is not left to run either:
asking for `shareHostDevices` without `privileged` fails the render rather than booting a
daemon that quietly serves gRPC while reporting rails.

On a Nitro v4+ nodepool running `aws-efa-k8s-device-plugin` it is the intended setting: the
commented block in [values-example.yaml](deploy/helm/pacer/values-example.yaml) lists the
prerequisites, and the image must be built `--features efa` (release images are).

Check the startup log for what was actually registered — a hugepage request that fell back to
4 KiB pages is invisible in a throughput number, and so is a fallback to gRPC on a node where
you expected rails. Knobs and the measurements behind them:
[docs/helm/efa-and-rdma.md](docs/helm/efa-and-rdma.md).

### Delivering into a client's own memory (on by default, beta)

For a client whose bottleneck is copying a body out of a socket — a checkpoint loader
pulling multi-GiB shards — PACER puts the bytes into memory the client already owns and
answers header-only, removing the HTTP/TCP hop from the data path:

```
GET /bucket/key
  x-pacer-target: shm:/loader-7;offset=0x40000;len=16777216
                  ^-- the daemon maps and pins this region
        -- or --
  x-pacer-target: nic:0x7f2a40000000;len=1073741824;rails=<gid>/<qpn>/<rkey>
                  ^-- memory the CLIENT registered on its own NIC: host RAM
                      or GPU HBM, one rkey per rail, so the daemon needs no
                      CUDA and never maps it

200 OK
  Content-Length: 0            <- the object is already in the client's window
  x-pacer-delivered: 16777216  <- presence IS the completion signal
  x-pacer-checksum: crc32=...  <- over the delivered bytes (checksum=none opts out)
```

`nic:` is the scheme that reaches **GPU memory**, and the inverted registration is a
constraint rather than a preference: on EFA only dma-buf registers device memory and only the
owning process can export one, so the client registers and the daemon only WRITEs. A token
names one rkey per rail — an rkey is scoped to its protection domain — and the writer picks
the rail PCIe-local to the GPU.

The **absence** of `x-pacer-delivered` tells the client to read the body, so a target over
`delivery.maxTargetBytes` or `delivery.pinnedBytesMax` is served as an ordinary body rather
than failed. Behaviour is gated strictly on the request header, so a client that names no
target takes exactly the path it took before — which is why this ships on. The default costs
memory limit rather than behaviour: the chart adds the pinned-page reservation and a derived
working set to the container limit, so a stock render asks for 9Gi where the value says 4Gi.
`delivery.enabled: false` gets that 5Gi back. Both quotas ship on their floor, which admits
one maximal window at a time — anything delivering to several ranks on one node raises them
together. Descriptor grammar, that sizing rule and the `shm:` mount requirement:
[docs/helm/delivery.md](docs/helm/delivery.md). The client half of `nic:` is
[crates/pacer-client](crates/pacer-client); the PyTorch shim is not published yet.

### Write scatter (Standard backends)

A `PUT` over 128 MiB becomes a multipart upload whose parts are uploaded by the chunks' own
homes and stay cached there. **On by default where it applies**; on Express it is off and
asking for it fails the render. One consequence: a scattered PUT *is* multipart, so the ETag
is the composite `-N` form — anything comparing ETags against a local MD5 must move to
`x-amz-checksum-crc32` or set `scatter.enabled: false`.
See [docs/helm/write-scatter.md](docs/helm/write-scatter.md).

## Securing access

**Reachability is authorization.** The daemon strips the caller's signature and re-signs
outbound with the **node's** IAM identity, so whatever can reach `:9000` holds that
identity's S3 access — a deliberate confused deputy, the same shape as `aws-sigv4-proxy`.
`:9100` is likewise unauthenticated: `Announce` and `Invalidate` carry no credential and
`requester_node_id` is self-asserted.

The chart ships a **NetworkPolicy, enabled by default** — `:9100` restricted to the release's
own pods, `:9090` to probes plus `networkPolicy.metrics`, `:9000` to `networkPolicy.clients`.
Two things you must do yourself.

**1. Confirm your CNI enforces.** Every API server *accepts* a NetworkPolicy; only an
enforcing CNI acts on one, so an unconfigured cluster gives you a policy that installs
cleanly, reads correctly in `kubectl get netpol`, and constrains nothing — worse than
shipping none, because it looks like the box is ticked. The addon config is authoritative
(the `aws-node` DaemonSet template can disagree with its own live pods, and
`NETWORK_POLICY_ENFORCING_MODE` is a different switch):

```bash
aws eks describe-addon --cluster-name <cluster> --region <region> \
  --addon-name vpc-cni --query addon.configurationValues
# want: {"enableNetworkPolicy":"true"}
```

Calico and Cilium enforce by default. Whatever the CNI, confirm empirically: a `curl` to
`:9000` from a pod the policy does not admit must hang or be refused, not list a bucket.

**2. Narrow `networkPolicy.clients`.** The default is `namespaceSelector: {}` — *every pod in
the cluster* — chosen so upgrading an install cannot silently stop serving traffic, not
because it is a boundary. Name the pods allowed to spend the node's IAM identity:

```yaml
networkPolicy:
  clients:
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: my-workload}}
      podSelector: {matchLabels: {app: my-trainer}}
  # probes come from the node's own address: narrow this, don't empty it
  probeCidrs: ["10.0.0.0/16"]
```

Two limits: it **cannot express "same node"** (no node selector, VPC CNI pod IPs come from
shared subnets, and `internalTrafficPolicy: Local` constrains only Service-VIP routing), and
it **does not cover the RDMA data plane**, which goes device-to-device past the CNI's hooks —
scope that with the EFA security group and the placement group. Design notes, and what each
rule does and does not cover, are in
[docs/helm/network-policy.md](docs/helm/network-policy.md).

## Configuring clients

Point any S3 SDK at the node-local Service and sign with the placeholder credentials
(default `pacer`/`pacer` — not a secret; the daemon checks that signature only to reject
malformed requests, then re-signs outbound under its own identity):

```python
import boto3
s3 = boto3.client(
    "s3",
    # <release>.<namespace>.svc.cluster.local
    endpoint_url="http://pacer.pacer.svc.cluster.local:9000",
    aws_access_key_id="pacer", aws_secret_access_key="pacer", region_name="us-east-1",
    config=boto3.session.Config(s3={"addressing_style": "path"}),  # REQUIRED
)
s3.download_file("cache", "checkpoints/model.safetensors", "/tmp/model.safetensors")
```

Two rules, both required, both enforced by the daemon's config contract:

1. **Address buckets by alias, never by a real `*--x-s3` name.** That name flips every AWS SDK
   into Express behaviour — zonal DNS, `CreateSession`, the `s3express` signing scope — aimed
   past the proxy at S3, which a node-local daemon cannot honour. Map aliases with
   `config.bucketMap`; clients use the alias.
2. **Force path-style addressing.** Virtual-host puts the bucket in the hostname, which does
   not resolve against the node endpoint.

`AWS_REGION` must be set in-cluster — the IMDS hop limit is 1, so an in-pod SDK cannot
discover it — and the chart sets it from `config.awsRegion`.

**Clients that send `If-Match`** are served from cache when their ETag matches the one this
node resolved (`config.conditionalGetFromCache`, on); a mismatch passes through to S3, never a
`412`. This is what lets a caching S3 client compose in front — Mountpoint for S3 among them
puts `If-Match` on *every* GET, and a proxy that refuses conditional GETs would serve it
nothing while still charging a hop. One difference from S3: on a hit the comparison is against
the *cached* ETag, so `If-Match` cannot detect an in-place replacement. Set it `false` for
strict semantics.

## Observability

`:9090` serves `/healthz`, `/readyz` and `/metrics` (Prometheus text). A Grafana dashboard
built only from metrics the daemon actually exports is in
[deploy/grafana/](deploy/grafana/README.md). The chart can also render a PrometheusRule
(`monitoring.prometheusRule`, off by default — it needs the Prometheus Operator CRD) whose
memory alerts read the daemon's own cgroup accounting, the numbers the kernel kills on. See
[docs/helm/monitoring.md](docs/helm/monitoring.md).

## Local development

"Reopen in Container" gives you the image CI builds from; `.devcontainer/efa/` adds the EFA
userspace that `--features efa` needs to compile at all
([CONTRIBUTING.md](CONTRIBUTING.md#development-environment)).

```bash
make build      # debug build of the workspace
make test       # all unit/integration tests
make lint       # fmt --check + clippy -D warnings + helm lint
make smoke      # boot the debug daemon, probe /healthz like kubelet does
make ci         # the full local gate: lint, test, doc, deny, smoke
make image      # self-contained local image build
```

## Repository map

| Path | Contents |
|---|---|
| [crates/pacer-daemon](crates/pacer-daemon) | `s3s` front end, proxy (cached GETs, write-through, scatter), strip-and-re-sign auth, delivery, admin endpoint |
| [crates/pacer-cache](crates/pacer-cache) | Cache policy (chunking, admission, range slicing), the registered RAM slab, both disk tiers |
| [crates/pacer-backend](crates/pacer-backend) | `aws-sdk-s3` backend: Express session auth, endpoint and region config |
| [crates/pacer-ring](crates/pacer-ring) | Rendezvous hashing (seeded xxh3), membership, chunk directory |
| [crates/pacer-transport](crates/pacer-transport) | The `PeerTransport` trait, gRPC, and EFA-RDMA behind the `efa` feature |
| [crates/pacer-client](crates/pacer-client) | Client half of NIC-token delivery: register memory, publish a token, answer announces |
| [crates/pacer-proto](crates/pacer-proto) | Peer protobufs: handshake, blob fetch, invalidate, directory, scatter |
| [deploy/helm/pacer](deploy/helm/pacer) | The chart: DaemonSet, node-local Service, NetworkPolicy, optional Karpenter nodepool, EFA and delivery overlays |
| [deploy/grafana](deploy/grafana) | Dashboard JSON model + import notes |
| [docs/adr](docs/adr/README.md) | Architecture Decision Records — one per locked decision, with the reasoning and what it cost |
| [docs/helm](docs/helm/README.md) | Why each chart default is what it is, with its measurement |
| [docs/benchmarks](docs/benchmarks/README.md) | Published measurements, method, reproduction scripts |
| [docs/runbooks](docs/runbooks/daemon-oom.md) | Operational runbooks |
| [ci/](ci) | EFA builder image, release image staging, functional smoke test |

## CI

GitHub Actions ([ci.yaml](.github/workflows/ci.yaml)): **rust** (`fmt --check`, `clippy -D
warnings`, `cargo test`), **helm** (`lint`, and `template` over the base chart and a
Karpenter + EFA overlay), **image** (release image build, no push). CodeQL is configured on
the repository rather than as a workflow here. The `efa` feature links libibverbs/libfabric,
so `--all-features` clippy runs only inside [the EFA builder
image](ci/Dockerfile.efa-builder). Dependabot ([dependabot.yml](.github/dependabot.yml))
proposes grouped updates. Pushing a `vX.Y.Z` tag runs
[release.yaml](.github/workflows/release.yaml): per-arch native builds, a Trivy gate, then
the multi-arch manifest and the OCI chart.

## Contributing, security, license

See [CONTRIBUTING.md](CONTRIBUTING.md) and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

Report a potential security issue to AWS/Amazon Security via the
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/)
rather than opening a public issue
([details](CONTRIBUTING.md#security-issue-notifications)). The daemon's trust boundary, and
the controls an operator has to set, are in [Securing access](#securing-access) above.

Licensed under [MIT-0](LICENSE) (MIT No Attribution). Copyright Amazon.com, Inc. or its
affiliates.
