# PACER

> **Disclaimer** — an [AWS Samples](https://github.com/aws-samples) project: a sample of a
> pattern, **not an AWS supported offering**, with no service-level agreement, and not
> intended for production use as-is. Review and harden it for your environment first —
> start with [Securing access](docs/security.md), and see [LICENSE](LICENSE) for the terms
> it is provided under.

PACER (Peer-Accelerated Cache with EFA Replication) is a read-through cache for Amazon S3
that runs on your EKS nodes. Each node runs one daemon that speaks the S3 API, and your pods
point their existing S3 client at it: boto3, the AWS CLI, `s5cmd`, a PyTorch dataloader.

```
Pod (boto3 / CLI / s5cmd / PyTorch dataloader)
  |
  |  S3 API over HTTP, to the daemon on the same node
  v
pacer-daemon (:9000)
  |
  +- GET, cached on this node    -> RAM or NVMe
  +- GET, cached on another node -> that node, over EFA RDMA (:9100) or gRPC
  +- GET, not cached anywhere    -> Amazon S3, then cached
  +- PUT / DELETE / multipart    -> Amazon S3 (write-through)
```

It is built for workloads where many nodes read the same large objects: model weights,
checkpoints, training shards. Read bandwidth grows with the number of nodes instead of being
capped by the bucket.

- **3.6× regional S3** on warm-cache reads over plain HTTP with an unmodified client: 26.97
  GiB/s against 7.52 GiB/s on two `p5.48xlarge` ([benchmark](docs/benchmarks/README.md)).
- **A 131.4 GiB Llama-3.3-70B checkpoint into eight GPUs in about 6.3 s**, written directly
  into GPU memory over RDMA ([details and caveats](docs/status.md#performance)).

PACER is beta, v0.1.0. [docs/status.md](docs/status.md) lists what is shipped and what is not.

## Quick start

You need EKS 1.26+, a CNI that enforces NetworkPolicy, an S3 bucket, and EKS Pod Identity.
The full list is in [docs/install.md](docs/install.md).

```bash
helm install pacer oci://ghcr.io/aws-samples/sample-pacer/charts/pacer --version 0.1.0 \
  --namespace pacer --create-namespace \
  --set 'config.bucketMap.cache=<your-bucket>--use1-az4--x-s3' \
  --set config.s3Endpoint=https://s3express-use1-az4.us-east-1.amazonaws.com \
  --set config.awsRegion=us-east-1

aws eks create-pod-identity-association --cluster-name <cluster> \
  --namespace pacer --service-account pacer \
  --role-arn arn:aws:iam::<acct>:role/pacer-daemon
```

Then point a client at the daemon, using the bucket alias (`cache`) and path-style
addressing:

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://pacer.pacer.svc.cluster.local:9000",
    aws_access_key_id="pacer",      # placeholder; the daemon signs with its own role
    aws_secret_access_key="pacer",
    region_name="us-east-1",
    config=boto3.session.Config(s3={"addressing_style": "path"}),
)
s3.download_file("cache", "checkpoints/model.safetensors", "/tmp/model.safetensors")
```

> [!IMPORTANT]
> By default, any pod that can reach port 9000 gets the daemon's S3 access. Before sending
> real traffic, follow [docs/security.md](docs/security.md).

## Documentation

| Page | Covers |
|---|---|
| [Installing](docs/install.md) | Requirements, installation, configuration, monitoring |
| [Configuring clients](docs/clients.md) | Endpoint, credentials, bucket aliases, ETag behaviour |
| [Securing access](docs/security.md) | Auth modes, NetworkPolicy, what it does not cover |
| [Optional features](docs/features.md) | Hugepages, EFA RDMA, delivery into client memory, write scatter |
| [Status and performance](docs/status.md) | What is shipped, beta and planned; measured results |
| [Chart design notes](docs/helm/README.md) | Why each chart default is what it is |
| [Architecture decisions](docs/adr/README.md) | One record per design decision |

## Contributing, security, license

See [CONTRIBUTING.md](CONTRIBUTING.md), which also covers building, testing and the
repository layout, and [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).

Report potential security issues through the
[AWS vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/),
not in a public issue.

Licensed under [MIT-0](LICENSE). Copyright Amazon.com, Inc. or its affiliates.
