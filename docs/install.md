# Installing PACER

## Requirements

- **EKS 1.26 or later.**
- **A CNI that enforces NetworkPolicy, with enforcement turned on.** The EKS VPC CNI ships
  with it off. PACER's security model depends on it; see [security.md](security.md).
- **A bucket.** The default is an S3 Express One Zone directory bucket in the same AZ (by AZ
  ID) as the cache nodes, with a `com.amazonaws.<region>.s3express` Gateway VPC endpoint.
  PrivateLink is not supported for Express. For S3 Standard, set
  `config.backendType: standard`.
- **EKS Pod Identity** for the chart's ServiceAccount, with permission for the bucket's
  operations (`s3express:CreateSession` for Express).
- **Cache nodes**, ideally in one AZ, with instance-store NVMe (RAID0), and Nitro v4 or later
  for EFA. Set `karpenter.enabled` to have the chart create a Karpenter NodePool, or label
  your own nodes to match `nodeSelector` (default `pacer.io/nodepool: cache`).

## Install

```bash
# From a tagged release. The chart defaults to the matching GHCR image.
helm install pacer oci://ghcr.io/aws-samples/sample-pacer/charts/pacer --version 0.1.0 \
  --namespace pacer --create-namespace \
  --set 'config.bucketMap.cache=<your-bucket>--use1-az4--x-s3' \
  --set config.s3Endpoint=https://s3express-use1-az4.us-east-1.amazonaws.com \
  --set config.awsRegion=us-east-1

# Or from source, with the same flags.
helm install pacer deploy/helm/pacer --namespace pacer --create-namespace ...

# Give the daemon its IAM role. This needs the EKS Pod Identity Agent add-on.
aws eks create-pod-identity-association --cluster-name <cluster> \
  --namespace pacer --service-account pacer \
  --role-arn arn:aws:iam::<acct>:role/pacer-daemon
```

Release images and charts are published to GHCR when a `vX.Y.Z` tag is pushed. See
[Releases](https://github.com/aws-samples/sample-pacer/releases). If no release is listed
yet, install from source.

To declare rings as Kubernetes objects instead of one Helm release each, install the
operator and create a `CacheRing` per ring; the ring's `spec.values` take the same values
as above. See [helm/operator.md](helm/operator.md).

Then, before sending traffic:

1. Lock down access as described in [security.md](security.md).
2. Point your clients at the daemon as described in [clients.md](clients.md).

## Configuration

Every setting is documented in [values.yaml](../deploy/helm/pacer/values.yaml), and the chart
validates values against a schema, so a misspelled key fails the install.
[values-example.yaml](../deploy/helm/pacer/values-example.yaml) is a complete example.
[docs/helm/](helm/README.md) explains why each default was chosen.

Before raising `config.memCapacity`, read [memory-model.md](helm/memory-model.md). If a
daemon is being OOM-killed, see [the OOM runbook](runbooks/daemon-oom.md).

For hugepages, EFA RDMA, delivery into client memory and write scatter, see
[features.md](features.md).

## Observability

Port 9090 serves `/healthz`, `/readyz` and Prometheus `/metrics`. A Grafana dashboard is in
[deploy/grafana/](../deploy/grafana/README.md). Set `monitoring.prometheusRule` to add memory
alerts; it needs the Prometheus Operator CRDs. See [monitoring.md](helm/monitoring.md).
