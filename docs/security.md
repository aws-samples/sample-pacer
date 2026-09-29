# Securing access

In the default `auth.mode: node`, the daemon discards each client's signature and signs
requests to S3 with the node's IAM role. **Any pod that can reach port 9000 has that role's
S3 access.** The peer port, 9100, is unauthenticated. Network access is the only control, so
the steps below are required, not optional hardening.

If pods with different S3 permissions share nodes, use `auth.mode: requester` instead. The
daemon then forwards each client's own signature and S3 authorizes every request, at the cost
of one extra S3 round trip per GET. See [auth.md](helm/auth.md).

## The default NetworkPolicy

The chart installs a NetworkPolicy by default:

| Port | Purpose | Allowed from |
|---|---|---|
| 9000 | S3 API | `networkPolicy.clients` |
| 9090 | Health and metrics | Probes, plus `networkPolicy.metrics` |
| 9100 | Peer traffic | Other PACER pods in the release |

You must do two things yourself.

## 1. Check that your CNI enforces NetworkPolicy

Kubernetes accepts a NetworkPolicy even when nothing enforces it. For the VPC CNI, check the
add-on configuration:

```bash
aws eks describe-addon --cluster-name <cluster> --region <region> \
  --addon-name vpc-cni --query addon.configurationValues
# expect: {"enableNetworkPolicy":"true"}
```

Calico and Cilium enforce by default. With any CNI, test it: a `curl` to port 9000 from a
pod the policy does not allow should time out or be refused.

## 2. Narrow `networkPolicy.clients`

The default allows every pod in the cluster, so that upgrading an existing install does not
cut off its clients. List the pods that should be allowed to use the node's S3 access:

```yaml
networkPolicy:
  clients:
    - namespaceSelector: {matchLabels: {kubernetes.io/metadata.name: my-workload}}
      podSelector: {matchLabels: {app: my-trainer}}
  # Probes come from the node's own IP. Narrow this range; do not empty it.
  probeCidrs: ["10.0.0.0/16"]
```

## What the policy does not cover

NetworkPolicy cannot restrict access to pods on the same node. It also does not cover EFA
RDMA traffic, which bypasses the CNI; restrict that with the EFA security group and placement
group. See [network-policy.md](helm/network-policy.md).

## Reporting a security issue

Report potential security issues to AWS through the
[vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/), not
in a public issue ([details](../CONTRIBUTING.md#security-issue-notifications)).
