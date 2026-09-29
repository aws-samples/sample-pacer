# Optional features

## Hugepages (recommended)

Reserve hugepages on the cache nodes at boot (kernel command line or Karpenter `userData`),
then set `efa.hugepages` to the amount and `efa.hugepageSizeMib` to `2` or `1024`. This lets
the daemon use registered memory for its RAM cache and switches the disk tier to the faster
`O_DIRECT` chunk store. The chart refuses the install if the amount is too small, and tells
you the minimum.

Setting `efa.hugepages` also restricts scheduling to nodes that reserved hugepages. Without
it, the daemon still works, using ordinary 4 KiB pages.

The daemon picks the disk tier automatically: the chunk store when hugepages are set, and the
foyer hybrid cache otherwise. Set `config.diskTier: foyer` to force foyer. See
[cache-and-disk-tier.md](helm/cache-and-disk-tier.md).

## EFA RDMA between nodes

The daemon detects EFA at startup and uses it when present. It falls back to gRPC per peer
on any RDMA error, so RDMA is never required. The image must be built with `--features efa`;
release images are.

The base chart does not give the pod access to the EFA device, because both ways of doing so
depend on your nodes:

- `efa.enabled: true` requests a `vpc.amazonaws.com/efa` device. Pods stay Pending on nodes
  without one, and the device is no longer available to your training job.
- `efa.shareHostDevices: true` with `efa.privileged: true` mounts the host's EFA devices
  without taking one. It needs `privileged` for device access. The daemon still runs as an
  unprivileged user with no capabilities. The chart refuses `shareHostDevices` without
  `privileged`.

On a Nitro v4+ node pool running `aws-efa-k8s-device-plugin`, turn one of these on. The
startup log shows which devices were opened and whether hugepages were used; check it, since
a gRPC fallback does not show up anywhere else. See [efa-and-rdma.md](helm/efa-and-rdma.md)
and the commented block in [values-example.yaml](../deploy/helm/pacer/values-example.yaml).

## Delivery into client memory

A client can ask the daemon to write the object into memory the client provides, and get
back a `200` with no body. This avoids copying large objects through a socket, which is the
bottleneck when loading multi-GiB checkpoint shards.

```
GET /bucket/key
  x-pacer-target: shm:/loader-7;offset=0x40000;len=16777216
      (a shared-memory region the daemon maps)
  x-pacer-target: nic:0x7f2a40000000;len=1073741824;rails=<gid>/<qpn>/<rkey>
      (host or GPU memory the client registered with its EFA adapter;
       the daemon writes to it over RDMA)

200 OK
  Content-Length: 0
  x-pacer-delivered: 16777216     <- the bytes are in the client's memory
  x-pacer-checksum: crc32=...
```

If `x-pacer-delivered` is missing, the client reads the body as usual. That happens when the
target exceeds `delivery.maxTargetBytes` or `delivery.pinnedBytesMax`. Requests without
`x-pacer-target` are unaffected, which is why this is on by default.

It does raise the container's memory limit: a default install requests 9Gi instead of 4Gi.
Set `delivery.enabled: false` to get the 5Gi back. The default quotas allow one
maximum-size delivery at a time per node; raise both together to deliver to several GPUs on
one node. See [delivery.md](helm/delivery.md). The Rust client for `nic:` is
[crates/pacer-client](../crates/pacer-client).

## Write scatter (S3 Standard)

On S3 Standard, a `PUT` larger than 128 MiB becomes a multipart upload. Each part is uploaded
by the node that will serve it, and stays cached there, so a checkpoint is already cached
when it is read back. This is on by default for Standard and not available on Express.

It changes the object's ETag to the multipart `-N` form; see
[clients.md](clients.md#etags-of-large-uploads). See [write-scatter.md](helm/write-scatter.md).

## Warming the cache ahead of first read

`pacer-daemon warm` reads a bucket prefix, an exact key, or a manifest of either into the
cache before the workload that needs it starts, and sends none of the bytes back. It ships
in the daemon image, so a warm is a Kubernetes Job on that image, on a node of the ring you
want warm:

```bash
AWS_ACCESS_KEY_ID=pacer AWS_SECRET_ACCESS_KEY=pacer AWS_REGION=us-east-1 \
pacer-daemon warm --endpoint http://pacer.pacer.svc.cluster.local:9000 \
  s3://cache/models/llama-3-405b/
```

It expands the prefix, slices each object into bounded requests (`--slice`, default 1 GiB) so
no single request runs long, and warms them at bounded concurrency (`--concurrency`). Add
`--max-bytes` to refuse a warm larger than you intend to pay for, or `--dry-run` to see the
object count and total size first. Re-running it is safe and cheap: an already-warm slice
costs a cache hit. **One limit to know:** a warm fills each chunk at one of its
`cluster.replicationR` homes, so a reader on another node can still find its first copy of a
chunk cold ([#38](https://github.com/aws-samples/sample-pacer/issues/38)). Design:
[ADR-0048](adr/0048-warm-only-get.md).
