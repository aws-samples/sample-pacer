# Status and performance

PACER is beta, v0.1.0.

## Capabilities

| Capability | State |
|---|---|
| Node cache: RAM and NVMe tiers, write-through | Shipped |
| Cluster cache over gRPC: consistent placement across nodes, read-after-write across nodes | Shipped |
| Chunked caching (16 MiB chunks), multi-copy replication | Shipped |
| Split uploads on S3 Standard ([write scatter](features.md#write-scatter-s3-standard)) | Shipped, on by default |
| Serving `If-Match` requests from cache, so caching S3 clients such as Mountpoint for S3 can sit in front | Shipped; throughput with Mountpoint in front not yet measured |
| [EFA RDMA between nodes](features.md#efa-rdma-between-nodes) | Beta; off in the base chart |
| [Delivery into client memory](features.md#delivery-into-client-memory) (`shm:` and `nic:`) | Beta, on by default. `nic:` needs EFA. The Python loader is not published |
| Checkpoint restore and save across a fleet | Restore shipped and measured. Save is slower; it is limited by per-node network egress |
| [Warming an object or a prefix ahead of first read](features.md#warming-the-cache-ahead-of-first-read) (`pacer-daemon warm`), in both auth modes | Shipped — fills one of each chunk's homes until [#38](https://github.com/aws-samples/sample-pacer/issues/38) |
| Standard→Express two-tier cache, Append/Rename passthrough | Planned |

## Performance

| Workload | Result | Details |
|---|---|---|
| Warm-cache reads over plain HTTP, unmodified client, two `p5.48xlarge`, served from the peer node | 26.97 GiB/s, against 7.52 GiB/s reading the same objects from regional S3 (3.6×) | [benchmarks/](benchmarks/README.md), with a reproduction script |
| Loading `Llama-3.3-70B-Instruct` (131.4 GiB) into eight GPUs at TP=8, delivered into GPU memory | About 6.3 s (±20% across ranks), 20.8 GiB/s, output token-identical to the stock loader | One configuration, 2026-09-12 |

The second result depends on storing the checkpoint pre-arranged per GPU rank, one
contiguous block per rank. The tool that writes that layout and the loader that reads it are
not published yet, and a layout only works for the TP width it was written for.

On a cold cache, PACER reads from S3, so throughput matches S3.
