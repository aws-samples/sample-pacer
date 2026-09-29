# Configuring clients

This page describes the default `auth.mode: node`. With `auth.mode: requester`, clients use
the daemon as an HTTP proxy and keep their own credentials and bucket names; see
[auth.md](helm/auth.md#the-client-contract).

## Endpoint and credentials

Point any S3 SDK at the node-local Service. Sign with the placeholder credentials
`pacer`/`pacer`. They are not a secret: the daemon uses them only to reject malformed
requests, then signs the request to S3 with its own IAM role.

```python
import boto3

s3 = boto3.client(
    "s3",
    endpoint_url="http://pacer.pacer.svc.cluster.local:9000",  # <release>.<namespace>
    aws_access_key_id="pacer",
    aws_secret_access_key="pacer",
    region_name="us-east-1",
    config=boto3.session.Config(s3={"addressing_style": "path"}),  # required
)
s3.download_file("cache", "checkpoints/model.safetensors", "/tmp/model.safetensors")
```

## Two rules

The daemon rejects requests that break them.

1. **Use the bucket alias from `config.bucketMap`, not the real bucket name.** A name ending
   in `--x-s3` makes AWS SDKs switch to S3 Express behaviour and send the request to S3
   directly, bypassing the cache.
2. **Use path-style addressing.** Virtual-hosted style puts the bucket name in the hostname,
   which does not resolve to the daemon.

The chart sets `AWS_REGION` in the pod from `config.awsRegion`, because an SDK in a pod
cannot look the region up from instance metadata.

## Conditional requests

Clients that send `If-Match` are served from cache when the ETag matches the cached copy. A
mismatch goes to S3; it never returns `412`. This lets caching S3 clients such as Mountpoint
for S3, which sends `If-Match` on every GET, sit in front of PACER.

Because a cache hit compares against the cached ETag, `If-Match` cannot detect that the
object was replaced in S3. Set `config.conditionalGetFromCache: false` if you need S3's exact
behaviour.

## ETags of large uploads

On S3 Standard, a `PUT` over 128 MiB is uploaded in parts ([write
scatter](features.md#write-scatter-s3-standard)), so its ETag has the multipart `-N` form
rather than an MD5 of the content. If anything compares ETags to a local MD5, switch it to
`x-amz-checksum-crc32` or set `scatter.enabled: false`.
