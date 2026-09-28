> Design notes for the `auth` keys in
> [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file
> keeps one short comment per key; the reasoning lives here.

# Who authorizes a request — `auth.mode`

PACER can authorize reads and writes against S3 in one of two ways. The choice is made
once per release ([ADR-0041](../adr/0041-requester-identity-auth-mode.md)); no caller can
change it.

| | `node` (default) | `requester` |
|---|---|---|
| Client points the SDK at | the daemon, as its **endpoint** | the daemon, as its **HTTP proxy** |
| Client signs with | the placeholder credentials | **its own** credentials |
| S3 sees the request signed by | the node's IAM identity | the caller |
| What authorizes a request | reaching `:9000` ([ADR-0006](../adr/0006-strip-and-resign-auth.md)) | S3, judging the caller's own signature |
| CloudTrail attributes access to | the node | the caller |
| Bucket aliases (`config.bucketMap`) | required on Express | refused |
| Write scatter (`scatter.*`) | on for Standard by default | refused |

**Use `node`** when every pod that can reach the daemon is entitled to the same S3 access —
a single-tenant training cluster. It adds no round trip, and its NetworkPolicy
([network-policy.md](network-policy.md)) is the whole security model.

**Use `requester`** when pods with different S3 permissions share nodes. A pod that reaches
the daemon gets exactly the access its own credentials carry, judged by S3 on every request,
and revoking a credential takes effect on its next request. The daemon holds no secret and
does not use its own identity on anyone's behalf. The price is one extra S3 round trip per
GET — see [What it costs](#what-it-costs).

## Configuring `requester` mode

```yaml
auth:
  mode: requester
  requester:
    # The S3 base domains clients sign for. Required; never derived.
    s3Domains:
      - s3.us-east-2.amazonaws.com
      # on an Express backend, also the zonal domain:
      - s3express-use2-az1.us-east-2.amazonaws.com
    tls:
      secretName: pacer-tls   # a kubernetes.io/tls Secret; empty = plaintext only
      port: 9443
```

`s3Domains` is required and deliberately not derived from the region: a domain list that is
wrong does not fail — an unrecognised host is parsed as a bucket name, and the caller gets a
404 from S3. The daemon logs the list it resolved at startup; check it.

`tls.secretName` starts a second listener that speaks TLS to the **daemon's own**
certificate. It is the recommended shape (below). S3's own certificate is never presented or
impersonated: the client's TLS session ends at the daemon, and the daemon opens its own TLS
session to S3.

The chart refuses to render, and the daemon refuses to start, when requester mode is combined
with a non-empty `config.bucketMap`, an explicit `scatter.enabled: true`, or an empty
`s3Domains`; and when `tls.secretName` is set in node mode.

## The client contract

Three rules. The first two make the request authorizable; the third makes it cacheable.

1. **Proxy, not endpoint.** Configure the daemon as the SDK's proxy and leave the endpoint at
   S3's real host, so the request is signed for S3.
2. **Real bucket names, default endpoint resolution.** No aliases. Express directory buckets
   must be addressed on their **zonal** endpoint, which is what the SDKs resolve by default;
   S3 refuses a directory bucket addressed through the regional endpoint.
3. **Leave `Range` unsigned.** The daemon reads an object in fixed-size chunks and checks
   authorization with a one-byte ranged request, and it may only change the `Range` of a
   request whose signature does not cover it. A GET that signs `range` is forwarded untouched
   and never cached (it still succeeds).

### boto3, TLS to the daemon (recommended)

`proxy_use_forwarding_for_https` makes botocore send the request *inside* the TLS session to
the proxy instead of opening a `CONNECT` tunnel through it; the daemon refuses tunnels,
because it cannot see into one.

```python
import threading
import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    region_name="us-east-2",
    config=Config(
        proxies={"https": "https://pacer.pacer.svc.cluster.local:9443"},
        proxies_config={
            "proxy_ca_bundle": "/etc/pacer-ca/ca.crt",   # the CA that signed pacer-tls
            "proxy_use_forwarding_for_https": True,
        },
    ),
)

# Rule 3: keep Range out of the signature, using botocore's public events.
_pending = threading.local()

def _unsign_range(request, **_):
    value = request.headers.get("Range")
    if value is not None:
        del request.headers["Range"]
    _pending.range = value

def _resend_range(request, **_):
    value = getattr(_pending, "range", None)
    if value is not None:
        request.headers["Range"] = value
        _pending.range = None

s3.meta.events.register("before-sign.s3.GetObject", _unsign_range)
s3.meta.events.register("before-send.s3.GetObject", _resend_range)
```

### boto3, plaintext

The same client with `proxies={"http": "http://pacer.pacer.svc.cluster.local:9000"}` and an
`http://` endpoint (`endpoint_url="http://s3.us-east-2.amazonaws.com"`). Two things to know:
the hop between the pod and the daemon is then plaintext, exactly as it is in node mode; and an
**`https://` endpoint through a plaintext proxy URL makes the SDK open a `CONNECT` tunnel**,
which the daemon refuses — the client sees a proxy connection error, not a cache.

### Other clients

The Rust SDK can exclude `range` from signing through `aws-sigv4`'s signing settings. Clients
built on the AWS Common Runtime — Mountpoint for Amazon S3 among them — sign every header they
send and expose no option to exclude one, so their ranged GETs are forwarded uncached until
that changes upstream.

## What is served from the cache, and what is not

Every GET first issues one request to S3 with the caller's own signature and
`Range: bytes=0-0`. S3's answer is the authorization decision: a denial is returned to the
caller as S3 sent it, and nothing is read from the cache. An allowed answer also carries the
object's length and current ETag, so no `HeadObject` is ever issued.

| Request | What happens |
|---|---|
| GET of an object in the cacheable size band, `range` unsigned | served from the cache; misses read from S3 with the caller's signature and fill it |
| a cached chunk from an older version of the object | treated as a miss and re-read — never served (below) |
| GET below `config.minObjectSize` or above `config.maxObjectSize`, a `Cache-Control` bypass, an `If-Match` the cache cannot honour | passed through: the caller's request is sent to S3 unmodified and S3's response, headers included, is returned; nothing cached |
| GET that signs `range`, or carries `partNumber`, `versionId`, SSE-C or `If-None-Match` / `If-(Un)Modified-Since` | forwarded unmodified; nothing cached |
| every non-GET: PUT, multipart upload, DELETE, HEAD, list, `CreateSession` | forwarded unmodified |
| `CONNECT` | refused (405) |
| a request addressed to the daemon as an endpoint (relative URI) | refused (400) |

**Version safety.** Every chunk cached in requester mode records the ETag it was read under,
and a read uses a chunk — from this node or from a peer — only when that ETag matches the one
its own authorization request just returned. An object overwritten after it was cached, through
PACER or directly in S3, is therefore re-read rather than served stale. This is stronger than
node mode, which relies on invalidation-on-write and on objects not being overwritten in place.

**On a cluster** (`cluster.enabled`), a node never reads S3 on another node's behalf: it has no
signature to do so with. When no node holds a chunk, the node that received the GET reads it
with the caller's signature and then sends the bytes to the chunk's home node, which caches
them. The caller's signature never leaves the node it arrived on; the peer plane carries only
cached bytes, as in node mode. A home node enforces this itself — it refuses to read S3 or to
upload a part in requester mode whatever a peer asks — because the peer plane authenticates
no one. The same fact bounds what requester mode protects: authorization is enforced on every
read, but the integrity of cached bytes rests, as in node mode, on only the release's own
daemon pods being able to reach the peer port. A compromised daemon pod can serve or push
forged bytes to its peers.

## Writes

Every write is forwarded to S3 exactly as the client sent it, signed by the client — the
daemon never uploads anything itself. With `auth.requester.populateOnWrite: true` (the
default) the bytes of a `PutObject` and of each `UploadPart` are also copied onto the chunk
grid as they stream through and staged at each chunk's home node. They become visible only
once S3 says the object exists — a 2xx on the PUT, or a successful
`CompleteMultipartUpload` — and every staged chunk records the new ETag, so the next read of a
checkpoint just saved is served from the cache.

What populates:

| Write | Cached afterwards |
|---|---|
| `PutObject` | every chunk |
| multipart upload, part size a **multiple of the chunk size** (16 MiB by default) | every chunk except the object's last one |
| multipart upload, any other part size | nothing — the object is written and read correctly, just not pre-warmed |
| a part retried, a part S3 refused, parts of unequal size, a failed or aborted upload | nothing (the staged chunks are discarded) |
| `UploadPartCopy`, `CopyObject` | nothing (the bytes never pass through the daemon) |

So a writer that wants its uploads cached sets its part size to the chunk size or a multiple
of it. boto3's default is 8 MiB, which populates nothing against 16 MiB chunks:

```python
from boto3.s3.transfer import TransferConfig
s3.upload_file(path, bucket, key, Config=TransferConfig(multipart_chunksize=16 << 20))
```

Bodies sent in the `aws-chunked` encoding — the SDKs' streaming and trailing-checksum uploads
— are decoded before they are cached; a body that does not decode cleanly populates nothing.

**The tee never slows a write.** It holds at most `scatter.windowsInFlight` chunks between a
body and its staging and drops a chunk rather than wait for room
(`pacer_populate_windows_total{outcome="skipped"}`), and it tees at most as many bodies at once;
anything beyond that is forwarded without being cached.

## What is not available in requester mode yet

- **Delivery into client memory** (`delivery.*`, [delivery.md](delivery.md)) is off; a client
  that asks for it receives an ordinary response body. The delivery pre-flight query is
  refused.
- **The write scatter** (`scatter.*`) is refused: it converts a PUT into a multipart upload the
  daemon signs itself, and in this mode it has nothing to sign with.

## What it costs

- **One round trip to S3 per GET**, for the authorization request. It overlaps nothing: a cache
  hit waits for it. In-region latency to S3 Express One Zone is single-digit milliseconds, so
  this matters for many small reads and little for large sequential ones. It has not yet been
  measured on a cluster; `pacer_authz_probe_seconds` reports it.
- **One S3 request per GET against the request rate.** S3 Standard allows 5,500 GET/s per
  prefix, which a fleet-wide restore can approach; S3 Express One Zone allows far more per
  directory bucket.
- **Memory.** Each node holds a staging area for chunks waiting to be committed — pushed to it
  by peers, or written through it (`scatter.stagingBytes`, 2 GiB by default) — and, with
  `populateOnWrite`, up to `2 × scatter.windowsInFlight × chunkSize` (512 MiB by default) of
  chunks in flight from write bodies. The chart adds both to the container's memory limit.

## Metrics

| Series | Meaning |
|---|---|
| `pacer_authz_probe_total{result}` | authorization requests, by `allow` / `deny` / `error` |
| `pacer_authz_probe_seconds` | their latency — the added cost of every GET |
| `pacer_authz_signed_range_bypass_total` | GETs forwarded uncached because the caller signed `range` |
| `pacer_populate_windows_total{outcome}` | chunks populated without a read of their own — pushed to their home after a read, or teed from a write — by `committed` / `refused` / `failed` / `skipped` (no room; the write was not slowed) / `discarded` (the write failed or did not verify) |
| `pacer_cache_bypass_total` | includes every requester-mode pass-through |

A rising `pacer_authz_signed_range_bypass_total` means a client is not following rule 3 and
is getting no benefit from the cache.

## The NetworkPolicy in this mode

It still ships and still applies to `:9000` (and to the TLS port). In requester mode it is
defence in depth — it limits who can use the daemon's bandwidth and cache — not the
authorization: a pod it admits can do only what its own credentials allow.
