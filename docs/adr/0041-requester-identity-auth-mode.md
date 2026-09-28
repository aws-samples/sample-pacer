# ADR-0041: Two auth modes — node identity (default) and requester identity

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-09-23 · Status: **Proposed — design validated end to end against real S3
(Standard and Express One Zone) with a standalone spike; implemented behind `auth.mode`
(default `node`, unchanged) and tested in process, not yet measured on a cluster.** What the
implementation changed from the text below is recorded in
[Implementation](#implementation-amended-2026-09-28).
The spike, its test matrix and every measured row are in
`spike/authz-forward/`; the implementation plan is
`planning/29-auth-modes.md`.

## Context

[ADR-0006](0006-strip-and-resign-auth.md) makes the daemon a deliberate confused deputy: it
verifies a placeholder signature, discards it, and re-signs every backend request with the
**node's** IAM identity, so whatever can reach `:9000` holds that identity's bucket access. Its
only guard is a NetworkPolicy the AWS VPC CNI ships with enforcement **disabled**
(threat-model T-001/T-004). ADR-0006 named the multi-tenancy escape as "verify-then-re-sign via
`s3s::auth::S3Auth`" — but that trait's single method is `get_secret_key(access_key)`: s3s
verifies the HMAC itself, so implementing it means the daemon **holds every caller's real AWS
secret**, exactly the gaul/s3proxy model ADR-0006 rejected, and impossible anyway against
rotating STS credentials. That escape is withdrawn here.

The 2026-09-22 design review with the S3 team reframed the goal: *the cache should not take on
any responsibility to authenticate or authorize.* The way to do that without holding secrets is
to let **S3 authorize the caller's own signature** — which requires the daemon to be an HTTP
**proxy** the SDK is configured with, not an **endpoint** the SDK is pointed at, because SigV4
signs the `Host` header and only a proxy setup leaves it as the real S3 host.

Two SigV4 facts, both **measured against real S3** rather than reasoned, shape the design:

| fact | evidence |
|---|---|
| A client GET that sends no `Range` does not list `range` in `SignedHeaders`, so the proxy may **add** any `Range` and the signature still verifies | unranged CLI GET forwarded with `Range: bytes=0-0` injected → **206, 1 byte**, on Standard **and** on Express (session-token-signed) |
| A client that sends its own `Range` **signs it**, so that request cannot be rewritten | ranged CLI GET with the range rewritten → **403 SignatureDoesNotMatch** |
| The client chooses `SignedHeaders`: it can send `Range` and leave it **unsigned** | boto3 with `range` excluded from signing, sending `bytes=100-199`; proxy rewrote to `0-0` → **206 `bytes 0-0/1686`**. Both the supported route (`before-sign`/`before-send` events) and the one-line `SIGNED_HEADERS_BLACKLIST` patch |

Excluding a header an intermediary rewrites is a sanctioned SDK pattern, not a hack: the Rust
signer's default exclusions already carry `user-agent` ("Changes when sent by proxy") and
`transfer-encoding` ("can be erased by Cloudfront"); the CRT exposes
`aws_signing_config_aws.should_sign_header` for the same purpose.

## Decision

The chart gains **`auth.mode`**, with two values. Nothing else about either mode is
configurable by the caller; the daemon serves one mode per release.

### `node` — the default, unchanged

ADR-0006 exactly as shipped: placeholder credentials, strip-and-re-sign with EKS Pod Identity,
NetworkPolicy as the authorization boundary, bucket aliasing mandatory on Express. Every
existing deployment, benchmark arm and client stays valid with no change.

### `requester` — the caller's signature authorizes every request

1. **Clients configure the daemon as an HTTP(S) proxy**, keep `endpoint_url` unset (or set to
   S3's own host), address the **real** bucket, and sign as they always did. The request
   arrives in absolute form (`GET https://bucket.s3.region.amazonaws.com/key`), signed for the
   real host. Express clients address the **zonal** endpoint, which is what the SDK resolves by
   default; a regional-endpoint override for a directory bucket is rejected by S3 itself
   (`AuthorizationHeaderMalformed: incorrect service "s3express"`, measured).
2. **Everything that is not an object GET is forwarded byte-transparently** — method, URI in
   origin form, every header including `Authorization`, `x-amz-security-token`,
   `x-amz-s3session-token`, `Content-MD5`, checksum headers, and the body — over TLS to the host
   the client signed for. No s3s DTO, no `aws-sdk-s3`, no node identity on this path. Measured:
   Express `CreateSession` (`GET /?session`) then the session-token GET; HEAD; PUT with a signed
   payload (byte-identical on read-back); DELETE — on both backends.
   **Writes are also teed** (amended 2026-09-24): the bytes of a `PutObject` or of each client
   `UploadPart` are cloned onto the chunk grid as they stream past, staged at their homes with a
   populate-only `StoreChunk`, and committed under ADR-0032's single fence when S3 confirms the
   object exists. Nobody but the client uploads. The daemon cannot turn a client PUT into a
   multipart upload it signs itself, so ADR-0032's *distribution* half is unavailable in this
   mode; its *populate* half survives, identity-free, and for the first time reaches Express.
   Spec: `planning/30-requester-mode-writes.md`.
3. **An object GET is strip-and-hold.** `Authorization` is lifted off the request (s3s fails on
   its mere presence unless it can verify it — verified in the crate), the whole original
   request is held in the request extensions, and s3s parses an **anonymous** request with the
   S3 domains registered as virtual hosts (`MultiDomain{regional, zonal}`). Measured: s3s 0.14
   parses the absolute-form path-style regional request and the Express zonal virtual-host
   request, with `credentials=None`, and the held request comes out in `get_object`.
4. **Every GET issues exactly one probe to S3 with the caller's signature**: the held request,
   re-emitted with `Range: bytes=0-0`. Its status is the authorization verdict (2xx/416 allow;
   403/404/any other → the client gets S3's answer verbatim), and its headers
   (`Content-Range: bytes 0-0/<len>`, `ETag`, `Last-Modified`, `Content-Type`) are the object
   header, so the node-identity `HeadObject` disappears. `416` means *authorized* but
   unsatisfiable (a 0-byte object; S3 evaluates auth first) and is not a deny.
5. **A hit is served from cache after the probe allows.** The probe overlaps the cache read;
   only *emission* waits on the verdict, so the cost is `max(probe, read)`, not the sum.
6. **A miss is filled with the caller's signature**: each chunk is the held request re-emitted
   with that chunk's `Range`. This is legal only when the caller left `range` unsigned. A
   caller who **signed** `range` cannot be served from cache or filled per chunk without
   breaking the signature, so that request is forwarded verbatim and counted
   (`pacer_authz_signed_range_bypass_total`) — ranged readers that want the cache have to
   adopt the client contract below.
7. **The read path never uses the node's identity.** On a cluster, the requesting node asks the
   chunk's home for a copy it already holds (the probe authorized the caller); a chunk nobody
   holds is read from S3 by the **requesting** node with the caller's signature and then pushed
   to its home with the `StoreChunk` RPC ADR-0032 already has. The peer plane therefore still
   carries no credential (threat-model T-002/T-003 unchanged) and the caller's signature never
   leaves the node it arrived on.
8. **Held signatures are per request and never cached.** Verdict caching keyed on identity is
   impossible by construction — verifying identity needs the secret or a registered public key
   — so the per-GET probe is structural, not a tuning defect. A held request is dropped when its
   response completes and is never logged (the existing "never log a credential" discipline
   already covers `Authorization`/`x-amz-security-token`).
9. **Transport.** Plaintext proxying (`http://` endpoint through the `:9000` proxy) is
   supported and measured. The recommended shape is **TLS to the daemon's own certificate** on a
   second listener: botocore's `proxies_config={'proxy_ca_bundle': …,
   'proxy_use_forwarding_for_https': True}` sends the absolute-form request *inside* the TLS
   session to the proxy instead of `CONNECT`-tunnelling (measured on Standard and Express; the
   negative case — an `https://` endpoint through a plaintext proxy URL — tunnels, the daemon
   sees nothing, and the client gets `ProxyConnectionError`). S3's own certificate is never
   impersonated: pod→daemon is the daemon's cert, daemon→S3 is S3's.

### The client contract for `requester` mode

Stated once, here, because it is the whole difference between a cache and a pass-through:

* **proxy, not endpoint** — `proxies={'https': 'https://<node-local>:9443'}` (+
  `proxies_config`) or `proxies={'http': 'http://<node-local>:9000'}` with an `http://` S3
  endpoint;
* **real bucket, default endpoint resolution** — no aliases; Express on the zonal host;
* **`range` unsigned** — boto3: register `before-sign.s3.GetObject` to lift `Range` and
  `before-send.s3.GetObject` to put it back (eight lines, stock client); Rust `aws-sigv4`:
  `SigningSettings.excluded_headers`; CRT-based clients (Mountpoint-for-S3):
  `should_sign_header` — an **upstream change**, not a mount option, so Mountpoint composes with
  `requester` mode only as a signed-range pass-through until then.

### The Helm surface

```yaml
auth:
  mode: node                # node (ADR-0006) | requester (this ADR)
  requester:
    s3Domains: []           # virtual-host domains s3s parses; empty = derived from
                            # config.awsRegion (+ the zonal domain on express)
    tls:
      secretName: ""        # kubernetes.io/tls secret for the proxy listener; empty = plaintext only
      port: 9443
    populateOnWrite: true   # tee PUT / UploadPart bytes into the cache (ADR-0032's populate half)
```

Render-time refusals (`pacer.validateAuth`): `requester` with an explicit `scatter.enabled:
true` (the scatter re-signs parts with the node identity, which this mode does not have; the
three-state default resolves to off), `requester` with a non-empty `bucketMap` (an alias cannot
be signed for), `requester` on `express` with no derivable zonal domain. `serviceAccount` stops
being load-bearing in `requester` mode; the NetworkPolicy stays as defence in depth, and
`NOTES.txt` stops calling reachability "the authorization" when the mode is `requester`.

## Consequences

* **Confused deputy gone in `requester` mode.** A pod that reaches `:9000` gets exactly the S3
  access its own credentials carry, judged by S3, per request. CloudTrail attributes every read
  and write to the caller (T-014 closes for this mode); revocation and deletion take effect on
  the next request. The daemon holds no secret and no node identity on the read path.
* **What it costs, stated plainly:** one extra round trip per GET (the probe), so TTFB rises by
  one in-AZ S3 RTT — measured at 0.32 s Mac→us-east-2, which is WAN and **not** the production
  number; in-AZ Express is single-digit ms per AWS's documentation and **unmeasured in-cluster**.
  Bandwidth is untouched, so the trade favours the large sequential reads this cache exists for
  and penalises small-object workloads. Request rate: Standard's 5,500 GET/s per prefix could
  throttle a restore storm through the probe; Express's 200,000 reads/s per directory bucket
  makes this a null on the primary backend.
* **Two things are refused at render in `requester` mode:** the write scatter and bucket
  aliasing. Writes are forwarded verbatim and teed for populate (ADR-0007's invalidation still
  fires, keyed from the forwarded URL, before the commit), so an object above S3's 5 GiB
  single-PUT cap needs the client's own multipart upload — which is also what populates best:
  parts that are a multiple of `chunk_size` populate every interior window, parts that are not
  populate nothing. What is genuinely lost is spreading one client's upload across N nodes'
  egress; that needs the caller's signature on the peer plane and waits on peer-plane
  authentication.
* **The signed-range asymmetry is a cliff, not a slope.** A client that signs `range` is a
  pass-through in this mode — correct, authorized, uncached. The counter exists so a deployment
  can see it happening rather than infer it from a hit rate.
* **Integrity of the requested range moves inside the pod→daemon hop.** With `range` unsigned,
  something on that hop could alter which bytes are asked for; on a hit the daemon already
  controls the returned bytes, and with the TLS listener the hop is the daemon's own TLS
  session. With plaintext proxying the exposure is exactly ADR-0006's plaintext hop. `ETag`
  conditions (ADR-0039) evaluate against the probe's *current* ETag while cached bytes may be an
  older version — the same class of trade ADR-0039 accepted, now with a fresh ETag on every GET
  to compare against.
* **s3s's own `S3Auth` is bypassed, not replaced.** The requester-mode service is built with no
  auth provider; `PlaceholderAuth` stays in `node` mode only.
* **Nothing changes for `node` mode.** Not the wire format, not the client, not a default.

## What is deliberately not decided here

The in-cluster probe TTFB (open until measured on the ladder), whether the probe should be
overlapped with the *first chunk's* fill as well as with the cache read, and whether a future
Mountpoint release exposes header exclusion. The design does not depend on any of them; the
first two are tuning, the third is a third party.

## Implementation (amended 2026-09-28)

The decision stands. Building it changed six details, each recorded here because the text
above is what a reader will otherwise take as the behaviour.

1. **Every requester-mode chunk carries a version witness, and a read checks it.** A chunk is
   cached with the ETag of the authorization request that read it, holders return that ETag
   with the chunk, and a read uses a chunk only when it matches the ETag its own authorization
   request just returned — otherwise it is a miss. The text above relied on ADR-0007's
   invalidation for overwrites; that cannot work here, because node mode's invalidation learns
   the old object's length with a `HeadObject` under the node's identity. The witness also
   covers objects overwritten directly in S3, which invalidation never could.
2. **Home nodes enforce the identity rule themselves.** In requester mode a peer server
   answers a `FetchBlob` miss with "not cached" and refuses a `StoreChunk` that asks it to
   upload a part, regardless of the requester's flags — the peer plane authenticates nobody,
   so a flag cannot be the boundary. Point 7's "pushed to its home with `StoreChunk`" uses a
   new `populate_only` offer that stages without uploading, committed under the witness.
3. **Delivery into client memory ([0026](0026-client-supplied-target-memory.md)) is off in
   this mode, and its pre-flight ([0030](0030-delivery-registration-belongs-to-the-memory-owner.md))
   is refused.** Node mode answers the pre-flight before any authorization, using the node's
   `HeadObject`; here that would disclose an object's existence, length and holders to an
   unauthorized caller. Point 7's "Delivery is unchanged" is withdrawn until delivery can run
   after the authorization request and check the witness.
4. **Shapes the cache does not serve are passed through, not refused.** A GET below or above
   the admitted size band, with a `Cache-Control` bypass, or with an `If-Match` the cache
   cannot honour re-emits the caller's request unmodified and returns S3's response — status,
   headers and body — exactly as node mode hands the same shapes to its passthrough.
5. **`s3Domains` is required, not derived.** The Helm surface above derived it from
   `config.awsRegion`; the design's own warning — a wrong list yields a plausible bucket name
   and a 404, not an error — argues against guessing, so both the chart and the daemon refuse
   an empty list.
6. **The write tee is not implemented.** Writes are forwarded unmodified and do not populate
   the cache; `populateOnWrite` does not exist yet. A requester-mode node on a cluster still
   holds a staging area, for chunks peers push to it, and the chart and the daemon's startup
   memory check both charge it.

Two things are as the text says but are worth stating: the authorization request is never
overlapped with the cache read, so a hit waits for it (its in-cluster latency is still the
unmeasured number above); and the TLS listener has its own connection cap, so a node's
ceiling is twice `listen.max_connections` when it is on.
