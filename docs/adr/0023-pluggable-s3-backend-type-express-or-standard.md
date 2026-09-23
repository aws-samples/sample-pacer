# ADR-0023: Pluggable backend type — S3 Express OR S3 Standard, at full parity

Date: 2026-08-13 · Status: Accepted

## Context

The daemon was built Express-only. Decision #1 in `planning/README.md` and
ADR-0002 locked the backend to an **S3 Express One Zone directory bucket in the
same AZ ID as a single-AZ nodepool**: a cache miss is single-digit-ms, which is
what demotes the peer-cache tier from "core" to "optimization." Express-specific
assumptions leaked into the code accordingly:

- **Auth / addressing / signing.** A real directory-bucket name (`*--x-s3`)
  flips the AWS SDK into Express behavior — zonal DNS, transparent
  `s3express:CreateSession`, and the `sigv4-s3express` signing scheme
  (planning/03). The proxy hides this behind a client-facing **bucket alias**
  (ADR-0002 field note): clients address a plain name, the daemon rewrites it to
  the real `*--x-s3` name, and the SDK does the Express dance outbound.
- **Write-path normalization.** Directory buckets reject `Content-MD5` (501) and
  require multipart part numbers **consecutive from 1** (400 otherwise). The
  proxy stripped `Content-MD5` and rejected any non-consecutive completion.

Those are correct for Express and **wrong for S3 Standard**: Standard is a
general-purpose regional bucket that accepts `Content-MD5`, permits **sparse
(ascending-with-gaps) multipart part numbers**, needs no `CreateSession`, and
uses plain regional SigV4. Users want to point the daemon at an ordinary S3
Standard bucket (durability across AZs, no directory-bucket limitation list,
existing data) without losing reads, writes, multipart, or the cache.

Detecting the backend from the bucket name is fragile — a Standard bucket can be
named anything, and the alias the client uses hides the real name anyway — so the
shape must be **explicit config**.

## Decision

Add a **pluggable backend type**, `express | standard`, selected by explicit
config, and make **S3 Standard full functional parity** — reads, writes,
multipart, the chunked cache, and the correctness suite all work against a
regional Standard bucket as they do against an Express directory bucket. This
**relaxes** `planning/README.md` decision #1 and ADR-0002's Express-only /
single-AZ premise: Express remains the default (unset ⇒ Express, so existing
deployments are unchanged), Standard is a first-class alternative.

Config surface (ADR-0013 layering, defined once in `config.rs`):

- Env `PACER_BACKEND_TYPE`, file `backend.backend-type`, chart
  `config.backendType`. Default **`express`**. An unknown value is a **startup
  error**, never a silent default (the two shapes differ on the write path).

What is gated on the backend type, and where:

- **SDK Express session auth** (`crates/pacer-backend/src/lib.rs`,
  `build_client`): Standard calls `disable_s3_express_session_auth(true)` so no
  `CreateSession` is ever attempted and requests use plain regional SigV4 —
  belt-and-suspenders even if a name resembled a directory bucket. Express is
  left to the SDK, which manages session auth transparently (we write no auth
  code, per planning/03).
- **Multipart part-order rule** (`crates/pacer-daemon/src/proxy.rs`,
  `complete_multipart_upload` via `part_numbers_ok`): Express requires
  consecutive-from-1 (unchanged); Standard requires only ascending + ≥ 1, so a
  sparse completion Express would reject is accepted and proxied. This is the
  load-bearing correctness fix — the old check would reject valid Standard
  uploads.
- **`Content-MD5` stripping** (`proxy.rs`, `put_object` / `upload_part`):
  stripped only on Express (which 501s on it); forwarded on Standard so the
  backend validates the client's digest end-to-end.
- **Bucket aliasing** (`proxy.rs` `bucket_map`): the mechanism (a plain string
  rewrite) is backend-neutral and unchanged. Only the *requirement* was
  Express-specific — on Standard the real name is ordinary, so aliasing is
  optional; docs say so.

The wire/ownership contracts (ring hash, directory ABI, peer RPC, RDMA data
plane) are untouched — they are about inter-node cache traffic, not the backend,
and never depended on the backend shape.

## Consequences

- Standard buckets get durability across AZs and escape the directory-bucket
  limitation list (versioning, replication, tags, etc. — planning/03), at
  Standard's request/byte pricing.
- Express stays the default and its path is byte-for-byte unchanged; no existing
  deployment shifts behavior.
- Two write-path branches now exist (part-order, MD5); both are covered by a
  pure unit test (`part_numbers_ok`) and a Standard-parameterized correctness
  test, plus the existing Express tests.

## Caveat — parity is FUNCTIONAL, not performance-equivalent

Standard is **regional and cross-AZ**. The same-AZ-ID weld that gives Express
its single-digit-ms miss (ADR-0002, the whole reason the peer cache is "just an
optimization") **does not apply** to a Standard bucket: a miss crosses AZ
boundaries and is slower and higher-variance. So:

- On Standard, the peer-cache / RDMA tier is **more load-bearing**, not less —
  misses are dearer, so cross-node cache hits matter more.
- Cross-AZ **data-transfer charges** may apply to Standard traffic that the
  single-AZ Express design never incurred (planning/03 flagged this as
  unconfirmed and moot for Express; it is now in scope for Standard).

This ADR asserts **functional** parity only. The latency/throughput delta
between Express (same-AZ) and Standard (cross-AZ) is **quantified by the Phase-4
D2 benchmark**, not by this ADR.

## Verification status

- Tested locally (macOS, no `efa` feature) against an `s3s-fs` backend:
  backend-type config resolution (default/file/env/reject-unknown), the
  `part_numbers_ok` decision for both shapes, and a Standard-parameterized
  end-to-end suite (write-through, cold-fill → warm-hit cached reads, and a
  consecutive multipart round-trip).
- **Needs a real S3 Standard bucket on-cluster (D2):** the Express-vs-Standard
  latency/throughput/cost delta; `Content-MD5` pass-through against real S3
  (the SDK's request-checksum interaction); and **sparse** multipart completion
  succeeding end-to-end — `s3s-fs` itself enforces consecutive parts, so the
  proxy's sparse acceptance is only unit-tested locally.
