# ADR-0002: Backend = S3 Express One Zone directory bucket, same AZ ID as a single-AZ nodepool

Date: 2026-07-10 · Status: Accepted (user decision)

## Context

How much a peer cache has to carry depends on how expensive a miss is: the further the backend,
the more of the working set the cluster must hold itself. S3 Express One Zone changes that term
— single-digit-ms access, 200k read TPS/bucket (no per-prefix limits), and GETs ~13× cheaper
than Standard — against 4.8× storage cost, per-GB retrieval/upload charges, single-AZ durability,
and a long list of directory-bucket limitations (planning/03).

## Decision

Run a **single-AZ nodepool** and back the cache with an **S3 Express One Zone directory bucket in
the same AZ ID**. Access via Gateway VPC endpoint. IAM = `s3express:CreateSession` only; the Rust
SDK manages Express session auth automatically.

## Consequences

- A cache miss costs single-digit ms → the peer-cache tier is an optimization (bytes billed, NIC offload), not a necessity. Phasing reflects this.
- Node cache's economic job = reduce **bytes retrieved** ($0.0006/GB), not
  request count. The 200k read TPS/bucket ceiling is protected under a
  1000-node storm by cluster-wide single-flight per chunk (ADR-0012/0015/0017):
  each chunk hits Express once, not once per reader.
- Must absorb directory-bucket quirks in the proxy: virtual-hosted-only addressing (we can hide it), unsorted ListObjectsV2, consecutive multipart parts, CRC32-not-MD5, expiration-only lifecycle.
- Single-AZ is the right shape here, not a compromise: GPU clusters run
  single-AZ for low inter-node latency anyway, and the same-AZ-ID weld is
  exactly what makes a miss single-digit-ms (so the peer cache is an
  optimization, not a necessity). Scaling stays single-AZ by design.
- Single-AZ blast radius: AZ loss can lose data; no replication support.
  Read durability is deferred to Phase 4 (read-through from S3 Standard) — but
  that does **not** protect freshly-written data that lives only in Express
  (e.g. a checkpoint just PUT from the cluster, ADR-0015's own workload). Such
  writes are irreplaceable until copied out; the framework/operator owns that
  copy-to-Standard, not the cache. Anything irreplaceable must not live only in
  Express.
- Nodepool must pin the AZ **ID** (differs per account from AZ names).
- Bonus primitives the directory-bucket API makes available to expose: Append (write-offset)
  and RenameObject.
