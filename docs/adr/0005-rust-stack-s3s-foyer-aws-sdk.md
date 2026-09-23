# ADR-0005: Core stack: s3s + foyer + aws-sdk-s3 + tonic (+ git-pinned ibverbs for EFA)

Date: 2026-07-10 · Status: Accepted

## Context

Full ecosystem survey in planning/02 and /04. Requirements: serve the S3 REST API server-side,
hybrid RAM+NVMe cache with LRU, streaming S3 client with Express session auth, gRPC, and an
eventual EFA/SRD path.

## Decision

| Layer | Crate | Why |
|---|---|---|
| S3 server front | `s3s` (+`s3s-aws`) | only server-side S3 framework; SigV4 verify built in; hyper service |
| Cache | `foyer` HybridCache | RAM+NVMe hybrid, LRU/TinyLFU/S3-FIFO, production-proven (RisingWave, Chroma), Prometheus built in |
| Backend client | `aws-sdk-s3` | GA; ByteStream streaming; ranged GET; multipart; **automatic S3 Express session auth** |
| HTTP | hyper 1.x + tower 0.5 | one coherent streaming generation; s3s native |
| gRPC | `tonic` | standard; control plane + fallback data path |
| Hash ring | `hrw-hash`/`hashring` or hand-rolled | rendezvous (HRW) mapping; must produce ADR-0014's pinned xxh3 score (seed `0x10_7a`, `0xff` separator) — the *contract* is frozen (ADR-0014), the crate behind it is not |
| EFA (Phase 3) | `ibverbs` (jonhoo) **git-pinned to main** | unreleased `efa` feature: efadv/libefa/SRD QPs + `efa_srd` example. **Open risk (ADR-0018/0020):** the data plane needs one-sided **WRITE**, the directory needs one-sided **READ**, and both need memory-registration/rkey management — a larger surface than the two-sided `efa_srd` example. Whether ibverbs-main exposes one-sided EFA verbs + MR management is the project's load-bearing dependency question; the ADR-0018 and ADR-0020 hardware spikes validate it before any format is frozen. If it doesn't, the rejected libfabric-FFI path is back on the table (see Consequences). |

Rejected: OpenDAL `oay` (lags core), MinIO anything (archived/AGPL), `object_store`/`rust-s3`
(client-only), `async-rdma`/`sideway` (no EFA), hand-rolled libfabric FFI (superseded by ibverbs main).

## Consequences

- Every layer is off-the-shelf except the EFA transport — the project's effort concentrates where it differentiates.
- Risk register: `s3s` is pre-1.0 single-maintainer (biggest *maintenance* risk — pin + vendor-ready); the **biggest technical risk is whether jonhoo/ibverbs main exposes one-sided WRITE/READ + MR management for EFA** (ADR-0018/0020), not just the two-sided SRD example — if the spike shows it doesn't, hand-rolled libfabric FFI (rejected below on the *pre-one-sided* assumption) is reconsidered by amendment. Watch for a crates.io release either way.
- foyer's built-in metrics give cache observability nearly free.
