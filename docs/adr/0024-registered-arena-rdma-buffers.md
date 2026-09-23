# ADR-0024: One registered arena per rail, not a fixed pool of slots

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-19 · Status: Accepted

Supersedes the *buffer-pool shape* of [ADR-0018](0018-holder-driven-rdma-write-data-plane.md)
(its holder-driven-WRITE data plane is unchanged) and the *multi-QP rationale* of
[ADR-0022](0022-gpudirect-hbm-target-and-multi-rail-efa.md) (its HBM-target half is
unchanged and becomes more important — see § Status).

## Context

The daemon moves **14.8 GiB/s** on p5.48xlarge. planning/18 established by
transport-only measurement that the same hardware moves **~58 GiB/s** to host
memory, so the gap is ours, not the fabric's — and it is structural, not a tuning
oversight.

Throughput on the requester side is `slots × chunk ÷ hold_time`. Two of those three
terms are set by ADR-0018's pool shape
([`efa/mod.rs`](../../crates/pacer-transport/src/efa/mod.rs)):

- `DEFAULT_SLOT_BYTES = 64 << 20` and `DEFAULT_POOL_SLOTS = 64`, **divided** across
  rails (`requester_slots.div_ceil(n)`), so p5 gets **2 slots per rail**;
- a slot is 64 MiB while the ADR-0015 chunk is 16 MiB, so 3/4 of every pinned byte
  is unusable.

And `hold_time` is the killer. On the zero-copy serve path
([`blob_from_written_lease`](../../crates/pacer-transport/src/efa/mod.rs)) the
landed slot is handed to the S3 client as a `Bytes` that **owns the lease**
(planning/16 §5), so the slot is pinned until the client finishes draining —
measured at **~30–70× the wire time** (planning/15 rung 1). `slots_in_use` was
observed pinned at 64/64.

The arithmetic is decisive:

| hold time | slots needed for 58 GiB/s at 16 MiB |
|---|---:|
| wire-bound (~10 ms) | ~37 |
| client-drain-bound (~645 ms) | **~2340** |

At 64 MiB per slot, 2340 slots is 146 GiB of pinned memory — impossible. So the pool
cannot be scaled into the requirement; the *shape* is wrong.

The obvious patch — copy out of the landed slot and release it in wire time — was
rejected. It reintroduces the 0.30 CPU-s/GiB copy that planning/16 §5 removed (≈16
of p5's 192 vCPUs at full rate, worse on single-rail Graviton), to buy back capacity
we can get for free by registering more memory. The operator's framing is the right
one: *why copy out at all — can't we register the whole memory, and only touch the
ranges the requester was told about?*

## Decision

**Replace the fixed-slot pool with one hugepage-backed registered arena per rail,
carved into chunk-sized ranges.**

1. **One MR per rail, registered once at startup.** An anonymous `mmap` of
   `PACER_RDMA_ARENA_BYTES / rails`, registered via
   `ProtectionDomain::register_from_raw` — the call
   `spike/efa/src/regbuf.rs` validated on hardware.
   One rkey per rail; a range is identified by base + offset. This keeps ADR-0018's
   load-bearing rule — *register once, never per-read* — and drops only its
   fixed-slot representation.
2. **Ranges are `chunk_size` + headroom, not 64 MiB.** Same pinned bytes yield ~4×
   the concurrent transfers.
3. **Hugepage-backed.** 2 MiB pages by default where reserved. planning/18 proved
   page size does **not** affect throughput, so this is NOT a performance claim: it
   is to keep one MR over tens of GiB cheap to register and to keep the MR's
   translation footprint small. Base pages remain a working fallback.
4. **No copy-out, and no policy knob.** With thousands of ranges instead of 64
   slots, client-drain hold time stops being the binding constraint, so the
   requester keeps handing the S3 client `Bytes::from_owner` over the landed range.
   The retention rule is unchanged: anything that *keeps* bytes beyond the client
   stream (the layer-1 admit in `proxy.rs::maybe_admit_local`) still copies first.
5. **Admission scales with rails, not divided by them.** `HOLDER_SERVE_SLOTS` and
   the per-rail division are re-derived from the measured ceiling. The compile-time
   `assert!(HOLDER_SERVE_SLOTS <= DEFAULT_POOL_SLOTS)` invariant and the OOM
   protection it encodes (planning/15 B4: fan-in 7 × 256 slots OOM-killed a holder)
   are preserved — resident serve memory must stay bounded.
6. **The lease API does not change.** `lease`/`lease_owned`, `Lease`/`OwnedLease`/
   `WrittenSlot`, `remote()`, `local_slice()`, `with_bytes_mut()`, `slots_in_use()`
   keep their signatures and semantics, so call sites and the
   `pacer_rdma_requester_slots_in_use` gauge are untouched. This is an
   implementation swap behind a stable surface.

Sizing is documented arithmetic, not a magic number:
`ranges = target_throughput × hold_time ÷ chunk_size`. At 58 GiB/s and a 645 ms
drain that is ~2340 ranges ≈ 37 GiB — 1.8 % of p5's 2 TiB. The default stays
conservative and must move in lockstep with the node's hugepage reservation.

## Consequences

- **The requester stops being the bottleneck** without paying per-chunk CPU. The
  expected win is ~4× (14.8 → toward ~58 GiB/s); the ceiling itself is unchanged and
  is not something this ADR claims to move.
- **Pinned memory becomes an explicit, sized commitment** rather than an accident of
  `64 slots × 64 MiB`. It must be reflected in the chart's
  `efa.pinnedPoolReservation` and, for hugepages, in the node reservation —
  mismatch fails at startup (`ibv_reg_mr` `ENOMEM`), which is a boot failure, not a
  graceful degrade. That is deliberate: silently falling back to a tiny arena would
  reproduce today's invisible cap.
- **An rkey's blast radius grows** from one 64 MiB slot to a rail's whole arena. A
  leaked or stale descriptor exposes more. Accepted: peers are already trusted
  cluster members (ADR-0018), rkeys never leave the cluster, and the same rkey was
  always sufficient to reach *some* registered memory.
- **The holder bounce memcpy survives this ADR.** It is measured at only ~3.6–4 % of
  aggregate, and removing it requires foyer to allocate from the arena — a larger
  change, deliberately out of scope.
- **Kubernetes hugepage friction is now known** (planning/18 harness notes):
  `mmap(MAP_HUGETLB)` fails `ENOMEM` unless the pod *requests* `hugepages-<size>`,
  and Karpenter cannot provision such a pod unless a `NodeOverlay` declares the
  capacity and the `NodeOverlay` feature gate is on. Any chart change enabling
  hugepages must ship with that infrastructure or nodes will not launch.

## Status / relationship to other ADRs

- **ADR-0018** — data plane (holder-driven one-sided WRITE, done-as-response) stands
  unchanged. Only its pool *shape* is superseded here.
- **ADR-0022** — its multi-QP premise ("a single SRD QP tops out well below a
  100 Gbps rail") is **refuted** by measurement: one QP reaches 97.7 Gbps and
  `qps_per_rail` ∈ {1,2,4,8} is byte-identical (planning/18 § RESULT 2).
  `PACER_EFA_QPS_PER_RAIL` is retained as a contract but is not a lever. Its
  **HBM-target half is untouched and is now the only known route past ~58 GiB/s**,
  because host-memory DMA is the last surviving explanation for that ceiling.
- **ADR-0015** — `chunk_size` becomes the arena's range size, tightening the
  coupling this ADR relies on.

### Implementation note (2026-08-19, planning/19 D1+D2 landed)

Built as [`efa/arena.rs`](../../crates/pacer-transport/src/efa/arena.rs)'s
`HostArena`, replacing `efa/pool.rs`. Three places the code differs from this
ADR's letter, all deliberate:

1. **Point 6's stable surface is the S1 trait family, not the old concrete type
   names.** Call sites, the lease semantics and the
   `pacer_rdma_requester_slots_in_use` gauge are untouched exactly as required —
   but they reach the backend through `RdmaBuffers`/`RdmaLease`/`OwnedRdmaLease`
   ([`efa/buffers.rs`](../../crates/pacer-transport/src/efa/buffers.rs)), so the
   concrete types are named for their backend (`ArenaLease`, `WrittenRange`)
   rather than inheriting `Lease`/`OwnedLease`/`WrittenSlot`. Keeping the old
   names would have meant two backends fighting over one set of type names when
   track H lands.
2. **The holder arena is sized in RANGES, the requester arena in BYTES.** Point 5
   preserves the compile-time `HOLDER_SERVE_SLOTS <= depth` assert, and that is
   only checkable if the holder depth is a constant count — a byte budget's depth
   depends on the runtime `chunk_size`, so the assert would still compile and
   quietly stop constraining anything. Consequence, accepted and documented at
   the const: holder pinned memory is `HOLDER_ARENA_RANGES × chunk_size`, so it
   scales with `chunk_size`. `PACER_RDMA_ARENA_BYTES` therefore sizes the
   requester arena (the one this ADR's hold-time arithmetic is about).
3. **"`chunk_size` + headroom" is realized as page-rounding.** A chunk key encodes
   the `chunk_size` it was cut at (ADR-0015), so a holder can only serve a body
   for a key at the requester's own chunk size — the bound is structural, not
   conventional, and the only rounding needed is to a base page so no two ranges
   share one.

Also retired here: **`PACER_RDMA_REQUESTER_SLOTS`**, which configured the slot
count that no longer exists. It is rejected at startup with the byte conversion
rather than ignored — a deployment carrying the old name would otherwise boot with
the *default* arena, which is indistinguishable from the invisible cap this ADR
exists to remove. `PACER_RDMA_ARENA_PAGE_MIB` is new, and the chart derives it
from its own `efa.hugepages` request so the pod request and the mapping cannot
drift.

### Measured (2026-08-19, planning/19 D4 — 2 × p5.48xlarge)

**The decision holds; the expected ~4× does not.** A 256 GiB arena (16384 ranges,
512 per rail) moved the RAM-resident daemon rate from 14.766 to **15.691 GiB/s
(+6 %)**, with every arm serving `rdma fraction 1.000`.

What the ADR got right: the pool shape *was* an undeclared cap, and removing it
removed it — `pacer_rdma_requester_slots_in_use` measured **393 of 16384 (2.4 %)**
at 4× the baseline client concurrency, where the old pool sat pinned at 64/64. The
"no copy-out, no policy knob" call also holds: `requester_copy_seconds_total`
stayed at 0 and the holder's bounce copy measured 5.32 ms per 64 MiB serve
(12.0 GiB/s, 0.56 cores) — still not worth removing, exactly as § Consequences
predicted.

What it got wrong: the *arithmetic* silently assumed the buffer supply was the
binding term. It was not — the ceiling is invariant to buffers, to client
concurrency (4×: −0.2 %) and to bytes per serve (4×: −3 %), leaving **344 ms of
WRITE-completion wait per 64 MiB serve** (0.19 GiB/s per transfer) on a fabric
whose same silicon does 11.4 GiB/s for one message at a time. So "the requester
stops being the bottleneck" came true in the narrow sense and bought ~6 %, not ~4×.
See planning/19 § D5.

One operational consequence the ADR did not anticipate: registration is on the
**boot path before the admin listener binds**, at ~2.5 GiB/s, so a large arena
(260 GiB ≈ 100 s) is killed by kubelet's liveness probe long before it finishes.
The chart now ships a `startupProbe` for exactly this; without it the "boot failure,
not a graceful degrade" the ADR chose is a silent CrashLoop instead of a diagnosis.
