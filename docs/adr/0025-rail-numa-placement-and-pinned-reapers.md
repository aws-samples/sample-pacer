# ADR-0025: Every rail's registered memory and completion reaper are placed on that rail's NIC NUMA node

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-19 · Status: Accepted (**measured**, planning/19 D5 step 0)

Extends the data plane of [ADR-0018](0018-holder-driven-rdma-write-data-plane.md) and
the arena of [ADR-0024](0024-registered-arena-rdma-buffers.md) with a placement
requirement neither stated. Does not change either wire protocol or buffer shape.

## Context

planning/19 track D is measured against a bar — **~58 GiB/s** host-memory aggregate
on p5.48xlarge — taken from the transport-only bench in
spike/efa/src/multirail.rs. D4 got the daemon to
15.7 GiB/s with the arena in place and then ruled out, one arm at a time, every
explanation on the daemon's side of that gap: buffer supply (16384 ranges, 2.4 %
utilized), client concurrency (4× → −0.2 %), bytes per serve (4× → −3 %), and CPU
(holder at 0.76 of 192 cores). What remained was 344 ms of WRITE-completion wait per
serve against 1.6 µs of posting, with all 32 rails uniformly at ~4 % of line rate.

The bar was not a property of the fabric alone. That bench ships
spike/efa/src/affinity.rs precisely because, on a
dual-socket p5 with 16 rails per PCIe complex, two placement facts decide whether an
aggregate scales — and its module doc records the measured consequence of ignoring
them:

1. **A one-sided WRITE's NIC DMA-reads its source from host memory**, and the
   requester's NIC DMA-writes the landing range. Far-socket pages put every byte
   across the inter-socket link. The bench registers each rail's buffer while pinned
   to a CPU on that NIC's own node, so `ibv_reg_mr` first-touches the pages locally.
2. **A rail's reaper blocks on its completion-channel fd.** Unpinned, "several rail
   threads pack onto shared cores and their wakeup latency starves the in-flight
   window ... per-rail rate *collapses* as rails are added ... the signature of
   contention, not memory bandwidth."

The daemon implemented neither: `grep -r 'affinity\|numa' crates/` returned nothing.
It registered all 64 arenas (32 requester + 32 holder) in one startup-thread loop,
and ran all 32 reapers as tasks on one shared multi-threaded runtime. D4's symptoms
are that doc's prediction term for term, including planning/18's own daemon arms
where 32× the rails bought 1.94× (7.618 GiB/s on one rail → 0.46 per rail on 32).

So the comparison that framed the whole track was unfair: a daemon missing the
discipline, measured against a number the discipline produced.

## Decision

**Place both halves of every rail, and make the un-placed configuration reachable so
the placement can be proven rather than asserted.**

1. **Arenas are registered node-locally.** For each rail, the transport registers its
   two arenas on a *scoped thread pinned to that rail's `device/numa_node`*.
   `ibv_reg_mr` is what first-touches and pins the pages, so performing it there is
   what places them. A borrowed thread, not the caller: arena construction runs on a
   tokio worker during startup, and permanently narrowing a worker's affinity mask
   would hobble the runtime.
2. **Each reaper owns a pinned thread.** A rail's completion pump moves from "a task
   on the shared RDMA runtime" to its own OS thread, pinned to a distinct CPU on the
   rail's node, running its own current-thread tokio runtime — so an fd wakeup cannot
   queue behind any other task. This strictly strengthens the isolation the previous
   design reached for (keeping wakeups off the S3 proxy's workers). Threads are named
   `pacer-cq-<rail>`, so placement is verifiable from outside the process
   (`ps -To comm,psr`). Bring-up consequently takes no runtime handle at all.
3. **Best-effort, never fatal.** Unreadable sysfs, an unknown NUMA node, a refused
   `sched_setaffinity`: each degrades that rail to unplaced with a warning. A daemon
   must boot on a host whose topology it cannot read.
4. **`PACER_RDMA_AFFINITY` (default on) selects the policy**, and `0` reproduces the
   pre-placement daemon exactly. This is not a tuning knob — it is the control arm,
   and it is why one image can run both halves of the A/B on the same nodes.
5. **Placement is logged, at `info`.** `node_local_rails` on the arena line and a
   `rail placement resolved` line with the per-node distribution. A throughput number
   recorded without them is uninterpretable: a "placed" run on a host whose sysfs is
   unreadable is just a second control.

## Consequences

**Measured, 2026-08-19, 2 × p5.48xlarge (`gpu-az1`), 32 rails, RAM-resident,
identical nodes and keyset, every arm `rdma fraction 1.000` with 0 fallbacks and
0 read-throughs:**

| arm | GiB/s | Gbps |
|---|---:|---:|
| **placed** (this ADR) | **26.658** | 229 |
| unplaced control (`PACER_RDMA_AFFINITY=0`) | 16.138 | 138.6 |
| D4-A, `gpu-dev-8x`, unplaced | 15.691 | 134.8 |

**+65 %.** The control reproduces D4-A to within 2.8 %, so the pool difference is not
confounding the delta. Placement resolved as `rails=32 pinned=32
distribution=node0=16,node1=16` — the 16-rails-per-socket topology the bench's doc
describes — with `node_local_rails=32` versus `0` in the control.

- **The 344 ms completion wait was a symptom, not a defect.** Nothing in the
  completion path changed except where its reaper runs. Holder counters in the
  unplaced configuration (350 K serves): `post_batch` 1.8 µs, `holder_copy` 3.6 ms,
  `write_completion_wait` **72.7 ms** per serve. Do not go looking for a bug in
  `completion.rs`.
- **ADR-0024's arena is vindicated but was never the binding term.** Both arms ran the
  same 256 GiB arena and the same 16384 ranges; only placement differed.
- **32 extra OS threads** (one reaper per rail), each parked on an fd. Cheap against
  192 vCPUs, and the isolation is the point.
- **Track D's exit is still unmet**: 26.658 against ≥ 45 GiB/s. The remaining gap is
  2.2× to the ~58 GiB/s host-memory bar — down from 3.7× — and 13.5× to the
  360 GiB/s the H2 sweep measured into HBM on these same nodes.
- **A run that redeploys and immediately hammers can invalidate itself.** The first
  control arm died on the harness's validity gate (25 peer fallbacks) because
  `PACER_RDMA_AFFINITY` is in the deploy signature, so it rolled fresh pods whose AH
  caches were empty; the requester logged "no usable RDMA rail for peer (not
  negotiated)" until the handshake sweep caught up. Re-running against the live
  deployment produced the clean 16.138. Any future arm that changes a
  signature knob must warm the handshakes before it measures.

## Status / relationship to other ADRs

- **ADR-0018** — the holder-driven WRITE data plane and its "register once, never
  per-read" rule stand unchanged. This ADR only says *where* that registration and
  its completion reaping happen.
- **ADR-0024** — the arena shape stands. Its § Measured note records the +6 % the
  arena bought on its own; this ADR is why that number was so small.
- **ADR-0022** — its HBM half remains the only known route past the host-memory
  ceiling, and the H2 spike has now measured 360 GiB/s into HBM on this silicon. The
  placement discipline here is a prerequisite for that path too: the spike's own HBM
  run logs `multirail targets registered (NUMA-local)`.
- **planning/19 § D5** — the remaining steps, re-ordered by this result: an explicit
  per-rail in-flight window (the last premise the bench has and the daemon lacks), a
  hugepage arena (8 GiB per rail on 4 KiB pages is a different translation regime
  from the bench's 1 GiB buffer, so this is not planning/18's refuted arm), and the
  single-node client-path bound.
