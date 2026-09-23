# ADR-0037: A checkpoint may be stored in a rank's own memory layout, so a load is one contiguous RDMA WRITE per rank

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-09-10 · Status: **Proposed. Nothing here is measured.** Every figure is arithmetic over
measurements cited by file, and the gates in the last section are unrun. Do not quote a number from
this document as a result.

The vLLM loader's own ledger says two thirds of a 70B load is one rank waiting for the others
(`vllm-other-attribution.md`). That wait is
not a tuning defect: it is what a *published* safetensors checkpoint costs, because the bytes on
S3 are not laid out the way a tensor-parallel rank's parameters are, so the ranks must exchange.
This ADR removes the exchange by storing the layout the rank actually wants.

Extends [0030](0030-delivery-registration-belongs-to-the-memory-owner.md) (the client registers the
memory it owns and names it with a per-rail token) and [0026](0026-client-supplied-target-memory.md).
**It changes no daemon behaviour and no wire format** — the daemon still serves chunk-aligned ranges
into client-named memory. What changes is what the client asks for and what is stored.

## Context

### The measured load, and why the largest term is structural

At 70B/TP=8, warm, `copies` @ 2 GiB, one rail per rank, the loader charges every interval of its own
wall clock to a named term. Per rank of a 20.2–20.6 s load:

| term | seconds | share |
|---|---|---|
| waiting on the device — window-reuse fence + end-of-load drain | **13.6–14.4** | **67–70 %** |
| the fabric (`fetch`) | 3.80–5.07 | ~21 % |
| enqueuing 723 broadcasts | 1.09–1.91 | ~8 % |
| shard headers | 0.26–0.42 | ~2 % |
| vLLM's per-parameter `weight_loader` | 0.064–0.072 | **0.3 %** |
| residual | 0.006 | 0.03 % |

vLLM reported `Model loading took 25.507 s`, so ~5.1 s is engine work outside `load_weights`.

The wait is **cross-rank idle**, not device work: per-rank totals agree within 2 % while fills span
12.470–18.630 s and drains span 0.017–6.176 s. A rank owning 9 spans instead of 12 waits at the end
instead of in the middle. Reducing ownership skew therefore moves time between two columns and
changes nothing on its own.

It is also not removable by deleting the fence. `views` is that experiment already built — no
per-span barrier at all — and the identical cost reappears in its per-fill device-wide
`torch.cuda.synchronize()` at **31.272 s against 10.335**, load 2.0× slower. The fence is a
correctness barrier besides: without it the NIC overwrites bytes a pending clone has not read, which
produced fluent nonsense nondeterministically at 70B with `0 body`, correct span counts and no
warning from vLLM.

### Why direct placement into the published layout only reaches two thirds

The obvious fix is to register the parameters themselves and have the daemon write each tensor
straight into its final home. For a checkpoint in HuggingFace layout that works for exactly the
params whose rank-slice is a *contiguous* range of the source tensor. On
`Llama-3.3-70B-Instruct` (8192 hidden, 28672 intermediate, 80 layers, 8 KV heads, 128256 vocab,
bf16 — the 30-shard, 131.416 GiB checkpoint every figure above was measured on):

| params | TP split | source slice | params | bytes |
|---|---|---|---|---|
| fused `qkv_proj` (10240×8192), fused `gate_up_proj` (57344×8192), `embed_tokens`, `lm_head`, norms | dim 0 | **contiguous** — a row range of a row-major tensor | 46.39 B | **86.4 GiB (65.8 %)** |
| `o_proj` (8192×8192), `down_proj` (8192×28672) | dim 1 | **strided** — one sliver per row | 24.16 B | **45.0 GiB (34.2 %)** |

(80 × 855 654 400 + 2 101 346 304 + 8 192 = 70 553 706 496 params, ×2 B = 131.41 GiB, which is the
measured figure — so the split is over the real checkpoint, not a model of one.)

The strided third fails for two independent reasons, and the second is fatal:

1. **Descriptor size.** Rank *r*'s `o_proj` slice is 8192 rows × 2 KiB and its `down_proj` slice is
   8192 rows × 7 KiB, so 80 layers is **1 310 720 descriptors per rank averaging ~4.6 KiB** — below
   the size where an RDMA WRITE is efficient. The fabric leg gets worse, not better.
2. **Chunk-granularity amplification on the tier.** A 16 MiB source chunk of `o_proj` holds rows
   belonging to all 8 ranks, so serving 8 ranks independently reads it 8 times:
   **45.0 GiB × 8 = 360 GiB** of tier reads. The disk tier is measured at ~15.9 GiB/s
   ([0033](0033-chunk-store-owns-the-disk-tier.md)), i.e. **~22.6 s** — worse than today's entire
   load.

So a published-layout direct-placement loader needs either a new daemon protocol (read a chunk once,
scatter to N clients) or a different stored artifact. This ADR chooses the artifact.

## Decision

### 1. The stored object *is* a rank's memory image

One object per `(model, TP degree, PP degree, dtype, quantization, vLLM fingerprint, rank)`, holding
`params_per_rank` bytes laid out exactly as the loader will address them. Consequences that fall out
for free rather than needing design:

* **Alignment is by construction.** The slab is one contiguous byte range, so chunk *k* of the
  object is slab offset *k* × `chunk_size`. No parameter needs individual alignment and there is no
  padding. (Padding each of ~723 params to a 16 MiB boundary would cost up to ~11 GiB — see
  *Alternatives*.)
* **~1051 chunk writes per rank** at 16.427 GiB and a 16 MiB chunk, sequential, none overlapping.
* **The fence path is never exercised.** Nothing is ever overwritten, so the hazard that needed
  `_await_window_readers` cannot arise — not "is guarded against".
* **Perfectly symmetric.** TP splits evenly, so every rank reads exactly `params_per_rank`. The
  ownership skew that produces today's lock-step is gone at the root.
* **Tier reads drop to 1×** everywhere, including the formerly strided third, because a chunk now
  belongs to exactly one rank.

### 2. The layout is captured from vLLM, never reimplemented

The map from checkpoint bytes to parameter bytes encodes vLLM's own decisions — fused-QKV
`shard_id` offsets, `gate_up` fusion, vocabulary padding, MoE `w13` packing, per-model permutations.
Reimplementing that is how a loader silently diverges from the engine it serves.

Instead: run one ordinary `model.load_weights(...)` and **intercept every
`param.weight_loader(param, loaded, shard_id)` call**, recording `(param name, slab offset, nbytes,
dtype, shape)`. Serialize that as the plan. The writer then dumps `params_per_rank` bytes per rank.

Two things follow from intercepting *after* the transform rather than before it:

* **Quantization works, and is not an exclusion.** An fp8/AWQ/GPTQ `weight_loader` applies scales
  and repacks bits; the stored blob is the post-transform image, so what is captured is whatever
  vLLM would have built. This inverts the constraint on the published-layout design, where a
  `weight_loader` that is not a pure narrow-and-copy could not be bypassed at all.
* **vLLM's completeness assertion is satisfied by the same pass** that produces the plan, so the
  plan cannot claim a parameter the engine never asked for.

Conversion is one-time per `(model, TP)`: read 131.4 GiB, write 131.4 GiB — ~55 s of writing at the
save rate recorded in planning/26, amortized over every load after.

### 3. The fingerprint is a refusal, not a warning

The layout is coupled to the vLLM version that produced it. A mismatch does not fail loudly — it
loads the right number of bytes into the wrong parameters and serves plausible tokens. **This repo
has caught exactly that failure twice on exactly this path, and only because an arm generated
text**: the `placement=copies` span race, and the loader's own pre-flight, which climbed a healthy
daemon-side counter while installing zero client handles.

So the manifest carries `vllm_version`, the resolved model revision, `tp_degree`, `pp_degree`,
`dtype`, the quantization scheme, `chunk_size`, a plan digest, and `params_per_rank`; and a load
whose environment disagrees on any of them **fails hard before registering memory**. Not a warning,
not a fallback to the published-safetensors loader — a refusal, because a silent fallback is
indistinguishable from success in every metric the daemon exports.

### 4. What a load becomes

Register the slab once as an ADR-0030 token window (a token window is not charged against
`delivery.maxTargetBytes`; only a mapped `shm:` target reserves quota), re-point every
`param.data` at its slab offset, issue the plan's ranged GETs, done. **No collective, no fence, no
drain waiting on peers, no receive-buffer allocation, and no `weight_loader` copy** — every measured
term except `fetch` and the manifest read.

Aggregate fabric bytes do **not** increase: a column-parallel weight splits on dim 0 and a
row-parallel one on dim 1, so the ranks' slices are disjoint and their union is each tensor exactly
once. 131.4 GiB total, 16.427 GiB per rank, the same as today. Replicated params (norms) are read
8× and are ~1 MiB.

### 5. What does not change

The daemon, the wire format, the chunk grid, the delivery path. The published-safetensors loader
(`pacer_vllm.py`) **stays as-is and stays the default**,
because it needs no conversion step and that is the whole adoption story. This is a second load
format, not a replacement.

## Alternatives rejected

1. **Daemon-side read-once, scatter-to-N** for the strided third. A real answer to the 8×
   amplification, and a new protocol. Pre-layout makes it unnecessary; revisit only if the
   no-conversion path becomes the priority.
2. **Delete the fence.** A correctness barrier, and `views` measures the alternative at 2.0× slower.
3. **Delete the drain.** It is a measurement boundary, not work: the device work exists either way,
   and removing the `torch.cuda.synchronize()` moves it into vLLM's KV profiling where nothing times
   it. The loader's number improves and the user waits exactly as long.
4. **Pad every parameter to a chunk boundary** so each is independently addressable. Up to ~11 GiB
   of padding at ~723 params, against zero for one contiguous slab.
5. **A larger RAM tier** so the checkpoint is memory-resident. 131.4 GiB will not fit, and
   `memCapacity` = 32 GiB is a deliberate product choice (a user will not dedicate 128 GiB of RAM to
   a cache), reaffirmed by [0035](0035-daemon-resources-and-qos-class.md)'s reasoning about
   reservations the training pods have to schedule against.

## Consequences

**The wall moves from cross-rank synchronization to this daemon's disk tier**, which is the point of
the change and also its main caveat. All 8 ranks now read at once, and 131.4 GiB does not fit the RAM
tier, so one node's tier must supply it:

| | rate | 131.416 GiB | + ~5.1 s engine | vs 25.507 s |
|---|---|---|---|---|
| tier as measured (0033) | ~15.9 GiB/s | 8.27 s | **~13.4 s** | **~1.9×** |
| the same device under fio | 43.565 GiB/s | 3.02 s | ~8.1 s | ~3.2× |

Neither the fabric nor the reader is the constraint at that rate: Track H reached 360.709 GiB/s into
HBM on 32 rails, and C4 put a single reader's depth ceiling at 7.556 GiB/s — both far above
15.9 ÷ 8 ≈ 2 GiB/s per rank. **So this ADR's payoff is bounded by ADR-0033's gates 33.4 and 33.5,
which currently miss by ~2.7×.** The two named levers there — `ioEngine=uring` (the default is
still psync) and the `flushers` / `submitQueueThreshold` defaults — belong in the *same* arm as
G37.3, because an arm that discovers the tier is the wall afterwards pays for the node twice.

**The comparison arm changes, and the ratio will compress.** Today's 2.20× at 70B and 3.21× at 8B
are PACER-on-published-safetensors against a comparison loader reading the same published
safetensors — like for like. Against a pre-laid-out blob the honest comparison is a *sharded* load
format (vLLM's own, and the sharded mode of whatever loader the arm measures against — neither is
referenced anywhere in this repo yet, so both need verifying in the build pod before being named in
a result). Those are faster than the unsharded path, so the ratio
falls even as the absolute improves. Two rules follow: **report both arms** — published-safetensors
for the no-conversion story, pre-layout for peak — and **never divide a new absolute by the old
denominator**, a cross-run division already caught once on this path
(`vllm-placement.md` § validity).

**An artifact matrix appears.** One blob per TP degree, and per PP degree for the TP=8 PP=2 shapes
(DeepSeek-V3.1, Kimi-K2). 131.4 GiB × *k* configurations of storage, and a conversion that must run
before the first load — either on model onboarding or convert-on-first-load. That is adoption
friction and it is the honest cost of the peak number.

**The load-time memory ceiling improves.** Today `copies` needs `params_per_rank` + a 2 GiB window;
here the slab *is* the parameters. The recorded ceiling
`params_per_rank + 2 × largest_shard ≤ 79.65 GiB` becomes `params_per_rank`, which is what forced
Kimi-K2 to TP=16 under `copies` and TP=32 under `views`.

## Gates

Off-cluster first, because the correctness risk is entirely in the plan and the fingerprint and
neither needs a GPU, a daemon or a fabric.

| # | Gate | Where |
|---|---|---|
| **G37.1** | Plan capture is **complete and affine**: every parameter vLLM asks for is accounted, and a slab built from the plan is byte-identical to the parameters of an ordinary load on a small model | build pod, no GPU |
| **G37.2** | The fingerprint **refuses**: a manifest disagreeing on any field fails before any memory is registered, tested per field | build pod, no GPU |
| **G37.3** | 70B/TP=8 reported load **≤ 14 s**, with `body_spans` 0 and greedy-decode token ids identical to the published-safetensors arm **at the same TP** (the equality gate is only valid within a TP size — TP changes float summation order) | 1 × p5 |
| **G37.4** | The tier is demonstrably the wall in that same arm — store counters and `/proc/diskstats`, with psync vs `uring` as arms of one matrix | same arm as G37.3 |
| **G37.5** | An fp8 checkpoint converts, loads and decodes equal, proving the post-transform capture of § 2 | 1 × p5 |

G37.1 and G37.2 are the ones that decide whether this design is real. Nothing below them should be
built until they pass.
