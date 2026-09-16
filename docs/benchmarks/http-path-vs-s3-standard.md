# What the cache is worth on the plain HTTP/S3 path

A benchmark of PACER against the same objects read directly from regional S3, using an
unmodified third-party S3 load generator on both sides.

**Reproduce it:** [`run-http-path-benchmark.sh`](run-http-path-benchmark.sh) — the
reference implementation of everything below. It provisions no hardware and hard-codes
no bucket, account or cluster; you supply those.

---

## 1. Result

Measured on two `p5.48xlarge` instances in one availability zone, reading 16 MiB
objects from a general-purpose (regional) S3 bucket in the same region, 100 concurrent
GETs, three repetitions per arm.

| arm | throughput (median of 3) | observed range | **vs direct S3** |
|---|---:|---:|---:|
| **remote** — cache hit served across the network | **26.97 GiB/s** | 26.13 – 27.67 | **3.6×** |
| cached — cache hit already on this node (best case) | 32.63 GiB/s | 32.21 – 32.80 | 4.3× |
| **bypass** — direct from regional S3 (baseline) | **7.52 GiB/s** | 7.32 – 7.59 | 1.0× |

**PACER serves the same objects 3.6× faster than reading them directly from S3**, over
the same protocol, with the same client, on the same machine. Taking the least
favourable pairing of observed samples the ratio is 3.4×; the most favourable is 3.8×.

Two qualifications belong with the headline, not below it:

- **This is a throughput result, not a latency one.** Time-to-first-byte on the
  peer-plane path (27 ms) is *slightly worse* than direct S3 (24 ms) **at this load**.
  Both figures are measured with the cache at its throughput ceiling, where latency is
  dominated by queueing rather than by either path's intrinsic cost — so they are a
  like-for-like at c=100 and nothing more. § 7.2 gives the decomposition; § 8.7 states
  why a latency claim cannot be drawn from it. The cache's advantage is sustained
  bandwidth.
- **The bytes must already be cached.** Every number here is a warm-cache measurement,
  mechanically verified (§ 4.6). First reads still come from S3.

The number to quote is the **peer-plane** one. See § 2 for why the other is a best case
rather than the result.

## 2. What is being compared

Three arms. Every arm uses the **same** load generator at the same version, the same
concurrency, the same run length, the same object size and **the same keys**. The only
thing that varies is the path the bytes travel.

| arm | path | what it represents |
|---|---|---|
| **bypass** | client → `https://s3.<region>.amazonaws.com` → bucket | **The baseline.** What the workload gets today with no cache: a direct read from regional S3. No PACER process is in the path. |
| **remote** | client → node B's daemon → fabric → node A's cache | **The result.** A cache hit whose bytes live on *another* node and cross the network before reaching the client. |
| **cached** | client → node A's daemon → node A's cache | **A best case, not the result.** The bytes are already on the node the client is asking. |

**Why `remote` is the honest arm.** In a real deployment nothing arranges for the node
asking for a chunk to be the node that already holds it. Ownership is distributed, so
a given read is usually served from a peer and pays for the hop. `cached` deliberately
removes that hop; it is reported only to price the hop (§ 7), never as the headline.

**Why the baseline is direct S3 and not "PACER turned off".** Turning the cache off
still leaves the daemon proxying, which would measure a degraded PACER rather than the
alternative a user actually has. The bypass arm removes the daemon from the path
entirely: the client resolves and dials the public S3 endpoint itself.

**What "the same keys" means.** The daemon exposes an S3-compatible endpoint and maps a
bucket *alias* onto the real bucket. The cached arms address the alias and the bypass
arm addresses the real bucket, so the two name their target differently while reading
byte-identical objects at identical keys. Nothing is copied or re-encoded between arms.

## 3. Why the benchmark exists

An earlier measurement of this system's client path established an absolute figure —
throughput through the daemon on one instance — but had no baseline beside it, so it
could not say what the cache was *worth*. The one cached-vs-direct comparison that did
exist had been run on a 40 Gbps instance where **both** arms simply saturated the
network interface and the result was parity (1.02×). That says nothing about a machine
whose interface is not the constraint, which is the case this benchmark covers.

It also tests a specific objection: that any cache-vs-origin throughput ratio is an
artefact of client concurrency, and that raising concurrency on the direct path closes
the gap. § 6.3 settles it with a sweep rather than an argument.

## 4. Method

### 4.1 Hardware and software

| | |
|---|---|
| Instances | 2 × `p5.48xlarge` (192 vCPU, 100 Gbps/rail EFA), spot, **one AZ** |
| Homogeneity | Asserted, not assumed — the harness refuses a mixed pair, because the pool also offers a 200 Gbps/rail instance type and a mixed fleet would put a 2× network difference *inside* the measurement |
| Orchestration | Kubernetes; PACER as a DaemonSet, one daemon per node |
| Backend | General-purpose (regional) S3 bucket, **same region** as the cluster |
| Load generator | `minio/warp` **v1.3.1**, identical image on every arm |
| Object size | 16 MiB, equal to the daemon's chunk size |
| Keyset | 512 objects = 8 GiB, under a prefix containing nothing else |
| Cache memory tier | 16 GiB — larger than the keyset, so it is resident whole |
| Concurrency | 100 concurrent GETs per arm (swept 100/400/800 in § 6.3) |
| Run length | 180 s per arm; 30 s ramp, 90 s interior measurement window |
| Repetitions | 3 per arm, **interleaved** (§ 4.5) |

The load generator matters: it is a widely used third-party S3 benchmark, not
first-party code, and it is **the same binary on both sides of the comparison**. A
version difference between arms would sit inside the ratio invisibly — which is a real
hazard here, because versions before 1.2 cannot authenticate to real S3 at all under
temporary credentials (§ 8.2).

### 4.2 Warm-up, made explicit

Before any arm runs, the harness performs **one full pass** over the keyset through the
daemon on the owning node. This is a deliberate, separate step rather than something
the first arm does implicitly: an arm that warms itself measures a blend of *filling*
the cache and *serving* from it, and then reports the blend as serve throughput.

Consequence: **every number here is a warm-cache number.** § 8.1 states what that
excludes.

### 4.3 What is measured, and with what instrument

The reported throughput is the **load generator's own per-second median**, with ramp
and drain excluded.

- It is the *client's* view, which is the quantity the comparison is about — what a
  reader actually receives.
- It is the **median of per-second buckets**, not the whole-run average, because the
  average includes ramp-up and drain and would penalise every arm by a different amount
  depending on how fast it reaches steady state.
- It is used on **all three arms**, so the arms are instrumented identically. There is
  no path where a server-side counter substitutes for the client's number.

Latency is taken from the same report: request-latency percentiles and
**time-to-first-byte** percentiles. TTFB is reported separately from request latency
because the two answer different questions and only one of them is independent of
throughput: request latency for a 16 MiB object is dominated by transfer time, so it
largely restates the throughput result, whereas TTFB isolates how long the path takes to
*start* answering. § 7.2 reports what each actually did, including where the result does
not favour the cache.

**Server-side counters are used for validation, never for the headline.** The daemon's
Prometheus counters answer "was this really the path we claim?" (§ 4.6). One of them is
a specific trap: the cache-bytes counter increments only on a *local* tier hit, so on a
node that owns none of the keyset it reads ≈ 0 — using it for the `remote` arm would
report a collapse where there is none. This is why the client's number is the headline.

### 4.4 Statistics

Each arm's headline is the **median of 3 repetitions**, and the **min and max are
printed beside it**. The range is the honest precision of the result: this is shared,
virtualised hardware, and the same arm re-run on a *different* pair of instances of the
same type has been observed to differ by ~7% (§ 8.4). Quoting more significant figures
than the observed spread supports would misrepresent the measurement, so ratios are
given to two significant figures.

Median rather than mean: a single descheduled or throttled run should not move the
headline.

### 4.5 Repetitions are interleaved, not grouped

The order is `cached, remote, bypass, cached, remote, bypass, …` — not three of each in
a row.

Anything that drifts over the life of the fleet (S3 front-end warmth, a noisy
neighbour, thermal state, accumulated page cache) becomes a systematic offset **between
arms** if each arm's repetitions run back to back, and such an offset is
indistinguishable from the effect being measured. Interleaving converts drift into
*within-arm* variance, where the min/max range exposes it.

### 4.6 Validity assertions — every arm, every repetition

Each arm asserts, from the daemon's own counters, that it measured the path it claims.
A failed assertion aborts the run rather than producing a number.

| arm | asserted | why it matters |
|---|---|---|
| **bypass** | cache-bytes delta `== 0` **and** peer-serves delta `== 0` | Proves the daemon served no part of this arm — i.e. it really did bypass, rather than quietly reading through the cache. |
| **cached** | cache-bytes delta `> 0`, read-throughs `== 0` | Proves the bytes came from the local cache tier and **not one byte came from S3** during measurement. |
| **remote** | peer-serves delta `> 0`, read-throughs `== 0`, fallbacks `== 0`, RDMA fraction reported | Proves the bytes crossed the fabric from a peer (not served locally), came from cache rather than S3, and used the fast transport rather than silently degrading to a fallback path. |

"Read-throughs `== 0`" is the load-bearing one for honesty: it is the mechanical proof
that a cache arm is not being flattered by S3 quietly serving part of the traffic.

## 5. Procedure

The measurement is reproduced by [`run-http-path-benchmark.sh`](run-http-path-benchmark.sh).
It expects a cluster that already runs PACER and a bucket that already holds a keyset;
it provisions neither, because provisioning is site-specific and does not belong inside
a measurement.

```bash
KUBE_CONTEXT=<your-context>  NAMESPACE=pacer  RELEASE=pacer \
REAL_BUCKET=amzn-s3-demo-bucket  PREFIX=bench/  REGION=us-east-2 \
HOLDER=<node-a>  REQUESTER=<node-b> \
READONLY_SA=<read-only-serviceaccount> \
CONCURRENCY=100  DURATION=180s  REPS=3 \
  ./run-http-path-benchmark.sh
```

It then: preflights every input, warms the cache with one full pass, runs the three
arms interleaved for `REPS` repetitions asserting validity on each, and prints medians
with ranges and the ratios.

### 5.1 Preparing the keyset

The prefix must hold **equal-sized** objects and nothing else — the load generator
hammers everything it lists, so one stray object of a different size skews per-request
cost. 512 × 16 MiB is what was used here; any count whose total comfortably fits the
cache's memory tier reproduces the warm case.

### 5.2 The read-only credential requirement

The bypass arm needs real S3 credentials inside a pod, and the load generator has **no
AWS credential-chain support** — it takes static keys. The harness therefore resolves
credentials in an init container (`aws configure export-credentials`, which honours
whatever chain exists: pod-level identity, web identity, instance profile, or
environment) and hands the resolved *temporary* credentials, session token included, to
the load generator over a memory-backed volume. Nothing long-lived is created.

**Those credentials must be read-only, and this is not a formality.** The load
generator's own `--bucket` help reads *"ALL DATA WILL BE DELETED IN BUCKET!"*;
`--noclear` and `--list-existing` are what prevent it. If the bucket holds anything you
care about, one flag regression is all that stands between the benchmark and data loss —
so the arm is given credentials that **cannot** delete, and the harness refuses to run
under a service account whose role can. Verified before the first paid run by
attempting a delete and requiring `AccessDenied`.

## 6. Results

Every figure below is derived from the per-repetition artifacts (§ 9) by a script, not
transcribed by hand.

### 6.1 Throughput — the headline, n = 3 per arm

Two `p5.48xlarge` in one AZ, 16 MiB objects, c=100, 180 s per arm, interleaved.

| arm | r1 | r2 | r3 | median | range | spread | vs S3 |
|---|---:|---:|---:|---:|---:|---:|---:|
| cached (all-local) | 32.80 | 32.21 | 32.63 | **32.63** | 32.21–32.80 | 1.8% | 4.3× |
| **remote (peer plane)** | 26.97 | 26.13 | 27.67 | **26.97** | 26.13–27.67 | 5.7% | **3.6×** |
| bypass (direct S3) | 7.32 | 7.59 | 7.52 | **7.52** | 7.32–7.59 | 3.6% | — |

All GiB/s, load generator's per-second median. Ratio envelopes from the least and most
favourable observed pairings: **remote 3.44×–3.78×**, cached 4.25×–4.48×.

Request rates track throughput exactly, as they must at fixed object size: 2088 obj/s
(cached), 1726 (remote), 481 (bypass).

**The peer-plane hop costs 17.3%** (26.97 vs 32.63 on the same fleet). That is the price
of the bytes living on another machine, and it is the difference between the honest
number and the best case.

### 6.2 Latency

Median across the three repetitions.

| arm | TTFB p50 | TTFB p99 | request p50 | request p99 |
|---|---:|---:|---:|---:|
| cached (all-local) | **12 ms** | **49 ms** | **46.4 ms** | **97.5 ms** |
| remote (peer plane) | 27 ms | 138 ms | 54.9 ms | 170.2 ms |
| bypass (direct S3) | 24 ms | 125 ms | 178.6 ms | 649.4 ms |

TTFB was remarkably stable — identical to the millisecond across all three repetitions
of every arm (12/12/12, 27/27/27, 24/24/24), which is a good sign that it is measuring
a structural property of each path rather than momentary load.

### 6.3 The concurrency objection, tested

Measured on a **separate, single-node fleet** (so do not divide these against § 6.1 —
see § 8.4), **n = 1 per point**. Reported because the *effect* is far larger than any
plausible single-sample error.

| concurrency | cached | direct S3 | direct-S3 request p50 |
|---:|---:|---:|---:|
| 100 | 34.86 | 7.41 | 178 ms |
| 400 | 32.31 | 6.65 | 771 ms |
| 800 | 31.48¹ | 7.93 | **8382 ms** |

¹ Server-side counter; the client summary was lost to a harness race since fixed.

**Direct S3 does not convert concurrency into throughput from a single instance.** 8×
the concurrent GETs moved it by +7% (7.41 → 7.93 GiB/s) while multiplying request
latency **47×**. Its TTFB stayed flat at 24–26 ms throughout, which identifies the
mechanism: the round trip is unchanged and the added requests are simply queueing behind
a per-instance bandwidth limit.

The cache does not benefit from extra concurrency either — it is *fastest at the lowest
concurrency tested* (34.86 at c=100, falling to 31.48 at c=800), where its latency is
also best.

So the ratio is not an artefact of choosing a favourable concurrency: there is no tested
concurrency at which the direct path closes the gap, and none at which we would prefer to
be measured.

### 6.4 Reproducibility across fleets

The 2-node arms were run on two independently provisioned fleets on the same day:

| arm | fleet A (n=1) | fleet B (n=3, median) | delta |
|---|---:|---:|---:|
| remote | 27.56 | 26.97 | −2.1% |
| bypass | 7.51 | 7.52 | +0.1% |
| ratio | 3.67× | 3.59× | −2.2% |

The single-sample fleet-A ratio overstated fleet B's median by 2.2%, which is precisely
why the headline is given as **3.6×** and not to three significant figures.

## 7. Interpretation

### 7.1 What the throughput result means

A warm cache hit reaches the client 3.6× faster than the same object read from regional
S3 over the same protocol. The mechanism is visible in § 6.3: a single instance reading
directly from S3 is limited to ~7.5 GiB/s more or less regardless of how it asks, while
the cache path is limited by the node's network and memory rather than by the origin.

The cache is therefore doing something the client cannot do for itself by tuning. That
is the substance of the result, and it is why the concurrency sweep is part of it rather
than an appendix.

### 7.2 What the latency result means — including where it does not favour us

**Time-to-first-byte on the peer-plane path is slightly worse than direct S3: 27 ms vs
24 ms at p50, 138 ms vs 125 ms at p99.** The loss is real and reproducible *at this
load*, but it is **not** an intrinsic property of the path, and the reason matters
because it is easy to state wrongly.

At c=100 the cache is at its throughput ceiling, so per-request latency is queueing.
Little's Law reproduces both cache arms' request latency from throughput alone —
100 requests × 16 MiB ÷ 26.97 GiB/s = 57.9 ms predicted against 54.9 ms measured for
the peer arm (1.06), and 47.9 vs 46.4 ms for the all-local arm (1.03). Decomposing the
27 ms on that basis:

| component | value | evidence |
|---|---:|---|
| client-drain term, paid by both cache arms | ~12 ms | the all-local arm's TTFB, with no fabric involved at all |
| fabric term | ~15 ms | 27 − 12 |
| — of which actual wire time | 0.34–1.34 ms | 16 MiB at 4 rails / 1 rail of 100 Gbps |
| — of which queueing | **91–98%** | remainder |

The peer arm's TTFB (27 ms) sits essentially *on* half its drain time (29.0 ms), while
the all-local arm's (12 ms) sits well *below* half of its own (23.9 ms). That asymmetry
is the mechanism: a RAM read does not compete for the bottleneck resource, a contended
fabric transfer does.

So the ~12% TTFB deficit is ~15 ms of waiting for a saturated fabric, of which under
1.5 ms is the transfer itself. It is not a lookup cost (measured at 3.7 µs, 0.014% of
the figure) and not a round-trip cost.

**The comparison is also asymmetric in S3's favour**, which is worth stating plainly:
direct-S3 TTFB measured 24 ms at c=100 and 24–26 ms at c=100/400/800 — flat, because our
traffic is a negligible fraction of S3's capacity, so its queue is invisible to us. Our
TTFB includes our own saturation. Comparing them at equal client concurrency compares a
saturated system against an unloaded one.

Only the all-local arm improves TTFB (12 ms, 2× better than S3), and that arm is a best
case rather than the deployment shape.

**The request-latency advantage is the throughput advantage restated, not a second
finding.** Request time for a 16 MiB object is TTFB plus transfer, and transfer
dominates: 54.9 ms vs 178.6 ms at p50 is the 3.6× bandwidth difference expressing itself.
Counting it as an independent win would be double-counting one effect.

So the accurate summary is narrower than "faster on every axis": **PACER delivers
substantially more bandwidth per instance, at comparable time-to-first-byte.** For a
workload moving large objects in bulk that is the property that matters. For a workload
dominated by small-object first-byte latency, this benchmark does not demonstrate a
benefit, and § 8.3/8.5 mark that as untested rather than argued.

### 7.3 Why the tail matters more than the median for the baseline

The direct-S3 arm's request p99 is 649 ms against a 179 ms p50 — a 3.6× tail ratio. The
peer-plane path's is 170 ms against 54.9 ms, a 3.1× ratio, and its p99 in absolute terms
is *lower than the baseline's p50*. A consumer sizing timeouts or a loader with a
synchronous stage sees that difference more sharply than either median suggests.

## 8. Threats to validity, and what is not measured

Stated because a benchmark without this section is advertising.

### 8.1 Everything here is warm

The keyset fits the memory tier whole, the warm-up pass is explicit, and every arm
asserts zero backend read-throughs. So this measures **serving**, not **filling**. It
says nothing about cold first-touch — the first read of an object, which must come from
S3 on any cache. A workload that never re-reads anything would see none of this
benefit. Cold-path behaviour is a separate benchmark.

### 8.2 One client, and a version constraint that could have been invisible

All arms use one load generator. A different client with different connection reuse,
buffer sizing or parallelism would land elsewhere in absolute terms, though the *ratio*
is the claim and both arms share the client.

Versions of that generator before 1.2 have **no session-token flag at all**, so they
cannot authenticate to S3 with temporary credentials. The failure surfaces as an
authentication error mid-run, which is easy to mistake for a slow result. Both arms are
pinned to one image tag precisely so this cannot differ across the comparison.

### 8.3 One object size, whole-object reads only

16 MiB objects, chunk-aligned, read whole. Ranged reads take a different path in the
daemon and are out of scope. Earlier work on this system found object size did not move
the cached path materially (a 3.1× reduction in request rate moved throughput <1%), but
**the direct-S3 side was not swept on object size here**, so a large- or mixed-object
workload is untested.

### 8.4 Inter-instance variance is larger than intra-fleet variance

The `cached` arm, identical in configuration, read materially different absolute values
on two different pairs of the same instance type (~7% apart), and the direct-S3 arm's
TTFB moved likewise. Spot instances of one type are not identical machines.

**Therefore: only ratios computed from arms within one fleet are meaningful**, and every
ratio in § 6 is. Do not divide a number from one fleet by a number from another; the
reported min/max ranges bound run-to-run variance on a fixed fleet, not variance across
fleets, which is larger.

### 8.5 No fan-in

The `remote` arm is one reader pulling from one owner. It does not test many readers
converging on one owner's hot set, which is where a distributed cache's behaviour gets
interesting and where its advantage over a per-instance-limited origin should *grow*.
Untested here, and not claimed.

### 8.6 Single region, single AZ, spot

One region, one availability zone, spot capacity. A cross-AZ or cross-region baseline
would be slower than the one measured here, so this comparison is the **least**
favourable regional case for the cache.

### 8.7 Latency was measured only at saturation

Every arm ran at c=100, where the cache is at its throughput ceiling and latency is
therefore dominated by queueing (§ 7.2). **No latency figure here is the path's intrinsic
latency**, and none should be quoted as one. The intrinsic floor is unmeasured: a
low-concurrency sweep would give it, and from the decomposition it should be a few
milliseconds, but that is a prediction and not a result.

Two consequences. A latency-sensitive workload cannot size anything from these numbers.
And the TTFB comparison against S3 is a like-for-like only in the narrow sense of equal
client concurrency — not equal utilisation, since the same load saturates us and is
negligible to S3.

The serve path also carries no latency instrumentation: `pacer_delivery_chunk_seconds`
and the per-stage histograms cover the client-memory delivery path, not the HTTP body
path measured here, so the decomposition in § 7.2 is inference from throughput and two
arms rather than direct stage measurement. It reproduces both arms within 6%, which is
why it is stated — but a stage histogram on this path would settle it, and does not exist.

## 9. Artifacts

Every number in § 6 is backed by a result file and the load generator's **raw,
unedited report** saved beside it. The raw log is retained deliberately: the report
format has changed between generator releases, and a parser that silently matches
nothing yields an empty field rather than an error — so the primary evidence is kept,
not just the parse. The published medians, ranges and ratios are **derived from those
files by script**, not transcribed, so the arithmetic can be re-run against the
evidence.

[`run-http-path-benchmark.sh`](run-http-path-benchmark.sh) writes these for your own
runs, into `OUT_DIR`:

| artifact | contents |
|---|---|
| `<arm>-r<N>.txt` | per-repetition result: throughput, latency, TTFB, counter deltas |
| `<arm>-r<N>.warp.log` | the generator's full unedited output for that repetition |
| `<arm>.samples` | the per-repetition throughput values the median and range come from |

**Where our own artifacts live.** The 35 files behind § 6 are retained in the project's
internal repository alongside the harness that produced them, and are **not part of this
public snapshot** — the benchmark drivers are excluded from it because each is welded to
a specific cluster's node pools, storage and identity setup, and nobody outside could run
one as-is. That is a distribution choice, not a claim that the evidence is unavailable:
the raw reports exist, they are version-controlled, and they can be provided on request.
What ships here is the method and a runnable implementation of it, so the result can be
reproduced rather than merely inspected.

## 10. Provenance

Run on 2026-09-06. PACER daemon built from commit `3389b9c3` (x86-64). Load generator
`minio/warp` v1.3.1. AWS CLI 2.15.17 for credential resolution and the warm-up pass.
