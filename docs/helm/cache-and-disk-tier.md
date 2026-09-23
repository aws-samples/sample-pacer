> Design notes for the `config` and `cache` keys in [`deploy/helm/pacer/values.yaml`](../../deploy/helm/pacer/values.yaml). The values file keeps one short comment per key; the reasoning, the measurements and the failure history live here.

# The cache: backend, tiers and the foyer disk engine

## Config-file precedence (ADR-0013)

`config:` is rendered into a ConfigMap (`templates/configmap.yaml`) mounted at
`/etc/pacer/config.yaml` ([ADR-0013](../adr/0013-yaml-config-file-layered-over-env.md)).
Precedence: built-in defaults < this file < `PACER_*` env. A ConfigMap edit rolls the
DaemonSet via a checksum annotation.

The ConfigMap template states the same rule from the rendering side: this file is the
**middle** precedence layer. Only release-wide values live here; per-pod inputs (node
name, namespace) stay env (downward API) in the DaemonSet. The daemon reads it once at
startup — the checksum annotation on the DaemonSet is what rolls pods when this changes.

## Backend shape (ADR-0023) and bucket aliasing

- **`backendType`** (default `express`) — `"express"` (S3 Express One Zone directory
  bucket, same-AZ — the [ADR-0002](../adr/0002-s3-express-one-zone-same-az-backend.md)
  default) or `"standard"` (S3 Standard regional bucket, cross-AZ, full functional
  parity). Chosen explicitly, not sniffed from the bucket name. Standard drops the
  Express same-AZ latency weld (parity is functional, not performance-equivalent — see
  [ADR-0023](../adr/0023-pluggable-s3-backend-type-express-or-standard.md) /
  the D2 benchmark).

- **`s3Endpoint`** — zonal endpoint of the S3 Express directory bucket's AZ (same AZ as
  this nodepool). For a `"standard"` backend, the regional endpoint (usually left empty
  for default resolution). Empty = regular S3 endpoint resolution.

- **`forcePathStyle`** — force path-style addressing (test backends: localstack/minio).

- **`awsRegion`** — region for the backend SDK. Required in-cluster when pods can't
  reach IMDS (metadata hop limit 1). Empty = ambient resolution. Passed as env (`AWS_*`
  is the SDK's own contract), not part of the config file.

- **`bucketMap`** — bucket aliases, alias -> real bucket. On `"express"`, clients MUST
  NOT address directory buckets by their real names (`*--x-s3` flips SDKs into
  Express-specific addressing/signing that breaks a proxy endpoint); they use the alias
  and the daemon rewrites it. On `"standard"` the real name is an ordinary regional
  bucket, so aliasing is optional. Example:

  ```yaml
  bucketMap:
    cache: my-bucket--use2-az1--x-s3
  ```

## Where the cache lives on the node

- **`config.cacheDir`** (default `/var/cache/pacer`) — the path inside the daemon
  container where the chunk cache lives.

- **`cache.hostPath`** (default `/mnt/k8s-disks/0/pacer-cache`) — the host path for the
  daemon's on-disk chunk cache. It must be a dedicated **subdirectory** of the NVMe
  array Karpenter's `instanceStorePolicy: RAID0` mounts, **not** the array root: that
  same filesystem (`/dev/md127`) simultaneously backs `/var/lib/kubelet`,
  `/var/lib/containerd`, `/var/log/pods`, and `/var/lib/soci-snapshotter-grpc` on this
  node (confirmed via `mount` on a running node — they are bind/sub-mounts of the
  identical device, not separate volumes). A hostPath pinned at the array root means any
  recursive delete under it (an operator's `rm -rf`, a benchmark cache-reset job, a bug)
  can also delete the container runtime's own state — confirmed **2026-08-04**: a
  benchmark wipe step took down all 4 EFA nodes' containerd
  (`mkdir /var/lib/containerd/...: no such file or directory`), forcing a full Karpenter
  replacement of the fleet. The daemon's chown-cache init container
  ([`daemonset.yaml`](../../deploy/helm/pacer/templates/daemonset.yaml)) already scopes
  its `chown` to this path, so narrowing it to a subdir costs nothing and contains any
  future recursive op.

## Tier sizes: `memCapacity`, `diskCapacity`, `blockSize`

- **`memCapacity`** (default `1GiB`) — the RAM tier's capacity.
- **`diskCapacity`** (default `100GiB`) — the on-disk tier's capacity.
- **`blockSize`** (default `1GiB`) — foyer disk block: eviction unit, max on-disk entry
  size, and fd divisor (one fd per block; keep `diskCapacity`/`blockSize` under the
  nofile limit).

## Admission: `minObjectSize`, `maxObjectSize`, `chunkSize`

- **`minObjectSize`** (default `4MiB`) — the floor below which an object is served
  without going through the chunked cache.

- **`maxObjectSize`** — optional whole-object admission cap
  ([ADR-0015](../adr/0015-chunk-granular-caching.md)). Unset (the default) →
  unbounded: the chunked cache stores an object of any size as chunk-size pieces
  distributed across the ring — the whole point of chunking is a checkpoint far larger
  than any node's RAM. Set a value only as an operator safety valve to proxy
  pathologically large objects through uncached; it is decoupled from `blockSize`
  (entries are chunks, never whole objects). The ConfigMap's own version of this comment
  adds a detail worth keeping: per-read memory is bounded to
  `fill_parallelism × chunk-size`, so this is a policy valve, not a disk-persistence
  limit.

- **`chunkSize`** — cluster cache chunk size (ADR-0015): the unit of placement, fetch,
  and invalidation. Embedded in every chunk key, so changing it orphans (never
  corrupts) existing entries — a cache-flush event. Pin per cluster; never mix values
  across a rolling update. Empty → the daemon default (16 MiB).

## The flush buffer

- **`flushBufferSize`** — foyer DRAM→NVMe flush buffer. Unset/empty = daemon
  auto-sizes to 2×`blockSize`. Entries larger than this buffer are **silently dropped**
  on demotion (never reach the disk tier), so only set it explicitly if you know it
  exceeds the largest cacheable entry.

## I/O engine: psync or io_uring

- **`ioEngine`** (default `psync`) — disk I/O engine: `psync` (portable default) or
  `uring` (Linux, NVMe depths).

- **`uring`** (only used when `ioEngine` is `uring`) — io_uring tuning, sized for NVMe
  queue depths; sqpoll off (a poll thread burns a core):

  ```yaml
  uring:
    threads: 4
    ioDepth: 256
  ```

## foyer storage tuning

`config.tuning` holds the foyer storage-engine tuning knobs. These are no longer
foyer's own defaults: the 2x2 in
`c4-foyer-readpath.md` measured
foyer's defaults against these and the deltas replicated across both `promotion`
settings, so the tuned side is what ships. Set a knob to `0`/`""` to get foyer's value
back — that is what a control arm asks for, and it is still the historical behaviour.

The ConfigMap emission follows the same rule: a `0` still means "keep foyer's own
default", so a knob left at `0` is simply not emitted and each one stays movable alone
in a benchmark arm. The `tuning:` block itself is now always present, because
`submit-queue-threshold` is derived rather than defaulted to foyer's 16 MiB — foyer's
value silently drops demotions at any production chunk size
(c4-foyer-readpath.md), which is not a default worth preserving for the sake of not
rolling pods.

### `flushers` is a read-path knob

⚠ `flushers` is a **read-path** knob as much as a write one, and it is the one that
bounds per-node read throughput. foyer opens one file per block and picks the io_uring
worker for a read by `partition.id() % uring.threads`; one flusher appends every entry
into one block until it fills. So at `blockSize` 1GiB and a 16MiB chunk, 64 consecutive
chunks share a block and read back through a **single** io_uring shard, whose
page-cache memcpys serialize — which is why `uring.threads` 4 → 32 bought only **+16 %**
(`c4-fanout-depth.md`) and why the
per-node ceiling sat at **~4.2 GiB/s** with **36 ms** to read a page-cache-resident 16
MiB chunk. Spreading that read over `flushers` shards is worth **+52.8 % and +47.2 %**
on delivery, replicated (c4-foyer-readpath.md).

Default is `flushers: 32` — 32 to match a 32-thread uring engine one-for-one: 32
concurrently-open blocks map onto 32 shards under `partition.id() % uring.threads`.
Raising this past `uring.threads` buys nothing; keep the two in step.

When the ConfigMap emits it, its own comment restates the mechanism: blocks the disk
tier flushes into concurrently — and therefore how many io_uring shards a **sequential**
read spreads over, which is the part that is not obvious. One flusher appends
everything into one block until it fills, foyer opens one file per block, and it picks
the io_uring worker for a read by `partition.id() % uring.threads` — so with the default
1, 64 consecutive 16-MiB chunks share a block and read back through ONE shard,
serializing their page-cache memcpys however many threads exist. That is why
`uring.threads` 4 -> 32 bought only +16 %.

### That read-path win is io_uring-only

⚠⚠ That read-path win is **io_uring-only**, and `ioEngine` above defaults to `psync`.
The shard mapping is `partition.id() % uring.threads` inside foyer's uring engine; its
psync engine issues one `pread` per read on a `spawn_blocking` pool with no
partition→worker mapping at all, so it has no funnel for `flushers` to relieve. Under
psync these two knobs buy write concurrency only — one flusher serializes every
demotion, which is worth having, but it is not the +50 %. Nothing has yet measured
psync against a tuned uring on a checkpoint read, and on a node whose page cache holds
the whole working set (every p5 arm so far) psync's parallel preads may never have had
the problem. Until that arm runs, this block is sized for uring and the engine default
is left alone.

### `reclaimers`

Reclaim frees whole blocks for reuse, so one reclaimer against 32 writers is what runs
the engine out of clean blocks under a sustained fill. 8 is a quarter of `flushers` —
enough to keep up, without another 32 spinning tasks. Default is `reclaimers: 8`.

The ConfigMap's emitted comment: concurrent block reclaimers. Keep in step with
`flushers`: one reclaimer against N writers is what runs the engine out of clean
blocks.

### `submitQueueThreshold`: a data-loss fix, not a rate knob

In-flight DRAM→NVMe write budget. Empty → **derived** as `flushers × chunkSize × 4`
(see `pacer.submitQueueThreshold` in `_helpers.tpl`), which is 2GiB at the shipped
defaults. foyer's own default is 16MiB — exactly ONE 16MiB chunk entry — and an enqueue
past it is silently dropped, so the chunk never reaches disk and a later read misses to
a peer or the backend. Sizing it took
`foyer_storage_inner_op_total{op="channel_overflow"}` from **4229/2244 to exactly 0,
twice**; it is a data-loss fix, not a rate knob, and it holds on either engine. Set `0`
for foyer's 16MiB.

The derivation lives in
[`pacer.submitQueueThreshold`](../../deploy/helm/pacer/templates/_helpers.tpl), and its
own reasoning is worth folding in here:

- It is derived rather than left to foyer's own default because that default is 16 MiB,
  which equals exactly one chunk entry at the shipped `chunkSize`: about two chunks fit,
  and every further enqueue is silently dropped (foyer counts
  `storage_queue_channel_overflow`, the entry never reaches the disk tier, and a later
  read misses to a peer or to S3). Not a tuning preference —
  `c4-foyer-readpath.md` recorded
  4229 and 2244 drops on foyer's default and exactly 0 once this was sized, twice,
  deterministically.
- The rule is the write fan-out actually wanted, `flushers × chunkSize × depth`, so it
  tracks both knobs it depends on: raising `chunkSize` (a 64 MiB chunk would otherwise
  be 4x past foyer's whole default) or `flushers` cannot silently re-create the drops.
- Resolution order:
  1. `config.tuning.submitQueueThreshold` set explicitly -> that value, whatever it is,
     including `0` for "keep foyer's own 16 MiB" (a control arm has to be able to ask
     for the historical behaviour).
  2. otherwise -> derived, as above.
- "Set" must mean SET, including set to zero: Helm coerces `--set
  config.tuning.submitQueueThreshold=0` to the integer 0, which Go templates call
  falsey, so a bare `if` would re-derive a budget for the operator who asked for
  foyer's default. The template tests for absent (nil) or empty string instead.
- foyer's own default when `flushers` is 0/unset is ONE flusher, so that is the floor
  the derivation uses — never 0, which would emit a threshold of 0 and mean "foyer's
  default" to the daemon, i.e. the bug this helper exists to close.
- The depth term is fixed at **4**, because that is the reader look-ahead the C4 ladder
  found to be the knee
  (`c4-fanout-depth.md`: depth 4 is
  where delivery peaks, depth 8 buys +5.4 % for double the queue), so
  `flushers × chunk × 4` is the demotion burst a fill at the knee can actually produce.

The ConfigMap's emitted comment restates the operational consequence: an enqueue
arriving when this much is already unwritten is silently dropped (foyer counts
`storage_queue_channel_overflow`, the chunk never reaches disk, and a later read misses
to a peer or the backend). foyer's own default is 16 MiB — one chunk entry — which is
why it is derived here and not defaulted: sizing it took the drop count from 4229/2244
to exactly 0, twice (c4-foyer-readpath.md). `0` restores foyer's 16 MiB.

### `storageRuntimeThreads`

Worker threads for a runtime dedicated to the disk tier. `0` shares the main runtime
(foyer's default), putting every entry's XxHash64 + decode copy (**6.5 ms per 16MiB
chunk**) on the same threads as the proxy and the RDMA pumps.

Left at `0` deliberately while the two knobs above ship. It was part of the same
measured bundle, so its own contribution is confounded with `flushers` and cannot be
attributed — and unlike those two it changes the daemon's threading, which is not
something to default on an unattributed delta. `16` is the value the C4 arm used if you
want to isolate it.

The ConfigMap's emitted comment adds the contention it is isolating from: unset/0
shares the main runtime (foyer's default), which puts every entry's XxHash64 and decode
copy — 6.5 ms per 16-MiB chunk — on the same threads as the proxy, against RDMA
completion pumps that already burn **~14 cores**.

## Promotion

**`promotion`** — whether a chunk read promotes a disk hit into the RAM tier:
`on-disk-hit` (default, foyer's behaviour) or `never`. Empty → `on-disk-hit`.

The ConfigMap's emitted comment carries the trade: `on-disk-hit` (unset) is foyer's own
behaviour — its `get` is `get_or_fetch` underneath and inserts the loaded value at the
HOT end of the LRU. `never` serves the hit and drops it. On a one-pass checkpoint sweep
the RAM tier hit **5.3 %**, so the promotion mostly evicts fresh fill data that then has
to be written down — but `never` also gives up foyer's single-flight coalescing of
concurrent misses for one key, so it is a workload choice, not an upgrade.

## Which implementation owns the disk tier (ADR-0033)

**`diskTier`** — which implementation holds chunk bodies on disk
([ADR-0033](../adr/0033-chunk-store-owns-the-disk-tier.md), defaulted by
[ADR-0038](../adr/0038-chunk-store-is-the-default-disk-tier.md)): `"foyer"`
(an entry per chunk) or `"store"` (one `pread` into a registered frame).

**Empty → DERIVED, and the condition matters:** `"store"` wherever an ADR-0028 cache slab
is derived — i.e. wherever `efa.hugepages` is set, the same gate as
`cluster.cacheSlabBytes` — and `"foyer"` where none is. Setting either explicitly always
wins, in both directions.

Why the condition rather than a bare `"store"`: the store's read is `O_DIRECT` into a
registered frame, `O_DIRECT` needs a page-aligned buffer, and a heap `Vec` is not
aligned. So on a node with **no slab** every store read silently takes the BUFFERED
descriptor — **26.9-34.9 ms** per 16 MiB against **17.3** direct, through the page cache
the tier exists to bypass, visible only in
`pacer_cache_slab_heap_fallbacks_total`. **The daemon REFUSES TO START on that
combination** rather than serving half the rate that was asked for; the refusal names both
escapes (give the node a slab, or choose `"foyer"`). Since the chart's own defaults are
`efa.enabled: false`, an unconditional `"store"` would have made the default install fail
to boot.

The measured case for `"store"` where a slab exists: **23.750 GiB/s against foyer's 14.5
on the same node (1.633×)**, and foyer served **0.005 %** of its bytes from the drives at
all — its "disk tier" was the page cache
(`mountpoint-vs-pacer.md`). The
device itself reads a 16 MiB chunk in **1.335 ms** and the array gives **~48.7 GiB/s at
depth 16** (`nvme-device-truth.md`),
so 23.750 — taken with unbounded read depth, before `storeReadConcurrency` existed — is a
FLOOR for the store and the gap between the tiers is understated. ⚠ n=1, one instance, one
concurrency, one object size.

Object headers stay in foyer either way, and `promotion` above stops applying to chunks.
⚠ On a default `"store"` deployment that also makes `ioEngine`, `uring.*` and the
`tuning.*` block **header-cache** controls — so the **+50 % `flushers`** result must not be
carried across, it was not measured on a header cache.

⚠ **`diskCapacity` must cover the WHOLE working set on a `"store"` node**, and this now
binds by default wherever hugepages are set. foyer's effective capacity was
`memCapacity + diskCapacity` (a chunk lived in RAM and demoted); the store has only
`diskCapacity`. Under-sizing it evicts mid-warm and sends those chunks to S3, which reads
as the store being slow when it is the tier being too small.

The ConfigMap's emitted comment adds the cost breakdown for `"foyer"`: an entry per
chunk costs a fresh page-aligned 16 MiB allocation, one io op for the whole entry, an
XxHash64 over every byte and a decode copy — measured at 40-75 ms for a 16 MiB entry the
device serves in 1.335 ms (nvme-device-truth.md). `"store"` is one `pread` into a
registered frame. Object headers stay in foyer either way; `promotion` applies only to
`"foyer"` — the store IS the tier, so it has nothing to promote into.

⚠ Not a hot flip: the two keep chunk bodies in different files, so switching leaves the
other's bytes behind as garbage until they are evicted or the dir is wiped. Correct
either way (an invalidation clears both), but a switched node starts cold for chunks.

## Conditional GETs (ADR-0039)

**`conditionalGetFromCache`** (default `true`) — whether a GET carrying `If-Match` may be
served from the cache when the ETag the client named equals the one this node resolved
([ADR-0039](../adr/0039-if-match-may-be-served-from-cache.md)).

**Why it defaults on:** Mountpoint-for-S3 puts `If-Match` on **every** GET it issues, so with
this off its measured hit rate against PACER is **0 %** and 100 % of its reads bypass —
209 493 of 209 493 in the arm that found it
(`mountpoint-vs-pacer.md`). Any client
that sends the header defensively was in the same position.

**A mismatch passes through and is never a 412.** S3 may hold the ETag the client named while
this node holds an older one, so answering 412 would turn a serviceable request into a hard
failure about state this cache does not own. That means the feature can only ever cost the
optimisation, never correctness.

⚠ **What it trades, and the reason it is a knob.** On a hit the ETag compared against is the
one in **our cache**, not a fresh `HeadObject` — so a client cannot use `If-Match` through
PACER to *detect* that an object was replaced in place. It is not a new staleness class (a
plain GET already returns cached bytes for a replaced object), and the invariant the header
exists for is preserved (every range of a multi-range read resolves against one cached header,
so a read is never a mix of versions). ADR-0015 requires a new name per version, under which
the condition can never legitimately fail. Set `false` for strict semantics: every conditional
GET then bypasses, exactly as before ADR-0039.

`if-none-match`, `if-modified-since`, `if-unmodified-since`, `versionId` and SSE-C still
bypass unconditionally — unchanged, and not covered by this knob.

**How to confirm it engaged:** `pacer_conditional_get_served_total`. A client whose every GET
is conditional looks identical in `pacer_cache_hits_total`/`pacer_cache_bypass_total` whether
the ETag check honoured the request or turned it away, which is why this counter exists.

## Chunk-body verification

**`verifyChunkBody`** (default `false`) — verify a chunk body's CRC32 on every store
read. Costs **~1.1 ms** per 16 MiB against a 1.335 ms device read, to re-check what
NVMe end-to-end protection and the client's delivery digest already cover — so it is
off, and the slot's KEY is checked regardless (that one catches OUR bugs, and is not
optional). The CRC is written either way, so turning this on needs no migration.

The ConfigMap's emitted comment quantifies the cost as a rate, not just a latency:
off by default, deliberately, because a full pass over a 16 MiB chunk is ~1.1 ms against
the 1.335 ms the device takes to read it, so it is a **~45 % throughput tax** to re-check
what NVMe end-to-end protection already covers and what the client's delivery digest
checks again end to end. What is checked regardless is the slot's KEY, which is the
failure this daemon's own bookkeeping can cause (an index naming a slot that holds
another key's bytes). That check is never optional. See ADR-0033 § Integrity for the
full trade.

## Runtime threads and log level

- **`workerThreads`** (default `0`) — tokio worker threads; 0 = one per core (tokio
  default).

- **`rdmaWorkerThreads`** (default `0`) — tokio worker threads for the dedicated RDMA
  serve-path runtime (EFA completion pump + holder WRITEs), isolated from the main
  runtime so client-role S3 GETs cannot starve RDMA serve scheduling. 0 = one per core;
  only used on EFA nodes in cluster mode. A small explicit count (e.g. 2-4) is the
  intended production setting — the serve path is latency-bound and should not
  over-subscribe cores against the main runtime.

- **`logLevel`** (default `info`) — the daemon's log level.

## See also

- [`memory-model.md`](memory-model.md) — how `memCapacity`, the RDMA arenas and the
  ADR-0028 slab add up against the container's memory limit.
- [`efa-and-rdma.md`](efa-and-rdma.md) — the RDMA serve-path runtime that
  `rdmaWorkerThreads` isolates from the main one.
- [ADR-0013 — YAML config file layered over env](../adr/0013-yaml-config-file-layered-over-env.md)
- [ADR-0015 — Chunk-granular caching](../adr/0015-chunk-granular-caching.md)
- [ADR-0023 — Pluggable S3 backend type (Express or Standard)](../adr/0023-pluggable-s3-backend-type-express-or-standard.md)
- [ADR-0033 — Chunk store owns the disk tier](../adr/0033-chunk-store-owns-the-disk-tier.md)
