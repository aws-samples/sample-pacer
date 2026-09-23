# ADR-0017: Chunk directory — sharded by the ring, soft state, sharer sets

Date: 2026-07-12 · Status: Accepted (partially implemented, B2)

## Context

Once copies are plural (ADR-0016), "compute the owner" no longer answers
"who has it?" — a requester needs the current holder set to pick a source.
The options with prior art:

- **Neighbor cache digests** (Summary Cache / Squid Cache Digests): each node
  gossips Bloom summaries to a small peer set. Rejected — documented dead
  end: digest staleness, false positives, and no story beyond the
  neighborhood without hierarchies that reintroduce hotspots. The field
  moved to deterministic placement precisely to escape this.
- **Centralized metadata master** (Mooncake's master, 3FS-on-FoundationDB,
  Meta Owl's trackers): correct and proven, but a new stateful service, a
  SPOF to engineer around, and unnecessary — PACER already has deterministic
  placement and membership.
- **Full directory replication**: at the target workload the whole directory
  is ~30–60 MB and would fit on every node, but keeping ~1000 replicas
  current during a restore storm (thousands of admits/s cluster-wide) is the
  gossip fan-out problem again. Rejected.

## Decision

The directory is **sharded across all nodes by the existing rendezvous hash**
(ADR-0014 contract): a chunk's home node *is* its directory shard. No new
service, no new placement function, one deterministic hop from anywhere —
this is directory-based cache coherence (ccNUMA directories, Meta Owl) at
cluster scale.

- **Entry**: `chunk_key → { (node, tier, generation), … }` — the sharer set.
  Tier ∈ {dram, nvme}, carried **only as a source-selection latency hint** (a
  requester prefers a dram holder over an nvme one); it is not a byte address and
  the holder still drives the transfer (ADR-0018). `generation` is a per-holder
  admission counter that lets a re-admitted chunk be distinguished from a stale
  registration.
  **Deliberately no byte addresses**: the directory maps chunk → node+tier
  only. Byte-level location stays private to the holder (foyer relocates and
  evicts entries; exposing offsets cluster-wide would fight the library and
  poison remote readers — see ADR-0018 for why holders drive the transfer).
- **Reader contract vs. home bookkeeping — the split that reconciles v1 and v2.**
  What a *reader* needs is only the **holder set + a tier hint**; per-holder `tier`
  and `generation` are the *home's* private CPU-side bookkeeping to fold
  out-of-order `admit`/`evict` announcements, not reader-facing correctness state.
  This is what lets ADR-0020's one-sided v2 use a **sharer bitmap** (pure set
  membership, one bit per node) with a single entry-level `tier_bits` (coarse
  "dram present") + entry-level `generation` (admission epoch, paired with the
  seqlock): a bitmap cannot carry per-holder tier/generation, and it does not need
  to. Picking a "wrong" holder is a benign `NotCached` → retry because chunks are
  immutable (ADR-0015 key embeds size+index), so the tier hint is advisory and the
  generation need not be per-holder on the wire. v1 RPC *may* still list
  `(node, tier, gen)` per holder for a workload that wants finer source selection;
  v2 deliberately trades that granularity for NIC-served lookups. ADR-0020 owns the
  v2 layout; this ADR owns the semantic (re-admit ≠ stale registration).
- **Maintenance**: holders announce `admit(chunk, tier, gen)` /
  `evict(chunk, gen)` to the chunk's home. Update traffic is proportional to
  fills/evictions, **not reads**.
- **Soft state, no persistence, no consensus.** A home crash loses its
  shard; the rendezvous re-home (only that node's keys move, ADR-0014)
  starts empty and repopulates from read-through and periodic holder
  re-announcements. A stale entry costs one failed fetch and a fallback —
  the same failure envelope as Phase 2's `NotCached` → backend path
  (ADR-0012 rule 3). Correctness never depends on directory freshness.
- **Read path**: local miss → compute home → lookup (RPC now; one-sided READ
  per ADR-0020 later) → fetch from a listed holder (ADR-0018) → optionally
  admit locally (ADR-0016) and announce. When the home is itself a holder —
  the common case for warm-but-not-hot chunks, since homes fill on first
  miss (ADR-0012) — lookup and fetch collapse into one hop.
- **Miss path**: empty sharer set → the home performs/delegates the S3
  read-through (ADR-0012 unchanged, per chunk), registers itself, replies.
  The home remains the fill serialization point, so the single-flight
  property extends cluster-wide per chunk.
- **Invalidation** (ADR-0007/0016): the writer invalidates via the home,
  which fans out to its listed sharers and clears the entry — the sharer
  set is exactly the "who can hold this" bound that replaced ADR-0012's
  two-node argument.

## Trade-offs

Pros:
- One deterministic hop to find any chunk from any of 1000 nodes — no
  neighborhood limit, no gossip convergence, no new infrastructure.
- Inherits ring failure semantics wholesale (ADR-0014); soft state means
  the recovery story is "repopulate", not "restore".
- Metadata write load scales with cache churn, not read QPS.

Cons:
- One extra metadata hop on warm-read misses when the home isn't a holder
  (mitigated: home-fills-first makes home-is-holder the common case;
  ADR-0020 drops the hop cost to ~5 µs).
- Directory lookups for a scorching chunk concentrate on its home — the
  hotspot recurses at the metadata layer. Bounded by entry size (bytes, not
  chunk bytes) and removed entirely by ADR-0020's NIC-served lookups.
- The home is simultaneously the chunk's data owner, its directory shard, and its
  first filler — so a storm-hot chunk is hot on both data and metadata at one node.
  This co-location is deliberate, not an oversight: **decoupling the directory from
  the data owner would not remove the hotspot, only relocate the metadata half to a
  guaranteed-different node while taxing every warm read with a mandatory second
  hop.** Co-location instead lets the common warm case (home filled first, so
  home-is-holder) collapse lookup+fetch into one hop. The heat is split by
  *mechanism*, not by node: data fan-in → ADR-0016 replication; metadata fan-in →
  ADR-0020 NIC-served lookups (with no replication floor today — see ADR-0016).
- Sharer sets are advisory; a burst of evictions can briefly strand readers
  on retry/fallback. Accepted — bounded by the fallback path.

## Knobs

- `reannounce_interval` (default **5 min**) — holder re-registration period;
  the repopulation bound after a home crash and the staleness bound on
  entries whose evict-announce was lost. Lower = fresher directory, more
  metadata traffic.
- `max_sharers_tracked` (default **16**) — cap on sharer-set size per entry;
  beyond it the home records "widely held" and stops listing (readers pick
  co-homes/random known holders). Bounds entry size and invalidation fan-out
  for storm-hot chunks; must be ≥ `replication_r` (ADR-0016).
- Lookup transport: `rpc` (v1) vs `rdma_read` (v2, ADR-0020) — per-cluster
  flag during Phase 3 bring-up, `rdma_read` as the end state on EFA nodes.
- **Open (Phase 3 benchmark)**: heat observability once ADR-0020 blinds the
  home to lookups — sampled hit reports piggybacked on announcements vs.
  holder-side detection. Decide with storm data; record as an amendment.

## Implementation status (B2)

Landed in `pacer-ring::directory` + the peer plane (design notes:
planning/12-chunk-directory.md):

- **In**: the sharded soft-state shard (`Directory`/`SharedDirectory`) with
  sharer sets `node → (tier, generation)`, generation-folded admit/evict
  (out-of-order safe: a stale message can neither clobber a fresher admit nor
  undo a re-admit), the `max_sharers_tracked` → "widely held" cap, and the v1
  RPC transport (`Announce` = admit/evict, `LookupSharers`) on `pacer.v1.Peer`
  alongside `Invalidate`, which now also clears the home's entry. **Admit-on-
  fill is live**: both owner-fill paths (proxy `FillCtx`, peer read-through)
  self-register locally — a chunk's home *is* its owner, so this is a local
  shard write, not an announce RPC. `max_sharers_tracked` is an ADR-0013 knob
  (`PACER_MAX_SHARERS_TRACKED` / `cluster.max-sharers-tracked`).
- **Landed in B3** (ADR-0016, planning/13):
  - *Lookup-on-read*: the read path's `chunk_sources` now queries the home's
    sharer set and merges layer-1 admitters (DRAM-hinted first) into the source
    list; a lookup miss/failure is non-fatal (the R homes are always valid).
  - *Remote-announce*: a layer-1 requester-local admission fire-and-forget
    `announce_admit`s the admitting node to the chunk's home, so other nodes can
    fetch from it. Invalidation fans out over the resulting sharer set ∪ the R
    homes, awaited (ADR-0007).
- **Still deferred, with triggers**:
  - *Evict-announce on eviction*: foyer's `EventListener::on_leave` fires on
    the DRAM→NVMe **demotion**, not true eviction, so it cannot drive evicts
    (a demoted chunk is still servable — announcing an evict would wrongly
    strand readers). Evict-announce therefore stays best-effort: it rides
    invalidation (done) and the periodic re-announce (`reannounce_interval`,
    landed — see amendment below). This is exactly the ADR's soft-state
    contract — a lost evict costs one failed fetch + fallback, never a wrong
    serve.
  - *v2 one-sided RDMA lookup* (ADR-0020): not built; the RPC lookup is the v1
    end state B2 ships.

## Amendment (2026-08-13, Phase 4 A5): generation persistence + re-announce loop

Two soft-state gaps closed together, because they share one failure —
**a restart makes a still-present holder invisible to its home.**

- **R4 — generation must survive a process restart (`pacer-cache::admission`).**
  The layer-1 `AdmissionGate` vended `generation` from 0 on every process
  start. A pod that restarts keeps its K8s node name, hence its `node_id`, so
  its fresh admits (gen 1, 2, …) are *lower* than the generations its previous
  instance already announced to the homes. The fold (keep-higher-generation,
  above) then drops every re-admit as stale, leaving the restarted holder
  invisible until each chunk's home entry ages out. Fix: persist a
  high-water mark to a file beside the foyer cache blocks (foyer reuses that
  dir across restarts, so the mark lives exactly as long as the chunks it
  describes), reserve a block of generations ahead of the mark before vending
  (`GENERATION_RESERVE_BLOCK`), and on startup resume strictly above the
  reserved mark. Crash-safe: a crash between reservations loses at most one
  block, never re-vends a used generation. Absent/corrupt file → resume from 0
  (first boot). Unit-tested (a "restarted" gate vends strictly above the prior
  instance's last, and above the whole reserved block).
- **`reannounce_interval` re-announce loop (`pacer-daemon`).** A cluster-gated
  background task sweeps this node's holdings once per `reannounce_interval`
  and re-asserts each chunk to its *current* home: home==self → idempotent
  local shard write; home==peer → `announce_admit` over the transport (a
  failure is logged and retried next sweep). Holdings come from two bounded
  soft-state sources merged by chunk key — the local directory shard
  (`Directory::holdings_of`, chunks this node homes/co-homes and filled) and
  the layer-1 admission record (`AdmissionGate::admitted_holdings`, peer copies,
  carrying the R4-persisted generation, which wins on the rare key present in
  both). No unbounded structure is introduced. This converges a restarted home
  within one interval without waiting on organic re-reads, and — because it
  routes by the live ring owner — also re-homes co-home fills recorded locally
  to their true remote home. Outcomes are logged (`total`/`local`/`remote_ok`/
  `remote_err`), no new metric. Knob follows ADR-0013 (`PACER_REANNOUNCE_
  INTERVAL_SECS` / `cluster.reannounce-interval-secs`, default 300 s).
