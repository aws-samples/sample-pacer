# ADR-0020: Directory lookup as a one-sided RDMA READ — fixed-slot table ABI

Date: 2026-07-12 · Status: Accepted (design frozen; **activation benchmark-gated
per ADR-0008**) · Amends ADR-0017 · v1 RPC lookup ships as the default; v2
one-sided READ flips on only if the restore-storm benchmark shows the home-CPU
lookup wall is real *after* ADR-0016 replication and ADR-0017 home-fills-first
have drained what they can. The ABI below is frozen so that if/when v2 flips,
rolling updates across it degrade rather than corrupt.

## Context

ADR-0017's directory serves lookups by RPC (v1). During a restore storm,
lookup QPS on the homes of hot chunks spikes ~1000× while sharer sets churn —
and even eRPC-class messaging (ADR-0019) spends home CPU per lookup. A
one-sided RDMA READ served entirely by the home's NIC costs the home zero CPU
and runs at NIC message rate (~10⁷/s).

One-sided lookups are viable only under conditions this directory happens to
meet (the general case, per eRPC/FaRM experience, is a trap): lookups must
complete in **exactly one READ** (two READs already lose to one RPC), entries
are small and read-mostly, and — decisively — **EFA/SRD has no RDMA atomics
and no ordering guarantees**, which rules out lock-based or pointer-chasing
remote structures anyway. Precedent: Pilaf (ATC '13) — self-verifying
fixed-slot hash table, CPU as sole writer, checksum-validated lockless
remote reads.

## Decision

Each node exposes its directory shard as a **flat, fixed-slot, power-of-two
hash table in registered memory** at a base address distributed eagerly with
membership (ADR-0019; see "Descriptor distribution" below). Lookup = compute slot
from the pinned key hash
(ADR-0014's xxh3 family) → one RDMA READ of the slot's bucket → validate
locally. This is v2, the intended end state on EFA nodes *if the benchmark
justifies it*; the v1 RPC lookup remains as the non-EFA path and the shipping
default (ADR-0017 knob).

- **Entry layout (versioned wire ABI, like `PINNED_SCORE`)**: fixed-size,
  ~256 B — `key_fingerprint: u64` (xxh3 of chunk key, distinct seed),
  `seqlock: u32`, `tier_bits + generation`, `sharer_bitmap` (128 B ≈ 1024
  node slots, indexed by ring-membership order — the node-name-ascending order
  frozen in ADR-0014), `checksum: u64` over the entry. A bucket of 4 slots (1 KiB)
  is fetched per READ — collision tolerance without a second round trip.
- **Full-bucket failure mode**: if more than `dir_bucket_slots` distinct keys
  hash to one bucket, the home refuses the in-table registration for the overflow
  key and marks it RPC-only (v2 readers get a fingerprint/checksum miss → v1
  fallback). At the default load factor (~0.01%) this is astronomically rare; it
  degrades to RPC, never corrupts or silently drops a holder.
- **The bitmap carries set membership, not per-holder attributes — by design**
  (ADR-0017's reader-contract split). `tier_bits` and `generation` are
  **entry-level**, not per-node: `tier_bits` is a coarse "dram present in this
  set" hint, `generation` the admission epoch paired with the seqlock. A bitmap
  cannot express per-holder tier/generation and does not need to — picking a
  "wrong" holder is a benign `NotCached` retry because chunks are immutable
  (ADR-0015), so the tier is advisory and per-holder generation stays the home's
  private CPU-side bookkeeping (ADR-0017). v1 RPC keeps per-holder `(node, tier,
  gen)` for workloads that want finer source selection; v2 trades that granularity
  for NIC-served lookups.
- **"Widely held" sentinel** (ADR-0017's `max_sharers_tracked`): a high bitmap
  popcount *is* the widely-held signal — it needs no special encoding, and unlike
  the v1 list the bitmap costs the same 128 B at any popcount. `max_sharers_tracked`
  therefore still governs invalidation fan-out (ADR-0007/0016) on the write side,
  but does not cap what v2 can *list*.
- **Concurrency without atomics**: the home CPU is the **only writer** to
  its table. Writes take seqlock odd → mutate → even, recompute checksum.
  Remote readers validate `checksum && seqlock is even && fingerprint
  matches`; any failure → retry the READ (µs-cheap), bounded, then fall back
  to the v1 RPC lookup. No remote locks, no CAS — which EFA could not
  provide regardless.
- **Staleness is benign by construction** (inherits ADR-0017): a stale
  bitmap listing a departed holder costs one `NotCached` retry; a missing
  new holder costs a marginally worse source choice. No correctness
  dependence, so no freshness protocol.
- **Descriptor distribution — eager, with membership.** A node's directory
  descriptor `(fi_addr, table base address, rkey, slot count, entry layout, both
  hash seeds, ABI version)` is **static** for the process lifetime (registered once
  at startup, never relocated), so it is distributed cluster-wide alongside
  membership over the gRPC channel (ADR-0019), keyed by node-name-ascending
  identity (ADR-0014). Every node therefore holds every home's descriptor before it
  ever needs a lookup — a directory READ can target a **never-contacted home with
  no bootstrap round-trip**, which is exactly the storm cold path. This is an
  address, not a connection (SRD `fi_av`, connectionless), so it is eager without
  the QP cost that keeps data-plane setup lazy (ADR-0019). A version mismatch
  between peers disables v2 toward that peer (v1 RPC fallback) — rolling updates
  across ABI changes degrade, never corrupt.
- **Known cost — the home goes blind**: NIC-served lookups generate no CPU
  signal, removing the natural hot-chunk detector. The heat detector moves to
  **ADR-0018's holders as the primary source** (they see every fetch command —
  now firmly the case since both control and data cross the holder CPU,
  ADR-0003/0018), with sampled hit-reports piggybacked on admit announcements as
  a secondary cross-check (`sample_rate` default 1/64, knob below). The storm
  benchmark validates whether holder-side alone suffices or the sampled channel is
  needed — resolved here rather than left fully open (was ADR-0017's open question).

At the target workload (~115k chunks cluster-wide, ADR-0015) a shard is tens
of KB; the table is sized generously (2²⁰ slots ≈ 256 MB registered per node
at 256 B — see Knobs for the real default) so load factor is never the
binding constraint.

## Trade-offs

Pros:
- Hot-chunk lookup hotspot vanishes as a CPU problem: the NIC serves reads
  at message rate while the CPU only applies admits/evicts.
- Lookup latency ~4–6 µs flat, independent of home load — during the storm,
  exactly when RPC queues would be deepest.
- No cold-peer bootstrap hop: eager static-descriptor distribution (above) means
  the very first lookup to any home is a direct one-sided READ, never a
  handshake-then-READ — the storm's cold path pays one READ, not a round-trip plus
  a READ.
- No new consistency machinery: single-writer + seqlock + checksum, all
  forced choices given EFA's missing atomics.

Cons:
- A second frozen wire ABI (table layout + hashes) with the same migration
  cost as ADR-0014's: changing it needs dual-version support or a flush.
- Registered table memory is pinned per node whether or not the shard is
  busy; slot count × entry size is a permanent tax.
- Seqlock retries under heavy churn (storm admits) add tail latency for
  readers of exactly the entries being updated; bounded by retry cap +
  RPC fallback.
- Heat observability must be rebuilt (holder-side or sampled) — the RPC
  directory got it for free.

## Knobs

- `dir_table_slots` (default **2²⁰**) — **part of the ABI, carried in the
  descriptor distributed with membership** (above), not a freely-rolling knob: a
  mismatch disables v2 toward that peer (v1 fallback), like the entry layout.
  Tunable per *cluster* at a flush / rolling-ABI-bump cost, not per node. ×
  `dir_entry_size` (**256 B**,
  ABI) = 256 MB registered per node. At ~115 chunks/shard average the load factor
  is ~0.01 % — the generous default buys collision immunity for many-object
  workloads; drop to 2¹⁶ (16 MB) if pinned memory matters before then.
- `dir_bucket_slots` (default **4**, ABI) — slots fetched per READ; raises
  collision tolerance at 256 B/slot READ-size cost.
- `seqlock_retry_limit` (default **3**) before falling back to v1 RPC.
- `sharer_bitmap_bits` (default **1024**, ABI) — max cluster size the entry
  can express; resizing is an ABI version bump.
- Heat signal source: `holder_side` (default) vs `sampled_admit_reports`
  (`sample_rate` default 1/64) — benchmark under storm, then fix by
  amendment.
