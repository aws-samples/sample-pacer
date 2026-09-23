# ADR-0038: The chunk store is the default disk tier, wherever a slab exists

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-09-14 · Status: **Accepted. Defaults only — no code path changes, and no new
measurement was taken for it.** Every figure below is cited to an arm already on record.

[ADR-0033](0033-chunk-store-owns-the-disk-tier.md) built the chunk store and left it
`config.diskTier=store`, off by default, "until 33.5 is settled honestly". This ADR keeps 33.5
open and flips the default anyway, because **33.5 turns out to be the wrong question to gate a
default on**, and adds the startup refusal the flip requires.

## The decision

1. `config.diskTier` empty now resolves to **`store` wherever an ADR-0028 cache slab is
   derived** — the same `efa.hugepages` gate `pacer.cacheSlabBytes` already uses — and to
   **`foyer`** where none is. The chart helper is `pacer.diskTier`.
2. The daemon **refuses to start** when the effective tier is `store` and the node installed no
   frame source (`assert_store_has_a_slab`).
3. An explicit `config.diskTier` always wins, in both directions. ADR-0033's promise that a
   regression is one flag back is preserved verbatim.

## Why flip, when gate 33.5 still FAILS

33.5 is an **absolute** bar — per-node disk-tier read rate ≥ 25 GiB/s — and it is unmet by
*both* tiers. It was the right gate for "is the store good enough to build on". It is not the
question a default answers.

A default answers **which of the two available tiers should a reader who sets nothing get**, and
on that question there is one within-node comparison on record
(`mountpoint-vs-pacer.md`, one p5, c=100):

| tier | read rate | share of its bytes served from the drives |
|---|---:|---:|
| `store` | **23.750 GiB/s** | 100.02 % |
| `foyer` (the shipped default) | 14.5 GiB/s | **0.005 %** |

**1.633× on one node, and the mechanism is corroborated three ways** — foyer essentially never
touches the drives at all, so its "disk tier" was serving from the page cache, which is the thing
the tier exists to bypass. A reader deploying the chart got the slower tier *and* an unbudgeted
page-cache dependency.

Two caveats that bound the claim and neither of which changes its direction:

* **n=1, one instance, one concurrency, one object size.** The 1.633× is a within-instance ratio.
  Its mechanism is corroborated three ways, which is stronger evidence here than a repeat rate
  would be, but no arm has repetitions.
* **23.750 is a FLOOR for `store`, not a ceiling.** That arm ran with unbounded store read depth,
  before `storeReadConcurrency` existed; the same array gives ~48.7 GiB/s at depth 16 and warp at
  c=100 offers far more than 16 (`nvme-device-truth.md`).
  So the gap between the two tiers is understated, not overstated.

## Why the default must be CONDITIONAL, and why a bare `store` would have been a bug

The store's read is `O_DIRECT` into a registered slab frame. `O_DIRECT` requires a page-aligned
buffer; a heap `Vec`'s alignment is its element's. So on a node with **no slab**, every store read
abandons the direct descriptor and goes through the **buffered** one:

| store read | service time per 16 MiB | page cache |
|---|---:|---|
| into a slab frame (`O_DIRECT`) | **17.3 ms** | bypassed |
| onto the heap (buffered) | **26.9–34.9 ms** | polluted |

That is not a degraded store — it is roughly half the rate of the thing that was asked for, with
the page-cache behaviour foyer was rejected for, and it is invisible in every figure except
`pacer_cache_slab_heap_fallbacks_total`.

The chart's own defaults are `efa.enabled: false` and `efa.hugepages: ""`, which derive **no
slab**. So a bare `diskTier: store` would have shipped exactly that silent half-rate path as the
default — and, once the refusal in decision 2 exists, would instead have made the chart's default
install **fail to boot**. This is `scatter.enabled`'s bare `true` again, which broke every Express
deployment; gating the default on the same condition that derives the slab is what makes the flip
safe. `pacer.diskTier` and `pacer.cacheSlabBytes` therefore read the same values, so the two can
never disagree about whether a node can do a direct read.

## Why a refusal and not a warning

Because buffered `store` is **measured against nothing**, while its alternative is measured on the
same node in the same arm. `foyer` at 14.5 GiB/s is a known quantity; buffered `store` is not, and
"probably still better than foyer" is an argument, not a number. An operator who has asked for a
tier this node cannot deliver should be told, not quietly given a third thing.

The precedent is this repo's most expensive recurring failure: a knob that silently did not apply,
producing an arm that measured its own control and published it as the treatment. A warning in a
DaemonSet's logs is not read. A refused pod start is.

**It cannot fire on a healthy node.** `frames::has_frame_source()` is false only where no slab was
installed at all — a process-lifetime configuration fact, and the same condition the chart couples
the `store` default to. Transient frame exhaustion is a *different* state, keeps its existing heap
fallback, and is deliberately not checked: a read must still be served.

## Consequences

* **`diskCapacity` must cover the whole working set on a `store` node**, and that now applies by
  default wherever hugepages are set. This is ADR-0033 § Consequences, unchanged, but it stops
  being something only an opt-in operator meets: foyer's effective capacity was
  `memCapacity + diskCapacity` because a chunk lived in RAM and demoted; the store has only
  `diskCapacity`. Under-sizing it evicts mid-warm and sends those chunks to S3, which reads as the
  store being slow when it is the tier being too small.
* **The ConfigMap now always carries `disk-tier`.** It used to render only when the value was set,
  so the shipped chart emitted no key and the tier was whatever `config.rs` defaulted to — a
  reader of the ConfigMap could not tell which tier a node ran.
* **`promotion`, `ioEngine`, `uring.*` and `StorageTuning` now apply to the header cache only on a
  default deployment**, since that is all foyer holds there. They are not deleted and are not
  wrong; they simply stop being read-path controls where they used to be. ⚠ This makes the
  outstanding **psync-vs-uring** question a header-cache question by default, which is *not* what
  the +50 % `flushers` result was measured on — do not carry that figure across.
* **A node that switches tiers starts cold for chunks**: the two keep bodies in different files, so
  the other's bytes remain as garbage until evicted or the directory is wiped. Correct either way
  (an invalidation clears both). ADR-0033 already said this; it is now reachable by upgrading the
  chart rather than only by setting a flag.
* **Gate 33.5 stays open and stays FAILING** at ~15.9 GiB/s corrected. This ADR does not claim it
  is met, and nothing here should be read as retiring it. What it claims is narrower: of the two
  tiers this daemon ships, the store is the one a reader should get.

## What this does not decide

Whether the store's remaining ~2.7× gap to fio is closable — ADR-0033's open question, and the
reason 33.4/33.5 remain the two gates that matter. Nor does it touch the RAM tier: on the HTTP
path RAM still beats store by ~1.5× and **why is open**
(`http-path-ceiling.md`).

Supersedes ADR-0033's "**Off by default**" consequence only. Everything else in ADR-0033 — the
mechanism, the seven gates and their verdicts, the two measured nulls — stands unchanged.
