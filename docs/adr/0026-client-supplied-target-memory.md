# ADR-0026: A cooperating S3 client may name memory it owns, and the daemon delivers into it instead of into the response body

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-20 · Status: Accepted (protocol decided; host-memory implementation
first, GPU targets in [ADR-0027](0027-gpu-memory-delivery-targets.md))

Extends the data plane of [ADR-0018](0018-holder-driven-rdma-write-data-plane.md) by
one hop — from "peer daemon → requester daemon registered memory" to "peer daemon →
**client** memory" — and answers the constraint planning/19 D5.3 measured. Does not
change the peer wire protocol, the ring, or the cache.

## Context

D5.3 isolated the leg every previous track-D number silently included. On
2 × p5.48xlarge, RAM-resident, same nodes and keyset:

| arm | GiB/s |
|---|---:|
| client-delivery path alone (`run.sh local`, 0 peer serves, 0 read-throughs) | **31.589** |
| full paired arm (RDMA peer plane + client delivery) | 27.502 |

The paired arm is **87 % of the delivery-only arm**, i.e. the RDMA peer plane costs
~13 % and the *delivery* leg is what the daemon spends its throughput on. The daemon's
own half of that leg is already zero-copy — `requester_copy_seconds_total` reads 0, the
landed arena range reaches hyper as a refcounted slice (`collect_blob` explicitly
refuses to concatenate) — so what remains is below the daemon: hyper's `sendmsg` copies
user→kernel, TCP/IP segments, a veth pair carries it into the client's namespace via
softirq, and the client's `recvmsg` copies kernel→user. At 27.5 GiB/s that is ~57 GB/s
of socket-copy traffic and ~3.2 M packets/s at a 9001 MTU, **none of which appears in
the daemon's process CPU** — which is why the holder measured 0.76 of 192 cores while
being the apparent bottleneck.

Two facts frame the decision:

- **The mechanism to avoid it already exists in this codebase.** ADR-0018 has the
  requester lease a registered range, offer `(addr, rkey, len)` on the fetch RPC, and
  the holder WRITE into it, with the RPC response as the completion. Nothing about that
  shape requires the range to belong to a *daemon*.
- **Tuning the TCP leg is not the same as removing it.** As written this said the leg
  had "real headroom in request shape," on the strength of planning/07's 42.6 GiB/s
  through this endpoint at 50 MiB objects on a third of a p5's cores. **Both halves of
  that are now dead** (C0, 2026-08-20): request shape bought −0.9 %, the leg is
  bandwidth-bound at 34–37 GiB/s, and 42.6 was never reproduced. The conclusion is
  unaffected and strengthened — there was no knob, only the copy-through-kernel path,
  which is what this ADR removes. **And it stays removed rather than tuned: ~30 GiB/s is
  good enough for an HTTP client** (2026-08-23), so the stock leg's ~37 GiB/s is the bar
  delivery must beat and not a number anyone should go raise.

## Decision

**A client may name memory it owns; the daemon delivers the object into that memory and
returns a header-only 200. Clients that do not ask are unaffected.**

1. **Opt-in by request header, on the existing S3 GET.** A cooperating client sends a
   target descriptor naming memory it owns. Host memory is a POSIX shared-memory
   segment, which is *nameable* — so the descriptor is a string in a header and needs
   no `SCM_RIGHTS`, no unix socket, and no change to the TCP listener:

   ```
   GET /bucket/key
   x-pacer-target: shm:/pacer-loader-7;offset=0x40000;len=16777216
   ```

2. **The response carries a completion, not a body.** `200`, `Content-Length: 0`,
   `x-pacer-delivered: <bytes>` plus the integrity header of point 5. The client reads
   its own buffer.

3. **The client never speaks RDMA.** It cannot: an rkey is scoped to the protection
   domain that issued it, and making every loader an EFA participant is a non-starter.
   The DAEMON `shm_open`s + `mmap`s + `ibv_reg_mr`s the client's segment on the relevant
   rail's PD and offers *that* rkey to the holder. The client's only obligations are to
   allocate, name, and not free early.
4. **Delivery mechanism follows the tier, and only the remote tier is RDMA.** The client
   asked for bytes in its buffer, not for a transport:

   | chunk location | how it lands in client memory |
   |---|---|
   | remote peer's cache | holder-driven **RDMA WRITE**, peer NIC → client memory |
   | local daemon RAM | `memcpy` from the cache's `Bytes` |
   | local NVMe tier | disk read into the buffer (GDS later — planning/20) |

   All three remove the TCP payload and the kernel networking. Only the first is RDMA
   end-to-end, so `rdma fraction` below 1.0 on this path is a *local hit*, not a fault —
   a distinction any dashboard reading it must make.
5. **Integrity moves into the headers.** When the body never crosses HTTP, the SDK's own
   CRC32/MD5 check disappears with it. The response therefore carries a checksum of the
   delivered bytes, and the shim verifies it — the discipline planning/15's C2 established
   for the peer plane, applied to the client leg. A delivery that cannot be checksummed
   is a delivery that cannot be trusted.
6. **Chunk fan-in is a feature, not an obstacle.** An object is N chunks (ADR-0015) on
   possibly N holders. The daemon directs each holder to WRITE at
   `target + chunk_offset`, so multiple holders across multiple rails fill one client
   buffer concurrently. This is the only shape in which a single client read can
   approach the aggregate the fabric offers (H2 measured 360.709 GiB/s to HBM).
7. **Lifetime is the completion, and it is the client's contract.** The buffer must stay
   mapped and untouched until the 200 arrives; freeing early corrupts the client's own
   memory. The daemon deregisters and unmaps when the request completes, and MUST NOT
   hold a registration across requests without an explicit release step.
8. **Pinned client memory is quota'd.** Registration pins pages, so a client can
   otherwise pin the node. The daemon enforces a per-client and node-wide cap and
   refuses over-quota targets with a 4xx **and a body-delivered fallback**, never a
   failed read.

## Consequences

- **The requester's arena leaves the data path** for opted-in reads:
  `holder cache → holder arena → [RDMA] → client memory`, two hops shorter than today.
  The requester daemon becomes a control-plane broker for those reads — it still owns
  the directory lookup, the offer, and admission.
- **Compatibility is structural, not aspirational.** The empty-body response is gated
  strictly on the request header, so any client that does not send one gets today's
  behaviour. That gate needs a test asserting a *stock* client never sees an empty
  body; without it, the compatibility claim is an intention.
- **Acceleration is opt-in and requires client code.** No stock SDK allocates a segment,
  publishes a name, and reads from it, and none can be made to — we cannot hand memory
  to a client that does not know it exists. The benefit reaches exactly the clients we
  ship a shim for. In the path that matters (F1/F2 checkpoint loading) the client is
  ours, which is what makes this worth building.
- **Per-request HTTP RTT survives**, because the control plane is deliberately still
  HTTP. Delivery gets cheaper; request *count* does not. Small reads stay RTT-bound, so
  this does not remove the incentive to read in chunk-sized units.
- **It is a proprietary extension.** S3 has no "deliver into my memory" semantics. That
  is acceptable for a node-local accelerator cache and unacceptable to hide: the
  extension lives in `x-pacer-*` headers, is absent unless requested, and is documented
  as ours.
- **A delivery client need not be a ring member, and we should not close that door.**
  Nothing in this protocol requires the process that owns the target memory to be a cache
  participant: it names memory, presents a token, and a holder WRITEs into it. Today's
  client reaches its node-local daemon and that daemon is in the ring, so the two roles
  coincide — but that is the current deployment, not the design. Three shapes this leaves
  open, none of them explored: a client on a node with no daemon, a client reaching a
  daemon that is not its node-local one, and a consumer outside the cluster. Each is
  plausible for a GPU pod that wants bytes without hosting a cache tier, and none of them
  needs a protocol change. What they *do* need is that no part of the delivery path assume
  the receiver is in the ring — which is one of the two reasons
  [ADR-0030](0030-delivery-registration-belongs-to-the-memory-owner.md) point 6 gives for
  the direct path not populating the cache: for a non-member client, "which node's cache
  populates?" has no well-defined answer, and such a client could not become a sharer in
  any case (ADR-0017 keys the sharer set on a ring `NodeId`, and the target's lifetime is
  one request). Flagged here rather than decided: the membership question is open, and the
  cost of keeping it open is currently zero.
- **New trust surface.** A holder receives an rkey into *client* memory. Inside the
  trusted cluster ADR-0018 already assumes, that is the same exposure as an rkey into a
  daemon arena; across tenants it would not be, so the target must be a segment the
  requesting client owns and the daemon must not accept a name it cannot attribute.

## Implementation status (C1 built AND first-run measured, 2026-08-20)

The host-memory half is **built, merged, and exercised on hardware** — it delivers
correctly and loses on throughput at 16 MiB objects, for reasons measured below
(§ Measured 2026-08-20). Where it lives, and the four places the code departs
from — or narrows — the decision above:

- **Protocol + mapping + quota**:
  [`pacer-daemon/src/delivery.rs`](../../crates/pacer-daemon/src/delivery.rs) —
  descriptor parsing (single-path-component segment names only, so a descriptor
  cannot name anything outside the configured directory), the `MAP_SHARED`
  mapping, the two-ceiling quota with drop-released reservations, and the
  chunk→window arithmetic. **Delivery is off by default**
  (`PACER_DELIVERY_ENABLED`): mapping and pinning memory a client named is a
  privilege, not a tuning knob.
- **Read path**: `proxy.rs`'s `deliver`/`deliver_window`, sharing the source order
  and the bounded look-ahead of the body path so delivery changes *where* bytes go,
  never *which copy* answers.
- **Registration**:
  [`pacer-transport/src/efa/target.rs`](../../crates/pacer-transport/src/efa/target.rs)
  — `register_client_target` + `fetch_chunk_into`. The peer wire protocol,
  `peer.rs`, and ADR-0018's holder are untouched: a holder cannot tell whose
  memory it is writing into.
- **Client shim**: `clients/python/`, with the
  quota fallback handled transparently, so a caller has one code path whether or
  not the daemon delivered.

Narrower than the decision, deliberately, and each is a planning/19 item rather
than an oversight:

1. **One rail per request, not per chunk.** An rkey is PD-scoped, so registering a
   window on all 32 rails would cost 32 `ibv_reg_mr` calls per request to buy
   intra-request rail spread. Point 6's *cross-holder* fan-in works (each holder
   WRITEs at `target + chunk_offset`); spreading one read across rails is **C3**.
2. **Whole chunks only take the one-sided path.** A holder WRITEs an entire cached
   body (ADR-0018), so a byte range that starts or ends mid-chunk has its edge
   chunks fetched normally and copied. Chunk-aligned reads — every whole-object
   read — are one-sided throughout.
3. **Registration is per request, and its cost is published** rather than assumed
   (`pacer_delivery_register_seconds_total`), because point 7 forbids holding a
   registration across requests. One registration per GET amortizes over every
   chunk of the object; if C1's measurement finds it material, the fix is an
   explicit release step, which is a protocol change this ADR would have to amend.
4. **No layer-1 admission on a delivered chunk** (ADR-0016 layer 1). On the RDMA
   path the bytes never enter the daemon's address space, so admitting would mean
   reading them back out of client memory — and doing it on some paths but not
   others would make `pacer_local_admits_total` mean two things. Hot chunks are
   still admitted by ordinary body reads of the same key.

### Amendment 2026-08-21: the checksum is opt-out, and delivery has its own fan-out

Two changes the checkpoint shape forced, recorded here because the first is a
**change to point 5**, not an implementation note.

1. **`checksum=none` on the descriptor.** Point 5 makes a checksum mandatory
   ("a delivery that cannot be checksummed is a delivery that cannot be trusted"),
   and that stands as the default. But verification is **O(delivered bytes) on both
   sides**, and on the RDMA path the daemon must *read back* bytes it never
   touched: a 100 GB window would pay a full serial pass in the daemon before the
   200 is sent, plus another in the client. So the descriptor may carry
   `;checksum=none`, the response then omits `x-pacer-checksum` (absence is the
   signal — never an empty value), and a client that verifies its own bytes or is
   about to deserialize them anyway can decline. Default unchanged: ask for nothing,
   get CRC32.
2. **Delivery's fan-out is its own knob** (`PACER_DELIVERY_PARALLELISM`, default
   64), not the body path's `fill_parallelism` (8). The body path is bounded because
   it must emit *in order*; delivery windows are disjoint destinations with no
   ordering constraint. One GET for a 4 GiB checkpoint is 256 windows, so the
   inherited 8 turned a fabric-limited transfer into 32 serial rounds.

The checksum, when requested, is now computed **per window by the task that
delivered it** and folded in offset order (`crc32fast`'s combine tracks each side's
length, so the fold is exact — a test asserts it equals the single-pass digest).
That keeps integrity off the critical path instead of appended to it.

### Measured 2026-08-20: point 7's per-request registration is the binding cost

The first hardware run (planning/19 § Track C C1) confirms the protocol —
184 776 requests / 3.10 TB delivered into client memory, daemon and client byte
counts identical, every delivery CRC32-verified, zero fallbacks, quota released — and
refutes the assumption that per-request registration would be immaterial:

| registered window | `ibv_reg_mr` + map, per request |
|---|---:|
| 64 MiB | **27.4 ms** |
| 16 MiB | **4.39 ms** |

At 16 MiB objects delivery therefore reads **0.89–0.92×** of an ordinary body read.
⚠ **16 MiB is the ladder's shape, not the workload's** — the rungs force
`object == chunk` to prove byte-exact ownership, which on a single node protects
nothing. The workload this ADR exists for is ONE GET for a multi-GiB object, where
the same registration amortizes over 64–256× the bytes; the 2026-08-21 amendment
above and `bench/ladder/c1-delivery.sh`'s `1GiB`/`4GiB` arms exist to measure that
shape instead.
Registration is ~4.4 ms of the +21.4 ms it adds to p50; the client's own checksum
(~8 ms over 16 MiB) and the per-request map/unmap are the rest. Two caveats keep this
from being a verdict on the design: the Python load generator caps at ~6 GiB/s
against a stock path measured at 34–37 (C0), so the socket copy this ADR removes was
never the binding cost in that comparison; and 16 MiB is the worst case for a
per-request cost — a 1 GiB window amortizes the same pinning over 64× the bytes.

**Re-measured at checkpoint size, 2026-08-21** (planning/19 § Track C;
results): with ONE GET per
multi-GiB object the protocol **wins ~2×** (12.80 vs 5.778 GiB/s at 1 GiB objects,
9.033 vs 4.573 at 4 GiB, 12.67 with the checksum off), while a same-session 16 MiB
control still loses at 0.972×. So the loss above was the *harness's* object size.

What did NOT change is the cost's nature, and it is worse than "a per-request tax": the
per-request window cost measured **3.61 GB/s**, so it is **proportional to the window**
and therefore **does not amortize with object size at all** — ~1.19 s for a 4 GiB window,
~47 % of every client thread's time in the 4 GiB arm, and ~28 s for a single 100 GB
GET. Only the *fixed* costs (RTT, and the `shm_open`/`mmap` of a segment already mapped)
amortize.

> **Correction 2026-08-21 — 3.61 GB/s is not `ibv_reg_mr`, and the constant is 20–30×
> smaller than it implies.** This section originally attributed the figure to pinning and
> to page count. ADR-0028's `reg-timing` spike (`spike/efa`'s `reg-timing` role,
> `k8s/run-regtiming.sh`; six arms on one p5, pages pre-faulted) measured registration
> alone:
>
> | pages | pinning rate, 1 rail / 32 rails |
> |---|---:|
> | 4 KiB anonymous | 12.3 / 14.4 GB/s |
> | 2 MiB | **306 / 239 GB/s** |
> | 1 GiB | **433 / 430 GB/s** |
>
> So raw 4 KiB pinning is already 3.4–4× faster than 3.61, and hugepages are worth
> **~20–30×** beyond that. What the delivery path was actually paying is `shm_open` +
> `mmap` + **page-table population for a freshly mapped tmpfs segment** — visible inside
> the 4 KiB arms too (0.700 s for the first registration, 0.250 s once warm). Three
> things follow, and they change the ranking rather than the conclusion:
>
> - **The shape of the cost stands.** It is still proportional to the window, so it still
>   does not amortize with object size, and the ~47 % is a truthful account of *this arm*
>   (tmpfs, per-request, freshly mapped). Only the constant was wrong.
> - **The handle amendment below must cache the MAPPING, not just the registration** —
>   most of the cost is on the mapping side, so a handle that re-`mmap`s per GET would
>   keep most of the tax it exists to remove.
> - **Hugepage-backed client segments are the largest single lever and need no protocol
>   change**, which promotes them above the amendment in ordering. That 4 GiB window
>   becomes ~14–18 ms on 2 MiB pages, and the 100 GB GET drops from ~28 s to under half a
>   second. The cost is deployment rather than code: the *client's* pod must request
>   `hugepages-2Mi` (an unrequested hugepage `mmap` fails `ENOMEM`), which is the same
>   plumbing [ADR-0028](0028-cache-ram-tier-is-the-registered-arena.md) made load-bearing
>   on the daemon side.

**Hugepage-backed client segments are BUILT (2026-08-23), and they confirmed that this
ADR's point 2 already made them a values change.** `HugeTarget`/`make_target` in
`clients/python/pacer_delivery.py` create the segment as a hugetlbfs file instead of a
POSIX shm object; the descriptor, the header and the daemon are byte-identical, because
the daemon resolves `shm:/<name>` by `open()`-ing it under `PACER_DELIVERY_SHM_DIR` and
deliberately never links `shm_open` (`delivery.rs`'s `DEFAULT_SHM_DIR`). So the *page
size is a deployment choice*, and the daemon needed no change at all. Three things the
implementation had to settle, none of them protocol:

- **Where the hugepages are charged.** To the cgroup that *faults* them — which is the
  client, since `prefault()` runs before the GET. So `hugepages-2Mi` belongs on the
  client pod, in both `requests` and `limits` (the resource is not overcommittable), and
  the daemon needs none. That matters beyond tidiness: an unsatisfiable hugepages request
  makes Karpenter refuse to launch the node, so adding one speculatively to the DaemonSet
  would break every node that has no reservation.
- **The mount path now follows `shmDir`** in the chart, so the directory the daemon
  resolves names under and the directory it mounts cannot drift apart — that pair is the
  entire configuration difference between the two arms — which is now literally true rather
  than by inspection: the two arms differ in ONE values LAYER,
  `bench/ladder/values/delivery-pages-shm.yaml` versus
  `bench/ladder/values/delivery-pages-hugetlbfs.yaml` (the old `values-c1.yaml` /
  `values-c1-hugepages.yaml` pair, whose names still resolve through
  `bench/ladder/values/index.env`).
- **The realized page size is verified, not assumed.** A directory that is not hugetlbfs
  silently yields base pages, so `make_target` reads the mount's page size and refuses;
  the bench also reports requested-vs-realized. Without that, the arm produces a
  complete, plausible number and attributes the base-page wall to hugepages — and the
  check is sound for both processes, since page size is a property of the file's backing
  filesystem and the daemon maps the same inode.

**Unmeasured.** The A/B is `run.sh deliver` twice in one session with
`LADDER_DELIVERY_PAGES=base` then `2Mi`. Read it on the summary's `ms/GiB pinned` line
rather than on throughput alone: every delivery arm so far has been bandwidth-bound at
the client, which is exactly when a pinning win appears as CPU headroom instead of GiB/s.

**This ADR therefore owes the amendment point 7 itself anticipates**: a segment
registered once and reused across requests, with an **explicit release step**, rather
than re-pinned per GET. Until that lands, the honest scope of the host-memory half is
"correct, and worth it only where the window is large or the client registers once".

**And one fix that needs no protocol change at all, found while reading the 2026-08-21
arms:** `deliver()` registers the window *before* it knows whether any chunk is
remote. Point 4's table says only the remote tier is RDMA — a local hit is a `memcpy`
and needs no rkey — so on an all-local read the registration is **pure waste**. Every
arm measured so far was single-node, i.e. **100 % of that pinning bought nothing**.
Registering lazily, on the first chunk that actually needs an rkey, is strictly
complementary to the handle amendment and reclaims the same ~47 % on any read the
node can serve locally (which, for a warm checkpoint, is all of them).
The amendment is a protocol change — a release verb and its failure semantics — so it
belongs in a revision of this ADR, not in an implementation note.

**Lazy registration LANDED 2026-08-21** (`5c9cf5d2`): `ClientMemory` in
`crates/pacer-daemon/src/proxy.rs` registers on the first chunk that needs an rkey, held
in a `OnceCell` so concurrently resolving windows cannot race into two registrations of
one segment, and remembers a *failed* registration rather than retrying it per window. A
locally served read now pins nothing. It is **unmeasured on hardware**: read
`pacer_delivery_registrations_total` per delivered request — a warm, locally served arm
should report **zero**. Note this makes the correction above less urgent than it looks for
the warm case, and no less urgent for the remote one: laziness removes the cost where
there is no peer, hugepages and the handle remove it where there is.

Point 8's "per-client" cap is realized as **per-request + node-wide**: the only
client identity this protocol carries is the segment name, which the client owns,
so a genuine per-tenant cap needs authenticated identities and is not something a
header can be trusted for.

The compatibility claim is enforced by a test, not an intention:
`tests/daemon/delivery.rs::stock_get_is_untouched` drives a GET with delivery **enabled**
and no target header, and asserts the full body plus the absence of both response
headers.

## Status / relationship to other ADRs

- **ADR-0018** — unchanged. This reuses its holder-driven WRITE and done-as-response
  contract with a different owner for the destination range.
- **ADR-0024 / ADR-0025** — the arena and its placement stay exactly as they are: the
  holder still stages into a NUMA-local registered range before it WRITEs. Only the
  *destination* changes, so both ADRs' measurements remain valid.
- **ADR-0027** — the GPU half: the same protocol with a CUDA IPC handle instead of a shm
  name, which is what gives track H's H1 a consumer and Phase 4's F2 its path.
- **ADR-0015** — chunking is what makes point 6 possible; `chunk_size` remains the unit
  a client should read in.
- **planning/19 § Track C** — the sequence, and the host-memory measurement that gates
  ADR-0027's implementation.
