# ADR-0030: Delivery registration belongs to the process that owns the memory

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-23 · Status: Accepted; the mechanism is settled, cache population is
decided (§ Decision point 6 — the direct path does not populate), and the one primitive
that was unproven — a same-node loopback RDMA WRITE on EFA — is **proven on hardware the
same day** (`bench/ladder/results/c2-loopback-gate.md`), which also **amends point 2
below**: the client does need an address handle, and a second arm settled how it gets the
endpoint for one (`c2-announce-gate.md` — the writer announces itself). Reading the
reference EFA implementation (`abcdabcd987/libfabric-efa-demo`) then **corrected point 1**
(the token is a per-rail set, not a tuple) and added points 3's immediate-data signal and
8's queue-admission consequence; building the parser then added point 7 (a token target is
not a mappable one, so the local path is a loopback WRITE and the digest moves source-side).
**Point 2 was refined again on 2026-08-31**: the handles are now built from a **pre-flight
endpoint exchange** — a request header on an ordinary `GetObject`, answered in the body — *before*
the delivery request, which makes the ordering structural instead of raced, demotes the announce
to the repair path, and — because a prefetch can name a fleet — bounds both the answer and the
client's handle set to the concurrency the delivery fan-out already implies

Supersedes the *mechanism* of [ADR-0027](0027-gpu-memory-delivery-targets.md); its goal —
a delivery target may be GPU memory — stands unchanged. Extends
[ADR-0026](0026-client-supplied-target-memory.md) and
[ADR-0018](0018-holder-driven-rdma-write-data-plane.md).

## Context

ADR-0027 chose a **CUDA IPC handle in the `x-pacer-target` header** because it is
serializable: no fd passing, no second listener, no fork in ADR-0026's transport. The
daemon would open the handle and register the client's window with
`cuMemGetHandleForAddressRange` → `ibv_reg_dmabuf_mr`. Hardware has now refuted the
premise, in a way no amount of care on the daemon side can fix.

**1. `nvidia_peermem` cannot load on EFA** (2026-08-22): the peer-memory client API is an
out-of-tree patch to `ib_core` that AWS's `efa` does not carry. So `ibv_reg_mr` on a
device virtual address is unavailable and **dma-buf is the only way to register device
memory with a NIC on this platform.** Everything below follows from that.

**2. A legacy-IPC pointer is not dma-buf exportable.** `cuMemGetHandleForAddressRange`
documents its provenances: a range from `cuMemAlloc`, or one from `cuMemAddressReserve`
fully mapped by `cuMemMap`. A `cuIpcOpenMemHandle` pointer is neither, and is refused with
`CUDA_ERROR_INVALID_VALUE` (a bad provenance) rather than `CUDA_ERROR_NOT_SUPPORTED` (a
capability that is off).

**3. The other provenance does work — and still does not help.** A process that imports a
*virtual-memory-management* handle holds exactly a `cuMemAddressReserve` range it mapped
itself, and it can dma-buf export it: measured on p5.48xlarge, a non-owning process
exported `fd=37` over the full 64 MiB after verifying the owner's bytes
(`bench/ladder/results/c2-vmm-dmabuf-provenance.md`). But:

- `cuMemAlloc`/`cudaMalloc` memory reports `allowed_handle_types=0`, so it can **only**
  travel as a legacy IPC handle. PyTorch's caching allocator is `cudaMalloc`, so a real
  tensor can never take this path.
- VMM and legacy IPC are mutually exclusive (`legacy_ipc_capable=0` on a `cuMemCreate`
  window), so adopting VMM removes the legacy option rather than adding to it.
- Fabric handles — the only VMM variant that fits a header the way a 64-byte IPC handle
  does — are `NOT_PERMITTED` without `/dev/nvidia-caps-imex-channels`, which is absent on
  p5 even though the device attribute advertises `fabric=1`.

**Therefore no serializable handle exists that lets the daemon register a client's
PyTorch tensor.** ADR-0027's serializability argument was correct about what it wanted and
wrong about what the platform offers.

**4. NVIDIA reached the same place independently.** cuObject's S3 flow puts registration
on the side that owns the memory: the client library selects a NIC, registers its own
buffer, and ships an RDMA token (`x-amz-rdma-token`); data nodes register only their own
local buffers and RDMA-WRITE into the client's. No cross-process CUDA handle anywhere.

## Decision

**Registration belongs to the process that owns the memory. The client registers its own
GPU memory and names it with a NIC-scoped token in the `x-pacer-target` header. The
daemon never holds a CUDA handle, and never needs CUDA at all.**

1. **The token replaces the handle.** `gid`, `qpn`, `rkey`, `addr`, `len` — an address and
   a capability, not a name for another process's memory. The client dma-buf exports its
   own tensor block in-process (`cuMemGetAddressRange` for the true block boundaries, then
   `cuMemGetHandleForAddressRange` with `flags=0`) and registers it against its own EFA
   device. The fd never leaves the process.

   **Corrected 2026-08-23: the token is a SET, one entry per client rail, not a single
   tuple.** An rkey is scoped to the protection domain that issued it, and a PD belongs to
   one device — so a window a client wants reachable from more than one of its own rails is
   registered once *per rail*, yielding a distinct `rkey` per rail alongside that rail's own
   `gid`/`qpn`. The writer picks the entry for the rail that is PCIe-local to the GPU
   holding the tensor (point 5) and addresses that rail; a token naming one rail confines
   delivery to it. `len` and the window's `addr` are common to the set; `gid`, `qpn` and
   `rkey` vary per entry. This is what the reference EFA implementation does — the CONNECT
   message in `abcdabcd987/libfabric-efa-demo` (`src/15_lazy.cpp`) carries `num_nets`
   addresses and `num_nets × buffers` keys for exactly this reason — and our own L0.6 arm
   already proved a WRITE from one rail into a window registered on another lands
   (`bench/ladder/results/c2-loopback-gate.md`). Header encoding is therefore a list, and a
   client with one rail is the degenerate case rather than the shape.

   **Implemented and amended 2026-08-24 — the writer does not "pick", it SPREADS.** Both
   halves now exist: `pacer_client`'s `WindowSpec::rail_count` registers one window on N
   consecutive rails (one `ibv_reg_mr`/`ibv_reg_dmabuf_mr` per rail's PD, one mapping, one
   dma-buf export, an announce pump per rail), and the daemon's `address_client` round-robins
   every chunk WRITE over every entry the token names. What forced it is a measurement: with
   ONE destination rail, C5 put 16 shards of a checkpoint in flight and stopped at ~10 GiB/s
   aggregate — that rail's ~11 GiB/s line rate — so client concurrency was exhausted as a
   lever and only more destinations remained
   (`bench/ladder/results/c5-safetensors-8b.md`).

   **Measured the same day, and the answer is that striping is not a bandwidth lever for a
   single-GPU window** (`bench/ladder/results/c5-multirail.md`): 1 rail and 4 rails deliver the
   same 131 GiB checkpoint at the same rate, and the peak delivered-chunk rate (11.46 vs
   11.93 GiB/s) is one rail's line rate either way. The four rails a GPU owns share one PCIe
   switch uplink — planning/22's byte-identical 2-rail ≡ 4-rail 13.128 GiB/s — and an HBM window
   must sit behind its GPU's switch, so the set cannot be widened without losing the affinity
   Track H measured at 6.95×. The mechanism is kept because it is correct, costs nothing at
   `rail_count: 1`, and is what a *multi-GPU* loader will need; it is not what makes one GPU
   faster.

   Two consequences worth stating, because they change what the *order* in this set means.
   The order no longer selects for a striping writer, it only orders — so **publishing a rail
   is consent to be written on it**, and for an HBM window a client must publish only rails
   PCIe-local to the GPU (`rail = 4 × gpu, rail_count = 4` on a p5, planning/22); a writer
   that picks exactly one still honours preference by taking the first. And the destination
   cursor is the daemon's own, deliberately not derived from which of ITS rails posts: a node
   with a single healthy rail would otherwise always choose destination 0 and stripe nothing,
   which is exactly the cheap single-interface shape (r8gd) where the client is most likely to
   be the bottleneck.

2. **The client's RDMA burden is one passive QP and one MR PER RAIL it publishes — and one
   address handle per daemon it talks to, per rail.** No work requests and no completion
   polling: **EFA SRD is connectionless**, so one passive QP serves any number of initiators.
   (Originally "one QP, one MR": that is the one-rail case, and striping — point 1's
   2026-08-24 amendment — multiplies both by the rail count, along with one announce-pump
   thread each. The *data path* stays untouched; what scales is setup.) This is the property
   mlx5 needs DC to emulate, and it makes this design *simpler* on EFA than on ConnectX (see
   [ADR-0031](0031-nixl-publish-as-a-backend-plugin.md)).

   **Amended 2026-08-23, measured:** this ADR originally said "no address handle, no
   connection handshake". That is wrong, and loopback is where it was tested — with the
   target holding no AH for the requester, a WRITE between two endpoints **on one device**
   completes `EFA_IO_COMP_STATUS_REMOTE_ERROR_UNKNOWN_PEER` (vendor error 14) and the bytes
   do not land (`bench/ladder/results/c2-loopback-gate.md`). SRD is *reliable*, so the
   target's NIC has to send transport ACKs and needs an AH for the initiator's GID even
   though it issues no work request — planning/09 finding 10, which same-device addressing
   does not exempt. So: the client must learn the **daemon's** `gid`/`qpn` (in the response
   to its registration, or in the same exchange that carries its own token), build an AH for
   it, retain it, and **rebuild it when the daemon's endpoint changes** — a DaemonSet
   restart brings up a new `qp_num`, and a stale AH addresses a dead QP (A1 finding 11,
   applied to the client side). Delivery cannot precede that AH, so there is a barrier, as
   on the peer plane. The client's *data path* is still untouched; what is gone is the
   "no handshake" simplification.

   **How the client learns a writer's endpoint — measured 2026-08-23, and it needs no
   advance knowledge** (`bench/ladder/results/c2-announce-gate.md`, L1.1-L1.3 PASS). This
   matters because a *remote* hit is written by the **holder** (point 4), and the client
   cannot know which holders those are — the daemon owns placement. It does not have to:
   **a writer announces itself with its own first packet.** A two-sided SEND *is* delivered
   to a target that has not AH-inserted the sender (the receive path resolves the sender
   from the packet; only the RDMA path needs the AV entry), so the sequence is holder SENDs
   its 23-byte endpoint → client decodes it and builds the AH → the one-sided WRITE lands.
   One SEND per (client, writer) pair, ever. Two mechanics the run pinned down: the payload
   arrives at **offset 0** (EFA puts no GRH in a receive buffer) and the completion carries
   the sender's **`src_qp` but not its GID** — reaching that needs `efadv_wc_read_sgid`, a
   `static inline` over a provider op — so the endpoint travels in the **payload**, as
   libfabric's own RDM protocol does. ⚠ And a SEND to a client with **no receive posted
   does not fail, it hangs** (no completion at all, a reliable-transport retry), so the
   client library must keep a receive ring posted and the daemon must put its own deadline
   on an announce. The alternatives — a prepare message from the client's local daemon
   (which already holds every holder's `EfaEndpoint`), or pre-inserting from the ring's
   membership — remain fallbacks, both strictly more protocol for the same effect: neither
   is self-healing, so both would need an invalidation channel that the announce *is*.

   ⚠ **"One SEND per (client, writer) pair, ever" is exactly wrong, and it cost an 8-GPU arm
   (2026-08-25).** The record of who has been announced to is keyed by `(gid, qpn)` — the
   endpoint, not a name the client chose, which is what keeps a *restarted* client from
   inheriting a dead process's handle. But **a queue-pair number is recycled**: four
   window-opens into a run, a fresh process is handed a QPN a finished one had, the record
   matches, no announce is sent, and every WRITE to it completes `UNKNOWN_PEER` until the
   retry ladder gives up — degrading a whole GET to a body, because one unplaceable window
   fails `run_delivery`. No key can distinguish a recycled pair from the one it was created
   for, so **membership must be invalidated by evidence, not trusted**: an `UNKNOWN_PEER`
   completion is proof the target holds no handle for us, and the writer forgets the record
   and re-announces once before retrying (`Announcer::forget`). That is the same rule
   `ClientReady` already applied to its proof of a successful WRITE — this ADR now states it
   for the announce too. The barrier point 2 wants (the client acknowledging that its handles
   are in) remains the right end state; this makes the failure recoverable rather than
   impossible, and it is what the arm at
   `c5-multigpu-remeasure.md` was
   measured on.

   **Built** (2026-08-23): the wire format lives in `pacer_transport::announce` — version
   byte, rail count, then `{gid, qpn, rail}` per rail, deliberately outside the `efa`
   feature gate so its tests run in every build, since ADR-0031 makes the decoder C++ and
   `GOLDEN_ONE_RAIL` is what that port is checked against. One message names **every** rail
   a writer may post from, so it stays one SEND per (client, writer) pair however many rails
   the writer round-robins across. The sender is `pacer_transport::efa::Announcer`:
   idempotent per client endpoint keyed on `(gid, qpn)` — the QPN is in the key because a
   restarted client reuses its GID with a fresh queue pair — and it records a client only
   *after* the completion, so a failed announce is retried by the next delivery. Its
   deadline is 250 ms because of the measured hang, and a caller must read an error as "fall
   back for this request", never as "retry in place". Owed: the caller (the token path) and
   hardware coverage of this implementation, as opposed to of the mechanism.

   **Refined 2026-08-31 — *when* the handle is built, and the announce is demoted to the repair
   path.** Everything above stands: the client must hold an address handle for each writer and
   rebuild it when that writer's endpoint changes. What this adds is the **ordering**, because
   the announce cannot supply it. `announce_to` awaits the announce SEND's completion, which
   proves the message reached the client's queue pair — **not** that the client decoded it and
   called `ibv_create_ah`. That happens later, on the client's own pump thread, so a WRITE can
   beat it, complete `UNKNOWN_PEER`, and (before the retry ladder above) fail a customer's GET
   with a 500. Every mitigation so far has been a *wait*: ten posts over ~2 s, a re-announce,
   three per-window re-attempts with a 50 ms backoff. Each is correct and none of them makes the
   race impossible.

   So a client now builds every handle it will need **before it issues the request that
   authorises anyone to write**, from addresses it asked for:

   ```text
   1. GET /bucket/key   x-pacer-get-endpoints: offset=…   ──►  ring lookup for the windows
      Range: bytes=0-0                                          this read will touch (NO I/O)
         ◄──── 200, x-pacer-endpoints: 1, {"nodes":[{"announce":<hex>}]} ──────────────────
   2. for each holder: in cache? else ibv_create_ah   ← purely LOCAL, no I/O, no handshake
      ═══ nothing can write yet: no delivery request has been issued ═══
   3. GET /bucket/key  x-pacer-target: nic:…  ─────►  delivery proceeds
   4.                                                 writes land, acknowledged ✔
   ```

   Because step 3 is what sets writes in motion and step 2 completed first, **no writer can be
   early.** The ordering is structural rather than raced, and it costs no acknowledgement
   message, no nonce and no new receive path on any daemon — the answer carries the *same*
   `pacer_transport::announce` bytes a writer would SEND, so there is one wire format, one
   decoder and one golden vector covering both paths.

   **The ask is a request header on an ordinary `GetObject`, not an endpoint of its own**, which
   is the shape this protocol already has twice (`x-pacer-target` in, `x-pacer-delivered` /
   `x-pacer-checksum` out). Three consequences, and each is the reason:

   * The client **signs nothing by hand.** The request is a real `GetObject`, so an SDK signs it
     natively and the header rides in on the same post-signing hook the target descriptor uses
     (SigV4 covers `host` and `x-amz-*` only). A dedicated endpoint needed a hand-rolled SigV4
     signature over a URL, i.e. a shim depending on private botocore internals to do its own
     signing — a maintenance liability whose failure mode is *silent* (priming just stops).
   * There is **no query string to canonicalise**, and that trap is real: `s3s` verifies the
     signature against the query it decoded and re-encoded with strict RFC3986 rules, while
     botocore canonicalises the **raw** query, so the two agree only when the raw form is already
     canonical.
   * The **endpoint list is the response BODY.** A GID is 32 hex characters, so a 32-rail p5 node
     is ~1 KB and the concurrency-bounded worst case (the bound below, 64 nodes) is ~67 KB. Today's
     real maximum, 8 nodes, would fit a header; the worst case would not, and many HTTP stacks cap
     one header line at 8 KB. A marked, otherwise-empty body is already what a delivery answers.

   **A reserved path was tried and rejected**, and the reason is worth recording because it is not
   obvious: `s3s` parses the S3 path *before* it consults a custom route and rejects an invalid
   bucket name outright, so a namespace chosen precisely so it could never shadow a bucket
   (`/_pacer/…` — `_` is illegal in a bucket name) is a `400 InvalidBucketName` that never reaches
   the handler, while any prefix that *is* a valid bucket name would shadow a real bucket of that
   name.

   **The answer is bounded, and the tail is announce's job.** An object on a 1000-node fleet has
   holders approaching the whole fleet; naming them would be ~32 000 `ibv_create_ah` calls at
   roughly a millisecond each — slower than the race it removes, and past the client's own bound,
   so it would *evict* the handles it just built. So the answer names **this node plus the holders
   of the first `delivery.parallelism` windows from the read's start, capped at
   `delivery.parallelism` nodes** — one number used twice because it is one fact: at most that many
   windows are in flight at an instant and each has at most one writer, so that IS the set of
   writers that can be posting while the client's pump is still catching up. Windows past it
   resolve later, when the pump has caught up, and a holder that was not named simply announces
   itself: one chunk pays the ladder once per endpoint (`ClientReady` serialises first contact) and
   every later chunk to that holder is free.

   **It moves no bytes.** The query needs no object *length* — the chunk indices come from the
   ask's own offset and the bound above, and a chunk key needs only an index — so it resolves no
   object header, reads no cache entry and issues no backend request. That is asserted rather than
   asserted-in-a-comment: `an_endpoint_query_moves_no_bytes` asks for a key that does not exist and
   still gets an answer, which a `HeadObject` would have turned into a 404. The cost of the
   indices possibly running past the object's end is bounded by the same cap and is one handle for
   a writer that never writes — cheaper than the `HeadObject` it would take to avoid.

   **The announce is not removed.** It demotes from "the mechanism" to "the repair path",
   covering the three cases a prediction cannot: a holder past the bounded set, a holder the
   pre-flight did not name (the source set can shift between the two calls — eviction, rebalance,
   a node joining), and a client that skipped the pre-flight entirely (an older shim, or a daemon
   with delivery off — which ignores the marker exactly as it ignores a target descriptor, so both
   simply serve the read; that is why the ask carries `Range: bytes=0-0` and the client
   discriminates on the *response* marker rather than the status, bounding the fallback to one
   byte). `announce_to`, `UNKNOWN_PEER_ATTEMPTS`, `Announcer::forget` and the per-window
   re-attempts are therefore unchanged. That fallback is what makes the pre-flight safe to ship —
   and it is also its one hazard, because a client that stops priming still *works*, just slower,
   so nothing breaks and nobody looks. Two counters exist for exactly that:
   `pacer_delivery_preflight_total` on the daemon (flat at zero while
   `pacer_delivery_requests_total` climbs = nobody is priming) and, on the client, announced rails
   whose handle was **already held** against those the announce had to build
   (`pacer_client::handles`).

   **One consequence the ADR has to carry: the client's handle set is now bounded.** Point 2 as
   written implied "keep every handle for the client's life", which was safe while the set could
   only contain writers that had actually announced. A client that *prefetches* holders can be
   handed a much larger set — on a 1000-node fleet at 32 rails, a naive prefetch is ~32 000
   device objects at roughly a millisecond of firmware admin command each, which would be slower
   than the race it removes. So the handle set is a bounded LRU cache keyed on the **GID alone**
   (`ibv_create_ah` takes a GID; the destination QPN travels per work request — which also means
   a daemon restart, same GIDs and a fresh QPN, keeps the handles a client already holds). The
   bound tracks **concurrency, not cluster size**: windows in flight are bounded by
   `delivery.parallelism`, so distinct writer nodes writing at any instant are too, and the
   default is that fan-out (64) times a p5's rail count (32). Sizing it that way is what makes
   eviction safe — an evicted handle is `ibv_destroy_ah`'d, and destroying one while a WRITE from
   that writer is in flight is the `UNKNOWN_PEER` case again, so the least-recently-used entry
   must never be a writer with a window in flight. Evictions are therefore **counted and warned**
   rather than silent: a non-zero count is the alarm that the bound was smaller than the
   concurrent working set, not a tuning hint.

   > ⚠ **OWED: the answer over-registers by up to 32x, and that is what makes eviction reachable.**
   > A holder's source rail is chosen by round-robin at write time
   > (`pick_healthy_rail`: `next_rail.fetch_add(1) % healthy.len()`), so the client cannot predict
   > *which* of a holder's devices will carry its chunk and must hold a handle for all of them. But
   > a holder contributing one chunk writes from exactly **one** device. So at the shape rendezvous
   > hashing actually produces — an object spanning N chunks over N nodes, one chunk each — the
   > client builds 32 handles per holder and uses 1: at the 64-node bound that is 2048 registered
   > against ~64 used.
   >
   > The cost is not the wasted handles, it is **starvation of the tail**: a fixed budget spent 32x
   > over on the early holders can be exhausted before the last holders get their single handle
   > each, and a holder with no handle is the `UNKNOWN_PEER` bounce this whole section exists to
   > prevent. Over-registration therefore makes the dangerous case *more* likely, not less.
   >
   > The fix is to stop letting the writer pick freely: allocate devices per holder — inversely to
   > holder count, so few holders each get many rails and many holders each get one — and have the
   > write path **honour** the allocation the pre-flight named. Total handles then track chunks in
   > flight (~64) rather than holders x 32, and the bound can shrink from 2048 to a couple of
   > hundred, which is what takes eviction off the correctness path rather than merely alarming on
   > it. Two things it needs that are not decided here: whether the allocation is a deterministic
   > function of the chunk key or a commitment carried in the pre-flight answer, and what a writer
   > does when its allocated device is unhealthy (bounce and let the announce ladder repair it is
   > the cheap answer, and it is the path that already exists).
   >
   > **Not fixed in this ADR because it is a write-path change** (`pick_healthy_rail`), which is
   > ADR-0018's holder-driven WRITE rather than this ADR's registration rule — so it needs deciding
   > across both. Recorded here because the bound above is where the consequence lands, and because
   > `pacer_delivery_preflight_nodes_total` divided by the served count is the number that says how
   > close a real fleet is running to it.

   The daemon side is `crates/pacer-daemon/src/preflight.rs`, answered from the **first thing**
   `get_object` does — before the op counter, before the cache-control decision, before anything
   that could read a byte. It is gated on `delivery.enabled` (with delivery off there is no window
   to write into, so nothing to prime), sources the holder set through the *same* `chunk_sources`
   the read path selects with, and takes the rail GIDs from the per-rail `AhCache` the ADR-0019
   handshake already populated — no RPC, no re-derivation. A pre-flight that disagreed with the
   read would be worse than no pre-flight at all.

   **Owed: the arm.** That priming actually removes the declines is *not* claimed here — the
   mechanism is built, and the measurement is `pacer_delivery_unknown_peer_{retries,declines}_total`
   reaching zero at the shape that produced them (8 GPUs, 32 endpoints, delivery depth ≥ 8),
   which needs a multi-GPU p5.

3. **The HTTP 200 is the completion.** SRD delivers out of order by design, so a partially
   arrived buffer is normal mid-flight and the only correctness barrier is the initiator's
   completion, which the response encodes. **A client must not read its buffer before the
   200**, and GPU-side visibility additionally obeys
   `CU_DEVICE_ATTRIBUTE_GPU_DIRECT_RDMA_FLUSH_WRITES_OPTIONS`.

   **A second signal exists and is worth having for striped reads: 4 bytes of immediate
   data on the last WRITE.** A WRITE-with-immediate raises a completion *on the target*
   carrying that word, so a client can learn a transfer finished from its own completion
   queue with no response message — which is how the reference implementation signals
   completion (`src/15_lazy.cpp` sets `imm_data` on the final WRITE of a request and the
   owner matches it against a pending-op id; EFA carries **exactly 4 bytes**, so the word is
   an op id, never data). The HTTP 200 stays authoritative for an ordinary GET, because a
   stock-shaped request must not require the client to poll a CQ. Where this earns its keep
   is a striped read served by several holders at once: per-holder completion arrives
   directly instead of being serialised behind one response. Two conditions before adopting
   it: at the verbs level a WRITE-with-immediate **consumes a receive work request on the
   target**, so the client must keep a receive ring posted — which point 2's announce
   already requires — and the 4-byte id has to be allocated by whoever will match it, i.e.
   the client, and travel out with the token.

4. **A remote hit becomes one hop.** Given the client's token, the *holder* writes straight
   into the client's HBM. Today's path is two hops — holder → requester's DRAM by RDMA,
   then requester → client — so this removes a full store-and-forward of every byte and
   deletes the H2D copy F2 measured at 32.3 % of the realistic loader path. ADR-0026's
   "one hop further" becomes two hops further.

5. **The client library owns four things**, none of which the tenant writes:
   - **Rail↔GPU affinity, mandatory not advisory.** Track H measured 32 rails on one GPU
     collapsing to 51.9 GiB/s — *below* host memory, a 6.95× penalty. Register on the EFA
     device that is PCIe-local to the GPU holding the tensor, never device 0.
   - **MR cache keyed on `CU_POINTER_ATTRIBUTE_BUFFER_ID`**, not the address: torch
     recycles blocks, and an MR that outlives its tensor is a NIC-writable window over
     reused memory. Deregister when the tensor dies.
   - **The don't-read-before-200 rule**, enforced in the library rather than documented.
   - **A non-EFA fallback** (ordinary HTTP GET + H2D) so a pod without EFA still works.

6. **The direct path does not populate the cache** (2026-08-23 — this closes what
   § Open gates called the most consequential open question). A holder writing straight
   into the client's memory bypasses the requester's DRAM copy, which on the ordinary path
   *is* the newly cached chunk. The requester keeps no copy, and does not read one back out
   of the client's window to make one.

   The reason is that the copy would buy no bandwidth. H2b measured the ceiling per **leg**
   — 57 GiB/s host→host, 107 host→HBM, 361 HBM→HBM (planning/19) — i.e. each host-memory
   leg costs about the same and a host leg is a host leg whether the DRAM is on this node or
   one SRD hop away. So a *later* read of a locally cached chunk into HBM is bound by the
   same DRAM↔root-complex traversal as pulling it from a peer's DRAM; a local hit is not
   the cheaper source it is on the body path, it is the same source at lower latency.
   Populating therefore spends an extra DRAM write, cache residency that a genuinely
   colder chunk could use, and an announce RPC, in exchange for latency the checkpoint
   workload is not bound by. **The premise is that the fabric leg is exercised
   properly** — rail↔GPU PCIe affinity (point 5) and NUMA-local rail placement
   (ADR-0025), both of which cost 6.95× and 1.65× respectively when they are not — so this
   decision rests on the same requirement point 5 already makes mandatory.

   **The second reason is independent of any measurement, and it is the one that keeps a
   door open: a delivery client need not be a ring member.** Nothing in this design
   requires the process holding the target memory to be a cache participant — it names
   memory and presents a token, and the holder WRITEs into it. Today's client happens to
   reach its node-local daemon, and that daemon happens to be in the ring, so "populate the
   requester" has an obvious meaning; that is a property of the current deployment, not of
   the protocol. A client on a node with no daemon, one reaching a daemon that is not its
   node-local one, or a consumer outside the cluster entirely would leave "which node's
   cache populates?" with no well-defined answer. Building population into the direct path
   would quietly make ring membership a precondition for accelerated delivery and bake that
   assumption into the data path. Note also that such a client could never *become* a
   sharer even if we wanted it to: ADR-0017's sharer set is keyed on a ring `NodeId`, and
   the client's window lives for one request, not for a cache residency. **We have not
   explored the non-member client and this ADR does not close it** — see
   [ADR-0026](0026-client-supplied-target-memory.md) § Consequences.

   Precisely scoped, because the unqualified sentence would break the fill discipline of
   ADR-0012/0016:

   - **Layer-1 requester-local admission (ADR-0016) is skipped**, and with it the
     remote-announce that follows an admit. This is not new code: `deliver_window` in
     `crates/pacer-daemon/src/proxy.rs` already skips it, on the narrower ground that the
     bytes never pass through the process and admitting on some delivery paths but not
     others would make `pacer_local_admits_total` mean two things. That reason stands; this
     one is why it is a design decision rather than an implementation convenience.
   - **Owner/R-home read-through fill is unchanged.** A cluster *miss* still fills at the
     homes (`deliver_window` reaches `fetch_from_backend(.., self.admit)` when
     `owns_chunk` holds), because the alternative is every node's first read paying an
     Express miss at 6.4–8.4 GiB/s — an order of magnitude below either DRAM leg, and the
     999-on-1 hotspot moved onto S3. The bandwidth argument above says a local DRAM copy
     is no better than a remote one; it says nothing in favour of no copy anywhere.
   - **The cost is sharer-set growth, not read bandwidth.** Layer-1 admits are how a hot
     chunk acquires holders beyond its R homes (ADR-0017), and B4 rung 3 measured
     ownership skew as the limiter at N=8 — the busiest owner saturates while smaller
     owners have less to serve. A cluster whose reads are *all* deliveries never widens a
     hot chunk's holder set, so fan-in stays on the R homes. Watch it at the fan-in rungs;
     the lever if it binds is `replication_r`, or an async populate reintroduced
     deliberately with this ADR amended, not admission smuggled back onto the read path.

7. **Rejected, with the measurement that rejected each:**
   - *Daemon imports a legacy IPC handle* (ADR-0027) — impossible: no exportable
     provenance.
   - *Daemon imports a VMM handle* — possible, measured, but requires `cuMemCreate`, which
     excludes every PyTorch tensor; and its header-shaped variant needs IMEX channels that
     are absent.
   - *Client exports the dma-buf fd, daemon receives it* by `SCM_RIGHTS` (a hostPath
     socket) or `pidfd_getfd` (`hostPID` on the tenant's pods + `CAP_SYS_PTRACE` on the
     daemon) — workable, and strictly worse: both need a pod-spec change on pods we do not
     control, both need the same loopback WRITE, and both put the MR lifetime of a tenant's
     recycled allocation in the daemon's hands. Note that the receiving *mechanism* is not
     itself a risk: an fd installed by inheritance, `SCM_RIGHTS` or `pidfd_getfd` is the
     same object, and `ibv_reg_dmabuf_mr` cannot tell them apart.
   - *Daemon-owned HBM slab + client-side `cuMemcpyDtoD`* — **kept as the named fallback**,
     because it is the only option that needs no loopback WRITE (the client does the copy
     with CUDA). Its own blocker is that a DaemonSet never sees `/dev/nvidia-uvm`: those
     nodes are created lazily on first CUDA use and a container's `/dev` is fixed at
     creation. Costs one D2D copy at HBM bandwidth, which *replaces* rather than adds to
     today's H2D.
   - *Daemon `mmap()`s the client's dma-buf and `memcpy`s* — a local-path-only option the
     export documentation names, gated on `CU_DEVICE_ATTRIBUTE_DMA_BUF_MMAP_SUPPORTED`;
     write-combined stores on x86, so expect PCIe-bound and CPU-expensive. Recorded for
     completeness, not planned.

7. **A token target is not a mappable target, and that reshapes the local path and the
   integrity check.** Found while building the parser (2026-08-23). `DeliveryTarget` — the
   daemon's existing abstraction over client memory — is written around `copy_in` and
   `digest_window`, both of which *touch* the client's memory: ADR-0026's `shm:` target is
   mapped by the daemon and ADR-0027's `cuda-ipc:` one was to be imported by it. A `nic:`
   target is neither. The daemon holds an rkey and nothing else, by design, so:
   - a **local hit is a loopback WRITE**, not a memcpy — which is exactly what
     `bench/ladder/results/c2-loopback-gate.md` measured, and why that gate had to pass
     before this scheme was worth parsing;
   - the **integrity check moves to the side that holds the bytes**. The daemon cannot read
     the delivered window back to digest it, so `x-pacer-checksum` must be computed over
     the source (the cache frame) rather than the destination. ADR-0028 already argued for
     holder-side checksums on cost grounds; under this scheme it is not an optimisation but
     the only place the bytes are readable.

   So the token does not become a third arm of `DeliveryTarget`; it needs its own path.
   **Built on the transport side** (2026-08-23): `pacer_transport::token::TokenWindow` folds
   `base_addr + offset` once and refuses a sub-range that would reach past what the client
   registered, and `EfaRdmaTransport::write_into_token` addresses the client (a per-rail
   `ClientAhCache`, keyed on `(gid, qpn)` so a restarted client cannot inherit a dead queue
   pair's handle), announces to it, then posts the same WRITE the peer path posts — from an
   ADR-0028 frame in place, or one staging copy. It returns `Ok(false)` for every benign
   reason not to use RDMA, so the caller falls back exactly as it does when a holder
   declines.

   **Wired 2026-08-23.** `ClientMemory` is now an enum over the two registration models and
   every placement goes through one funnel (`Proxy::place_window`), so the difference between
   "the daemon owns this memory" and "the client does" lives in exactly one place: a `memcpy`
   on one arm, a WRITE on the other. Consequences worth stating, because two of them correct
   things said earlier in this ADR's own history:
   - **A remote chunk is not a dead end.** Its bytes arrive in this node's memory and are
     then written on like any other — two hops until the holder-writes-directly path lands,
     never a fallback to the body.
   - **The digest is per-arm.** A token window is digested over the bytes *sent*
     (`DeliveryDigest::of`); a mapped window is still read back, which proves what is in the
     client's memory rather than what we sent, and is the stronger check where it is possible.

     **Amended 2026-09-12: per-arm, and only when REQUESTED.** This clause said which digest
     each arm uses and never said whether to compute one, and the token arm read that as
     "always". It cost a CRC32 over every delivered byte on every arm that asked for none —
     131.4 GiB on a 70B pre-layout load — on the blocking pool the chunk store's `pread`s
     share, discarded immediately by the caller. The flag is the token's own (`checksum=none`,
     which point 6's remote half has always honoured for the reason stated there: verification
     is O(bytes) on whoever computes it, so the request has to reach them). Both arms now gate
     on it, and `stage_seconds{stage="digest"}` is what makes the gate observable — **absent**
     on an arm that asked for nothing.
   - **The one all-or-nothing case** is "no usable RDMA path to this window" (no healthy
     rail, the client is not announceable, geometry mismatch). Then there is no `memcpy` to
     fall back to, so the request degrades to a body — and windows already written are
     harmless, because a 200 without `x-pacer-delivered` means the client reads the body.

     **Amended 2026-08-31: the degrade is still per-request, but a decline is no longer
     automatically final.** The five reasons a token WRITE declines are now a typed
     `TokenDecline` split by `is_transient()`, and the two transient ones — the client is not
     announceable, and the client never installed this writer — are re-attempted for **that
     one window** (3 attempts, 50 ms doubling) before the request degrades. Both are "the
     client's pump has not caught up yet" and both were measured resolving in milliseconds,
     so treating them as terminal was what made one slow endpoint cost a whole checkpoint its
     acceleration. The three terminal ones (no healthy rail, not addressable, body exceeds
     staging) now **short-circuit the pipeline** instead of letting `buffered` deliver and
     then discard every remaining window. New series
     `pacer_delivery_declines_total{reason}`.

     **Why the degrade itself stays per-request, and must.** A per-window fallback to the
     body is not expressible under point 3: `x-pacer-delivered`'s *presence* is the completion
     signal and the CRC32 folds only the windows that landed, so a short byte count is
     indistinguishable to a client from a complete delivery of a smaller range. Both shipped
     shims would accept it — read the count, verify the checksum over exactly those bytes
     (which passes, since the daemon digested the same subset), and return a buffer with a
     hole in it. Changing that needs a wire-level way to say "these ranges landed, read the
     rest from the body", which is a protocol change and a new decision, not an amendment.

   **Amended 2026-08-31: the remote half is BUILT, and it needed no new mechanism —
   only a field.** `FetchBlobRequest.client_token` carries the client's window (one
   `{gid, qpn, rkey}` per client rail, plus this chunk's absolute destination address,
   resolved by the requester so a holder does no window arithmetic), and the holder runs
   the *same* `write_into_token` the reading node runs for a local hit — its own
   `Announcer`, its own `ClientAhCache`, its own first-contact gate, its own rails. That
   is planning/19's **C3**: N holders writing one client buffer at once, which is the only
   shape in which a single read can exceed one node's fabric. Three things the build
   settled:

   * **A separate proto message, not a wider `RdmaBuffer`.** `RdmaBuffer.rail` names the
     *requester's* rail and binds the holder to its own same-index rail, because only that
     PD's rkey is valid. A client token is a **set** — the client registered its window
     once per rail — and any holder rail can reach any client rail (L0.6), so the holder
     picks its source freely and spreads over the client's. One message meaning both would
     be a holder posting from the wrong PD.
   * **The digest moves with the bytes.** The requester never sees them and cannot read
     the window back, so the holder reports a CRC32 of what it wrote
     (`BlobMeta.written_crc32`) and the requester folds it in offset order like any other
     window's — point 7's source-side digest, one node further out. A *requested* checksum
     that does not come back is treated as a protocol violation and the chunk falls back,
     because folding a hole would produce a digest that verifies over fewer bytes than
     were delivered, which is exactly the silent corruption point 3 refuses to make
     expressible. `crc32fast::Hasher::new_with_initial_len` is what makes the fold exact:
     combination needs each side's LENGTH, so a reported digest rebuilt without it folds
     at the wrong offset and every partly-remote delivery's checksum fails on good data.
   * **A decline is not a decline of the request.** A holder that cannot reach the client
     streams the body and the requester writes it from here — i.e. the two-hop path above
     *is* this path's per-chunk fallback, so the worst case of the remote half is the
     behaviour that preceded it. That is why it is gated (`delivery.remoteWrite`,
     `PACER_DELIVERY_REMOTE_WRITE`, **default off**) as a *control arm* rather than as a
     safety valve: both arms deliver the same bytes with the same guarantee, and the hop's
     worth is unmeasurable unless one image can run both.

   **Ran on hardware 2026-08-31 — the path is PROVEN and the rate is not
   (`c3-remote-half.md`).** Two p5 in one AZ,
   Llama-3.1-8B into a host `nic:` window, `replicationR=1` so half of every read is remote:
   **946/946 remote chunks were written into the client's window by their HOLDER's NIC**, with
   `peer_stream 0` and **zero declines** — so the announce cost this shifts was absorbed
   entirely at this scale (one client rail set, two holders), and
   `pacer_delivery_declines_total{reason="writer_not_installed"}` never moved. The integrity
   arm passed on the treatment, which validates `BlobMeta.written_crc32` and the length-aware
   fold end to end rather than only in a unit test. The reader's own WRITE count is the
   mechanism in a counter: 1920 with the remote half off (its 974 plus the 946 it relayed),
   974 with it on.

   **The rate question is still open, and the arm mis-specified it rather than answering it.**
   At one client rail both arms sit at one rail's line rate (8–11 GiB/s) and at four they both
   sit at ~14.8, so what bound the delivery was where the bytes LAND, not who sourced them —
   and a 32-rail reader relaying half a checkpoint is never short of rails, so the funnel the
   arm was built to expose did not exist on that shape. Run-to-run spread reached **2.2×** at
   a fixed setting, which swamps any effect; **no ratio from that arm should be quoted.** What
   would bind the source side is N ≥ 4 nodes (so the reader relays 3× what it reads locally), a
   window per GPU, and reader CPU read as a primary signal — the staging copy this removes is
   CPU per relayed byte, which is the shape of win ADR-0028's slab arm found on a rung whose
   throughput did not move.

   **Amended 2026-08-24: hardware coverage is done, and the counterparty is no longer a
   spike.** The path ran end to end on one p5 — a 32 MiB object written into a
   client-registered host window and into a client-registered H100 HBM window, both sides
   agreeing on `crc32=310d8327`, with `delivery.gpuTargets` *off*
   (`bench/ladder/results/c2-token-gate.md`). And the client half named in point 5 now exists
   as [`crates/pacer-client`](../../crates/pacer-client): it registers the window, renders the
   token, and runs the thread that keeps a receive posted and inserts an address handle per
   announce — with a C ABI, since the callers are Python (`clients/python/pacer_nic.py`) and
   C++ (ADR-0031 § Decision 3, which asked for exactly this). `spike/efa`'s `token-client`
   role is now a thin driver over that library, so the gate tests the code that ships.

   **Amended again 2026-08-24, and this is the load-bearing measurement: the daemon registered
   NOTHING at checkpoint scale.** C5 put a real published checkpoint through this path —
   Llama-3.1-8B, 14.958 GiB, into a host window and into an H100 HBM window on one p5
   (`bench/ladder/results/c5-safetensors-8b.md`) — and `pacer_delivery_registrations_total`
   stayed at **0 across 44 delivery requests**, with `pacer_delivery_pinned_bytes` never
   leaving 0. The reason this ADR exists is therefore a counter now rather than an argument:
   ADR-0026's per-request `ibv_reg_mr` cost C4 24.1 ms per 512 MiB window and dominated its
   result, and on this path it does not appear at all — the client registers once for the
   window's life (6.7–7.1 s for 16 GiB of base pages, outside every arm). Delivery beat an
   otherwise identical body reader **3.201× into host memory and 2.526× into HBM**, and the
   tensors handed out were views: `copies == 0` over 291 tensors, device views checked for
   pointer identity — the property only a window that outlives its request can have.

   ~~What the library does **not** yet do: resolve rail↔GPU affinity itself.~~ **Amended
   2026-09-06 — it does now.** `pacer_client::topology::affine_rails(gpu, want)` resolves it
   from the host: `cuDeviceGetPCIBusId` for the GPU's address, `/sys` for each rail's PCI
   ancestry, and the pair scored by shared bridges
   (`pacer_transport::rdma_device::shared_bridges`). Reachable from C
   (`pacer_client_affine_rails`) and Python (`pacer_nic.affine_rails`, and
   `NicTarget(rail=None)` — now the **default**).

   Three reasons it had to stop being the caller's job, all of them paid for:

   * **planning/22's map is a p5 map.** A p5 puts 4 rails and 1 GPU behind a switch; a
     p6-b200 puts 2 rails and 2 GPUs behind each of 4. A table written for one is wrong on
     the other, and wrong in the direction that still works.
   * **It went stale silently.** When `rdma_device::is_efa` began filtering non-EFA devices,
     every hand-written index shifted by two on p6-b200 — two `mlx5_core` interfaces had been
     occupying 0 and 1.
   * **The pod cannot apply a table keyed on a GPU ordinal.** Inside a one-GPU container the
     ordinal is always 0 whatever GPU was allocated; `cuDeviceGetPCIBusId` reports the *host*
     address, which is what makes discovery possible at all.

   It reports **`distinct = false`** when no rail shares more than the root complex with the
   GPU, rather than crowning the arbitrary first one — on such a host pinning buys nothing,
   and a caller told "these are the affine rails" would spend the arm explaining a rate with
   no topological cause. What it still does **not** decide is *how many* rails to ask for:
   that is a bandwidth-vs-registration trade, and on p6-b200 more is not better (8 rails
   measured 15 % slower than 2). ⚠ Discovery is unit-tested and compiles under
   `--features efa`, but was **not yet confirmed against hardware** — validating it against a
   hand-derived pair on a live p6 is outstanding.

   Also absent, and deliberately:
   no `BUFFER_ID`-keyed MR cache, because a window is registered once for its whole life, which
   is what the token model made possible.

8. **Delivery WRITEs share the holder's send queues, so the admission invariant has to be
   re-established across both posters.** Today only the holder posts, and a full send queue
   is unreachable *by construction* rather than handled: `MAX_SEND_WR` is
   `HOLDER_ARENA_RANGES` (`efa/context.rs`) and `HOLDER_SERVE_SLOTS <= HOLDER_ARENA_RANGES`
   is a compile-time assert (`pacer-daemon/src/peer.rs`), so the daemon cannot post more
   WRITEs than the queue holds. A delivery WRITE is posted on the *same* QPs under a
   *different* gate (the pinned-window quota), which that assert does not cover — so
   whoever builds delivery must either extend the invariant to the sum of both admissions or
   handle a full queue. It matters more than a lost WRITE would suggest: `post_write`'s
   error path returns `Err`, and a failed fetch calls `evict_stale_ah`, which drops the peer
   on **every** rail and produces the gRPC storm D5.1 measured. The reference EFA
   implementation takes the other route — `abcdabcd987/libfabric-efa-demo`
   (`src/15_lazy.cpp`) queues every op in software and treats `-FI_EAGAIN` as flow control,
   pushing the op back to the front of its own queue and retrying on the next completion
   poll — which is the pattern to copy if bounding admission across both posters turns out
   to be awkward.

9. **The daemon must not REQUEST an EFA device, only reach one.** Found trying to run the
   first end-to-end arm (2026-08-23): with `efa.enabled=true` the DaemonSet requests
   `vpc.amazonaws.com/efa`, and on a one-interface node (r8gd/m8gd.24xlarge — the cheap cache
   pool) it takes the only one, so the *client* pod could not be scheduled at all and the arm
   never reached the delivery. That is not a test artefact: under this ADR the client
   registers on **its own** NIC, and a tenant running multi-node NCCL needs those interfaces
   too. The cache is a guest on the node's fabric exactly as point 5 makes it a guest on the
   node's GPUs — *visibility is not allocation*, and the rule "never request `nvidia.com/gpu`
   on the daemon" extends verbatim to `vpc.amazonaws.com/efa`.

   The mechanism is available: an EFA device serves many protection domains and queue pairs
   concurrently (the L0 probe held four contexts on one device), so what a device-plugin
   request actually buys is `/dev/infiniband/uverbsN` being mounted plus scheduler
   accounting. The daemon needs the first and must not have the second, which a **hostPath
   mount of `/dev/infiniband`** plus the `IPC_LOCK` the chart already offers provides. One
   ordering constraint to design around rather than discover: Karpenter attaches an EFA ENI
   because a *pending pod requests* one (planning/14), so something must still ask at
   node-launch time — the tenant's own pods in production, the launcher in a test arm (which
   already drops its request once the ENI is attached, `3f5ff21f`).

   **Built 2026-08-23:** `efa.shareHostDevices` suppresses the resource request and hostPath-
   mounts `/dev/infiniband` instead (`type: Directory`, so a node with no ENI fails the mount
   rather than starting a daemon that silently finds no device and falls back to gRPC).
   `efa.enabled` keeps its other meaning — this daemon does RDMA, so the container limit
   covers the pinned pools — and the two compose. The dev GPU profile
   (`values-dev-gpu.yaml`, used by `bench/ladder/c2-token.sh`) sets it **even though a p5 has
   32 interfaces and the request would starve nobody there**: a dev profile that models
   production wrongly is how the rule gets forgotten.

   **AMENDED 2026-08-24 — the hostPath is necessary and NOT sufficient.** The rule above
   stands; the mechanism under it was wrong, and the "verified on a p5" note it shipped with
   verified the wrong thing. A mount makes a device *visible*; it does not make it
   *openable*. Enumeration walks `/sys/class/infiniband*`, which every pod on the node sees
   in full, while `ibv_open_device` goes through `/dev/infiniband/uverbsN`, which the
   kubelet's **device cgroup** admits only for units a device plugin ALLOCATED to the pod. So
   what a request buys is not "the uverbs mount plus scheduler accounting" — it is the mount,
   the accounting, *and the cgroup rule*, and the daemon needs the third as much as the
   first. Measured on a p5 with 32 free units: `rail placement resolved rails=32` followed by
   `EFA capability probe failed … opening the RDMA device context`, and by hand
   `head -c0 /dev/infiniband/uverbs0` → `Operation not permitted`. The daemon ran gRPC-only
   and served every delivery as a body — indistinguishable from `write_into_token` declining,
   which is what made this cost a paid node to find.

   Three ways to give the daemon a fabric, then, and only three:
   1. **request a unit** — ruled out above, and still ruled out;
   2. **run the container privileged** (`efa.privileged`, off by default) — the device cgroup
      admits everything, no unit is consumed, and it was measured opening all 32 rails as
      uid 65532. **DECIDED 2026-08-24: this is the supported production mechanism**, not a
      stopgap — see the posture note below;
   3. **advertise the same devices through a plugin of our own, or a DRA driver** — a small
      DaemonSet advertising e.g. `pacer.io/efa-shared: N`, or a DRA driver publishing a
      `ResourceSlice` whose `NodePrepareResources` returns a CDI device listing every uverbs
      node. Either way the daemon requests one of those and never a `vpc.amazonaws.com/efa`,
      so it gets the cgroup rule while the tenant's units stay untouched and one device keeps
      serving many holders (which the hardware has always allowed — L0 held four contexts on
      one device). **Optional polish, not owed work** — it earns its keep only where policy
      forbids a privileged pod.

   **Posture note (the reason privileged is acceptable here).** This cache targets LLM
   training clusters, which are single-tenant: the team that installs the daemon owns the
   nodes and the jobs on them. The EFA device plugin on the very same node already runs
   `privileged: true, runAsUser: 0`, as do the CSI drivers and the GPU operator, so a
   privileged infrastructure DaemonSet is unremarkable company. What privilege we take is
   also narrower than the word: measured on a p5, `CapEff` is all zeros, because the daemon
   runs as uid 65532 with no file capabilities — what it gains is device-cgroup allow-all,
   not a bag of capabilities. And point 9's actual concern was never privilege but
   **starvation**, which `efa.privileged` removes completely: it consumes no units, so the
   delivery client and the tenant's NCCL job keep all of them.

   The one cost that survives is **installability, not security**: a namespace enforcing PSA
   `restricted` (or an equivalent Kyverno/OPA rule) will refuse the pod, and for those
   installs option 3 is the answer. That is why the knob stays off by default — a chart
   should never silently escalate — and why a DRA driver remains worth building if someone
   needs it. Prerequisites for it are confirmed present on EKS 1.36 (kubelet
   `DynamicResourceAllocation: true`, containerd `enableCDI: true`); the CDI-by-annotation
   shortcut is NOT (`spike/cdi/README.md`).

   One transport consequence, fixed the same day: with a partial allocation the openable
   devices are the plugin's choice — a one-unit request was granted `uverbs2` — so opening
   "device 0" is not a safe assumption. `EfaContext::bring_up_rails` and `open_device` now
   skip devices they cannot open and fail only when none opens.

## Open gates

- ~~**A same-node loopback RDMA WRITE on EFA.**~~ **SETTLED 2026-08-23 — GO**
  (`efa-spike loopback`, one p5.48xlarge, `bench/ladder/results/c2-loopback-gate.md`).
  L0.1-L0.6 all PASS: the firmware accepts an AH for the node's own GID, a host → host
  loopback WRITE lands (the control arm), the owner's dma-buf window registers on the
  owner's own PD, the WRITE into it completes, **the delivered bytes are correct** (position
  stamps at a non-zero offset between sentinel guards — the byte-integrity proof this bullet
  asked for), and a WRITE from a **different rail** of the same node lands in the same
  window. One rail moves 11.039 GiB/s at depth 8. Two facts worth carrying: the same-rail
  case never touches the port's packet counters (the device services it internally) while
  the cross-rail case is 2048 real packets out one rail and into the other, and the run
  amended point 2 above.
- ~~**How a client learns which holders may write to it.**~~ **SETTLED 2026-08-23 — a
  writer announces itself** (`efa-spike announce`, L1.1-L1.3 PASS,
  `bench/ladder/results/c2-announce-gate.md`). A SEND reaches a target that has not
  AH-inserted the sender, so no advance exchange and no membership view are needed; see
  point 2 for the sequence and for the receive-ring requirement the run exposed. Still
  same-node evidence for a cross-node claim — the first two-node delivery arm should
  assert it rather than assume it.
- **Whether the local path is worth more than one rail.** The gate answered *can it
  happen*, at one rail with nothing fanned out. The alternatives for a local hit (the
  daemon-owned slab's `cuMemcpyDtoD`, or `mmap`-ing the dma-buf) are only comparable against
  a multi-rail loopback number, and the 11.039 GiB/s above is unexplained: no packet left
  the port, so a port line rate is not what bounds it.
- ~~**Cache population on the direct path.**~~ **DECIDED 2026-08-23 — the direct path does
  not populate; see § Decision point 6.** The holder-writes-twice and
  requester-populates-asynchronously candidates are rejected on the same ground: H2b's
  per-leg ceiling makes a local DRAM copy no cheaper to read into HBM than a peer's, so
  neither variant buys bandwidth for the cost it adds. Owner/R-home read-through fill is
  explicitly *not* affected. What is left to watch is sharer-set growth, not throughput.
- **Token scope and lifetime.** An rkey is a bearer capability over a tenant's GPU memory,
  and a striped read hands it to several holders at once. cuObject scopes its token per
  request; ours must decide scope, lifetime and leak behaviour deliberately. **Partly settled
  2026-09-08 (amendment, quality item R4): the daemon-side *state* lifetime is decided; the
  *capability* scope is still open.** The three maps a token's endpoints populated — the
  per-rail client address-handle cache, the per-rail first-contact gate, and the node-wide
  announce record — grew for the process lifetime, which is one leaked `ibv_ah` per departed
  client. All three are now one bounded structure (`pacer_transport::client_registry`): capacity
  `PACER_EFA_MAX_CLIENTS` (default 2048 = 8 ranks × 4 windows × 32 rails × 2 generations) with
  least-recently-used eviction, plus an idle TTL `PACER_EFA_CLIENT_TTL` (default 300 s) that
  drops a client which vanished without unregistering. Eviction is safe because a handle is held
  by reference count and its `Arc` travels with the work request's source guard, so
  `ibv_destroy_ah` cannot run while a WRITE or announce is outstanding. The TTL is also the
  evidence-free half of the recycled-QPN defence (§ point 2's 2026-08-25 measurement). What
  remains open is unchanged and is the capability question: an rkey's scope per request, and
  what revokes one a client never withdrew.
