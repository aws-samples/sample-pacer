# ADR-0034: NVIDIA cuObject is not adoptable on EFA — the constraint is distribution, not fabric capability

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-22 · Status: Accepted (no-change decision + standing watch item)

Records a third-party convergence on [ADR-0026](0026-client-supplied-target-memory.md) /
[ADR-0027](0027-gpu-memory-delivery-targets.md)'s architecture, and why it changes nothing
we build. Written because the *reason* is easy to get wrong in a way that would either
waste a spike or close the question permanently when it should stay open.

## Context

[cuObject](https://docs.nvidia.com/gpudirect-storage/cuobject/index.html) (v1.2.0 at
time of writing) is NVIDIA's GPUDirect Storage path for **S3-compatible object stores**.
Its shape is the one this repo arrived at independently:

| | cuObject | PACER Track C |
|---|---|---|
| control plane | standard S3 GET/PUT over HTTP | same |
| target named in | `x-amz-rdma-token` (request) | `x-pacer-target` ([`delivery.rs`](../../crates/pacer-daemon/src/delivery.rs)) |
| server ack | `x-amz-rdma-reply` (response) | `x-pacer-delivered` |
| data plane | server-initiated `RDMA_WRITE` (GET) / `RDMA_READ` (PUT) into client GPU **or** host memory | holder-driven one-sided WRITE (ADR-0018) into client shm or HBM |
| server side | `registerBuffer` / `allocHostBuffer` / `handleGetObject` / `handlePutObject` | `register_client_target` + the serve path |

Two libraries: a **client** library (in CUDA Toolkit 13.1.1+, which intercepts S3 requests
and manages the RDMA sink/source) and a **server** library (separate download, integrated
into "the storage partner object server"). Supported operations are PUT, GET, UPLOAD_PART,
RANGE_GET.

The line that prompted this ADR: **"The current implementation of cuObject requires
Dynamic Connection (DC) transport"**, over InfiniBand or RoCEv2.

### The wrong conclusion, and why it is wrong

The tempting reading is "EFA can't do what DC does, so this is fabric-incompatible." That
is false, and believing it would mis-file the question forever.

Ask what DC is *for*: a Dynamically Connected initiator QP reaches many targets without
pre-establishing a connection per peer. For a storage server facing thousands of GPU
clients that is the difference between one QP and N QPs — a scalability property, not an
InfiniBand feature they happen to prefer.

**SRD has that property.** It is a reliable *datagram* transport: one QP addresses many
peers through address handles, which is exactly what this repo's symmetric-AH negotiation
exploits ([ADR-0019](0019-rdma-control-plane-grpc-edges.md) /
[ADR-0021](0021-efadv-ibverbs-not-libfabric.md)). And EFA exposes the operations cuObject
needs — one-sided WRITE (its GET) and READ (its PUT), both Nitro v4+ and both
hardware-proven here (planning/09,
planning/04 §2). The one EFA gap that bites
elsewhere, **no atomics** (spike finding 7), is not something cuObject appears to need.

So EFA is a semantically valid substrate for cuObject's *design*. The blocker is
elsewhere.

### The actual blocker

- **Both libraries are closed prebuilt binaries** under the CUDA Toolkit EULA. No source,
  no headers, no repository, no license permitting a port.
- **There is no transport abstraction, provider plugin, or backend interface** — the
  documentation was queried directly on this point and describes none.
- **DC is not a generic rdma-core QP type.** It exists only as a Mellanox direct-verbs
  extension (`mlx5dv`, DCT via `IBV_QPT_DRIVER`), so the implementation is bound to the
  mlx5 provider. EFA's equivalent is `efadv_create_qp_ex` for SRD QPs
  (planning/04 §4, ADR-0021) — a different
  provider and a different QP type with no shared seam to swap at. *Inference:* the docs
  do not disclose the underlying library, but DC is reachable no other way.

"Adapt it to EFA" therefore means recompiling a binary we do not have, against a provider
it was never written for, through an abstraction that does not exist. That is not a hard
project; it is not a project.

**Contrast NIXL**, which makes the distinction concrete: NIXL *has* a plugin
architecture, which is precisely why AWS could contribute an in-tree libfabric/EFA plugin
and reach GA in 2026-03 (see the standing note in planning/04 §7;
the NIXL decision itself is [ADR-0031](0031-nixl-publish-as-a-backend-plugin.md), not this
one). cuObject documents no such slot. That is
the line between "AWS can add EFA support" and "only NVIDIA can."

### Why control-plane-only compatibility is also unavailable

The appealing middle path — honour `x-amz-rdma-token` so a stock CUDA-13.1.1+ loader gets
accelerated with no PACER shim, keeping SRD underneath — does not survive the closed
client. The token that client emits **describes a DC target** (rkey plus DCT number and IB
addressing). A PACER holder needs an SRD address handle plus rkey. Consuming their header
would require NVIDIA's client to be able to *describe* a non-DC target, which it cannot.

The same argument applies in the other direction: even if AWS one day shipped a
cuObject-speaking S3, PACER could not act as its **client** either, because the daemon
would be the side required to open a DC QP.

## Decision

1. **Do not adopt cuObject, in any of its three possible roles.** PACER does not
   implement its server library, does not target its header protocol, and does not consume
   it as a client. `x-pacer-target` (ADR-0026/0027) remains the sole delivery protocol,
   and **Track C's C2 proceeds unchanged**.
2. **Record the reason as distribution + provider binding, explicitly NOT as a fabric
   capability gap**, so the question is re-openable on the correct trigger and is not
   re-litigated on the incorrect one. In particular: do not spend a spike testing whether
   SRD can carry cuObject's semantics. It can. That was never the obstacle.
3. **Keep it as a standing watch item** with the two named triggers below, rather than a
   closed question — *"the current implementation requires DC"* is the phrasing of a
   constraint its authors expect to relax.
4. **Correct planning/04's novelty claims** (§5 "we'd be first", §7 "nobody in the
   S3-cache category uses RDMA on AWS"). cuObject does not falsify either as stated — it
   ships no EFA support — but it makes the *architecture* explicitly non-novel, and a
   claim that depends on nobody else having had the idea is not one to keep leaning on.

### The only two triggers that re-open this

- **cuObject gains a transport abstraction, or an AWS-contributed EFA/libfabric provider
  appears** — the only path by which anyone but NVIDIA can act *on cuObject itself*. Note
  that **cuObject will not get this by adopting NIXL**: the layering runs the other way (see
  below), so NIXL's AWS-contributed LIBFABRIC backend does nothing for cuObject's DC
  requirement. Still worth asking for a **transport-agnostic, documented, versioned**
  `x-amz-rdma-token` independently of EFA support, since without it cuObject-on-EFA would
  only ever interoperate between NVIDIA's client and a partner linking NVIDIA's closed
  server library.

## The path that does exist: a NIXL vendor engine (verified in source, 2026-08-22)

Reading NIXL's OBJ plugin settled a question this ADR originally left open, and the answer
reverses the assumed layering. **NIXL sits *above* cuObject, not below it.**
`src/plugins/obj/meson.build` treats `cuobj_dep` as an *optional* dependency: when found it
enables the "S3 Accelerated engines," and Dell's ObjectScale engine
(`src/plugins/obj/s3_accel/dell/`) states plainly that it "utilizes the CUDA Toolkit CUObject
Client library and the AWS S3 SDK." So cuObject is NIXL's accelerator, and asking cuObject
to adopt NIXL is architecturally backwards.

What the same source reveals is better than the ask it replaces — **NIXL already has an
open, in-tree, Apache-2.0 seam for exactly what we want to be:**

- `objAccelEngineRegistry` / `objAccelEngineRegistrar` — a string-keyed registry of
  accelerated object engines, selected at runtime by the backend params
  `accelerated=true, type=<vendor>`.
- `awsS3AccelClient`, documented verbatim as "the base class for vendor-specific accelerated
  implementations … to provide custom S3-compatible storage behavior (e.g., GPU-direct
  transfers)."
- A working precedent, Dell, whose `rdma_interface.h` defines `getObjectRdmaAsync` /
  `putObjectRdmaAsync` over an **opaque `std::string_view rdma_desc`** — a *vendor-defined*
  RDMA descriptor. Nothing in that interface shape mandates DC.

**cuObject enters only at the vendor leaf, and that is the whole point.** The natural
objection is "NIXL's accelerated path depends on cuObject, so a PR cannot escape DC." The
source refutes it layer by layer:

| layer | file | cuObject? |
|---|---|---|
| `iS3Client` — pure interface, `getObjectAsync(key, data_ptr, data_len, offset, cb)` | `obj_backend.h` | **none** |
| `DefaultObjEngineImpl` — vanilla S3 engine | `s3/engine_impl.h` | **none** |
| `S3AccelObjEngineImpl` — accel base, only overrides `getClient()` | `s3_accel/engine_impl.h` | **none** |
| Dell's engine | `s3_accel/dell/` | **yes**, `cuObjClient(…, CUOBJ_PROTO_RDMA_DC_V1)` |

So **NIXL does not depend on cuObject; Dell's engine does.** `obj_engine_registry.cpp/h` sits
in the *unconditional* `obj_sources` list — only the engine impls are inside
`if cuobj_dep.found()` — so the gate is an artifact of every accel engine written so far
happening to be cuObject-based, not a structural dependency. A `type=pacer` engine would
implement `iS3Client` over our own EFA/SRD transport and **need no cuObject details at all**.

`s3_accel/engine_impl.h` even invites it: "Vendor engines that support GPU-direct transfers
should override this to include `VRAM_SEG`" — a documented hook for precisely ADR-0027's
capability.

Note also the impedance match: `iS3Client` hands the engine a plain
`(data_ptr, data_len, offset)` destination, which for a `VRAM_SEG` registration is a device
pointer. That is exactly `x-pacer-target`'s shape, so the engine's real job is translating
NIXL's memory descriptor into our target descriptor.

Crucially, **NIXL's OBJ plugin is the *client* side** (it is an S3 client built on
aws-sdk-cpp, taking `endpoint_override`, `bucket`, credentials). So a `type=pacer` engine
would be a *consumer of* PACER, talking to the daemon over its existing protocol. That
places NIXL + aws-sdk-cpp + C++20 in the **consumer's** dependency closure — where NIXL
already is by assumption — and **not in the daemon image**. The ADR-0021 "genuine second
stack" objection does not apply; the daemon stays Rust/efadv/SRD.

That makes this a **pull request, not a negotiation**, and it is the concrete form of
learning #1 above: the client half wants to be a library that middleware links, and NIXL is
the middleware. It would deliver the "stock loader, no PACER-specific shim" lever this ADR
otherwise rules out — a Dynamo/vLLM/SGLang user sets two backend params and gets PACER's
EFA data plane.

**One real obstacle, and it is small:** `meson.build` wraps *all* of `s3_accel/` — Dell
included — in `if cuobj_dep.found()`, so today a vendor engine cannot build without the
closed cuObject client present. Decoupling that gate so a non-cuObject vendor engine can
compile is a ~10-line meson change, and it is the *entire* ask. Open design question for
the PR review rather than a blocker: `iDellS3RdmaClient` is scoped "Only Dell-specific
clients implement this interface," so a PACER engine either brings its own analogous
interface or NVIDIA prefers to generalize one.

**A NIXL build-vs-buy ADR remains a prerequisite** for pursuing this — not because of the
runtime cost (there is none for the daemon) but because contributing a NIXL engine is a
statement about NIXL's place in the project, and the repo currently has none.

> **Amendment, 2026-09-03 (merged a day late and reconciled on the way in).** That
> prerequisite ADR is [ADR-0031](0031-nixl-publish-as-a-backend-plugin.md), taken 2026-08-23
> on the strength of this section, **and it supersedes the route this section recommends.**
> The layering finding stands verbatim — NIXL sits above cuObject, only Dell's vendor engine
> links it, and the accel seam is cuObject-free layer by layer — but the *ask* built on it is
> withdrawn: re-verified against `ai-dynamo/nixl` @ `8770b655`, **`OBJ_SEG` is a core NIXL
> memory type** and `registerMem` hands every engine the `metaInfo` blob that `obj` itself
> uses as the object key, so a PACER engine needs neither `type=pacer`, nor the
> `objAccelEngineRegistry`, nor the `cuobj_dep` meson gate this section asks NVIDIA to
> loosen. One out-of-tree plugin declaring `{OBJ_SEG, DRAM_SEG, VRAM_SEG}` is the whole
> deliverable, and it is built and green (`spike/nixl`, 10/10).
> Read this section as the evidence that decided ADR-0031, not as a live proposal.

### The protocol constant names its transport — which is the precise ask

`s3_accel/dell/engine_impl.cpp:323` (and `:358`) constructs the client as:

```cpp
cuClient_ = std::make_shared<cuObjClient>(obs_ops, CUOBJ_PROTO_RDMA_DC_V1);
```

Two things follow. It **confirms in source** what the docs only implied — the DC binding is
real and sits in the client's own API surface, so this ADR's central inference is no longer
an inference. And it **sharpens the ask to something concrete**: the protocol is already
selected by a versioned, transport-named constant, so "make `x-amz-rdma-token`
transport-agnostic" reduces to *"add a `CUOBJ_PROTO_RDMA_SRD_V1` variant"* — an extension
the enum is visibly shaped for, not an architectural change. That is a far easier request to
carry into a conversation than anything else in this ADR.

### Their integrity answer is weaker than ours, and it is a source-verified finding

Dell's engine hits the same problem ADR-0026 point 5 solved: with data out-of-band the HTTP
body is empty, so the SDK would validate `x-amz-checksum-*` headers against zero bytes and
fail. Their fix, in code at `engine_impl.cpp:307-318` and applied in **both** constructors,
is to force `resp_checksum = "required"` — i.e. **disable** SDK response-body validation —
justified in-comment as "Transport integrity is ensured by RoCEv2 iCRC." Grepping the whole
636-line engine for `checksum|crc|verif|integrity|digest|md5|sha` finds nothing that
re-establishes an equivalent check.

**iCRC is not a substitute for an object checksum**, in three specific ways:

- It cannot see the **source-side staging** path (storage read → server buffer → DMA source).
  A flipped bit or a wrong buffer there produces a perfectly iCRC-valid transfer of wrong
  bytes.
- It is structurally blind to **placement errors** — correct bytes written at the wrong
  offset in the client's buffer is a semantic fault, not a corruption one.
- It is **per-hop, not end-to-end memory-to-memory**, which is the guarantee
  `x-amz-checksum-*` exists to provide.

So enabling RDMA on that path silently downgrades integrity from an end-to-end object
checksum to a wire CRC, invisibly to whoever set `accelerated=true`.

**Why this matters for EFA specifically: the mitigation does not port.** SRD is not RoCEv2,
and there is no documented end-to-end iCRC equivalent to cite, so an EFA port of this pattern
would inherit the disabled validation with nothing standing behind it. Confirming SRD's
actual end-to-end integrity guarantee is a question for the EFA team, who can answer it
authoritatively — and it should be asked before anyone advocates porting this pattern.

Ours is the stronger answer and is already built: `x-pacer-checksum: crc32=<hex>` over the
delivered bytes, verified client-side. CRC32 deliberately, because it is the same algorithm
the SDK's own checksum uses — it **restores the guarantee that was removed** rather than
inventing a new one — and it is re-checkable from Python's stdlib `zlib.crc32` with no new
dependency. The `checksum=none` opt-out and its rationale (verification is O(delivered
bytes); a 100 GB window would otherwise pay a full extra pass) is the pragmatic half a
vendor would need. This maps to a **second, smaller NIXL PR**: carry an out-of-band checksum
header instead of disabling validation.

**Limit on this finding, and it must be stated before the finding is used as a criticism:**
the check is absent from the *engine*. Whether the closed cuObject client performs one
internally is not verifiable from here. So the correct opening is a question — "where does
end-to-end integrity live on this path?" — not a published claim that it is missing.
- **AWS ships cuObject server support in S3 itself**, which would make it a backend
  question (how PACER *fetches*) rather than a delivery question (how PACER *serves*).

Neither is actionable from here, which is exactly why this is a watch item and not a
track.

## Consequences

- **No code changes, and no schedule change.** Track C keeps its critical-path position;
  C2 is unaffected. Nothing in *this* decision is a build item — but see **What we keep**
  below, which carries two things that should become work in their own right (a client-half
  ABI, and a client-memory *source* for writes), plus a design lever for C1's registration
  wall. None of them is a dependency of C2.
- **It is design validation, not a threat.** A third party converged independently on
  control-plane-over-S3-HTTP plus client-named RDMA target plus server-initiated WRITE
  into client GPU memory. That strengthens ADR-0026/0027's premise. And because cuObject
  runs only on ConnectX-class fabrics while PACER runs on EFA, there is **no overlap in
  deployable hardware today** — PACER-on-EFA is the AWS-shaped answer to the same problem.
  That is a better and more defensible framing for planning/04 than novelty was.
- **Do not confuse this with Track N.** cuObject and cuFile/GDS are different NVIDIA
  products sharing a docs domain, and every `cufile` reference in this repo
  (planning/20, `spike/gds/`) is GDS.
  Track N is untouched by this ADR.
- **One unverified compatibility claim, cheap to close.** A cuObject client pointed at
  PACER's S3 endpoint should send `x-amz-rdma-token`, get no `x-amz-rdma-reply`, and fall
  back to reading a normal body — the same graceful degradation ADR-0026 point 8 built for
  its own quota fallback. It *must* fall back, or it could not talk to any ordinary S3
  endpoint. But strip-and-re-sign ([ADR-0006](0006-strip-and-resign-auth.md)) touches
  headers, so the interaction is inferred rather than tested. Recorded in planning/04's
  unconfirmed list; it is a ten-minute test if a cuObject client is ever to hand, and it
  is the one thing here that would be a real bug if false.

## What we keep from it

"Not adoptable" is not "nothing to learn." Five things transfer, and they are the
reason this ADR is worth more than a one-line dismissal.

1. **The client/server split is a distribution strategy, and it is the thing we lack.**
   NVIDIA owns the *client* and ships it inside the CUDA Toolkit, so every CUDA
   application already has it; storage vendors then compete on the *server* half. PACER
   owns both halves, so adoption costs a prospect two decisions instead of one. Today the
   client half is three Python files (~870 lines in `clients/python/`)
   — small and cleanly separable, which is good — but it is a **shim, not a library**:
   nothing about it invites Torch DCP, a safetensors loader, or any other middleware to
   link it. Factoring the client half behind a small stable ABI (with the Python shim as
   one binding rather than the whole product) is what would let PACER be adopted by
   *middleware* instead of by application code. It is also the precondition for ever
   playing the cuObject-server role, should the transport question ever open.
2. **`allocHostBuffer` in their server API points at C1's wall.** Their server library
   allocates and registers pinned memory itself, rather than registering client memory per
   request. That is precisely the cost C1 measured and could not amortize: `ibv_reg_mr` at
   3.61 GB/s, ~1.19 s to pin a 4 GiB window, ~47 % of every client thread's time, and —
   unlike an HTTP round trip — **flat in object size** (planning/19 § Track C). Their API
   shape suggests inverting the ownership: one side allocates and registers once, the other
   side uses it. Worth weighing against ADR-0026 point 7's declare-once handle when that
   amendment is written. *Inference from a function signature, not a documented rationale* —
   but the direction ("never register per request") is corroborated by our own measurement,
   which is what makes it worth acting on.
3. **A target descriptor must be transport-agnostic, and ours accidentally is.** cuObject's
   whole blocker reduces to its token encoding a DC target. `x-pacer-target`'s
   scheme-prefixed grammar extends by scheme and carries no fabric assumption, and parsing
   rejects an unknown scheme rather than guessing ([`delivery.rs`](../../crates/pacer-daemon/src/delivery.rs)).
   **Protect that deliberately**: never let an SRD address handle, rkey, or rail index leak
   into the descriptor's public grammar. Their mistake is instructive exactly because it is
   the one we are one careless commit away from making.
4. **Their op set names a real gap in ours: writes.** cuObject covers PUT and UPLOAD_PART
   by RDMA-READ *out of* client memory. PACER's delivery is **GET-only** by construction,
   and [ADR-0007](0007-write-through-read-after-write.md)'s write-through path has no
   delivery equivalent. Checkpoint *save* is as hot as restore in a training loop — every N
   steps, frequently blocking it — so a client-memory *source* is the mirror-image feature
   Track C does not have. This is the most concrete thing cuObject teaches us about our own
   scope, and it deserves its own ADR rather than a bullet here.
5. **RANGE_GET as a first-class operation** corroborates
   [ADR-0015](0015-chunk-granular-caching.md): a GPU-direct object protocol needs ranged
   reads as a primitive, not as an afterthought, which is what chunking already made true
   here.

## Limits of the evidence

Sourced from the cuObject documentation index page only, at v1.2.0. That page states no
supported-NIC list, does not say whether the server library ships as source, does not
disclose its underlying RDMA library, and makes no forward-looking transport statement —
so the mlx5 binding is inference (well-founded: DC has no other implementation) and the
absence of a plugin interface is absence of evidence rather than a documented "no."
Neither uncertainty changes the decision: without source or a plugin slot, the port is
not ours to make regardless of what it is built on.

## Status / relationship to other ADRs

- **ADR-0026 / ADR-0027** — unchanged and reaffirmed. This ADR explains why an
  externally-standardized header protocol does not displace `x-pacer-target`.
- **[ADR-0031](0031-nixl-publish-as-a-backend-plugin.md)** — the NIXL decision this ADR asked
  for, taken the next day on this ADR's source reading, and the reason the `type=pacer`
  accel-engine route above is **superseded** (see the amendment in § *The path that does
  exist*). ADR-0031 also carries the cuObject/DC finding forward as its own deciding input,
  so the two agree on the fabric argument and differ only on the seam to publish into.
- **ADR-0021** — the efadv/ibverbs choice is what makes the provider mismatch concrete.
- **ADR-0022** — its HBM-target direction is the capability cuObject also exercises,
  reached by a different stack.
- **planning/04 §5, §7** — novelty claims amended per decision point 4.
- **planning/06** — standing-verification-list entry carries the two re-open triggers.
