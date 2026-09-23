# ADR-0031: Publish PACER as a NIXL backend plugin; do not adopt NIXL as the transport

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-08-23 · Status: Accepted. Re-verified against the NIXL tree the same day
(`ai-dynamo/nixl` @ `8770b655`, v1.4.0), which **withdrew this ADR's own claim that
object-keyed access does not fit a backend engine** and collapsed § Decision 4 into
§ Decision 2: one plugin, object-keyed, no upstream dependency and no `cuobjclient` gate

**Then observed, same day, by `spike/nixl` (10/10 checks):** an
out-of-tree `PACER_HELLO` plugin declaring `{OBJ_SEG, DRAM_SEG, VRAM_SEG}`, loaded from
`NIXL_PLUGIN_DIR` into a NIXL built without the `obj` backend, served `OBJ_SEG ↔ DRAM_SEG`
and `OBJ_SEG → VRAM_SEG` byte-for-byte against the registered keys with no CUDA present. The
last inferred step of § Decision 2 is therefore no longer inferred; § Consequences' "one
conclusion is read from the interface, not observed" is discharged, and § Decision 2 gains a
packaging caveat about `NIXL_PLUGIN_DIR` that the run exposed.

The ADR `planning/06-roadmap.md` § "Standing verification list" has been asking for —
*"NIXL on EFA is GA (2026-03) and an ADR is owed — decide, do not re-research."* Also
records the first written position on **cuObject**, which appears nowhere else in this
repository despite being the reason our own transport is not redundant.

## Context

**cuObject cannot reach EFA, and that is structural.** NVIDIA's cuObject/GDS S3 flow
states that the library *"establishes a DC connection with the client"*. DC — Dynamically
Connected transport — is an mlx5/ConnectX-specific transport type reached through
`mlx5dv`/DevX; it is not in the IBTA specification and no other vendor implements it. EFA
offers SRD and UD-like semantics only: no RC, no DC, no atomics — the constraint set the A0
spike already had to design around ([ADR-0021](0021-efadv-ibverbs-not-libfabric.md),
planning/21). The surrounding ecosystem agrees: UCX warns that a BAR1-mapped dma-buf handle
*"can only be used in conjunction with `mlx5dv_reg_dmabuf_mr`"*, and Mooncake pins its
export to `flags=0` because the PCIe/BAR1 route is mlx5's Direct-NIC path.

Not verified, and cheap to settle: `cuobjclient` is a closed binary from the CUDA toolkit
and NIXL's seam exposes no transport surface (`awsS3AccelClient` is a thin base over
`awsS3Client`), so *"cuObject has no alternative non-DC path"* remains inference. Build the
`s3_accel` tier on an EFA node and see whether it enumerates the device at all.

**Consequence for [ADR-0022](0022-gpudirect-hbm-target-and-multi-rail-efa.md) and
[ADR-0030](0030-delivery-registration-belongs-to-the-memory-owner.md):** our EFA transport
is not a reimplementation of something NVIDIA already ships. It is the piece that makes
GPU-direct object delivery possible on AWS at all. That is a sharper claim than "we are
faster", and it is the argument for keeping the transport.

**NIXL's structure, verified in-tree rather than assumed.** Three levels, and NIXL
overloads "engine" across two of them:

- A **backend engine** is a `nixlBackendEngine` subclass (e.g. `nixlMooncakeEngine`) that
  registers memory, connects to peers and posts transfers. This is the code.
- A **plugin** is the ABI-versioned descriptor that advertises an engine, built by
  `nixlBackendPluginCreator<EngineType>` (`src/api/cpp/backend/backend_plugin.h:63`):
  api version, `createEngine`/`destroyEngine`, `getName()`, `getVersion()`, `getParams()`
  (the customer's config keys) and `getMemList()` (the memory types it advertises at
  discovery time — note that *routing* reads the engine's `getSupportedMems()` instead, and
  the two may disagree; see below). It ships either as `extern "C" nixl_plugin_init()` in a
  `dlopen`ed `.so`, or compiled in behind `STATIC_PLUGIN_<NAME>` — **the same engine, one
  `#ifdef` apart**.
- Separately, *inside* the `obj` backend, an `objAccelEngineRegistry` selects a
  sub-implementation (`s3`, `s3_crt`, `s3_accel/dell`) by the `type=` config parameter.
  These are not plugins; Dell's contribution is `type=dell`, not a plugin named DELL.

Two facts make this cheap for us. `src/core/nixl_plugin_manager.cpp` discovers and
`dlopen`s plugins from a directory that `NIXL_PLUGIN_DIR` overrides — so **a plugin needs
no upstream involvement to reach customers**. And `src/plugins/mooncake/` and
`src/plugins/uccl/` are each *three source files* plus a `meson.build` and a README: both
are independent third-party transfer engines published exactly this way, which is our
situation precisely.

**Object-keyed access is not confined to the `obj` backend.** The first draft of this ADR
asserted that *"GET key X into this tensor"* does not fit the interface a backend engine
implements, and offered that as the reason the `obj` backend exists at all. **That was
wrong**, and the mechanism which falsifies it is `obj`'s own. Four facts, each read from
the tree rather than inferred:

- **`OBJ_SEG` is a core memory type**, not a property of the `obj` plugin:
  `enum nixl_mem_t {DRAM_SEG, VRAM_SEG, BLK_SEG, OBJ_SEG, FILE_SEG}`
  (`src/api/cpp/nixl_types.h:41`). Nothing under `src/core/` gates it to a plugin name.
- **Every engine receives a metadata blob at registration, and that blob is the object
  key.** `registerMem(const nixlBlobDesc &mem, …)` is on the base class
  (`src/api/cpp/backend/backend_engine.h:120`) and `nixlBlobDesc` carries
  `nixl_blob_t metaInfo` (`src/api/cpp/nixl_descriptors.h:148`). `obj` reads exactly that
  — `s3/engine_impl.cpp:138` uses `mem.metaInfo`, falling back to
  `std::to_string(mem.devId)` when empty — and `posix` does the same with a
  `"<modes>:<path>"` string (`posix_backend.cpp:235`).
- **Dispatch is by declared memory type, taken from the engine at runtime.**
  `nixl_agent.cpp:401` iterates `backend->getSupportedMems()` into `memToBackend[]`, which
  a transfer looks up as `memToBackend[descs.getType()]` (`:456`). `getSupportedMems()` is
  pure virtual per engine (`backend_engine.h:114`), so the **engine's** claim routes — not
  the plugin descriptor's static `getMemList()`, which for `obj` is `{DRAM_SEG, OBJ_SEG}`
  and omits VRAM (`obj_plugin.cpp:26`).
- **GPU-direct object delivery is an engine opt-in, nothing more.** The vendor seam adds it
  precisely this way: `s3_accel/dell/engine_impl.h:135` returns
  `{OBJ_SEG, DRAM_SEG, VRAM_SEG}`, and `obj/README.md:330` requires a vendor engine to
  override `getSupportedMems()` to include `VRAM_SEG`.

**What survives of the claim is narrower: the key binds at registration, not per
transfer.** `nixl_xfer_dlist_t` is `nixlDescList<nixlBasicDesc>` and carries no blob
(`nixl_descriptors.h:466`); only `nixl_reg_dlist_t` does (`:471`). At transfer time an
engine sees `nixl_meta_dlist_t`, whose elements hold just the opaque `nixlBackendMD*
metadataP` it returned from its own `registerMem` (`backend_aux.h:92`). So a key survives
as engine-private state hung off registration — register the key, transfer against the
handle. A previously unseen key therefore costs a `registerMem`, which makes a handle
cache mandatory; [ADR-0030](0030-delivery-registration-belongs-to-the-memory-owner.md) § 5
already requires one, keyed on `CU_POINTER_ATTRIBUTE_BUFFER_ID`.

**And the `cuobjclient` gate is narrower than first recorded.** `meson.build:152-157`
probes `cuobjclient-13.3`, `-13.2` and `-13.1` in turn, and `if cuobj_dep.found()`
compiles the whole `s3_accel/` subtree, base plus `dell`
(`src/plugins/obj/meson.build:43-55`). **The gate is on that subtree, not on `OBJ_SEG` or
`VRAM_SEG`.** That asymmetry is the opening: NIXL's only GPU-direct object path needs
cuObject present to build, while the two memory types it uses are free for any plugin to
declare — and per the DC finding above, we are the only engine that could serve
`OBJ_SEG → VRAM_SEG` on EFA.

## Decision

1. **Do not adopt NIXL as the daemon's transport.** Phase 3 measured ours to line rate and
   360.709 GiB/s into HBM across 32 rails; swapping a C++20 dependency into that path
   trades a measured result for convenience. `keep` is the answer to keep-vs-wrap-vs-adopt
   for the data plane.

2. **Publish the client half as a NIXL backend plugin declaring
   `{OBJ_SEG, DRAM_SEG, VRAM_SEG}`** — `OBJ_SEG` because we are object-keyed, DRAM and VRAM
   because those are exactly ADR-0026's and ADR-0030's two delivery targets — with
   `getParams()` carrying the endpoint and bucket, and the object key arriving as
   `nixlBlobDesc::metaInfo` on `registerMem`. Like `obj`, it is `supportsLocal()` true and
   `supportsRemote()` false (`obj_backend.h:137`): the agent pulls from PACER into its own
   memory, so there is no NIXL-level peer-metadata exchange to implement. Out-of-tree `.so`
   first (we keep our release cadence), upstream beside
   `mooncake` and `uccl` when it has run in anger. Same code, one `#ifdef`; there is no
   rework between the two, so "ship now, upstream later" costs nothing.

   **Delivery is "drop the `.so` into the customer's plugin directory", not "point
   `NIXL_PLUGIN_DIR` at ours" — corrected by `spike/nixl`.** `getPluginDir()` returns the
   environment variable *or* the default, never both, and only that one directory is ever
   scanned: in the spike, setting `NIXL_PLUGIN_DIR` to a directory holding only our plugin
   made in-tree `POSIX` **disappear** from `getAvailPlugins()`. Repointing the variable would
   therefore hide the UCX/LIBFABRIC/OBJ plugins the customer's stack already depends on. The
   install target is `<dir of libnixl.so>/plugins`, which needs no environment variable at
   all; `NIXL_PLUGIN_DIR` is for a directory that holds *every* plugin, or for our own
   testing. Two smaller mechanics from the same run: the **filename** `libplugin_<NAME>.so`
   is what names the backend to `createBackend()` (`get_plugin_name()` is cosmetic), and an
   out-of-tree build needs `-lnixl -lnixl_build`, two include roots and an Abseil matching
   the core's — see the spike README.

3. **Build the client half once, as a library with a stable `extern "C"` surface**, so the
   PACER client API and the NIXL plugin are both thin adapters over it. ADR-0030 § 5 lists
   what that library owns — rail↔GPU affinity, the `BUFFER_ID`-keyed MR cache, the
   don't-read-before-200 rule, the non-EFA fallback — and **that list is what a NIXL
   backend engine implements.** The work is not additive; only the front door differs. A
   Python-shaped client library built first and adapted later pays for it twice.

   **Done as of 2026-08-24: [`crates/pacer-client`](../../crates/pacer-client)** is that
   library — window registration (host `mmap` or `ibv_reg_dmabuf_mr` over HBM the client
   exported), token rendering, and the announce pump that keeps a receive posted and inserts
   an address handle per writer — behind a `#[no_mangle] extern "C"` surface with opaque
   handles and out-parameters. Two adapters exist already and neither reimplements anything:
   `clients/python/pacer_nic.py` (ctypes) and `spike/efa`'s `token-client` role, which is what
   puts the library on hardware. A NIXL backend engine is the third, and the shape of its work
   is now visible rather than projected: `pacer_client_open` → `nixlBackendEngine::registerMem`,
   `pacer_client_token` → the metadata a `postXfer` carries, `pacer_client_stats` →
   `checkXfer`'s health answer. Of ADR-0030 § 5's list it implements the last two and defers
   the first two by construction (one rail, one lifetime registration).

4. **`type=pacer` inside the `obj` backend is not needed, and that is this ADR's main
   finding.** Decision 2's plugin already serves object-keyed reads into DRAM or HBM
   without entering the `s3_accel` subtree, without the `cuobjclient` gate and without
   upstream involvement of any kind. **Routes 2 and 4 are one artifact**, so the earlier
   framing — that object-keyed access *belongs* inside `obj`, is blocked on a build gate,
   and needs a sidestep ("sit as a peer to `s3`/`s3_crt`", or upstream a per-engine gate) —
   is withdrawn in full. `type=pacer` stays available later as an **ergonomic** choice, not
   a capability one: a customer already configuring `OBJ` would flip a `type=` string
   instead of installing a plugin. If we ever want it, the portability argument still holds
   — *make the accelerated object-engine seam work on non-mlx5 fabrics* is a general
   improvement to NIXL that AWS has an interest in, not a vendor favour. It is on no
   critical path.

5. **Record the reach argument, since it is the whole point.** Today a customer must point
   a loader at PACER. As a NIXL plugin, PACER becomes a config string inside a stack
   (vLLM, Dynamo, LMCache) that they already run. That inverts the adoption problem, and it
   is worth more than any throughput increment. Decision 4's collapse is what makes this
   true of the *shippable* artifact rather than of a deferred one: a loader asking for a
   key gets PACER, so the reach argument no longer depends on anything upstream.

## Consequences

- **A C ABI shim over `pacer-transport`.** Plugin entry points are C++; ours is Rust.
  Mooncake and UCCL are C++ calling C++, so we would be the first C-ABI plugin here.
  Routine, but real, and it is the reason Decision 3 specifies `extern "C"` from the start.
  NIXL's Rust bindings are for the *client* API, not for authoring plugins.
- **Publishing gives up protocol control — and this now applies to the one artifact we
  ship, not to a deferred route.** `x-pacer-target` becomes an internal detail of the
  engine, and NIXL's config surface plus the `metaInfo` key convention become the public
  contract.
- ~~**One conclusion is read from the interface, not observed.**~~ **Discharged the same day
  by `spike/nixl`.** `obj` being the only in-tree `OBJ_SEG`
  declarer left *"an out-of-tree plugin may declare `OBJ_SEG`"* resting on the four facts
  above rather than on a run; the hello-world plugin this bullet asked for was built and is
  green, 10/10 checks, in a NIXL image with the `obj` backend deliberately absent. Three of
  this ADR's readings became observations: `createXferReq` routes an `OBJ_SEG` descriptor to
  an out-of-tree engine; routing follows the **engine's** `getSupportedMems()`, not the
  descriptor's `getMemList()` (the spike's descriptor omits `VRAM_SEG` and the
  `OBJ_SEG → VRAM_SEG` leg still routed and delivered); and the key binds at `registerMem`
  and reaches transfer time as engine-private state. A fourth thing fell out that was not
  predicted: **nothing under `src/core/` validates that a `VRAM_SEG` range is device memory**
  — the whole VRAM leg ran in an image with no CUDA toolkit and no GPU, on host memory
  wearing the label. NIXL dispatches on the declaration; device residency is entirely
  [ADR-0030](0030-delivery-registration-belongs-to-the-memory-owner.md)'s problem.
- **EFA is already first-class upstream.** `nixlAgentData::warnAboutEfaHardwareMismatch()`
  (`nixl_agent.cpp:299-315`) warns at first registration when EFA devices are present but
  `UCX` was configured without `LIBFABRIC`. Minor, but it confirms Decision 1 is not
  fighting the grain: upstream steers EFA users toward the fabric-native backend rather
  than assuming ConnectX.
- **Our numbers become publicly comparable.** `benchmark/nixlbench` exists, so publishing
  puts PACER into a standardised comparison against cuObject-on-ConnectX. Given the
  measured results that is likely a plus, but it should be a choice rather than a surprise.
- **IP posture must be settled before anything is shared.** Upstream is Apache-2.0 and this
  repository is MIT-0; permissive-to-permissive is compatible, but it needs deciding rather
  than defaulting.
- **This changes who calls us, not whether the primitive works.** ADR-0030's loopback-WRITE
  gate is untouched by any of the above. One exception worth checking: NIXL selects a
  backend per peer, so a same-node peer may be served by `cuda_ipc` rather than the fabric
  — which would sidestep the loopback question instead of answering it, at the cost of
  putting CUDA back in whichever process does the copy.
