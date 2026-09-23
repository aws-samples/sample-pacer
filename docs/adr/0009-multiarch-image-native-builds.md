# ADR-0009: Multi-arch image (amd64+arm64), built natively per arch in CI

> **The figures here are development-phase experiment records, not benchmarks** — see
> [the note in the index](README.md#the-figures-in-these-records-are-not-benchmarks). Numbers
> meant for quoting live in [`docs/benchmarks/`](../benchmarks/README.md).

Date: 2026-07-10 · Status: Accepted

## Context

Phase 0 shipped an amd64-only image, and CI globally pinned job pods to amd64 because pods
landing on Graviton nodes died in the runner's prepare stage. Two pressures against staying
amd64-only:

- The DaemonSet should eventually run on whatever node families make sense per workload;
  Graviton (arm64) instances are a real candidate for cost/perf, and single-arch images
  would make the chart arch-dependent.
- The CI cluster's `gitlab-ci` nodepool already serves both arches, so the build
  infrastructure imposes no constraint.

Root cause of the Phase 0 arm64 failure (understood only during this work): the GitLab
runner's Kubernetes executor selects the **helper-image architecture from the job pod's
`kubernetes.io/arch` node selector**. Phase 0 jobs carried no arch selector, so pods that
happened to land on Graviton were given the amd64 helper and failed to start. It was a
scheduling/selector bug, not an arm64-capability limitation.

Constraints on the build mechanism:

- The runner has no Docker socket; images are built with kaniko, which cannot execute
  `RUN` steps under emulation (no buildx/QEMU path) — a single multi-platform Dockerfile
  build is not possible.
- The workspace pulls in C dependencies (`aws-lc-sys`, `ring`, `zstd-sys`), so any
  cross-compile needs a full aarch64 GCC toolchain plus linker env plumbing.

## Decision

1. **The image is multi-arch: linux/amd64 + linux/arm64**, published as a manifest list
   (`:latest` and `:<short-sha>`) so `image:` references stay arch-agnostic.
2. **Each arch binary is built natively** on a node of its own architecture: the
   `build-binary` matrix job overwrites `KUBERNETES_NODE_SELECTOR_ARCH` per entry.
   No cross toolchain.
3. **Every CI job carries an explicit arch node selector** (global default `amd64`).
   This is the invariant that prevents the Phase 0 helper failure — the selector is what
   tells the runner which helper image to use.
4. **Image assembly is split from compilation**: kaniko builds one single-arch image per
   prebuilt binary from a COPY-only `Dockerfile.runtime` (`--custom-platform` picks the
   distroless base variant); `mplatform/manifest-tool` stitches the arch-suffixed tags
   into the manifest list on the default branch. The root `Dockerfile` remains a
   self-contained single-arch build for local use.

## Consequences

- `helm install` works unchanged on amd64 and arm64 nodes; the registry serves the right
  image per node. Node-arch choices become a pure cost/perf decision.
- Native builds keep CI simple (plain `cargo build`) and honest: the arm64 binary is
  compiled — and can later be tested — on real arm64 hardware, not an emulated or
  cross-linked approximation. An earlier cross-compile variant worked but carried a
  toolchain-install step and per-target linker env; superseded the same day.
- Two binaries and two kaniko runs per pipeline instead of one; arch builds run in
  parallel so wall-clock is unchanged (~3 min build stage). Cargo caches are keyed
  per arch.
- `Dockerfile` and `Dockerfile.runtime` runtime stages (ENV/ENTRYPOINT) must be kept in
  sync by hand — noted in both files.
- Anything added to CI later (new jobs, services) must keep an explicit arch selector or
  risk the prepare-stage helper failure returning intermittently.
- **Phase 3 / `efa` is multi-arch, not amd64-only.** EFA-capable families span both
  arches — amd64 (p5/p5en/p4d) and arm64 (c8gn/c9gn-class Graviton, Nitro v5+,
  hundreds of Gbps EFA; ADR-0004). This *reinforces* native per-arch builds: the
  arm64 image is a real RDMA target, so its binary must be genuinely EFA-capable,
  not a symmetry stub. Concrete CI consequence: when the `efa` deps land, the arm64
  build node needs `libibverbs`/`libfabric` (efadv) present just like amd64 — fold
  this into the per-arch build image, and run the ADR-0008 one-sided WRITE/READ
  spike on real Graviton EFA hardware, not only x86.
