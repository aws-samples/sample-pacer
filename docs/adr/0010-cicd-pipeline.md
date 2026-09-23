# ADR-0010: CI/CD pipeline — stages, quality gates, and release process

Date: 2026-07-10 · Status: Accepted

## Context

Phase 0 left CI covering lint (fmt/clippy/helm), unit tests (amd64 only), per-arch native
binary builds, and multi-arch image publish (ADR-0009). Missing before the project can call
its pipeline "done": functional tests, supply-chain checks, versioned releases, and a story
for load testing. The roadmap already treats benchmarks as *phase gates* (Warp methodology in
05-architecture.md), which shapes where load testing belongs.

Constraints carried over from ADR-0009: no Docker socket (kaniko only, no `RUN` under
emulation), and **every job must carry an explicit `kubernetes.io/arch` node selector**
(global default `amd64`) or the runner picks the wrong helper image.

## Decision

Pipeline stages: `check → build → functional → image → scan → manifest → release`.
Branch pipelines run through `build`/`functional` (images built but not pushed off the
default branch); the default branch additionally publishes `:latest` + `:<sha>`; `v*` tags
publish the versioned release.

1. **check** — `cargo fmt`, `clippy -D warnings` (default and `--all-features`, which covers
   the `efa` flag as it grows a real dependency graph), `cargo test`, `cargo doc
   --no-deps` with warnings denied, `helm lint` + template renders, and **`cargo deny check`**
   (RustSec advisories, license allowlist compatible with Apache-2.0, source policy) against
   the committed `deny.toml`. Advisory ignores must carry a written reason in `deny.toml`.
   **`efa`-feature note:** once the `efa` flag pulls in git-pinned ibverbs + system
   libibverbs/libfabric (ADR-0005), `--all-features` clippy/doc require those libraries on
   the CI build image — on **both** arches (ADR-0009). Provide them in the build image when
   the deps land, or `--all-features` breaks `check`. `efa` code paths that need real RDMA
   hardware are **not** unit-tested in CI; they are exercised only on the ADR-0008 spike nodes.
2. **build** — unchanged per-arch native release builds, plus **`cargo test --release` now
   runs in both matrix entries**, closing ADR-0009's "can later be tested on real arm64
   hardware" gap: the arm64 binary is tested on a Graviton node every pipeline.
3. **functional** — `ci/functional-smoke.sh` boots the *release binary from the build stage*
   (both arches, natively) and exercises the admin endpoint as kubelet would. This stage is
   the designated home of the **Phase 1 correctness suite** (boto3/AWS CLI against the S3
   listener, incl. read-after-write): the roadmap's "correctness suite green" exit criterion
   is defined to mean *green in this CI stage*, not on someone's laptop.
4. **scan** — Trivy scans both arch images (CRITICAL/HIGH, `--ignore-unfixed`, fail the
   pipeline) after they are pushed, *before* the manifest list is stitched — a failed scan
   blocks `:latest`/release tags but not the throwaway `<sha>-<arch>` tags. Runs only where
   images are pushed (default branch + tags).
5. **release (git tag `v*`)** — the same pipeline additionally: pushes the image manifest
   list as `:vX.Y.Z` (does **not** move `:latest`; that tracks the default branch), packages
   the Helm chart with `--version X.Y.Z --app-version vX.Y.Z` and pushes it to the project's
   OCI registry under `charts/`, and creates a GitLab Release for the tag. Chart `appVersion`
   = image tag, so a released chart pins its released image with no values overrides.
6. **Load testing is deliberately NOT a per-commit gate.** Real-hardware Warp runs are
   cluster-scale, minutes-long, and noisy — as a merge gate they would be flaky and expensive.
   Two tiers instead:
   - micro-benchmarks (criterion) on hot in-process paths (cache policy, ring hashing) get a
     CI job when those paths exist (Phase 1/2), with results archived as artifacts for trend
     review;
   - the Warp methodology of 05-architecture.md runs as a **manual pipeline job against the
     test cluster starting Phase 2**, results committed to `planning/`. Phase 3 adds the
     **restore-storm benchmark** (ADR-0015/0016/0020 exit + tuning gate: chunk_size,
     replication_r, admission thresholds, directory v2). This job is the evidence source for
     ADR-0008's **per-primitive** flips — WRITE data plane (throughput + CPU at fan-out) and
     directory READ v2 (home-CPU under storm) are separate decisions with separate signals,
     not one "RDMA flip."
7. **Crates are not published to crates.io** (`publish = false` workspace-wide); the release
   artifacts are the container image and the Helm chart. Revisit only if a crate (e.g.
   `pacer-ring`) proves independently useful.

Supporting change: the AWS SDK dependencies now disable default features to drop the legacy
hyper-0.14/rustls-0.21 TLS stack the SDK still carries for backward compatibility — it had
three open RustSec advisories and the modern `default-https-client` path replaces it.

## Consequences

- An MR/branch cannot merge with: fmt/clippy/doc warnings, failing unit or smoke tests on
  either arch, a RustSec advisory, or a license outside the allowlist. New dependencies with
  LGPL/MPL/unknown licenses require an explicit `deny.toml` change, which is reviewable.
- `deny.toml` currently ignores two unmaintained-crate advisories pulled in by `foyer`
  (bincode 1.x, paste) with reasons recorded; re-check on every foyer upgrade.
- Trivy runs post-push, so a vulnerable base image can briefly exist as `<sha>-<arch>` tags;
  it can never become `:latest` or a release tag.
- Releases are cheap and mechanical: `git tag v0.2.0 && git push origin v0.2.0`. Nothing else
  edits versions — Chart.yaml's committed version stays `0.1.0` as a placeholder and is
  overridden at package time.
- Two more jobs per pipeline (deny, smoke×2) at ~1–3 min each, parallel with existing work;
  wall-clock impact is small. The scan stage adds ~1 min on publish pipelines only.
- Anything added later must keep the arch-selector invariant (ADR-0009) — the global default
  covers new jobs unless they override `KUBERNETES_NODE_SELECTOR_ARCH`.
