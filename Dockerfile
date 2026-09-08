# Self-contained build for local use. CI builds the multi-arch image from
# Dockerfile.runtime instead (natively built per-arch binaries), and the
# GitHub release workflow from ci/Dockerfile.release — keep the runtime stage
# (ENV/ENTRYPOINT) in sync across the three.
# Build stage: AL2023 build → AL2023-minimal runtime (matching glibc lineage —
# no ECR Public mirror exists for gcr.io/distroless, and mixing glibc families
# between build and runtime risks a GLIBC_x.y-not-found failure at load time).
#
# Deliberately NO `--features efa` here: this is the plain local build (no EFA
# hardware on a dev box). The shipping EFA image is built by CI from
# Dockerfile.runtime + ci/Dockerfile.efa-builder (planning/11 item #4).
FROM public.ecr.aws/amazonlinux/amazonlinux:2023.12.20260831.0 AS builder
RUN dnf install -y --allowerasing gcc gcc-c++ curl ca-certificates
ARG RUST_VERSION=1.96.0
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y --default-toolchain "${RUST_VERSION}" --profile minimal
ENV PATH="/root/.cargo/bin:${PATH}"
WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --release -p pacer-daemon

FROM public.ecr.aws/amazonlinux/amazonlinux:2023.12.20260831.0-minimal
# checkov:skip=CKV_DOCKER_2:Health is checked by Kubernetes, not by Docker. This
# image only ever runs as the DaemonSet's container, where the chart's
# livenessProbe/readinessProbe hit /healthz and /readyz — and a container runtime
# under kubelet ignores HEALTHCHECK entirely. Satisfying the check would also mean
# putting a shell and an HTTP client into an -minimal image that deliberately has
# neither, which trades a real reduction in attack surface for a directive nothing
# in the deployment path reads.
# UID/GID 65532 is a hard contract with deploy/helm/pacer (the DaemonSet's init
# chown) and the bench Job manifests (runAsUser: 65532) — it must match exactly
# what the old gcr.io/distroless/*:nonroot tag baked in. AL2023-minimal has no
# useradd/shadow-utils, so add the passwd/group entries directly, the same way
# distroless itself fakes a "nonroot" user.
RUN echo 'pacer:x:65532:65532:pacer:/nonexistent:/sbin/nologin' >> /etc/passwd \
 && echo 'pacer:x:65532:' >> /etc/group
COPY --from=builder /src/target/release/pacer-daemon /usr/local/bin/pacer-daemon
# Cache dir is a hostPath mount in-cluster; this default serves local runs.
ENV PACER_CACHE_DIR=/tmp/pacer-cache
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/pacer-daemon"]
