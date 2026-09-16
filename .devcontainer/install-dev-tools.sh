#!/usr/bin/env bash
# The two tools `make ci` needs that no Rust base image carries: helm (for
# `helm lint deploy/helm/pacer`) and cargo-deny (for the licence/advisory gate).
#
# Shared by BOTH devcontainer variants — baked into the slim image at build time,
# run as postCreateCommand in the EFA one — so the versions are pinned in exactly
# one place and the two containers cannot drift apart.

set -euo pipefail

## Matches CARGO_DENY_VERSION in .gitlab-ci.yml. Bump both together, or `make
## deny` starts disagreeing with the `deny` job about what is allowed.
readonly CARGO_DENY_VERSION=${CARGO_DENY_VERSION:-0.20.2}

## Deliberately unpinned WITHIN the major line, because CI is too: the `helm` job
## runs on the floating `alpine/helm:4` tag, so pinning a version here would make
## the container STRICTER than the gate it is meant to reproduce. Set
## DESIRED_VERSION (e.g. v4.2.4) to pin it anyway.
##
## The installer is per-major (upstream ships get-helm-3 AND get-helm-4 side by
## side, and Helm 3 is still maintained in parallel), so this URL — not a version
## string — is what holds the workspace on Helm 4. `helm upgrade` reads and writes
## the same `sh.helm.release.v1` records under both, so a release installed by
## Helm 3 upgrades in place.
readonly HELM_INSTALLER=https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-4

## Where both tools land: on PATH for every shell, without touching CARGO_HOME
## (which is a mounted volume in the devcontainers and would make the install
## vanish on the next `docker volume rm`).
readonly BIN_DIR=/usr/local/bin

# cargo-deny publishes per-target tarballs and helm per-GOARCH ones, so the same
# script has to work on an Apple Silicon laptop (arm64) and an x86 CI node.
case "$(uname -m)" in
  x86_64)         deny_target=x86_64-unknown-linux-musl ;;
  aarch64|arm64)  deny_target=aarch64-unknown-linux-musl ;;
  *) echo "install-dev-tools: unsupported architecture $(uname -m)" >&2; exit 1 ;;
esac

echo "install-dev-tools: cargo-deny $CARGO_DENY_VERSION ($deny_target)"
deny_archive="cargo-deny-$CARGO_DENY_VERSION-$deny_target"
curl -sSfL "https://github.com/EmbarkStudios/cargo-deny/releases/download/$CARGO_DENY_VERSION/$deny_archive.tar.gz" |
  tar xz --strip-components=1 -C "$BIN_DIR" "$deny_archive/cargo-deny"

echo "install-dev-tools: helm (${DESIRED_VERSION:-latest 4.x})"
curl -sSfL "$HELM_INSTALLER" | HELM_INSTALL_DIR="$BIN_DIR" USE_SUDO=false bash

cargo-deny --version
helm version --short
