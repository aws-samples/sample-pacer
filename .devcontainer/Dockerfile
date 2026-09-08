# syntax=docker/dockerfile:1
# Default PACER devcontainer: everything `make ci` needs for the workspace as it
# builds by default (no `efa` feature).
#
# Base is the exact image the `rust` CI job uses (.gitlab-ci.yml `.rust`), so
# "green in my container" and "green in CI" mean the same thing for fmt, clippy,
# test, doc and smoke. The one gate it CANNOT reproduce is `--features efa`, which
# needs the EFA userspace — that is what .devcontainer/efa is for.
FROM public.ecr.aws/docker/library/rust:1.96-bookworm

# The official Rust image carries a MINIMAL toolchain: `rustup component list
# --installed` in 1.96-bookworm returns cargo, rustc and rust-std — no rustfmt and
# no clippy, so `make lint` fails on a bare image. CI's `rust` job adds exactly
# these two for the same reason.
RUN rustup component add rustfmt clippy

# helm + cargo-deny, the two things `make lint` and `make deny` call that a Rust
# image has no reason to ship. Baked in rather than run as postCreateCommand so a
# rebuilt container is immediately ready and the download happens once.
COPY install-dev-tools.sh /tmp/install-dev-tools.sh
RUN /tmp/install-dev-tools.sh && rm /tmp/install-dev-tools.sh

# A LOGIN shell (`bash -l`) sources /etc/profile, which resets PATH and drops the
# image's own /usr/local/cargo/bin — `cargo: command not found` in a terminal that
# happens to open one. Measured, not theoretical. Put it back.
RUN printf 'export PATH="%s/bin:$PATH"\n' "$CARGO_HOME" > /etc/profile.d/rust.sh

# `make smoke` boots the daemon and probes /healthz with curl; the rust image
# carries curl but not the rest of what a shell session wants.
RUN apt-get update \
    && apt-get install -y --no-install-recommends jq less \
    && rm -rf /var/lib/apt/lists/*
