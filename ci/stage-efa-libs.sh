#!/bin/sh
# Stage the EFA userspace runtime closure for the daemon image (Phase 3 A2,
# planning/11 item #4). Runs INSIDE the EFA builder image (where the
# aws-efa-installer has run) after `cargo build --features efa`, and emits a
# mini-rootfs under $DEST that Dockerfile.runtime COPYs onto the AL2023-minimal
# base preserving absolute paths — so glibc's default loader path resolves
# everything with no extra installer run and minimal shell/package-manager
# surface in the final image.
#
# RPM/FHS layout only (Amazon Linux 2023): 64-bit libs live directly under
# /usr/lib64, no per-arch multiarch triplet subdirectory the way Debian's
# /usr/lib/<triplet>/ does — so unlike that layout, there is nothing to detect.
#
# Why not a plain `ldd` copy: libibverbs loads its actual device provider
# (libefa-rdmav*.so) via dlopen at ibv_get_device_list time, NOT as a linked
# dependency — so `ldd pacer-daemon` never lists it. Copy only the ldd set and
# the image starts fine but enumerates ZERO devices → the capability probe fails
# → the daemon silently runs gRPC-only forever. So the closure is three parts:
#   1. rdma-core/efadv libs linked by the binary and by the provider plugin
#      (ldd, transitively), minus what distroless/cc already ships (glibc,
#      libgcc) — those must NOT be shadowed by copies.
#   2. the EFA provider plugin dir (the dlopen target).
#   3. /etc/libibverbs.d — tells libibverbs which provider .so to load.
#
# This staging is validated for PRESENCE only (see the asserts): CI has no EFA
# device, so it cannot prove enumeration works. Actual cross-node RDMA is proven
# by the A1 hardware harness (spike/efa a1-transport-test; planning/09/11).
set -eu

BIN=${1:?usage: stage-efa-libs.sh <daemon-binary> <dest-dir>}
DEST=${2:?usage: stage-efa-libs.sh <daemon-binary> <dest-dir>}

# The binary must exist: the explicit-soname + provider-dir staging below would
# otherwise happily produce a non-empty rootfs and exit 0 even though the whole
# point — the daemon and its linked closure — is missing.
[ -f "$BIN" ] || { echo "FAIL: binary not found: $BIN" >&2; exit 1; }

# glibc-family sonames amazonlinux:2023-minimal already provides — copying
# these would shadow the base image's own copies with build-image versions.
# Everything else an EFA lib pulls in (notably libnl) must be carried.
# Verified identical between amazonlinux:2023 (this builder) and
# amazonlinux:2023-minimal (the runtime base) — both ship glibc 2.34-231.
BASE_PROVIDED="libc.so libm.so libdl.so libpthread.so librt.so libresolv.so libgcc_s.so ld-linux"

provider_dir="/usr/lib64/libibverbs"

mkdir -p "$DEST"

# --- copy one absolute path into $DEST, normalizing /lib → /usr/lib ---
# glibc's usrmerge means ldd reports libs under /lib (or /lib64) even though
# they physically live under /usr/lib (or /usr/lib64). Staging to a literal
# /lib/... in the rootfs would (a) miss the /usr/lib presence asserts and (b)
# on the AL2023-minimal base, where /lib and /lib64 are SYMLINKS to usr/lib{,64},
# a COPY of a real /lib dir clobbers that symlink. Normalize every /lib and
# /lib64 path to /usr/lib{,64} so the rootfs only ever writes real directories
# glibc already searches.
norm_path() {
    case "$1" in
        /lib/*)   printf '/usr%s' "$1" ;;
        /lib64/*) printf '/usr%s' "$1" ;;
        *)        printf '%s' "$1" ;;
    esac
}

stage() {
    src=$1
    [ -e "$src" ] || return 1
    dst=$(norm_path "$src")
    mkdir -p "$DEST$(dirname "$dst")"
    cp -aL "$src" "$DEST$(dirname "$dst")/"
}

is_base_provided() {
    for base in $BASE_PROVIDED; do
        case "$1" in *"$base"*) return 0 ;; esac
    done
    return 1
}

# --- 1. linked closure of the binary + every provider plugin ---
# ldd resolves each to an absolute path; keep the non-glibc ones. Running ldd
# over the provider plugins too catches libs the provider needs (e.g. libnl)
# that the daemon itself does not link directly.
ldd_targets="$BIN"
[ -d "$provider_dir" ] && ldd_targets="$ldd_targets $(find "$provider_dir" -name '*.so*')"

for t in $ldd_targets; do
    # "libfoo => /path/libfoo.so (0x...)" lines; the resolved path is field 3.
    ldd "$t" | awk '/=>/ {print $3} /ld-linux/ {print $1}' | while read -r lib; do
        [ -n "$lib" ] && [ -e "$lib" ] || continue
        is_base_provided "$lib" && continue
        stage "$lib" || true
    done
done

# Explicitly stage the core EFA sonames regardless of ldd. `-lefa` links
# --as-needed, so if the daemon's efadv symbol references ever get optimized out
# libefa would vanish from `ldd` — but the dlopen'd provider still needs it at
# runtime. Glob every versioned soname so the SONAME symlink chain resolves.
for soname in libibverbs libefa; do
    for f in "/usr/lib64/$soname".so*; do
        [ -e "$f" ] && stage "$f" || true
    done
done

# Fail loud on any unresolved link — a bad installer bump renames/moves a lib.
if ldd "$BIN" | grep -q "not found"; then
    echo "FAIL: pacer-daemon has unresolved shared libraries:" >&2
    ldd "$BIN" | grep "not found" >&2
    exit 1
fi

# --- 2. the dlopen'd provider plugin dir ---
if [ -d "$provider_dir" ]; then
    mkdir -p "$DEST$provider_dir"
    cp -aL "$provider_dir"/. "$DEST$provider_dir/"
fi

# --- 3. libibverbs provider selection config ---
stage /etc/libibverbs.d || true
if [ -d /etc/libibverbs.d ]; then
    mkdir -p "$DEST/etc/libibverbs.d"
    cp -aL /etc/libibverbs.d/. "$DEST/etc/libibverbs.d/"
fi

# --- presence tripwires (drift guard against installer changes) ---
fail=0
ls "$DEST"/usr/lib64/libibverbs.so* >/dev/null 2>&1 || { echo "FAIL: libibverbs not staged" >&2; fail=1; }
ls "$DEST"/usr/lib64/libefa.so*     >/dev/null 2>&1 || { echo "FAIL: libefa not staged" >&2; fail=1; }
# The EFA provider plugin (dlopen target) — the silent-fallback guard.
ls "$DEST$provider_dir"/*efa*.so*   >/dev/null 2>&1 || { echo "FAIL: EFA provider plugin not staged (would enumerate 0 devices)" >&2; fail=1; }
ls "$DEST"/etc/libibverbs.d/*.driver >/dev/null 2>&1 || { echo "FAIL: no libibverbs .driver config staged" >&2; fail=1; }
[ "$fail" -eq 0 ] || exit 1

echo "stage-efa-libs: staged $(find "$DEST" -type f | wc -l) files into $DEST"
