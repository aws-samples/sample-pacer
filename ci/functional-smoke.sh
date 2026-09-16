#!/bin/sh
# Functional smoke test: boot a prebuilt pacer-daemon and exercise the admin
# endpoint the way kubelet probes do. Runs in CI against the release binary
# (ADR-0010) and locally against any build:
#
#     cargo build -p pacer-daemon && ci/functional-smoke.sh target/debug/pacer-daemon
#
# Phase 1 extends this stage with the S3 correctness suite (boto3/AWS CLI
# against PACER_LISTEN_ADDR, incl. read-after-write) — see planning/06-roadmap.md.
set -eu

BIN=${1:?usage: functional-smoke.sh <path-to-pacer-daemon>}
ADMIN=127.0.0.1:19090

PACER_CACHE_DIR=$(mktemp -d)
export PACER_CACHE_DIR
export PACER_MEM_CAPACITY=64MiB
export PACER_DISK_CAPACITY=256MiB
export PACER_ADMIN_ADDR=$ADMIN
export PACER_LISTEN_ADDR=127.0.0.1:19000

"$BIN" &
PID=$!
trap 'kill "$PID" 2>/dev/null || true; rm -rf "$PACER_CACHE_DIR"' EXIT

i=0
until curl -fsS "http://$ADMIN/healthz" >/dev/null 2>&1; do
    kill -0 "$PID" 2>/dev/null || { echo "FAIL: daemon exited during startup" >&2; exit 1; }
    i=$((i + 1))
    [ "$i" -le 30 ] || { echo "FAIL: /healthz not up after 30s" >&2; exit 1; }
    sleep 1
done

curl -fsS "http://$ADMIN/healthz" | grep -q ok
curl -fsS "http://$ADMIN/readyz" | grep -q ready
curl -fsS "http://$ADMIN/metrics" >/dev/null
code=$(curl -s -o /dev/null -w '%{http_code}' "http://$ADMIN/no-such-path")
[ "$code" = 404 ] || { echo "FAIL: expected 404 for unknown path, got $code" >&2; exit 1; }

echo "functional-smoke: OK"
