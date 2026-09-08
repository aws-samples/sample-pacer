#!/usr/bin/env bash
#
# run-http-path-benchmark.sh — measure what the PACER cache is worth on the plain
# HTTP/S3 path, against the same objects read directly from regional S3.
#
# This is the reference implementation of the method described in
# http-path-vs-s3-standard.md. It is deliberately self-contained: it provisions no
# hardware, assumes no particular Kubernetes distribution, and hard-codes no bucket,
# account or cluster. You point it at a cluster that already runs PACER and a bucket
# that already holds a keyset, and it runs the arms and prints the comparison.
#
# WHAT IT MEASURES. Two (optionally three) arms, identical in every respect except the
# path the bytes travel:
#
#   cached   warp -> the node-local PACER daemon -> its cache        (bytes never leave the node)
#   bypass   warp -> https://s3.<region>.amazonaws.com -> the bucket (no daemon in the path)
#   remote   warp -> a peer node's daemon -> fabric -> the owning node's cache
#            (only when REQUESTER is set; this is the shape a real fleet has, because
#             nothing guarantees the node asking for a chunk is the node that owns it)
#
# Same client, same build, same node, same keys, same concurrency, same duration. The
# ratio of their throughputs is the cache's benefit on this path; the ratio of their
# time-to-first-byte is the part that does not move when you change concurrency.
#
# WHAT IT DOES NOT MEASURE. Cold first-touch (every arm here is warm by construction —
# the warm-up pass is explicit and the validity check requires zero backend
# read-throughs during measurement), ranged reads, mixed object sizes, or fan-in from
# many readers onto one owner. Those are separate questions; see the write-up.
#
# REQUIREMENTS
#   * kubectl, with a context that can create pods in $NAMESPACE.
#   * PACER already installed in $NAMESPACE as release $RELEASE, with its S3 proxy
#     listening on $S3_PORT and its metrics on $METRICS_PORT.
#   * A general-purpose (not directory/express) S3 bucket in $REGION holding
#     $PREFIX with equal-sized objects, reachable by the daemon.
#   * A ServiceAccount ($READONLY_SA) whose credentials can GET and LIST that bucket.
#     It should be READ-ONLY. warp's own --bucket help reads "ALL DATA WILL BE DELETED
#     IN BUCKET!" — --noclear and --list-existing below are what stop that, and
#     read-only credentials are what still stop it if either flag ever regresses.
#     Do not reuse a ServiceAccount whose role can delete objects in this bucket.
#   * A warp image at v1.2 or newer. Older warp has no --session-token flag at all, so
#     the temporary credentials any modern credential chain issues cannot sign against
#     real S3, and the failure surfaces as an auth error that reads like a slow result.
#
# USAGE
#   KUBE_CONTEXT=my-cluster NAMESPACE=pacer RELEASE=pacer \
#   REAL_BUCKET=amzn-s3-demo-bucket PREFIX=bench/ REGION=us-east-2 \
#   HOLDER=node-a REQUESTER=node-b READONLY_SA=bench-readonly \
#   CONCURRENCY=100 DURATION=180s REPS=3 \
#     ./run-http-path-benchmark.sh
#
# Everything is an environment variable with no hidden default that points at real
# infrastructure: the four that identify YOUR resources are required and the script
# refuses to guess them.
set -euo pipefail

# --- required inputs ----------------------------------------------------------

: "${REAL_BUCKET:?set REAL_BUCKET to a general-purpose S3 bucket holding the keyset}"
: "${HOLDER:?set HOLDER to the node whose daemon owns/serves the keyset}"
: "${READONLY_SA:?set READONLY_SA to a ServiceAccount with read-only GET/LIST on REAL_BUCKET}"

# --- optional inputs, with defaults that reference nothing real ---------------

NAMESPACE=${NAMESPACE:-pacer}
RELEASE=${RELEASE:-pacer}
REGION=${REGION:-us-east-2}
## Key prefix inside REAL_BUCKET. Must contain equal-sized objects and nothing else:
## warp --list-existing hammers everything it lists, so a stray large object skews the
## per-request cost and a stray small one skews it the other way.
PREFIX=${PREFIX:-bench/}
## Bucket alias the daemon maps to REAL_BUCKET. Clients address the alias, never the
## real bucket name, so the cached arm and the bypass arm name their target differently
## while reading identical keys.
BUCKET_ALIAS=${BUCKET_ALIAS:-cache}
## The daemon's S3 proxy port and Prometheus port.
S3_PORT=${S3_PORT:-9000}
METRICS_PORT=${METRICS_PORT:-9090}
## Second node. Unset -> the `remote` arm is skipped and only the all-local best case
## is measured, which is a weaker claim; see the write-up.
REQUESTER=${REQUESTER:-}
## Concurrent GETs per arm. 100 is a reasonable default: enough to fill a 100 Gbps
## class link on the cached path, and past the point where the direct-S3 path stops
## converting concurrency into throughput and starts converting it into queueing.
CONCURRENCY=${CONCURRENCY:-100}
## Run length per arm, and the client-side statistic is warp's per-second median, so
## this must be long enough for that median to be stable. 180s gives ~180 buckets.
DURATION=${DURATION:-180s}
## Repetitions of the whole interleaved set. One sample is not a measurement on
## shared/virtualised hardware; 3 gives a median and an observable spread.
REPS=${REPS:-3}
## warp >= 1.2 (see REQUIREMENTS). Pinned by tag so a silent upgrade cannot change the
## client mid-comparison.
WARP_IMAGE=${WARP_IMAGE:-minio/warp:v1.3.1}
## Any image with the AWS CLI v2.13+ on PATH (`aws configure export-credentials`).
AWSCLI_IMAGE=${AWSCLI_IMAGE:-amazon/aws-cli:2.15.17}
## Where result files land.
OUT_DIR=${OUT_DIR:-./results}
## Placeholder SigV4 identity for the CACHED arm. The daemon accepts a fixed dummy key
## on its proxy and does not treat it as a secret — real S3 auth happens on the daemon
## side, not the client's. Nothing here is a credential.
PROXY_KEY=${PROXY_KEY:-pacer}

KUBECTL=(kubectl)
[[ -n "${KUBE_CONTEXT:-}" ]] && KUBECTL=(kubectl --context "$KUBE_CONTEXT")
kc() { "${KUBECTL[@]}" -n "$NAMESPACE" "$@"; }

log()  { printf '== %s\n' "$*" >&2; }
die()  { printf 'FATAL: %s\n' "$*" >&2; exit 1; }

# --- preflight ----------------------------------------------------------------
# Every check here failed for real at least once during development, and each one is
# cheaper to hit now than after a measurement has been paid for.

preflight() {
  command -v kubectl >/dev/null || die "kubectl not on PATH"
  kc get ns >/dev/null 2>&1 || die "cannot reach namespace '$NAMESPACE' (check KUBE_CONTEXT)"
  kc get sa "$READONLY_SA" >/dev/null 2>&1 ||
    die "ServiceAccount '$READONLY_SA' not found in '$NAMESPACE' — the bypass arm needs it for credentials"
  "${KUBECTL[@]}" get node "$HOLDER" >/dev/null 2>&1 || die "node '$HOLDER' not found"
  [[ -z "$REQUESTER" ]] || "${KUBECTL[@]}" get node "$REQUESTER" >/dev/null 2>&1 ||
    die "node '$REQUESTER' not found"
  [[ "$REAL_BUCKET" != *--x-s3 ]] ||
    die "'$REAL_BUCKET' looks like an S3 directory (express) bucket: warp cannot authenticate to one, so the bypass arm is impossible against it. Use a general-purpose bucket."
  daemon_pod_on "$HOLDER" >/dev/null || die "no '$RELEASE' daemon pod on $HOLDER"
  mkdir -p "$OUT_DIR"
}

## The daemon pod on a given node. Selected by the chart's instance label so this does
## not depend on the DaemonSet's generated pod-name suffix.
daemon_pod_on() {
  local node=$1 pod
  pod=$(kc get pods -l "app.kubernetes.io/instance=$RELEASE" \
    --field-selector "spec.nodeName=$node" \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null)
  [[ -n "$pod" ]] || return 1
  printf '%s\n' "$pod"
}

daemon_ip_on() {
  local node=$1 pod
  pod=$(daemon_pod_on "$node") || return 1
  kc get pod "$pod" -o jsonpath='{.status.podIP}' 2>/dev/null
}

## One counter's value from a node's daemon. Returns 0 for an absent counter so a
## metric added in a later PACER version cannot abort a run on an older one.
counter_on() {
  local node=$1 name=$2 pod v
  pod=$(daemon_pod_on "$node") || { echo 0; return; }
  v=$(kc exec "$pod" -- sh -c \
      "wget -qO- http://127.0.0.1:$METRICS_PORT/metrics 2>/dev/null || curl -s http://127.0.0.1:$METRICS_PORT/metrics" \
      2>/dev/null | awk -v n="$name" '$1==n {print $2; exit}')
  printf '%s\n' "${v:-0}"
}

# --- pod rendering ------------------------------------------------------------
# Both arms run the SAME warp binary with the SAME flags; only --host, the credential
# source and the bucket name differ. That is the whole experimental design, so the two
# functions below are deliberately near-identical and should stay that way.
#
# Shared warp flags and why each is load-bearing:
#   --list-existing     read the pre-existing keyset; never PUT (so never mutate).
#   --noclear           never clear the bucket. Without it warp deletes bucket contents.
#   --disable-multipart whole-object GETs; ranged reads take a different path in the
#                       daemon and would not be the same comparison.
#   --concurrent        the only load knob, held equal across arms.

warp_pod_cached() {
  local pod=$1 node=$2 ip=$3
  cat <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  labels: { app: pacer-bench-warp, arm: cached }
spec:
  restartPolicy: Never
  nodeSelector: { kubernetes.io/hostname: $node }
  tolerations: [{ operator: Exists }]
  containers:
    - name: warp
      image: $WARP_IMAGE
      command: ["/bin/sh", "-c"]
      args:
        - |
          exec /warp get --host="$ip:$S3_PORT" --tls=false \\
            --access-key="$PROXY_KEY" --secret-key="$PROXY_KEY" \\
            --region="$REGION" --bucket="$BUCKET_ALIAS" --prefix="$PREFIX" \\
            --list-existing --disable-multipart --noclear \\
            --concurrent="$CONCURRENCY" --duration="$DURATION"
      resources:
        requests: { cpu: "8", memory: 8Gi }
        limits:   { memory: 24Gi }
YAML
}

warp_pod_bypass() {
  local pod=$1 node=$2
  # The credential split exists because warp takes static keys and has no AWS
  # credential-chain support. `aws configure export-credentials` resolves whatever
  # chain the pod has (Pod Identity, IRSA web identity, instance profile, env vars)
  # and emits the resolved TEMPORARY credentials, including the session token warp
  # cannot obtain for itself. Nothing long-lived is created; the volume is
  # memory-backed so the credentials never touch a disk.
  cat <<YAML
apiVersion: v1
kind: Pod
metadata:
  name: $pod
  labels: { app: pacer-bench-warp, arm: bypass }
spec:
  restartPolicy: Never
  serviceAccountName: $READONLY_SA
  nodeSelector: { kubernetes.io/hostname: $node }
  tolerations: [{ operator: Exists }]
  volumes:
    - name: creds
      emptyDir: { medium: Memory }
  initContainers:
    - name: creds
      image: $AWSCLI_IMAGE
      command: ["sh", "-c"]
      args:
        - |
          set -eu
          aws configure export-credentials --format env-no-export > /creds/env
          # Fail here, loudly, rather than letting warp start with an empty token: a
          # missing session token is the one failure mode that looks like a result.
          grep -q '^AWS_SESSION_TOKEN=' /creds/env || {
            echo "no session token in the resolved credentials" >&2; exit 1; }
      volumeMounts: [{ name: creds, mountPath: /creds }]
  containers:
    - name: warp
      image: $WARP_IMAGE
      command: ["/bin/sh", "-c"]
      args:
        - |
          set -eu
          . /creds/env
          exec /warp get --host="s3.$REGION.amazonaws.com" --tls \\
            --access-key="\$AWS_ACCESS_KEY_ID" --secret-key="\$AWS_SECRET_ACCESS_KEY" \\
            --session-token="\$AWS_SESSION_TOKEN" \\
            --region="$REGION" --bucket="$REAL_BUCKET" --prefix="$PREFIX" \\
            --lookup=host --list-existing --disable-multipart --noclear \\
            --concurrent="$CONCURRENCY" --duration="$DURATION"
      volumeMounts: [{ name: creds, mountPath: /creds }]
      resources:
        requests: { cpu: "8", memory: 8Gi }
        limits:   { memory: 24Gi }
YAML
}

# --- running one arm ----------------------------------------------------------

## Wait for a TERMINAL phase, never for readiness. `--for=condition=Ready=false` is
## satisfied while a pod is still in Init, so it returns immediately on the bypass arm
## and logs get captured before warp has emitted a line — reporting "nothing measured"
## over a run that was about to succeed.
wait_terminal() {
  local pod=$1 budget=$2 waited=0 phase
  while (( waited < budget )); do
    phase=$(kc get pod "$pod" -o jsonpath='{.status.phase}' 2>/dev/null || true)
    case "$phase" in
      Succeeded) return 0 ;;
      Failed)    kc logs "$pod" --tail=30 >&2 || true; die "arm pod $pod FAILED" ;;
    esac
    sleep 5; waited=$((waited + 5))
  done
  die "arm pod $pod did not finish within ${budget}s"
}

## warp's report lines. Verified against v1.3.1 output; a moved heading yields an empty
## field rather than an error, which is why the raw log is always kept beside the result.
warp_field() {
  local log=$1 what=$2
  case "$what" in
    median) awk '/Throughput, split into/{s=1} s && /50% Median:/{sub(/^[ \t]*/,"");print;exit}' "$log" ;;
    avg)    awk '/Report: GET/{s=1} s && /\* Average:/{sub(/^[ \t]*/,"");print;exit}' "$log" ;;
    reqs)   awk '/Report: GET/{s=1} s && /\* Reqs:/{sub(/^[ \t]*/,"");print;exit}' "$log" ;;
    ttfb)   awk '/Report: GET/{s=1} s && /\* TTFB:/{sub(/^[ \t]*/,"");print;exit}' "$log" ;;
  esac
}

## Median throughput in GiB/s, parsed from warp's per-second median line, which reports
## either MiB/s or GiB/s depending on magnitude. Normalising here is what makes the arms
## comparable as numbers rather than as strings.
median_gibps() {
  local line=$1
  awk '{
    for (i = 1; i <= NF; i++) {
      if ($i ~ /MiB\/s,?$/) { gsub(/[^0-9.]/, "", $i); printf "%.3f", $i / 1024; exit }
      if ($i ~ /GiB\/s,?$/) { gsub(/[^0-9.]/, "", $i); printf "%.3f", $i;        exit }
    }
  }' <<<"$line"
}

## One arm: render, run, collect, and assert the path really was the path claimed.
## $1 arm name (cached|bypass|remote), $2 label, $3 node warp runs on
run_arm() {
  local arm=$1 label=$2 node=$3
  local pod="pacerbench-$arm-${label//[^a-z0-9-]/}" stamp="$arm-$label"
  local logf="$OUT_DIR/$stamp.warp.log" resf="$OUT_DIR/$stamp.txt"

  # Baselines on BOTH nodes: the assertions below are about what did and did not
  # happen anywhere in the ring, not just on the node under load.
  local c0 p0 r0
  c0=$(counter_on "$HOLDER" pacer_bytes_from_cache_total)
  p0=$(counter_on "$HOLDER" pacer_peer_serves_total)
  r0=$(counter_on "$HOLDER" pacer_peer_readthroughs_total)

  kc delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  case "$arm" in
    cached|remote) warp_pod_cached "$pod" "$node" "$(daemon_ip_on "$node")" | kc apply -f - >/dev/null ;;
    bypass)        warp_pod_bypass "$pod" "$node"                            | kc apply -f - >/dev/null ;;
  esac
  log "$stamp: warp on $node (c=$CONCURRENCY, $DURATION)"
  wait_terminal "$pod" "$(( ${DURATION%s} + 300 ))"
  kc logs "$pod" >"$logf" 2>/dev/null || true
  [[ -s "$logf" ]] || die "no warp log for $pod — nothing was measured"

  local c1 p1 r1
  c1=$(counter_on "$HOLDER" pacer_bytes_from_cache_total)
  p1=$(counter_on "$HOLDER" pacer_peer_serves_total)
  r1=$(counter_on "$HOLDER" pacer_peer_readthroughs_total)
  local dcache=$(( c1 - c0 )) dpeer=$(( p1 - p0 )) drthru=$(( r1 - r0 ))

  local med gib
  med=$(warp_field "$logf" median); gib=$(median_gibps "$med")
  {
    echo "=== $stamp ==="
    echo "arm:            $arm (warp on $node, c=$CONCURRENCY, $DURATION)"
    echo "throughput:     $gib GiB/s   [warp per-second median, ramp+drain excluded]"
    echo "warp median:    ${med:-<unparsed; see the log>}"
    echo "warp average:   $(warp_field "$logf" avg)"
    echo "req latency:    $(warp_field "$logf" reqs)"
    echo "TTFB:           $(warp_field "$logf" ttfb)"
    echo "daemon cache bytes delta:  $dcache"
    echo "daemon peer serves delta:  $dpeer"
    echo "daemon read-throughs delta: $drthru"
  } | tee "$resf"

  # Validity. These are what separate a measurement from a number.
  case "$arm" in
    cached)
      (( dcache > 0 )) || die "$stamp: the daemon served nothing from cache — this was not the cached path"
      (( drthru == 0 )) || die "$stamp: $drthru backend read-throughs — part of this came from S3, so it is not a warm cache measurement"
      ;;
    bypass)
      (( dcache == 0 && dpeer == 0 )) ||
        die "$stamp: daemon counters moved (cache $dcache / peer $dpeer) — this was NOT a clean bypass"
      ;;
    remote)
      (( dpeer > 0 )) || die "$stamp: 0 peer serves — the keyset homed locally, so this is not the peer-plane arm"
      (( drthru == 0 )) || die "$stamp: $drthru backend read-throughs — not a warm cache measurement"
      ;;
  esac
  printf '%s\n' "$gib" >>"$OUT_DIR/$arm.samples"
}

# --- warm-up ------------------------------------------------------------------

## One full pass through the holder's daemon, so the measured arms are warm and the
## `read-throughs == 0` assertion can hold. Made explicit rather than left to the first
## arm: an arm that warms itself measures a blend of fill and serve, and reports it as
## serve.
warm_cache() {
  local pod=pacerbench-warm ip
  ip=$(daemon_ip_on "$HOLDER")
  kc delete pod "$pod" --ignore-not-found >/dev/null 2>&1 || true
  log "warming: one full pass over $PREFIX through the daemon on $HOLDER"
  cat <<YAML | kc apply -f - >/dev/null
apiVersion: v1
kind: Pod
metadata: { name: $pod, labels: { app: pacer-bench-warm } }
spec:
  restartPolicy: Never
  nodeSelector: { kubernetes.io/hostname: $HOLDER }
  tolerations: [{ operator: Exists }]
  containers:
    - name: warm
      image: $AWSCLI_IMAGE
      command: ["sh", "-c"]
      args:
        - |
          set -eu
          export AWS_ACCESS_KEY_ID=$PROXY_KEY AWS_SECRET_ACCESS_KEY=$PROXY_KEY
          export AWS_DEFAULT_REGION=$REGION
          aws --endpoint-url http://$ip:$S3_PORT s3 cp \\
            "s3://$BUCKET_ALIAS/$PREFIX" /dev/null --recursive >/dev/null
          echo WARM_OK
      resources:
        requests: { cpu: "4", memory: 4Gi }
YAML
  wait_terminal "$pod" 3600
  kc logs "$pod" 2>/dev/null | grep -q WARM_OK || die "warm-up pass did not complete"
}

# --- report -------------------------------------------------------------------

## Median and range of the samples for one arm. Median (not mean) because a single
## descheduled or throttled run should not drag the headline, and the RANGE is printed
## because it is the honest precision of the result: quoting more digits than the
## spread supports is how a benchmark stops being one.
summarise() {
  # Two 'local's, not one: a single 'local arm=$1 f=...$arm...' evaluates every RHS
  # against the values before this statement ran, so f would see arm's PRE-CALL value
  # (global/unset) rather than the $1 just assigned (verified: SC2318).
  local arm=$1
  local f="$OUT_DIR/$arm.samples"
  [[ -s "$f" ]] || { printf '%s\tn/a\tn/a\tn/a\t0\n' "$arm"; return; }
  sort -g "$f" | awk -v arm="$arm" '
    {v[NR]=$1}
    END {
      n=NR; med = (n%2) ? v[(n+1)/2] : (v[n/2]+v[n/2+1])/2
      printf "%s\t%.3f\t%.3f\t%.3f\t%d\n", arm, med, v[1], v[n], n
    }'
}

report() {
  local cached_med remote_med bypass_med
  echo
  echo "================ RESULTS ================"
  printf 'arm\tmedian\tmin\tmax\tn\n'
  summarise cached
  [[ -n "$REQUESTER" ]] && summarise remote
  summarise bypass
  echo
  bypass_med=$(summarise bypass | cut -f2)
  cached_med=$(summarise cached | cut -f2)
  echo "Ratios vs direct S3 (median of medians, GiB/s):"
  awk -v c="$cached_med" -v b="$bypass_med" \
    'BEGIN{ if (b>0) printf "  cached (all-local) : %.2fx  (%s vs %s)\n", c/b, c, b }'
  if [[ -n "$REQUESTER" ]]; then
    remote_med=$(summarise remote | cut -f2)
    awk -v r="$remote_med" -v b="$bypass_med" \
      'BEGIN{ if (b>0) printf "  remote (peer plane): %.2fx  (%s vs %s)   <-- the number to quote\n", r/b, r, b }'
    awk -v r="$remote_med" -v c="$cached_med" \
      'BEGIN{ if (c>0) printf "  cost of the peer plane: %.1f%%\n", (1 - r/c) * 100 }'
  else
    echo "  NOTE: REQUESTER was not set, so only the cache's BEST case was measured."
    echo "        Quote the peer-plane number instead; see http-path-vs-s3-standard.md."
  fi
  echo "Per-arm result files and raw warp logs: $OUT_DIR"
  echo "========================================="
}

# --- main ---------------------------------------------------------------------

preflight
rm -f "$OUT_DIR"/*.samples
warm_cache
for rep in $(seq 1 "$REPS"); do
  log "repetition $rep/$REPS"
  # Interleaved, not grouped: anything that drifts over the session (S3 front-end
  # warmth, a noisy neighbour, thermal state) becomes a systematic offset BETWEEN arms
  # if each arm runs its repetitions back to back, and that is indistinguishable from
  # the effect under measurement. Interleaving turns drift into within-arm variance,
  # where it is visible.
  run_arm cached "r$rep" "$HOLDER"
  [[ -n "$REQUESTER" ]] && run_arm remote "r$rep" "$REQUESTER"
  run_arm bypass "r$rep" "$HOLDER"
done
report
