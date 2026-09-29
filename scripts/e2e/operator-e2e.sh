#!/usr/bin/env bash
#
# operator-e2e.sh — end-to-end test of pacer-operator (ADR-0047) on a real cluster.
#
# Installs the operator chart, creates one CacheRing, and asserts every behaviour the
# ADR claims, each against the live API server:
#
#   1. converge   the ring becomes Ready with the expected members, and every object
#                 it applied is owned by the ring and labelled with it
#   2. refuse     an invalid value is reported as Rendered=False/ValuesRefused, and the
#                 last good daemons keep running
#   3. roll       a value that changes the pod template rolls the DaemonSet and the ring
#                 converges on the new generation
#   4. prune      turning a template off (networkPolicy.enabled) deletes its object;
#                 turning it back on recreates it
#   5. repair     an owned object deleted by hand is recreated on the next reconcile
#   6. members    deleting a daemon pod shows up in status.members well inside the
#                 periodic resync, i.e. through the EndpointSlice watch
#   7. collect    deleting the ring garbage-collects everything it applied
#
# and, across all seven, that the operator container never restarted.
#
# It needs a cluster with at least E2E_EXPECT_MEMBERS schedulable nodes matching the
# ring's values, and the operator image already pullable. It knows nothing about any
# particular cluster: node selection, image and capacity all come from the values files.
#
# Usage:
#   E2E_NAMESPACE=cache \
#   E2E_OPERATOR_IMAGE=ghcr.io/aws-samples/sample-pacer-operator:v0.1.0 \
#   E2E_RING_VALUES=ring-values.yaml \
#   E2E_EXPECT_MEMBERS=2 \
#     scripts/e2e/operator-e2e.sh
#
# Environment:
#   E2E_NAMESPACE         namespace for the operator and the ring (must exist)       [required]
#   E2E_OPERATOR_IMAGE    operator image, repository:tag                             [required]
#   E2E_RING_VALUES       pacer chart values for the ring (a YAML mapping)           [required]
#   E2E_EXPECT_MEMBERS    nodes the ring must converge on                            [required]
#   E2E_OPERATOR_VALUES   extra values file for the operator chart (e.g. a nodeSelector)
#   E2E_RING              CacheRing name                            (default pacer-e2e)
#   E2E_OPERATOR_RELEASE  operator helm release name                (default pacer-operator-e2e)
#   E2E_TIMEOUT           seconds any one wait may take             (default 600)
#   E2E_KEEP=1            leave everything installed at the end (and on failure)
#   E2E_DELETE_CRD=1      also delete the CacheRing CRD at teardown, if no ring is left
#   KUBECTL, HELM         client binaries (default kubectl, helm); the caller pins the context

set -euo pipefail

: "${E2E_NAMESPACE:?set E2E_NAMESPACE}"
: "${E2E_OPERATOR_IMAGE:?set E2E_OPERATOR_IMAGE (repository:tag)}"
: "${E2E_RING_VALUES:?set E2E_RING_VALUES (a pacer chart values file)}"
: "${E2E_EXPECT_MEMBERS:?set E2E_EXPECT_MEMBERS}"
RING=${E2E_RING:-pacer-e2e}
RELEASE=${E2E_OPERATOR_RELEASE:-pacer-operator-e2e}
TIMEOUT=${E2E_TIMEOUT:-600}
KUBECTL=${KUBECTL:-kubectl}
HELM=${HELM:-helm}
NS=$E2E_NAMESPACE

REPO_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
CHART="$REPO_ROOT/deploy/helm/pacer-operator"

# How long a membership change may take to reach status for step 6 to count it as
# event-driven: well under the reconciler's 300 s RESYNC (crates/pacer-operator/src/
# reconcile.rs), and above a daemon's own drain (20 s) plus a watch round-trip.
MEMBERSHIP_WINDOW=90
# How long a change that needs NO rollout — a refused value withdrawn, a template turned
# off or on, a deleted object — may take to show up. Above the reconciler's 30 s RETRY
# (its bound on staleness while a ring is not Ready) and far below the 300 s RESYNC: a
# first cluster run took 361 s here, and this bound is what makes that a failure.
EVENT_WINDOW=90
# Poll interval for every wait below. Short: the waits are bounded by TIMEOUT, not by it.
POLL=2

k() { "$KUBECTL" -n "$NS" "$@"; }
log() { printf '\n==> %s\n' "$*"; }
ok() { printf '  ok: %s\n' "$*"; }
fail() { printf '  FAIL: %s\n' "$*" >&2; exit 1; }

# Poll until "$@" succeeds, or fail after TIMEOUT seconds naming <what>.
# Args: <what> <timeout> <command...>
await() {
  local what=$1 timeout=$2 start=$SECONDS
  shift 2
  until "$@" >/dev/null 2>&1; do
    (( SECONDS - start < timeout )) || fail "timed out after ${timeout}s waiting for: $what"
    sleep "$POLL"
  done
  ok "$what ($(( SECONDS - start ))s)"
}

ring_field() { k get cachering "$RING" -o "jsonpath=$1"; }
condition() { ring_field "{.status.conditions[?(@.type==\"$1\")].$2}"; }

# True when the Ready condition says Converged for the ring's CURRENT generation — a
# stale Converged from before the last spec change does not count.
converged() {
  [[ "$(condition Ready reason)" == Converged ]] &&
    [[ "$(condition Ready observedGeneration)" == "$(ring_field '{.metadata.generation}')" ]]
}

# Every inventory entry, as `kind/name` lines.
inventory() {
  ring_field '{range .status.inventory[*]}{.kind}/{.name}{"\n"}{end}'
}

inventory_name() { inventory | awk -F/ -v kind="$1" '$1 == kind { print $2; exit }'; }

dump() {
  echo "---- diagnostics ----" >&2
  k get cachering "$RING" -o yaml >&2 2>&1 || true
  k get ds,pods -l "app.kubernetes.io/instance=$RING" -o wide >&2 2>&1 || true
  k logs "deploy/$RELEASE" --tail=80 >&2 2>&1 || true
}

teardown() {
  local status=$?
  (( status == 0 )) || dump
  if [[ "${E2E_KEEP:-}" == 1 ]]; then
    echo "E2E_KEEP=1: leaving ring $RING and release $RELEASE in $NS" >&2
    return
  fi
  log "teardown"
  k delete cachering "$RING" --ignore-not-found --wait=true --timeout=180s || true
  "$HELM" uninstall "$RELEASE" -n "$NS" --wait --timeout 180s 2>/dev/null || true
  if [[ "${E2E_DELETE_CRD:-}" == 1 ]]; then
    if [[ -z "$("$KUBECTL" get cacherings -A -o name 2>/dev/null)" ]]; then
      "$KUBECTL" delete crd cacherings.pacer.io --ignore-not-found || true
    else
      echo "CacheRings remain elsewhere; CRD left in place" >&2
    fi
  fi
}

ring_manifest() {
  cat <<EOF
apiVersion: pacer.io/v1alpha1
kind: CacheRing
metadata:
  name: $RING
  namespace: $NS
spec:
  values:
EOF
  sed 's/^/    /' "$E2E_RING_VALUES"
}

step_install() {
  log "install the operator ($E2E_OPERATOR_IMAGE)"
  k get cachering "$RING" >/dev/null 2>&1 && fail "CacheRing $RING already exists in $NS; refusing to reuse it"
  local extra=()
  [[ -n "${E2E_OPERATOR_VALUES:-}" ]] && extra=(-f "$E2E_OPERATOR_VALUES")
  "$HELM" upgrade --install "$RELEASE" "$CHART" -n "$NS" \
    --set watchNamespace="$NS" \
    --set image.repository="${E2E_OPERATOR_IMAGE%:*}" \
    --set image.tag="${E2E_OPERATOR_IMAGE##*:}" \
    ${extra[@]+"${extra[@]}"} --wait --timeout "${TIMEOUT}s"
  ok "operator Deployment available"
}

# The operator container's restart count. `helm --wait` above is NOT evidence it runs:
# the Deployment has no readiness probe, so it reports Available the moment the process
# starts — the first live run passed that wait with an operator that panicked on its
# first TLS config and crash-looped from then on.
operator_restarts() {
  k get pods -l "app.kubernetes.io/instance=$RELEASE" \
    -o jsonpath='{range .items[*]}{.status.containerStatuses[0].restartCount}{"\n"}{end}' |
    awk '{ s += $1 } END { print s + 0 }'
}

step_operator_steady() {
  log "the operator ran the whole test without restarting"
  local restarts
  restarts=$(operator_restarts)
  [[ "$restarts" == 0 ]] || fail "operator container restarted $restarts time(s)"
  ok "0 restarts"
}

step_converge() {
  log "1. converge"
  # SERVER-side apply, deliberately: a `key: null` in the ring values (how a values file
  # removes a chart default, e.g. its `pacer.io/nodepool` selector) survives create and
  # server-side apply, but client-side `kubectl apply` reads null as "delete this field"
  # and never sends it — found on the first EKS run, where the ring kept the chart's
  # default selector and matched no node. docs/helm/operator.md warns users of the same.
  ring_manifest | k apply --server-side --field-manager=pacer-e2e -f -
  await "ring Converged" "$TIMEOUT" converged
  local members desired
  members=$(ring_field '{.status.members}')
  desired=$(ring_field '{.status.desired}')
  [[ "$members" == "$E2E_EXPECT_MEMBERS" ]] || fail "members=$members, expected $E2E_EXPECT_MEMBERS"
  [[ "$desired" == "$E2E_EXPECT_MEMBERS" ]] || fail "desired=$desired, expected $E2E_EXPECT_MEMBERS"
  ok "members=$members desired=$desired nodes=$(ring_field '{.status.memberNodes}')"

  local uid entry kind name owner ring_label manager
  uid=$(ring_field '{.metadata.uid}')
  while IFS=/ read -r kind name; do
    [[ -n "$kind" ]] || continue
    entry=$(k get "$kind" "$name" -o jsonpath='{.metadata.ownerReferences[0].uid}|{.metadata.labels.pacer\.io/ring}|{.metadata.labels.app\.kubernetes\.io/managed-by}') ||
      fail "inventory names $kind/$name but it does not exist"
    IFS='|' read -r owner ring_label manager <<<"$entry"
    [[ "$owner" == "$uid" ]] || fail "$kind/$name is not owned by the ring"
    [[ "$ring_label" == "$RING" ]] || fail "$kind/$name lacks pacer.io/ring=$RING"
    [[ "$manager" == pacer-operator ]] || fail "$kind/$name managed-by=$manager"
  done < <(inventory)
  ok "all $(inventory | grep -c .) inventory objects exist, are owned by the ring and labelled"
}

step_refuse() {
  log "2. refuse an invalid value, keep the last good daemons"
  local ready_before
  ready_before=$(ring_field '{.status.ready}')
  k patch cachering "$RING" --type merge -p '{"spec":{"values":{"e2eNoSuchKey":1}}}'
  refused() {
    [[ "$(condition Rendered reason)" == ValuesRefused ]] &&
      [[ "$(condition Rendered observedGeneration)" == "$(ring_field '{.metadata.generation}')" ]]
  }
  await "Rendered=False/ValuesRefused" "$TIMEOUT" refused
  condition Rendered message | grep -q e2eNoSuchKey || fail "refusal message does not name the key: $(condition Rendered message)"
  ok "message names the key"
  local ds ready_now
  ds=$(inventory_name DaemonSet)
  ready_now=$(k get ds "$ds" -o jsonpath='{.status.numberReady}')
  [[ "$ready_now" == "$ready_before" ]] || fail "daemons went from $ready_before to $ready_now ready on a refused render"
  ok "DaemonSet $ds still has $ready_now ready daemons"
  k patch cachering "$RING" --type json -p '[{"op":"remove","path":"/spec/values/e2eNoSuchKey"}]'
  await "ring Converged again after removing the value" "$EVENT_WINDOW" converged
}

step_roll() {
  log "3. roll the DaemonSet on a pod-template change"
  local ds gen_before level
  ds=$(inventory_name DaemonSet)
  gen_before=$(k get ds "$ds" -o jsonpath='{.metadata.generation}')
  # config.* feeds the ConfigMap, whose checksum is a pod-template annotation.
  level=$(ring_field '{.spec.values.config.logLevel}')
  [[ "$level" == debug ]] && level=info || level=debug
  k patch cachering "$RING" --type merge -p "{\"spec\":{\"values\":{\"config\":{\"logLevel\":\"$level\"}}}}"
  rolled() { (( $(k get ds "$ds" -o jsonpath='{.metadata.generation}') > gen_before )); }
  await "DaemonSet $ds generation advanced past $gen_before" "$TIMEOUT" rolled
  await "ring Converged on the new template" "$TIMEOUT" converged
  local updated desired
  updated=$(ring_field '{.status.updated}')
  desired=$(ring_field '{.status.desired}')
  [[ "$updated" == "$desired" ]] || fail "updated=$updated desired=$desired after the roll"
  ok "updated=$updated/$desired"
}

step_prune() {
  log "4. prune an object the render stops producing"
  local np
  np=$(inventory_name NetworkPolicy)
  [[ -n "$np" ]] || fail "no NetworkPolicy in the inventory to prune (is networkPolicy.enabled off in the ring values?)"
  k patch cachering "$RING" --type merge -p '{"spec":{"values":{"networkPolicy":{"enabled":false}}}}'
  gone() { ! k get networkpolicy "$np" >/dev/null 2>&1 && [[ -z "$(inventory_name NetworkPolicy)" ]]; }
  await "NetworkPolicy $np deleted and dropped from the inventory" "$EVENT_WINDOW" gone
  k patch cachering "$RING" --type merge -p '{"spec":{"values":{"networkPolicy":{"enabled":true}}}}'
  back() { k get networkpolicy "$np" >/dev/null 2>&1; }
  await "NetworkPolicy $np recreated" "$EVENT_WINDOW" back
  await "ring Converged" "$EVENT_WINDOW" converged
}

step_repair() {
  log "5. repair drift"
  local pdb
  pdb=$(inventory_name PodDisruptionBudget)
  [[ -n "$pdb" ]] || fail "no PodDisruptionBudget in the inventory"
  k delete pdb "$pdb" --wait=true
  # Any change to the ring is an event the controller reconciles on; an annotation
  # changes no generation, so it asks for a pass without changing what is desired.
  k annotate cachering "$RING" --overwrite "e2e.pacer.io/nudge=$SECONDS" >/dev/null
  present() { k get pdb "$pdb" >/dev/null 2>&1; }
  await "PodDisruptionBudget $pdb recreated" "$EVENT_WINDOW" present
}

step_members() {
  log "6. membership reaches status through the EndpointSlice watch"
  local pod start
  # The operator labels the objects it applies, not the pods their controllers create;
  # a daemon pod carries the chart's selector labels, whose instance is the ring's name.
  pod=$(k get pods -l "app.kubernetes.io/instance=$RING" -o jsonpath='{.items[0].metadata.name}')
  [[ -n "$pod" ]] || fail "no daemon pod found for the ring"
  start=$SECONDS
  k delete pod "$pod" --wait=false
  dropped() { (( $(ring_field '{.status.members}') < E2E_EXPECT_MEMBERS )); }
  await "status.members dropped below $E2E_EXPECT_MEMBERS" "$MEMBERSHIP_WINDOW" dropped
  ok "seen $(( SECONDS - start ))s after deleting $pod (resync would be 300s)"
  await "ring Converged with $E2E_EXPECT_MEMBERS members again" "$TIMEOUT" converged
}

step_collect() {
  log "7. deleting the ring collects everything it applied"
  local objects
  objects=$(inventory)
  k delete cachering "$RING" --wait=true --timeout="${TIMEOUT}s"
  all_gone() {
    local kind name
    while IFS=/ read -r kind name; do
      [[ -n "$kind" ]] || continue
      k get "$kind" "$name" >/dev/null 2>&1 && return 1
    done <<<"$objects"
    return 0
  }
  await "all $(grep -c . <<<"$objects") objects garbage-collected" "$TIMEOUT" all_gone
}

trap teardown EXIT
step_install
step_converge
step_refuse
step_roll
step_prune
step_repair
step_members
step_collect
step_operator_steady
log "operator e2e PASSED"
