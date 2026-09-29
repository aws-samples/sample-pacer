#!/usr/bin/env bash
#
# operator-kind.sh — run scripts/e2e/operator-e2e.sh (ADR-0047) on a throwaway kind cluster.
#
# What CI runs on every change to the operator, and what anyone with Docker and kind can
# run locally with the same result:
#
#   docker build -t pacer:e2e .
#   docker build -f Dockerfile.operator -t pacer-operator:e2e .
#   scripts/e2e/operator-kind.sh
#
# Creates the cluster in scripts/e2e/kind/kind.yaml (one control plane, two workers
# labelled for the chart's default nodeSelector), loads the two images into it, runs the
# e2e against a two-member ring, and deletes the cluster. The kubeconfig is a file of its
# own, so this never touches ~/.kube/config or the caller's current context.
#
# Environment:
#   DAEMON_IMAGE      daemon image, already built locally        (default pacer:e2e)
#   OPERATOR_IMAGE    operator image, already built locally      (default pacer-operator:e2e)
#   KIND_CLUSTER      kind cluster name                          (default pacer-operator-e2e)
#   KIND              kind binary                                (default kind)
#   KIND_KEEP=1       leave the cluster (and the e2e's objects) up afterwards; the
#                     kubeconfig path is printed
#
# The ring's values (scripts/e2e/kind/ring-values.yaml) name `pacer:e2e`; DAEMON_IMAGE
# other than that is re-tagged to it before loading.

set -euo pipefail

DAEMON_IMAGE=${DAEMON_IMAGE:-pacer:e2e}
OPERATOR_IMAGE=${OPERATOR_IMAGE:-pacer-operator:e2e}
CLUSTER=${KIND_CLUSTER:-pacer-operator-e2e}
KIND=${KIND:-kind}
HERE=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The image the ring values name. Kept in one place with the values file it must match.
RING_IMAGE=pacer:e2e
NAMESPACE=pacer-e2e
# kind nodes and two workers report Ready well inside this on a GitHub runner.
NODES_READY_TIMEOUT=300s

KUBECONFIG=$(mktemp -t pacer-kind-kubeconfig.XXXXXX)
export KUBECONFIG

cleanup() {
  local status=$?
  if [[ "${KIND_KEEP:-}" == 1 ]]; then
    echo "KIND_KEEP=1: cluster $CLUSTER left up; export KUBECONFIG=$KUBECONFIG" >&2
    return
  fi
  "$KIND" delete cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" >/dev/null 2>&1 || true
  rm -f "$KUBECONFIG"
  return "$status"
}
trap cleanup EXIT

echo "==> kind cluster $CLUSTER"
"$KIND" create cluster --name "$CLUSTER" --config "$HERE/kind/kind.yaml" --kubeconfig "$KUBECONFIG" --wait "$NODES_READY_TIMEOUT"

if [[ "$DAEMON_IMAGE" != "$RING_IMAGE" ]]; then
  docker tag "$DAEMON_IMAGE" "$RING_IMAGE"
fi
echo "==> load $RING_IMAGE and $OPERATOR_IMAGE into the nodes"
"$KIND" load docker-image --name "$CLUSTER" "$RING_IMAGE" "$OPERATOR_IMAGE"

kubectl create namespace "$NAMESPACE"

E2E_NAMESPACE=$NAMESPACE \
E2E_OPERATOR_IMAGE=$OPERATOR_IMAGE \
E2E_OPERATOR_VALUES="$HERE/kind/operator-values.yaml" \
E2E_RING_VALUES="$HERE/kind/ring-values.yaml" \
E2E_EXPECT_MEMBERS=2 \
E2E_KEEP=${KIND_KEEP:-} \
  "$HERE/operator-e2e.sh"
