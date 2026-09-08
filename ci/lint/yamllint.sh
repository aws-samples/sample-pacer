#!/usr/bin/env bash
# ci/lint/yamllint.sh — yamllint every tracked YAML file, from one list.
#
#   ci/lint/yamllint.sh
#
# Scope and rule choices live in the repo-root .yamllint (its own header explains the
# two `ignore:` trees — Helm's Go-template chart templates and this harness's envsubst
# `k8s/` manifests are both templates, not YAML, and neither parses on its own).
# `--strict` promotes yamllint's own warnings to failures: the rules in .yamllint were
# picked to matter (truthy, line-length, comment spacing), so a warning here is meant
# to gate the job the same way an error does — same contract as
# ci/lint/shellcheck.sh's `--severity=warning`.
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

readonly BASELINE_FILE="ci/lint/yamllint-baseline.txt"

mapfile -t all_files < <(git ls-files '*.yaml' '*.yml')

baseline=()
if [[ -f "$BASELINE_FILE" ]]; then
  while IFS= read -r line; do
    [[ -z "$line" || "$line" == \#* ]] && continue   # blank or a comment-only line
    baseline+=("$line")
  done <"$BASELINE_FILE"
fi

in_baseline() {
  local f=$1 b
  for b in ${baseline[@]+"${baseline[@]}"}; do
    [[ "$f" == "$b" ]] && return 0
  done
  return 1
}

gating=() baselined=()
for f in "${all_files[@]}"; do
  if in_baseline "$f"; then baselined+=("$f"); else gating+=("$f"); fi
done

echo "yamllint: ${#all_files[@]} tracked YAML file(s) (.yamllint's own 'ignore:'" \
  "drops the Go/envsubst templates among them), ${#baselined[@]} baselined" \
  "(must shrink to 0 — see $BASELINE_FILE), ${#gating[@]} gating this job"

status=0
if [[ ${#gating[@]} -gt 0 ]]; then
  yamllint -c .yamllint --strict "${gating[@]}" || status=$?
fi

if [[ ${#baselined[@]} -gt 0 ]]; then
  echo
  echo "--- baselined files: findings shown for visibility, NOT gating ---"
  yamllint -c .yamllint --strict "${baselined[@]}" || true
fi

exit "$status"
