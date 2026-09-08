#!/usr/bin/env bash
# ci/lint/shellcheck.sh — shellcheck every shell script in the repo, from one list.
#
#   ci/lint/shellcheck.sh
#
# WHY a wrapper instead of a bare `shellcheck **/*.sh` in .gitlab-ci.yml: the scope is
# not just `*.sh`. Several harness entry points ship with NO extension on purpose (a
# git hook, a driver meant to be `source`d or exec'd bare — scripts/dev/pacer-dev,
# scripts/dev/build-pod, .githooks/pre-commit, …), and shellcheck has no way to guess
# that from a glob; it infers the dialect from the shebang, not the filename. Building
# that file list here means the CI job, a local run and the next contributor adding a
# script all see the same rule instead of three copies of it drifting apart.
#
# external-sources/source-path live in the repo-root .shellcheckrc, which shellcheck
# picks up on its own from the run's cwd — this script always runs from the repo root
# so that resolution is deterministic. Severity is NOT an .shellcheckrc key (shellcheck
# 0.11.0 silently ignores one there), so it is passed explicitly below.
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

readonly BASELINE_FILE="ci/lint/shellcheck-baseline.txt"
# error+warning gate; info/style (SC2016 in every single-quoted envsubst template,
# SC1091 "not following" a sourced file, …) stay visible but non-blocking — see
# .shellcheckrc's note.
readonly SEVERITY="warning"

# Every tracked *.sh file, repo-wide (fixed extension, unambiguous scope).
mapfile -t sh_files < <(git ls-files '*.sh')

# Every extension-less file under the four directories that ship shell without one,
# filtered to an actual bash/sh shebang so a stray non-shell file dropped in one of
# them (there is exactly one today: bench/incluster/Dockerfile) is not swept in.
extensionless=()
while IFS= read -r f; do
  case "$(basename "$f")" in
    *.*) continue ;;  # has an extension — the *.sh list above already covers it
  esac
  if head -1 "$f" 2>/dev/null | grep -qE '^#!.*/(env +)?(bash|sh)$'; then
    extensionless+=("$f")
  fi
done < <(git ls-files scripts bench ci .githooks)

all_files=("${sh_files[@]}" "${extensionless[@]}")

# The baseline: whole files this job does not gate on yet (see the file's own header
# for why, and the shrink-to-zero rule). Never a whole rule and never the whole repo —
# only ever specific paths, each with a reason recorded beside it.
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

echo "shellcheck: ${#all_files[@]} in-scope script(s), ${#baselined[@]} baselined" \
  "(must shrink to 0 — see $BASELINE_FILE), ${#gating[@]} gating this job"

status=0
if [[ ${#gating[@]} -gt 0 ]]; then
  shellcheck --severity="$SEVERITY" "${gating[@]}" || status=$?
fi

if [[ ${#baselined[@]} -gt 0 ]]; then
  echo
  echo "--- baselined files: findings shown for visibility, NOT gating ---"
  shellcheck --severity="$SEVERITY" "${baselined[@]}" || true
fi

exit "$status"
