#!/usr/bin/env bash
# ci/lint/no-embedded-languages-test.sh — decision table for the ratchet, with no
# dependency on this repo's own tree: every case below builds a throwaway git repo
# under a temp dir, populates it with fixture files, and runs the REAL checker
# (ci/lint/no-embedded-languages.sh, unmodified) against that fixture repo instead.
#
#   ci/lint/no-embedded-languages-test.sh
#
# Why fixtures rather than asserting against this repo's own baseline: this repo's
# tree changes (that is the whole point of a ratchet), so a test pinned to today's
# per-file counts would break on the next unrelated edit. What has to hold instead
# is the DECISION each detector makes — heredoc-fed vs. file-fed `-f -`, `-c` vs. a
# bare `--version`, a call vs. a path vs. a comment, over-threshold vs. at it, in
# scope vs. out of it — and that the ratchet's own unit is a (file, kind) COUNT, not
# a (file, line, kind) triple: a line shift in a baselined file must not fail the
# job, and only a count that goes up beyond its baseline (or above an implicit 0 for
# a file the baseline has never named) may. The checker resolves its own repo root
# via `git rev-parse --show-toplevel` (not `dirname "$0"`) for exactly this reason:
# run from inside a fixture repo, it scans THAT repo, never this one.
set -euo pipefail
cd "$(dirname "$0")/../.."   # this repo's root, just to locate the script under test
readonly CHECKER="$PWD/ci/lint/no-embedded-languages.sh"

FIX=$(mktemp -d "${TMPDIR:-/tmp}/no-embedded-languages-test.XXXXXX")
trap 'rm -rf "$FIX"' EXIT

pass=0 fail=0
OUT="" RC=0
ok()  { echo "ok   $1"; pass=$((pass + 1)); }
bad() { echo "FAIL $1 — $2" >&2; printf '%s\n' "$OUT" | sed 's/^/       /' >&2; fail=$((fail + 1)); }

# Args: <name> <want-exit> <expect-substring>...
check() {
  local name=$1 want=$2 s
  shift 2
  if [[ "$RC" != "$want" ]]; then bad "$name" "exit $RC, wanted $want"; return; fi
  for s in "$@"; do
    [[ "$OUT" == *"$s"* ]] || { bad "$name" "output lacks '$s'"; return; }
  done
  ok "$name"
}
check_absent() {
  if [[ "$OUT" != *"$2"* ]]; then ok "$1"; else bad "$1" "output contains '$2'"; fi
}

# Fresh fixture repo per case, so one case's file cannot leak into another's scan.
# `git ls-files` is what the checker walks, so files must be ADDED, not just written.
new_repo() {
  rm -rf "$FIX/repo"
  mkdir -p "$FIX/repo"
  git -C "$FIX/repo" init -q
  git -C "$FIX/repo" config user.email test@example.com
  git -C "$FIX/repo" config user.name test
}
add() {
  local path=$1
  shift
  mkdir -p "$FIX/repo/$(dirname "$path")"
  printf '%s\n' "$@" >"$FIX/repo/$path"
  git -C "$FIX/repo" add "$path"
}
run_checker() {
  RC=0
  OUT=$(cd "$FIX/repo" && bash "$CHECKER" 2>&1) || RC=$?
}

echo "== (a) heredoc-fed kubectl apply -f -, vs. a file-fed one =="

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  'kubectl apply -f - <<EOF' \
  'kind: Pod' \
  'EOF'
run_checker
check "a heredoc into 'kubectl apply -f -' is a NEW finding (no baseline yet)" 1 \
  "heredoc-kubectl-apply" "script.sh:2:heredoc-kubectl-apply"

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  'kubectl apply -f "manifest.yaml"' \
  'kubectl apply -f - < "$RENDERED"'
run_checker
check "kubectl apply -f <file> and -f - <file (no heredoc) trip nothing" 0 \
  "no new occurrences"

echo "== (b) python3 -c is caught, python3 --version is not =="

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "python3 -c 'print(1)'"
run_checker
check "python3 -c is a NEW finding" 1 "python-inline" "script.sh:2:python-inline"

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  'python3 --version' \
  'python3 script.py'
run_checker
check "python3 --version / python3 <file>.py trip nothing" 0 "no new occurrences"

echo "== (c) an envsubst CALL is caught; a comment or a path to the binary is not =="

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "envsubst '\${FOO}' < in.yaml | kubectl apply -f -"
run_checker
check "an envsubst call is a NEW finding" 1 "envsubst-call" "script.sh:2:envsubst-call"

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  '# envsubst is stubbed for this test' \
  'chmod +x "$FIX/bin/envsubst"'
run_checker
check "a comment mentioning envsubst, and a path to the envsubst binary, trip nothing" 0 \
  "no new occurrences"

echo "== (d) an oversized YAML block scalar under bench/, scoped and thresholded =="

new_repo
big_body=()
for _ in $(seq 1 9); do big_body+=("    line of program text"); done
add bench/ladder/k8s/big.yaml "spec:" "  command: |" "${big_body[@]}"
run_checker
check "a 9-line block scalar body under bench/ (over the 8-line default) is a NEW finding" 1 \
  "block-scalar" "bench/ladder/k8s/big.yaml:2:block-scalar"

new_repo
small_body=()
for _ in $(seq 1 8); do small_body+=("    line of program text"); done
add bench/ladder/k8s/small.yaml "spec:" "  command: |" "${small_body[@]}"
run_checker
check "an 8-line body (AT the default, not over it) trips nothing" 0 "no new occurrences"

new_repo
add docs/notes.yaml "spec:" "  command: |" "${big_body[@]}"
run_checker
check "the SAME 9-line body outside bench/ or deploy/ is out of scope" 0 "no new occurrences"

echo "== (e) an oversized inline awk/sed -E/jq program, vs. a short one and prose =="

new_repo
# Built rather than hand-written, so its length is provably > MAX_INLINE_PROGRAM_CHARS
# (200) instead of eyeballed prose that quietly lands under the threshold it exists to
# test — the exact mistake this comment replaces (a 147-char first draft passed the
# "over 200 chars" case by asserting nothing, since it truly was not over 200 chars).
clause="{ n += 1; s = s \"x\"; } "
long_prog=""
for _ in $(seq 1 12); do long_prog+="$clause"; done
add script.sh \
  '#!/usr/bin/env bash' \
  "echo x | awk '$long_prog'"
run_checker
check "an awk program over 200 chars is a NEW finding" 1 "inline-program" "script.sh:2:inline-program"

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "echo x | awk '{print \$1}'" \
  "echo y | jq -r '.name'" \
  "echo z | sed -E 's/a/b/'"
run_checker
check "short awk/jq/sed -E one-liners trip nothing" 0 "no new occurrences"

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "# awk's split() and jq's select() both read this comment, not a program"
run_checker
check "prose mentioning \"awk's\"/\"jq's\" (possessive apostrophe, not an opening quote) trips nothing" 0 \
  "no new occurrences"

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "echo x | awk '" \
  '  {print $1}' \
  "'"
run_checker
check "a multi-line awk program (no closing quote on the opening line) is flagged outright" 1 \
  "inline-program" "script.sh:2:inline-program"

echo "== the ratchet itself: baselined counts pass, exceeded counts fail, --update tracks reality =="

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "python3 -c 'print(1)'"
RC=0
OUT=$(cd "$FIX/repo" && bash "$CHECKER" --update 2>&1) || RC=$?
check "--update writes a baseline that captures the one existing (file, kind) entry" 0 \
  "wrote 1 (file, kind) entries"
if ! grep -qxF "$(printf 'script.sh\tpython-inline\t1')" "$FIX/repo/ci/lint/embedded-languages-baseline.txt" 2>/dev/null; then
  bad "the written baseline names file, kind and count, tab-separated" \
    "not found in $FIX/repo/ci/lint/embedded-languages-baseline.txt"
else
  ok "the written baseline names file, kind and count, tab-separated"
fi
run_checker
check "a SECOND run against the SAME tree, now baselined, passes" 0 "no new occurrences"

echo "== the whole point of counting rather than lining: a line shift alone changes nothing =="

# Same ONE python-inline call, but pushed down several lines by unrelated content
# above it. A file:line:kind baseline would go red here for an edit that touched
# none of the ratcheted patterns; file<TAB>kind<TAB>count must not.
add script.sh \
  '#!/usr/bin/env bash' \
  '# a comment that was not here before' \
  '# and another one' \
  'echo "unrelated setup work"' \
  "python3 -c 'print(1)'"
run_checker
check "inserting unrelated lines ABOVE the same single occurrence still passes" 0 \
  "no new occurrences"

echo "== a genuinely new occurrence in the same file: baseline exceeded =="

add script.sh \
  '#!/usr/bin/env bash' \
  '# a comment that was not here before' \
  '# and another one' \
  'echo "unrelated setup work"' \
  "python3 -c 'print(1)'" \
  "python3 -c 'print(2)'"
run_checker
check "a SECOND python-inline call in the same file exceeds the baselined count of 1" 1 \
  "python-inline          current   2   baseline   1" \
  "EXCEEDED baseline" "script.sh  python-inline: current 2, baseline 1"

echo "== a brand new file with no baseline entry at all: also exceeded, from zero =="

new_repo
add other.sh \
  '#!/usr/bin/env bash' \
  "python3 -c 'print(1)'"
run_checker
check "a file the baseline has never seen fails from an implicit baseline of 0" 1 \
  "EXCEEDED baseline" "other.sh  python-inline: current 1, baseline 0"

echo "== fixing an occurrence: the count may shrink, and shrinking still passes =="

new_repo
add script.sh \
  '#!/usr/bin/env bash' \
  "python3 -c 'print(1)'" \
  "python3 -c 'print(2)'"
RC=0
OUT=$(cd "$FIX/repo" && bash "$CHECKER" --update 2>&1) || RC=$?
check "--update baselines the file at its current count of 2" 0 "wrote 1 (file, kind) entries"

add script.sh \
  '#!/usr/bin/env bash' \
  "python3 -c 'print(1)'"
run_checker
check "dropping back to ONE occurrence (below the baselined 2) still passes" 0 \
  "no new occurrences" "run 'ci/lint/no-embedded-languages.sh --update'"

RC=0
OUT=$(cd "$FIX/repo" && bash "$CHECKER" --update 2>&1) || RC=$?
check "re-running --update tightens the baseline to the new, lower count" 0 \
  "wrote 1 (file, kind) entries"
run_checker
check "...and a run against that tightened baseline no longer hints to shrink further" 0 \
  "no new occurrences"
check_absent "...specifically, no more shrink hint" "run 'ci/lint/no-embedded-languages.sh --update'"

echo
if (( fail == 0 )); then
  echo "✓ no-embedded-languages OK — $pass checks: each detector fires on its real shape and stays" \
    "silent on its look-alike, the block-scalar and inline-program rules respect their thresholds" \
    "and scope, a baselined (file, kind) COUNT survives an unrelated line shift in that file," \
    "and the ratchet fails only when a count truly goes up — from its baseline, or from zero" \
    "for a file never seen before — while a count that shrinks passes with a hint to retighten."
else
  echo "✗ no-embedded-languages FAILED — $fail of $((pass + fail)) checks" >&2
  exit 1
fi
