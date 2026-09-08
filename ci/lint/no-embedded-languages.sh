#!/usr/bin/env bash
# ci/lint/no-embedded-languages.sh — ratchet on embedded programs this harness has
# accumulated: a heredoc'd manifest piped into `kubectl apply|create -f -`, a Python
# one-liner inside shell, an `envsubst` template call, an oversized YAML block scalar
# under bench/ or deploy/, and an oversized inline awk/sed -E/jq program. None of these
# is a compile error — each is a program written where nothing checks its syntax,
# tests it, or greps it for the next contributor, so the fix for "found one" is never
# "delete the pattern" (the harness needs kubectl, sometimes needs Python, always
# needs envsubst until H1 lands) but "move it to a file the image/repo already ships
# and versions". This job cannot demand that repo-wide today — the count is too large
# — so it RATCHETS: a baseline records today's occurrence COUNT per (file, kind), and
# the job fails only when a file's count for a kind goes UP relative to that.
#
# Keyed by (file, kind) -> count, deliberately NOT (file, line, kind): a baseline
# keyed by line number turns red on ANY edit to a baselined file that merely SHIFTS
# a later line — a comment added above it, an unrelated function inserted earlier —
# even though the ratcheted pattern itself never changed. Counting occurrences per
# file instead means only a genuine new (or removed) occurrence moves the number.
#
#   ci/lint/no-embedded-languages.sh              # scan, compare to the baseline
#   ci/lint/no-embedded-languages.sh --update      # rewrite the baseline to match now
#
# Kinds (see ci/lint/no-embedded-languages-test.sh for the decision table each one is
# tested against):
#   heredoc-kubectl-apply   a `<<` heredoc feeding `kubectl … (apply|create) … -f -`
#   python-inline            `python`/`python3 -c …` or `python3 -` inside shell
#   envsubst-call             an `envsubst` invocation inside shell
#   block-scalar              a YAML `|`/`|-`/`|+`/`>`/`>-`/`>+` block under bench/ or
#                             deploy/ whose body exceeds MAX_BLOCK_SCALAR_LINES
#   inline-program            an `awk`/`sed -E`/`jq` program longer than
#                             MAX_INLINE_PROGRAM_CHARS
set -euo pipefail
# The CALLER's repo root, not this script's own — `git ls-files` below has to walk
# whatever tree is under test, and no-embedded-languages-test.sh's fixtures are a
# throwaway repo of their own, never this one.
cd "$(git rev-parse --show-toplevel)"

readonly BASELINE_FILE="ci/lint/embedded-languages-baseline.txt"

# A Pod command needing more than this many lines of YAML block-scalar body is a
# program, not a command — and belongs in a file the image ships, versioned and
# testable on its own, not folded into a chart/manifest as an anonymous string.
readonly MAX_BLOCK_SCALAR_LINES=8

# Anything past this is "wrote a program in a shell argument" rather than "used a
# one-liner" — 200 chars is roughly a `BEGIN{...} {...} END{...}` awk skeleton with
# one real clause in each, which is where these have kept crossing over in practice.
readonly MAX_INLINE_PROGRAM_CHARS=200

# ---- shell scope: the same file list ci/lint/shellcheck.sh builds -----------------
mapfile -t SHELL_FILES < <(git ls-files '*.sh')
extless=()
while IFS= read -r f; do
  case "$(basename "$f")" in
    *.*) continue ;;
  esac
  if head -1 "$f" 2>/dev/null | grep -qE '^#!.*/(env +)?(bash|sh)$'; then
    extless+=("$f")
  fi
done < <(git ls-files scripts bench ci .githooks)
SHELL_FILES+=("${extless[@]}")

# ---- yaml scope: *.yaml/*.yml under bench/ and deploy/ (rule (d) is scoped there) --
mapfile -t YAML_FILES < <(git ls-files 'bench/**/*.yaml' 'bench/**/*.yml' \
  'deploy/**/*.yaml' 'deploy/**/*.yml')

# (a) A `<<` heredoc on the same logical line as a `kubectl … (apply|create) … -f -`.
# Two-step rather than one regex: the heredoc opener and the apply call are the same
# physical line in every real case here, but grep's `<<` search first keeps the
# pattern readable instead of one unreadable alternation.
find_heredoc_kubectl_apply() {
  local file=$1 lineno rest
  while IFS=: read -r lineno rest; do
    if [[ "$rest" =~ kubectl.*(apply|create).*-f[[:space:]]*- ]]; then
      printf '%s:%s:heredoc-kubectl-apply\n' "$file" "$lineno"
    fi
  done < <(grep -n '<<' "$file" 2>/dev/null || true)
}

# (b) `python`/`python3` invoked with `-c` (inline code) or a bare `-` (read a script
# from stdin, almost always via a heredoc — see c2-token.sh for the real case this
# repo carries today).
find_python_inline() {
  local file=$1 lineno
  while IFS=: read -r lineno _; do
    printf '%s:%s:python-inline\n' "$file" "$lineno"
  done < <(grep -nE '(^|[^A-Za-z0-9_./-])python3?[[:space:]]+(-c\b|-[[:space:]]*(<<|$))' \
    "$file" 2>/dev/null || true)
}

# (c) `envsubst` invoked as a command — not the word appearing in a comment/doc
# string, and not a fixture path like "$FIX/bin/envsubst" (the reup test's stub).
find_envsubst_call() {
  local file=$1 lineno rest trimmed
  while IFS=: read -r lineno rest; do
    trimmed="${rest#"${rest%%[! ]*}"}"
    [[ "$trimmed" == \#* ]] && continue
    [[ "$rest" == *"/envsubst"* ]] && continue   # a path to the binary, not a call
    printf '%s:%s:envsubst-call\n' "$file" "$lineno"
  done < <(grep -nE '(^|[|&;[:space:]])envsubst([[:space:]]|$)' "$file" 2>/dev/null || true)
}

# (d) A YAML block scalar under bench/ or deploy/ whose body runs past
# MAX_BLOCK_SCALAR_LINES. Plain bash rather than awk/sed: this file's own rule (e)
# below would otherwise have to exempt itself for writing the checker rule (d) needs.
find_oversized_block_scalars() {
  local file=$1 lineno=0 line stripped indent
  local in_block=0 block_start=0 block_indent=-1 body_lines=0
  while IFS= read -r line || [[ -n "$line" ]]; do
    lineno=$((lineno + 1))
    if (( in_block )); then
      stripped="${line#"${line%%[! ]*}"}"
      indent=$(( ${#line} - ${#stripped} ))
      if [[ -n "$stripped" ]] && (( indent <= block_indent )); then
        if (( body_lines > MAX_BLOCK_SCALAR_LINES )); then
          printf '%s:%s:block-scalar\n' "$file" "$block_start"
        fi
        in_block=0
      else
        body_lines=$((body_lines + 1))
        continue
      fi
    fi
    if [[ "$line" =~ :[[:space:]]*[\|\>][+-]?[0-9]*[[:space:]]*(\#.*)?$ ]]; then
      stripped="${line#"${line%%[! ]*}"}"
      block_indent=$(( ${#line} - ${#stripped} ))
      block_start=$lineno
      body_lines=0
      in_block=1
    fi
  done <"$file"
  if (( in_block )) && (( body_lines > MAX_BLOCK_SCALAR_LINES )); then
    printf '%s:%s:block-scalar\n' "$file" "$block_start"
  fi
}

# (e) An `awk …'…'`, `sed -E …'…'` or `jq …'…'` program longer than
# MAX_INLINE_PROGRAM_CHARS. A program whose closing quote is not on the same line as
# its opener is flagged outright: a real multi-line awk/sed/jq body is, in every
# instance in this repo, already well past the threshold, and re-accumulating length
# across lines buys precision nothing here checks.
find_oversized_inline_programs() {
  local file=$1 lineno rest prog trimmed
  while IFS=: read -r lineno rest; do
    # A comment mentioning "awk's"/"jq's" reads to the regex below as the command
    # word followed immediately by an opening quote (the possessive apostrophe) —
    # excluded here rather than tightened further, since prose about these tools is
    # exactly where that word is most likely to appear right before a quote mark.
    trimmed="${rest#"${rest%%[! ]*}"}"
    [[ "$trimmed" == \#* ]] && continue
    case "$rest" in
      *\'*)
        prog="${rest#*\'}"
        if [[ "$prog" == *"'"* ]]; then
          prog="${prog%\'*}"
          if (( ${#prog} > MAX_INLINE_PROGRAM_CHARS )); then
            printf '%s:%s:inline-program\n' "$file" "$lineno"
          fi
        else
          printf '%s:%s:inline-program\n' "$file" "$lineno"
        fi
        ;;
    esac
  done < <(grep -nE "(^|[^A-Za-z0-9_])(awk|jq)\\b[[:space:]][^'\"]*'|(^|[^A-Za-z0-9_])sed\\b[[:space:]][^'\"]*-E[^'\"]*'" \
    "$file" 2>/dev/null || true)
}

# ---- scan --------------------------------------------------------------------------
findings_file=$(mktemp "${TMPDIR:-/tmp}/no-embedded-languages.XXXXXX")
trap 'rm -f "$findings_file"' EXIT

for f in "${SHELL_FILES[@]}"; do
  find_heredoc_kubectl_apply "$f"
  find_python_inline "$f"
  find_envsubst_call "$f"
  find_oversized_inline_programs "$f"
done >"$findings_file"

for f in "${YAML_FILES[@]}"; do
  find_oversized_block_scalars "$f"
done >>"$findings_file"

sort -t: -k1,1 -k2,2n -k3,3 -o "$findings_file" "$findings_file"

# Aggregate the per-occurrence findings to a per-(file, kind) COUNT — the unit both
# the stored baseline and the gate below compare in. Plain bash associative arrays,
# not awk/sort -u: this is the checker's own bookkeeping, not a shell one-liner, and
# the file:line detail survives in $findings_file for the "which lines" report below.
declare -A current_counts_by_key current_counts_by_kind
while IFS=: read -r f _ k; do
  [[ -z "${f:-}" ]] && continue
  key="$f"$'\t'"$k"
  current_counts_by_key["$key"]=$(( ${current_counts_by_key["$key"]:-0} + 1 ))
  current_counts_by_kind["$k"]=$(( ${current_counts_by_kind["$k"]:-0} + 1 ))
done <"$findings_file"

# ---- --update: rewrite the baseline to match the current tree ----------------------
if [[ "${1:-}" == "--update" ]]; then
  mkdir -p "$(dirname "$BASELINE_FILE")"
  {
    echo "# file<TAB>kind<TAB>count — generated by ci/lint/no-embedded-languages.sh --update."
    echo "# Keyed by (file, kind), not (file, line, kind): an edit to a baselined file that"
    echo "# shifts line numbers without changing how many times a pattern occurs there must"
    echo "# not turn this red. Shrinks as occurrences are moved to a real file or fixed;"
    echo "# grows only with a reviewed reason, never silently."
    echo "#"
    echo "# THE EXCEPTIONS — entries that are NOT backlog and will never shrink to zero. They"
    echo "# are written from this script rather than by hand because --update regenerates this"
    echo "# whole header, so a note added to the file would be lost on the next run."
    echo "#"
    echo "#   ci/lint/no-embedded-languages{,-test}.sh — the checker's OWN doc comments naming"
    echo "#     the patterns it looks for, and its test's fixture strings building a literal"
    echo "#     example of each one. A pattern-matcher cannot tell 'this is a real occurrence'"
    echo "#     from 'this is the string a test wrote to look like one'; that is inherent to"
    echo "#     the approach. The count moves only when the test gains or loses fixture cases."
    echo "#"
    echo "#   bench/ladder/bench-chart-equivalence-test.sh — the FROZEN renders of the 21"
    echo "#     envsubst manifests deploy/helm/pacer-bench replaced (H1). Two of those pods"
    echo "#     really did carry a python one-liner and a 'python3 -' heredoc, so the frozen"
    echo "#     copy of what they rendered contains that text by definition. H2 moved the live"
    echo "#     copies into clients/python/pacer_nvme_report.py and bench/lib/*.sh; this is the"
    echo "#     historical record they are diffed against, and editing it to please this"
    echo "#     checker would destroy the proof."
    for key in "${!current_counts_by_key[@]}"; do
      printf '%s\t%d\n' "$key" "${current_counts_by_key[$key]}"
    done | sort
  } >"$BASELINE_FILE"
  echo "wrote ${#current_counts_by_key[@]} (file, kind) entries to $BASELINE_FILE"
  exit 0
fi

# ---- compare to baseline -------------------------------------------------------------
declare -A baseline_counts_by_key baseline_counts_by_kind
while IFS=$'\t' read -r f k c; do
  [[ -z "${f:-}" || "$f" == \#* ]] && continue
  baseline_counts_by_key["$f"$'\t'"$k"]=$c
  baseline_counts_by_kind["$k"]=$(( ${baseline_counts_by_kind["$k"]:-0} + c ))
done < <(grep -v '^[[:space:]]*$' "$BASELINE_FILE" 2>/dev/null || true)

echo "no-embedded-languages: current vs. baselined, per kind"
for kind in heredoc-kubectl-apply python-inline envsubst-call block-scalar inline-program; do
  printf '  %-22s current %3d   baseline %3d\n' "$kind" \
    "${current_counts_by_kind[$kind]:-0}" "${baseline_counts_by_kind[$kind]:-0}"
done

# Every key either side has an opinion about, so a file that dropped to zero
# occurrences (removed from $current_counts_by_key entirely) is still visited.
all_keys=$(
  { printf '%s\n' "${!current_counts_by_key[@]}" "${!baseline_counts_by_key[@]}"; } \
    2>/dev/null | sort -u
)

exceeded=() shrunk=0
while IFS= read -r key; do
  [[ -z "$key" ]] && continue
  cur=${current_counts_by_key[$key]:-0}
  base=${baseline_counts_by_key[$key]:-0}
  if (( cur > base )); then
    exceeded+=("$key")
  elif (( cur < base )); then
    shrunk=$((shrunk + 1))
  fi
done <<<"$all_keys"

if [[ ${#exceeded[@]} -gt 0 ]]; then
  echo
  echo "EXCEEDED baseline (fix them, or re-run --update after review to accept the count):"
  for key in "${exceeded[@]}"; do
    f=${key%%$'\t'*} k=${key#*$'\t'}
    printf '  %s  %s: current %d, baseline %d\n' "$f" "$k" \
      "${current_counts_by_key[$key]:-0}" "${baseline_counts_by_key[$key]:-0}"
    grep ":$k\$" "$findings_file" | grep -F "$f:" || true
  done
  exit 1
fi

if (( shrunk > 0 )); then
  echo
  echo "$shrunk (file, kind) count(s) shrank — run 'ci/lint/no-embedded-languages.sh --update'" \
    "to tighten the baseline so the ratchet holds at the new, lower count."
fi

echo "no new occurrences — the ratchet holds"
