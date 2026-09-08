#!/usr/bin/env bash
# ci/lint/docs-budget.sh — the documentation budget, as a check (quality item G6).
#
#   ci/lint/docs-budget.sh
#
# TWO assertions, both of which were prose rules until they drifted:
#
#   1. CLAUDE.md is at most 40 lines. It had reached 238 — the file every agent reads
#      FIRST, at a length nobody reads, narrating five subsystems that each already have
#      their own document. The measured cost: required reading for the dev loop was
#      2,492 lines across nine documents, three of which described the same mechanism
#      with different details, and the only rule agents reliably obeyed was the one a
#      hook enforced. A budget makes the file a screen again, and a screen is the only
#      length at which "read this first" is a real instruction.
#
#   2. scripts/dev/README.md still has the sections `dev help` sends readers to. That
#      list is read out of the DEV_README_SECTIONS array in scripts/dev/dev, never
#      repeated here: one list, so a renamed heading fails HERE rather than quietly
#      turning every pointer in the façade's help into a link that lands at the top of a
#      400-line file — the failure nobody reports because it looks like their own
#      mistake.
#
# Decision-table style: every failure prints the number it measured, so the fix is
# arithmetic rather than a hunt. No tools beyond bash and coreutils, because this runs
# in the plain `alpine:3` of the `shell-tests` CI job.
set -euo pipefail
cd "$(dirname "$0")/../.."   # repo root

## The budget, and why THIS number: 40 lines is about one terminal screen, which is the
## length at which an agent reads the whole file before acting instead of skimming the
## first rule and starting. It is a ceiling on the ENTRY POINT only — the narrative it
## used to hold moved to scripts/dev/README.md and scripts/dev/README-session.md, which
## have no budget, because a document someone opens on purpose may be as long as it
## needs to be.
readonly CLAUDE_MD=CLAUDE.md
readonly CLAUDE_MD_MAX_LINES=40

## The dev-loop guide, and the façade whose help points into it.
readonly DEV_GUIDE=scripts/dev/README.md
readonly DEV_FACADE=scripts/dev/dev

## The bash array in $DEV_FACADE that declares those section names. Parsed, not sourced:
## sourcing the façade would run its dispatch, and this check must not be able to invoke
## anything.
readonly SECTION_ARRAY=DEV_README_SECTIONS

failures=0

fail() { printf 'docs-budget: FAIL %s\n' "$*" >&2; failures=$(( failures + 1 )); }
ok()   { printf 'docs-budget: ok   %s\n' "$*"; }

# ---- 1. the entry point fits on a screen -------------------------------------

if [[ ! -f $CLAUDE_MD ]]; then
  fail "$CLAUDE_MD is missing"
else
  lines=$(wc -l <"$CLAUDE_MD" | tr -d ' ')
  if (( lines > CLAUDE_MD_MAX_LINES )); then
    fail "$CLAUDE_MD is $lines lines, budget is $CLAUDE_MD_MAX_LINES ($(( lines - CLAUDE_MD_MAX_LINES )) over) — move the narrative to $DEV_GUIDE, scripts/dev/README-session.md or planning/, and leave the rule plus the tool that enforces it"
  else
    ok "$CLAUDE_MD is $lines/$CLAUDE_MD_MAX_LINES lines"
  fi
fi

# ---- 2. the guide still has the sections the help points at ------------------

if [[ ! -f $DEV_FACADE ]]; then
  fail "$DEV_FACADE is missing, so the section list cannot be read"
elif [[ ! -f $DEV_GUIDE ]]; then
  fail "$DEV_GUIDE is missing"
else
  # One `readonly -a NAME=( … )` block, one double-quoted string per line. Anchored on
  # the array name so an unrelated array cannot be picked up by accident.
  sections=()
  while IFS= read -r name; do
    sections+=("$name")
  done < <(
    sed -n "/^readonly -a $SECTION_ARRAY=(/,/^)/p" "$DEV_FACADE" |
      sed -n 's/^[[:space:]]*"\(..*\)"[[:space:]]*$/\1/p'
  )

  if (( ${#sections[@]} == 0 )); then
    # An empty list would make every assertion below vacuous, so it is the loudest
    # failure here rather than a silent pass.
    fail "read 0 section names out of $DEV_FACADE's $SECTION_ARRAY — the array was renamed or reformatted, and this check would otherwise assert nothing"
  else
    missing=()
    for name in "${sections[@]}"; do
      grep -qxF "## $name" "$DEV_GUIDE" || missing+=("$name")
    done
    if (( ${#missing[@]} )); then
      fail "$DEV_GUIDE lacks ${#missing[@]} of ${#sections[@]} section(s) that 'dev help' points at: ${missing[*]} — rename them back, or rename them in $SECTION_ARRAY too"
    else
      ok "$DEV_GUIDE has all ${#sections[@]} sections 'dev help' points at"
    fi
  fi
fi

if (( failures )); then
  printf '\ndocs-budget: %d assertion(s) failed\n' "$failures" >&2
  exit 1
fi
printf '\ndocs-budget: both assertions hold\n'
