#!/usr/bin/env bash
# lint-modimpact.sh BASE_REV -- print repo package dirs (one per line) that
# directly import (non-test, test or external-test) any module whose go.mod
# requirement or replace changed between BASE_REV and the working tree.
#
# Direct importers only: an API break in a bumped module surfaces where the
# module's packages are called; a package that merely depends on it through
# another repo package is covered when that package is linted. Transitive
# closure would re-select the whole repo for any x/sys bump.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
base="${1:?usage: lint-modimpact.sh BASE_REV}"

command -v jq >/dev/null 2>&1 || {
  echo "lint-modimpact: jq not found on PATH" >&2
  exit 1
}

git diff --quiet "$base" -- go.mod 2>/dev/null && exit 0

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

git show "$base:go.mod" >"$tmp/old.mod" 2>/dev/null || : >"$tmp/old.mod"

mods() {
  go mod edit -json "$1" | jq -r '
    ((.Require // [])[] | "r \(.Path) \(.Version)"),
    ((.Replace // [])[] | "p \(.Old.Path) \(.Old.Version // "") \(.New.Path) \(.New.Version // "")")'
}

mods "$tmp/old.mod" | LC_ALL=C sort -u >"$tmp/old.txt"
mods go.mod | LC_ALL=C sort -u >"$tmp/new.txt"

# Modules added, removed or re-versioned (either side of the diff).
{ LC_ALL=C comm -3 "$tmp/old.txt" "$tmp/new.txt"; } |
  sed -E 's/^[[:space:]]+//' | awk '{print $2}' | sort -u >"$tmp/changed.txt"

[[ -s "$tmp/changed.txt" ]] || exit 0

go list -e -f '{{.Dir}}|{{join .Imports " "}} {{join .TestImports " "}} {{join .XTestImports " "}}' ./... |
  awk -F'|' -v root="$PWD/" -v mods="$tmp/changed.txt" '
    BEGIN { while ((getline m < mods) > 0) mod[m] = 1 }
    {
      n = split($2, imp, " ")
      for (i = 1; i <= n; i++) {
        p = imp[i]
        while (p != "") {
          if (p in mod) { d = $1; if (index(d, root) == 1) d = substr(d, length(root) + 1); else d = "."; print d; next }
          if (!sub("/[^/]*$", "", p)) break
        }
      }
    }' | sort -u
