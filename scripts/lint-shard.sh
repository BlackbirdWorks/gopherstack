#!/usr/bin/env bash
# Usage: lint-shard.sh list <index 0..total-1> <total> | check <total>
# Deterministic LOC-balanced (LPT) split of the root module's packages for CI lint.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

tags=$(sed -n '/^  build-tags:/,/^[^ ]/{s/^    - //p}' .golangci.yml | paste -sd, -)

weighted() {
  local root dir n
  root=$(pwd)
  go list -e -tags "$tags" -f '{{.Dir}}' ./... | sort | while IFS= read -r dir; do
    n=$(cat "$dir"/*.go 2>/dev/null | wc -l)
    dir=${dir#"$root"}
    echo "$n ./${dir#/}"
  done | sed 's#^\([0-9]*\) \./$#\1 .#'
}

assign() {
  local total="$1"
  weighted | LC_ALL=C sort -k1,1nr -k2,2 | awk -v t="$total" '
    BEGIN { for (i = 0; i < t; i++) load[i] = 0 }
    {
      best = 0
      for (i = 1; i < t; i++) if (load[i] < load[best]) best = i
      load[best] += $1
      print best, $1, $2
    }'
}

cmd="${1:-}"
case "$cmd" in
  list)
    [[ $# -eq 3 ]] || { echo "usage: $0 list <index 0..total-1> <total>" >&2; exit 2; }
    idx="$2" total="$3"
    (( idx >= 0 && idx < total )) || { echo "lint-shard: index $idx out of range 0..$((total - 1))" >&2; exit 2; }
    assign "$total" | awk -v i="$idx" '$1 == i { print $3 }' | LC_ALL=C sort
    ;;
  check)
    [[ $# -eq 2 ]] || { echo "usage: $0 check <total>" >&2; exit 2; }
    total="$2"
    out=$(assign "$total")
    awk '{ n[$1]++; w[$1] += $2 } END { for (i in n) printf "lint-shard: shard %d: %d packages, %d LOC\n", i, n[i], w[i] }' <<<"$out" | sort -V
    expected=$(weighted | awk '{ print $2 }' | LC_ALL=C sort)
    actual=$(awk '{ print $3 }' <<<"$out" | LC_ALL=C sort)
    if [[ "$expected" != "$actual" ]]; then
      echo "lint-shard: union of shards differs from go list:" >&2
      diff <(echo "$expected") <(echo "$actual") >&2 || true
      exit 1
    fi
    echo "lint-shard: OK: $(wc -l <<<"$actual") packages across $total shards, no gaps or duplicates"
    ;;
  *)
    echo "usage: $0 list <index 0..total-1> <total> | check <total>" >&2
    exit 2
    ;;
esac
