#!/usr/bin/env bash
# gocache-trim.sh must never wipe the cache; it only deletes entries older than the thresholds.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)" || exit 1
c=$(mktemp -d); trap 'rm -rf "$c"' EXIT
seed() { rm -rf "${c:?}"/*; mkdir -p "$c/ab"; echo x >"$c/ab/new"; echo x >"$c/ab/old"; touch -d '2 days ago' "$c/ab/old"; echo x >"$c/ab/mid"; touch -d '5 hours ago' "$c/ab/mid"; }
run() { GOCACHE="$c" GOCACHE_CLEAN_GB=0 "$@" 2>&1; }

seed; out=$(run env GOCACHE_MAX_GB=0 GOCACHE_MIN_FREE_GB=0 bash scripts/gocache-trim.sh --if-low); rc=$?
[[ $rc -eq 0 && ! -e $c/ab/old && -e $c/ab/new && -e $c/ab/mid ]] || { echo "FAIL: over-max aged trim: $out"; exit 1; }

seed; out=$(run env GOCACHE_MIN_FREE_GB=0 bash scripts/gocache-trim.sh --if-low)
[[ -e $c/ab/old && -e $c/ab/new ]] || { echo "FAIL: trimmed while under thresholds"; exit 1; }

seed; out=$(run env GOCACHE_MIN_FREE_GB=999999 bash scripts/gocache-trim.sh --if-low)
[[ ! -e $c/ab/old && ! -e $c/ab/mid && -e $c/ab/new && -d $c ]] || { echo "FAIL: low-free trim: $out"; exit 1; }
grep -q WARNING <<<"$out" || { echo "FAIL: no warning when still low"; exit 1; }

seed; out=$(run bash scripts/gocache-trim.sh)
[[ ! -e $c/ab/old && ! -e $c/ab/mid && -e $c/ab/new ]] || { echo "FAIL: unconditional trim: $out"; exit 1; }
grep -q 'clean -cache' <<<"$out" && { echo "FAIL: full wipe path"; exit 1; }
echo PASS
