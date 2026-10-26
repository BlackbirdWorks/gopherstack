#!/usr/bin/env bash
# gocache-trim.sh -- keep the Go build cache from filling the disk.
#
#   scripts/gocache-trim.sh                 trim unconditionally
#   scripts/gocache-trim.sh --if-low        trim only if GOCACHE's filesystem has < GOCACHE_MIN_FREE_GB free,
#                                           or the cache is over GOCACHE_MAX_GB (entries older than GOCACHE_AGED_MINUTES)
#
# Env: GOCACHE_TRIM_MINUTES (180) delete entries older than this,
#      GOCACHE_CLEAN_GB (200) if the cache exceeds this, `go clean -cache` instead,
#      GOCACHE_MIN_FREE_GB (60) free-space floor for --if-low,
#      GOCACHE_MAX_GB (150) / GOCACHE_AGED_MINUTES (720) size cap for --if-low.
# Two agents building in parallel can add >100GB between runs, so the floor is generous.
set -euo pipefail

cache=$(go env GOCACHE)
minutes="${GOCACHE_TRIM_MINUTES:-180}"
clean_gb="${GOCACHE_CLEAN_GB:-200}"
min_free_gb="${GOCACHE_MIN_FREE_GB:-60}"
max_gb="${GOCACHE_MAX_GB:-150}"
aged_minutes="${GOCACHE_AGED_MINUTES:-720}"

free_gb() {
  df -P -BG "$cache" 2>/dev/null | awk 'NR==2 {gsub("G","",$4); print $4+0}'
}

if [[ "${1:-}" == "--if-low" ]]; then
  [[ -d "$cache" ]] || exit 0
  free=$(free_gb)
  if [[ -n "$free" && "$free" -ge "$min_free_gb" ]]; then
    kb=$(du -sk "$cache" 2>/dev/null | awk '{print $1}')
    if [[ -z "$kb" || "$kb" -le $((max_gb * 1048576)) ]]; then
      exit 0
    fi
    echo "gocache-trim: $cache is over ${max_gb}GB -- deleting entries older than ${aged_minutes}m"
    find "$cache" -type f -mmin "+$aged_minutes" -delete
    exit 0
  fi
  echo "gocache-trim: only ${free:-?}GB free on GOCACHE filesystem (< ${min_free_gb}GB) -- trimming $cache"
fi

[[ -d "$cache" ]] || { echo "gocache-trim: $cache does not exist"; exit 0; }

size_kb() { du -sk "$cache" 2>/dev/null | awk '{print $1}'; }
human() { awk -v k="$1" 'BEGIN {printf "%.1fGB", k/1048576}'; }

before=$(size_kb)
echo "gocache-trim: $cache is $(human "$before") before"
if [[ "$before" -gt $((clean_gb * 1048576)) ]]; then
  echo "gocache-trim: over ${clean_gb}GB -- go clean -cache"
  go clean -cache
else
  find "$cache" -type f -mmin "+$minutes" -delete
fi
after=$(size_kb)
echo "gocache-trim: $(human "$after") after (freed $(human $((before - after))), $(free_gb)GB free)"
