#!/usr/bin/env bash
# lint-shard.sh SHARD -- full-repo golangci-lint, split by package path so each
# CI shard finishes inside the timeout. Shard "rest" takes every package that no
# named shard matches, so new top-level dirs and services are never skipped.
#
# Usage: scripts/lint-shard.sh {svc-a|svc-b-c|...|rest}   (--list prints shard names)
# Env: LINT_TIMEOUT (55m), LINT_LIST_ONLY=1 prints the package list and exits.
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

declare -A shard_re=(
  [svc-a]='^services/a'
  [svc-b-c]='^services/[bc]'
  [svc-d-e]='^services/[de]'
  [svc-f-h]='^services/[f-h]'
  [svc-i-m]='^services/[i-m]'
  [svc-n-o]='^services/[no]'
  [svc-p-r]='^services/[p-r]'
  [svc-s]='^services/s'
  [svc-t-z]='^services/[t-z_]'
  [pkgs]='^pkgs/'
)
order=(svc-a svc-b-c svc-d-e svc-f-h svc-i-m svc-n-o svc-p-r svc-s svc-t-z pkgs rest)

if [[ "${1:-}" == "--list" ]]; then
  printf '%s\n' "${order[@]}"
  exit 0
fi
shard="${1:?usage: lint-shard.sh SHARD|--list}"

module=$(go list -m)
all_re=$(printf '%s|' "${shard_re[@]}")
all_re="(${all_re%|})"

mapfile -t pkgs < <(
  go list -f '{{.ImportPath}}' ./... | sed -e "s|^$module/\\{0,1\\}||" -e 's|^$|.|' |
    while IFS= read -r rel; do
      if [[ "$shard" == rest ]]; then
        [[ "$rel" =~ $all_re ]] || echo "$rel"
      else
        [[ -n "${shard_re[$shard]:-}" ]] || {
          echo "lint-shard: unknown shard $shard" >&2
          exit 2
        }
        [[ "$rel" =~ ${shard_re[$shard]} ]] && echo "$rel"
      fi
    done
)

if [[ ${#pkgs[@]} -eq 0 ]]; then
  echo "lint-shard: shard $shard matched no packages" >&2
  exit 1
fi
echo "lint-shard: $shard -> ${#pkgs[@]} package(s)"
[[ "${LINT_LIST_ONLY:-0}" == 1 ]] && { printf '  %s\n' "${pkgs[@]}"; exit 0; }

args=()
for p in "${pkgs[@]}"; do
  [[ "$p" == . ]] && args+=(.) || args+=("./$p")
done
exec golangci-lint run --timeout "${LINT_TIMEOUT:-55m}" "${args[@]}"
