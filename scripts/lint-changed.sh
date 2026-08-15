#!/usr/bin/env bash
# lint-changed.sh — golangci-lint + go vet, scoped to the packages touched
# by the current diff, not the whole repo and not one hand-picked service.
#
# gopherstack-a8b5: every prior gate ("golangci-lint run ./services/<svc>/...",
# "go vet ." at the repo root) is scoped to a fixed directory, so a change
# that also touches test/ is linted by nobody until CI's repo-wide run
# catches it at merge time. This resolves the actual diff to package
# directories and lints exactly those -- closes the hole without paying
# repo-wide cost on every commit.
#
# Diff scope = uncommitted working-tree changes (staged + unstaged + new
# untracked files) UNION commits on this branch since it diverged from
# origin/main. Either alone misses a case an agent hits: verifying before
# committing needs the working-tree diff; verifying a multi-commit campaign
# at commit time needs the branch-vs-merge-base diff.
#
# Usage:
#   scripts/lint-changed.sh          # auto-detect changed files from git
#   scripts/lint-changed.sh FILE...  # lint only these files' packages

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

BATCH_SIZE=300

for bin in golangci-lint go; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "lint-changed: $bin not found on PATH" >&2
    exit 1
  }
done

if [[ $# -gt 0 ]]; then
  changed_files=("$@")
else
  merge_base=""
  if git rev-parse --verify -q origin/main >/dev/null 2>&1; then
    merge_base=$(git merge-base HEAD origin/main)
  fi
  changed_files=()
  while IFS= read -r f; do
    [[ -n "$f" ]] && changed_files+=("$f")
  done < <(
    {
      [[ -n "$merge_base" ]] && git diff --no-renames --name-only --diff-filter=ACMR "$merge_base" -- '*.go'
      git diff --no-renames --name-only --diff-filter=ACMR HEAD -- '*.go'
      git ls-files --others --exclude-standard -- '*.go'
    } | sort -u
  )
fi

if [[ ${#changed_files[@]} -eq 0 ]]; then
  echo "lint-changed: no changed Go files (working tree vs HEAD, HEAD vs origin/main) -- nothing to check."
  exit 0
fi

declare -A seen
pkgs=()
skipped=()
for f in "${changed_files[@]}"; do
  [[ -f "$f" ]] || continue
  case "/$f" in
    */testdata/*|*/vendor/*|*/.*)
      skipped+=("$f (excluded dir)")
      continue
      ;;
  esac
  dir=$(dirname -- "$f")
  [[ -n "${seen[$dir]:-}" ]] && continue
  seen["$dir"]=1
  if [[ -d "$dir" ]] && compgen -G "$dir/*.go" >/dev/null; then
    pkgs+=("$dir")
  else
    skipped+=("$dir (no .go files left)")
  fi
done

if [[ ${#pkgs[@]} -eq 0 ]]; then
  echo "lint-changed: ${#changed_files[@]} changed Go file(s), but every containing package was deleted or excluded -- nothing to check."
  if [[ ${#skipped[@]} -gt 0 ]]; then
    printf '  skipped: %s\n' "${skipped[@]}"
  fi
  exit 0
fi

patterns=()
for d in "${pkgs[@]}"; do
  [[ "$d" == "." ]] && patterns+=(".") || patterns+=("./$d")
done
IFS=$'\n' sorted_patterns=($(sort <<<"${patterns[*]}")); unset IFS

echo "lint-changed: checking ${#sorted_patterns[@]} package(s):"
printf '  %s\n' "${sorted_patterns[@]}"
if [[ ${#skipped[@]} -gt 0 ]]; then
  echo "lint-changed: skipped ${#skipped[@]} path(s) (deleted or excluded, never silently dropped from the count above):"
  printf '  %s\n' "${skipped[@]}"
fi

# Batched, not one giant argv, so a large diff can't silently hit an OS
# argv limit -- every package printed above is guaranteed to land in some
# batch and every batch's result is folded into the final exit status.
run_batched() {
  local desc="$1"
  shift
  local -a cmd=("$@")
  local status=0
  local i=0
  local n=${#sorted_patterns[@]}
  while [[ $i -lt $n ]]; do
    local batch=("${sorted_patterns[@]:$i:$BATCH_SIZE}")
    "${cmd[@]}" "${batch[@]}" || status=1
    i=$((i + BATCH_SIZE))
  done
  if [[ $status -ne 0 ]]; then
    echo "lint-changed: $desc FAILED"
  else
    echo "lint-changed: $desc passed"
  fi
  return $status
}

overall=0
run_batched "golangci-lint" golangci-lint run --timeout 20m || overall=1
run_batched "go vet" go vet || overall=1

exit $overall
