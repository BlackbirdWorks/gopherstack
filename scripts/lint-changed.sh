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
#   scripts/lint-changed.sh                # auto-detect changed files (working tree + branch vs origin/main)
#   scripts/lint-changed.sh --wip          # only uncommitted + last commit (HEAD~1) changes (fast on huge branches)
#   scripts/lint-changed.sh --base <ref>   # diff against <ref> instead of origin/main
#   scripts/lint-changed.sh FILE...        # lint only these files' packages
#
# Env: LINT_BATCH (15) max packages per golangci-lint/go vet invocation,
#      LINT_TIMEOUT (30m) per-batch limit, LINT_CONCURRENCY (from GOFLAGS -p, else nproc/2),
#      LINT_LOCK_RETRIES (20) x 15s waits when another golangci-lint holds the lock.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

BATCH_SIZE="${LINT_BATCH:-15}"
LINT_TIMEOUT="${LINT_TIMEOUT:-30m}"
LOCK_RETRIES="${LINT_LOCK_RETRIES:-20}"
BASE_REF="${LINT_BASE:-}"
WIP_ONLY=0

args=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    --wip)
      WIP_ONLY=1
      shift
      ;;
    --base)
      BASE_REF="$2"
      shift 2
      ;;
    *)
      args+=("$1")
      shift
      ;;
  esac
done

bash "$(dirname -- "${BASH_SOURCE[0]}")/gocache-trim.sh" --if-low || true

lint_conc="${LINT_CONCURRENCY:-}"
if [[ -z "$lint_conc" ]]; then
  if [[ "${GOFLAGS:-}" =~ -p=([0-9]+) ]]; then
    lint_conc="${BASH_REMATCH[1]}"
  else
    lint_conc=$(( $(nproc) / 2 ))
    [[ "$lint_conc" -lt 1 ]] && lint_conc=1
  fi
fi

timeout_secs() {
  case "$1" in
    *h) echo $(( ${1%h} * 3600 )) ;;
    *m) echo $(( ${1%m} * 60 )) ;;
    *s) echo "${1%s}" ;;
    *) echo "$1" ;;
  esac
}
hard_timeout=$(( $(timeout_secs "$LINT_TIMEOUT") + 120 ))

for bin in golangci-lint go; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "lint-changed: $bin not found on PATH" >&2
    exit 1
  }
done

if [[ ${#args[@]} -gt 0 ]]; then
  changed_files=("${args[@]}")
else
  merge_base=""
  if [[ "$WIP_ONLY" -eq 1 ]]; then
    if git rev-parse --verify -q HEAD~1 >/dev/null 2>&1; then
      merge_base="HEAD~1"
    fi
  elif [[ -n "$BASE_REF" ]]; then
    if git rev-parse --verify -q "$BASE_REF" >/dev/null 2>&1; then
      merge_base=$(git merge-base HEAD "$BASE_REF")
    fi
  elif git rev-parse --verify -q origin/main >/dev/null 2>&1; then
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
  echo "lint-changed: no changed Go files -- nothing to check."
  exit 0
fi

declare -A seen
pkgs=()
skipped=()
for f in "${changed_files[@]}"; do
  [[ -f "$f" ]] || continue
  case "/$f" in
    */testdata/*|*/vendor/*|*/.*|/desktop/*)
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

# Packages whose files all carry a //go:build constraint (test/e2e) are empty
# under default tags and make go vet/golangci-lint fail with "build
# constraints exclude all Go files". Detect the tags and lint those packages
# in their own group with the tags set.
detect_tags() {
  local d="$1"
  if [[ -n "$(go list -e -f '{{.GoFiles}}{{.TestGoFiles}}{{.XTestGoFiles}}' "$d" 2>/dev/null | tr -d '[] ')" ]]; then
    return 0
  fi
  # || true: no build tags (e.g. a nested module the root `go list` can't see)
  # makes grep exit 1, which would abort the whole script under set -e/pipefail.
  { cat "$d"/*.go 2>/dev/null | grep -h '^//go:build' | sed -E 's/![A-Za-z0-9_.]+//g' |
    tr -c 'A-Za-z0-9_\n' ' ' | tr ' ' '\n' |
    grep -Ev '^(go|build|ignore|cgo|linux|darwin|windows|freebsd|amd64|arm64|386|arm|unix|)$' |
    sort -u | paste -sd, -; } || true
}

declare -A group_pats
tagless=()
for p in "${sorted_patterns[@]}"; do
  tags=$(detect_tags "$p")
  if [[ -z "$tags" ]]; then
    if [[ -z "$(go list -e -f '{{.GoFiles}}{{.TestGoFiles}}{{.XTestGoFiles}}' "$p" 2>/dev/null | tr -d '[] ')" ]]; then
      echo "lint-changed: skipping $p (every file is build-constrained and no tag could be determined)"
      continue
    fi
    tagless+=("$p")
  else
    echo "lint-changed: $p is build-tagged -- using tags: $tags"
    group_pats["$tags"]+="$p"$'\n'
  fi
done

# run_one DESC CMD... -- bounded by a hard timeout, retried while another
# golangci-lint run holds the lock, and names the batch on timeout.
run_one() {
  local desc="$1" attempt=0 rc out
  shift
  out=$(mktemp)
  while :; do
    set +e
    timeout --foreground "$hard_timeout" "$@" 2>&1 | tee "$out"
    rc=${PIPESTATUS[0]}
    set -e
    if [[ $rc -ne 0 ]] && grep -q 'parallel golangci-lint is running' "$out" && [[ $attempt -lt $LOCK_RETRIES ]]; then
      attempt=$((attempt + 1))
      echo "lint-changed: another golangci-lint holds the lock; retry $attempt/$LOCK_RETRIES in 15s"
      sleep 15
      continue
    fi
    break
  done
  if [[ $rc -eq 124 ]] || grep -q 'context deadline exceeded' "$out"; then
    echo "lint-changed: TIMEOUT (LINT_TIMEOUT=$LINT_TIMEOUT) in $desc -- raise LINT_TIMEOUT or lower LINT_BATCH" >&2
  fi
  runs_total=$((runs_total + 1))
  if [[ $rc -ne 0 ]]; then
    runs_failed=$((runs_failed + 1))
    local n
    n=$(sed -nE 's/^([0-9]+) issues?[.:]?$/\1/p' "$out" | tail -1)
    issues_total=$((issues_total + ${n:-0}))
    if [[ -z "$n" || "$n" -eq 0 ]]; then
      # Non-zero exit with no findings (e.g. systemd scope/OOM kill, exit 201):
      # a tool failure, never a clean lint.
      tool_failures=$((tool_failures + 1))
      echo "lint-changed: $desc exited $rc but reported no findings -- tool failure (OOM kill / scope / crash), NOT a clean run; re-run it" >&2
    fi
  fi
  rm -f "$out"
  return "$rc"
}

# Batched, not one giant argv, so a large diff can't silently hit an OS
# argv limit -- every package printed above is guaranteed to land in some
# batch and every batch's result is folded into the final exit status.
# Usage: run_batched DESC TAGS CMD... -- the tags are appended as
# golangci-lint --build-tags / go vet -tags by the callers via TAGFLAG.
run_batched() {
  local desc="$1" tags="$2" flag="$3"
  shift 3
  local -a cmd=("$@")
  [[ -n "$tags" ]] && cmd+=("$flag=$tags")
  local -a pats
  if [[ -z "$tags" ]]; then
    pats=("${tagless[@]}")
  else
    mapfile -t pats < <(printf '%s' "${group_pats[$tags]}")
  fi
  local status=0 i=0 n=${#pats[@]}
  while [[ $i -lt $n ]]; do
    local batch=("${pats[@]:$i:$BATCH_SIZE}")
    run_one "$desc batch $((i / BATCH_SIZE + 1))/$(((n + BATCH_SIZE - 1) / BATCH_SIZE)) (${batch[0]} ... ${batch[-1]})" "${cmd[@]}" "${batch[@]}" || status=1
    i=$((i + BATCH_SIZE))
  done
  if [[ $status -ne 0 ]]; then
    echo "lint-changed: $desc${tags:+ [tags=$tags]} FAILED"
  else
    echo "lint-changed: $desc${tags:+ [tags=$tags]} passed"
  fi
  return $status
}

overall=0
runs_total=0 runs_failed=0 issues_total=0 tool_failures=0
mulint_bin=""
mulint_bin=$(go tool -n mulint-vet 2>/dev/null) || mulint_bin=""
group_keys=("")
for k in "${!group_pats[@]}"; do group_keys+=("$k"); done
for tags in "${group_keys[@]}"; do
  if [[ -z "$tags" && ${#tagless[@]} -eq 0 ]]; then continue; fi
  run_batched "golangci-lint" "$tags" --build-tags golangci-lint run --timeout "$LINT_TIMEOUT" --concurrency "$lint_conc" || overall=1
  run_batched "go vet" "$tags" -tags go vet || overall=1
  if [[ -n "$mulint_bin" ]]; then
    run_batched "mulint-vet" "$tags" -tags go vet -vettool="$mulint_bin" || overall=1
  fi
done

if [[ $overall -ne 0 ]]; then
  echo "lint-changed: SUMMARY: FAILED -- $issues_total issue(s) across $runs_failed failing run(s) of $runs_total ($tool_failures tool failure(s))"
else
  echo "lint-changed: SUMMARY: OK -- 0 issues across $runs_total run(s)"
fi

exit $overall
