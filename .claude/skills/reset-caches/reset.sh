#!/usr/bin/env bash
# Full dev-box reset: stop gopherstack servers, test binaries and every
# gopherstack/testcontainers container, then reclaim regenerable caches.
# --soft skips the kill phase (cache cleanup only, safe while builds run).
set -uo pipefail

AGE_MIN=${AGE_MIN:-60}
FORCE=0
SOFT=0
OTHER_SCRATCH=0
for a in "$@"; do
	case "$a" in
	--force) FORCE=1 ;;
	--soft) SOFT=1 ;;
	--other-scratch) OTHER_SCRATCH=1 ;;
	-h | --help)
		echo "usage: reset.sh [--soft] [--force] [--other-scratch]  (AGE_MIN=$AGE_MIN)"
		exit 0
		;;
	esac
done

GOCACHE_DIR=$(go env GOCACHE 2>/dev/null || echo /mnt/fast/gocache)
GOTMP_DIR=$(go env GOTMPDIR 2>/dev/null || echo /mnt/fast/gotmp)
SCRATCH_ROOT="$GOTMP_DIR/claude-$(id -u)"
REPO=$(git -C "$(dirname "$0")" rev-parse --show-toplevel 2>/dev/null || echo "$HOME/gopherstack")
STRAY_CACHES=("$HOME/gocache-worker" "$HOME/.cache/go-build-lambda-task" "$HOME/gotmp-worker")

if [[ $SOFT -eq 0 ]]; then
	echo "== kill: gopherstack servers and test binaries"
	pkill -f '/gopherstack (serve|--)' && echo "stopped gopherstack servers"
	pkill -f '\.test( |$)' && echo "stopped test binaries"
	if command -v docker >/dev/null; then
		echo "== kill: gopherstack / testcontainers containers"
		ids=$( (docker ps -aq --filter 'name=gopherstack'; docker ps -aq --filter 'label=org.testcontainers=true') | sort -u)
		if [[ -n "$ids" ]]; then
			# shellcheck disable=SC2086
			docker rm -f $ids >/dev/null && echo "removed $(wc -w <<<"$ids") containers"
		fi
	fi
	FORCE=1
fi

building=0
if pgrep -f '/pkg/tool/.*/(compile|link)' >/dev/null || pgrep -x golangci-lint >/dev/null; then
	building=1
fi
age_args=()
if [[ $building -eq 1 && $FORCE -eq 0 ]]; then
	age_args=(-mmin "+$AGE_MIN")
	echo "Go build active: only removing cache entries older than ${AGE_MIN}m (use --force to wipe)."
fi

before=$(df -h --output=avail "$GOCACHE_DIR" / | tail -2 | tr -s ' \n' ' ')

echo "== Go build cache: $GOCACHE_DIR"
if [[ ${#age_args[@]} -eq 0 ]]; then
	go clean -cache
else
	find "$GOCACHE_DIR" -type f "${age_args[@]}" -delete 2>/dev/null
fi

echo "== stale go-build temp dirs in $GOTMP_DIR"
find "$GOTMP_DIR" -maxdepth 1 -name 'go-build*' -mmin "+$AGE_MIN" -exec rm -rf {} + 2>/dev/null

echo "== golangci-lint cache"
golangci-lint cache clean >/dev/null 2>&1 || true

echo "== stray agent caches"
for d in "${STRAY_CACHES[@]}"; do
	[[ -d "$d" ]] && rm -rf "$d" && echo "removed $d"
done

echo "== this session's old task logs (> ${AGE_MIN}m)"
find "$SCRATCH_ROOT" -path '*/tasks/*' -type f -mmin "+$AGE_MIN" -delete 2>/dev/null

if [[ $OTHER_SCRATCH -eq 1 ]]; then
	echo "== other sessions' scratch dirs untouched for > 1 day"
	find "$SCRATCH_ROOT"/*/ -mindepth 1 -maxdepth 1 -type d -mtime +0 -exec rm -rf {} + 2>/dev/null
fi

echo "== git worktrees: remove clean, fully-pushed linked worktrees"
git -C "$REPO" worktree prune
main_wt=$(git -C "$REPO" rev-parse --show-toplevel)
git -C "$REPO" worktree list --porcelain | awk '/^worktree /{print $2}' | while read -r wt; do
	[[ "$wt" == "$main_wt" ]] && continue
	if [[ -n "$(git -C "$wt" status --porcelain 2>/dev/null)" ]]; then
		echo "kept (uncommitted changes): $wt"
		continue
	fi
	if ! git -C "$wt" rev-parse --abbrev-ref '@{u}' >/dev/null 2>&1; then
		echo "kept (branch has no upstream): $wt"
		continue
	fi
	if [[ -n "$(git -C "$wt" rev-list '@{u}..HEAD' 2>/dev/null)" ]]; then
		echo "kept (unpushed commits): $wt"
		continue
	fi
	git -C "$REPO" worktree remove "$wt" && echo "removed $wt"
done

if command -v docker >/dev/null; then
	echo "== docker: unused images, build cache, dangling volumes (running containers untouched)"
	docker image prune -af >/dev/null && echo "images pruned"
	docker builder prune -af >/dev/null && echo "build cache pruned"
	docker volume prune -af >/dev/null && echo "unused volumes pruned"
fi

after=$(df -h --output=avail "$GOCACHE_DIR" / | tail -2 | tr -s ' \n' ' ')
echo "avail before: $before | after: $after"
