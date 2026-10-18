#!/usr/bin/env bash
# gsdebug.sh — inspect and reset a RUNNING gopherstack server.
#
# Usage:
#   scripts/dev/gsdebug.sh health
#   scripts/dev/gsdebug.sh reset [service]
#   scripts/dev/gsdebug.sh requests [-n N] [--failed | --status S] [--service SVC] [--json]
#   scripts/dev/gsdebug.sh snapshot [service]
#
#   requests  recent captured requests, newest first (S: 404, 4xx, 5xx, failed)
#   snapshot  no arg: services with snapshot sizes; with arg: that service's state
#
# Endpoint: $GOPHERSTACK_ENDPOINT (default http://localhost:8123).

set -euo pipefail

ENDPOINT="${GOPHERSTACK_ENDPOINT:-http://localhost:8123}"
ENDPOINT="${ENDPOINT%/}"

usage() {
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//' >&2
  exit 2
}

[[ $# -ge 1 ]] || usage
if [[ "$1" == "-h" || "$1" == "--help" ]]; then
  sed -n '2,13p' "$0" | sed 's/^# \{0,1\}//'
  exit 0
fi

for bin in curl jq; do
  command -v "$bin" >/dev/null 2>&1 || {
    echo "gsdebug: $bin not found on PATH" >&2
    exit 2
  }
done

# call METHOD PATH [curl args...] — prints body; fails clearly on unreachable/HTTP error.
call() {
  local method="$1" path="$2" out code
  shift 2
  out="$(mktemp)"
  if ! code="$(curl -sS -o "$out" -w '%{http_code}' -X "$method" "$@" "$ENDPOINT$path" 2>/dev/null)"; then
    rm -f "$out"
    echo "gsdebug: cannot reach gopherstack at $ENDPOINT" >&2
    echo "  start it: .claude/skills/run-gopherstack/driver.sh up   (or: task run)" >&2
    echo "  other port: export GOPHERSTACK_ENDPOINT=http://localhost:<port>" >&2
    exit 1
  fi
  if [[ "$code" != 2* ]]; then
    echo "gsdebug: $method $path -> HTTP $code" >&2
    cat "$out" >&2
    echo >&2
    rm -f "$out"
    exit 1
  fi
  cat "$out"
  rm -f "$out"
}

cmd_health() {
  call GET /_gopherstack/health | jq -c 'if (.services|type)=="array" then .services |= length else . end' 
}

cmd_reset() {
  [[ $# -le 1 ]] || usage
  if [[ $# -eq 1 ]]; then
    call POST /_gopherstack/reset -G --data-urlencode "service=$1" | jq -c .
  else
    call POST /_gopherstack/reset | jq -c .
  fi
}

cmd_requests() {
  local n="" status="" service="" raw=0
  while [[ $# -gt 0 ]]; do
    case "$1" in
      -n) n="${2:?-n needs a value}"; shift 2 ;;
      --failed) status="failed"; shift ;;
      --status) status="${2:?--status needs a value}"; shift 2 ;;
      --service) service="${2:?--service needs a value}"; shift 2 ;;
      --json) raw=1; shift ;;
      *) usage ;;
    esac
  done
  local args=(-G)
  [[ -z "$n" ]] || args+=(--data-urlencode "n=$n")
  [[ -z "$status" ]] || args+=(--data-urlencode "status=$status")
  [[ -z "$service" ]] || args+=(--data-urlencode "service=$service")
  local body
  body="$(call GET /_gopherstack/requests "${args[@]}")"
  if [[ $raw -eq 1 ]]; then
    jq . <<<"$body"
    return
  fi
  jq -r '.[] | [
      (.timestamp | sub("\\.[0-9]+"; "") | sub("^[^T]*T"; "")),
      .method, (.service // "-"), (.status | tostring),
      (.path + (if (.query // "") != "" then "?" + .query else "" end)),
      (if (.error_code // "") != "" then .error_code + ": " + (.error_message // "") else "" end)
    ] | map(.[0:90]) | @tsv' <<<"$body" | column -t -s "$(printf '\t')"
}

cmd_snapshot() {
  [[ $# -le 1 ]] || usage
  local body
  body="$(call POST /_gopherstack/snapshot)"
  if [[ $# -eq 0 ]]; then
    jq -r '.services | to_entries | sort_by(-(.value | tojson | length))[]
      | "\(.key)\t\(.value | tojson | length)"' <<<"$body" | column -t -s "$(printf '\t')"
    return
  fi
  local key
  key="$(jq -r --arg s "$1" '.services | keys[] | select(ascii_downcase == ($s | ascii_downcase))' <<<"$body" | head -n1)"
  if [[ -z "$key" ]]; then
    echo "gsdebug: no service '$1' in snapshot; available:" >&2
    jq -r '.services | keys | join(" ")' <<<"$body" | fold -s -w 100 >&2
    exit 1
  fi
  jq --arg s "$key" '.services[$s]' <<<"$body"
}

sub="$1"
shift
case "$sub" in
  health) cmd_health "$@" ;;
  reset) cmd_reset "$@" ;;
  requests) cmd_requests "$@" ;;
  snapshot) cmd_snapshot "$@" ;;
  *) usage ;;
esac
