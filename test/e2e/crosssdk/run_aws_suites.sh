#!/usr/bin/env bash
# Starts a gopherstack binary on a free port with a temp data dir, runs the
# boto3, AWS SDK v3 and AWS CLI suites against it, then stops it by PID.
# Usage: run_aws_suites.sh /path/to/gopherstack
set -uo pipefail

BIN="${1:?usage: run_aws_suites.sh /path/to/gopherstack}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DATA="$(mktemp -d)"
PORT="$(python3 -c 'import socket; s = socket.socket(); s.bind(("127.0.0.1", 0)); print(s.getsockname()[1])')"

(cd "$DATA" && PORT="$PORT" exec "$BIN" >"$DATA/server.log" 2>&1) &
PID=$!
cleanup() {
  kill "$PID" 2>/dev/null
  wait "$PID" 2>/dev/null
  rm -rf "$DATA"
}
trap cleanup EXIT

for _ in $(seq 1 60); do
  curl -fsS "http://localhost:$PORT/_gopherstack/health" >/dev/null 2>&1 && break
  kill -0 "$PID" 2>/dev/null || { echo "gopherstack exited early" >&2; cat "$DATA/server.log" >&2; exit 1; }
  sleep 0.5
done

export AWS_ENDPOINT_URL="http://localhost:$PORT"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
cd "$HERE"
rc=0
python3 aws_smoke.py || rc=1
node aws_smoke.mjs || rc=1
./aws_cli_smoke.sh || rc=1
[[ $rc -eq 0 ]] || { echo "--- server log tail ---" >&2; tail -30 "$DATA/server.log" >&2; }
exit $rc
