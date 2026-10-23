#!/usr/bin/env bash
# Sends orders to a Service Bus queue, runs the worker that stores them in
# Cosmos DB, then reads them back through the Cosmos change feed.
# Idempotent: the queue is recreated each run and orders are upserted by id.
set -euo pipefail

cd "$(dirname "$0")"

SERVICEBUS_URL="${SERVICEBUS_URL:-http://localhost:10003}"
COSMOS_URL="${COSMOS_URL:-http://localhost:8081}"
SERVICEBUS_QUEUE="${SERVICEBUS_QUEUE:-orders}"
export SERVICEBUS_URL COSMOS_URL SERVICEBUS_QUEUE

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

wait_for() {
  local name="$1" url="$2" deadline=$((SECONDS + 30))
  echo "Waiting for ${name} (${url})..."
  until curl -sS --connect-timeout 1 --max-time 2 -o /dev/null "${url}/" 2>/dev/null; do
    ((SECONDS < deadline)) || fail "${name} not ready within 30s"
    sleep 0.5
  done
}

send() {
  curl -fsS -X POST "${SERVICEBUS_URL}/${SERVICEBUS_QUEUE}/messages" \
    -H 'Content-Type: application/json' --data "$1" -o /dev/null || fail "send $1"
}

wait_for "Service Bus" "$SERVICEBUS_URL"
wait_for "Cosmos DB" "$COSMOS_URL"

curl -sS -X DELETE "${SERVICEBUS_URL}/${SERVICEBUS_QUEUE}" -o /dev/null || true
curl -fsS -X PUT "${SERVICEBUS_URL}/${SERVICEBUS_QUEUE}" -o /dev/null || fail "create queue"

send '{"id":"order-1","customer":"alice","amount":10.50}'
send '{"id":"order-2","customer":"bob","amount":20.00}'
send '{"id":"order-3","customer":"alice","amount":7.25}'
echo "Sent 3 orders to queue ${SERVICEBUS_QUEUE}"

go build -o ./.worker . || fail "go build"
trap 'rm -f ./.worker' EXIT

worker_out="$(./.worker worker -n 3 2>&1)" || { echo "$worker_out"; fail "worker"; }
echo "$worker_out"
feed_out="$(./.worker feed -n 3 2>&1)" || { echo "$feed_out"; fail "change feed reader"; }
echo "$feed_out"

for id in order-1 order-2 order-3; do
  grep -q "stored ${id} " <<<"$worker_out" || fail "worker did not store ${id}"
  grep -q "change feed: ${id} " <<<"$feed_out" || fail "${id} missing from change feed"
done

echo "PASS: azure-servicebus-cosmos-worker (3 messages consumed, stored and seen in the change feed)"
