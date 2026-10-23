#!/usr/bin/env bash
# Builds a tiny Cloud Run container, deploys it to the Cloud Run emulator,
# subscribes it to a Pub/Sub topic with a push subscription, publishes three
# messages and checks that the service received and logged them.
# Needs Docker (image build + Cloud Run container runtime). Idempotent.
set -euo pipefail

cd "$(dirname "$0")"

RUN_ADDR="${RUN_ADDR:-localhost:8126}"
PUBSUB_ADDR="${PUBSUB_ADDR:-localhost:8085}"
RUN_INGRESS_ADDR="${RUN_INGRESS_ADDR:-localhost:8127}"
IMAGE="gopherstack-example/push-receiver:latest"
export RUN_ADDR PUBSUB_ADDR

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

command -v docker >/dev/null || fail "docker is required"
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

wait_for_port() {
  local name="$1" addr="$2" deadline=$((SECONDS + 30))
  echo "Waiting for ${name} (${addr})..."
  until (exec 3<>"/dev/tcp/${addr%:*}/${addr#*:}") 2>/dev/null; do
    ((SECONDS < deadline)) || fail "${name} not ready within 30s"
    sleep 0.5
  done
}

wait_for_port "Cloud Run" "$RUN_ADDR"
wait_for_port "Pub/Sub" "$PUBSUB_ADDR"

echo "--- build container image ---"
CGO_ENABLED=0 go build -o service/push-receiver ./service || fail "go build service"
docker build -q -t "$IMAGE" service >/dev/null || fail "docker build"

cleanup() { go run ./driver cleanup >/dev/null 2>&1 || true; rm -f service/push-receiver; }
trap cleanup EXIT

echo "--- deploy service and push subscription ---"
deploy_out="$(go run ./driver deploy 2>&1)" || { echo "$deploy_out"; fail "deploy"; }
echo "$deploy_out"
host="$(sed -n 's/^SERVICE_HOST=//p' <<<"$deploy_out")"
[[ -n "$host" ]] || fail "driver did not report SERVICE_HOST"

echo "--- publish ---"
go run ./driver publish 3 || fail "publish"

echo "--- wait for push deliveries ---"
deadline=$((SECONDS + 30))
msgs=""
while ((SECONDS < deadline)); do
  msgs="$(curl -fsS -H "Host: ${host}" "http://${RUN_INGRESS_ADDR}/messages" 2>/dev/null || true)"
  [[ "$(grep -o '"data":"order-' <<<"$msgs" | wc -l)" -ge 3 ]] && break
  sleep 0.5
done
echo "service /messages: ${msgs}"
for n in 1 2 3; do
  grep -q "\"data\":\"order-${n}\"" <<<"$msgs" || fail "order-${n} was not delivered to the service"
done

echo "--- container log ---"
cid="$(docker ps -q --filter "ancestor=${IMAGE}" | head -1)"
[[ -n "$cid" ]] || fail "Cloud Run container not found"
logs="$(docker logs "$cid" 2>&1)"
grep 'received pubsub message' <<<"$logs"
for n in 1 2 3; do
  grep -q "received pubsub message .*data=\"order-${n}\"" <<<"$logs" || fail "order-${n} not in container log"
done

echo "PASS: gcp-run-pubsub-push (3 Pub/Sub messages pushed to the Cloud Run service and logged)"
