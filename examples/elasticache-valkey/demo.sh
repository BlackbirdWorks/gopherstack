#!/bin/sh
set -e

echo "=== Installing tools ==="
apk add --no-cache aws-cli valkey-cli jq bind-tools > /dev/null 2>&1
echo "Done."

# Point DNS at Gopherstack's embedded DNS server so AWS-style
# *.cache.amazonaws.com hostnames resolve to 127.0.0.1.
echo "nameserver 127.0.0.1" > /etc/resolv.conf
echo "options ndots:0" >> /etc/resolv.conf

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
ENDPOINT="http://localhost:8000"

echo ""
echo "=== Creating ElastiCache cluster ==="
aws elasticache create-cache-cluster \
  --cache-cluster-id my-valkey-cluster \
  --engine redis \
  --cache-node-type cache.t3.micro \
  --num-cache-nodes 1 \
  --endpoint-url "$ENDPOINT" \
  --no-cli-pager

echo ""
echo "=== Describing cluster ==="
RESULT=$(aws elasticache describe-cache-clusters \
  --cache-cluster-id my-valkey-cluster \
  --show-cache-node-info \
  --endpoint-url "$ENDPOINT" \
  --output json \
  --no-cli-pager)

echo "$RESULT" | jq .

# Extract endpoint from the describe response.
HOST=$(echo "$RESULT" | jq -r '.CacheClusters[0].CacheNodes[0].Endpoint.Address')
PORT=$(echo "$RESULT" | jq -r '.CacheClusters[0].CacheNodes[0].Endpoint.Port')

echo ""
echo "=== Resolving $HOST via embedded DNS ==="
nslookup "$HOST" 127.0.0.1 || true

echo ""
echo "=== Connecting to Valkey at $HOST:$PORT ==="

echo "PING:"
valkey-cli -h "$HOST" -p "$PORT" PING

echo ""
echo "SET greeting \"Hello from Gopherstack!\":"
valkey-cli -h "$HOST" -p "$PORT" SET greeting "Hello from Gopherstack!"

echo ""
echo "GET greeting:"
valkey-cli -h "$HOST" -p "$PORT" GET greeting

echo ""
echo "=== Demo complete ==="
