#!/usr/bin/env bash
set -e

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "=== Initializing OpenTofu ==="
tofu init

echo ""
echo "=== Deploying Kinesis Aggregator Stack to Local Gopherstack ==="
tofu apply -auto-approve

STREAM_NAME=$(tofu output -raw stream_name)
TABLE_NAME=$(tofu output -raw table_name)

echo ""
echo "=== Sending Test Events to Kinesis ==="
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

aws --endpoint-url http://localhost:8000 --region us-east-1 \
  kinesis put-record \
  --stream-name event-stream \
  --partition-key "user1" \
  --cli-binary-format raw-in-base64-out \
  --data '{"type":"click", "user":"alice"}'

aws --endpoint-url http://localhost:8000 --region us-east-1 \
  kinesis put-record \
  --stream-name event-stream \
  --partition-key "user2" \
  --cli-binary-format raw-in-base64-out \
  --data '{"type":"purchase", "user":"bob"}'

aws --endpoint-url http://localhost:8000 --region us-east-1 \
  kinesis put-record \
  --stream-name event-stream \
  --partition-key "user1" \
  --cli-binary-format raw-in-base64-out \
  --data '{"type":"click", "user":"alice"}'

aws --endpoint-url http://localhost:8000 --region us-east-1 kinesis put-record \
  --stream-name "$STREAM_NAME" \
  --partition-key "user3" \
  --cli-binary-format raw-in-base64-out \
  --data '{"type":"purchase", "user":"charlie"}'

aws --endpoint-url http://localhost:8000 --region us-east-1 kinesis put-record \
  --stream-name "$STREAM_NAME" \
  --partition-key "user4" \
  --cli-binary-format raw-in-base64-out \
  --data '{"type":"click", "user":"dave"}'

echo ""
echo "=== Waiting for Event Source Mapping to Poll (5s) ==="
sleep 5

echo ""
echo "=== Aggregated Metrics in DynamoDB ==="
aws --endpoint-url http://localhost:8000 --region us-east-1 dynamodb scan \
  --table-name "$TABLE_NAME" \
  --query 'Items[*]' \
  --output json
