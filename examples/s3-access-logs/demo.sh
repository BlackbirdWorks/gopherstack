#!/bin/sh
# S3 server access logging demo against the gopherstack mock AWS API.
#
# Creates a source bucket and a log bucket, enables S3 server access logging,
# performs a realistic object upload/read path, then verifies a log object was
# written to the target bucket with the expected REST.GET.OBJECT record.
set -eu

AWS="aws --endpoint-url ${ENDPOINT:-http://localhost:8000} --no-cli-pager --output json"

SRC_BUCKET=app-upload-source
LOG_BUCKET=app-upload-access-logs
OBJECT_KEY=incoming/order-1001.json
LOG_KEY=""

cleanup() {
  rm -f /tmp/logging.json /tmp/order.json /tmp/downloaded-order.json /tmp/access.log
  $AWS s3api delete-object --bucket "$SRC_BUCKET" --key "$OBJECT_KEY" >/dev/null 2>&1 || true
  for key in $($AWS s3api list-objects-v2 \
    --bucket "$LOG_BUCKET" \
    --prefix access/ \
    --query 'Contents[].Key' \
    --output text 2>/dev/null || true); do
    $AWS s3api delete-object --bucket "$LOG_BUCKET" --key "$key" >/dev/null 2>&1 || true
  done
  $AWS s3api delete-bucket --bucket "$SRC_BUCKET" >/dev/null 2>&1 || true
  $AWS s3api delete-bucket --bucket "$LOG_BUCKET" >/dev/null 2>&1 || true
}
trap cleanup EXIT

echo "=== Creating S3 buckets ==="
$AWS s3api create-bucket --bucket "$SRC_BUCKET" >/dev/null
$AWS s3api create-bucket --bucket "$LOG_BUCKET" >/dev/null

cat >/tmp/logging.json <<JSON
{
  "LoggingEnabled": {
    "TargetBucket": "$LOG_BUCKET",
    "TargetPrefix": "access/"
  }
}
JSON

echo ""
echo "=== Enabling server access logging ==="
$AWS s3api put-bucket-logging \
  --bucket "$SRC_BUCKET" \
  --bucket-logging-status file:///tmp/logging.json

cat >/tmp/order.json <<JSON
{"orderId":"o-1001","total":129.95,"source":"mobile"}
JSON

echo ""
echo "=== Uploading and reading application object ==="
$AWS s3api put-object \
  --bucket "$SRC_BUCKET" \
  --key "$OBJECT_KEY" \
  --body /tmp/order.json \
  --content-type application/json >/dev/null
$AWS s3api get-object \
  --bucket "$SRC_BUCKET" \
  --key "$OBJECT_KEY" \
  /tmp/downloaded-order.json >/dev/null

echo ""
echo "=== Waiting for access log delivery ==="
for i in $(seq 1 30); do
  LOG_KEYS=$($AWS s3api list-objects-v2 \
    --bucket "$LOG_BUCKET" \
    --prefix access/ \
    --query 'Contents[].Key' \
    --output text 2>/dev/null || true)

  for key in $LOG_KEYS; do
    $AWS s3api get-object \
      --bucket "$LOG_BUCKET" \
      --key "$key" \
      /tmp/access.log >/dev/null

    if grep -q "REST.GET.OBJECT" /tmp/access.log; then
      LOG_KEY=$key
      echo "log object: $LOG_KEY"
      break 2
    fi
  done

  echo "  attempt $i: pending"
  sleep 1
done

if [ -z "$LOG_KEY" ] || [ "$LOG_KEY" = "None" ]; then
  echo "ERROR: no access log object appeared in $LOG_BUCKET" >&2
  exit 1
fi

echo ""
echo "=== Access log record ==="
cat /tmp/access.log

grep -q "REST.GET.OBJECT" /tmp/access.log
grep -q "$SRC_BUCKET" /tmp/access.log
grep -q "$OBJECT_KEY" /tmp/access.log
grep -q "200" /tmp/access.log

echo ""
echo "=== SUCCESS: S3 wrote realistic server access logs to the target bucket ==="
