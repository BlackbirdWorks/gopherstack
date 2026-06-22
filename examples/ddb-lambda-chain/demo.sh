#!/bin/sh
# DynamoDB -> Lambda (Go, zip) -> DynamoDB chain demo against gopherstack.
#
# Builds a Go zip Lambda, creates three DynamoDB tables with streams, wires
# each table's stream to the Lambda via an event source mapping, then inserts
# one seed item into chain-1. The Lambda copies it down the chain, producing a
# 3-link sequence of DynamoDB events:
#
#   put chain-1 -> stream -> lambda -> put chain-2 -> stream -> lambda -> chain-3
#
# Exercises the real DynamoDB Streams -> Lambda event-source-mapping plumbing
# inside gopherstack (Docker-run Go Lambda), not isolated API mocks.
#
# Ref (canonical AWS pattern):
# https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_serverless_DynamoDB_Lambda_section.html
set -eu

AWS="aws --endpoint-url ${ENDPOINT:-http://localhost:8000} --region us-east-1 --no-cli-pager --output json"
FUNC=chain-fn
ROLE_ARN="arn:aws:iam::000000000000:role/chain-fn-role"
TABLES="chain-1 chain-2 chain-3"
WORK="$(mktemp -d)"

cleanup() {
  for t in $TABLES; do $AWS dynamodb delete-table --table-name "$t" >/dev/null 2>&1 || true; done
  $AWS lambda delete-function --function-name "$FUNC" >/dev/null 2>&1 || true
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "=== Building Go zip Lambda (bootstrap, provided.al2) ==="
# Lambda source is mounted read-only at /lambda in Docker, but fallback to local directory.
LAMBDA_SRC="${LAMBDA_SRC:-/lambda}"
if [ ! -d "$LAMBDA_SRC" ]; then
  LAMBDA_SRC="$(cd "$(dirname "$0")" && pwd)/lambda"
fi
cp -r "$LAMBDA_SRC"/* "$WORK"/
( cd "$WORK" && go mod download && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags lambda.norpc -o bootstrap . )
( cd "$WORK" && zip -q function.zip bootstrap )
echo "built $WORK/function.zip"

echo ""
echo "=== Creating 3 DynamoDB tables with streams (NEW_IMAGE) ==="
for t in $TABLES; do
  $AWS dynamodb create-table \
    --table-name "$t" \
    --attribute-definitions AttributeName=id,AttributeType=S \
    --key-schema AttributeName=id,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST \
    --stream-specification StreamEnabled=true,StreamViewType=NEW_IMAGE \
    >/dev/null
  echo "  created $t"
done

echo ""
echo "=== Creating the Lambda function ==="
# package-type Zip is explicit so the bootstrap zip isn't misread as an image.
$AWS lambda create-function \
  --function-name "$FUNC" \
  --runtime provided.al2 \
  --package-type Zip \
  --role "$ROLE_ARN" \
  --handler bootstrap \
  --zip-file "fileb://$WORK/function.zip" \
  --environment "Variables={AWS_ENDPOINT_URL=${LAMBDA_ENDPOINT:-http://host.docker.internal:8000},AWS_ACCESS_KEY_ID=test,AWS_SECRET_ACCESS_KEY=test,AWS_DEFAULT_REGION=us-east-1}" \
  >/dev/null
echo "  created function $FUNC"

echo ""
echo "=== Wiring each table stream -> Lambda (event source mappings) ==="
for t in $TABLES; do
  STREAM_ARN=$($AWS dynamodb describe-table --table-name "$t" \
    --query 'Table.LatestStreamArn' --output text)
  $AWS lambda create-event-source-mapping \
    --function-name "$FUNC" \
    --event-source-arn "$STREAM_ARN" \
    --starting-position LATEST \
    --batch-size 10 \
    >/dev/null
  echo "  mapped $t stream -> $FUNC"
done

echo ""
echo "=== Inserting seed item into chain-1 (hop 1) ==="
$AWS dynamodb put-item --table-name chain-1 \
  --item '{"id":{"S":"order-42"},"hop":{"N":"1"},"from":{"S":"seed"}}'

echo ""
echo "=== Waiting for the chain to propagate (stream -> lambda x2) ==="
i=0
while [ "$i" -lt 30 ]; do
  COUNT=$($AWS dynamodb scan --table-name chain-3 --select COUNT --query Count --output text 2>/dev/null || echo 0)
  [ "$COUNT" != "0" ] && break
  i=$((i + 1)); sleep 1
done

echo ""
echo "=== Result: contents of each table ==="
for t in $TABLES; do
  echo "--- $t ---"
  $AWS dynamodb scan --table-name "$t" --query 'Items'
done

FINAL=$($AWS dynamodb scan --table-name chain-3 --query 'Count' --output text)
echo ""
echo "=== Lambda Logs ==="
$AWS logs filter-log-events --log-group-name /aws/lambda/$FUNC --query 'events[*].message' --output text || true
echo ""
if [ "$FINAL" = "1" ]; then
  echo "SUCCESS: event chained chain-1 -> chain-2 -> chain-3 (3 DynamoDB events)."
else
  echo "FAIL: chain-3 has $FINAL items (expected 1)."; exit 1
fi
