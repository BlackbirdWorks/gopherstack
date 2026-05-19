#!/bin/sh
# EventBridge rule -> SQS target demo against the gopherstack mock AWS API.
#
# Creates an EventBridge rule on the default bus that matches a custom event
# pattern, wires an SQS queue as the rule target, fires the event with
# PutEvents, then drains the queue and confirms the event was delivered.
# Exercises the real EventBridge delivery wiring inside gopherstack
# (DeliveryTargets configured in cli.go).
set -eu

AWS="aws --endpoint-url ${ENDPOINT:-http://localhost:8000} --no-cli-pager --output json"

QUEUE_NAME=audit-events
RULE_NAME=order-created
QUEUE_URL=""

cleanup() {
  if [ -n "$QUEUE_URL" ]; then
    $AWS events remove-targets --rule "$RULE_NAME" --ids 1 >/dev/null 2>&1 || true
    $AWS events delete-rule --name "$RULE_NAME" >/dev/null 2>&1 || true
    $AWS sqs delete-queue --queue-url "$QUEUE_URL" >/dev/null 2>&1 || true
  fi
}
trap cleanup EXIT

echo "=== Creating SQS target queue ==="
QUEUE_URL=$($AWS sqs create-queue --queue-name "$QUEUE_NAME" --query QueueUrl --output text)
QUEUE_ARN=$($AWS sqs get-queue-attributes \
  --queue-url "$QUEUE_URL" \
  --attribute-names QueueArn \
  --query 'Attributes.QueueArn' \
  --output text)
echo "queue: $QUEUE_URL"
echo "arn:   $QUEUE_ARN"

echo ""
echo "=== Creating EventBridge rule ==="
$AWS events put-rule \
  --name "$RULE_NAME" \
  --event-pattern '{"source":["com.example.orders"],"detail-type":["OrderCreated"]}' \
  --state ENABLED

echo ""
echo "=== Attaching SQS queue as target ==="
$AWS events put-targets \
  --rule "$RULE_NAME" \
  --targets "Id=1,Arn=$QUEUE_ARN"

echo ""
echo "=== PutEvents ==="
$AWS events put-events --entries '[
  {
    "Source": "com.example.orders",
    "DetailType": "OrderCreated",
    "Detail": "{\"orderId\":\"o-42\",\"total\":17.95}"
  }
]'

# Give the delivery goroutine a moment.
sleep 1

echo ""
echo "=== Draining target queue ==="
$AWS sqs receive-message --queue-url "$QUEUE_URL" --wait-time-seconds 2 --max-number-of-messages 1 >/tmp/eventbridge-message.json
cat /tmp/eventbridge-message.json
grep -q "com.example.orders" /tmp/eventbridge-message.json
grep -q "OrderCreated" /tmp/eventbridge-message.json
grep -q "o-42" /tmp/eventbridge-message.json

echo ""
echo "=== SUCCESS: EventBridge rule delivered event to SQS ==="
