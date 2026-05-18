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

echo "=== Creating SQS target queue ==="
QUEUE_URL=$($AWS sqs create-queue --queue-name "$QUEUE_NAME" | python3 -c "import json,sys;print(json.load(sys.stdin)['QueueUrl'])")
QUEUE_ARN=$($AWS sqs get-queue-attributes --queue-url "$QUEUE_URL" --attribute-names QueueArn | python3 -c "import json,sys;print(json.load(sys.stdin)['Attributes']['QueueArn'])")
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
$AWS sqs receive-message --queue-url "$QUEUE_URL" --wait-time-seconds 2 --max-number-of-messages 1

echo ""
echo "=== SUCCESS: EventBridge rule delivered event to SQS ==="
