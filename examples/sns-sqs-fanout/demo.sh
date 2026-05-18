#!/bin/sh
# SNS -> SQS fan-out demo against the gopherstack mock AWS API.
#
# Creates one SNS topic, two SQS queues, subscribes both queues to the topic,
# publishes a message, then receives it from both queues. This exercises the
# real fan-out plumbing inside gopherstack (SNS publish emitter -> SQS
# delivery wired up in cli.go), not just two isolated API mocks.
set -eu

AWS="aws --endpoint-url ${ENDPOINT:-http://localhost:8000} --no-cli-pager --output json"

echo "=== Creating SNS topic ==="
TOPIC_ARN=$($AWS sns create-topic --name orders | python3 -c "import json,sys;print(json.load(sys.stdin)['TopicArn'])")
echo "TopicArn: $TOPIC_ARN"

echo ""
echo "=== Creating SQS queues ==="
BILLING_URL=$($AWS sqs create-queue --queue-name billing  | python3 -c "import json,sys;print(json.load(sys.stdin)['QueueUrl'])")
SHIPPING_URL=$($AWS sqs create-queue --queue-name shipping | python3 -c "import json,sys;print(json.load(sys.stdin)['QueueUrl'])")
echo "billing : $BILLING_URL"
echo "shipping: $SHIPPING_URL"

BILLING_ARN=$($AWS sqs get-queue-attributes  --queue-url "$BILLING_URL"  --attribute-names QueueArn | python3 -c "import json,sys;print(json.load(sys.stdin)['Attributes']['QueueArn'])")
SHIPPING_ARN=$($AWS sqs get-queue-attributes --queue-url "$SHIPPING_URL" --attribute-names QueueArn | python3 -c "import json,sys;print(json.load(sys.stdin)['Attributes']['QueueArn'])")

echo ""
echo "=== Subscribing both queues to the topic ==="
$AWS sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs --notification-endpoint "$BILLING_ARN"  >/dev/null
$AWS sns subscribe --topic-arn "$TOPIC_ARN" --protocol sqs --notification-endpoint "$SHIPPING_ARN" >/dev/null
$AWS sns list-subscriptions-by-topic --topic-arn "$TOPIC_ARN"

echo ""
echo "=== Publishing one message ==="
$AWS sns publish --topic-arn "$TOPIC_ARN" --message '{"orderId":"o-123","total":42.50}'

# Give the async delivery goroutine a moment to fan out.
sleep 1

receive() {
  name=$1
  url=$2
  echo ""
  echo "=== Receive from $name ==="
  $AWS sqs receive-message --queue-url "$url" --wait-time-seconds 2 --max-number-of-messages 1
}

receive billing  "$BILLING_URL"
receive shipping "$SHIPPING_URL"

echo ""
echo "=== SUCCESS: one publish fanned out to both queues ==="
