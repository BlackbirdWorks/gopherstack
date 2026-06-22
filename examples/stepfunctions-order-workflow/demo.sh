#!/usr/bin/env bash
set -e

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test

echo "=== Initializing OpenTofu ==="
tofu init

echo ""
echo "=== Deploying Order Workflow Stack to Local Gopherstack ==="
tofu apply -auto-approve

# Get the State Machine ARN
SFN_ARN=$(tofu output -raw state_machine_arn)

echo ""
echo "=== Starting Step Functions Execution ==="
ORDER_ID=$RANDOM
EXEC_ARN=$(aws --endpoint-url http://localhost:8000 --region us-east-1 stepfunctions start-execution \
  --state-machine-arn "$SFN_ARN" \
  --input "{\"orderId\": \"ORD-$ORDER_ID\", \"amount\": 150}" \
  --query 'executionArn' --output text)

echo "Execution started: $EXEC_ARN"

echo ""
echo "=== Waiting for Execution to Complete ==="
sleep 5 # Wait a bit for the workflow to complete (Lambdas take ~1-2s each)

echo ""
echo "=== Execution History ==="
aws --endpoint-url http://localhost:8000 --region us-east-1 stepfunctions get-execution-history \
  --execution-arn "$EXEC_ARN" \
  --query 'events[*].[type, stateEnteredEventDetails.name, stateExitedEventDetails.name, taskSucceededEventDetails.resource]' \
  --output table

echo ""
echo "=== Execution Result ==="
aws --endpoint-url http://localhost:8000 --region us-east-1 stepfunctions describe-execution \
  --execution-arn "$EXEC_ARN" \
  --query '{Status: status, Output: output}' \
  --output json
