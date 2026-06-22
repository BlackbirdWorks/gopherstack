#!/bin/bash
set -e
export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1

examples=(
  "elasticache-valkey"
  "eventbridge-sqs"
  "kinesis-lambda-aggregator"
  "s3-access-logs"
  "s3-lambda-processor"
  "sns-sqs-fanout"
  "stepfunctions-order-workflow"
)

for ex in "${examples[@]}"; do
  echo "========================================="
  echo "Testing $ex"
  echo "========================================="
  
  cd "examples/$ex"
  
  docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
  
  echo "Starting gopherstack..."
  docker-compose up -d --build
  
  echo "Running demo.sh..."
  if ! ./demo.sh; then
    echo "FAILED: $ex"
    docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
    exit 1
  fi
  
  echo "Tearing down gopherstack..."
  docker-compose down -v --remove-orphans >/dev/null 2>&1 || true
  
  cd ../..
  
  echo "SUCCESS: $ex"
  echo ""
done

echo "ALL EXAMPLES PASSED!"
