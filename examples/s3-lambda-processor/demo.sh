#!/bin/sh
set -eu

# We use the ENDPOINT provided by docker-compose, or localhost.
AWS="aws --endpoint-url ${ENDPOINT:-http://localhost:8000} --no-cli-pager --output json"
WORK="$(mktemp -d)"

cleanup() {
  rm -rf "$WORK"
}
trap cleanup EXIT

echo "=== Building Go zip Lambda ==="
cp -r /lambda/* "$WORK"/
( cd "$WORK" && go mod tidy && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -tags lambda.norpc -o bootstrap . )
( cd "$WORK" && zip -q function.zip bootstrap )
# Copy the compiled binary back to our workspace so Terraform can zip it
cp "$WORK/bootstrap" ./bootstrap

echo ""
echo "=== Initializing and Applying Terraform/Tofu ==="
# Determine if we have tofu or terraform
TF=""
if command -v tofu >/dev/null 2>&1; then
    TF="tofu"
elif command -v terraform >/dev/null 2>&1; then
    TF="terraform"
else
    echo "Neither tofu nor terraform found! Please install one to run this demo."
    exit 1
fi

$TF init
$TF apply -auto-approve -var="provider_endpoint=${ENDPOINT:-http://localhost:8000}" -var="lambda_endpoint=${LAMBDA_ENDPOINT:-http://localhost:8000}"

echo ""
echo "=== Uploading object to S3 ==="
$AWS s3api get-bucket-notification-configuration --bucket incoming-uploads

# Upload a file to the bucket. The endpoint handles s3:// directly, but we use path style.
echo "Hello from S3 Event Notifications!" > "$WORK/testfile.txt"
$AWS s3 cp "$WORK/testfile.txt" s3://incoming-uploads/testfile.txt

echo ""
echo "=== Waiting for Lambda execution ==="
# It can take a moment for the event to fire, the container to launch, and logs to flush.
# We will poll for up to 30 seconds.
SUCCESS=0
for i in $(seq 1 15); do
    sleep 2
    if $AWS logs filter-log-events --log-group-name /aws/lambda/s3-processor --query 'events[*].message' --output text 2>/dev/null | grep -q "Successfully read object!"; then
        SUCCESS=1
        break
    fi
done

sleep 15

echo "=== Lambda Container Logs ==="
# Find all containers matching gopherstack-lambda-s3-processor
for container in $(curl -s --unix-socket /var/run/docker.sock http://localhost/containers/json?all=1 | grep -o 'gopherstack-lambda-s3-processor-[^"]*'); do
    echo "Logs for $container:"
    curl -s --unix-socket /var/run/docker.sock "http://localhost/containers/$container/logs?stdout=1&stderr=1"
done

echo "=== Lambda Logs ==="
for i in {1..10}; do
    aws logs filter-log-events --log-group-name /aws/lambda/s3-processor --endpoint-url "$ENDPOINT" 2>/dev/null && break
    sleep 2
done

if ! aws logs filter-log-events --log-group-name /aws/lambda/s3-processor --endpoint-url "$ENDPOINT" | grep -q "Successfully read object"; then
    echo "FAIL: Could not find success message in Lambda logs."
    exit 1
fi

if [ "$SUCCESS" -eq 1 ]; then
    echo ""
    echo "SUCCESS: Lambda correctly processed the S3 event notification!"
else
    echo ""
    echo "FAIL: Could not find success message in Lambda logs."
    exit 1
fi
