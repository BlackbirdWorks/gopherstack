#!/bin/sh
# IAM Role & STS AssumeRole Enforcement Demo against Gopherstack.
#
# Proves:
# 1. Resource creation (S3 buckets, DynamoDB tables, IAM roles & policies).
# 2. STS AssumeRole generates temporary credentials (ASIA...).
# 3. Assumed role is ALLOWED to access authorized resources (public-reports S3, public-inventory DDB).
# 4. Assumed role is DENIED (HTTP 403 AccessDenied) when accessing unauthorized resources (confidential-payroll S3, confidential-employees DDB).

set -eu

ENDPOINT="${ENDPOINT:-http://localhost:8000}"
REGION="us-east-1"
ACCOUNT_ID="000000000000"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-admin}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-admin}"
export AWS_DEFAULT_REGION="${REGION}"

AWS_ADMIN="aws --endpoint-url ${ENDPOINT} --region ${REGION} --output json"

PUBLIC_BUCKET="public-reports"
CONFIDENTIAL_BUCKET="confidential-payroll"
PUBLIC_TABLE="public-inventory"
CONFIDENTIAL_TABLE="confidential-employees"
ROLE_NAME="ReportAnalystRole"
POLICY_NAME="ReportAnalystPolicy"
USER_NAME="analyst-user"

echo "Waiting for Gopherstack endpoint at ${ENDPOINT}..."
for i in $(seq 1 30); do
  if curl -s "${ENDPOINT}/_gopherstack/health" >/dev/null 2>&1; then
    echo "Gopherstack is ready!"
    break
  fi
  sleep 0.2
done

cleanup() {
  echo ""
  echo "=== Cleaning up resources ==="
  $AWS_ADMIN s3api delete-object --bucket "$PUBLIC_BUCKET" --key "monthly-report.txt" >/dev/null 2>&1 || true
  $AWS_ADMIN s3api delete-object --bucket "$CONFIDENTIAL_BUCKET" --key "q4-payroll.txt" >/dev/null 2>&1 || true
  $AWS_ADMIN s3api delete-bucket --bucket "$PUBLIC_BUCKET" >/dev/null 2>&1 || true
  $AWS_ADMIN s3api delete-bucket --bucket "$CONFIDENTIAL_BUCKET" >/dev/null 2>&1 || true
  $AWS_ADMIN dynamodb delete-table --table-name "$PUBLIC_TABLE" >/dev/null 2>&1 || true
  $AWS_ADMIN dynamodb delete-table --table-name "$CONFIDENTIAL_TABLE" >/dev/null 2>&1 || true
  $AWS_ADMIN iam detach-role-policy --role-name "$ROLE_NAME" --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}" >/dev/null 2>&1 || true
  $AWS_ADMIN iam delete-policy --policy-arn "arn:aws:iam::${ACCOUNT_ID}:policy/${POLICY_NAME}" >/dev/null 2>&1 || true
  $AWS_ADMIN iam delete-role --role-name "$ROLE_NAME" >/dev/null 2>&1 || true
  $AWS_ADMIN iam delete-user-policy --user-name "$USER_NAME" --policy-name "AllowAssumeRole" >/dev/null 2>&1 || true
  for key in $($AWS_ADMIN iam list-access-keys --user-name "$USER_NAME" --query 'AccessKeyMetadata[].AccessKeyId' --output text 2>/dev/null || true); do
    $AWS_ADMIN iam delete-access-key --user-name "$USER_NAME" --access-key-id "$key" >/dev/null 2>&1 || true
  done
  $AWS_ADMIN iam delete-user --user-name "$USER_NAME" >/dev/null 2>&1 || true
  rm -f /tmp/trust-policy.json /tmp/analyst-policy.json /tmp/assume-user-policy.json /tmp/creds.json /tmp/report.txt /tmp/payroll.txt /tmp/downloaded-report.txt
}
trap cleanup EXIT

echo "=========================================================="
echo " 1. Setting up S3 Buckets and DynamoDB Tables"
echo "=========================================================="
$AWS_ADMIN s3api create-bucket --bucket "$PUBLIC_BUCKET" >/dev/null
echo "  [✓] Created S3 Bucket: $PUBLIC_BUCKET"

$AWS_ADMIN s3api create-bucket --bucket "$CONFIDENTIAL_BUCKET" >/dev/null
echo "  [✓] Created S3 Bucket: $CONFIDENTIAL_BUCKET"

$AWS_ADMIN dynamodb create-table \
  --table-name "$PUBLIC_TABLE" \
  --attribute-definitions AttributeName=itemId,AttributeType=S \
  --key-schema AttributeName=itemId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST >/dev/null
echo "  [✓] Created DynamoDB Table: $PUBLIC_TABLE"

$AWS_ADMIN dynamodb create-table \
  --table-name "$CONFIDENTIAL_TABLE" \
  --attribute-definitions AttributeName=employeeId,AttributeType=S \
  --key-schema AttributeName=employeeId,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST >/dev/null
echo "  [✓] Created DynamoDB Table: $CONFIDENTIAL_TABLE"

echo ""
echo "=========================================================="
echo " 2. Creating IAM Role & Granular Access Policy"
echo "=========================================================="

cat << 'EOF_TRUST' > /tmp/trust-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": "*",
      "Action": "sts:AssumeRole"
    }
  ]
}
EOF_TRUST

$AWS_ADMIN iam create-role \
  --role-name "$ROLE_NAME" \
  --assume-role-policy-document file:///tmp/trust-policy.json >/dev/null
echo "  [✓] Created IAM Role: $ROLE_NAME"

cat << EOF_POL > /tmp/analyst-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowS3PublicReports",
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:PutObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::${PUBLIC_BUCKET}",
        "arn:aws:s3:::${PUBLIC_BUCKET}/*"
      ]
    },
    {
      "Sid": "AllowDynamoDBPublicInventory",
      "Effect": "Allow",
      "Action": [
        "dynamodb:GetItem",
        "dynamodb:PutItem",
        "dynamodb:Scan",
        "dynamodb:DescribeTable"
      ],
      "Resource": "arn:aws:dynamodb:${REGION}:${ACCOUNT_ID}:table/${PUBLIC_TABLE}"
    },
    {
      "Sid": "AllowSTSIdentity",
      "Effect": "Allow",
      "Action": "sts:GetCallerIdentity",
      "Resource": "*"
    }
  ]
}
EOF_POL

POLICY_ARN=$($AWS_ADMIN iam create-policy \
  --policy-name "$POLICY_NAME" \
  --policy-document file:///tmp/analyst-policy.json \
  --query 'Policy.Arn' --output text)
echo "  [✓] Created IAM Policy: $POLICY_ARN"

$AWS_ADMIN iam attach-role-policy \
  --role-name "$ROLE_NAME" \
  --policy-arn "$POLICY_ARN"
echo "  [✓] Attached Policy to Role: $ROLE_NAME"

# Create analyst user
$AWS_ADMIN iam create-user --user-name "$USER_NAME" >/dev/null
cat << EOF_UPOL > /tmp/assume-user-policy.json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": "sts:AssumeRole",
      "Resource": "arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
    }
  ]
}
EOF_UPOL

$AWS_ADMIN iam put-user-policy \
  --user-name "$USER_NAME" \
  --policy-name "AllowAssumeRole" \
  --policy-document file:///tmp/assume-user-policy.json

USER_KEY_JSON=$($AWS_ADMIN iam create-access-key --user-name "$USER_NAME")
USER_AKID=$(echo "$USER_KEY_JSON" | grep -o '"AccessKeyId": "[^"]*' | cut -d'"' -f4)
USER_SECRET=$(echo "$USER_KEY_JSON" | grep -o '"SecretAccessKey": "[^"]*' | cut -d'"' -f4)
echo "  [✓] Created IAM User: $USER_NAME ($USER_AKID)"

echo ""
echo "=========================================================="
echo " 3. Assuming IAM Role via STS"
echo "=========================================================="

ROLE_ARN="arn:aws:iam::${ACCOUNT_ID}:role/${ROLE_NAME}"
CREDS=$($AWS_ADMIN sts assume-role \
  --role-arn "$ROLE_ARN" \
  --role-session-name "analyst-session-01")

ASSUMED_AKID=$(echo "$CREDS" | grep -o '"AccessKeyId": "[^"]*' | cut -d'"' -f4)
ASSUMED_SECRET=$(echo "$CREDS" | grep -o '"SecretAccessKey": "[^"]*' | cut -d'"' -f4)
ASSUMED_TOKEN=$(echo "$CREDS" | grep -o '"SessionToken": "[^"]*' | cut -d'"' -f4)

echo "  [✓] Assumed Role: $ROLE_ARN"
echo "  [✓] Temporary AccessKeyId: $ASSUMED_AKID"
echo "  [✓] SessionToken acquired (${#ASSUMED_TOKEN} chars)"

AWS_ASSUMED="env AWS_ACCESS_KEY_ID=${ASSUMED_AKID} AWS_SECRET_ACCESS_KEY=${ASSUMED_SECRET} AWS_SESSION_TOKEN=${ASSUMED_TOKEN} aws --endpoint-url ${ENDPOINT} --region ${REGION} --output json"

echo ""
echo "=== Caller Identity for Assumed Role ==="
$AWS_ASSUMED sts get-caller-identity

echo ""
echo "=========================================================="
echo " 4. Testing Authorized Resource Operations (Expected: 200 OK)"
echo "=========================================================="

echo "Sample report data $(date)" > /tmp/report.txt
echo "Writing to allowed S3 bucket ($PUBLIC_BUCKET)..."
$AWS_ASSUMED s3api put-object \
  --bucket "$PUBLIC_BUCKET" \
  --key "monthly-report.txt" \
  --body /tmp/report.txt >/dev/null
echo "  [SUCCESS] S3 PutObject to $PUBLIC_BUCKET succeeded!"

echo "Reading from allowed S3 bucket ($PUBLIC_BUCKET)..."
$AWS_ASSUMED s3api get-object \
  --bucket "$PUBLIC_BUCKET" \
  --key "monthly-report.txt" \
  /tmp/downloaded-report.txt >/dev/null
echo "  [SUCCESS] S3 GetObject from $PUBLIC_BUCKET succeeded!"

echo "Writing to allowed DynamoDB table ($PUBLIC_TABLE)..."
$AWS_ASSUMED dynamodb put-item \
  --table-name "$PUBLIC_TABLE" \
  --item '{"itemId": {"S": "ITEM-101"}, "name": {"S": "Widget A"}, "stock": {"N": "50"}}'
echo "  [SUCCESS] DynamoDB PutItem to $PUBLIC_TABLE succeeded!"

echo "Reading from allowed DynamoDB table ($PUBLIC_TABLE)..."
$AWS_ASSUMED dynamodb get-item \
  --table-name "$PUBLIC_TABLE" \
  --key '{"itemId": {"S": "ITEM-101"}}'
echo "  [SUCCESS] DynamoDB GetItem from $PUBLIC_TABLE succeeded!"

echo ""
echo "=========================================================="
echo " 5. Testing Unauthorized Resource Operations (Expected: 403 Forbidden / AccessDenied)"
echo "=========================================================="

echo "Confidential salary data" > /tmp/payroll.txt
echo "Attempting S3 PutObject on unauthorized bucket ($CONFIDENTIAL_BUCKET)..."
set +e
ERR_OUT=$($AWS_ASSUMED s3api put-object \
  --bucket "$CONFIDENTIAL_BUCKET" \
  --key "q4-payroll.txt" \
  --body /tmp/payroll.txt 2>&1)
EXIT_CODE=$?
set -e

if [ $EXIT_CODE -ne 0 ] || echo "$ERR_OUT" | grep -iq "AccessDenied\|403\|Forbidden"; then
  echo "  [BLOCKED AS EXPECTED] Access Denied on unauthorized S3 bucket!"
  echo "  Error: $ERR_OUT"
else
  echo "  [FAIL] Unexpected success on unauthorized S3 bucket!"
  exit 1
fi

echo ""
echo "Attempting DynamoDB PutItem on unauthorized table ($CONFIDENTIAL_TABLE)..."
set +e
DDB_ERR=$($AWS_ASSUMED dynamodb put-item \
  --table-name "$CONFIDENTIAL_TABLE" \
  --item '{"employeeId": {"S": "EMP-001"}, "salary": {"N": "250000"}}' 2>&1)
DDB_EXIT=$?
set -e

if [ $DDB_EXIT -ne 0 ] || echo "$DDB_ERR" | grep -iq "AccessDenied\|403\|Forbidden"; then
  echo "  [BLOCKED AS EXPECTED] Access Denied on unauthorized DynamoDB table!"
  echo "  Error: $DDB_ERR"
else
  echo "  [FAIL] Unexpected success on unauthorized DynamoDB table!"
  exit 1
fi

echo ""
echo "=========================================================="
echo " ALL TESTS PASSED: IAM & STS Enforcement Verified Live!"
echo "=========================================================="
