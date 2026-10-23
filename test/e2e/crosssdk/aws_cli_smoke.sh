#!/usr/bin/env bash
# AWS CLI cross-SDK compatibility suite for gopherstack.
# Env: AWS_ENDPOINT_URL (default http://localhost:8000). Prints PASS/FAIL per case; exits non-zero on any failure.
set -euo pipefail

export AWS_ENDPOINT_URL="${AWS_ENDPOINT_URL:-http://localhost:8000}"
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER="" AWS_MAX_ATTEMPTS=1
EP=(--endpoint-url "$AWS_ENDPOINT_URL")
RUN=$(od -An -N4 -tx1 /dev/urandom | tr -d ' \n')
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

aws_() { aws "${EP[@]}" "$@"; }
eq() { [[ "$1" == "$2" ]] || { echo "expected [$2] got [$1]" >&2; return 1; }; }

s3_cp_sync_ls_rm() {
  local b="sdk-cli-$RUN-s3"
  aws_ s3 mb "s3://$b" >/dev/null
  echo "hello" >"$WORK/a.txt"
  aws_ s3 cp "$WORK/a.txt" "s3://$b/dir/a.txt" >/dev/null
  mkdir -p "$WORK/src/sub" "$WORK/dst"
  echo one >"$WORK/src/1.txt"; echo two >"$WORK/src/sub/2.txt"
  aws_ s3 sync "$WORK/src" "s3://$b/synced" >/dev/null
  eq "$(aws_ s3 ls "s3://$b/synced/" --recursive | wc -l | tr -d ' ')" 2
  aws_ s3 sync "s3://$b/synced" "$WORK/dst" >/dev/null
  eq "$(cat "$WORK/dst/sub/2.txt")" two
  aws_ s3 cp "s3://$b/dir/a.txt" "$WORK/back.txt" >/dev/null
  eq "$(cat "$WORK/back.txt")" hello
  aws_ s3 ls "s3://$b" | grep -q 'PRE dir/'
  aws_ s3 rm "s3://$b" --recursive >/dev/null
  eq "$(aws_ s3 ls "s3://$b" | wc -l | tr -d ' ')" 0
  aws_ s3 rb "s3://$b" >/dev/null
}

s3_multipart_cp() {
  local b="sdk-cli-$RUN-mpu"
  aws_ s3 mb "s3://$b" >/dev/null
  head -c $((20 * 1024 * 1024)) /dev/urandom >"$WORK/big.bin"
  aws_ s3 cp "$WORK/big.bin" "s3://$b/big.bin" >/dev/null
  aws_ s3 cp "s3://$b/big.bin" "$WORK/big.out" >/dev/null
  cmp "$WORK/big.bin" "$WORK/big.out"
  aws_ s3 rm "s3://$b/big.bin" >/dev/null
  aws_ s3 rb "s3://$b" >/dev/null
}

s3api_ops() {
  local b="sdk-cli-$RUN-api"
  aws_ s3api create-bucket --bucket "$b" >/dev/null
  echo body >"$WORK/o.txt"
  aws_ s3api put-object --bucket "$b" --key k1 --body "$WORK/o.txt" --metadata mk=mv --content-type text/plain >/dev/null
  local head
  head=$(aws_ s3api head-object --bucket "$b" --key k1 --output json)
  eq "$(jq -r .ContentType <<<"$head")" text/plain
  eq "$(jq -r .Metadata.mk <<<"$head")" mv
  aws_ s3api put-object-tagging --bucket "$b" --key k1 --tagging 'TagSet=[{Key=a,Value=b}]' >/dev/null
  eq "$(aws_ s3api get-object-tagging --bucket "$b" --key k1 --output json | jq -r '.TagSet[0].Key')" a
  for i in 1 2 3; do aws_ s3api put-object --bucket "$b" --key "p$i" --body "$WORK/o.txt" >/dev/null; done
  eq "$(aws_ s3api list-objects-v2 --bucket "$b" --prefix p --output json | jq '.Contents | length')" 3
  eq "$(aws_ s3api list-objects-v2 --bucket "$b" --prefix p --max-items 2 --output json | jq '.Contents | length')" 2
  aws_ s3api delete-objects --bucket "$b" --delete 'Objects=[{Key=k1},{Key=p1},{Key=p2},{Key=p3}]' >/dev/null
  eq "$(aws_ s3api list-objects-v2 --bucket "$b" --output json | jq '(.Contents // []) | length')" 0
  aws_ s3api delete-bucket --bucket "$b" >/dev/null
}

dynamodb_put_get_query() {
  local t="sdk-cli-$RUN-ddb"
  aws_ dynamodb create-table --table-name "$t" \
    --attribute-definitions AttributeName=pk,AttributeType=S AttributeName=sk,AttributeType=N \
    --key-schema AttributeName=pk,KeyType=HASH AttributeName=sk,KeyType=RANGE \
    --billing-mode PAY_PER_REQUEST --output json >/dev/null
  aws_ dynamodb wait table-exists --table-name "$t"
  for i in 1 2 3; do
    aws_ dynamodb put-item --table-name "$t" --item "{\"pk\":{\"S\":\"u\"},\"sk\":{\"N\":\"$i\"},\"v\":{\"S\":\"v$i\"}}" >/dev/null
  done
  eq "$(aws_ dynamodb get-item --table-name "$t" --key '{"pk":{"S":"u"},"sk":{"N":"2"}}' --output json | jq -r .Item.v.S)" v2
  local q
  q=$(aws_ dynamodb query --table-name "$t" --key-condition-expression 'pk = :p AND sk > :s' \
    --expression-attribute-values '{":p":{"S":"u"},":s":{"N":"1"}}' --output json)
  eq "$(jq .Count <<<"$q")" 2
  eq "$(aws_ dynamodb scan --table-name "$t" --output json | jq .Count)" 3
  eq "$(aws_ dynamodb scan --table-name "$t" --max-items 2 --output json | jq '.Items | length')" 2
  aws_ dynamodb delete-table --table-name "$t" >/dev/null
}

sqs_send_receive() {
  local u
  u=$(aws_ sqs create-queue --queue-name "sdk-cli-$RUN-q" --output json | jq -r .QueueUrl)
  aws_ sqs send-message --queue-url "$u" --message-body hi >/dev/null
  local r
  r=$(aws_ sqs receive-message --queue-url "$u" --wait-time-seconds 2 --output json)
  eq "$(jq -r '.Messages[0].Body' <<<"$r")" hi
  aws_ sqs delete-message --queue-url "$u" --receipt-handle "$(jq -r '.Messages[0].ReceiptHandle' <<<"$r")" >/dev/null
  aws_ sqs delete-queue --queue-url "$u" >/dev/null
}

sns_to_sqs() {
  local arn u qarn
  arn=$(aws_ sns create-topic --name "sdk-cli-$RUN-t" --output json | jq -r .TopicArn)
  u=$(aws_ sqs create-queue --queue-name "sdk-cli-$RUN-sub" --output json | jq -r .QueueUrl)
  qarn=$(aws_ sqs get-queue-attributes --queue-url "$u" --attribute-names QueueArn --output json | jq -r .Attributes.QueueArn)
  aws_ sns subscribe --topic-arn "$arn" --protocol sqs --notification-endpoint "$qarn" >/dev/null
  aws_ sns publish --topic-arn "$arn" --message fanout >/dev/null
  local r
  r=$(aws_ sqs receive-message --queue-url "$u" --wait-time-seconds 3 --output json)
  eq "$(jq -r '.Messages[0].Body | fromjson | .Message' <<<"$r")" fanout
  aws_ sqs delete-queue --queue-url "$u" >/dev/null
  aws_ sns delete-topic --topic-arn "$arn" >/dev/null
}

sts_get_caller_identity() {
  local r
  r=$(aws_ sts get-caller-identity --output json)
  [[ "$(jq -r .Account <<<"$r")" =~ ^[0-9]{12}$ ]]
  [[ "$(jq -r .Arn <<<"$r")" == arn:aws:* ]]
}

cloudformation_deploy_describe_delete() {
  local stack="sdk-cli-$RUN-stack"
  cat >"$WORK/t.yaml" <<YAML
AWSTemplateFormatVersion: '2010-09-09'
Resources:
  Queue:
    Type: AWS::SQS::Queue
    Properties:
      QueueName: sdk-cli-$RUN-cfn-q
  Topic:
    Type: AWS::SNS::Topic
    Properties:
      TopicName: sdk-cli-$RUN-cfn-t
Outputs:
  QueueUrl:
    Value: !Ref Queue
YAML
  aws_ cloudformation deploy --stack-name "$stack" --template-file "$WORK/t.yaml" --no-fail-on-empty-changeset >/dev/null
  local d
  d=$(aws_ cloudformation describe-stacks --stack-name "$stack" --output json)
  eq "$(jq -r '.Stacks[0].StackStatus' <<<"$d")" CREATE_COMPLETE
  eq "$(jq -r '.Stacks[0].Outputs[0].OutputKey' <<<"$d")" QueueUrl
  aws_ sqs get-queue-url --queue-name "sdk-cli-$RUN-cfn-q" >/dev/null
  aws_ sns list-topics --output json | jq -e --arg n "sdk-cli-$RUN-cfn-t" '.Topics | map(.TopicArn) | any(endswith($n))' >/dev/null
  aws_ cloudformation delete-stack --stack-name "$stack"
  aws_ cloudformation wait stack-delete-complete --stack-name "$stack"
  if aws_ sqs get-queue-url --queue-name "sdk-cli-$RUN-cfn-q" >/dev/null 2>&1; then
    echo "stack queue should be deleted with the stack" >&2
    return 1
  fi
}

# Each case runs in its own process: set -e is ignored inside a command
# substitution used as an if condition, which would mask mid-function failures.
if [[ "${1:-}" == "--run" ]]; then
  "$2"
  exit 0
fi

failed=0
total=0
for fn in s3_cp_sync_ls_rm s3_multipart_cp s3api_ops dynamodb_put_get_query sqs_send_receive sns_to_sqs sts_get_caller_identity cloudformation_deploy_describe_delete; do
  if [[ $# -gt 0 && " $* " != *" $fn "* ]]; then continue; fi
  total=$((total + 1))
  if out=$("$0" --run "$fn" 2>&1); then
    echo "PASS $fn"
  else
    failed=$((failed + 1))
    echo "FAIL $fn"
    sed 's/^/    /' <<<"$out" | tail -15
  fi
done
echo "cli: $((total - failed)) ok, $failed failed"
[[ $failed -eq 0 ]]
