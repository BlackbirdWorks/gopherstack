#!/bin/sh
set -eu

echo "=== Installing tools (aws-cli, openssh-client, jq) ==="
apk add --no-cache aws-cli openssh-client jq >/dev/null 2>&1
echo "Done."

export AWS_ACCESS_KEY_ID=test
export AWS_SECRET_ACCESS_KEY=test
export AWS_DEFAULT_REGION=us-east-1
ENDPOINT="http://gopherstack:8000"
KEY_NAME="ec2-docker-demo"
KEY_FILE="/tmp/${KEY_NAME}.pem"
AMI_ID="ami-0abcdef1234567890" # standard Amazon Linux 2 AMI id (id is mocked, image used is amazonlinux:2)

cleanup() {
  if [ -n "${INSTANCE_ID:-}" ]; then
    echo ""
    echo "=== Tearing down instance ${INSTANCE_ID} ==="
    aws ec2 terminate-instances \
      --instance-ids "${INSTANCE_ID}" \
      --endpoint-url "$ENDPOINT" \
      --no-cli-pager >/dev/null || true
  fi

  echo "=== Removing key pair ${KEY_NAME} ==="
  aws ec2 delete-key-pair \
    --key-name "${KEY_NAME}" \
    --endpoint-url "$ENDPOINT" \
    --no-cli-pager >/dev/null || true

  rm -f "${KEY_FILE}"
}
trap cleanup EXIT

echo ""
echo "=== Creating key pair ${KEY_NAME} ==="
aws ec2 create-key-pair \
  --key-name "${KEY_NAME}" \
  --endpoint-url "$ENDPOINT" \
  --query 'KeyMaterial' \
  --output text \
  --no-cli-pager >"${KEY_FILE}"
chmod 600 "${KEY_FILE}"
echo "Saved private key to ${KEY_FILE}"

echo ""
echo "=== Launching EC2 instance (${AMI_ID}, t3.micro) ==="
INSTANCE_ID=$(aws ec2 run-instances \
  --image-id "${AMI_ID}" \
  --instance-type t3.micro \
  --key-name "${KEY_NAME}" \
  --endpoint-url "$ENDPOINT" \
  --query 'Instances[0].InstanceId' \
  --output text \
  --no-cli-pager)
echo "Launched ${INSTANCE_ID}"

echo ""
echo "=== Waiting for instance to become reachable ==="
PRIVATE_IP=""
for i in $(seq 1 60); do
  RESULT=$(aws ec2 describe-instances \
    --instance-ids "${INSTANCE_ID}" \
    --endpoint-url "$ENDPOINT" \
    --output json \
    --no-cli-pager)
  STATE=$(echo "$RESULT" | jq -r '.Reservations[0].Instances[0].State.Name')
  PRIVATE_IP=$(echo "$RESULT" | jq -r '.Reservations[0].Instances[0].PrivateIpAddress // empty')
  echo "  attempt ${i}: state=${STATE} private_ip=${PRIVATE_IP:-<pending>}"
  if [ "${STATE}" = "running" ] && [ -n "${PRIVATE_IP}" ]; then
    break
  fi
  sleep 1
done

if [ -z "${PRIVATE_IP}" ]; then
  echo "ERROR: instance never reported a private IP" >&2
  exit 1
fi

echo ""
echo "=== Waiting for sshd on ${PRIVATE_IP}:22 ==="
for i in $(seq 1 30); do
  if nc -z -w 1 "${PRIVATE_IP}" 22 2>/dev/null; then
    echo "sshd is up after ${i}s"
    break
  fi
  sleep 1
done

echo ""
echo "=== ssh ec2-user@${PRIVATE_IP} 'echo hello world from \$(cat /etc/hostname)' ==="
OUTPUT=$(ssh -i "${KEY_FILE}" \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o LogLevel=ERROR \
  -o ConnectTimeout=10 \
  "ec2-user@${PRIVATE_IP}" \
  'echo "hello world from $(cat /etc/hostname)"')
echo "${OUTPUT}"

case "${OUTPUT}" in
  "hello world from "*)
    echo ""
    echo "=== SUCCESS: round-tripped through the docker-backed EC2 instance ==="
    ;;
  *)
    echo "ERROR: unexpected ssh output: ${OUTPUT}" >&2
    exit 1
    ;;
esac
