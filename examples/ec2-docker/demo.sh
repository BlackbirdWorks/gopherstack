#!/usr/bin/env bash
# Run a full EC2 docker-compute round-trip against the gopherstack instance
# spun up by docker-compose.yml. **This script is intended to run on the
# host** (outside the compose stack) so it mirrors what a real frontend
# integration would do: hit the AWS API over HTTP, look up the instance
# hostname, then ssh into the launched container by that hostname.
set -euo pipefail

ENDPOINT="${ENDPOINT:-http://localhost:8000}"
DNS_ADDR="${DNS_ADDR:-127.0.0.1}"
DNS_PORT="${DNS_PORT:-10053}"
KEY_NAME="${KEY_NAME:-ec2-docker-demo}"
KEY_FILE="${KEY_FILE:-/tmp/${KEY_NAME}.pem}"
AMI_ID="${AMI_ID:-ami-0abcdef1234567890}"

export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"

for tool in aws jq dig ssh; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "ERROR: required tool '$tool' is not installed" >&2
    exit 1
  fi
done

INSTANCE_ID=""
cleanup() {
  if [ -n "${INSTANCE_ID}" ]; then
    echo
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

echo "=== Creating key pair ${KEY_NAME} ==="
aws ec2 create-key-pair \
  --key-name "${KEY_NAME}" \
  --endpoint-url "$ENDPOINT" \
  --query 'KeyMaterial' \
  --output text \
  --no-cli-pager >"${KEY_FILE}"
chmod 600 "${KEY_FILE}"
echo "Saved private key to ${KEY_FILE}"

echo
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

echo
echo "=== Waiting for instance to expose its SSH endpoint ==="
PUBLIC_DNS=""
SSH_HOST=""
SSH_PORT=""
for i in $(seq 1 60); do
  RESULT=$(aws ec2 describe-instances \
    --instance-ids "${INSTANCE_ID}" \
    --endpoint-url "$ENDPOINT" \
    --output json \
    --no-cli-pager)
  STATE=$(echo "$RESULT" | jq -r '.Reservations[0].Instances[0].State.Name')
  PUBLIC_DNS=$(echo "$RESULT" | jq -r '.Reservations[0].Instances[0].PublicDnsName // empty')
  SSH_HOST=$(echo "$RESULT" | jq -r '.Reservations[0].Instances[0].Tags[]? | select(.Key=="gopherstack:ssh-host") | .Value')
  SSH_PORT=$(echo "$RESULT" | jq -r '.Reservations[0].Instances[0].Tags[]? | select(.Key=="gopherstack:ssh-port") | .Value')
  echo "  attempt ${i}: state=${STATE} dns=${PUBLIC_DNS:-<pending>} host=${SSH_HOST:-<pending>} port=${SSH_PORT:-<pending>}"
  if [ "${STATE}" = "running" ] && [ -n "${PUBLIC_DNS}" ] && [ -n "${SSH_PORT}" ]; then
    break
  fi
  sleep 1
done

if [ -z "${PUBLIC_DNS}" ] || [ -z "${SSH_PORT}" ]; then
  echo "ERROR: instance never reported a hostname / SSH port" >&2
  exit 1
fi

echo
echo "=== Resolving ${PUBLIC_DNS} via the embedded DNS (${DNS_ADDR}:${DNS_PORT}) ==="
RESOLVED=$(dig "@${DNS_ADDR}" -p "${DNS_PORT}" +short "${PUBLIC_DNS}" | head -n1)
if [ -z "${RESOLVED}" ]; then
  echo "ERROR: embedded DNS did not return an address for ${PUBLIC_DNS}" >&2
  exit 1
fi
echo "${PUBLIC_DNS} -> ${RESOLVED}"

echo
echo "=== Waiting for sshd on ${RESOLVED}:${SSH_PORT} ==="
for i in $(seq 1 30); do
  if (echo > "/dev/tcp/${RESOLVED}/${SSH_PORT}") >/dev/null 2>&1; then
    echo "sshd is up after ${i}s"
    break
  fi
  sleep 1
done

echo
echo "=== ssh -p ${SSH_PORT} ec2-user@${RESOLVED} (hostname=${PUBLIC_DNS}) ==="
OUTPUT=$(ssh -i "${KEY_FILE}" \
  -o StrictHostKeyChecking=no \
  -o UserKnownHostsFile=/dev/null \
  -o LogLevel=ERROR \
  -o ConnectTimeout=10 \
  -p "${SSH_PORT}" \
  "ec2-user@${RESOLVED}" \
  'echo "hello world from $(cat /etc/hostname)"')
echo "${OUTPUT}"

case "${OUTPUT}" in
  "hello world from "*)
    echo
    echo "=== SUCCESS: round-tripped through the docker-backed EC2 instance ==="
    ;;
  *)
    echo "ERROR: unexpected ssh output: ${OUTPUT}" >&2
    exit 1
    ;;
esac
