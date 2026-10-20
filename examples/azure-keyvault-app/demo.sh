#!/usr/bin/env bash
# Runs the Key Vault app against gopherstack and checks its output.
# Idempotent: re-running adds a new secret/key version and passes again.
set -euo pipefail

cd "$(dirname "$0")"

KEYVAULT_URL="${KEYVAULT_URL:-https://localhost:10004/vaults/demo-vault}"
export KEYVAULT_URL

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

wait_for_port() {
  local hostport="${KEYVAULT_URL#*://}"
  hostport="${hostport%%/*}"
  local deadline=$((SECONDS + 30))
  echo "Waiting for Key Vault (${hostport})..."
  until curl -ksS --connect-timeout 1 --max-time 2 -o /dev/null "https://${hostport}/" 2>/dev/null; do
    ((SECONDS < deadline)) || fail "Key Vault at ${hostport} not ready within 30s"
    sleep 0.5
  done
}

wait_for_port

# Trust-on-first-use: pin the local emulator's presented certificate.
if [[ -z "${KEYVAULT_CA_FILE:-}" ]]; then
  hostport="${KEYVAULT_URL#*://}"
  hostport="${hostport%%/*}"
  ca_dir="$(mktemp -d)"
  trap 'rm -rf "$ca_dir"' EXIT
  KEYVAULT_CA_FILE="${ca_dir}/keyvault.pem"
  openssl s_client -connect "${hostport}" -servername localhost -showcerts </dev/null 2>/dev/null |
    openssl x509 >"${KEYVAULT_CA_FILE}" 2>/dev/null || true
fi
[[ -s "${KEYVAULT_CA_FILE}" ]] || fail "could not obtain the Key Vault certificate (set KEYVAULT_CA_FILE to a PEM file)"
export KEYVAULT_CA_FILE

out="$(go run . 2>&1)" || { echo "$out"; fail "app exited non-zero"; }
echo "$out"

check() {
  grep -q -- "$1" <<<"$out" || fail "$2"
}

check 'config secret: service=billing' "config secret was not read back"
check 'signed 32-byte digest' "payload was not signed"
check 'tampered payload rejected' "signature verification did not complete"

echo "PASS: azure-keyvault-app (secret read, payload signed, signature verified)"
