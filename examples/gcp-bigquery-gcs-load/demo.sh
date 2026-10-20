#!/usr/bin/env bash
# Uploads sales.csv to Cloud Storage, loads it into BigQuery, runs an
# aggregate query and checks the result. Idempotent: the bucket/dataset are
# reused and the table is truncated on each load.
set -euo pipefail

cd "$(dirname "$0")"

GCS_URL="${GCS_URL:-http://localhost:4443}"
BIGQUERY_URL="${BIGQUERY_URL:-http://localhost:9050}"
export GCS_URL BIGQUERY_URL

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

wait_for() {
  local name="$1" url="$2" deadline=$((SECONDS + 30))
  echo "Waiting for ${name} (${url})..."
  until curl -sS --connect-timeout 1 --max-time 2 -o /dev/null "${url}/" 2>/dev/null; do
    ((SECONDS < deadline)) || fail "${name} not ready within 30s"
    sleep 0.5
  done
}

wait_for "Cloud Storage" "$GCS_URL"
wait_for "BigQuery" "$BIGQUERY_URL"

out="$(go run . sales.csv 2>&1)" || { echo "$out"; fail "app exited non-zero"; }
echo "$out"

expect() {
  grep -Eq -- "$1" <<<"$out" || fail "$2"
}

expect 'uploaded gs://sales-lake/in/sales.csv' "CSV was not uploaded"
expect 'sales.orders has 6 rows' "load job did not load 6 rows"
expect '^amer +20 +110\.00$' "amer aggregate wrong"
expect '^apac +25 +86\.00$' "apac aggregate wrong"
expect '^emea +14 +65\.00$' "emea aggregate wrong"

echo "PASS: gcp-bigquery-gcs-load (CSV in GCS loaded into BigQuery, aggregate verified)"
