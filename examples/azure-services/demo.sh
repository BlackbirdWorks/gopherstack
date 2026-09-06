#!/usr/bin/env bash
# Cross-service Azure smoke demo against the gopherstack mock Azure API.
#
# Exercises Azure Blob Storage, Azure Queue Storage, Azure Table Storage, and
# Azure Cosmos DB (Core/SQL API) end-to-end via plain curl requests against
# gopherstack's Azurite-compatible / Cosmos-compatible listeners. No SDKs or
# CLI tools are required beyond curl -- auth verification is off by default
# for all four services (see docs/services/azureblob.md et al.), so no
# SharedKey signing or master-key HMAC is needed to talk to them locally.
set -euo pipefail

BLOB_URL="${BLOB_URL:-http://localhost:10000}"
QUEUE_URL="${QUEUE_URL:-http://localhost:10001}"
TABLE_URL="${TABLE_URL:-http://localhost:10002}"
COSMOS_URL="${COSMOS_URL:-http://localhost:8081}"
ACCOUNT="devstoreaccount1"

CONTAINER="demo-container"
BLOB_NAME="hello.txt"
QUEUE_NAME="demo-queue"
TABLE_NAME="demotable"
DB_NAME="demo-db"
COLL_NAME="demo-container"

curl_check() {
  # curl_check <description> <curl args...>
  # Runs curl, echoing the response, and fails the script (via set -e) with a
  # clear message if the HTTP status is not 2xx.
  local desc="$1"
  shift
  echo "--- ${desc} ---"
  local status body
  body=$(curl -sS -w '\n%{http_code}' "$@")
  status="${body##*$'\n'}"
  body="${body%$'\n'*}"
  echo "$body"
  if [[ "$status" -lt 200 || "$status" -ge 300 ]]; then
    echo "FAILED: ${desc} returned HTTP ${status}" >&2
    exit 1
  fi
}

echo "=== Azure Blob Storage (port 10000) ==="
curl_check "create container" -X PUT "${BLOB_URL}/${ACCOUNT}/${CONTAINER}?restype=container"
curl_check "put blob" -X PUT "${BLOB_URL}/${ACCOUNT}/${CONTAINER}/${BLOB_NAME}" \
  -H "x-ms-blob-type: BlockBlob" \
  --data-binary "hello from gopherstack"
curl_check "list blobs" "${BLOB_URL}/${ACCOUNT}/${CONTAINER}?restype=container&comp=list"
curl_check "get blob" "${BLOB_URL}/${ACCOUNT}/${CONTAINER}/${BLOB_NAME}"
curl_check "delete blob" -X DELETE "${BLOB_URL}/${ACCOUNT}/${CONTAINER}/${BLOB_NAME}"
curl_check "delete container" -X DELETE "${BLOB_URL}/${ACCOUNT}/${CONTAINER}?restype=container"

echo ""
echo "=== Azure Queue Storage (port 10001) ==="
curl_check "create queue" -X PUT "${QUEUE_URL}/${ACCOUNT}/${QUEUE_NAME}"
curl_check "put message" -X POST "${QUEUE_URL}/${ACCOUNT}/${QUEUE_NAME}/messages" \
  --data '<QueueMessage><MessageText>hello from gopherstack</MessageText></QueueMessage>'
MESSAGES=$(curl -sS "${QUEUE_URL}/${ACCOUNT}/${QUEUE_NAME}/messages?numofmessages=1")
echo "--- get messages ---"
echo "$MESSAGES"
MESSAGE_ID=$(echo "$MESSAGES" | grep -o '<MessageId>[^<]*' | head -1 | sed 's/<MessageId>//')
POP_RECEIPT=$(echo "$MESSAGES" | grep -o '<PopReceipt>[^<]*' | head -1 | sed 's/<PopReceipt>//')
if [[ -z "$MESSAGE_ID" || -z "$POP_RECEIPT" ]]; then
  echo "FAILED: could not parse MessageId/PopReceipt from GetMessages response" >&2
  exit 1
fi
curl_check "delete message" -X DELETE \
  "${QUEUE_URL}/${ACCOUNT}/${QUEUE_NAME}/messages/${MESSAGE_ID}?popreceipt=${POP_RECEIPT}"
curl_check "delete queue" -X DELETE "${QUEUE_URL}/${ACCOUNT}/${QUEUE_NAME}"

echo ""
echo "=== Azure Table Storage (port 10002) ==="
curl_check "create table" -X POST "${TABLE_URL}/${ACCOUNT}/Tables" \
  -H "Content-Type: application/json" \
  --data "{\"TableName\":\"${TABLE_NAME}\"}"
curl_check "insert entity" -X POST "${TABLE_URL}/${ACCOUNT}/${TABLE_NAME}" \
  -H "Content-Type: application/json" \
  --data '{"PartitionKey":"p1","RowKey":"r1","Name":"example"}'
curl_check "get entity" \
  "${TABLE_URL}/${ACCOUNT}/${TABLE_NAME}(PartitionKey='p1',RowKey='r1')"
curl_check "delete table" -X DELETE "${TABLE_URL}/${ACCOUNT}/Tables('${TABLE_NAME}')"

echo ""
echo "=== Azure Cosmos DB (port 8081) ==="
curl_check "database account root" "${COSMOS_URL}/"
curl_check "create database" -X POST "${COSMOS_URL}/dbs" \
  -H "Content-Type: application/json" \
  --data "{\"id\":\"${DB_NAME}\"}"
curl_check "create container" -X POST "${COSMOS_URL}/dbs/${DB_NAME}/colls" \
  -H "Content-Type: application/json" \
  --data "{\"id\":\"${COLL_NAME}\",\"partitionKey\":{\"paths\":[\"/pk\"],\"kind\":\"Hash\"}}"
curl_check "create document" -X POST "${COSMOS_URL}/dbs/${DB_NAME}/colls/${COLL_NAME}/docs" \
  -H "Content-Type: application/json" \
  --data '{"id":"doc1","pk":"partition1","name":"example"}'
curl_check "query document" -X POST "${COSMOS_URL}/dbs/${DB_NAME}/colls/${COLL_NAME}/docs" \
  -H "Content-Type: application/query+json" \
  -H "x-ms-documentdb-isquery: true" \
  --data '{"query":"SELECT * FROM c WHERE c.id = @id","parameters":[{"name":"@id","value":"doc1"}]}'
curl_check "delete database" -X DELETE "${COSMOS_URL}/dbs/${DB_NAME}"

echo ""
echo "=== All four Azure services exercised successfully ==="
