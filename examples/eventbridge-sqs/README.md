# eventbridge-sqs

An EventBridge rule on the default bus delivers a matching event to an SQS queue.

## What it shows

1. `aws sqs create-queue` for `audit-events`.
2. `aws events put-rule` matching `source=com.example.orders` and `detail-type=OrderCreated`.
3. `aws events put-targets` with the queue ARN as target id `1`.
4. `aws events put-events` with one matching event.
5. `aws sqs receive-message` prints the delivered event and the script checks it.

## Prerequisites

- Docker, AWS CLI v2.

## Run

```bash
docker compose up -d --build
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_DEFAULT_REGION=us-east-1
./demo.sh
```

Set `ENDPOINT` to use a server other than `http://localhost:8000`.

## Expected output

Abridged. The CLI prints the JSON pretty-printed.

```
=== PutEvents ===
{ "FailedEntryCount": 0, "Entries": [ { "EventId": "..." } ] }
=== Draining target queue ===
{ "Messages": [ { "Body": "{\"detail-type\":\"OrderCreated\", ...}" } ] }
=== SUCCESS: EventBridge rule delivered event to SQS ===
```

The script exits non-zero if the message is missing. It deletes the rule, target and queue on exit.

## Clean up

```bash
docker compose down -v
```
