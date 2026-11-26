# sns-sqs-fanout

One SNS publish is delivered to every SQS queue subscribed to the topic.

## What it shows

1. `aws sns create-topic` for `orders`.
2. `aws sqs create-queue` for `billing` and `shipping`.
3. `aws sns subscribe` for both queues.
4. `aws sns publish` with one JSON message.
5. `aws sqs receive-message` on both queues. Each gets the message.

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

```
TopicArn: arn:aws:sns:us-east-1:000000000000:orders
=== Receive from billing ===
{"Messages": [{"Body": "{\"Type\":\"Notification\", ... \"Message\":\"{\\\"orderId\\\":\\\"o-123\\\",\\\"total\\\":42.5}\" ...}"}]}
=== Receive from shipping ===
{"Messages": [{"Body": "{\"Type\":\"Notification\", ... }]}
=== SUCCESS: one publish fanned out to both queues ===
```

The CLI prints the JSON pretty-printed. The script exits non-zero if either queue is empty.

## Clean up

```bash
docker compose down -v
```
