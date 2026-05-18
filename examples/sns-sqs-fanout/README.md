# SNS → SQS fan-out demo

Demonstrates the real fan-out plumbing inside gopherstack: an SNS publish is
delivered to every SQS queue subscribed to the topic, by the SNS publish
emitter that `cli.go` wires to the SQS backend at startup.

This is the same wiring AWS uses; the only difference is the storage is
in-memory.

## Architecture

```
┌──────────────┐      publish        ┌────────────┐     fan-out    ┌──────────┐
│ aws-cli demo │ ─────────────────► │  gopherstack │ ─────────────► │ billing  │
└──────────────┘   SNS topic        │   :8000      │   SQS queue   └──────────┘
                                    │              │                ┌──────────┐
                                    │              │ ─────────────► │ shipping │
                                    └────────────┘                  └──────────┘
```

## What the demo does

1. `aws sns create-topic --name orders`
2. `aws sqs create-queue` for `billing` and `shipping`
3. `aws sns subscribe` both queues to the topic
4. `aws sns publish` one JSON message
5. `aws sqs receive-message` from both queues — the message arrives at each

## Run it

```sh
cd examples/sns-sqs-fanout
docker compose up --build --abort-on-container-exit
docker compose down --remove-orphans
```

Expected (truncated):

```
TopicArn: arn:aws:sns:us-east-1:000000000000:orders
=== Receive from billing ===
{"Messages": [{"Body": "{\"Type\":\"Notification\", ... \"Message\":\"{\\\"orderId\\\":\\\"o-123\\\",\\\"total\\\":42.5}\" ...}"}]}
=== Receive from shipping ===
{"Messages": [{"Body": "{\"Type\":\"Notification\", ... }]}
=== SUCCESS: one publish fanned out to both queues ===
```
