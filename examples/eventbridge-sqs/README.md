# EventBridge rule → SQS target demo

Demonstrates the real EventBridge delivery plumbing inside gopherstack:
an `aws events put-events` call matches a rule on the default bus, and the
rule's SQS queue target receives the event — exactly as on real AWS, just
backed by in-memory storage. The cross-service wiring is set up in
`cli.go` via `DeliveryTargets`.

## Architecture

```
┌──────────────┐   put-events    ┌─────────────────────┐
│ aws-cli demo │ ──────────────► │ EventBridge default │
└──────────────┘                 │ bus + rule          │
                                 └─────────┬───────────┘
                                           │ matches event-pattern
                                           ▼
                                 ┌─────────────────────┐
                                 │ SQS queue           │
                                 │ audit-events        │
                                 └─────────────────────┘
```

## What the demo does

1. `aws sqs create-queue --queue-name audit-events`
2. `aws events put-rule` with an event pattern matching
   `source=com.example.orders` and `detail-type=OrderCreated`.
3. `aws events put-targets` attaching the SQS queue ARN as target id `1`.
4. `aws events put-events` firing one matching event.
5. `aws sqs receive-message` and prints the delivered event JSON envelope.

## Run it

```sh
cd examples/eventbridge-sqs
docker compose up --build --abort-on-container-exit
docker compose down --remove-orphans
```

Expected (truncated):

```
=== PutEvents ===
{ "FailedEntryCount": 0, "Entries": [ { "EventId": "..." } ] }
=== Draining target queue ===
{ "Messages": [ { "Body": "{\"detail-type\":\"OrderCreated\", ...}" } ] }
=== SUCCESS: EventBridge rule delivered event to SQS ===
```
