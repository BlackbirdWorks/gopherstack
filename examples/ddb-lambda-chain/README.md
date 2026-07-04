# DynamoDB → Lambda → DynamoDB chain

A Go **zip** Lambda (the `provided.al2` custom runtime) driven by **DynamoDB
Streams**, chaining three DynamoDB events end to end against the gopherstack
mock AWS API.

This mirrors the canonical AWS pattern
([Serverless DynamoDB + Lambda](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/example_serverless_DynamoDB_Lambda_section.html))
— a DynamoDB stream invoking a Lambda — but wires it into a small **chain** so
you can watch one event cascade through three tables.

## Flow

```
put item ─▶ chain-1 ──stream──▶ λ chain-fn ──put──▶ chain-2 ──stream──▶ λ chain-fn ──put──▶ chain-3
   (hop 1)                       (hop 1→2)                                (hop 2→3, terminal)
```

One `PutItem` into `chain-1` produces **3 DynamoDB events**: the seed insert,
then two stream-driven Lambda copies. The Lambda decides the next table from
the stream ARN that triggered it (`chain-1 → chain-2 → chain-3`); `chain-3` has
no successor, so the chain stops.

## What it exercises (real plumbing, not isolated mocks)

- **DynamoDB Streams** with `NEW_IMAGE` view type
- **Lambda event source mappings** (stream → function) with batching
- **Go zip Lambda on real Docker** (`provided.al2`, `bootstrap` entrypoint)
- The Lambda calling **DynamoDB** back through the gopherstack endpoint
  (`AWS_ENDPOINT_URL`) — full round trip

## Run

```sh
docker compose up --build --abort-on-container-exit
```

Expected tail:

```
=== Result: contents of each table ===
--- chain-1 ---  [{ id: order-42, hop: 1, from: seed }]
--- chain-2 ---  [{ id: order-42, hop: 2, from: chain-1 }]
--- chain-3 ---  [{ id: order-42, hop: 3, from: chain-2 }]

SUCCESS: event chained chain-1 -> chain-2 -> chain-3 (3 DynamoDB events).
```

## Files

| file                | purpose                                                        |
|---------------------|---------------------------------------------------------------|
| `lambda/main.go`    | Go handler (`events.DynamoDBEvent` → `PutItem` next table)    |
| `lambda/go.mod`     | `aws-lambda-go` + `aws-sdk-go-v2` deps                         |
| `demo.sh`           | build+zip Lambda, create tables/streams/ESMs, seed, verify    |
| `docker-compose.yml`| gopherstack (with Docker socket) + go/aws-cli demo runner     |

## Notes

- gopherstack mounts the host Docker socket so it can launch the Lambda
  container; the demo image installs `awscli` + `zip` and builds the Go binary.
- The handler only reacts to `INSERT` events, so it never loops on its own
  writes beyond the intended chain.
