# azure-servicebus-cosmos-worker

A Go worker moves orders from a Service Bus queue into Cosmos DB. A second command reads them back from the change feed.

## What it shows

1. `demo.sh` sends three JSON orders to the queue `orders`.
2. `worker` peek-locks each message, upserts it into Cosmos DB (partition key `/customer`) with the `azcosmos` SDK, then completes it. Malformed messages are abandoned so Service Bus redelivers them and eventually dead-letters them.
3. `feed` reads the Cosmos DB change feed from the beginning with `ReadChangeFeed` and prints each document.

| Service | Port |
|---------|------|
| Service Bus (REST) | `10003` |
| Cosmos DB | `8081` |

Service Bus calls use the Brokered Messaging REST API over `net/http`, not the `azservicebus` SDK. Gopherstack also serves AMQP 1.0 on `5672` (plain) and `5671` (TLS), but the compose file publishes neither. Cosmos DB uses the public emulator master key.

## Prerequisites

- Docker, Go, `curl`.

## Run

```bash
docker compose up -d --build
./demo.sh
```

Without Docker, run `go run .` in the repo root instead of the first command. Override endpoints with `SERVICEBUS_URL` and `COSMOS_URL`.

## Expected output

```
Sent 3 orders to queue orders
stored order-1 (customer=alice amount=10.50)
stored order-2 (customer=bob amount=20.00)
stored order-3 (customer=alice amount=7.25)
worker handled 3 message(s)
change feed: order-1 customer=alice amount=10.50
change feed: order-2 customer=bob amount=20.00
change feed: order-3 customer=alice amount=7.25
feed reader saw 3 document(s)
PASS: azure-servicebus-cosmos-worker (3 messages consumed, stored and seen in the change feed)
```

Failures print `FAIL:` and exit non-zero. Re-running is safe: the queue is recreated and orders are upserted by id.

> [!NOTE]
> The emulated change feed has one partition range and reports creates and updates only, not deletes.

## Clean up

```bash
docker compose down -v
```
