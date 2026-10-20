# azure-servicebus-cosmos-worker

A queue-driven worker plus a change-feed reader:

1. `demo.sh` sends three JSON orders to an Azure Service Bus queue.
2. `worker` peek-locks each message, upserts the order into Cosmos DB (partition key `/customer`) with the real `azcosmos` SDK, then completes the message. Malformed messages are abandoned so Service Bus redelivers them and eventually dead-letters them.
3. `feed` reads the Cosmos DB **change feed** from the beginning with `ReadChangeFeed` and prints each new document until it is caught up.

Ports: Service Bus `10003`, Cosmos DB `8081`. The Service Bus calls use the Brokered Messaging REST API with `net/http`, because the official `azservicebus` SDK speaks only AMQP, which gopherstack does not serve. Cosmos uses its well-known public emulator master key (no real credentials).

## Prerequisites

- A Go toolchain (the example has its own `go.mod`, using the same dependency versions as the root module).
- `curl`.
- Gopherstack running: `docker-compose up -d` here, or `go run ../..` from the repo root.

## Running

```bash
docker-compose up -d
./demo.sh
```

Override endpoints with `SERVICEBUS_URL` and `COSMOS_URL`.

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

The script exits non-zero with a `FAIL:` line on any problem. It is idempotent: the queue is recreated and orders are upserted by id, so re-runs show the same three documents.

## Notes

The emulator's change feed has one partition range and reports only creates and updates (no deletes).

## Teardown

```bash
docker-compose down -v
```
