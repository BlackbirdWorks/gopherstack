# Azure Queue Storage

In-memory Azure Queue Storage with an Azurite-compatible wire protocol.

## Quick start

```bash
curl -X PUT "http://localhost:10001/devstoreaccount1/my-queue"

curl -X POST "http://localhost:10001/devstoreaccount1/my-queue/messages" \
    --data '<QueueMessage><MessageText>hello world</MessageText></QueueMessage>'

curl "http://localhost:10001/devstoreaccount1/my-queue/messages?numofmessages=1"

# id and popreceipt come from the get response
curl -X DELETE "http://localhost:10001/devstoreaccount1/my-queue/messages/<id>?popreceipt=<popreceipt>"
```

Go SDK:

```go
cred, _ := azqueue.NewSharedKeyCredential("devstoreaccount1",
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==")
svc, _ := azqueue.NewServiceClientWithSharedKeyCredential(
    "http://localhost:10001/devstoreaccount1", cred, nil)
queue := svc.NewQueueClient("my-queue")
_, _ = queue.Create(ctx, nil)
_, _ = queue.EnqueueMessage(ctx, "hello world", nil)
resp, _ := queue.DequeueMessages(ctx, nil)
```

The account is path-style. The built-in account is Azurite's `devstoreaccount1` with its published development key.

## Supported operations

| Operation | Notes |
|-----------|-------|
| `ListQueues` | `include=metadata`. No `prefix`, `marker` or `maxresults`: one page |
| `CreateQueue` | Optional `x-ms-meta-*`. Same metadata on an existing queue returns 204, different metadata `409 QueueAlreadyExists`. Name rules are validated |
| `QueueExists` | `GET` on the queue |
| `DeleteQueue` | |
| `SetQueueMetadata`, `GetQueueMetadata` | `GetQueueMetadata` returns `x-ms-approximate-messages-count` |
| `PutMessage` | `visibilitytimeout`, `messagettl` |
| `GetMessages` | Up to 32 visible messages (`numofmessages`); rotates the pop receipt and increments `DequeueCount` |
| `PeekMessages` | `peekonly=true`; no visibility change |
| `UpdateMessage` | Needs `popreceipt` and `visibilitytimeout`; an optional body replaces the text |
| `DeleteMessage` | Needs `popreceipt`; a stale one is `400 PopReceiptMismatch` |
| `ClearMessages` | |
| `GetServiceProperties` | Always returns empty properties |

## Configuration

The queue service runs on its own TCP port. Startup fails if the port is taken.

| Flag | Env | Default |
|------|-----|---------|
| `--azure-queue-port` | `AZURE_QUEUE_PORT` | `10001` |

## Differences from Azure

> [!WARNING]
> Signatures are never verified. The `Authorization` header is parsed, and any credentials, or none, are accepted.

- `messagettl=-1` ("never expire") is stored as a 100-year TTL.
- `ListQueues` ignores `prefix`, `marker` and `maxresults`.
- Set/Get Queue ACL is not routed. SAS tokens are not verified.
- Set Queue Service Properties and Get Queue Service Stats are not routed. `GetServiceProperties` returns an empty body.
- `DequeueCount` is tracked. Azure has no server-side poison queue either, so nothing acts on it.

See also the [service parity audit](../../services/azurequeue/PARITY.md) and [AZURE.md](../../AZURE.md).
