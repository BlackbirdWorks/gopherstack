# Azure Service Bus

In-memory Azure Service Bus with two front ends over one backend: the Brokered Messaging REST API and an AMQP 1.0 listener. Queues, topics, subscriptions, peek-lock, dead-letter queues and sessions.

## Quick start

Official SDKs (`azservicebus`) speak AMQP 1.0 only. Connect to the AMQP listener with the development emulator connection string:

```text
Endpoint=sb://localhost;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=2R1W2VORtFi9HrmRQ1Gxp7xbySq7W0FAs2BvTZDdXeo=;UseDevelopmentEmulator=true;
```

For TLS on port 5671 the certificate is self-signed. Supply `ClientOptions.TLSConfig` and set `ServerName`, because the SDK uses `host:port` as the TLS server name.

REST with curl:

```bash
curl -X PUT  "http://localhost:10003/my-queue"
curl -X POST "http://localhost:10003/my-queue/messages" --data "hello world"

# Peek-lock: BrokerProperties carries MessageId/LockToken; Location gives the complete/abandon URL
curl -i -X POST "http://localhost:10003/my-queue/messages/head"
curl -X DELETE "http://localhost:10003/my-queue/messages/<id>/<locktoken>"   # complete
curl -X PUT    "http://localhost:10003/my-queue/messages/<id>/<locktoken>"   # abandon

# Topic and subscription
curl -X PUT  "http://localhost:10003/my-topic?type=topic"
curl -X PUT  "http://localhost:10003/my-topic/subscriptions/my-sub"
curl -X POST "http://localhost:10003/my-topic/messages" --data "fan-out"
curl -X POST "http://localhost:10003/my-topic/subscriptions/my-sub/messages/head"

# Dead-letter sub-queue
curl -X POST "http://localhost:10003/my-queue/\$DeadLetterQueue/messages/head"
```

## Supported operations (REST)

| Area | Operations |
|------|------------|
| Queues | `CreateQueue`, `DeleteQueue`, `GetQueue`, `ListQueues` |
| Topics | `CreateTopic`, `DeleteTopic`, `GetTopic`, `ListTopics` |
| Subscriptions | `CreateSubscription`, `DeleteSubscription`, `GetSubscription`, `ListSubscriptions` |
| Messages | `SendMessage` (metadata in the `BrokerProperties` header), `PeekLockMessage`, `ReceiveAndDeleteMessage`, `PeekMessages`, `CompleteMessage`, `AbandonMessage`, `DeadLetterMessage` |

- Queue vs topic is detected from the Atom+XML body root (`QueueDescription` or `TopicDescription`), or from `?type=topic`.
- `LockDuration`, `MaxDeliveryCount` and `DefaultMessageTimeToLive` are per entity.
- `PeekLock` honours a long-poll `timeout`, capped at 30 s.
- A janitor (10 s) releases expired locks, dead-letters on `MaxDeliveryCount` exhaustion and dead-letters TTL-expired messages.
- Sessions work on the AMQP plane. Over REST a session entity rejects a send without `SessionId`, and peek-lock or destructive reads return 400.

## Configuration

Each listener runs on its own port. Startup fails if a port is taken.

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--azure-servicebus-port` | `AZURE_SERVICEBUS_PORT` | `10003` | REST port |
| `--azure-servicebus-amqp-port` | `AZURE_SERVICEBUS_AMQP_PORT` | `5672` | Plain AMQP 1.0. `0` disables |
| `--azure-servicebus-amqp-tls-port` | `AZURE_SERVICEBUS_AMQP_TLS_PORT` | `5671` | AMQP 1.0 over TLS, self-signed certificate. `0` disables |
| `--azure-servicebus-validate-sas` | `AZURE_SERVICEBUS_VALIDATE_SAS` | `false` | Verify SAS token HMAC-SHA256 signatures |

Default namespace `sbemulatorns`, key name `RootManageSharedAccessKey`, key `2R1W2VORtFi9HrmRQ1Gxp7xbySq7W0FAs2BvTZDdXeo=`.

## Differences from Azure

> [!WARNING]
> SAS signatures are not verified by default. The token is parsed for key name, scope and expiry, and any credentials, or none, are accepted.

- Subscription SQL and correlation filter rules are accepted but discarded. There is no rules endpoint. Every subscription behaves as `TrueFilter`.
- `ListQueues`, `ListTopics` and `ListSubscriptions` ignore `$skip` and `$top`.
- AMQP bodies are flattened. `amqp-value` and sequence bodies are stored as bytes and returned as a data section. Application property types become strings after a persistence restore or when set through REST.
- Client dead-letter and `PeekMessages` use emulator-specific REST shapes: `DeadLetterReason` in `BrokerProperties` on the unlock `PUT`, and `GET .../messages`. The AMQP plane does both natively.
- AMQP limits: 64 KiB frames, 1 MiB messages.
- AMQP transactions, resume of unsettled state across reconnects, WebSocket transport and SASL EXTERNAL are not implemented.

See also the [service parity audit](../../services/azureservicebus/PARITY.md) and [AZURE.md](../../AZURE.md).
