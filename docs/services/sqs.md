# SQS

In-memory SQS with standard and FIFO queues, long polling, dead-letter queues and message move tasks.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws sqs create-queue --queue-name my-queue
aws sqs create-queue --queue-name my-queue.fifo \
    --attributes FifoQueue=true,ContentBasedDeduplication=true

URL=$(aws sqs get-queue-url --queue-name my-queue --query QueueUrl --output text)
aws sqs send-message --queue-url "$URL" --message-body "hello"
aws sqs receive-message --queue-url "$URL" --max-number-of-messages 5 --wait-time-seconds 5
aws sqs delete-message --queue-url "$URL" --receipt-handle "<receipt-handle>"
aws sqs get-queue-attributes --queue-url "$URL" --attribute-names ApproximateNumberOfMessages
aws sqs purge-queue --queue-url "$URL"
```

## Supported operations

| Area | Operations |
|------|------------|
| Queues | `CreateQueue`, `DeleteQueue`, `ListQueues`, `GetQueueUrl`, `GetQueueAttributes`, `SetQueueAttributes`, `PurgeQueue` |
| Messages | `SendMessage`, `SendMessageBatch`, `ReceiveMessage`, `DeleteMessage`, `DeleteMessageBatch` |
| Visibility | `ChangeMessageVisibility`, `ChangeMessageVisibilityBatch` |
| Dead-letter | `ListDeadLetterSourceQueues` |
| Move tasks | `StartMessageMoveTask`, `CancelMessageMoveTask`, `ListMessageMoveTasks` |
| Permissions | `AddPermission`, `RemovePermission` |
| Tags | `TagQueue`, `UntagQueue`, `ListQueueTags` |

SNS topics can deliver to SQS queues when both services run (the default).

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--sqs-disable-purge-cooldown` | `GOPHERSTACK_SQS_DISABLE_PURGE_COOLDOWN` | `false` | Allow `PurgeQueue` within 60 s of the previous purge |
| `--sqs-disable-recently-deleted-wait` | `GOPHERSTACK_SQS_DISABLE_RECENTLY_DELETED_COOLDOWN` | `false` | Allow recreating a queue within 60 s of deleting it |
| `--sqs-disable-message-retention` | `GOPHERSTACK_SQS_DISABLE_MESSAGE_RETENTION` | `false` | Never expire messages by `MessageRetentionPeriod` |
| `--sqs-disable-cloudwatch-metrics` | `SQS_DISABLE_CLOUDWATCH_METRICS`, `GOPHERSTACK_SQS_DISABLE_CLOUDWATCH_METRICS` | `false` | Do not publish `AWS/SQS` metrics to CloudWatch |

## Differences from AWS

> [!NOTE]
> SSE attributes (`SqsManagedSseEnabled`, `KmsMasterKeyId`, `KmsDataKeyReusePeriodSeconds`) are validated and returned, but messages are never encrypted and no KMS data keys are used.
