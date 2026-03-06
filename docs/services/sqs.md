# SQS — Simple Queue Service

Full in-memory SQS implementation supporting standard queues, FIFO queues, visibility timeouts, dead-letter queues, and batch operations.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateQueue` | Create a new standard or FIFO queue |
| `DeleteQueue` | Delete a queue and all its messages |
| `ListQueues` | List queues with optional name prefix filter |
| `GetQueueUrl` | Look up the URL for a queue by name |
| `GetQueueAttributes` | Get queue attributes (depth, ARN, visibility timeout, etc.) |
| `SetQueueAttributes` | Update queue attributes |
| `SendMessage` | Enqueue a single message |
| `ReceiveMessage` | Receive up to 10 messages (long-polling supported) |
| `DeleteMessage` | Delete a message by receipt handle |
| `ChangeMessageVisibility` | Extend or reset a message's visibility timeout |
| `SendMessageBatch` | Enqueue up to 10 messages in one request |
| `DeleteMessageBatch` | Delete up to 10 messages in one request |
| `ChangeMessageVisibilityBatch` | Change visibility for up to 10 messages |
| `PurgeQueue` | Delete all messages in a queue |
| `TagQueue` | Add tags to a queue |
| `UntagQueue` | Remove tags from a queue |
| `ListQueueTags` | List all tags on a queue |

## AWS CLI Examples

```bash
# Create a standard queue
aws --endpoint-url http://localhost:8000 sqs create-queue --queue-name my-queue

# Create a FIFO queue
aws --endpoint-url http://localhost:8000 sqs create-queue \
    --queue-name my-queue.fifo \
    --attributes FifoQueue=true,ContentBasedDeduplication=true

# Send a message
aws --endpoint-url http://localhost:8000 sqs send-message \
    --queue-url http://localhost:8000/000000000000/my-queue \
    --message-body "Hello, world!"

# Receive messages
aws --endpoint-url http://localhost:8000 sqs receive-message \
    --queue-url http://localhost:8000/000000000000/my-queue \
    --max-number-of-messages 5

# Delete a message
aws --endpoint-url http://localhost:8000 sqs delete-message \
    --queue-url http://localhost:8000/000000000000/my-queue \
    --receipt-handle "<receipt-handle>"

# Get queue depth
aws --endpoint-url http://localhost:8000 sqs get-queue-attributes \
    --queue-url http://localhost:8000/000000000000/my-queue \
    --attribute-names ApproximateNumberOfMessages

# Purge a queue
aws --endpoint-url http://localhost:8000 sqs purge-queue \
    --queue-url http://localhost:8000/000000000000/my-queue
```

## Known Limitations

- Dead-letter queues are accepted at creation time but messages are not automatically moved to them after `maxReceiveCount` failures.
- Long-polling (`WaitTimeSeconds > 0`) is supported but uses a short-poll with a sleep rather than true blocking.
- Message retention period is not enforced; messages persist until explicitly deleted or the queue is purged.
- SNS-SQS subscription fan-out is supported when both services are wired together in Gopherstack.

## Configuration

| Flag | Env Var | Default | Description |
|------|---------|---------|-------------|
| `--sqs-visibility-timeout` | `SQS_VISIBILITY_TIMEOUT` | `30` | Default visibility timeout in seconds |
