# SNS — Simple Notification Service

Full in-memory SNS implementation with topic management, HTTP/HTTPS/SQS/Lambda subscriptions, and message fan-out.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateTopic` | Create a new topic (standard or FIFO) |
| `DeleteTopic` | Delete a topic and all its subscriptions |
| `ListTopics` | List all topics |
| `GetTopicAttributes` | Get topic attributes (ARN, display name, etc.) |
| `SetTopicAttributes` | Update topic attributes |
| `Subscribe` | Subscribe an endpoint to a topic |
| `ConfirmSubscription` | Confirm a pending subscription |
| `Unsubscribe` | Remove a subscription |
| `ListSubscriptions` | List all subscriptions |
| `ListSubscriptionsByTopic` | List subscriptions for a specific topic |
| `Publish` | Publish a message to a topic |
| `PublishBatch` | Publish up to 10 messages in one request |
| `GetSubscriptionAttributes` | Get attributes of a subscription |
| `ListTagsForResource` | List tags on a topic |
| `TagResource` | Add tags to a topic |
| `UntagResource` | Remove tags from a topic |

## AWS CLI Examples

```bash
# Create a topic
aws --endpoint-url http://localhost:8000 sns create-topic --name my-topic

# Subscribe an SQS queue to the topic
aws --endpoint-url http://localhost:8000 sns subscribe \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic \
    --protocol sqs \
    --notification-endpoint arn:aws:sqs:us-east-1:000000000000:my-queue

# Publish a message
aws --endpoint-url http://localhost:8000 sns publish \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic \
    --message "Hello, subscribers!"

# Publish with subject and attributes
aws --endpoint-url http://localhost:8000 sns publish \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic \
    --subject "Test notification" \
    --message "Event payload" \
    --message-attributes '{"EventType":{"DataType":"String","StringValue":"OrderPlaced"}}'

# List subscriptions for a topic
aws --endpoint-url http://localhost:8000 sns list-subscriptions-by-topic \
    --topic-arn arn:aws:sns:us-east-1:000000000000:my-topic
```

## Known Limitations

- HTTP/HTTPS endpoint delivery is attempted but subscription confirmation is automatic (no real webhook call).
- Email and SMS protocols are accepted but messages are not actually sent.
- Message filtering policies are stored but not fully evaluated during fan-out.
- SNS FIFO topics are accepted but strict ordering guarantees are not enforced.

## Configuration

No additional configuration required. SNS runs on the main Gopherstack port.
