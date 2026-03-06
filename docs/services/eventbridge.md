# EventBridge

In-memory EventBridge implementation with event buses, rules, targets, and event routing.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateEventBus` | Create a custom event bus |
| `DeleteEventBus` | Delete a custom event bus |
| `ListEventBuses` | List all event buses |
| `DescribeEventBus` | Get details of an event bus |
| `PutRule` | Create or update a rule on an event bus |
| `DeleteRule` | Delete a rule |
| `ListRules` | List rules on an event bus |
| `DescribeRule` | Get rule details |
| `EnableRule` | Enable a disabled rule |
| `DisableRule` | Disable a rule |
| `PutTargets` | Add or update targets for a rule |
| `RemoveTargets` | Remove targets from a rule |
| `ListTargetsByRule` | List targets for a rule |
| `PutEvents` | Publish events to an event bus |
| `ListTagsForResource` | List tags on a rule or event bus |
| `TagResource` | Add tags |
| `UntagResource` | Remove tags |

## AWS CLI Examples

```bash
# Create a custom event bus
aws --endpoint-url http://localhost:8000 events create-event-bus \
    --name my-bus

# Create a rule to match all events from a source
aws --endpoint-url http://localhost:8000 events put-rule \
    --name my-rule \
    --event-bus-name my-bus \
    --event-pattern '{"source":["myapp.orders"]}' \
    --state ENABLED

# Add an SQS queue as a target
aws --endpoint-url http://localhost:8000 events put-targets \
    --rule my-rule \
    --event-bus-name my-bus \
    --targets '[{"Id":"1","Arn":"arn:aws:sqs:us-east-1:000000000000:my-queue"}]'

# Publish an event
aws --endpoint-url http://localhost:8000 events put-events \
    --entries '[{
        "EventBusName":"my-bus",
        "Source":"myapp.orders",
        "DetailType":"OrderPlaced",
        "Detail":"{\"orderId\":\"123\"}"
    }]'

# List rules
aws --endpoint-url http://localhost:8000 events list-rules \
    --event-bus-name my-bus
```

## Known Limitations

- Event pattern matching supports `source`, `detail-type`, and basic `detail` field matching. Complex JSONPath conditions may not be fully evaluated.
- Target invocation (Lambda, SQS, SNS, etc.) is performed synchronously in-process when services are wired together. HTTP and Kinesis targets are not supported.
- EventBridge Pipes and EventBridge Scheduler are separate services.
