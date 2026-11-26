# EventBridge

In-memory EventBridge with event buses, rules, targets, archives and replays, connections and API destinations, global endpoints, partner event sources, and the Schemas registry.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws events create-event-bus --name my-bus
aws events put-rule --name my-rule --event-bus-name my-bus \
    --event-pattern '{"source":["myapp.orders"]}' --state ENABLED
aws events put-targets --rule my-rule --event-bus-name my-bus \
    --targets '[{"Id":"1","Arn":"arn:aws:sqs:us-east-1:000000000000:my-queue"}]'
aws events put-events --entries '[{
    "EventBusName":"my-bus","Source":"myapp.orders",
    "DetailType":"OrderPlaced","Detail":"{\"orderId\":\"123\"}"}]'
aws events list-rules --event-bus-name my-bus
```

## Supported operations

| Area | Operations |
|------|------------|
| Event buses | `CreateEventBus`, `DescribeEventBus`, `ListEventBuses`, `UpdateEventBus`, `DeleteEventBus`, `PutPermission`, `RemovePermission` |
| Rules | `PutRule`, `DescribeRule`, `ListRules`, `EnableRule`, `DisableRule`, `DeleteRule`, `TestEventPattern` |
| Targets | `PutTargets`, `RemoveTargets`, `ListTargetsByRule`, `ListRuleNamesByTarget` |
| Events | `PutEvents`, `PutPartnerEvents` |
| Archives and replays | `Create/Describe/List/Update/DeleteArchive`, `StartReplay`, `DescribeReplay`, `ListReplays`, `CancelReplay` |
| Connections and API destinations | `Create/Describe/List/Update/DeleteConnection`, `DeauthorizeConnection`, `Create/Describe/List/Update/DeleteApiDestination` |
| Global endpoints | `Create/Describe/List/Update/DeleteEndpoint` |
| Partner event sources | `Create/Describe/List/DeletePartnerEventSource`, `ListPartnerEventSourceAccounts`, `ActivateEventSource`, `DeactivateEventSource`, `DescribeEventSource`, `ListEventSources` |
| Tags | `TagResource`, `UntagResource`, `ListTagsForResource` |
| Schemas registry | `Create/Describe/List/Update/DeleteRegistry`, `Create/Describe/List/Update/Delete/SearchSchema(s)`, `ListSchemaVersions`, `DeleteSchemaVersion`, `GetDiscoveredSchema`, `PutCodeBinding`, `DescribeCodeBinding`, `GetCodeBindingSource`, `ExportSchema`, discoverer CRUD with `StartDiscoverer` and `StopDiscoverer`, `Get/Put/DeleteResourcePolicy` |

Rule targets delivered in-process: Lambda, SQS, SNS, Kinesis, Step Functions, ECS, Batch, CloudWatch Logs, other event buses (same account, cross-bus and cross-region), API destinations, API Gateway, Systems Manager, AppSync, CodeBuild, CodePipeline, Redshift and SageMaker.

## Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `EVENTBRIDGE_ALLOW_SCHEDULE_ON_CUSTOM_BUS` | `false` | Accept scheduled rules on custom buses (AWS allows them only on the default bus) |

## Differences from AWS

> [!NOTE]
> The backend models one account. A target event bus in another account cannot be resolved, so that delivery is dropped.

- Inspector is not a deliverable target. Such ARNs log `unsupported target ARN type`.
- `ThrottledRules` and `PutEventsApproximateThrottledCount` are never emitted: no invocation or `PutEvents` throttle limits apply.
