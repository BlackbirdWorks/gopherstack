# Step Functions

In-memory Step Functions that runs Amazon States Language (ASL) definitions, with JSONPath and JSONata, Parallel and Map (including distributed Map) states, activities, versions and aliases.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws stepfunctions create-state-machine --name my-workflow \
    --role-arn arn:aws:iam::000000000000:role/sfn-role \
    --definition '{"StartAt":"Hello","States":{"Hello":{"Type":"Pass","End":true}}}'

EXEC_ARN=$(aws stepfunctions start-execution \
    --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:my-workflow \
    --input '{"key":"value"}' --query executionArn --output text)

aws stepfunctions describe-execution --execution-arn "$EXEC_ARN"
aws stepfunctions get-execution-history --execution-arn "$EXEC_ARN"
```

## Supported operations

| Area | Operations |
|------|------------|
| State machines | `CreateStateMachine`, `DescribeStateMachine`, `ListStateMachines`, `UpdateStateMachine`, `DeleteStateMachine`, `ValidateStateMachineDefinition`, `TestState` |
| Versions and aliases | `PublishStateMachineVersion`, `ListStateMachineVersions`, `DeleteStateMachineVersion`, `CreateStateMachineAlias`, `DescribeStateMachineAlias`, `ListStateMachineAliases`, `UpdateStateMachineAlias`, `DeleteStateMachineAlias` |
| Executions | `StartExecution`, `StartSyncExecution` (Express only), `StopExecution`, `DescribeExecution`, `DescribeStateMachineForExecution`, `ListExecutions`, `GetExecutionHistory`, `RedriveExecution` |
| Map runs | `DescribeMapRun`, `ListMapRuns`, `UpdateMapRun` |
| Activities and callbacks | `CreateActivity`, `DescribeActivity`, `ListActivities`, `DeleteActivity`, `GetActivityTask`, `SendTaskSuccess`, `SendTaskFailure`, `SendTaskHeartbeat` |
| Tags | `TagResource`, `UntagResource`, `ListTagsForResource` |

ASL features:

- State types: Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map.
- `Retry` and `Catch`, path processing (`InputPath`, `OutputPath`, `ResultPath`, `Parameters`, `ResultSelector`), intrinsic functions, JSONata expressions.
- Task integrations: optimized integrations such as Lambda, SQS, SNS, DynamoDB, ECS, Glue, Athena, API Gateway and EventBridge, plus `aws-sdk:<service>:<action>`. The `.sync` and `.waitForTaskToken` patterns are supported.

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--stepfunctions-execution-retention` | `SFN_EXECUTION_RETENTION` | `24h` | How long execution history is kept |
| `--stepfunctions-janitor-interval` | `SFN_JANITOR_INTERVAL` | `1m` | Janitor tick |
| `--stepfunctions-task-token-ttl` | `SFN_TASK_TOKEN_TTL` | `1h` | Lifetime of an unreceived task token |
| `--stepfunctions-jsonata-map-object-items` | `SFN_JSONATA_MAP_OBJECT_ITEMS` | `true` | Accept an object as a JSONata Map `Items` value (AWS documents arrays only) |

## Differences from AWS

> [!NOTE]
> Integrations outside the built-in table are not implemented: Bedrock optimized integrations other than `invokeModel`, and `aws-sdk` integrations for services outside the built-in service table.

- `ExecutionThrottled` is never emitted: there is no account-level `StartExecution` throttling.
- Under `--enforce-iam`, some service-initiated calls are not checked against a customer role: Firehose delivery roles, CloudWatch Logs subscription-filter roles, IoT rule action roles, SNS `SubscriptionRoleArn`, Step Functions activity paths and Scheduler DLQ sends.
