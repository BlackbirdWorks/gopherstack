# Step Functions

In-memory Step Functions implementation with full ASL (Amazon States Language) execution, including Parallel states, Map states, Wait states, and retry/catch logic.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateStateMachine` | Create a state machine from an ASL definition |
| `DeleteStateMachine` | Delete a state machine |
| `ListStateMachines` | List all state machines |
| `DescribeStateMachine` | Get state machine definition and metadata |
| `UpdateStateMachine` | Update the ASL definition or role |
| `StartExecution` | Start a new execution |
| `StopExecution` | Stop a running execution |
| `DescribeExecution` | Get execution status and input/output |
| `ListExecutions` | List executions for a state machine |
| `GetExecutionHistory` | Get the full event history of an execution |
| `ListTagsForResource` | List tags on a state machine |
| `TagResource` | Add tags |
| `UntagResource` | Remove tags |

## AWS CLI Examples

```bash
# Create a simple state machine
aws --endpoint-url http://localhost:8000 stepfunctions create-state-machine \
    --name my-workflow \
    --role-arn arn:aws:iam::000000000000:role/sfn-role \
    --definition '{
        "Comment": "A simple pass-through",
        "StartAt": "Hello",
        "States": {
            "Hello": {"Type": "Pass", "End": true}
        }
    }'

# Start an execution
EXEC_ARN=$(aws --endpoint-url http://localhost:8000 stepfunctions start-execution \
    --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:my-workflow \
    --input '{"key":"value"}' \
    --query executionArn --output text)

# Get execution status
aws --endpoint-url http://localhost:8000 stepfunctions describe-execution \
    --execution-arn "$EXEC_ARN"

# Get execution history
aws --endpoint-url http://localhost:8000 stepfunctions get-execution-history \
    --execution-arn "$EXEC_ARN"

# List executions
aws --endpoint-url http://localhost:8000 stepfunctions list-executions \
    --state-machine-arn arn:aws:states:us-east-1:000000000000:stateMachine:my-workflow
```

## Supported ASL Features

- **State types**: Pass, Task, Choice, Wait, Succeed, Fail, Parallel, Map
- **Task integrations**: SQS (`sendMessage`), SNS (`publish`), DynamoDB (`putItem`, `getItem`, `updateItem`, `deleteItem`)
- **Error handling**: `Retry` with `IntervalSeconds`, `MaxAttempts`, `BackoffRate`; `Catch` with result path
- **Data transformations**: `InputPath`, `OutputPath`, `ResultPath`, `Parameters`, `ResultSelector`
- **Choice rules**: String/numeric/boolean comparisons, `And`/`Or`/`Not` operators, reference path operators

## Known Limitations

- Lambda task integration requires the Lambda service to be running.
- Activity tasks are not supported.
- Express workflows behave identically to Standard workflows (no high-throughput mode).
- `heartbeatSeconds` is accepted but not enforced.
