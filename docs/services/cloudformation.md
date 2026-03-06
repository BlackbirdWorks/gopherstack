# CloudFormation

In-memory CloudFormation implementation supporting stack lifecycle management and change sets.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateStack` | Create a new stack from a template |
| `UpdateStack` | Update an existing stack |
| `DeleteStack` | Delete a stack and its resources |
| `DescribeStacks` | Get stack details and outputs |
| `ListStacks` | List stacks with optional status filter |
| `DescribeStackEvents` | Get the event history for a stack |
| `CreateChangeSet` | Create a change set to preview stack changes |
| `DescribeChangeSet` | Get change set details |
| `ExecuteChangeSet` | Apply a change set to the stack |
| `DeleteChangeSet` | Delete a change set |
| `ListChangeSets` | List change sets for a stack |
| `GetTemplate` | Get the template body for a stack |

## AWS CLI Examples

```bash
# Create a stack
aws --endpoint-url http://localhost:8000 cloudformation create-stack \
    --stack-name my-stack \
    --template-body '{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"MyBucket":{"Type":"AWS::S3::Bucket"}}}'

# Describe stacks
aws --endpoint-url http://localhost:8000 cloudformation describe-stacks \
    --stack-name my-stack

# Create a change set
aws --endpoint-url http://localhost:8000 cloudformation create-change-set \
    --stack-name my-stack \
    --change-set-name my-changes \
    --template-body '{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"MyBucket":{"Type":"AWS::S3::Bucket"},"MyQueue":{"Type":"AWS::SQS::Queue"}}}'

# Execute the change set
aws --endpoint-url http://localhost:8000 cloudformation execute-change-set \
    --stack-name my-stack \
    --change-set-name my-changes

# Get stack events
aws --endpoint-url http://localhost:8000 cloudformation describe-stack-events \
    --stack-name my-stack

# Delete a stack
aws --endpoint-url http://localhost:8000 cloudformation delete-stack \
    --stack-name my-stack
```

## Known Limitations

- Templates are stored but not executed; declared resources are not actually created in Gopherstack.
- Stack outputs and exports are returned as defined in the template but not computed from live resources.
- Stack rollback on failure is not implemented.
- CloudFormation StackSets and nested stacks are not supported.
- Custom resources (Lambda-backed) are not invoked.
