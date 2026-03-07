# CloudFormation

In-memory CloudFormation implementation supporting stack lifecycle management, resource introspection, cross-stack exports, and change sets.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateStack` | Create a new stack from a template |
| `UpdateStack` | Update an existing stack |
| `DeleteStack` | Delete a stack and its resources |
| `DescribeStacks` | Get stack details and outputs |
| `ListStacks` | List stacks with optional status filter |
| `DescribeStackEvents` | Get the event history for a stack |
| `DescribeStackResource` | Get details of a specific resource by logical ID |
| `ListStackResources` | Paginated list of all resources in a stack |
| `DescribeStackResources` | Describe all resources in a stack |
| `ListExports` | List all exported output values across stacks |
| `ListImports` | List stacks that import a specific export |
| `CreateChangeSet` | Create a change set to preview stack changes |
| `DescribeChangeSet` | Get change set details |
| `ExecuteChangeSet` | Apply a change set to the stack |
| `DeleteChangeSet` | Delete a change set |
| `ListChangeSets` | List change sets for a stack |
| `GetTemplate` | Get the template body for a stack |

## Template Intrinsics

| Intrinsic | Description |
|-----------|-------------|
| `Ref` | Reference a parameter or resource physical ID |
| `Fn::Sub` | String substitution with `${Variable}` placeholders |
| `Fn::Join` | Join a list of values with a delimiter |
| `Fn::Split` | Split a string by delimiter |
| `Fn::Select` | Select a value from a list by index |
| `Fn::FindInMap` | Look up a value in the `Mappings` section |
| `Fn::If` | Conditional value selection based on a `Conditions` entry |
| `Fn::Equals` | Equality comparison for use in Conditions |
| `Fn::And` / `Fn::Or` / `Fn::Not` | Boolean operators for Conditions |
| `Fn::ImportValue` | Import a cross-stack export value |

## Cross-Stack Exports

When a stack output declares an `Export.Name`, the value becomes available to
other stacks via `Fn::ImportValue`:

```yaml
Outputs:
  VpcId:
    Value: !Ref MyVPC
    Export:
      Name: shared-vpc-id
```

```json
{
  "Resources": {
    "MyInstance": {
      "Type": "AWS::EC2::Instance",
      "Properties": {
        "SubnetId": {"Fn::ImportValue": "shared-vpc-id"}
      }
    }
  }
}
```

Exports are removed automatically when the owning stack is deleted.

## AWS CLI Examples

```bash
# Create a stack
aws --endpoint-url http://localhost:8000 cloudformation create-stack \
    --stack-name my-stack \
    --template-body '{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"MyBucket":{"Type":"AWS::S3::Bucket"}}}'

# Describe stacks
aws --endpoint-url http://localhost:8000 cloudformation describe-stacks \
    --stack-name my-stack

# Describe a specific resource in a stack
aws --endpoint-url http://localhost:8000 cloudformation describe-stack-resource \
    --stack-name my-stack \
    --logical-resource-id MyBucket

# List all resources in a stack
aws --endpoint-url http://localhost:8000 cloudformation list-stack-resources \
    --stack-name my-stack

# Describe all resources in a stack
aws --endpoint-url http://localhost:8000 cloudformation describe-stack-resources \
    --stack-name my-stack

# List all cross-stack exports
aws --endpoint-url http://localhost:8000 cloudformation list-exports

# List stacks that import a specific export
aws --endpoint-url http://localhost:8000 cloudformation list-imports \
    --export-name shared-vpc-id

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

- CloudFormation StackSets and nested stacks are not supported.
- Custom resources (Lambda-backed) are not invoked.
- Stack rollback on failure is simulated but resources are not actually rolled back.
