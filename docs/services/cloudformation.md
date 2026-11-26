# CloudFormation

In-memory CloudFormation. Stacks provision real resources in the other Gopherstack services, with change sets, nested stacks, StackSets, drift detection, custom resources and rollback.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws cloudformation create-stack --stack-name my-stack --template-body \
  '{"AWSTemplateFormatVersion":"2010-09-09","Resources":{"MyBucket":{"Type":"AWS::S3::Bucket"}}}'
aws cloudformation describe-stacks --stack-name my-stack
aws cloudformation list-stack-resources --stack-name my-stack
aws cloudformation describe-stack-events --stack-name my-stack

aws cloudformation create-change-set --stack-name my-stack --change-set-name c1 \
    --template-body file://v2.json
aws cloudformation execute-change-set --stack-name my-stack --change-set-name c1

aws cloudformation delete-stack --stack-name my-stack
```

## Supported operations

| Area | Operations |
|------|------------|
| Stacks | `CreateStack`, `UpdateStack`, `DeleteStack`, `DescribeStacks`, `ListStacks`, `DescribeStackEvents`, `DescribeEvents`, `UpdateTerminationProtection`, `CancelUpdateStack`, `ContinueUpdateRollback`, `RollbackStack`, `SignalResource` |
| Resources | `DescribeStackResource`, `DescribeStackResources`, `ListStackResources` |
| Templates | `GetTemplate`, `GetTemplateSummary`, `ValidateTemplate`, `EstimateTemplateCost` |
| Change sets | `CreateChangeSet`, `DescribeChangeSet`, `ExecuteChangeSet`, `DeleteChangeSet`, `ListChangeSets`, `DescribeChangeSetHooks` |
| Exports | `ListExports`, `ListImports` |
| Stack policy | `SetStackPolicy`, `GetStackPolicy` |
| Drift | `DetectStackDrift`, `DetectStackResourceDrift`, `DescribeStackDriftDetectionStatus`, `DescribeStackResourceDrifts` |
| StackSets | `Create/Update/Delete/Describe/ListStackSet(s)`, `Create/Update/Delete/ListStackInstances`, `DescribeStackInstance`, `DetectStackSetDrift`, `ListStackSetOperations`, `DescribeStackSetOperation`, `StopStackSetOperation`, `ListStackSetOperationResults`, `ListStackSetAutoDeploymentTargets`, `ImportStacksToStackSet`, `ListStackInstanceResourceDrifts` |
| Generated templates and scans | `Create/Update/Delete/Describe/Get/ListGeneratedTemplate(s)`, `StartResourceScan`, `DescribeResourceScan`, `ListResourceScans`, `ListResourceScanResources`, `ListResourceScanRelatedResources` |
| Stack refactors | `CreateStackRefactor`, `DescribeStackRefactor`, `ExecuteStackRefactor`, `ListStackRefactors`, `ListStackRefactorActions` |
| Type registry | `DescribeType`, `ActivateType`, `DeactivateType`, `RegisterType`, `DeregisterType`, `PublishType`, `SetTypeDefaultVersion`, `SetTypeConfiguration`, `BatchDescribeTypeConfigurations`, `ListTypes`, `ListTypeVersions`, `ListTypeRegistrations`, `DescribeTypeRegistration`, `TestType`, `RegisterPublisher`, `DescribePublisher` |
| Hooks | `GetHookResult`, `ListHookResults`, `RecordHandlerProgress` |
| Organizations | `Activate/Deactivate/DescribeOrganizationsAccess` |
| Account | `DescribeAccountLimits` |

Template features: `Ref`, `Fn::GetAtt`, `Fn::Sub`, `Fn::Join`, `Fn::Split`, `Fn::Select`, `Fn::FindInMap`, `Fn::If`, `Fn::Equals`, `Fn::And`, `Fn::Or`, `Fn::Not`, `Fn::ImportValue`, `Fn::GetAZs`, `Fn::Cidr`, `Fn::Length`, `Fn::ToJsonString`, `Fn::ForEach`, `Fn::Transform`, and the AWS SAM transform (`AWS::Serverless-2016-10-31`). Output `Export.Name` values are importable by other stacks and removed when the owning stack is deleted.

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--cloudformation-provisioning-delay` | `GOPHERSTACK_CFN_PROVISIONING_DELAY` | `300ms` | Time each stack step stays `*_IN_PROGRESS`. `0` completes stacks instantly |
| `--cloudformation-validate-properties` | `GOPHERSTACK_CLOUDFORMATION_VALIDATE_PROPERTIES` | `true` | Reject unknown, wrong-typed and missing required resource properties. Disable with `--no-cloudformation-validate-properties` |

## Differences from AWS

> [!WARNING]
> Resource types are checked by service namespace only. A type whose service is unknown fails with `Unrecognized resource types`. An unmodelled resource type of a known service is accepted but creates no backing resource.

- Nested property types are not validated, and registry-only conditional requirements are not modelled.
- Custom resources (`Custom::*`, `AWS::CloudFormation::CustomResource`) wait up to 30 s for a Lambda response.
- Change-set replacement detection covers a curated subset of resource types. Resource property schemas are not part of the AWS SDK.
- Stack policy: `Replacement: Conditionally` is treated as `Update:Replace`. The logical-ID and resource-type default-deny rule is not modelled.
- Not modelled: `CreateChangeSet` `IncludeNestedStacks`, and the effect of `ActivateType` `MajorVersion`/`VersionBump`.
- Nested stacks and StackSet instances are provisioned and deleted in one step inside the parent, not per resource.
- SAM: `AWS::Serverless::CapacityProvider`, `NetworkConnector`, `MicrovmImage`, `WebFunction`, and `Application` with a Serverless Application Repository location are not expanded. The API `Deployment` hash is SHA-1 over the sorted swagger JSON, so IDs differ from real SAM.
- `ListResourceScanRelatedResources` always returns an empty list.
