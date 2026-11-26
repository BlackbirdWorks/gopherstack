# Pinning generated resource IDs

A create-time tag, `gopherstack:id`, pins the ID a resource would otherwise get at random. Real AWS ignores unknown tags, so the same IaC works against both. The LocalStack tag `_custom_id_` is accepted as an alias.

## Rules

- Honoured only on the create call. The tag is consumed: it is not stored and does not appear in `List*Tags`, `Describe*` tag sets or the create response.
- A value that does not match the resource's ID format is rejected with the service's validation error naming the expected format.
- A value already in use is rejected with the service's already-exists or conflict error.
- Tagging an existing resource with `gopherstack:id` is rejected.

## Supported resources

| Service | Create call | Pinned value | Format |
|---|---|---|---|
| EC2 | `RunInstances` (one instance) | Instance ID, `TagSpecification` type `instance` | `i-` + 8 or 17 lowercase hex |
| EC2 | `CreateVpc` | VPC ID, type `vpc` | `vpc-` + 8 or 17 lowercase hex |
| EC2 | `CreateSubnet` | Subnet ID, type `subnet` | `subnet-` + 8 or 17 lowercase hex |
| EC2 | `CreateSecurityGroup` | Group ID, type `security-group` | `sg-` + 8 or 17 lowercase hex |
| ECS | `RunTask` (one task) | Task ID | 32 hex digits (UUID dashes allowed) |
| API Gateway | `CreateRestApi` | REST API ID | 1-32 lowercase alphanumerics or hyphens |
| API Gateway v2 | `CreateApi` | API ID | 1-32 lowercase alphanumerics or hyphens |
| Cognito | `CreateUserPool` (`UserPoolTags`) | User pool ID | `<region>_` + alphanumerics |
| Cognito | `CreateUserPoolClient` | Client ID, via client name `_custom_id_:<id>` | `[\w+]{1,128}` |
| Secrets Manager | `CreateSecret` | ARN suffix | 6 alphanumerics |
| KMS | `CreateKey` | Key ID | Lowercase UUID, or `mrk-` + 32 hex when `MultiRegion` |
| CloudFront | `CreateDistributionWithTags` | Distribution ID | `E` + 13 uppercase alphanumerics |
| ELBv2 | `CreateLoadBalancer` | ARN suffix | 16 lowercase hex |
| EFS | `CreateFileSystem` | File system ID | `fs-` + 8 to 40 lowercase hex |
| RDS | `CreateDBInstance` | `DbiResourceId` | `db-` + 26 uppercase alphanumerics |

Lambda, SQS and DynamoDB identifiers derive from the resource name, so there is nothing to pin.

## Example

```sh
aws ec2 create-vpc --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=gopherstack:id,Value=vpc-0a1b2c3d4e5f60718}]'
```

The VPC is created as `vpc-0a1b2c3d4e5f60718` with no `gopherstack:id` tag.
