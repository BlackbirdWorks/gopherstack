# Pinning generated resource IDs

Tests and snapshots are easier to write when resource IDs are deterministic.
Gopherstack reserves one create-time tag, `gopherstack:id`, to pin the ID a
resource would otherwise get at random. Real AWS ignores unknown tags, so the
same IaC works against both.

The LocalStack tag name `_custom_id_` is accepted as an alias wherever `gopherstack:id` is (API Gateway REST and HTTP APIs accept 1-32 lowercase alphanumerics or hyphens).

## Rules

- Honoured only on the create call. The tag is consumed: it is never stored
  and never appears in `List*Tags`, `Describe*` tag sets or the create response.
- A value that does not match the resource type's ID format is rejected with the
  service's validation error and a message naming the expected format.
- A value that is already in use is rejected with the service's already-exists
  or conflict error.
- Tagging an existing resource with `gopherstack:id` is rejected.

## Supported resources

| Service | Create call | Pinned value | Format |
|---|---|---|---|
| EC2 | `RunInstances` (one instance) | instance ID, `TagSpecification` type `instance` | `i-` + 8 or 17 lowercase hex |
| EC2 | `CreateVpc` | VPC ID, type `vpc` | `vpc-` + 8 or 17 lowercase hex |
| EC2 | `CreateSubnet` | subnet ID, type `subnet` | `subnet-` + 8 or 17 lowercase hex |
| EC2 | `CreateSecurityGroup` | group ID, type `security-group` | `sg-` + 8 or 17 lowercase hex |
| ECS | `RunTask` (one task) | task ID | 32 hex digits (UUID dashes allowed) |
| API Gateway | `CreateRestApi` | REST API ID | 10 lowercase alphanumerics |
| API Gateway v2 | `CreateApi` | API ID | 10 lowercase alphanumerics |
| Cognito | `CreateUserPool` (`UserPoolTags`) | user pool ID | `<region>_` + alphanumerics |
| Secrets Manager | `CreateSecret` | ARN suffix | 6 alphanumerics |
| KMS | `CreateKey` | key ID | lowercase UUID, or `mrk-` + 32 hex when `MultiRegion` |
| CloudFront | `CreateDistributionWithTags` | distribution ID | `E` + 13 uppercase alphanumerics |
| ELBv2 | `CreateLoadBalancer` | ARN suffix | 16 lowercase hex |
| EFS | `CreateFileSystem` | file system ID | `fs-` + 8 to 40 lowercase hex |
| RDS | `CreateDBInstance` | `DbiResourceId` | `db-` + 26 uppercase alphanumerics |

Lambda, SQS and DynamoDB identifiers are derived from the resource name, so
there is nothing to pin. Step Functions execution names are caller-supplied.

## Example

```sh
aws ec2 create-vpc --cidr-block 10.0.0.0/16 \
  --tag-specifications 'ResourceType=vpc,Tags=[{Key=gopherstack:id,Value=vpc-0a1b2c3d4e5f60718}]'
```

The VPC is created as `vpc-0a1b2c3d4e5f60718` and carries no `gopherstack:id` tag.
