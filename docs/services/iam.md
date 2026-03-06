# IAM — Identity and Access Management

In-memory IAM implementation covering users, roles, policies, groups, access keys, and instance profiles.
Optionally enforces policies against every AWS API call when `--enforce-iam` / `GOPHERSTACK_ENFORCE_IAM=true` is set.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateUser` | Create an IAM user |
| `GetUser` | Get user details |
| `ListUsers` | List all users |
| `DeleteUser` | Delete an IAM user |
| `CreateRole` | Create an IAM role with a trust policy |
| `GetRole` | Get role details |
| `ListRoles` | List all roles |
| `DeleteRole` | Delete an IAM role |
| `CreatePolicy` | Create a managed policy |
| `GetPolicy` | Get policy metadata |
| `GetPolicyVersion` | Get the document for a specific policy version |
| `ListPolicies` | List managed policies |
| `DeletePolicy` | Delete a managed policy |
| `AttachUserPolicy` | Attach a managed policy to a user |
| `AttachRolePolicy` | Attach a managed policy to a role |
| `DetachRolePolicy` | Detach a managed policy from a role |
| `ListAttachedUserPolicies` | List managed policies attached to a user |
| `ListAttachedRolePolicies` | List managed policies attached to a role |
| `ListRolePolicies` | List inline policies for a role |
| `CreateGroup` | Create an IAM group |
| `DeleteGroup` | Delete an IAM group |
| `AddUserToGroup` | Add a user to a group |
| `CreateAccessKey` | Create an access key for a user |
| `DeleteAccessKey` | Delete an access key |
| `ListAccessKeys` | List access keys for a user |
| `CreateInstanceProfile` | Create an EC2 instance profile |
| `DeleteInstanceProfile` | Delete an instance profile |
| `ListInstanceProfiles` | List instance profiles |
| `TagRole` | Add tags to a role |
| `UntagRole` | Remove tags from a role |
| `ListRoleTags` | List tags on a role |
| `TagPolicy` | Add tags to a policy |
| `UntagPolicy` | Remove tags from a policy |
| `ListPolicyTags` | List tags on a policy |
| `TagUser` | Add tags to a user |
| `UntagUser` | Remove tags from a user |
| `ListUserTags` | List tags on a user |

## AWS CLI Examples

```bash
# Create a role
aws --endpoint-url http://localhost:8000 iam create-role \
    --role-name MyRole \
    --assume-role-policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}'

# Create and attach a policy
aws --endpoint-url http://localhost:8000 iam create-policy \
    --policy-name MyPolicy \
    --policy-document '{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:*","Resource":"*"}]}'

aws --endpoint-url http://localhost:8000 iam attach-role-policy \
    --role-name MyRole \
    --policy-arn arn:aws:iam::000000000000:policy/MyPolicy

# Create a user and access key
aws --endpoint-url http://localhost:8000 iam create-user --user-name alice
aws --endpoint-url http://localhost:8000 iam create-access-key --user-name alice

# List roles
aws --endpoint-url http://localhost:8000 iam list-roles
```

## Known Limitations

- SCP (Service Control Policies) and permission boundaries are not implemented.
- MFA devices are not supported.
- Cross-account trust and federated identities are not evaluated.

## Policy Enforcement (optional)

Start Gopherstack with `--enforce-iam` (or `GOPHERSTACK_ENFORCE_IAM=true`) to activate policy enforcement.
Every incoming AWS API call is evaluated against the caller's attached IAM policies before dispatch.

Supported features:
- **Allow / Deny** effects with explicit deny winning
- **Action wildcards** — `s3:*`, `dynamodb:Get*`, etc.
- **Resource wildcards** — `arn:aws:s3:::my-bucket/*`
- **NotAction / NotResource** — negated action and resource matching
- **Condition operators** — `StringEquals`, `StringLike`, `IpAddress`, `ArnLike`, `Bool`, `Null`, and more with `...IfExists` variants
- **Policy variables** — `${aws:username}`, `${aws:userid}`, `${aws:sourceip}`
- **Resource-based policies** — S3 bucket policies and SQS queue policies are evaluated alongside identity policies

When enforcement is off (default), all API calls succeed regardless of attached policies so existing tooling is unaffected.
