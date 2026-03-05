# IAM — Identity and Access Management

In-memory IAM implementation covering users, roles, policies, groups, access keys, and instance profiles. Policies are stored but not evaluated against API calls.

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

- Policies are stored but **not enforced**. All API operations succeed regardless of attached policies.
- SCP (Service Control Policies) and permission boundaries are not implemented.
- MFA devices are not supported.
- IAM Conditions in policy documents are not evaluated.
