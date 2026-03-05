# STS — Security Token Service

In-memory STS implementation for authentication, role assumption, and credential management.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `GetCallerIdentity` | Return the current account, user ID, and ARN |
| `AssumeRole` | Assume an IAM role and get temporary credentials |
| `GetSessionToken` | Get temporary credentials for the current user |
| `GetAccessKeyInfo` | Get the account ID associated with an access key |
| `DecodeAuthorizationMessage` | Decode an authorization failure message |

## AWS CLI Examples

```bash
# Get caller identity (always returns the mock account)
aws --endpoint-url http://localhost:8000 sts get-caller-identity

# Assume a role
aws --endpoint-url http://localhost:8000 sts assume-role \
    --role-arn arn:aws:iam::000000000000:role/MyRole \
    --role-session-name my-session

# Get a session token
aws --endpoint-url http://localhost:8000 sts get-session-token

# Get access key info
aws --endpoint-url http://localhost:8000 sts get-access-key-info \
    --access-key-id AKIAIOSFODNN7EXAMPLE
```

## Response Values

All operations return the configured mock account ID (`--account-id`, default `000000000000`) and region (`--region`, default `us-east-1`).

Assumed-role credentials return dummy access key IDs and secrets that are accepted by Gopherstack but are not valid for real AWS.

## Known Limitations

- Session policies and inline session policies are accepted but not enforced.
- MFA token validation is not implemented.
- `AssumeRoleWithWebIdentity` and `AssumeRoleWithSAML` are not supported.
