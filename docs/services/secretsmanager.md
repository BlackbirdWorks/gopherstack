# Secrets Manager

Full in-memory Secrets Manager implementation with versioning, rotation, and tagging.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateSecret` | Create a new secret (string or binary) |
| `GetSecretValue` | Retrieve the current or a specific version of a secret |
| `PutSecretValue` | Store a new version of a secret |
| `UpdateSecret` | Update secret metadata or value |
| `DescribeSecret` | Get secret metadata and version list |
| `ListSecrets` | List all secrets with optional filters |
| `DeleteSecret` | Mark a secret for deletion (with optional recovery window) |
| `RestoreSecret` | Cancel a pending deletion |
| `RotateSecret` | Trigger secret rotation (calls a Lambda rotator) |
| `TagResource` | Add tags to a secret |
| `UntagResource` | Remove tags from a secret |

## AWS CLI Examples

```bash
# Create a string secret
aws --endpoint-url http://localhost:8000 secretsmanager create-secret \
    --name my-db-password \
    --secret-string "supersecret"

# Create a JSON secret
aws --endpoint-url http://localhost:8000 secretsmanager create-secret \
    --name my-db-config \
    --secret-string '{"username":"admin","password":"s3cr3t","host":"localhost"}'

# Get the secret value
aws --endpoint-url http://localhost:8000 secretsmanager get-secret-value \
    --secret-id my-db-password

# Update the secret value
aws --endpoint-url http://localhost:8000 secretsmanager put-secret-value \
    --secret-id my-db-password \
    --secret-string "newpassword"

# List secrets
aws --endpoint-url http://localhost:8000 secretsmanager list-secrets

# Delete a secret (immediate, no recovery window)
aws --endpoint-url http://localhost:8000 secretsmanager delete-secret \
    --secret-id my-db-password \
    --force-delete-without-recovery
```

## Known Limitations

- `RotateSecret` marks the secret as rotating but does not actually invoke a Lambda function unless Lambda is running and a rotation function is configured.
- Resource-based policies are not implemented.
- Cross-account replication is not supported.

## Persistence

Secrets Manager participates in Gopherstack's snapshot persistence. With `--persist` or `PERSIST=true`, all secrets survive restarts.
