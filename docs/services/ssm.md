# SSM — Systems Manager Parameter Store

In-memory Parameter Store implementation supporting String, StringList, and SecureString parameters with hierarchy paths, versioning, and tagging.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `PutParameter` | Create or update a parameter |
| `GetParameter` | Get the value of a single parameter |
| `GetParameters` | Get values for up to 10 parameters |
| `GetParameterHistory` | Get all versions of a parameter |
| `GetParametersByPath` | Get all parameters under a path prefix |
| `DescribeParameters` | Describe parameters with optional filters |
| `DeleteParameter` | Delete a single parameter |
| `DeleteParameters` | Delete up to 10 parameters |
| `AddTagsToResource` | Add tags to a parameter |
| `RemoveTagsFromResource` | Remove tags from a parameter |
| `ListTagsForResource` | List tags on a parameter |

## AWS CLI Examples

```bash
# Create a plain string parameter
aws --endpoint-url http://localhost:8000 ssm put-parameter \
    --name "/myapp/config/region" \
    --value "us-east-1" \
    --type String

# Create a SecureString parameter
aws --endpoint-url http://localhost:8000 ssm put-parameter \
    --name "/myapp/secrets/db-password" \
    --value "supersecret" \
    --type SecureString

# Get a parameter value
aws --endpoint-url http://localhost:8000 ssm get-parameter \
    --name "/myapp/config/region"

# Get a SecureString with decryption
aws --endpoint-url http://localhost:8000 ssm get-parameter \
    --name "/myapp/secrets/db-password" \
    --with-decryption

# Get all parameters under a path
aws --endpoint-url http://localhost:8000 ssm get-parameters-by-path \
    --path "/myapp/config" \
    --recursive

# Update a parameter
aws --endpoint-url http://localhost:8000 ssm put-parameter \
    --name "/myapp/config/region" \
    --value "eu-west-1" \
    --type String \
    --overwrite
```

## Known Limitations

- SecureString parameters are stored in plaintext (KMS encryption is not applied).
- Advanced-tier parameters (large values, policies) are not distinguished from standard-tier.
- Parameter policies (expiration, notification) are not implemented.
