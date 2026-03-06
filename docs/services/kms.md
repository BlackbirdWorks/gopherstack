# KMS — Key Management Service

In-memory KMS implementation with symmetric key creation, encryption, decryption, data key generation, and key rotation status.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateKey` | Create a new symmetric KMS key |
| `DescribeKey` | Get key metadata |
| `ListKeys` | List all keys |
| `EnableKey` | Re-enable a disabled key |
| `DisableKey` | Disable a key (operations will fail) |
| `ScheduleKeyDeletion` | Schedule key deletion with a waiting period |
| `CancelKeyDeletion` | Cancel a pending key deletion |
| `Encrypt` | Encrypt plaintext using a KMS key |
| `Decrypt` | Decrypt ciphertext |
| `ReEncrypt` | Re-encrypt ciphertext under a different key |
| `GenerateDataKey` | Generate a data key (plaintext + encrypted copy) |
| `GenerateDataKeyWithoutPlaintext` | Generate an encrypted data key only |
| `EnableKeyRotation` | Enable automatic key rotation |
| `DisableKeyRotation` | Disable automatic key rotation |
| `GetKeyRotationStatus` | Check if rotation is enabled |
| `CreateAlias` | Create an alias for a key |
| `DeleteAlias` | Delete an alias |
| `ListAliases` | List all aliases |
| `CreateGrant` | Create a key grant (stored, not enforced) |

## AWS CLI Examples

```bash
# Create a key
aws --endpoint-url http://localhost:8000 kms create-key \
    --description "My encryption key"

# Encrypt data
aws --endpoint-url http://localhost:8000 kms encrypt \
    --key-id arn:aws:kms:us-east-1:000000000000:key/<key-id> \
    --plaintext "SGVsbG8gV29ybGQ=" \
    --query CiphertextBlob --output text

# Decrypt data
aws --endpoint-url http://localhost:8000 kms decrypt \
    --ciphertext-blob fileb://ciphertext.bin \
    --query Plaintext --output text

# Generate a data key for envelope encryption
aws --endpoint-url http://localhost:8000 kms generate-data-key \
    --key-id arn:aws:kms:us-east-1:000000000000:key/<key-id> \
    --key-spec AES_256

# Create an alias
aws --endpoint-url http://localhost:8000 kms create-alias \
    --alias-name alias/my-key \
    --target-key-id <key-id>

# List keys
aws --endpoint-url http://localhost:8000 kms list-keys
```

## Known Limitations

- Asymmetric keys and HMAC keys are not supported; only `SYMMETRIC_DEFAULT` (AES-256-GCM) is implemented.
- Key grants are stored but not enforced (all callers can use all keys).
- Automatic key rotation does not actually re-encrypt existing ciphertext.
- KMS key policies are not evaluated.
- AWS-managed keys (`aws/s3`, `aws/rds`, etc.) are not pre-created; create them manually if needed.
