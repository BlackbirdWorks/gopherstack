# KMS

In-memory KMS with symmetric, asymmetric and HMAC keys, aliases, grants, rotation, key import and custom key stores.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

KEY=$(aws kms create-key --description "My key" --query KeyMetadata.KeyId --output text)
aws kms create-alias --alias-name alias/my-key --target-key-id "$KEY"
aws kms encrypt --key-id alias/my-key --plaintext "SGVsbG8=" \
    --query CiphertextBlob --output text
aws kms generate-data-key --key-id alias/my-key --key-spec AES_256
aws kms enable-key-rotation --key-id "$KEY"
```

## Supported operations

| Area | Operations |
|------|------------|
| Keys | `CreateKey`, `DescribeKey`, `ListKeys`, `EnableKey`, `DisableKey`, `ScheduleKeyDeletion`, `CancelKeyDeletion`, `UpdateKeyDescription`, `ReplicateKey`, `UpdatePrimaryRegion`, `GetKeyLastUsage` |
| Crypto | `Encrypt`, `Decrypt`, `ReEncrypt`, `GenerateDataKey`, `GenerateDataKeyWithoutPlaintext`, `GenerateDataKeyPair`, `GenerateDataKeyPairWithoutPlaintext`, `GenerateRandom`, `Sign`, `Verify`, `GetPublicKey`, `DeriveSharedSecret`, `GenerateMac`, `VerifyMac` |
| Rotation | `EnableKeyRotation`, `DisableKeyRotation`, `GetKeyRotationStatus`, `RotateKeyOnDemand`, `ListKeyRotations` |
| Key material | `GetParametersForImport`, `ImportKeyMaterial`, `DeleteImportedKeyMaterial` |
| Aliases | `CreateAlias`, `UpdateAlias`, `DeleteAlias`, `ListAliases` |
| Grants | `CreateGrant`, `ListGrants`, `ListRetirableGrants`, `RetireGrant`, `RevokeGrant` |
| Key policy | `PutKeyPolicy`, `GetKeyPolicy`, `ListKeyPolicies` |
| Tags | `TagResource`, `UntagResource`, `ListResourceTags` |
| Custom key stores | `CreateCustomKeyStore`, `UpdateCustomKeyStore`, `DeleteCustomKeyStore`, `DescribeCustomKeyStores`, `ConnectCustomKeyStore`, `DisconnectCustomKeyStore` |

## Behaviour

- AWS managed keys (`alias/aws/s3`, `alias/aws/dynamodb`, ...) are created on first reference, as on AWS.
- `ScheduleKeyDeletion` enforces the 7-30 day window. A janitor purges keys past their deletion date and runs automatic rotations that are due.

## Configuration

| Env | Default | Purpose |
|-----|---------|---------|
| `KMS_JANITOR_INTERVAL` | `1m` | Janitor tick |

## Differences from AWS

> [!WARNING]
> IAM enforcement (`--enforce-iam`) evaluates identity and key policies only. Grants are not an authorization source there, so a grantee with no policy allow is denied.

- External key store proxies are never contacted. The `Xks*` and VPC endpoint service errors are not produced.
- `ListKeyRotations` omits multi-generation key-material fields.
