# Secrets Manager

In-memory Secrets Manager with versions and staging labels, rotation, replication, resource policies and tags. Secrets survive restarts with `--persist`.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws secretsmanager create-secret --name my-db-config \
    --secret-string '{"username":"admin","password":"s3cr3t"}'
aws secretsmanager get-secret-value --secret-id my-db-config
aws secretsmanager put-secret-value --secret-id my-db-config --secret-string "new"
aws secretsmanager list-secrets
aws secretsmanager delete-secret --secret-id my-db-config --force-delete-without-recovery
```

## Supported operations

| Area | Operations |
|------|------------|
| Secrets | `CreateSecret`, `DescribeSecret`, `ListSecrets`, `UpdateSecret`, `DeleteSecret`, `RestoreSecret` |
| Values and versions | `GetSecretValue`, `BatchGetSecretValue`, `PutSecretValue`, `ListSecretVersionIds`, `UpdateSecretVersionStage`, `GetRandomPassword` |
| Rotation | `RotateSecret`, `CancelRotateSecret` |
| Replication | `ReplicateSecretToRegions`, `RemoveRegionsFromReplication`, `StopReplicationToReplica` |
| Resource policies | `PutResourcePolicy`, `GetResourcePolicy`, `DeleteResourcePolicy`, `ValidateResourcePolicy` |
| Tags | `TagResource`, `UntagResource` |

## Behaviour

- `RotateSecret` runs the four-step Lambda rotation and moves `AWSPENDING` to `AWSCURRENT`. It needs the Lambda service and a configured rotation function. Scheduled rotation follows `RotationRules`.

## Differences from AWS

> [!NOTE]
> `PutSecretValue` `RotationToken` is not validated: there is no cross-account or assumed-role rotation identity to check it against.
