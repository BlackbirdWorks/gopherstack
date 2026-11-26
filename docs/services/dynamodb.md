# DynamoDB

In-memory DynamoDB with expressions, PartiQL, secondary indexes, transactions, streams, TTL, backups, global tables, import and export.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws dynamodb create-table --table-name Users \
    --attribute-definitions AttributeName=UserId,AttributeType=S \
    --key-schema AttributeName=UserId,KeyType=HASH \
    --billing-mode PAY_PER_REQUEST

aws dynamodb put-item --table-name Users \
    --item '{"UserId":{"S":"u123"},"Name":{"S":"Alice"},"Age":{"N":"30"}}'

aws dynamodb update-item --table-name Users \
    --key '{"UserId":{"S":"u123"}}' \
    --update-expression "SET Age = :a" \
    --expression-attribute-values '{":a":{"N":"31"}}'

aws dynamodb scan --table-name Users \
    --filter-expression "Age > :min" \
    --expression-attribute-values '{":min":{"N":"25"}}'

aws dynamodb transact-write-items --transact-items '[
  {"Put":{"TableName":"Users","Item":{"UserId":{"S":"u456"}}}},
  {"Delete":{"TableName":"Users","Key":{"UserId":{"S":"u789"}}}}]'
```

## Supported operations

| Area | Operations |
|------|------------|
| Tables | `CreateTable`, `DeleteTable`, `DescribeTable`, `ListTables`, `UpdateTable`, `DescribeEndpoints`, `DescribeLimits` |
| Items | `PutItem`, `GetItem`, `UpdateItem`, `DeleteItem`, `Query`, `Scan`, `BatchGetItem`, `BatchWriteItem`, `TransactGetItems`, `TransactWriteItems` |
| PartiQL | `ExecuteStatement`, `BatchExecuteStatement`, `ExecuteTransaction` |
| TTL | `DescribeTimeToLive`, `UpdateTimeToLive` |
| Backups | `CreateBackup`, `DeleteBackup`, `DescribeBackup`, `ListBackups`, `RestoreTableFromBackup`, `RestoreTableToPointInTime`, `DescribeContinuousBackups`, `UpdateContinuousBackups` |
| Import and export | `ExportTableToPointInTime`, `DescribeExport`, `ListExports`, `ImportTable`, `DescribeImport`, `ListImports` |
| Global tables | `CreateGlobalTable`, `DescribeGlobalTable`, `ListGlobalTables`, `UpdateGlobalTable`, `DescribeGlobalTableSettings`, `UpdateGlobalTableSettings` |
| Auto scaling | `DescribeTableReplicaAutoScaling`, `UpdateTableReplicaAutoScaling` |
| Contributor Insights | `DescribeContributorInsights`, `ListContributorInsights`, `UpdateContributorInsights` |
| Kinesis streaming | `EnableKinesisStreamingDestination`, `DisableKinesisStreamingDestination`, `DescribeKinesisStreamingDestination`, `UpdateKinesisStreamingDestination` |
| Resource policies | `PutResourcePolicy`, `GetResourcePolicy`, `DeleteResourcePolicy` |
| Tags | `TagResource`, `UntagResource`, `ListTagsOfResource` |
| Other | `SearchVectors` |

Streams (`DescribeStream`, `GetShardIterator`, `GetRecords`, `ListStreams`) are served as the separate DynamoDB Streams service on the same port.

## Configuration

| Flag / env | Default | Purpose |
|------------|---------|---------|
| `DYNAMODB_REGION` | `us-east-1` | Default region |
| `DYNAMODB_JANITOR_INTERVAL` | `500ms` | Janitor tick |
| `DYNAMODB_TTL_SWEEP_BATCH_SIZE` | `1000` | Items checked per lock acquisition during a TTL sweep |
| `DYNAMODB_CREATE_DELAY` | `0s` | Simulated `CREATING` to `ACTIVE` delay; `0` makes tables active immediately |
| `DYNAMODB_ENFORCE_THROUGHPUT` | `false` | Enforce provisioned throughput with a per-table token bucket |
| `GOPHERSTACK_ENFORCE_SERVICE_QUOTAS` | `false` | Reject table creates beyond the Service Quotas limit |

## Differences from AWS

- TTL deletes expired items in a background sweep (every 5 s), not at the exact expiry time.
- Export and import read and write through the S3 emulator.
- Not modelled: `INACCESSIBLE_ENCRYPTION_CREDENTIALS` replica state (no live KMS key check), `BackupExpiryDateTime` (only for system backups, which are never created), and Contributor Insights `FailureException` (no insights processing to fail).
