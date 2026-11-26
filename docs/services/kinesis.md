# Kinesis Data Streams

In-memory Kinesis Data Streams with shard split and merge, enhanced fan-out consumers, retention and encryption settings.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws kinesis create-stream --stream-name my-stream --shard-count 2
aws kinesis put-record --stream-name my-stream --data "SGVsbG8gV29ybGQ=" --partition-key partition-1

IT=$(aws kinesis get-shard-iterator --stream-name my-stream \
    --shard-id shardId-000000000000 --shard-iterator-type TRIM_HORIZON \
    --query ShardIterator --output text)
aws kinesis get-records --shard-iterator "$IT"
aws kinesis list-shards --stream-name my-stream
```

## Supported operations

| Area | Operations |
|------|------------|
| Streams | `CreateStream`, `DeleteStream`, `DescribeStream`, `DescribeStreamSummary`, `ListStreams`, `UpdateStreamMode`, `UpdateStreamWarmThroughput`, `UpdateMaxRecordSize` |
| Records | `PutRecord`, `PutRecords`, `GetShardIterator`, `GetRecords` |
| Shards | `ListShards`, `SplitShard`, `MergeShards`, `UpdateShardCount` |
| Consumers | `RegisterStreamConsumer`, `DescribeStreamConsumer`, `ListStreamConsumers`, `DeregisterStreamConsumer`, `SubscribeToShard` |
| Retention and encryption | `IncreaseStreamRetentionPeriod`, `DecreaseStreamRetentionPeriod`, `StartStreamEncryption`, `StopStreamEncryption` |
| Monitoring and limits | `EnableEnhancedMonitoring`, `DisableEnhancedMonitoring`, `DescribeLimits`, `DescribeAccountSettings`, `UpdateAccountSettings` |
| Resource policies | `PutResourcePolicy`, `GetResourcePolicy`, `DeleteResourcePolicy` |
| Tags | `AddTagsToStream`, `RemoveTagsFromStream`, `ListTagsForStream`, `TagResource`, `UntagResource`, `ListTagsForResource` |
| Channels | `CreateChannel`, `DescribeChannel`, `ListChannels`, `UpdateChannel`, `DeleteChannel` |

A janitor evicts records older than the stream's retention period.

## Configuration

| Flag | Env | Default | Purpose |
|------|-----|---------|---------|
| `--kinesis-latency` | `KINESIS_RESPONSE_LATENCY` | `0s` | Artificial delay before each API response |
| `--kinesis-shard-limit` | `KINESIS_ACCOUNT_SHARD_LIMIT` | `0` | Open shard limit. `0` is the AWS default of 500, negative is unlimited |
| `--kinesis-on-demand-stream-count-limit` | `KINESIS_ACCOUNT_ON_DEMAND_STREAM_LIMIT` | `0` | On-demand stream limit. `0` is the AWS default of 10, negative is unlimited |
| none | `KINESIS_JANITOR_INTERVAL` | `1m` | Janitor tick |

## Differences from AWS

> [!NOTE]
> A channel with an S3 Tables (Iceberg) destination accepts `PutRecord` but never buffers or flushes. Plain S3 delivery works.

- `ReadProvisionedThroughputExceeded` and `WriteProvisionedThroughputExceeded` occur only through the FIS throughput fault. There is no per-shard throughput model.
- Channel control-plane throttling (`LimitExceededException`) is not applied.
- `KMSAccessDeniedException` on `StartStreamEncryption` and `StopStreamEncryption` is never raised: there is no KMS key-grant model.
