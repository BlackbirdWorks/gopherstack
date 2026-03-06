# Kinesis Data Streams

In-memory Kinesis implementation supporting stream creation, shard management, and record production/consumption.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateStream` | Create a new Kinesis stream |
| `DeleteStream` | Delete a stream |
| `DescribeStream` | Get detailed stream description including shards |
| `DescribeStreamSummary` | Get a summary description of the stream |
| `ListStreams` | List all streams |
| `PutRecord` | Write a single record to a stream |
| `PutRecords` | Write up to 500 records in one request |
| `GetShardIterator` | Get a shard iterator for consuming records |
| `GetRecords` | Read records from a shard |
| `ListShards` | List shards in a stream |
| `AddTagsToStream` | Add tags to a stream |
| `RemoveTagsFromStream` | Remove tags from a stream |
| `ListTagsForStream` | List tags on a stream |

## AWS CLI Examples

```bash
# Create a stream with 2 shards
aws --endpoint-url http://localhost:8000 kinesis create-stream \
    --stream-name my-stream \
    --shard-count 2

# Put a record
aws --endpoint-url http://localhost:8000 kinesis put-record \
    --stream-name my-stream \
    --data "SGVsbG8gV29ybGQ=" \
    --partition-key "partition-1"

# Get a shard iterator
SHARD_ITERATOR=$(aws --endpoint-url http://localhost:8000 kinesis get-shard-iterator \
    --stream-name my-stream \
    --shard-id shardId-000000000000 \
    --shard-iterator-type TRIM_HORIZON \
    --query ShardIterator --output text)

# Read records
aws --endpoint-url http://localhost:8000 kinesis get-records \
    --shard-iterator "$SHARD_ITERATOR"

# List shards
aws --endpoint-url http://localhost:8000 kinesis list-shards \
    --stream-name my-stream
```

## Known Limitations

- Shard splitting and merging are not supported.
- Enhanced fan-out (RegisterStreamConsumer) is not implemented.
- Stream retention period is not enforced; all records are kept in memory.
- Kinesis Data Analytics integration is not available.
