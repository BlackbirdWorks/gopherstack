# Kinesis Data Firehose

In-memory Firehose implementation supporting delivery stream management, record ingestion, and actual S3 delivery with optional Lambda transformation.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateDeliveryStream` | Create a new delivery stream with optional S3 destination and Lambda transformation |
| `DeleteDeliveryStream` | Delete a delivery stream |
| `DescribeDeliveryStream` | Get stream configuration, status, and destination config |
| `ListDeliveryStreams` | List all delivery streams |
| `PutRecord` | Write a single record to a stream |
| `PutRecordBatch` | Write up to 500 records in one request |
| `UpdateDestination` | Modify delivery stream destination configuration |
| `ListTagsForDeliveryStream` | List tags on a stream |
| `TagDeliveryStream` | Add tags to a stream |
| `UntagDeliveryStream` | Remove tags from a stream |

## S3 Delivery

When a stream is created with `S3DestinationConfiguration` or `ExtendedS3DestinationConfiguration`, records are buffered in memory and flushed to S3 when:

- **Size threshold** — the buffer reaches the configured `SizeInMBs` (default: 5 MB)
- **Interval threshold** — the `IntervalInSeconds` since the last flush elapses (default: 300 s, checked every second by a background worker)
- **FlushAll** — called programmatically (useful for tests or graceful shutdown)

S3 object keys use timestamp partitioning: `[prefix/]YYYY/MM/DD/HH/stream-name-YYYY-MM-DD-HH-MM-SS`

### Supported Compression Formats

| Format | Notes |
|--------|-------|
| `UNCOMPRESSED` | Default — raw concatenated records |
| `GZIP` | gzip-compressed with `Content-Encoding: gzip` |

## Lambda Transformation

When `ProcessingConfiguration` is enabled with a Lambda processor, Firehose:

1. Batches all buffered records into a Lambda invocation payload
2. Invokes the configured Lambda function synchronously
3. Parses the response: records marked `Ok` are delivered to S3, `Dropped` and `ProcessingFailed` records are discarded

Lambda event format:
```json
{
  "invocationId": "...",
  "deliveryStreamArn": "...",
  "region": "us-east-1",
  "records": [
    {
      "recordId": "...",
      "approximateArrivalTimestamp": 1234567890,
      "data": "base64-encoded-data"
    }
  ]
}
```

## AWS CLI Examples

```bash
# Create a delivery stream with S3 destination
aws --endpoint-url http://localhost:8000 firehose create-delivery-stream \
    --delivery-stream-name my-stream \
    --s3-destination-configuration '{
        "RoleARN":"arn:aws:iam::000000000000:role/firehose",
        "BucketARN":"arn:aws:s3:::my-bucket",
        "BufferingHints":{"SizeInMBs":5,"IntervalInSeconds":60},
        "CompressionFormat":"GZIP"
    }'

# Create with Lambda transformation (ExtendedS3)
aws --endpoint-url http://localhost:8000 firehose create-delivery-stream \
    --delivery-stream-name transform-stream \
    --extended-s3-destination-configuration '{
        "RoleARN":"arn:aws:iam::000000000000:role/firehose",
        "BucketARN":"arn:aws:s3:::my-bucket",
        "ProcessingConfiguration":{
            "Enabled":true,
            "Processors":[{
                "Type":"Lambda",
                "Parameters":[{
                    "ParameterName":"LambdaArn",
                    "ParameterValue":"my-transform-fn"
                }]
            }]
        }
    }'

# Put a record
aws --endpoint-url http://localhost:8000 firehose put-record \
    --delivery-stream-name my-stream \
    --record '{"Data":"eyJrZXkiOiJ2YWx1ZSJ9"}'

# Put a batch of records
aws --endpoint-url http://localhost:8000 firehose put-record-batch \
    --delivery-stream-name my-stream \
    --records '[{"Data":"eyJpZCI6MX0="},{"Data":"eyJpZCI6Mn0="}]'

# List delivery streams
aws --endpoint-url http://localhost:8000 firehose list-delivery-streams

# Describe a delivery stream (includes S3DestinationDescriptions)
aws --endpoint-url http://localhost:8000 firehose describe-delivery-stream \
    --delivery-stream-name my-stream

# Update destination
aws --endpoint-url http://localhost:8000 firehose update-destination \
    --delivery-stream-name my-stream \
    --current-delivery-stream-version-id 1 \
    --destination-id destinationId-000000000001 \
    --s3-destination-update '{"BucketARN":"arn:aws:s3:::new-bucket"}'
```

## Known Limitations

- Only S3 delivery is implemented; Redshift, OpenSearch, and HTTP endpoint destinations are not supported.
- Only UNCOMPRESSED and GZIP compression formats are implemented (ZIP and Snappy are not).
- S3BackupConfiguration (error output bucket) is not implemented.
- Delivery failure and retry logic is not implemented.
