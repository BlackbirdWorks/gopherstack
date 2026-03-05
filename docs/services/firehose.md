# Kinesis Data Firehose

In-memory Firehose implementation supporting delivery stream management and record ingestion.

## Supported Operations

| Operation | Description |
|-----------|-------------|
| `CreateDeliveryStream` | Create a new delivery stream |
| `DeleteDeliveryStream` | Delete a delivery stream |
| `DescribeDeliveryStream` | Get stream configuration and status |
| `ListDeliveryStreams` | List all delivery streams |
| `PutRecord` | Write a single record to a stream |
| `PutRecordBatch` | Write up to 500 records in one request |
| `ListTagsForDeliveryStream` | List tags on a stream |
| `TagDeliveryStream` | Add tags to a stream |
| `UntagDeliveryStream` | Remove tags from a stream |

## AWS CLI Examples

```bash
# Create a delivery stream (S3 destination stub)
aws --endpoint-url http://localhost:8000 firehose create-delivery-stream \
    --delivery-stream-name my-stream \
    --s3-destination-configuration '{
        "RoleARN":"arn:aws:iam::000000000000:role/firehose",
        "BucketARN":"arn:aws:s3:::my-bucket"
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

# Describe a delivery stream
aws --endpoint-url http://localhost:8000 firehose describe-delivery-stream \
    --delivery-stream-name my-stream
```

## Known Limitations

- Records are accepted and acknowledged but **not delivered** to S3, Redshift, OpenSearch, or other destinations.
- Stream buffering, compression, and format conversion are not applied.
- Delivery failure and retry logic is not implemented.
- Data transformation via Lambda is not supported.
