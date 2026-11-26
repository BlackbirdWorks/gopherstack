# Kinesis Data Firehose

In-memory Firehose with buffered delivery to S3 and other destinations, Lambda transformation and Kinesis stream sources.

## Quick start

```bash
export AWS_ENDPOINT_URL=http://localhost:8000

aws firehose create-delivery-stream --delivery-stream-name my-stream \
    --s3-destination-configuration '{
        "RoleARN":"arn:aws:iam::000000000000:role/firehose",
        "BucketARN":"arn:aws:s3:::my-bucket",
        "BufferingHints":{"SizeInMBs":5,"IntervalInSeconds":60},
        "CompressionFormat":"GZIP"}'

aws firehose put-record --delivery-stream-name my-stream \
    --record '{"Data":"eyJrZXkiOiJ2YWx1ZSJ9"}'
aws firehose put-record-batch --delivery-stream-name my-stream \
    --records '[{"Data":"eyJpZCI6MX0="},{"Data":"eyJpZCI6Mn0="}]'
aws firehose describe-delivery-stream --delivery-stream-name my-stream
```

With Lambda transformation, add to `--extended-s3-destination-configuration`:

```json
"ProcessingConfiguration": {"Enabled": true, "Processors": [{
  "Type": "Lambda",
  "Parameters": [{"ParameterName": "LambdaArn", "ParameterValue": "my-transform-fn"}]}]}
```

## Supported operations

`CreateDeliveryStream`, `DeleteDeliveryStream`, `DescribeDeliveryStream`, `ListDeliveryStreams`, `UpdateDestination`, `PutRecord`, `PutRecordBatch`, `StartDeliveryStreamEncryption`, `StopDeliveryStreamEncryption`, `TagDeliveryStream`, `UntagDeliveryStream`, `ListTagsForDeliveryStream`.

## Delivery

- Records are buffered in memory and flushed to S3 when the buffer reaches `SizeInMBs` (default 5) or `IntervalInSeconds` (default 300) elapses.
- Keys are time-partitioned: `[prefix/]YYYY/MM/DD/HH/stream-name-YYYY-MM-DD-HH-MM-SS`.
- Compression formats: `UNCOMPRESSED`, `GZIP`, `ZIP`, `SNAPPY`, `HADOOP_SNAPPY`.
- Lambda transformation invokes the function synchronously per batch. Records returned `Ok` are delivered. `Dropped` and `ProcessingFailed` records are not.
- Destinations: S3, Extended S3, HTTP endpoint, Splunk, Redshift, Elasticsearch and OpenSearch, Iceberg and Snowflake.
- Kinesis stream sources are polled per shard.

## Differences from AWS

> [!NOTE]
> Iceberg and Snowflake destinations stage to S3: there is no Iceberg table writer or Snowpipe to deliver to. OpenSearch Serverless is rejected.

- MSK sources and database-source snapshot or CDC are not polled: they need a real Kafka consumer or database endpoint.
- `DeleteDeliveryStream` ignores `AllowForceDelete`.
- Role authorization covers S3, S3 backup, staging, Lambda and OpenSearch/Elasticsearch domain ARNs. Glue catalog and Snowflake calls have no backend to authorize against. HTTP and Splunk authenticate with an access key or HEC token, not the role.
