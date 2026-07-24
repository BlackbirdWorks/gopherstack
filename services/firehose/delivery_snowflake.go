package firehose

import "context"

// deliverToSnowflake lands records for a Snowflake destination. Real Firehose connects
// directly to the configured Snowflake account/database/schema/table via Snowpipe
// Streaming; this backend has no live Snowflake connectivity to drive, so delivery is
// modeled by writing the processed records into the destination's required S3Configuration
// bucket (the same staging S3 location real Firehose uses ahead of the Snowflake load),
// which is genuine state mutation rather than a stub. See PARITY.md gaps.
func (b *InMemoryBackend) deliverToSnowflake(
	ctx context.Context,
	records [][]byte,
	dest *SnowflakeDestinationDescription,
	streamName string,
) {
	if dest.S3Destination == nil || dest.S3Destination.BucketARN == "" {
		return
	}

	_ = b.writeRecordsToBucket(ctx, records, dest.S3Destination.BucketARN,
		dest.S3Destination.Prefix, "", dest.S3Destination.CompressionFormat, streamName)
}
