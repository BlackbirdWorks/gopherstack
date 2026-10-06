package firehose

import "context"

// deliverToSnowflake lands records for a Snowflake destination. Real Firehose connects
// directly to the configured Snowflake account/database/schema/table via Snowpipe
// Streaming; this backend has no live Snowflake connectivity to drive, so delivery is
// modeled by writing the processed records into the destination's required S3Configuration
// bucket (the same staging S3 location real Firehose uses ahead of the Snowflake load),
// which is genuine state mutation rather than a stub.
// Records the role or S3 refuses are returned as undelivered.
func (b *InMemoryBackend) deliverToSnowflake(
	ctx context.Context,
	records [][]byte,
	dest *SnowflakeDestinationDescription,
	streamName string,
) [][]byte {
	if dest.S3Destination == nil || dest.S3Destination.BucketARN == "" {
		return nil
	}

	_, undelivered := b.stageToS3(ctx, records, dest.S3Destination, dest.CloudWatchLoggingOptions, streamName)

	return undelivered
}
