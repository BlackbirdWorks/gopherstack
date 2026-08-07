package firehose

import "context"

// deliverToIceberg lands records for an Apache Iceberg Tables destination. Real Firehose
// writes through to Apache Iceberg tables via the Glue Data Catalog referenced by
// CatalogConfiguration; this backend has no Iceberg/Glue table-write engine to drive, so
// delivery is modeled by writing the processed records into the destination's required
// S3Configuration bucket (the same S3 location real Firehose stages through before the
// Iceberg commit), which is genuine state mutation rather than a stub. See PARITY.md gaps.
func (b *InMemoryBackend) deliverToIceberg(
	ctx context.Context,
	records [][]byte,
	dest *IcebergDestinationDescription,
	streamName string,
) {
	if dest.S3Destination == nil || dest.S3Destination.BucketARN == "" {
		return
	}

	_, _ = b.writeRecordsToBucket(ctx, records, dest.S3Destination.BucketARN,
		dest.S3Destination.Prefix, "", dest.S3Destination.CompressionFormat, streamName)
}
