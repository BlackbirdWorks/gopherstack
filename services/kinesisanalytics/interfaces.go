package kinesisanalytics

import (
	"context"

	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
)

// KinesisStreamReader is the subset of Kinesis operations DiscoverInputSchema needs to sample
// records from a live Kinesis stream. Same narrow shape as services/firehose's own
// KinesisReader (services/firehose/interfaces.go): a sibling backend implements it, and cli.go
// wires the two together. Like firehose's KinesisReader, kinesis.InMemoryBackend does not
// satisfy this directly (its real methods are ctx+typed-struct shaped, e.g.
// GetRecords(ctx, *GetRecordsInput) (*GetRecordsOutput, error) -- see
// services/kinesis/records.go) so an adapter is needed at the wiring site.
type KinesisStreamReader interface {
	ListShards(streamName string) ([]string, error)
	GetShardIterator(streamName, shardID string) (string, error)
	GetRecords(shardIterator string, limit int) (records [][]byte, nextIterator string, err error)
}

// S3ObjectReader is the subset of S3 operations DiscoverInputSchema needs to sample an object's
// content. Uses the real SDK request/response shapes directly, so s3.InMemoryBackend.GetObject
// (services/s3/objects.go) satisfies this interface with no adapter needed -- the same pattern
// as services/firehose's S3Storer (services/firehose/interfaces.go).
type S3ObjectReader interface {
	GetObject(ctx context.Context, input *sdk_s3.GetObjectInput) (*sdk_s3.GetObjectOutput, error)
}
