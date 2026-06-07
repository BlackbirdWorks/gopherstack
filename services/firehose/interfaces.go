package firehose

import "context"

// StorageBackend defines the interface for Firehose backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateDeliveryStream(ctx context.Context, input CreateDeliveryStreamInput) (*DeliveryStream, error)
	DeleteDeliveryStream(ctx context.Context, name string) error
	DescribeDeliveryStream(ctx context.Context, name string) (*DeliveryStream, error)
	ListDeliveryStreams(ctx context.Context) []string
	PutRecord(ctx context.Context, streamName string, data []byte) error
	PutRecordBatch(ctx context.Context, streamName string, records [][]byte) (int, error)
	UpdateDestination(ctx context.Context, streamName, currentVersionID string, dest *S3DestinationDescription) error
	ListTagsForDeliveryStream(ctx context.Context, name string) (map[string]string, error)
	TagDeliveryStream(ctx context.Context, name string, kv map[string]string) error
	UntagDeliveryStream(ctx context.Context, name string, keys []string) error
	StartDeliveryStreamEncryption(ctx context.Context, name string, input *EncryptionConfigInput) error
	StopDeliveryStreamEncryption(ctx context.Context, name string) error
	Reset()
	Region() string
	RunFlusher(ctx context.Context)
	FlushAll(ctx context.Context)
	Snapshot() []byte
	Restore(data []byte) error
	AddStreamInternal(s *DeliveryStream)
}
