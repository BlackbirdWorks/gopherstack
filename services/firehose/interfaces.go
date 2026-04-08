package firehose

import "context"

// StorageBackend defines the interface for Firehose backend implementations.
// All mutating methods must be safe for concurrent use.
type StorageBackend interface {
	CreateDeliveryStream(input CreateDeliveryStreamInput) (*DeliveryStream, error)
	DeleteDeliveryStream(name string) error
	DescribeDeliveryStream(name string) (*DeliveryStream, error)
	ListDeliveryStreams() []string
	PutRecord(streamName string, data []byte) error
	PutRecordBatch(streamName string, records [][]byte) (int, error)
	UpdateDestination(streamName string, dest *S3DestinationDescription) error
	ListTagsForDeliveryStream(name string) (map[string]string, error)
	TagDeliveryStream(name string, kv map[string]string) error
	UntagDeliveryStream(name string, keys []string) error
	StartDeliveryStreamEncryption(ctx context.Context, name string, input *EncryptionConfigInput) error
	StopDeliveryStreamEncryption(ctx context.Context, name string) error
}
