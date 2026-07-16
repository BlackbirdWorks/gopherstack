package kinesis

import (
	"context"
	"time"
)

// StorageBackend defines the interface for a Kinesis backend.
//
// Every method takes a context.Context so the per-request AWS region can be
// threaded through and resources kept isolated per region. The region is read
// from the context via getRegion, falling back to the backend's default region
// when the context carries no region.
type StorageBackend interface {
	CreateStream(ctx context.Context, input *CreateStreamInput) error
	DeleteStream(ctx context.Context, input *DeleteStreamInput) error
	DescribeStream(ctx context.Context, input *DescribeStreamInput) (*DescribeStreamOutput, error)
	ListStreams(ctx context.Context, input *ListStreamsInput) (*ListStreamsOutput, error)
	PutRecord(ctx context.Context, input *PutRecordInput) (*PutRecordOutput, error)
	PutRecords(ctx context.Context, input *PutRecordsInput) (*PutRecordsOutput, error)
	GetShardIterator(ctx context.Context, input *GetShardIteratorInput) (*GetShardIteratorOutput, error)
	GetRecords(ctx context.Context, input *GetRecordsInput) (*GetRecordsOutput, error)
	ListShards(ctx context.Context, input *ListShardsInput) (*ListShardsOutput, error)
	RegisterStreamConsumer(
		ctx context.Context,
		input *RegisterStreamConsumerInput,
	) (*RegisterStreamConsumerOutput, error)
	DescribeStreamConsumer(
		ctx context.Context,
		input *DescribeStreamConsumerInput,
	) (*DescribeStreamConsumerOutput, error)
	ListStreamConsumers(ctx context.Context, input *ListStreamConsumersInput) (*ListStreamConsumersOutput, error)
	DeregisterStreamConsumer(ctx context.Context, input *DeregisterStreamConsumerInput) error
	SubscribeToShard(ctx context.Context, input *SubscribeToShardInput) (*SubscribeToShardOutput, error)
	UpdateShardCount(ctx context.Context, input *UpdateShardCountInput) (*UpdateShardCountOutput, error)
	EnableEnhancedMonitoring(
		ctx context.Context,
		input *EnableEnhancedMonitoringInput,
	) (*EnableEnhancedMonitoringOutput, error)
	DisableEnhancedMonitoring(
		ctx context.Context,
		input *DisableEnhancedMonitoringInput,
	) (*DisableEnhancedMonitoringOutput, error)
	IncreaseStreamRetentionPeriod(ctx context.Context, input *IncreaseStreamRetentionPeriodInput) error
	DecreaseStreamRetentionPeriod(ctx context.Context, input *DecreaseStreamRetentionPeriodInput) error
	MergeShards(ctx context.Context, input *MergeShardsInput) error
	SplitShard(ctx context.Context, input *SplitShardInput) error
	StartStreamEncryption(ctx context.Context, input *StartStreamEncryptionInput) error
	StopStreamEncryption(ctx context.Context, input *StopStreamEncryptionInput) error
	DeleteResourcePolicy(ctx context.Context, input *DeleteResourcePolicyInput) error
	GetResourcePolicy(ctx context.Context, input *GetResourcePolicyInput) (*GetResourcePolicyOutput, error)
	PutResourcePolicy(ctx context.Context, input *PutResourcePolicyInput) error
	ListTagsForResource(ctx context.Context, input *ListTagsForResourceInput) (*ListTagsForResourceOutput, error)
	TagResource(ctx context.Context, input *TagResourceInput) error
	UntagResource(ctx context.Context, input *UntagResourceInput) error
	UpdateStreamMode(ctx context.Context, input *UpdateStreamModeInput) error
	UpdateAccountSettings(ctx context.Context, input *UpdateAccountSettingsInput) error
	UpdateMaxRecordSize(ctx context.Context, input *UpdateMaxRecordSizeInput) error
	UpdateStreamWarmThroughput(ctx context.Context, input *UpdateStreamWarmThroughputInput) error
	DescribeAccountSettings(ctx context.Context) (*DescribeAccountSettingsOutput, error)
	CountOpenShards(ctx context.Context) int
	ListAll(ctx context.Context) []StreamInfo
}

// Compile-time assertion that InMemoryBackend implements StorageBackend.
var _ StorageBackend = (*InMemoryBackend)(nil)

// resetter is implemented by backends that support a full in-memory reset.
type resetter interface {
	Reset()
}

// purger is implemented by backends that support time-based purging.
type purger interface {
	Purge(ctx context.Context, cutoff time.Time)
}
