package kinesis

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	// streamStatusActive is the status when a stream is ready for use.
	streamStatusActive = "ACTIVE"

	// encryptionTypeKMS is the KMS encryption type.
	encryptionTypeKMS = "KMS"

	// encryptionTypeNone is the no-encryption type.
	encryptionTypeNone = "NONE"

	// defaultShardCount is the default number of shards for a new PROVISIONED stream.
	defaultShardCount = 1

	// defaultOnDemandShardCount is the number of shards AWS allocates to a
	// freshly created ON_DEMAND stream (capacity is auto-managed thereafter).
	defaultOnDemandShardCount = 4

	// defaultRetentionHours is the default retention period for a stream in hours.
	defaultRetentionHours = 24

	// maxRecordsPerShard is the maximum number of records stored per shard.
	maxRecordsPerShard = 10000

	// defaultMaxRecordSizeBytes is the default per-record data size limit (1 MiB).
	defaultMaxRecordSizeBytes = 1_048_576

	// absoluteMaxRecordSizeBytes is the maximum allowed record size after UpdateMaxRecordSize (10 MiB).
	absoluteMaxRecordSizeBytes = 10_485_760

	// iteratorTypeTrimHorizon reads from the oldest record.
	iteratorTypeTrimHorizon = "TRIM_HORIZON"
	// iteratorTypeLatest reads only new records after the iterator is created.
	iteratorTypeLatest = "LATEST"
	// iteratorTypeAtSequenceNumber reads starting at the given sequence number.
	iteratorTypeAtSequenceNumber = "AT_SEQUENCE_NUMBER"
	// iteratorTypeAfterSequenceNumber reads after the given sequence number.
	iteratorTypeAfterSequenceNumber = "AFTER_SEQUENCE_NUMBER"
	// iteratorTypeAtTimestamp reads starting at the given timestamp.
	iteratorTypeAtTimestamp = "AT_TIMESTAMP"

	// maxGetRecordsLimit is the maximum number of records per GetRecords call.
	maxGetRecordsLimit = 10000
	// defaultGetRecordsLimit is the default limit for GetRecords.
	defaultGetRecordsLimit = 1000
	// maxGetRecordsResponseBytes is the AWS 10 MiB cap on GetRecords response payload.
	maxGetRecordsResponseBytes = 10 * 1024 * 1024

	// millisPerSecond is the number of milliseconds in one second.
	// Used to convert between Unix second timestamps (float64) and millisecond timestamps.
	millisPerSecond = 1000.0

	// maxHashKeyBits is the bit-width of the Kinesis hash key space.
	maxHashKeyBits = 128
	// maxShardCount is the maximum number of shards allowed in a stream.
	maxShardCount = 1000

	// kinesisDefaultShardLimit is the default account-level shard limit.
	kinesisDefaultShardLimit = 500

	// defaultOnDemandStreamCountLimit is the default limit for on-demand streams.
	defaultOnDemandStreamCountLimit = 10

	// hashKeyDecimalBase is the base used for parsing Kinesis hash key strings.
	hashKeyDecimalBase = 10

	// minRetentionHours is the minimum retention period AWS allows (24 h).
	minRetentionHours = 24
	// maxRetentionHours is the maximum retention period AWS allows (8 760 h = 365 days).
	maxRetentionHours = 8760

	// consumerStatusActive is the status when a consumer is ready for use.
	consumerStatusActive = "ACTIVE"

	// maxConsumersPerStream is the AWS limit on registered enhanced fan-out
	// consumers per stream.
	maxConsumersPerStream = 20

	// scalingTypeUniformScaling is the only supported scaling type for UpdateShardCount.
	scalingTypeUniformScaling = "UNIFORM_SCALING"

	// StreamModeProvisioned is the PROVISIONED stream mode.
	StreamModeProvisioned = "PROVISIONED"
	// StreamModeOnDemand is the ON_DEMAND stream mode.
	StreamModeOnDemand = "ON_DEMAND"

	// maxShardsPerStream is the per-stream limit on shard count enforced at creation time.
	maxShardsPerStream = 100

	// maxTagsPerStream is the maximum number of tags AWS allows per stream.
	maxTagsPerStream = 50

	// maxPartitionKeyLen is the maximum allowed partition key length in bytes.
	maxPartitionKeyLen = 256

	// streamStatusDeleting is the status when a stream is being deleted.
	streamStatusDeleting = "DELETING"

	// iteratorTTL is the maximum age of a shard iterator before it expires.
	iteratorTTL = 300 * time.Second
)

const (
	streamModeProvisioned = StreamModeProvisioned
	streamModeOnDemand    = StreamModeOnDemand
)

// Stream represents an in-memory Kinesis stream.
type Stream struct {
	CreatedAt time.Time `json:"createdAt"`
	mu        *lockmetrics.RWMutex
	Tags      *tags.Tags           `json:"tags,omitempty"`
	Consumers map[string]*Consumer `json:"consumers,omitempty"`
	Name      string               `json:"name"`
	ARN       string               `json:"arn"`
	// Region is the AWS region this stream lives in. It is the second half of
	// the composite key (see streamKey in backend.go) that keeps same-named
	// streams in different regions isolated inside the single flat
	// store.Table[Stream] — the region-nested map it replaced used the
	// region as an outer map key instead of a field on Stream itself.
	Region             string   `json:"region,omitempty"`
	Status             string   `json:"status"`
	EncryptionType     string   `json:"encryptionType,omitempty"`
	KeyID              string   `json:"keyId,omitempty"`
	StreamMode         string   `json:"streamMode,omitempty"`
	Shards             []*Shard `json:"shards"`
	EnhancedMonitoring []string `json:"enhancedMonitoring,omitempty"`
	RetentionPeriod    int      `json:"retentionPeriod"`
	// MaxRecordSizeBytes is the per-record data payload size limit for this stream.
	// Defaults to defaultMaxRecordSizeBytes (1 MiB); updatable via UpdateMaxRecordSize.
	MaxRecordSizeBytes int `json:"maxRecordSizeBytes,omitempty"`
}

// Shard represents a single Kinesis shard within a stream.
type Shard struct {
	ID                    string       `json:"id"`
	HashKeyRangeStart     string       `json:"hashKeyRangeStart"`
	HashKeyRangeEnd       string       `json:"hashKeyRangeEnd"`
	ParentShardID         string       `json:"parentShardId,omitempty"`
	AdjacentParentShardID string       `json:"adjacentParentShardId,omitempty"`
	Records               shardRecords `json:"records"`
	NextSeq               uint64       `json:"nextSeq"`
	Closed                bool         `json:"closed,omitempty"`
}

// Record represents a single Kinesis data record.
type Record struct {
	ApproximateArrivalTimestamp time.Time `json:"approximateArrivalTimestamp"`
	PartitionKey                string    `json:"partitionKey"`
	SequenceNumber              string    `json:"sequenceNumber"`
	Data                        []byte    `json:"data"`
}

// StreamInfo holds summary information about a stream, safe to return without lock.
type StreamInfo struct {
	Name       string
	ARN        string
	Status     string
	ShardCount int
}

// ShardIterator holds the position within a shard for GetRecords.
// Region is encoded into the iterator token so that GetRecords resolves the
// record store of the same region the iterator was issued in, keeping
// same-named streams in different regions isolated on the record hot path.
type ShardIterator struct {
	CreatedAt      time.Time `json:"CreatedAt"`
	StreamName     string    `json:"StreamName"`
	ShardID        string    `json:"ShardID"`
	SequenceNumber string    `json:"SequenceNumber"`
	Region         string    `json:"Region"`
	Position       int       `json:"Position"`
}

// --- Input/Output types ---

// CreateStreamInput is the input for CreateStream.
type CreateStreamInput struct {
	StreamName string
	Region     string
	AccountID  string
	StreamMode string
	ShardCount int
}

// DeleteStreamInput is the input for DeleteStream.
type DeleteStreamInput struct {
	StreamName string
}

// DescribeStreamInput is the input for DescribeStream.
type DescribeStreamInput struct {
	StreamName string
	// ExclusiveStartShardID resumes shard pagination after the given shard ID.
	ExclusiveStartShardID string
	// Limit caps the number of ShardDescription entries returned (AWS default
	// 100, max 10000). Zero means "use the AWS default".
	Limit int
}

// DescribeStreamOutput is the output for DescribeStream.
type DescribeStreamOutput struct {
	StreamCreationTimestamp time.Time
	StreamName              string
	StreamARN               string
	StreamStatus            string
	EncryptionType          string
	StreamMode              string
	KeyID                   string
	Shards                  []ShardDescription
	EnhancedMonitoring      []string
	RetentionPeriodHours    int
	// HasMoreShards indicates the shard list was truncated by Limit and more
	// shards can be fetched with a follow-up call using ExclusiveStartShardID.
	HasMoreShards bool
}

// ShardDescription describes a shard in a DescribeStream response.
type ShardDescription struct {
	ShardID                  string
	HashKeyRangeStart        string
	HashKeyRangeEnd          string
	SequenceNumberRangeStart string
	SequenceNumberRangeEnd   string
	ParentShardID            string
	AdjacentParentShardID    string
	Closed                   bool
}

// ListStreamsInput is the input for ListStreams.
type ListStreamsInput struct {
	NextToken                string
	ExclusiveStartStreamName string
	Limit                    int
}

// ListStreamsOutput is the output for ListStreams.
type ListStreamsOutput struct {
	NextToken      string
	StreamNames    []string
	HasMoreStreams bool
}

// PutRecordInput is the input for PutRecord.
type PutRecordInput struct {
	StreamName      string
	PartitionKey    string
	ExplicitHashKey string
	Data            []byte
}

// PutRecordOutput is the output for PutRecord.
type PutRecordOutput struct {
	ShardID        string
	SequenceNumber string
	EncryptionType string
}

// PutRecordsEntry is a single entry in a PutRecords request.
type PutRecordsEntry struct {
	PartitionKey    string
	ExplicitHashKey string
	Data            []byte
}

// PutRecordsResultEntry is a single result entry in a PutRecords response.
type PutRecordsResultEntry struct {
	ShardID        string
	SequenceNumber string
	ErrorCode      string
	ErrorMessage   string
}

// PutRecordsInput is the input for PutRecords.
type PutRecordsInput struct {
	StreamName string
	Records    []PutRecordsEntry
}

// PutRecordsOutput is the output for PutRecords.
type PutRecordsOutput struct {
	Records           []PutRecordsResultEntry
	FailedRecordCount int
}

// GetShardIteratorInput is the input for GetShardIterator.
type GetShardIteratorInput struct {
	Timestamp              time.Time
	StreamName             string
	ShardID                string
	ShardIteratorType      string
	StartingSequenceNumber string
}

// GetShardIteratorOutput is the output for GetShardIterator.
type GetShardIteratorOutput struct {
	ShardIterator string
}

// GetRecordsInput is the input for GetRecords.
type GetRecordsInput struct {
	ShardIterator string
	Limit         int
}

// GetRecordResult is a single record returned by GetRecords.
type GetRecordResult struct {
	ApproximateArrivalTimestamp time.Time
	PartitionKey                string
	SequenceNumber              string
	Data                        []byte
}

// GetRecordsOutput is the output for GetRecords.
type GetRecordsOutput struct {
	NextShardIterator  string
	Records            []GetRecordResult
	MillisBehindLatest int64
}

// ListShardsInput is the input for ListShards.
type ListShardsInput struct {
	StreamName            string
	NextToken             string
	ExclusiveStartShardID string
	// ShardFilter controls which shards are returned.
	// Supported values: "FROM_TRIM_HORIZON" (all shards including closed),
	// "AT_LATEST" (open shards only), "AFTER_SHARD_ID", "AT_TIMESTAMP", "FROM_TIMESTAMP".
	// Empty string defaults to open shards only.
	ShardFilter        string
	ShardFilterType    string
	ShardFilterShardID string
	MaxResults         int
}

// ListShardsOutput is the output for ListShards.
type ListShardsOutput struct {
	NextToken string
	Shards    []ShardDescription
}

// Consumer represents a registered Kinesis enhanced fan-out consumer.
type Consumer struct {
	ConsumerCreationTimestamp time.Time `json:"consumerCreationTimestamp"`
	ConsumerName              string    `json:"consumerName"`
	ConsumerARN               string    `json:"consumerARN"`
	ConsumerStatus            string    `json:"consumerStatus"`
	StreamARN                 string    `json:"streamARN"`
}

// RegisterStreamConsumerInput is the input for RegisterStreamConsumer.
type RegisterStreamConsumerInput struct {
	StreamARN    string
	ConsumerName string
}

// RegisterStreamConsumerOutput is the output for RegisterStreamConsumer.
type RegisterStreamConsumerOutput struct {
	Consumer Consumer
}

// DescribeStreamConsumerInput is the input for DescribeStreamConsumer.
type DescribeStreamConsumerInput struct {
	StreamARN    string
	ConsumerARN  string
	ConsumerName string
}

// DescribeStreamConsumerOutput is the output for DescribeStreamConsumer.
type DescribeStreamConsumerOutput struct {
	ConsumerDescription Consumer
}

// ListStreamConsumersInput is the input for ListStreamConsumers.
type ListStreamConsumersInput struct {
	StreamARN  string
	NextToken  string
	MaxResults int
}

// ListStreamConsumersOutput is the output for ListStreamConsumers.
type ListStreamConsumersOutput struct {
	NextToken string
	Consumers []Consumer
}

// DeregisterStreamConsumerInput is the input for DeregisterStreamConsumer.
type DeregisterStreamConsumerInput struct {
	StreamARN    string
	ConsumerARN  string
	ConsumerName string
}

// StartingPosition describes where to start reading in SubscribeToShard.
type StartingPosition struct {
	Timestamp      *time.Time `json:"Timestamp,omitempty"`
	Type           string     `json:"Type"`
	SequenceNumber string     `json:"SequenceNumber,omitempty"`
}

// SubscribeToShardInput is the input for SubscribeToShard.
type SubscribeToShardInput struct {
	ConsumerARN      string
	ShardID          string
	StartingPosition StartingPosition
}

// SubscribeToShardEvent is a single event in the SubscribeToShard response.
type SubscribeToShardEvent struct {
	ContinuationSequenceNumber string
	Records                    []GetRecordResult
	MillisBehindLatest         int64
}

// SubscribeToShardOutput is the output for SubscribeToShard.
type SubscribeToShardOutput struct {
	Event SubscribeToShardEvent
}

// UpdateShardCountInput is the input for UpdateShardCount.
type UpdateShardCountInput struct {
	StreamName       string
	ScalingType      string
	TargetShardCount int
}

// UpdateShardCountOutput is the output for UpdateShardCount.
type UpdateShardCountOutput struct {
	StreamName        string
	CurrentShardCount int
	TargetShardCount  int
}

// EnableEnhancedMonitoringInput is the input for EnableEnhancedMonitoring.
type EnableEnhancedMonitoringInput struct {
	StreamName        string
	ShardLevelMetrics []string
}

// EnableEnhancedMonitoringOutput is the output for EnableEnhancedMonitoring.
type EnableEnhancedMonitoringOutput struct {
	StreamName               string
	CurrentShardLevelMetrics []string
	DesiredShardLevelMetrics []string
}

// DisableEnhancedMonitoringInput is the input for DisableEnhancedMonitoring.
type DisableEnhancedMonitoringInput struct {
	StreamName        string
	ShardLevelMetrics []string
}

// DisableEnhancedMonitoringOutput is the output for DisableEnhancedMonitoring.
type DisableEnhancedMonitoringOutput struct {
	StreamName               string
	CurrentShardLevelMetrics []string
	DesiredShardLevelMetrics []string
}

// IncreaseStreamRetentionPeriodInput is the input for IncreaseStreamRetentionPeriod.
type IncreaseStreamRetentionPeriodInput struct {
	StreamName           string
	RetentionPeriodHours int
}

// DecreaseStreamRetentionPeriodInput is the input for DecreaseStreamRetentionPeriod.
type DecreaseStreamRetentionPeriodInput struct {
	StreamName           string
	RetentionPeriodHours int
}

// MergeShardsInput is the input for MergeShards.
type MergeShardsInput struct {
	StreamName           string
	StreamARN            string
	ShardToMerge         string
	AdjacentShardToMerge string
}

// SplitShardInput is the input for SplitShard.
type SplitShardInput struct {
	StreamName         string
	StreamARN          string
	ShardToSplit       string
	NewStartingHashKey string
}

// StartStreamEncryptionInput is the input for StartStreamEncryption.
type StartStreamEncryptionInput struct {
	StreamName     string
	StreamARN      string
	EncryptionType string
	KeyID          string
}

// StopStreamEncryptionInput is the input for StopStreamEncryption.
type StopStreamEncryptionInput struct {
	StreamName     string
	StreamARN      string
	EncryptionType string
	KeyID          string
}

// DeleteResourcePolicyInput is the input for DeleteResourcePolicy.
type DeleteResourcePolicyInput struct {
	ResourceARN string
}

// GetResourcePolicyInput is the input for GetResourcePolicy.
type GetResourcePolicyInput struct {
	ResourceARN string
}

// GetResourcePolicyOutput is the output for GetResourcePolicy.
type GetResourcePolicyOutput struct {
	Policy string
}

// PutResourcePolicyInput is the input for PutResourcePolicy.
type PutResourcePolicyInput struct {
	ResourceARN string
	Policy      string
}

// ListTagsForResourceInput is the input for ListTagsForResource.
type ListTagsForResourceInput struct {
	ResourceARN string
}

// ListTagsForResourceOutput is the output for ListTagsForResource.
type ListTagsForResourceOutput struct {
	Tags map[string]string
}

// DescribeAccountSettingsOutput is the output for DescribeAccountSettings.
type DescribeAccountSettingsOutput struct {
	ShardLimit               int
	OnDemandStreamCount      int
	OnDemandStreamCountLimit int
}

// UpdateStreamModeInput is the input for UpdateStreamMode.
type UpdateStreamModeInput struct {
	StreamARN         string
	StreamModeDetails StreamModeDetails
}

// StreamModeDetails describes the mode of a Kinesis stream.
type StreamModeDetails struct {
	StreamMode string
}

// UpdateAccountSettingsInput is the input for UpdateAccountSettings.
type UpdateAccountSettingsInput struct {
	// OnDemandStreamCountLimit sets the account-level limit for ON_DEMAND streams.
	OnDemandStreamCountLimit int
}

// UpdateMaxRecordSizeInput is the input for UpdateMaxRecordSize.
type UpdateMaxRecordSizeInput struct {
	StreamName         string
	StreamARN          string
	MaxRecordSizeBytes int
}

// UpdateStreamWarmThroughputInput is the input for UpdateStreamWarmThroughput.
type UpdateStreamWarmThroughputInput struct {
	StreamName         string
	StreamARN          string
	WriteCapacityUnits int64
	ReadCapacityUnits  int64
}

// TagResourceInput is the input for TagResource (ARN-based tagging).
type TagResourceInput struct {
	Tags        map[string]string
	ResourceARN string
}

// UntagResourceInput is the input for UntagResource (ARN-based tag removal).
type UntagResourceInput struct {
	ResourceARN string
	TagKeys     []string
}
