package kinesis

import (
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

const (
	// streamStatusActive is the status when a stream is ready for use.
	streamStatusActive = "ACTIVE"

	// defaultShardCount is the default number of shards for a new stream.
	defaultShardCount = 1

	// defaultRetentionHours is the default retention period for a stream in hours.
	defaultRetentionHours = 24

	// maxRecordsPerShard is the maximum number of records stored per shard.
	maxRecordsPerShard = 10000

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

	// millisToSeconds divides Unix milliseconds to get a float64 second timestamp.
	millisToSeconds = 1000.0

	// maxHashKeyBits is the bit-width of the Kinesis hash key space.
	maxHashKeyBits = 128

	// consumerStatusActive is the status when a consumer is ready for use.
	consumerStatusActive = "ACTIVE"
)

// Stream represents an in-memory Kinesis stream.
type Stream struct {
	CreatedAt          time.Time            `json:"createdAt"`
	Tags               *tags.Tags           `json:"tags,omitempty"`
	Consumers          map[string]*Consumer `json:"consumers,omitempty"`
	Name               string               `json:"name"`
	ARN                string               `json:"arn"`
	Status             string               `json:"status"`
	Shards             []*Shard             `json:"shards"`
	EnhancedMonitoring []string             `json:"enhancedMonitoring,omitempty"`
	RetentionPeriod    int                  `json:"retentionPeriod"`
}

// Shard represents a single Kinesis shard within a stream.
type Shard struct {
	ID                string    `json:"id"`
	HashKeyRangeStart string    `json:"hashKeyRangeStart"`
	HashKeyRangeEnd   string    `json:"hashKeyRangeEnd"`
	Records           []*Record `json:"records"`
	nextSeq           uint64
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
type ShardIterator struct {
	StreamName     string `json:"StreamName"`
	ShardID        string `json:"ShardID"`
	SequenceNumber string `json:"SequenceNumber"`
	Position       int    `json:"Position"`
}

// --- Input/Output types ---

// CreateStreamInput is the input for CreateStream.
type CreateStreamInput struct {
	StreamName string
	Region     string
	AccountID  string
	ShardCount int
}

// DeleteStreamInput is the input for DeleteStream.
type DeleteStreamInput struct {
	StreamName string
}

// DescribeStreamInput is the input for DescribeStream.
type DescribeStreamInput struct {
	StreamName string
}

// DescribeStreamOutput is the output for DescribeStream.
type DescribeStreamOutput struct {
	StreamName           string
	StreamARN            string
	StreamStatus         string
	Shards               []ShardDescription
	RetentionPeriodHours int
}

// ShardDescription describes a shard in a DescribeStream response.
type ShardDescription struct {
	ShardID                  string
	HashKeyRangeStart        string
	HashKeyRangeEnd          string
	SequenceNumberRangeStart string
	SequenceNumberRangeEnd   string
}

// ListStreamsInput is the input for ListStreams.
type ListStreamsInput struct {
	NextToken string
	Limit     int
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
	StreamName string
	NextToken  string
	MaxResults int
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
