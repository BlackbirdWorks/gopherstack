package kinesis

import "time"

const (
	kinesisRegion    = "us-east-1"
	kinesisAccountID = "000000000000"

	// streamStatusCreating is the status when a stream is being created.
	streamStatusCreating = "CREATING"
	// streamStatusActive is the status when a stream is ready for use.
	streamStatusActive = "ACTIVE"
	// streamStatusDeleting is the status when a stream is being deleted.
	streamStatusDeleting = "DELETING"

	// defaultShardCount is the default number of shards for a new stream.
	defaultShardCount = 1

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

	// maxGetRecordsLimit is the maximum number of records per GetRecords call.
	maxGetRecordsLimit = 10000
	// defaultGetRecordsLimit is the default limit for GetRecords.
	defaultGetRecordsLimit = 1000
)

// Stream represents an in-memory Kinesis stream.
type Stream struct {
	Tags        map[string]string
	Shards      []*Shard
	Name        string
	ARN         string
	Status      string
	CreatedAt   time.Time
	RetentionPeriod int // hours; default 24
}

// Shard represents a single Kinesis shard within a stream.
type Shard struct {
	Records          []*Record
	ID               string
	HashKeyRangeStart string
	HashKeyRangeEnd   string
	nextSeq          uint64
}

// Record represents a single Kinesis data record.
type Record struct {
	Data           []byte
	PartitionKey   string
	SequenceNumber string
	ApproximateArrivalTimestamp time.Time
}

// StreamInfo holds summary information about a stream, safe to return without lock.
type StreamInfo struct {
	Name   string
	ARN    string
	Status string
}

// ShardIterator holds the position within a shard for GetRecords.
type ShardIterator struct {
	StreamName     string
	ShardID        string
	Position       int    // index into shard.Records
	SequenceNumber string // used for AT/AFTER_SEQUENCE_NUMBER types
}

// --- Input/Output types ---

// CreateStreamInput is the input for CreateStream.
type CreateStreamInput struct {
	StreamName string
	ShardCount int
	Region     string
	AccountID  string
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
	Shards []ShardDescription
	StreamName string
	StreamARN  string
	StreamStatus string
	RetentionPeriodHours int
}

// ShardDescription describes a shard in a DescribeStream response.
type ShardDescription struct {
	ShardID               string
	HashKeyRangeStart     string
	HashKeyRangeEnd       string
	SequenceNumberRangeStart string
	SequenceNumberRangeEnd   string
}

// ListStreamsInput is the input for ListStreams.
type ListStreamsInput struct {
	Limit     int
	NextToken string
}

// ListStreamsOutput is the output for ListStreams.
type ListStreamsOutput struct {
	StreamNames []string
	NextToken   string
	HasMoreStreams bool
}

// PutRecordInput is the input for PutRecord.
type PutRecordInput struct {
	StreamName     string
	PartitionKey   string
	Data           []byte
	ExplicitHashKey string
}

// PutRecordOutput is the output for PutRecord.
type PutRecordOutput struct {
	ShardID        string
	SequenceNumber string
}

// PutRecordsEntry is a single entry in a PutRecords request.
type PutRecordsEntry struct {
	PartitionKey    string
	Data            []byte
	ExplicitHashKey string
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
	Records          []PutRecordsResultEntry
	FailedRecordCount int
}

// GetShardIteratorInput is the input for GetShardIterator.
type GetShardIteratorInput struct {
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
	Data                        []byte
	PartitionKey                string
	SequenceNumber              string
	ApproximateArrivalTimestamp time.Time
}

// GetRecordsOutput is the output for GetRecords.
type GetRecordsOutput struct {
	Records           []GetRecordResult
	NextShardIterator string
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
	Shards    []ShardDescription
	NextToken string
}
