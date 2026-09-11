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

	// bytesPerKiB converts UpdateMaxRecordSize's wire unit (MaxRecordSizeInKiB)
	// to the bytes this backend stores per-stream (Stream.MaxRecordSizeBytes).
	bytesPerKiB = 1024

	// maxWarmThroughputMiBps is AWS's documented default cap on UpdateStreamWarmThroughput:
	// "you cannot scale to more than 10 GiBps for an on-demand stream" (10*1024 MiBps).
	maxWarmThroughputMiBps = 10 * 1024

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
	// defaultGetRecordsLimit is the default limit for GetRecords (api_op_GetRecords.go:
	// "Specify a value of up to 10,000 ... The default value is 10,000.").
	defaultGetRecordsLimit = 10000
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

	// minimumThroughputBillingCommitmentEnabled/Disabled are the only two
	// values MinimumThroughputBillingCommitmentInput.Status accepts
	// (types.MinimumThroughputBillingCommitmentInputStatus's enum). A third,
	// Output-only status, ENABLED_UNTIL_EARLIEST_ALLOWED_END
	// (types.MinimumThroughputBillingCommitmentOutputStatus), is reported
	// mid-way through ending a commitment window; this backend never produces
	// it, since that requires the commitment-window/billing model gopherstack
	// doesn't have (see PARITY.md).
	minimumThroughputBillingCommitmentEnabled  = "ENABLED"
	minimumThroughputBillingCommitmentDisabled = "DISABLED"
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
	// Defaults to defaultMaxRecordSizeBytes (1 MiB); updatable via UpdateMaxRecordSize
	// (wire unit is MaxRecordSizeInKiB; converted to bytes on write via bytesPerKiB).
	MaxRecordSizeBytes int `json:"maxRecordSizeBytes,omitempty"`
	// WarmThroughputMiBps is the stream's current UpdateStreamWarmThroughput
	// setting. Applied synchronously (this backend has no UPDATING transient
	// state), so Current and Target always match on read -- see
	// UpdateStreamWarmThroughputOutput and PARITY.md.
	WarmThroughputMiBps int `json:"warmThroughputMiBps,omitempty"`
}

// Shard represents a single Kinesis shard within a stream.
type Shard struct {
	// StartedAt is when this shard became open (stream creation for the initial
	// shard set, or reshard time for shards born from SplitShard/MergeShards/
	// UpdateShardCount/UpdateStreamMode). Used by ListShards' AT_TIMESTAMP/
	// FROM_TIMESTAMP/AT_TRIM_HORIZON ShardFilter to bound shard lineage by time.
	StartedAt time.Time `json:"startedAt"`
	// ClosedAt is when this shard was closed (zero if still open). Populated
	// alongside Closed by closeShard. omitempty has no effect on a struct
	// field like time.Time, so it is intentionally omitted here.
	ClosedAt              time.Time    `json:"closedAt"`
	ID                    string       `json:"id"`
	HashKeyRangeStart     string       `json:"hashKeyRangeStart"`
	HashKeyRangeEnd       string       `json:"hashKeyRangeEnd"`
	ParentShardID         string       `json:"parentShardId,omitempty"`
	AdjacentParentShardID string       `json:"adjacentParentShardId,omitempty"`
	Records               shardRecords `json:"records"`
	NextSeq               uint64       `json:"nextSeq"`
	Closed                bool         `json:"closed,omitempty"`
}

// closeShard marks a shard CLOSED and records the closure time, keeping
// Closed/ClosedAt in sync everywhere a shard is retired (SplitShard,
// MergeShards, UpdateShardCount, UpdateStreamMode's mode-transition reshard).
func closeShard(s *Shard) {
	s.Closed = true
	s.ClosedAt = time.Now()
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
	// MaxRecordSizeInKiB mirrors CreateStreamInput's own field of the same
	// name (kinesis@v1.46.4 api_op_CreateStream.go:101-103), not just
	// UpdateMaxRecordSize's. Zero means "not specified" -- CreateStream keeps
	// defaultMaxRecordSizeBytes, same as an omitted request member.
	MaxRecordSizeInKiB int
	// WarmThroughputMiBps mirrors CreateStreamInput's own field of the same
	// name (kinesis@v1.46.4 api_op_CreateStream.go:119-121). Zero means "not
	// specified".
	WarmThroughputMiBps int
}

// DeleteStreamInput is the input for DeleteStream.
type DeleteStreamInput struct {
	StreamName string
	// EnforceConsumerDeletion mirrors the real DeleteStreamInput field: unset
	// or false with registered consumers fails the call with
	// ResourceInUseException instead of deleting the stream.
	EnforceConsumerDeletion bool
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
	// MaxRecordSizeBytes and WarmThroughputMiBps mirror the same-named Stream
	// fields (see UpdateMaxRecordSize/UpdateStreamWarmThroughput). Real
	// StreamDescriptionSummary carries both (MaxRecordSizeInKiB/WarmThroughput);
	// StreamDescription (DescribeStream) does not, so only
	// handleDescribeStreamSummary reads these.
	MaxRecordSizeBytes  int
	WarmThroughputMiBps int
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
// Timestamp is a pointer so a genuinely omitted value (nil) can be
// distinguished from an explicit epoch-zero timestamp; required (non-nil)
// when ShardIteratorType is AT_TIMESTAMP.
type GetShardIteratorInput struct {
	Timestamp              *time.Time
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
	EncryptionType              string
	Data                        []byte
}

// GetRecordsOutput is the output for GetRecords.
type GetRecordsOutput struct {
	NextShardIterator  string
	Records            []GetRecordResult
	ChildShards        []ChildShard
	MillisBehindLatest int64
}

// ChildShard describes a shard that resulted from splitting or merging the
// shard a GetRecords call just finished reading (aws-sdk-go-v2
// types.ChildShard). Real AWS only returns this "when the end of the
// current shard is reached" -- i.e. exactly when NextShardIterator is empty
// because the shard is Closed and fully consumed.
type ChildShard struct {
	ShardID           string
	HashKeyRangeStart string
	HashKeyRangeEnd   string
	ParentShards      []string
}

// ListShardsInput is the input for ListShards.
type ListShardsInput struct {
	ShardFilterTimestamp  *time.Time
	StreamName            string
	NextToken             string
	ExclusiveStartShardID string
	ShardFilter           string
	ShardFilterType       string
	ShardFilterShardID    string
	MaxResults            int
}

// ListShardsOutput is the output for ListShards.
type ListShardsOutput struct {
	NextToken string
	Shards    []ShardDescription
}

// Consumer represents a registered Kinesis enhanced fan-out consumer.
type Consumer struct {
	ConsumerCreationTimestamp time.Time         `json:"consumerCreationTimestamp"`
	Tags                      map[string]string `json:"tags,omitempty"`
	ConsumerName              string            `json:"consumerName"`
	ConsumerARN               string            `json:"consumerARN"`
	ConsumerStatus            string            `json:"consumerStatus"`
	StreamARN                 string            `json:"streamARN"`
}

// RegisterStreamConsumerInput is the input for RegisterStreamConsumer.
type RegisterStreamConsumerInput struct {
	Tags         map[string]string
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
	StreamARN                string
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
	StreamARN                string
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

// MinimumThroughputBillingCommitmentInput is the input shape for the
// commitment status requested via UpdateAccountSettings
// (types.MinimumThroughputBillingCommitmentInput; Status is its only,
// required, member -- kinesis@v1.46.4 types/types.go:168-176).
type MinimumThroughputBillingCommitmentInput struct {
	// Status is required: minimumThroughputBillingCommitmentEnabled or
	// minimumThroughputBillingCommitmentDisabled.
	Status string
}

// MinimumThroughputBillingCommitmentOutput is the account's current minimum
// throughput billing commitment (types.MinimumThroughputBillingCommitmentOutput,
// kinesis@v1.46.4 types/types.go:178-197). This backend has no billing engine:
// Status/StartedAt/EndedAt only track the state transitions UpdateAccountSettings
// requests; EarliestAllowedEndAt is never populated since computing it needs a
// commitment-window model this backend doesn't have (see PARITY.md gaps), and
// Status never reports minimumThroughputBillingCommitmentEnabledUntilEnd for
// the same reason.
type MinimumThroughputBillingCommitmentOutput struct {
	EarliestAllowedEndAt time.Time `json:"earliestAllowedEndAt"`
	EndedAt              time.Time `json:"endedAt"`
	StartedAt            time.Time `json:"startedAt"`
	Status               string    `json:"status"`
}

// DescribeAccountSettingsOutput is the output for DescribeAccountSettings.
type DescribeAccountSettingsOutput struct {
	MinimumThroughputBillingCommitment MinimumThroughputBillingCommitmentOutput
}

// UpdateStreamModeInput is the input for UpdateStreamMode.
type UpdateStreamModeInput struct {
	StreamARN         string
	StreamModeDetails StreamModeDetails
	// WarmThroughputMiBps mirrors UpdateStreamModeInput's own field
	// (kinesis@v1.46.4 api_op_UpdateStreamMode.go, "only valid when the
	// stream mode is being updated to on-demand"). Zero means "not
	// specified".
	WarmThroughputMiBps int
}

// StreamModeDetails describes the mode of a Kinesis stream.
type StreamModeDetails struct {
	StreamMode string
}

// UpdateAccountSettingsInput is the input for UpdateAccountSettings.
type UpdateAccountSettingsInput struct {
	// MinimumThroughputBillingCommitment is required.
	MinimumThroughputBillingCommitment *MinimumThroughputBillingCommitmentInput
}

// UpdateAccountSettingsOutput is the output for UpdateAccountSettings.
type UpdateAccountSettingsOutput struct {
	MinimumThroughputBillingCommitment MinimumThroughputBillingCommitmentOutput
}

// UpdateMaxRecordSizeInput is the input for UpdateMaxRecordSize. Unlike most
// stream-identifying inputs in this file, the real shape has no StreamName
// member -- only StreamARN (and StreamId, reserved for future use) --
// kinesis@v1.46.4 api_op_UpdateMaxRecordSize.go:30-47.
type UpdateMaxRecordSizeInput struct {
	StreamARN string
	// MaxRecordSizeInKiB is required; wire unit is KiB, not bytes.
	MaxRecordSizeInKiB int
}

// UpdateStreamWarmThroughputInput is the input for UpdateStreamWarmThroughput.
type UpdateStreamWarmThroughputInput struct {
	StreamName string
	StreamARN  string
	// WarmThroughputMiBps is required (api_op_UpdateStreamWarmThroughput.go:63-70).
	WarmThroughputMiBps int
}

// WarmThroughputObject mirrors types.WarmThroughputObject
// (kinesis@v1.46.4 types/types.go:729-740).
type WarmThroughputObject struct {
	CurrentMiBps int
	TargetMiBps  int
}

// UpdateStreamWarmThroughputOutput is the output for UpdateStreamWarmThroughput.
type UpdateStreamWarmThroughputOutput struct {
	StreamARN      string
	StreamName     string
	WarmThroughput WarmThroughputObject
}

// --- Channel types (CreateChannel/DeleteChannel/DescribeChannel/ListChannels/UpdateChannel) ---

const (
	// channelStatusActive is the only ChannelStatus (types.ChannelStatus,
	// kinesis@v1.53.0 types/enums.go:41-50) this backend ever produces:
	// CreateChannel/UpdateChannel are documented as asynchronous
	// (CREATING->ACTIVE, UPDATING->ACTIVE) but this backend applies every
	// change synchronously, matching the precedent already set by
	// UpdateStreamWarmThroughput/UpdateStreamMode -- see PARITY.md.
	channelStatusActive = "ACTIVE"

	// channelDestinationTypeS3/S3Tables mirror types.ChannelDestinationType
	// (kinesis@v1.53.0 types/enums.go:5-22), used on ChannelSummary.
	channelDestinationTypeS3       = "S3"
	channelDestinationTypeS3Tables = "S3_TABLES"

	// Valid types.RecordFormatType values (kinesis@v1.53.0 types/enums.go:196-217).
	recordFormatTypeGSRJSON   = "GSR_JSON"
	recordFormatTypeJSON      = "JSON"
	recordFormatTypeString    = "STRING"
	recordFormatTypeByteArray = "BYTE_ARRAY"

	// channelEncryptionTypeKMS is the only valid ChannelEncryptionType value
	// ("The only valid value is KMS.", kinesis@v1.53.0 types/types.go:87).
	channelEncryptionTypeKMS = "KMS"

	// minChannelDataFreshnessSeconds/maxChannelDataFreshnessSeconds bound
	// S3DestinationConfiguration/S3TablesDestinationConfiguration's
	// DataFreshnessInSeconds ("Valid range is 300 to 900 seconds",
	// kinesis@v1.53.0 types/types.go:634-636/776-778).
	minChannelDataFreshnessSeconds = 300
	maxChannelDataFreshnessSeconds = 900
	// defaultChannelDataFreshnessSeconds is applied when a channel is
	// created with no DataFreshnessInSeconds ("The default value is 300
	// seconds.").
	defaultChannelDataFreshnessSeconds = 300

	// maxChannelsListResults is ListChannels' documented default/cap:
	// "The default value is 100. If you specify a value greater than 100,
	// at most 100 results are returned." (api_op_ListChannels.go).
	maxChannelsListResults = 100

	// maxChannelStreams/maxS3TablesConfigs enforce "Currently, one stream
	// [table] is supported per channel" (CreateChannelInput.StreamConfigurationList
	// and types.S3TablesDestinationConfiguration.S3TablesConfigurationList
	// doc comments).
	maxChannelStreams  = 1
	maxS3TablesConfigs = 1
)

// ChannelRecordConfig mirrors types.RecordConfiguration (kinesis@v1.53.0
// types/types.go:600-624).
type ChannelRecordConfig struct {
	RecordFormatType string `json:"recordFormatType"`
	GSRSchemaARN     string `json:"gsrSchemaARN,omitempty"`
}

// ChannelStreamConfig mirrors the source-stream binding shared by
// types.ChannelStreamConfiguration (CreateChannelInput's StreamConfigurationList),
// types.ChannelStreamDescription (ChannelDescription's StreamConfigurationList),
// and types.ChannelStreamIdentifier (ChannelSummary's Streams) --
// kinesis@v1.53.0 types/types.go:122-172.
type ChannelStreamConfig struct {
	StreamCreationTimestamp time.Time           `json:"streamCreationTimestamp"`
	StreamARN               string              `json:"streamARN"`
	RecordConfiguration     ChannelRecordConfig `json:"recordConfiguration"`
}

// ChannelCloudWatchLogsConfig mirrors types.CloudWatchLogs /
// types.CloudWatchLogsUpdateInput (kinesis@v1.53.0 types/types.go:256-289).
type ChannelCloudWatchLogsConfig struct {
	LogGroupName  string `json:"logGroupName,omitempty"`
	LogStreamName string `json:"logStreamName,omitempty"`
	Enabled       bool   `json:"enabled"`
}

// ChannelEncryptionConfig mirrors types.ChannelEncryptionConfiguration
// (kinesis@v1.53.0 types/types.go:83-97).
type ChannelEncryptionConfig struct {
	EncryptionType string `json:"encryptionType"`
	KeyID          string `json:"keyID"`
}

// ChannelS3StorageConfig mirrors types.S3StorageConfiguration
// (kinesis@v1.53.0 types/types.go:678-714).
type ChannelS3StorageConfig struct {
	BucketARN           string `json:"bucketARN"`
	CompressionType     string `json:"compressionType"`
	ExpectedBucketOwner string `json:"expectedBucketOwner,omitempty"`
	OutputKeyTemplate   string `json:"outputKeyTemplate,omitempty"`
	StorageClass        string `json:"storageClass,omitempty"`
}

// ChannelDeadLetterQueueS3Config mirrors types.DeadLetterQueueS3Configuration
// (kinesis@v1.53.0 types/types.go:364-381).
type ChannelDeadLetterQueueS3Config struct {
	BucketARN           string `json:"bucketARN"`
	ExpectedBucketOwner string `json:"expectedBucketOwner,omitempty"`
	ErrorOutputPrefix   string `json:"errorOutputPrefix,omitempty"`
}

// ChannelS3Destination mirrors the merged shape of types.S3DestinationConfiguration
// (CreateChannelInput) and types.S3DestinationDescription (ChannelDescription) --
// both carry the same members, only required-ness differs between the two
// (kinesis@v1.53.0 types/types.go:626-663).
type ChannelS3Destination struct {
	DeadLetterQueueS3Configuration *ChannelDeadLetterQueueS3Config `json:"deadLetterQueueS3Configuration,omitempty"`
	StorageConfiguration           ChannelS3StorageConfig          `json:"storageConfiguration"`
	DataFreshnessInSeconds         int                             `json:"dataFreshnessInSeconds"`
}

// ChannelPartitionField mirrors types.PartitionField (kinesis@v1.53.0
// types/types.go:479-493).
type ChannelPartitionField struct {
	SourceName string `json:"sourceName"`
	Transform  string `json:"transform"`
}

// ChannelPartitionSpec mirrors types.PartitionSpec (kinesis@v1.53.0
// types/types.go:495-504).
type ChannelPartitionSpec struct {
	PartitionFields []ChannelPartitionField `json:"partitionFields"`
}

// ChannelS3TablesConfig mirrors types.S3TablesConfiguration (kinesis@v1.53.0
// types/types.go:718-746).
type ChannelS3TablesConfig struct {
	PartitionSpec   *ChannelPartitionSpec `json:"partitionSpec,omitempty"`
	TableBucketARN  string                `json:"tableBucketARN"`
	Namespace       string                `json:"namespace"`
	TableName       string                `json:"tableName"`
	CompressionType string                `json:"compressionType"`
}

// ChannelS3TablesDestination mirrors types.S3TablesDestinationConfiguration /
// types.S3TablesDestinationDescription (kinesis@v1.53.0 types/types.go:762-802).
type ChannelS3TablesDestination struct {
	DeadLetterQueueS3Configuration *ChannelDeadLetterQueueS3Config `json:"deadLetterQueueS3Configuration,omitempty"`
	S3TablesConfigurationList      []ChannelS3TablesConfig         `json:"s3TablesConfigurationList"`
	DataFreshnessInSeconds         int                             `json:"dataFreshnessInSeconds"`
}

// Channel represents an in-memory Kinesis Data Streams channel -- a
// CreateChannel-provisioned delivery pipe from a stream to either a general
// purpose S3 bucket or a streaming table (Apache Iceberg / Amazon S3 Tables)
// destination (types.ChannelDescription, kinesis@v1.53.0 types/types.go:9-79).
// Records put to the source stream are NOT delivered to either destination by
// this backend -- see PARITY.md.
type Channel struct {
	ChannelCreationTimestamp         time.Time                   `json:"channelCreationTimestamp"`
	EncryptionConfiguration          *ChannelEncryptionConfig    `json:"encryptionConfiguration,omitempty"`
	S3DestinationConfiguration       *ChannelS3Destination       `json:"s3DestinationConfiguration,omitempty"`
	S3TablesDestinationConfiguration *ChannelS3TablesDestination `json:"s3TablesDestinationConfiguration,omitempty"`
	Tags                             map[string]string           `json:"tags,omitempty"`
	ChannelID                        string                      `json:"channelID"`
	ChannelARN                       string                      `json:"channelARN"`
	ChannelName                      string                      `json:"channelName"`
	ChannelStatus                    string                      `json:"channelStatus"`
	ServiceExecutionRoleARN          string                      `json:"serviceExecutionRoleARN"`
	Region                           string                      `json:"region,omitempty"`
	StreamConfigurationList          []ChannelStreamConfig       `json:"streamConfigurationList"`
	LoggingConfiguration             ChannelCloudWatchLogsConfig `json:"loggingConfiguration"`
}

// CreateChannelInput is the input for CreateChannel.
type CreateChannelInput struct {
	EncryptionConfiguration          *ChannelEncryptionConfig
	LoggingConfiguration             *ChannelCloudWatchLogsConfig
	S3DestinationConfiguration       *ChannelS3Destination
	S3TablesDestinationConfiguration *ChannelS3TablesDestination
	Tags                             map[string]string
	ChannelName                      string
	ServiceExecutionRoleARN          string
	StreamConfigurationList          []ChannelStreamConfig
}

// CreateChannelOutput is the output for CreateChannel.
type CreateChannelOutput struct {
	ChannelDescription Channel
}

// DeleteChannelInput is the input for DeleteChannel.
type DeleteChannelInput struct {
	ChannelARN string
}

// DescribeChannelInput is the input for DescribeChannel.
type DescribeChannelInput struct {
	ChannelARN string
}

// DescribeChannelOutput is the output for DescribeChannel.
type DescribeChannelOutput struct {
	ChannelDescription Channel
}

// ChannelStreamFilter mirrors types.StreamFilter (kinesis@v1.53.0
// types/types.go:1141-1153), used by ListChannels to filter by source stream.
type ChannelStreamFilter struct {
	StreamARN string
}

// ListChannelsInput is the input for ListChannels.
type ListChannelsInput struct {
	NextToken    string
	StreamFilter []ChannelStreamFilter
	MaxResults   int
}

// ListChannelsOutput is the output for ListChannels.
type ListChannelsOutput struct {
	NextToken        string
	ChannelSummaries []Channel
}

// UpdateChannelInput is the input for UpdateChannel. Per the real op's doc
// comment, only LoggingConfiguration and the active destination's
// DataFreshnessInSeconds can be changed: "You cannot change the
// destination, source stream, record format, schema, encryption
// configuration, or service execution role of an existing channel".
type UpdateChannelInput struct {
	LoggingConfiguration             *ChannelCloudWatchLogsConfig
	S3DestinationConfiguration       *ChannelS3Destination
	S3TablesDestinationConfiguration *ChannelS3TablesDestination
	ChannelARN                       string
}

// UpdateChannelOutput is the output for UpdateChannel.
type UpdateChannelOutput struct {
	ChannelDescription Channel
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
