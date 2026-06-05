package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_rddata "github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

var (
	// ErrNotFound is returned when a delivery stream is not found.
	ErrNotFound = awserr.New("ResourceNotFoundException", awserr.ErrNotFound)
	// ErrAlreadyExists is returned when a delivery stream already exists.
	ErrAlreadyExists = awserr.New("ResourceInUseException", awserr.ErrAlreadyExists)
	// ErrTransformPayload is a sentinel error indicating the Lambda transform
	// payload could not be built. Use [errors.Is] to check for this condition.
	ErrTransformPayload = errors.New("failed to build Lambda transform payload")
	// ErrRecordTooLarge is returned when a record exceeds the 1,000 KB per-record limit.
	ErrRecordTooLarge = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	// ErrBatchTooLarge is returned when a PutRecordBatch request exceeds the 500-record limit.
	ErrBatchTooLarge = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
	// ErrValidation is returned for invalid input parameters.
	ErrValidation = awserr.New("InvalidArgumentException", awserr.ErrInvalidParameter)
)

// maxRecordBytes is the maximum size of a single Firehose record (1,000 KB).
const maxRecordBytes = 1_000 * 1024

// maxBatchRecords is the maximum number of records allowed in a single PutRecordBatch call.
const maxBatchRecords = 500

// maxBatchBytes is the AWS Firehose limit on the combined payload of a
// PutRecordBatch request (4 MiB).
const maxBatchBytes = 4 * 1024 * 1024

const (
	// deliveryStreamTypeDirectPut is the default stream type for direct-put streams.
	deliveryStreamTypeDirectPut     = "DirectPut"
	deliveryStreamTypeKinesisSource = "KinesisStreamAsSource"
)

// S3Storer is the subset of S3 operations that Firehose needs to deliver objects.
type S3Storer interface {
	PutObject(ctx context.Context, input *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error)
}

// LambdaInvoker is the subset of Lambda operations that Firehose needs for transformation.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
}

// KinesisReader is the subset of Kinesis operations that Firehose needs to poll source streams.
type KinesisReader interface {
	// ListShards returns all open shard IDs for the named stream.
	ListShards(streamName string) ([]string, error)
	// GetShardIterator returns a TRIM_HORIZON iterator token for the given stream/shard.
	GetShardIterator(streamName, shardID string) (string, error)
	// GetRecords reads up to limit records. Returns raw data slices, next iterator token, and error.
	GetRecords(shardIterator string, limit int) (records [][]byte, nextIterator string, err error)
}

// BufferingHints controls when buffered records are delivered to S3.
type BufferingHints struct {
	SizeInMBs         int `json:"SizeInMBs"`
	IntervalInSeconds int `json:"IntervalInSeconds"`
}

// ProcessorParameter is a key-value parameter for a processor.
type ProcessorParameter struct {
	ParameterName  string `json:"ParameterName"`
	ParameterValue string `json:"ParameterValue"`
}

// Processor describes a single transformation step.
type Processor struct {
	Type       string               `json:"Type"`
	Parameters []ProcessorParameter `json:"Parameters,omitempty"`
}

// ProcessingConfiguration describes Lambda-based transformation.
type ProcessingConfiguration struct {
	Processors []Processor `json:"Processors,omitempty"`
	Enabled    bool        `json:"Enabled"`
}

// CloudWatchLoggingOptions configures CloudWatch logging for a destination.
type CloudWatchLoggingOptions struct {
	LogGroupName  string `json:"LogGroupName,omitempty"`
	LogStreamName string `json:"LogStreamName,omitempty"`
	Enabled       bool   `json:"Enabled"`
}

// KMSEncryptionConfig holds a KMS key ARN for S3 encryption.
type KMSEncryptionConfig struct {
	AWSKMSKeyARN string `json:"AWSKMSKeyARN"`
}

// S3EncryptionConfiguration holds the S3 object encryption config.
type S3EncryptionConfiguration struct {
	KMSEncryptionConfig *KMSEncryptionConfig `json:"KMSEncryptionConfig,omitempty"`
	NoEncryptionConfig  string               `json:"NoEncryptionConfig,omitempty"`
}

// DynamicPartitioningConfiguration controls dynamic partitioning.
type DynamicPartitioningConfiguration struct {
	RetryOptions *RetryOptions `json:"RetryOptions,omitempty"`
	Enabled      bool          `json:"Enabled"`
}

// RetryOptions holds a retry duration.
type RetryOptions struct {
	DurationInSeconds int `json:"DurationInSeconds"`
}

// EncryptionConfigInput holds the optional SSE configuration for a delivery stream.
type EncryptionConfigInput struct {
	KeyARN  string `json:"KeyARN,omitempty"`
	KeyType string `json:"KeyType"`
}

// EncryptionConfig holds the effective SSE configuration for a delivery stream.
type EncryptionConfig struct {
	FailureDescription *FailureDescription `json:"FailureDescription,omitempty"`
	KeyARN             string              `json:"KeyARN,omitempty"`
	KeyType            string              `json:"KeyType"`
	Status             string              `json:"Status"`
}

// FailureDescription holds error context for SSE failures.
type FailureDescription struct {
	Details string `json:"Details,omitempty"`
	Type    string `json:"Type,omitempty"`
}

// S3DestinationDescription holds the effective S3 destination config stored on the stream.
type S3DestinationDescription struct {
	BufferingHints                   *BufferingHints                   `json:"BufferingHints,omitempty"`
	ProcessingConfiguration          *ProcessingConfiguration          `json:"ProcessingConfiguration,omitempty"`
	S3BackupDescription              *S3BackupDescription              `json:"S3BackupDescription,omitempty"`
	EncryptionConfiguration          *S3EncryptionConfiguration        `json:"EncryptionConfiguration,omitempty"`
	CloudWatchLoggingOptions         *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions,omitempty"`
	DynamicPartitioningConfiguration *DynamicPartitioningConfiguration `json:"DynamicPartitioningConfiguration,omitempty"`
	BucketARN                        string                            `json:"BucketARN,omitempty"`
	RoleARN                          string                            `json:"RoleARN,omitempty"`
	Prefix                           string                            `json:"Prefix,omitempty"`
	ErrorOutputPrefix                string                            `json:"ErrorOutputPrefix,omitempty"`
	CompressionFormat                string                            `json:"CompressionFormat,omitempty"`
	FileExtension                    string                            `json:"FileExtension,omitempty"`
	CustomTimeZone                   string                            `json:"CustomTimeZone,omitempty"`
	DestinationID                    string                            `json:"DestinationId,omitempty"`
	S3BackupMode                     string                            `json:"S3BackupMode,omitempty"`
}

// S3BackupDescription holds the S3 backup destination configuration.
type S3BackupDescription struct {
	BufferingHints    *BufferingHints `json:"BufferingHints,omitempty"`
	BucketARN         string          `json:"BucketARN,omitempty"`
	RoleARN           string          `json:"RoleARN,omitempty"`
	Prefix            string          `json:"Prefix,omitempty"`
	CompressionFormat string          `json:"CompressionFormat,omitempty"`
}

// HTTPEndpointRequestConfiguration holds the content-encoding and attributes for HTTP requests.
type HTTPEndpointRequestConfiguration struct {
	ContentEncoding  string                        `json:"ContentEncoding,omitempty"`
	CommonAttributes []HTTPEndpointCommonAttribute `json:"CommonAttributes,omitempty"`
}

// HTTPEndpointCommonAttribute is a key-value attribute sent with HTTP requests.
type HTTPEndpointCommonAttribute struct {
	AttributeName  string `json:"AttributeName"`
	AttributeValue string `json:"AttributeValue"`
}

// HTTPEndpointDestinationDescription holds the HTTP endpoint destination config.
type HTTPEndpointDestinationDescription struct {
	ProcessingConfiguration  *ProcessingConfiguration          `json:"ProcessingConfiguration,omitempty"`
	EndpointConfiguration    *HTTPEndpointConfiguration        `json:"EndpointConfiguration,omitempty"`
	RequestConfiguration     *HTTPEndpointRequestConfiguration `json:"RequestConfiguration,omitempty"`
	BufferingHints           *BufferingHints                   `json:"BufferingHints,omitempty"`
	RetryOptions             *RetryOptions                     `json:"RetryOptions,omitempty"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions         `json:"CloudWatchLoggingOptions,omitempty"`
	S3BackupMode             string                            `json:"S3BackupMode,omitempty"`
	S3BackupDescription      *S3BackupDescription              `json:"S3BackupDescription,omitempty"`
	DestinationID            string                            `json:"DestinationId,omitempty"`
}

// HTTPEndpointConfiguration holds the HTTP endpoint URL and name.
type HTTPEndpointConfiguration struct {
	URL       string `json:"Url,omitempty"`
	Name      string `json:"Name,omitempty"`
	AccessKey string `json:"AccessKey,omitempty"`
}

// KinesisStreamSourceDescription describes a Kinesis stream source.
type KinesisStreamSourceDescription struct {
	DeliveryStartTimestamp string `json:"DeliveryStartTimestamp,omitempty"`
	KinesisStreamARN       string `json:"KinesisStreamARN,omitempty"`
	RoleARN                string `json:"RoleARN,omitempty"`
}

// MSKSourceDescription describes an MSK cluster source.
type MSKSourceDescription struct {
	AuthenticationConfiguration *MSKAuthenticationConfiguration `json:"AuthenticationConfiguration,omitempty"`
	MSKClusterARN               string                          `json:"MSKClusterARN,omitempty"`
	TopicName                   string                          `json:"TopicName,omitempty"`
	ReadFromTimestamp           string                          `json:"ReadFromTimestamp,omitempty"`
}

// MSKAuthenticationConfiguration holds MSK connectivity and role config.
type MSKAuthenticationConfiguration struct {
	Connectivity string `json:"Connectivity,omitempty"`
	RoleARN      string `json:"RoleARN,omitempty"`
}

// SourceDescription holds source details for non-DirectPut streams.
type SourceDescription struct {
	KinesisStreamSourceDescription *KinesisStreamSourceDescription `json:"KinesisStreamSourceDescription,omitempty"`
	MSKSourceDescription           *MSKSourceDescription           `json:"MSKSourceDescription,omitempty"`
}

// RedshiftDestinationDescription holds a Redshift destination config.
type RedshiftDestinationDescription struct {
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration,omitempty"`
	RetryOptions            *RetryOptions            `json:"RetryOptions,omitempty"`
	S3BackupDescription     *S3BackupDescription     `json:"S3BackupDescription,omitempty"`
	ClusterJDBCURL          string                   `json:"ClusterJDBCURL,omitempty"`
	DataTableName           string                   `json:"DataTableName,omitempty"`
	CopyOptions             string                   `json:"CopyOptions,omitempty"`
	DataTableColumns        string                   `json:"DataTableColumns,omitempty"`
	Username                string                   `json:"Username,omitempty"`
	RoleARN                 string                   `json:"RoleARN,omitempty"`
	S3BackupMode            string                   `json:"S3BackupMode,omitempty"`
}

// OpenSearchDestinationDescription holds an OpenSearch (Elasticsearch) destination config.
type OpenSearchDestinationDescription struct {
	ProcessingConfiguration  *ProcessingConfiguration  `json:"ProcessingConfiguration,omitempty"`
	BufferingHints           *BufferingHints           `json:"BufferingHints,omitempty"`
	RetryOptions             *RetryOptions             `json:"RetryOptions,omitempty"`
	CloudWatchLoggingOptions *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions,omitempty"`
	S3BackupDescription      *S3BackupDescription      `json:"S3BackupDescription,omitempty"`
	DomainARN                string                    `json:"DomainARN,omitempty"`
	ClusterEndpoint          string                    `json:"ClusterEndpoint,omitempty"`
	IndexName                string                    `json:"IndexName,omitempty"`
	TypeName                 string                    `json:"TypeName,omitempty"`
	IndexRotationPeriod      string                    `json:"IndexRotationPeriod,omitempty"`
	S3BackupMode             string                    `json:"S3BackupMode,omitempty"`
	RoleARN                  string                    `json:"RoleARN,omitempty"`
	DestinationID            string                    `json:"DestinationId,omitempty"`
}

// SplunkDestinationDescription holds a Splunk HEC destination config.
type SplunkDestinationDescription struct {
	ProcessingConfiguration           *ProcessingConfiguration  `json:"ProcessingConfiguration,omitempty"`
	RetryOptions                      *RetryOptions             `json:"RetryOptions,omitempty"`
	CloudWatchLoggingOptions          *CloudWatchLoggingOptions `json:"CloudWatchLoggingOptions,omitempty"`
	S3BackupDescription               *S3BackupDescription      `json:"S3BackupDescription,omitempty"`
	HECEndpoint                       string                    `json:"HECEndpoint,omitempty"`
	HECEndpointType                   string                    `json:"HECEndpointType,omitempty"`
	HECToken                          string                    `json:"HECToken,omitempty"`
	S3BackupMode                      string                    `json:"S3BackupMode,omitempty"`
	DestinationID                     string                    `json:"DestinationId,omitempty"`
	HECAcknowledgmentTimeoutInSeconds int                       `json:"HECAcknowledgmentTimeoutInSeconds,omitempty"`
}

// DeliveryMetrics tracks delivery statistics for a stream.
type DeliveryMetrics struct {
	TotalRecords  int64 `json:"TotalRecords"`
	FailedRecords int64 `json:"FailedRecords"`
	TotalBytes    int64 `json:"TotalBytes"`
}

// DeliveryStream represents a Kinesis Firehose delivery stream.
type DeliveryStream struct {
	lastFlush               time.Time
	CreateTimestamp         time.Time                           `json:"createTimestamp"`
	LastUpdateTimestamp     time.Time                           `json:"lastUpdateTimestamp"`
	Tags                    *tags.Tags                          `json:"tags,omitempty"`
	S3Destination           *S3DestinationDescription           `json:"s3Destination,omitempty"`
	HTTPEndpointDestination *HTTPEndpointDestinationDescription `json:"httpEndpointDestination,omitempty"`
	RedshiftDestination     *RedshiftDestinationDescription     `json:"redshiftDestination,omitempty"`
	OpenSearchDestination   *OpenSearchDestinationDescription   `json:"openSearchDestination,omitempty"`
	SplunkDestination       *SplunkDestinationDescription       `json:"splunkDestination,omitempty"`
	Encryption              *EncryptionConfig                   `json:"encryption,omitempty"`
	Source                  *SourceDescription                  `json:"source,omitempty"`
	DeliveryStreamType      string                              `json:"deliveryStreamType,omitempty"`
	Name                    string                              `json:"name"`
	ARN                     string                              `json:"arn"`
	VersionID               string                              `json:"versionID,omitempty"`
	Status                  string                              `json:"status"`
	AccountID               string                              `json:"accountID"`
	Region                  string                              `json:"region"`
	Records                 [][]byte                            `json:"records,omitempty"`
	BackupRecords           [][]byte                            `json:"backupRecords,omitempty"`
	Metrics                 DeliveryMetrics                     `json:"metrics"`
	bufferSizeBytes         int
}

// InMemoryBackend is the in-memory store for Firehose resources.
type InMemoryBackend struct {
	s3             S3Storer
	lambda         LambdaInvoker
	kinesisBackend KinesisReader
	streams        map[string]*DeliveryStream
	// pollerCancel maps stream name → cancel func for active Kinesis source pollers.
	pollerCancel map[string]context.CancelFunc
	mu           *lockmetrics.RWMutex
	// svcCtx is the service lifecycle context; delivery operations use it so
	// they are cancelled when the server shuts down rather than blocking indefinitely.
	svcCtx    context.Context
	accountID string
	region    string
}

var _ StorageBackend = (*InMemoryBackend)(nil)

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return NewInMemoryBackendWithContext(context.Background(), accountID, region)
}

// NewInMemoryBackendWithContext creates a new InMemoryBackend whose delivery
// operations are bounded by the provided parent context. Use this in production
// to ensure in-flight deliveries are cancelled on server shutdown.
// If svcCtx is nil, [context.Background] is used.
func NewInMemoryBackendWithContext(svcCtx context.Context, accountID, region string) *InMemoryBackend {
	if svcCtx == nil {
		svcCtx = context.Background()
	}

	return &InMemoryBackend{
		streams:      make(map[string]*DeliveryStream),
		pollerCancel: make(map[string]context.CancelFunc),
		accountID:    accountID,
		region:       region,
		mu:           lockmetrics.New("firehose"),
		svcCtx:       svcCtx,
	}
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// SetS3Backend wires the S3 backend for actual record delivery.
func (b *InMemoryBackend) SetS3Backend(s3 S3Storer) {
	b.s3 = s3
}

// SetLambdaBackend wires the Lambda backend for record transformation.
func (b *InMemoryBackend) SetLambdaBackend(lambda LambdaInvoker) {
	b.lambda = lambda
}

// SetKinesisBackend wires the Kinesis backend for polling KinesisStreamAsSource streams.
func (b *InMemoryBackend) SetKinesisBackend(k KinesisReader) {
	b.kinesisBackend = k
}

// CreateDeliveryStreamInput holds the input for creating a delivery stream.
type CreateDeliveryStreamInput struct {
	S3Destination           *S3DestinationDescription
	HTTPEndpointDestination *HTTPEndpointDestinationDescription
	RedshiftDestination     *RedshiftDestinationDescription
	OpenSearchDestination   *OpenSearchDestinationDescription
	SplunkDestination       *SplunkDestinationDescription
	Source                  *SourceDescription
	Name                    string
	DeliveryStreamType      string
}

// CreateDeliveryStream creates a new delivery stream.
func (b *InMemoryBackend) CreateDeliveryStream(input CreateDeliveryStreamInput) (*DeliveryStream, error) {
	if strings.TrimSpace(input.Name) == "" {
		return nil, fmt.Errorf("%w: DeliveryStreamName is required", ErrValidation)
	}

	b.mu.Lock("CreateDeliveryStream")

	if _, ok := b.streams[input.Name]; ok {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: stream %s already exists", ErrAlreadyExists, input.Name)
	}

	if input.S3Destination != nil && input.S3Destination.DestinationID == "" {
		input.S3Destination.DestinationID = "destinationId-000000000001"
	}

	streamType := input.DeliveryStreamType
	if streamType == "" {
		streamType = deliveryStreamTypeDirectPut
	}

	now := time.Now()
	streamARN := arn.Build("firehose", b.region, b.accountID, "deliverystream/"+input.Name)
	s := &DeliveryStream{
		Name:                    input.Name,
		ARN:                     streamARN,
		DeliveryStreamType:      streamType,
		VersionID:               "1",
		Status:                  "ACTIVE",
		Records:                 [][]byte{},
		BackupRecords:           [][]byte{},
		Tags:                    tags.New("firehose." + input.Name + ".tags"),
		AccountID:               b.accountID,
		Region:                  b.region,
		S3Destination:           input.S3Destination,
		HTTPEndpointDestination: input.HTTPEndpointDestination,
		RedshiftDestination:     input.RedshiftDestination,
		OpenSearchDestination:   input.OpenSearchDestination,
		SplunkDestination:       input.SplunkDestination,
		Source:                  input.Source,
		CreateTimestamp:         now,
		LastUpdateTimestamp:     now,
		lastFlush:               now,
	}
	b.streams[input.Name] = s

	// Collect Kinesis poller info while holding the lock.
	var kinesisStreamARN string
	shouldPoll := streamType == deliveryStreamTypeKinesisSource &&
		b.kinesisBackend != nil &&
		input.Source != nil &&
		input.Source.KinesisStreamSourceDescription != nil
	if shouldPoll {
		kinesisStreamARN = input.Source.KinesisStreamSourceDescription.KinesisStreamARN
	}

	result := streamCopy(s)

	b.mu.Unlock()

	if shouldPoll {
		b.launchKinesisPoller(input.Name, kinesisStreamARN)
	}

	return result, nil
}

// DeleteDeliveryStream deletes a delivery stream.
func (b *InMemoryBackend) DeleteDeliveryStream(name string) error {
	b.mu.Lock("DeleteDeliveryStream")

	s, ok := b.streams[name]
	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	if s.Tags != nil {
		s.Tags.Close()
	}

	delete(b.streams, name)

	// Stop Kinesis poller if one is running for this stream.
	cancel := b.pollerCancel[name]
	delete(b.pollerCancel, name)

	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	return nil
}

// DescribeDeliveryStream returns a delivery stream by name.
func (b *InMemoryBackend) DescribeDeliveryStream(name string) (*DeliveryStream, error) {
	b.mu.RLock("DescribeDeliveryStream")
	defer b.mu.RUnlock()

	s, ok := b.streams[name]
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	return streamCopy(s), nil
}

// ListDeliveryStreams returns all delivery stream names in alphabetical order.
func (b *InMemoryBackend) ListDeliveryStreams() []string {
	b.mu.RLock("ListDeliveryStreams")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.streams))
	for name := range b.streams {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// PutRecord appends a record to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecord(streamName string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: record Data must not be empty", ErrValidation)
	}

	if len(data) > maxRecordBytes {
		return fmt.Errorf("%w: record size %d exceeds maximum of %d bytes",
			ErrRecordTooLarge, len(data), maxRecordBytes)
	}

	b.mu.Lock("PutRecord")

	s, ok := b.streams[streamName]
	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	if s.DeliveryStreamType != deliveryStreamTypeDirectPut && s.DeliveryStreamType != "" {
		b.mu.Unlock()

		return fmt.Errorf("%w: PutRecord not allowed on %s stream; only DirectPut streams accept direct puts",
			ErrValidation, s.DeliveryStreamType)
	}

	s.Records = append(s.Records, data)
	s.bufferSizeBytes += len(data)
	s.Metrics.TotalRecords++
	s.Metrics.TotalBytes += int64(len(data))
	// If backup mode is enabled, also store a copy in backup records.
	if b.isBackupEnabledLocked(s) {
		s.BackupRecords = append(s.BackupRecords, data)
	}
	snap := b.extractForFlushLocked(s)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(b.svcCtx, snap, streamName)
	}

	return nil
}

// PutRecordBatch appends multiple records to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecordBatch(streamName string, records [][]byte) (int, error) {
	if len(records) > maxBatchRecords {
		return 0, fmt.Errorf("%w: batch size %d exceeds maximum of %d records",
			ErrBatchTooLarge, len(records), maxBatchRecords)
	}

	totalBytes := 0
	for i, rec := range records {
		if len(rec) == 0 {
			return 0, fmt.Errorf("%w: record %d Data must not be empty", ErrValidation, i)
		}

		if len(rec) > maxRecordBytes {
			return 0, fmt.Errorf("%w: record %d size %d exceeds maximum of %d bytes",
				ErrRecordTooLarge, i, len(rec), maxRecordBytes)
		}
		totalBytes += len(rec)
	}

	if totalBytes > maxBatchBytes {
		return 0, fmt.Errorf("%w: batch payload %d exceeds maximum of %d bytes",
			ErrBatchTooLarge, totalBytes, maxBatchBytes)
	}

	b.mu.Lock("PutRecordBatch")

	s, ok := b.streams[streamName]
	if !ok {
		b.mu.Unlock()

		return 0, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	if s.DeliveryStreamType != deliveryStreamTypeDirectPut && s.DeliveryStreamType != "" {
		b.mu.Unlock()

		return 0, fmt.Errorf("%w: PutRecordBatch not allowed on %s stream; only DirectPut streams accept direct puts",
			ErrValidation, s.DeliveryStreamType)
	}

	backupEnabled := b.isBackupEnabledLocked(s)
	for _, rec := range records {
		s.Records = append(s.Records, rec)
		s.bufferSizeBytes += len(rec)
		s.Metrics.TotalRecords++
		s.Metrics.TotalBytes += int64(len(rec))
		if backupEnabled {
			s.BackupRecords = append(s.BackupRecords, rec)
		}
	}

	snap := b.extractForFlushLocked(s)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(b.svcCtx, snap, streamName)
	}

	return 0, nil
}

// UpdateDestination updates the S3 destination configuration of an existing stream.
func (b *InMemoryBackend) UpdateDestination(streamName, currentVersionID string, dest *S3DestinationDescription) error {
	b.mu.Lock("UpdateDestination")
	defer b.mu.Unlock()

	s, ok := b.streams[streamName]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	if currentVersionID != "" && s.VersionID != currentVersionID {
		return fmt.Errorf("%w: version mismatch: expected %s got %s", ErrValidation, currentVersionID, s.VersionID)
	}

	s.S3Destination = dest
	s.LastUpdateTimestamp = time.Now()

	v, err := strconv.Atoi(s.VersionID)
	if err != nil {
		logger.Load(context.Background()).WarnContext(context.Background(),
			"firehose: unexpected non-integer VersionID; resetting to 1",
			"stream", streamName, "versionID", s.VersionID, "error", err)

		v = 0
	}

	s.VersionID = strconv.Itoa(v + 1)

	return nil
}

// FlushAll forces delivery of all buffered records across all streams.
// Used by tests and for graceful shutdown.
func (b *InMemoryBackend) FlushAll(ctx context.Context) {
	b.mu.RLock("FlushAll")
	names := make([]string, 0, len(b.streams))
	for name := range b.streams {
		names = append(names, name)
	}
	b.mu.RUnlock()

	for _, name := range names {
		b.flushStream(ctx, name)
	}
}

// RunFlusher starts the background interval flusher goroutine.
func (b *InMemoryBackend) RunFlusher(ctx context.Context) {
	go b.intervalFlusher(ctx)
}

// intervalFlusher periodically flushes streams whose interval threshold has been reached.
func (b *InMemoryBackend) intervalFlusher(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			b.mu.RLock("intervalFlusher")
			names := make([]string, 0, len(b.streams))
			for name, s := range b.streams {
				if b.shouldFlushByIntervalLocked(s) {
					names = append(names, name)
				}
			}
			b.mu.RUnlock()

			for _, name := range names {
				b.flushStream(ctx, name)
			}
		}
	}
}

// isBackupEnabledLocked returns true when S3 backup mode is enabled for the stream.
// Must be called with the write lock held.
func (b *InMemoryBackend) isBackupEnabledLocked(s *DeliveryStream) bool {
	if s.S3Destination != nil && strings.EqualFold(s.S3Destination.S3BackupMode, "Enabled") {
		return true
	}
	if s.HTTPEndpointDestination != nil && strings.EqualFold(s.HTTPEndpointDestination.S3BackupMode, "Enabled") {
		return true
	}

	return false
}

// bufferingHints returns the effective buffering hints for a stream, checking all
// configured destination types in priority order.
func bufferingHints(s *DeliveryStream) *BufferingHints {
	if s.S3Destination != nil && s.S3Destination.BufferingHints != nil {
		return s.S3Destination.BufferingHints
	}

	if s.HTTPEndpointDestination != nil && s.HTTPEndpointDestination.BufferingHints != nil {
		return s.HTTPEndpointDestination.BufferingHints
	}

	if s.OpenSearchDestination != nil && s.OpenSearchDestination.BufferingHints != nil {
		return s.OpenSearchDestination.BufferingHints
	}

	return nil
}

// shouldFlushLocked returns true when a size-based flush should happen.
// Must be called with the write lock held.
func (b *InMemoryBackend) shouldFlushLocked(s *DeliveryStream) bool {
	if len(s.Records) == 0 || !b.hasActiveDestinationLocked(s) {
		return false
	}

	hints := bufferingHints(s)
	sizeLimit := 5 // default 5 MB
	if hints != nil && hints.SizeInMBs > 0 {
		sizeLimit = hints.SizeInMBs
	}

	return s.bufferSizeBytes >= sizeLimit*1024*1024
}

// shouldFlushByIntervalLocked returns true when an interval-based flush should happen.
// Must be called with the read lock held.
func (b *InMemoryBackend) shouldFlushByIntervalLocked(s *DeliveryStream) bool {
	if len(s.Records) == 0 || !b.hasActiveDestinationLocked(s) {
		return false
	}

	hints := bufferingHints(s)
	interval := 300 // default 300 seconds
	if hints != nil && hints.IntervalInSeconds > 0 {
		interval = hints.IntervalInSeconds
	}

	return time.Since(s.lastFlush) >= time.Duration(interval)*time.Second
}

// flushSnapshot holds a point-in-time snapshot of records extracted from a stream.
type flushSnapshot struct {
	s3Dest         *S3DestinationDescription
	httpDest       *HTTPEndpointDestinationDescription
	redshiftDest   *RedshiftDestinationDescription
	openSearchDest *OpenSearchDestinationDescription
	splunkDest     *SplunkDestinationDescription
	streamARN      string
	streamName     string
	region         string
	records        [][]byte
}

// extractForFlushLocked snapshots and resets the stream buffer when shouldFlushLocked
// returns true. Returns nil when no flush is needed. Must be called with the write lock held.
func (b *InMemoryBackend) extractForFlushLocked(s *DeliveryStream) *flushSnapshot {
	if !b.shouldFlushLocked(s) {
		return nil
	}

	return b.extractAllRecordsLocked(s)
}

// hasActiveDestinationLocked reports whether the stream has at least one configured
// delivery destination. Must be called with the read lock (or write lock) held.
func (b *InMemoryBackend) hasActiveDestinationLocked(s *DeliveryStream) bool {
	if s.S3Destination != nil && b.s3 != nil {
		return true
	}

	if s.HTTPEndpointDestination != nil &&
		s.HTTPEndpointDestination.EndpointConfiguration != nil &&
		s.HTTPEndpointDestination.EndpointConfiguration.URL != "" {
		return true
	}

	if s.RedshiftDestination != nil && s.RedshiftDestination.ClusterJDBCURL != "" {
		return true
	}

	if s.OpenSearchDestination != nil &&
		(s.OpenSearchDestination.DomainARN != "" || s.OpenSearchDestination.ClusterEndpoint != "") {
		return true
	}

	if s.SplunkDestination != nil && s.SplunkDestination.HECEndpoint != "" {
		return true
	}

	return false
}

// extractAllRecordsLocked unconditionally snapshots and resets the stream buffer.
// Returns nil when there are no records or no active delivery destination.
// Must be called with the write lock held.
func (b *InMemoryBackend) extractAllRecordsLocked(s *DeliveryStream) *flushSnapshot {
	if len(s.Records) == 0 || !b.hasActiveDestinationLocked(s) {
		return nil
	}

	snap := &flushSnapshot{
		records:    s.Records,
		streamARN:  s.ARN,
		streamName: s.Name,
		region:     s.Region,
	}

	if s.S3Destination != nil && b.s3 != nil {
		d := *s.S3Destination
		snap.s3Dest = &d
	}

	if s.HTTPEndpointDestination != nil {
		d := *s.HTTPEndpointDestination
		snap.httpDest = &d
	}

	if s.RedshiftDestination != nil {
		d := *s.RedshiftDestination
		snap.redshiftDest = &d
	}

	if s.OpenSearchDestination != nil {
		d := *s.OpenSearchDestination
		snap.openSearchDest = &d
	}

	if s.SplunkDestination != nil {
		d := *s.SplunkDestination
		snap.splunkDest = &d
	}

	s.Records = [][]byte{}
	s.bufferSizeBytes = 0
	s.lastFlush = time.Now()

	return snap
}

// deliverSnapshot applies optional Lambda transformation and delivers records to all
// configured destinations. Called after the write lock has been released.
func (b *InMemoryBackend) deliverSnapshot(ctx context.Context, snap *flushSnapshot, streamName string) {
	records := snap.records

	// Apply Lambda transformation for S3 destination (only S3 supports it today).
	if snap.s3Dest != nil &&
		snap.s3Dest.ProcessingConfiguration != nil &&
		snap.s3Dest.ProcessingConfiguration.Enabled {
		transformed, err := b.transformRecords(ctx, records, snap.s3Dest, snap.streamARN, snap.region)
		if err == nil && len(transformed) > 0 {
			_ = b.deliverToS3(ctx, transformed, snap.s3Dest, streamName)
		}
	} else if snap.s3Dest != nil {
		_ = b.deliverToS3(ctx, records, snap.s3Dest, streamName)
	}

	if snap.httpDest != nil {
		b.deliverToHTTPEndpoint(ctx, records, snap.httpDest, snap.streamARN)
	}

	if snap.redshiftDest != nil {
		b.deliverToRedshift(ctx, records, snap.redshiftDest, snap.streamARN)
	}

	if snap.openSearchDest != nil {
		b.deliverToOpenSearch(ctx, records, snap.openSearchDest, snap.streamARN)
	}

	if snap.splunkDest != nil {
		b.deliverToSplunk(ctx, records, snap.splunkDest, snap.streamARN)
	}
}

// flushStream delivers all buffered records for a stream to S3.
func (b *InMemoryBackend) flushStream(ctx context.Context, streamName string) {
	b.mu.Lock("flushStream")

	s, ok := b.streams[streamName]
	if !ok {
		b.mu.Unlock()

		return
	}

	snap := b.extractAllRecordsLocked(s)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(ctx, snap, streamName)
	}
}

// transformRecords invokes the configured Lambda function to transform records.
// It returns only the records marked as "Ok" in the Lambda response.
// An error is returned if payload marshaling or Lambda invocation fails, allowing
// the caller to handle the failure (e.g., drop records) rather than silently
// delivering originals.
func (b *InMemoryBackend) transformRecords(
	ctx context.Context,
	records [][]byte,
	dest *S3DestinationDescription,
	streamARN, region string,
) ([][]byte, error) {
	if b.lambda == nil || dest.ProcessingConfiguration == nil {
		return records, nil
	}

	functionName := ""
	for _, proc := range dest.ProcessingConfiguration.Processors {
		if proc.Type == "Lambda" {
			for _, p := range proc.Parameters {
				if p.ParameterName == "LambdaArn" {
					functionName = p.ParameterValue
				}
			}
		}
	}

	if functionName == "" {
		return records, nil
	}

	payload := buildLambdaTransformPayload(records, streamARN, region)
	if payload == nil {
		return nil, ErrTransformPayload
	}

	result, _, err := b.lambda.InvokeFunction(ctx, functionName, "RequestResponse", payload)
	if err != nil {
		return nil, fmt.Errorf("lambda transform invocation failed: %w", err)
	}

	return parseLambdaTransformResponse(result), nil
}

// deliverToS3 concatenates records and writes a single S3 object.
func (b *InMemoryBackend) deliverToS3(
	ctx context.Context,
	records [][]byte,
	dest *S3DestinationDescription,
	streamName string,
) error {
	var buf bytes.Buffer
	for _, rec := range records {
		if len(rec) == 0 {
			continue
		}
		buf.Write(rec)
		// Add newline separator if the record doesn't already end with one.
		if rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	body := buf.Bytes()

	// Skip S3 delivery if all records were empty after filtering.
	if len(body) == 0 {
		return nil
	}

	compression := strings.ToUpper(dest.CompressionFormat)
	if compression == "" {
		compression = "UNCOMPRESSED"
	}

	var finalBody []byte
	var contentEncoding *string

	switch compression {
	case "GZIP":
		compressed, err := gzipCompress(body)
		if err != nil {
			return err
		}
		finalBody = compressed
		contentEncoding = aws.String("gzip")
	default:
		finalBody = body
	}

	bucket := bucketFromARN(dest.BucketARN)
	prefix := dest.Prefix
	key := buildS3Key(prefix, streamName, time.Now())

	input := &sdk_s3.PutObjectInput{
		Bucket:          aws.String(bucket),
		Key:             aws.String(key),
		Body:            io.NopCloser(bytes.NewReader(finalBody)),
		ContentLength:   aws.Int64(int64(len(finalBody))),
		ContentEncoding: contentEncoding,
	}

	_, err := b.s3.PutObject(ctx, input)

	return err
}

// buildS3Key constructs an S3 object key matching the AWS format:
// The key format is: {prefix}{yyyy/MM/dd/HH/}{stream-name}-1-{yyyy-MM-dd-HH-mm-ss}-{uuid}.
func buildS3Key(prefix, streamName string, t time.Time) string {
	ts := t.UTC().Format("2006/01/02/15/")
	filename := fmt.Sprintf("%s-1-%s-%s", streamName, t.UTC().Format("2006-01-02-15-04-05"), uuid.NewString())

	if prefix == "" {
		return ts + filename
	}

	// Ensure prefix ends with "/".
	if !strings.HasSuffix(prefix, "/") {
		prefix += "/"
	}

	return prefix + ts + filename
}

// bucketFromARN extracts the bucket name from an S3 ARN like arn:aws:s3:::bucket-name.
func bucketFromARN(bucketARN string) string {
	// S3 ARNs have the format arn:aws:s3:::bucket-name; split on ":::" to get the bucket name.
	const tripleColonParts = 2

	parts := strings.Split(bucketARN, ":::")
	if len(parts) == tripleColonParts {
		return parts[1]
	}

	// Fallback: last colon-separated segment.
	segments := strings.Split(bucketARN, ":")
	if len(segments) > 0 {
		return segments[len(segments)-1]
	}

	return bucketARN
}

// gzipCompress compresses data using gzip.
func gzipCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)

	if _, err := w.Write(data); err != nil {
		return nil, err
	}

	if err := w.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ListTagsForDeliveryStream returns tags for a delivery stream.
func (b *InMemoryBackend) ListTagsForDeliveryStream(name string) (map[string]string, error) {
	b.mu.RLock("ListTagsForDeliveryStream")
	defer b.mu.RUnlock()

	s, ok := b.streams[name]
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	return s.Tags.Clone(), nil
}

// TagDeliveryStream adds or updates tags on a delivery stream.
func (b *InMemoryBackend) TagDeliveryStream(name string, kv map[string]string) error {
	b.mu.Lock("TagDeliveryStream")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.Merge(kv)

	return nil
}

// UntagDeliveryStream removes tag keys from a delivery stream.
func (b *InMemoryBackend) UntagDeliveryStream(name string, keys []string) error {
	b.mu.Lock("UntagDeliveryStream")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.DeleteKeys(keys)

	return nil
}

// StartDeliveryStreamEncryption enables server-side encryption for a delivery stream.
// In this in-memory implementation the status transitions directly to ENABLED.
func (b *InMemoryBackend) StartDeliveryStreamEncryption(
	_ context.Context, name string, input *EncryptionConfigInput,
) error {
	if input != nil && input.KeyType == "CUSTOMER_MANAGED_CMK" && strings.TrimSpace(input.KeyARN) == "" {
		return fmt.Errorf("%w: KeyARN is required when KeyType is CUSTOMER_MANAGED_CMK", ErrValidation)
	}

	b.mu.Lock("StartDeliveryStreamEncryption")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	if s.DeliveryStreamType == deliveryStreamTypeKinesisSource {
		return fmt.Errorf("%w: cannot enable SSE on a KinesisStreamAsSource stream", ErrValidation)
	}

	cfg := &EncryptionConfig{Status: "ENABLED", KeyType: "AWS_OWNED_CMK"}
	if input != nil {
		if input.KeyType != "" {
			cfg.KeyType = input.KeyType
		}
		cfg.KeyARN = input.KeyARN
	}

	s.Encryption = cfg
	s.LastUpdateTimestamp = time.Now()

	return nil
}

// StopDeliveryStreamEncryption disables server-side encryption for a delivery stream.
// In this in-memory implementation the status transitions directly to DISABLED.
func (b *InMemoryBackend) StopDeliveryStreamEncryption(_ context.Context, name string) error {
	b.mu.Lock("StopDeliveryStreamEncryption")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Encryption = &EncryptionConfig{Status: "DISABLED"}
	s.LastUpdateTimestamp = time.Now()

	return nil
}

// Reset clears all delivery streams, closing their tag registries to prevent leaks.
func (b *InMemoryBackend) Reset() {
	b.mu.Lock("Reset")
	defer b.mu.Unlock()

	for _, s := range b.streams {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	b.streams = make(map[string]*DeliveryStream)
}

// AddStreamInternal deep-copies s into the backend, used for seeding test data.
func (b *InMemoryBackend) AddStreamInternal(s *DeliveryStream) {
	b.mu.Lock("AddStreamInternal")
	defer b.mu.Unlock()

	cp := streamCopy(s)
	b.streams[s.Name] = cp
}

// streamCopy returns a shallow copy of s with pointer fields independently copied.
func streamCopy(s *DeliveryStream) *DeliveryStream {
	cp := *s
	if s.S3Destination != nil {
		dest := *s.S3Destination
		cp.S3Destination = &dest
	}

	if s.HTTPEndpointDestination != nil {
		ep := *s.HTTPEndpointDestination
		cp.HTTPEndpointDestination = &ep
	}

	if s.RedshiftDestination != nil {
		rs := *s.RedshiftDestination
		cp.RedshiftDestination = &rs
	}

	if s.OpenSearchDestination != nil {
		os := *s.OpenSearchDestination
		cp.OpenSearchDestination = &os
	}

	if s.SplunkDestination != nil {
		sp := *s.SplunkDestination
		cp.SplunkDestination = &sp
	}

	if s.Encryption != nil {
		enc := *s.Encryption
		cp.Encryption = &enc
	}

	if s.Source != nil {
		src := *s.Source
		cp.Source = &src
	}

	cp.Records = nil
	cp.BackupRecords = nil

	return &cp
}

// recordIDBytes is the number of random bytes used when generating a record identifier.
const recordIDBytes = 16

// newRecordID generates a random hex record identifier.
func newRecordID() string {
	b := make([]byte, recordIDBytes)
	if _, err := rand.Read(b); err != nil {
		logger.Load(context.Background()).WarnContext(context.Background(),
			"firehose: rand.Read failed; falling back to timestamp-based record ID", "error", err)

		return fmt.Sprintf("rec-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
}

// httpDeliveryTimeout is the maximum time allowed for a single HTTP endpoint delivery attempt.
const httpDeliveryTimeout = 30 * time.Second

// httpMaxRetryDuration is the default max retry window when RetryOptions is not set.
const httpMaxRetryDuration = 300 * time.Second

// buildHTTPEndpointBody encodes records into the AWS Firehose HTTP endpoint JSON payload.
func buildHTTPEndpointBody(records [][]byte) ([]byte, error) {
	type httpRecord struct {
		Data string `json:"data"`
	}
	type httpPayload struct {
		Records   []httpRecord `json:"records"`
		RequestID string       `json:"requestId"`
		Timestamp int64        `json:"timestamp"`
	}

	httpRecords := make([]httpRecord, 0, len(records))
	for _, rec := range records {
		httpRecords = append(httpRecords, httpRecord{Data: base64.StdEncoding.EncodeToString(rec)})
	}

	return json.Marshal(httpPayload{
		RequestID: uuid.NewString(),
		Timestamp: time.Now().UnixMilli(),
		Records:   httpRecords,
	})
}

// buildHTTPEndpointRequest constructs a single POST request for the HTTP endpoint delivery loop.
func buildHTTPEndpointRequest(ctx context.Context, endpointURL, accessKey string, dest *HTTPEndpointDestinationDescription, body []byte) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpointURL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	if accessKey != "" {
		req.Header.Set("X-Amz-Firehose-Access-Key", accessKey)
	}

	if dest.RequestConfiguration != nil {
		for _, attr := range dest.RequestConfiguration.CommonAttributes {
			req.Header.Set(attr.AttributeName, attr.AttributeValue)
		}
	}

	return req, nil
}

// deliverToHTTPEndpoint POSTs records to a Firehose HTTP endpoint destination using the
// AWS Firehose HTTP endpoint delivery format. Retries are attempted within the configured
// RetryOptions.DurationInSeconds window (default 300s) with exponential back-off.
func (b *InMemoryBackend) deliverToHTTPEndpoint(
	ctx context.Context,
	records [][]byte,
	dest *HTTPEndpointDestinationDescription,
	streamARN string,
) {
	if dest.EndpointConfiguration == nil || dest.EndpointConfiguration.URL == "" {
		return
	}

	endpointURL := dest.EndpointConfiguration.URL
	accessKey := dest.EndpointConfiguration.AccessKey

	body, err := buildHTTPEndpointBody(records)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: failed to marshal HTTP endpoint payload", "error", err, "stream", streamARN)
		return
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 1 * time.Second
	client := &http.Client{Timeout: httpDeliveryTimeout}

	for {
		req, reqErr := buildHTTPEndpointRequest(ctx, endpointURL, accessKey, dest, body)
		if reqErr != nil {
			logger.Load(ctx).WarnContext(ctx, "firehose: failed to build HTTP endpoint request", "error", reqErr, "stream", streamARN)
			return
		}

		resp, doErr := client.Do(req)
		if doErr == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.Load(ctx).WarnContext(ctx, "firehose: failed to close HTTP response body", "error", closeErr)
			}

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: HTTP endpoint delivery failed after retries",
				"url", endpointURL, "stream", streamARN)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// redshiftRetryDuration is the default retry window for Redshift delivery.
const redshiftRetryDuration = 7200 * time.Second

// buildRedshiftInsertSQL constructs a batch INSERT SQL statement for Redshift delivery.
// Returns the SQL string and true, or ("", false) when records is empty.
func buildRedshiftInsertSQL(tableName, columns string, records [][]byte) (string, bool) {
	if columns == "" {
		columns = "data"
	}

	sqlParts := make([]string, 0, len(records))
	for _, rec := range records {
		encoded := base64.StdEncoding.EncodeToString(rec)
		escaped := strings.ReplaceAll(encoded, "'", "''")
		sqlParts = append(sqlParts, fmt.Sprintf("('%s')", escaped))
	}

	if len(sqlParts) == 0 {
		return "", false
	}

	return fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", tableName, columns, strings.Join(sqlParts, ",")), true
}

// deliverToRedshift inserts records into a Redshift table via the Redshift Data API
// (ExecuteStatement). Each record is inserted as a single-column row with the raw
// record bytes stored as a base64-encoded string in the configured DataTableName.
//
// The ClusterJDBCURL is parsed to extract the cluster endpoint and database name.
// Format: jdbc:redshift://<host>:<port>/<database>
func (b *InMemoryBackend) deliverToRedshift(
	ctx context.Context,
	records [][]byte,
	dest *RedshiftDestinationDescription,
	streamARN string,
) {
	if dest.ClusterJDBCURL == "" || dest.DataTableName == "" {
		return
	}

	// Parse JDBC URL: jdbc:redshift://host:port/database
	jdbcURL := strings.TrimPrefix(dest.ClusterJDBCURL, "jdbc:redshift://")
	parsed, parseErr := url.Parse("https://" + jdbcURL)
	if parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: cannot parse Redshift JDBC URL",
			"url", dest.ClusterJDBCURL, "stream", streamARN, "error", parseErr)
		return
	}

	host := parsed.Hostname()
	database := strings.TrimPrefix(parsed.Path, "/")

	// Extract cluster identifier from the host: <cluster>.<suffix>.redshift.amazonaws.com
	clusterID := strings.SplitN(host, ".", 2)[0]

	if clusterID == "" || database == "" {
		logger.Load(ctx).WarnContext(ctx, "firehose: Redshift JDBC URL missing cluster or database",
			"url", dest.ClusterJDBCURL, "stream", streamARN)
		return
	}

	insertSQL, ok := buildRedshiftInsertSQL(dest.DataTableName, dest.DataTableColumns, records)
	if !ok {
		return
	}

	rdClient := sdk_rddata.NewFromConfig(aws.Config{Region: b.region})

	maxRetry := redshiftRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 2 * time.Second

	for {
		_, execErr := rdClient.ExecuteStatement(ctx, &sdk_rddata.ExecuteStatementInput{
			ClusterIdentifier: aws.String(clusterID),
			Database:          aws.String(database),
			DbUser:            aws.String(dest.Username),
			Sql:               aws.String(insertSQL),
		})
		if execErr == nil {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: Redshift delivery failed after retries",
				"cluster", clusterID, "database", database, "stream", streamARN, "error", execErr)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 60*time.Second {
				backoff = 60 * time.Second
			}
		}
	}
}

// openSearchBulkTimeout is the HTTP timeout for an OpenSearch bulk index request.
const openSearchBulkTimeout = 30 * time.Second

// buildOpenSearchBulkBody assembles the NDJSON bulk payload for the OpenSearch _bulk API.
// Returns nil when there are no records to send.
func buildOpenSearchBulkBody(records [][]byte) []byte {
	var buf bytes.Buffer
	actionLine := []byte(`{"index":{}}` + "\n")
	for _, rec := range records {
		buf.Write(actionLine)
		buf.Write(rec)
		if len(rec) == 0 || rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	if buf.Len() == 0 {
		return nil
	}

	return buf.Bytes()
}

// deliverToOpenSearch bulk-indexes records into an OpenSearch / Elasticsearch cluster.
// Records are sent as NDJSON using the OpenSearch bulk API (_bulk endpoint).
// Each record becomes one "index" action; the document body is the raw record bytes
// decoded as JSON (or wrapped in {"data":"<base64>"} when the bytes are not valid JSON).
func (b *InMemoryBackend) deliverToOpenSearch(
	ctx context.Context,
	records [][]byte,
	dest *OpenSearchDestinationDescription,
	streamARN string,
) {
	endpoint := dest.ClusterEndpoint
	if endpoint == "" {
		// Derive endpoint from domain ARN: arn:aws:es:<region>:<account>:domain/<name>
		// Local OpenSearch is assumed at http://localhost:9200 in dev/test.
		endpoint = "http://localhost:9200"
	}

	endpoint = strings.TrimRight(endpoint, "/")
	indexName := dest.IndexName
	if indexName == "" {
		indexName = "firehose"
	}

	bulkURL := fmt.Sprintf("%s/%s/_bulk", endpoint, indexName)

	bodyBytes := buildOpenSearchBulkBody(records)
	if bodyBytes == nil {
		return
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 1 * time.Second
	client := &http.Client{Timeout: openSearchBulkTimeout}

	for {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, bulkURL, bytes.NewReader(bodyBytes))
		if reqErr != nil {
			logger.Load(ctx).WarnContext(ctx,
				"firehose: failed to build OpenSearch bulk request", "error", reqErr, "stream", streamARN)
			return
		}

		req.Header.Set("Content-Type", "application/x-ndjson")

		resp, doErr := client.Do(req)
		if doErr == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.Load(ctx).WarnContext(ctx, "firehose: failed to close OpenSearch response body", "error", closeErr)
			}

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: OpenSearch delivery failed after retries",
				"url", bulkURL, "stream", streamARN)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

// splunkHECTimeout is the HTTP timeout for a Splunk HEC request.
const splunkHECTimeout = 30 * time.Second

// buildSplunkBody assembles the request body and content-type for a Splunk HEC delivery.
// hecType should be the lower-cased HECEndpointType value.
// Returns (nil, "") when the resulting body is empty.
func buildSplunkBody(records [][]byte, hecType string) ([]byte, string) {
	if hecType == "event" {
		type hecEvent struct {
			Event string `json:"event"`
		}

		var buf bytes.Buffer
		for _, rec := range records {
			line, marshalErr := json.Marshal(hecEvent{Event: string(rec)})
			if marshalErr != nil {
				continue
			}
			buf.Write(line)
		}

		if buf.Len() == 0 {
			return nil, ""
		}

		return buf.Bytes(), "application/json"
	}

	var buf bytes.Buffer
	for _, rec := range records {
		buf.Write(rec)
		if len(rec) == 0 || rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	if buf.Len() == 0 {
		return nil, ""
	}

	return buf.Bytes(), "text/plain"
}

// deliverToSplunk POSTs records to a Splunk HTTP Event Collector (HEC) endpoint.
// Each record is sent as a separate JSON event in the HEC raw format, batched
// into a single POST when the HEC endpoint type is "Raw" (default).
// HECEndpointType "Event" wraps each record in the HEC event JSON envelope.
func (b *InMemoryBackend) deliverToSplunk(
	ctx context.Context,
	records [][]byte,
	dest *SplunkDestinationDescription,
	streamARN string,
) {
	if dest.HECEndpoint == "" {
		return
	}

	hecURL := strings.TrimRight(dest.HECEndpoint, "/")

	body, contentType := buildSplunkBody(records, strings.ToLower(dest.HECEndpointType))
	if len(body) == 0 {
		return
	}

	maxRetry := httpMaxRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	deadline := time.Now().Add(maxRetry)
	backoff := 1 * time.Second
	client := &http.Client{Timeout: splunkHECTimeout}

	for {
		req, reqErr := http.NewRequestWithContext(ctx, http.MethodPost, hecURL, bytes.NewReader(body))
		if reqErr != nil {
			logger.Load(ctx).WarnContext(ctx, "firehose: failed to build Splunk HEC request", "error", reqErr, "stream", streamARN)
			return
		}

		req.Header.Set("Content-Type", contentType)
		if dest.HECToken != "" {
			req.Header.Set("Authorization", "Splunk "+dest.HECToken)
		}

		resp, doErr := client.Do(req)
		if doErr == nil {
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.Load(ctx).WarnContext(ctx, "firehose: failed to close Splunk response body", "error", closeErr)
			}

			if resp.StatusCode >= 200 && resp.StatusCode < 300 {
				return
			}
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: Splunk HEC delivery failed after retries",
				"url", hecURL, "stream", streamARN)
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}
