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
	"github.com/blackbirdworks/gopherstack/pkgs/collections"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/store"
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
	DataFormatConversion             *DataFormatConversionConfig       `json:"DataFormatConversionConfiguration,omitempty"`
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

// RedshiftCopyCommand holds the Redshift COPY command configuration. On the wire this
// nests under RedshiftDestinationDescription.CopyCommand (and, on the request side,
// RedshiftDestinationConfiguration.CopyCommand) rather than as flat fields.
type RedshiftCopyCommand struct {
	DataTableName    string `json:"DataTableName"`
	DataTableColumns string `json:"DataTableColumns,omitempty"`
	CopyOptions      string `json:"CopyOptions,omitempty"`
}

// RedshiftDestinationDescription holds a Redshift destination config.
type RedshiftDestinationDescription struct {
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration,omitempty"`
	RetryOptions            *RetryOptions            `json:"RetryOptions,omitempty"`
	S3BackupDescription     *S3BackupDescription     `json:"S3BackupDescription,omitempty"`
	// S3Destination is the required intermediate S3 staging location that Amazon
	// Redshift's COPY command reads from (wire field "S3DestinationDescription").
	S3Destination  *S3DestinationDescription `json:"S3DestinationDescription,omitempty"`
	CopyCommand    *RedshiftCopyCommand      `json:"CopyCommand,omitempty"`
	ClusterJDBCURL string                    `json:"ClusterJDBCURL,omitempty"`
	Username       string                    `json:"Username,omitempty"`
	RoleARN        string                    `json:"RoleARN,omitempty"`
	S3BackupMode   string                    `json:"S3BackupMode,omitempty"`
	DestinationID  string                    `json:"DestinationId,omitempty"`
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

// regionContextKey is the context key for the per-request AWS region.
type regionContextKey struct{}

// InMemoryBackend is the in-memory store for Firehose resources.
type InMemoryBackend struct {
	s3             S3Storer
	lambda         LambdaInvoker
	kinesisBackend KinesisReader
	registry       *store.Registry
	// streams is a single flat table of every delivery stream, composite-keyed by
	// "region|name" (see regionKey/deliveryStreamKeyFn in store_setup.go) so that
	// same-named streams stay isolated across regions exactly as the old
	// map[region]map[name]*DeliveryStream nesting did.
	streams *store.Table[DeliveryStream]
	// streamsByRegion groups streams entries have registered exactly like the old
	// per-region outer map key did, so a region-scoped "list everything" (what
	// ListDeliveryStreamsByType and FlushAll need) stays an O(region size) index
	// lookup instead of an O(all regions) scan.
	streamsByRegion *store.Index[DeliveryStream]
	// pollerCancel maps region → stream name → cancel func for active Kinesis source pollers.
	pollerCancel map[string]map[string]context.CancelFunc
	// sortedNamesCache caches the alphabetically sorted stream names per region so
	// ListDeliveryStreams does not re-sort on every call. Invalidated on create/delete.
	sortedNamesCache map[string][]string
	// pendingFlush tracks the set of streams (region → name) that hold buffered records
	// eligible for an interval-based flush, so intervalFlusher only inspects streams that
	// could actually need flushing rather than scanning every stream each tick.
	pendingFlush map[string]map[string]struct{}
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

	b := &InMemoryBackend{
		registry:         store.NewRegistry(),
		pollerCancel:     make(map[string]map[string]context.CancelFunc),
		sortedNamesCache: make(map[string][]string),
		pendingFlush:     make(map[string]map[string]struct{}),
		accountID:        accountID,
		region:           region,
		mu:               lockmetrics.New("firehose"),
		svcCtx:           svcCtx,
	}
	registerAllTables(b)

	return b
}

// Region returns the AWS region this backend is configured for.
func (b *InMemoryBackend) Region() string { return b.region }

// getRegionFromContext extracts the per-request AWS region from ctx, falling
// back to the backend's configured region when none is present.
func getRegionFromContext(ctx context.Context, b *InMemoryBackend) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return b.region
}

// pollerStore returns the poller-cancel map for region, lazily creating it.
// Must be called with the lock held.
func (b *InMemoryBackend) pollerStore(region string) map[string]context.CancelFunc {
	if b.pollerCancel[region] == nil {
		b.pollerCancel[region] = make(map[string]context.CancelFunc)
	}

	return b.pollerCancel[region]
}

// invalidateNamesCacheLocked drops the cached sorted-name slice for region so the next
// ListDeliveryStreams rebuilds it. Must be called with the write lock held.
func (b *InMemoryBackend) invalidateNamesCacheLocked(region string) {
	delete(b.sortedNamesCache, region)
}

// markPendingFlushLocked records that region/name holds buffered records that may need an
// interval flush. Must be called with the write lock held.
func (b *InMemoryBackend) markPendingFlushLocked(region, name string) {
	if b.pendingFlush[region] == nil {
		b.pendingFlush[region] = make(map[string]struct{})
	}
	b.pendingFlush[region][name] = struct{}{}
}

// clearPendingFlushLocked removes region/name from the interval-flush watch set. Must be
// called with the write lock held.
func (b *InMemoryBackend) clearPendingFlushLocked(region, name string) {
	if set := b.pendingFlush[region]; set != nil {
		delete(set, name)
		if len(set) == 0 {
			delete(b.pendingFlush, region)
		}
	}
}

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
func (b *InMemoryBackend) CreateDeliveryStream(
	ctx context.Context, input CreateDeliveryStreamInput,
) (*DeliveryStream, error) {
	if strings.TrimSpace(input.Name) == "" {
		return nil, fmt.Errorf("%w: DeliveryStreamName is required", ErrValidation)
	}

	b.mu.Lock("CreateDeliveryStream")

	region := getRegionFromContext(ctx, b)

	if _, ok := b.streams.Get(regionKey(region, input.Name)); ok {
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
	streamARN := arn.Build("firehose", region, b.accountID, "deliverystream/"+input.Name)
	s := &DeliveryStream{
		Name:                    input.Name,
		ARN:                     streamARN,
		DeliveryStreamType:      streamType,
		VersionID:               "1",
		Status:                  "ACTIVE",
		Records:                 [][]byte{},
		BackupRecords:           [][]byte{},
		Tags:                    tags.New("firehose." + region + "." + input.Name + ".tags"),
		AccountID:               b.accountID,
		Region:                  region,
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
	b.streams.Put(s)
	b.invalidateNamesCacheLocked(region)

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
		b.launchKinesisPoller(region, input.Name, kinesisStreamARN)
	}

	return result, nil
}

// DeleteDeliveryStream deletes a delivery stream.
func (b *InMemoryBackend) DeleteDeliveryStream(ctx context.Context, name string) error {
	b.mu.Lock("DeleteDeliveryStream")

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	if s.Tags != nil {
		s.Tags.Close()
	}

	b.streams.Delete(regionKey(region, name))
	b.invalidateNamesCacheLocked(region)
	b.clearPendingFlushLocked(region, name)

	// Stop Kinesis poller if one is running for this stream.
	pollers := b.pollerStore(region)
	cancel := pollers[name]
	delete(pollers, name)

	b.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	return nil
}

// DescribeDeliveryStream returns a delivery stream by name.
func (b *InMemoryBackend) DescribeDeliveryStream(ctx context.Context, name string) (*DeliveryStream, error) {
	b.mu.RLock("DescribeDeliveryStream")
	defer b.mu.RUnlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	return streamCopy(s), nil
}

// ListDeliveryStreams returns all delivery stream names in the request's region
// in alphabetical order.
func (b *InMemoryBackend) ListDeliveryStreams(ctx context.Context) []string {
	return b.ListDeliveryStreamsByType(ctx, "")
}

// ListDeliveryStreamsByType returns delivery stream names in the request's region in
// alphabetical order, optionally filtered to a single DeliveryStreamType (DirectPut or
// KinesisStreamAsSource). An empty streamType returns all streams. The full sorted-name
// list is cached per region and reused across calls until a create/delete invalidates it,
// so repeated listing does not re-sort the whole namespace every time.
func (b *InMemoryBackend) ListDeliveryStreamsByType(ctx context.Context, streamType string) []string {
	region := getRegionFromContext(ctx, b)

	// Fast path: cached full list, no type filter.
	if streamType == "" {
		b.mu.RLock("ListDeliveryStreams")
		if cached, ok := b.sortedNamesCache[region]; ok {
			out := make([]string, len(cached))
			copy(out, cached)
			b.mu.RUnlock()

			return out
		}
		b.mu.RUnlock()
	}

	b.mu.Lock("ListDeliveryStreams")
	defer b.mu.Unlock()

	items := b.streamsByRegion.Get(region)

	sorted, ok := b.sortedNamesCache[region]
	if !ok {
		sorted = collections.Map(items, func(s *DeliveryStream) string { return s.Name })
		sort.Strings(sorted)
		b.sortedNamesCache[region] = sorted
	}

	if streamType == "" {
		out := make([]string, len(sorted))
		copy(out, sorted)

		return out
	}

	byName := make(map[string]*DeliveryStream, len(items))
	for _, s := range items {
		byName[s.Name] = s
	}

	filtered := make([]string, 0, len(sorted))
	for _, name := range sorted {
		s := byName[name]
		if s == nil {
			continue
		}
		effectiveType := s.DeliveryStreamType
		if effectiveType == "" {
			effectiveType = deliveryStreamTypeDirectPut
		}
		if effectiveType == streamType {
			filtered = append(filtered, name)
		}
	}

	return filtered
}

// PutRecord appends a record to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecord(ctx context.Context, streamName string, data []byte) error {
	if len(data) == 0 {
		return fmt.Errorf("%w: record Data must not be empty", ErrValidation)
	}

	if len(data) > maxRecordBytes {
		return fmt.Errorf("%w: record size %d exceeds maximum of %d bytes",
			ErrRecordTooLarge, len(data), maxRecordBytes)
	}

	b.mu.Lock("PutRecord")

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, streamName))
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
	b.updateFlushWatchLocked(region, streamName, s, snap)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(b.svcCtx, snap, streamName)
	}

	return nil
}

// updateFlushWatchLocked keeps the interval-flush watch set in sync after buffering
// records: a size-based flush (snap != nil) clears the entry, while remaining buffered
// records for an active destination mark the stream as pending. Must be called with the
// write lock held.
func (b *InMemoryBackend) updateFlushWatchLocked(region, name string, s *DeliveryStream, snap *flushSnapshot) {
	if snap != nil {
		b.clearPendingFlushLocked(region, name)

		return
	}

	if len(s.Records) > 0 && b.hasActiveDestinationLocked(s) {
		b.markPendingFlushLocked(region, name)
	}
}

// PutRecordBatch appends multiple records to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecordBatch(ctx context.Context, streamName string, records [][]byte) (int, error) {
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

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, streamName))
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
	b.updateFlushWatchLocked(region, streamName, s, snap)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(b.svcCtx, snap, streamName)
	}

	return 0, nil
}

// UpdateDestinationInput holds the destination update fields for UpdateDestination.
// Exactly one destination field should be non-nil.
type UpdateDestinationInput struct {
	S3Destination           *S3DestinationDescription
	HTTPEndpointDestination *HTTPEndpointDestinationDescription
	RedshiftDestination     *RedshiftDestinationDescription
	OpenSearchDestination   *OpenSearchDestinationDescription
	SplunkDestination       *SplunkDestinationDescription
}

// applyDestinationUpdate sets the single destination supplied in input and clears every
// other destination type, so an UpdateDestination call can switch a stream from one
// destination type to another. AWS permits exactly one destination update per call and a
// stream has exactly one active destination; providing none or more than one is rejected.
func applyDestinationUpdate(s *DeliveryStream, input UpdateDestinationInput) error {
	provided := 0
	if input.S3Destination != nil {
		provided++
	}
	if input.HTTPEndpointDestination != nil {
		provided++
	}
	if input.RedshiftDestination != nil {
		provided++
	}
	if input.OpenSearchDestination != nil {
		provided++
	}
	if input.SplunkDestination != nil {
		provided++
	}

	if provided != 1 {
		return fmt.Errorf("%w: exactly one destination update must be specified, got %d", ErrValidation, provided)
	}

	// Preserve the existing DestinationId across the switch when present.
	destID := currentDestinationID(s)

	s.S3Destination = input.S3Destination
	s.HTTPEndpointDestination = input.HTTPEndpointDestination
	s.RedshiftDestination = input.RedshiftDestination
	s.OpenSearchDestination = input.OpenSearchDestination
	s.SplunkDestination = input.SplunkDestination

	setDestinationID(s, destID)

	return nil
}

// currentDestinationID returns the DestinationId currently set on the stream's active
// destination, or the default when none is set.
func currentDestinationID(s *DeliveryStream) string {
	switch {
	case s.S3Destination != nil && s.S3Destination.DestinationID != "":
		return s.S3Destination.DestinationID
	case s.HTTPEndpointDestination != nil && s.HTTPEndpointDestination.DestinationID != "":
		return s.HTTPEndpointDestination.DestinationID
	case s.OpenSearchDestination != nil && s.OpenSearchDestination.DestinationID != "":
		return s.OpenSearchDestination.DestinationID
	case s.SplunkDestination != nil && s.SplunkDestination.DestinationID != "":
		return s.SplunkDestination.DestinationID
	case s.RedshiftDestination != nil && s.RedshiftDestination.DestinationID != "":
		return s.RedshiftDestination.DestinationID
	default:
		return "destinationId-000000000001"
	}
}

// setDestinationID stamps destID onto whichever destination is now active on the stream.
func setDestinationID(s *DeliveryStream, destID string) {
	switch {
	case s.S3Destination != nil:
		s.S3Destination.DestinationID = destID
	case s.HTTPEndpointDestination != nil:
		s.HTTPEndpointDestination.DestinationID = destID
	case s.OpenSearchDestination != nil:
		s.OpenSearchDestination.DestinationID = destID
	case s.SplunkDestination != nil:
		s.SplunkDestination.DestinationID = destID
	case s.RedshiftDestination != nil:
		s.RedshiftDestination.DestinationID = destID
	}
}

// UpdateDestination updates the destination configuration of an existing stream.
// AWS allows updating exactly one destination type per call.
func (b *InMemoryBackend) UpdateDestination(
	ctx context.Context,
	streamName, currentVersionID string,
	input UpdateDestinationInput,
) error {
	b.mu.Lock("UpdateDestination")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, streamName))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	// AWS requires CurrentDeliveryStreamVersionId on every UpdateDestination call and
	// rejects the request when it does not match the stream's current version.
	if currentVersionID == "" {
		return fmt.Errorf("%w: CurrentDeliveryStreamVersionId is required", ErrValidation)
	}

	if s.VersionID != currentVersionID {
		return fmt.Errorf("%w: version mismatch: expected %s got %s", ErrValidation, currentVersionID, s.VersionID)
	}

	if err := applyDestinationUpdate(s, input); err != nil {
		return err
	}

	s.LastUpdateTimestamp = time.Now()

	v, err := strconv.Atoi(s.VersionID)
	if err != nil {
		logger.Load(ctx).WarnContext(ctx,
			"firehose: unexpected non-integer VersionID; resetting to 1",
			"stream", streamName, "versionID", s.VersionID, "error", err)

		v = 0
	}

	s.VersionID = strconv.Itoa(v + 1)

	return nil
}

// FlushAll forces delivery of all buffered records across all streams in all regions.
// Used by tests and for graceful shutdown.
func (b *InMemoryBackend) FlushAll(ctx context.Context) {
	b.mu.RLock("FlushAll")
	type streamRef struct {
		region string
		name   string
	}
	all := b.streams.All()
	refs := make([]streamRef, 0, len(all))
	for _, s := range all {
		refs = append(refs, streamRef{region: s.Region, name: s.Name})
	}
	b.mu.RUnlock()

	for _, ref := range refs {
		b.flushStream(ctx, ref.region, ref.name)
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
			type streamRef struct {
				region string
				name   string
			}
			var refs []streamRef
			// Only inspect streams flagged as holding buffered records, rather than
			// scanning every region×stream on each tick.
			for region, pending := range b.pendingFlush {
				for name := range pending {
					s, ok := b.streams.Get(regionKey(region, name))
					if ok && b.shouldFlushByIntervalLocked(s) {
						refs = append(refs, streamRef{region: region, name: name})
					}
				}
			}
			b.mu.RUnlock()

			for _, ref := range refs {
				b.flushStream(ctx, ref.region, ref.name)
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
	// backupRecords are the source records copied for an S3 backup destination
	// (S3BackupMode=Enabled); they are delivered to the backup bucket verbatim.
	backupRecords [][]byte
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
		records:       s.Records,
		backupRecords: s.BackupRecords,
		streamARN:     s.ARN,
		streamName:    s.Name,
		region:        s.Region,
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
	s.BackupRecords = [][]byte{}
	s.bufferSizeBytes = 0
	s.lastFlush = time.Now()

	return snap
}

// deliverSnapshot applies optional Lambda transformation and delivers records to all
// configured destinations, routing processing/delivery failures to the S3 error output and
// recording the FailedRecords metric. Called after the write lock has been released.
func (b *InMemoryBackend) deliverSnapshot(ctx context.Context, snap *flushSnapshot, streamName string) {
	if snap.s3Dest != nil {
		b.deliverS3Destination(ctx, snap, streamName)
	}

	if snap.httpDest != nil {
		b.deliverProcessedNonS3(ctx, snap, streamName, snap.httpDest.ProcessingConfiguration,
			snap.httpDest.S3BackupDescription, snap.httpDest.CloudWatchLoggingOptions,
			func(recs [][]byte) {
				b.deliverToHTTPEndpoint(ctx, recs, snap.httpDest, snap.streamARN)
			})
	}

	if snap.redshiftDest != nil {
		b.deliverProcessedNonS3(ctx, snap, streamName, snap.redshiftDest.ProcessingConfiguration,
			snap.redshiftDest.S3BackupDescription, nil,
			func(recs [][]byte) {
				b.deliverToRedshift(ctx, recs, snap.redshiftDest, snap.streamARN)
			})
	}

	if snap.openSearchDest != nil {
		b.deliverProcessedNonS3(ctx, snap, streamName, snap.openSearchDest.ProcessingConfiguration,
			snap.openSearchDest.S3BackupDescription, snap.openSearchDest.CloudWatchLoggingOptions,
			func(recs [][]byte) {
				b.deliverToOpenSearch(ctx, recs, snap.openSearchDest, snap.streamARN)
			})
	}

	if snap.splunkDest != nil {
		b.deliverProcessedNonS3(ctx, snap, streamName, snap.splunkDest.ProcessingConfiguration,
			snap.splunkDest.S3BackupDescription, snap.splunkDest.CloudWatchLoggingOptions,
			func(recs [][]byte) {
				b.deliverToSplunk(ctx, recs, snap.splunkDest, snap.streamARN)
			})
	}
}

// deliverS3Destination runs the S3 delivery pipeline: Lambda transform, optional
// DataFormatConversion, dynamic-partitioned delivery, error-output routing for
// processing/conversion/partition failures, the FailedRecords metric, and S3 backup.
func (b *InMemoryBackend) deliverS3Destination(ctx context.Context, snap *flushSnapshot, streamName string) {
	dest := snap.s3Dest

	ok, failed, err := b.applyTransform(ctx, snap.records, dest.ProcessingConfiguration, snap.streamARN, snap.region)
	if err != nil {
		b.logDeliveryIssue(ctx, dest.CloudWatchLoggingOptions, streamName,
			"lambda transform invocation failed; routing records to error output", err)
		// Invocation failure: the transform produced no usable output, so every source
		// record is routed to the error output, matching AWS's processing-failed behaviour.
		failed = append(failed, snap.records...)
		ok = nil
	}

	if dest.DataFormatConversion != nil && dest.DataFormatConversion.Enabled {
		converted, convFailed := convertRecords(dest.DataFormatConversion, ok)
		ok = converted
		failed = append(failed, convFailed...)
	}

	unpartitioned, deliverErr := b.deliverToS3(ctx, ok, dest, streamName)
	failed = append(failed, unpartitioned...)
	if deliverErr != nil {
		b.logDeliveryIssue(ctx, dest.CloudWatchLoggingOptions, streamName, "S3 delivery failed", deliverErr)
		failed = append(failed, ok...)
	}

	if len(failed) > 0 {
		b.routeToErrorOutput(ctx, failed, dest, streamName)
		b.recordFailedRecords(snap.region, streamName, len(failed))
	}

	b.deliverS3Backup(ctx, snap, dest.S3BackupDescription, streamName)
}

// deliverProcessedNonS3 runs the shared delivery pipeline for non-S3 destinations: it
// applies the Lambda transform, routes processing failures to the S3 backup destination (if
// configured) and records them in the FailedRecords metric, then delivers the surviving
// records via the supplied deliver func. It also delivers any S3 backup copies.
func (b *InMemoryBackend) deliverProcessedNonS3(
	ctx context.Context,
	snap *flushSnapshot,
	streamName string,
	pc *ProcessingConfiguration,
	backup *S3BackupDescription,
	cwLog *CloudWatchLoggingOptions,
	deliver func(records [][]byte),
) {
	ok, failed, err := b.applyTransform(ctx, snap.records, pc, snap.streamARN, snap.region)
	if err != nil {
		b.logDeliveryIssue(ctx, cwLog, streamName,
			"lambda transform invocation failed; routing records to backup", err)
		failed = append(failed, snap.records...)
		ok = nil
	}

	if len(failed) > 0 {
		if backup != nil {
			_ = b.writeRecordsToBucket(ctx, failed, backup.BucketARN,
				backup.Prefix, "", backup.CompressionFormat, streamName)
		}
		b.recordFailedRecords(snap.region, streamName, len(failed))
	}

	if len(ok) > 0 {
		deliver(ok)
	}

	b.deliverS3Backup(ctx, snap, backup, streamName)
}

// deliverS3Backup delivers the buffered S3 backup copies (accumulated when S3BackupMode is
// Enabled) to the backup bucket. It is a no-op when there are no backup records or no
// backup destination is configured.
func (b *InMemoryBackend) deliverS3Backup(
	ctx context.Context,
	snap *flushSnapshot,
	backup *S3BackupDescription,
	streamName string,
) {
	if len(snap.backupRecords) == 0 || backup == nil || backup.BucketARN == "" {
		return
	}

	_ = b.writeRecordsToBucket(ctx, snap.backupRecords, backup.BucketARN,
		backup.Prefix, "", backup.CompressionFormat, streamName)
}

// applyTransform runs the configured Lambda transform over records, separating the records
// to deliver (ok) from records that failed processing (failed, to be routed to the error
// output). When no transform is configured, all records pass through as ok. A non-nil error
// indicates the invocation itself failed; callers route all source records to the error
// output in that case.
func (b *InMemoryBackend) applyTransform(
	ctx context.Context,
	records [][]byte,
	pc *ProcessingConfiguration,
	streamARN, region string,
) ([][]byte, [][]byte, error) {
	if b.lambda == nil || pc == nil || !pc.Enabled {
		return records, nil, nil
	}

	functionName := lambdaFunctionName(pc)
	if functionName == "" {
		return records, nil, nil
	}

	payload, idToOriginal := buildLambdaTransformPayload(records, streamARN, region)
	if payload == nil {
		return nil, nil, ErrTransformPayload
	}

	result, _, invokeErr := b.lambda.InvokeFunction(ctx, functionName, "RequestResponse", payload)
	if invokeErr != nil {
		return nil, nil, fmt.Errorf("lambda transform invocation failed: %w", invokeErr)
	}

	outcome, parsed := parseLambdaTransformResponse(result, idToOriginal)
	if !parsed {
		return nil, nil, fmt.Errorf("%w: malformed lambda transform response", ErrTransformPayload)
	}

	return outcome.Ok, outcome.Failed, nil
}

// lambdaFunctionName extracts the Lambda function ARN from a ProcessingConfiguration.
func lambdaFunctionName(pc *ProcessingConfiguration) string {
	for _, proc := range pc.Processors {
		if proc.Type != "Lambda" {
			continue
		}
		for _, p := range proc.Parameters {
			if p.ParameterName == "LambdaArn" {
				return p.ParameterValue
			}
		}
	}

	return ""
}

// recordFailedRecords increments the FailedRecords delivery metric for a stream.
func (b *InMemoryBackend) recordFailedRecords(region, streamName string, n int) {
	if n <= 0 {
		return
	}

	b.mu.Lock("recordFailedRecords")
	defer b.mu.Unlock()

	if s, ok := b.streams.Get(regionKey(region, streamName)); ok {
		s.Metrics.FailedRecords += int64(n)
	}
}

// logDeliveryIssue emits a delivery error to the logger, honouring the destination's
// CloudWatch logging options: when logging is enabled the configured log group/stream are
// attached so operators can correlate the failure, matching the CloudWatch error log that
// Firehose writes for failed deliveries.
func (b *InMemoryBackend) logDeliveryIssue(
	ctx context.Context,
	cwLog *CloudWatchLoggingOptions,
	streamName, msg string,
	err error,
) {
	attrs := []any{"stream", streamName, "error", err}
	if cwLog != nil && cwLog.Enabled {
		attrs = append(attrs, "logGroup", cwLog.LogGroupName, "logStream", cwLog.LogStreamName)
	}

	logger.Load(ctx).WarnContext(ctx, "firehose: "+msg, attrs...)
}

// flushStream delivers all buffered records for a stream to S3.
func (b *InMemoryBackend) flushStream(ctx context.Context, region, streamName string) {
	b.mu.Lock("flushStream")

	s, ok := b.streams.Get(regionKey(region, streamName))
	if !ok {
		b.mu.Unlock()

		return
	}

	snap := b.extractAllRecordsLocked(s)
	b.clearPendingFlushLocked(region, streamName)
	b.mu.Unlock()

	if snap != nil {
		b.deliverSnapshot(ctx, snap, streamName)
	}
}

// deliverToS3 writes records to the S3 destination, applying dynamic partitioning and the
// configured file extension. Records that cannot be partitioned (per dynamic-partitioning
// rules) are returned so the caller routes them to the error output. A non-nil error is
// returned when an underlying PutObject call fails.
func (b *InMemoryBackend) deliverToS3(
	ctx context.Context,
	records [][]byte,
	dest *S3DestinationDescription,
	streamName string,
) ([][]byte, error) {
	if b.s3 == nil || len(records) == 0 {
		return nil, nil
	}

	groups, unpartitioned := resolvePartitions(records, dest.Prefix, dest.DynamicPartitioningConfiguration)

	var firstErr error
	for _, group := range groups {
		if putErr := b.writeRecordsToBucket(
			ctx, group.records, dest.BucketARN, group.prefix,
			dest.FileExtension, dest.CompressionFormat, streamName,
		); putErr != nil && firstErr == nil {
			firstErr = putErr
		}
	}

	return unpartitioned, firstErr
}

// routeToErrorOutput writes failed records to the S3 error output location: the backup
// bucket under its prefix when an S3 backup destination is configured, otherwise the main
// bucket under ErrorOutputPrefix (defaulting to "processing-failed/"). This mirrors AWS,
// which delivers processing/delivery failures to the configured error prefix rather than
// dropping them.
func (b *InMemoryBackend) routeToErrorOutput(
	ctx context.Context,
	records [][]byte,
	dest *S3DestinationDescription,
	streamName string,
) {
	if b.s3 == nil || len(records) == 0 {
		return
	}

	bucketARN := dest.BucketARN
	prefix := dest.ErrorOutputPrefix
	if prefix == "" {
		prefix = "processing-failed/"
	}

	if dest.S3BackupDescription != nil && dest.S3BackupDescription.BucketARN != "" {
		bucketARN = dest.S3BackupDescription.BucketARN
	}

	_ = b.writeRecordsToBucket(ctx, records, bucketARN, prefix, "", dest.CompressionFormat, streamName)
}

// writeRecordsToBucket concatenates records (newline-separated), optionally gzip-compresses
// them, and writes a single object under the given bucket/prefix. fileExtension, when set,
// is appended to the generated object key. It is a no-op when the effective body is empty.
func (b *InMemoryBackend) writeRecordsToBucket(
	ctx context.Context,
	records [][]byte,
	bucketARN, prefix, fileExtension, compressionFormat, streamName string,
) error {
	if b.s3 == nil {
		return nil
	}

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
	if len(body) == 0 {
		return nil
	}

	compression := strings.ToUpper(compressionFormat)
	if compression == "" {
		compression = "UNCOMPRESSED"
	}

	var finalBody []byte
	var contentEncoding *string

	switch compression {
	case "GZIP":
		compressed, gzErr := gzipCompress(body)
		if gzErr != nil {
			return gzErr
		}
		finalBody = compressed
		contentEncoding = aws.String("gzip")
	default:
		finalBody = body
	}

	bucket := bucketFromARN(bucketARN)
	key := buildS3Key(prefix, streamName, fileExtension, time.Now())

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
// {prefix}{yyyy/MM/dd/HH/}{stream-name}-1-{yyyy-MM-dd-HH-mm-ss}-{uuid}{fileExtension}.
// When fileExtension is set it is appended verbatim (AWS accepts extensions with or without
// a leading dot).
func buildS3Key(prefix, streamName, fileExtension string, t time.Time) string {
	ts := t.UTC().Format("2006/01/02/15/")
	filename := fmt.Sprintf("%s-1-%s-%s", streamName, t.UTC().Format("2006-01-02-15-04-05"), uuid.NewString())
	filename += fileExtension

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
func (b *InMemoryBackend) ListTagsForDeliveryStream(ctx context.Context, name string) (map[string]string, error) {
	b.mu.RLock("ListTagsForDeliveryStream")
	defer b.mu.RUnlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return nil, fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	return s.Tags.Clone(), nil
}

// TagDeliveryStream adds or updates tags on a delivery stream.
func (b *InMemoryBackend) TagDeliveryStream(ctx context.Context, name string, kv map[string]string) error {
	b.mu.Lock("TagDeliveryStream")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.Merge(kv)

	return nil
}

// UntagDeliveryStream removes tag keys from a delivery stream.
func (b *InMemoryBackend) UntagDeliveryStream(ctx context.Context, name string, keys []string) error {
	b.mu.Lock("UntagDeliveryStream")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Tags.DeleteKeys(keys)

	return nil
}

// StartDeliveryStreamEncryption enables server-side encryption for a delivery stream.
// In this in-memory implementation the status transitions directly to ENABLED.
func (b *InMemoryBackend) StartDeliveryStreamEncryption(
	ctx context.Context, name string, input *EncryptionConfigInput,
) error {
	if input != nil && input.KeyType == "CUSTOMER_MANAGED_CMK" && strings.TrimSpace(input.KeyARN) == "" {
		return fmt.Errorf("%w: KeyARN is required when KeyType is CUSTOMER_MANAGED_CMK", ErrValidation)
	}

	b.mu.Lock("StartDeliveryStreamEncryption")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
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
func (b *InMemoryBackend) StopDeliveryStreamEncryption(ctx context.Context, name string) error {
	b.mu.Lock("StopDeliveryStreamEncryption")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
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

	for _, s := range b.streams.All() {
		if s.Tags != nil {
			s.Tags.Close()
		}
	}

	b.registry.ResetAll()
}

// AddStreamInternal deep-copies s into the backend, used for seeding test data.
func (b *InMemoryBackend) AddStreamInternal(s *DeliveryStream) {
	b.mu.Lock("AddStreamInternal")
	defer b.mu.Unlock()

	cp := streamCopy(s)
	if cp.Region == "" {
		// A store.Table key is a pure function of the value (see store_setup.go's
		// deliveryStreamKeyFn), so -- unlike the old map[region]map[name]*DeliveryStream,
		// where a caller-omitted Region only affected which outer-map bucket the entry
		// landed in -- cp.Region itself must be defaulted here for the entry to be
		// keyed (and therefore later found by DescribeDeliveryStream et al.) under the
		// backend's own region.
		cp.Region = b.region
	}
	b.streams.Put(cp)
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
func newRecordID(ctx context.Context) string {
	b := make([]byte, recordIDBytes)
	if _, err := rand.Read(b); err != nil {
		logger.Load(ctx).WarnContext(ctx,
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
		RequestID string       `json:"requestId"`
		Records   []httpRecord `json:"records"`
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
func buildHTTPEndpointRequest(
	ctx context.Context,
	endpointURL, accessKey string,
	dest *HTTPEndpointDestinationDescription,
	body []byte,
) (*http.Request, error) {
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
		logger.Load(ctx).
			WarnContext(ctx, "firehose: failed to marshal HTTP endpoint payload", "error", err, "stream", streamARN)

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
			logger.Load(ctx).
				WarnContext(ctx, "firehose: failed to build HTTP endpoint request", "error", reqErr, "stream", streamARN)

			return
		}

		resp, doErr := client.Do(req)
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: HTTP endpoint delivery failed after retries",
				"url", endpointURL, "stream", streamARN)

			return
		}

		if !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return
		}
	}
}

const httpBackoffMaxInterval = 30 * time.Second

// httpDeliveryBackoff waits for the next retry interval or returns false if the context
// is cancelled. backoff is doubled on each call and capped at httpBackoffMaxInterval.
// Returns true when the caller should proceed with the next attempt, false when it should stop.
func httpDeliveryBackoff(ctx context.Context, deadline time.Time, backoff *time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(*backoff):
		*backoff *= 2
		if *backoff > httpBackoffMaxInterval {
			*backoff = httpBackoffMaxInterval
		}

		return time.Now().Before(deadline)
	}
}

// checkHTTPDeliveryResponse closes the response body and returns true when the
// response indicates success (2xx status). When doErr is non-nil or the status
// is outside 2xx it returns false so the caller retries.
func checkHTTPDeliveryResponse(ctx context.Context, resp *http.Response, doErr error) bool {
	if doErr != nil {
		return false
	}
	if closeErr := resp.Body.Close(); closeErr != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: failed to close HTTP response body", "error", closeErr)
	}

	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// redshiftRetryDuration is the default retry window for Redshift delivery.
const redshiftRetryDuration = 7200 * time.Second

// redshiftHostParts is the SplitN limit for extracting cluster ID from JDBC host.
const redshiftHostParts = 2

const (
	redshiftBackoffInitial = 2 * time.Second
	redshiftBackoffMax     = 60 * time.Second
)

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
// parseRedshiftJDBCURL extracts the cluster identifier and database name from a
// Redshift JDBC connection string of the form
// jdbc:redshift://<cluster>.<suffix>.redshift.amazonaws.com:<port>/<database>.
// Returns an error when the URL cannot be parsed or is missing a cluster or database.
func parseRedshiftJDBCURL(clusterJDBCURL string) (string, string, error) {
	jdbcURL := strings.TrimPrefix(clusterJDBCURL, "jdbc:redshift://")
	parsed, parseErr := url.Parse("https://" + jdbcURL)
	if parseErr != nil {
		return "", "", parseErr
	}

	host := parsed.Hostname()
	database := strings.TrimPrefix(parsed.Path, "/")

	// Extract cluster identifier from the host: <cluster>.<suffix>.redshift.amazonaws.com
	clusterID := strings.SplitN(host, ".", redshiftHostParts)[0]

	if clusterID == "" || database == "" {
		return "", "", fmt.Errorf("%w: JDBC URL missing cluster or database", ErrValidation)
	}

	return clusterID, database, nil
}

// executeRedshiftInsertWithRetry runs insertSQL via the Redshift Data API, retrying
// with exponential back-off until maxRetry elapses or ctx is cancelled.
func (b *InMemoryBackend) executeRedshiftInsertWithRetry(
	ctx context.Context,
	clusterID, database, dbUser, insertSQL, streamARN string,
	maxRetry time.Duration,
) {
	rdClient := sdk_rddata.NewFromConfig(aws.Config{Region: b.region})

	deadline := time.Now().Add(maxRetry)
	backoff := redshiftBackoffInitial

	for {
		_, execErr := rdClient.ExecuteStatement(ctx, &sdk_rddata.ExecuteStatementInput{
			ClusterIdentifier: aws.String(clusterID),
			Database:          aws.String(database),
			DbUser:            aws.String(dbUser),
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
			if backoff > redshiftBackoffMax {
				backoff = redshiftBackoffMax
			}
		}
	}
}

func (b *InMemoryBackend) deliverToRedshift(
	ctx context.Context,
	records [][]byte,
	dest *RedshiftDestinationDescription,
	streamARN string,
) {
	if dest.ClusterJDBCURL == "" || dest.CopyCommand == nil || dest.CopyCommand.DataTableName == "" {
		return
	}

	clusterID, database, parseErr := parseRedshiftJDBCURL(dest.ClusterJDBCURL)
	if parseErr != nil {
		logger.Load(ctx).WarnContext(ctx, "firehose: cannot parse Redshift JDBC URL",
			"url", dest.ClusterJDBCURL, "stream", streamARN, "error", parseErr)

		return
	}

	insertSQL, ok := buildRedshiftInsertSQL(dest.CopyCommand.DataTableName, dest.CopyCommand.DataTableColumns, records)
	if !ok {
		return
	}

	maxRetry := redshiftRetryDuration
	if dest.RetryOptions != nil && dest.RetryOptions.DurationInSeconds > 0 {
		maxRetry = time.Duration(dest.RetryOptions.DurationInSeconds) * time.Second
	}

	b.executeRedshiftInsertWithRetry(ctx, clusterID, database, dest.Username, insertSQL, streamARN, maxRetry)
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
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: OpenSearch delivery failed after retries",
				"url", bulkURL, "stream", streamARN)

			return
		}

		if !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return
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
			logger.Load(ctx).
				WarnContext(ctx, "firehose: failed to build Splunk HEC request", "error", reqErr, "stream", streamARN)

			return
		}

		req.Header.Set("Content-Type", contentType)
		if dest.HECToken != "" {
			req.Header.Set("Authorization", "Splunk "+dest.HECToken)
		}

		resp, doErr := client.Do(req)
		if checkHTTPDeliveryResponse(ctx, resp, doErr) {
			return
		}

		if time.Now().After(deadline) {
			logger.Load(ctx).WarnContext(ctx, "firehose: Splunk HEC delivery failed after retries",
				"url", hecURL, "stream", streamARN)

			return
		}

		if !httpDeliveryBackoff(ctx, deadline, &backoff) {
			return
		}
	}
}
