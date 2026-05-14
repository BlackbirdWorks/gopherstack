package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
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

// S3Storer is the subset of S3 operations that Firehose needs to deliver objects.
type S3Storer interface {
	PutObject(ctx context.Context, input *sdk_s3.PutObjectInput) (*sdk_s3.PutObjectOutput, error)
}

// LambdaInvoker is the subset of Lambda operations that Firehose needs for transformation.
type LambdaInvoker interface {
	InvokeFunction(ctx context.Context, name string, invocationType string, payload []byte) ([]byte, int, error)
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

// EncryptionConfigInput holds the optional SSE configuration for a delivery stream.
type EncryptionConfigInput struct {
	KeyARN  string `json:"KeyARN,omitempty"`
	KeyType string `json:"KeyType"`
}

// EncryptionConfig holds the effective SSE configuration for a delivery stream.
type EncryptionConfig struct {
	KeyARN  string `json:"KeyARN,omitempty"`
	KeyType string `json:"KeyType"`
	Status  string `json:"Status"`
}

// S3DestinationDescription holds the effective S3 destination config stored on the stream.
type S3DestinationDescription struct {
	BufferingHints          *BufferingHints          `json:"BufferingHints,omitempty"`
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration,omitempty"`
	S3BackupDescription     *S3BackupDescription     `json:"S3BackupDescription,omitempty"`
	BucketARN               string                   `json:"BucketARN,omitempty"`
	RoleARN                 string                   `json:"RoleARN,omitempty"`
	Prefix                  string                   `json:"Prefix,omitempty"`
	ErrorOutputPrefix       string                   `json:"ErrorOutputPrefix,omitempty"`
	CompressionFormat       string                   `json:"CompressionFormat,omitempty"`
	DestinationID           string                   `json:"DestinationId,omitempty"`
	S3BackupMode            string                   `json:"S3BackupMode,omitempty"`
}

// S3BackupDescription holds the S3 backup destination configuration.
type S3BackupDescription struct {
	BufferingHints    *BufferingHints `json:"BufferingHints,omitempty"`
	BucketARN         string          `json:"BucketARN,omitempty"`
	RoleARN           string          `json:"RoleARN,omitempty"`
	Prefix            string          `json:"Prefix,omitempty"`
	CompressionFormat string          `json:"CompressionFormat,omitempty"`
}

// HTTPEndpointDestinationDescription holds the HTTP endpoint destination config.
type HTTPEndpointDestinationDescription struct {
	ProcessingConfiguration *ProcessingConfiguration   `json:"ProcessingConfiguration,omitempty"`
	EndpointConfiguration   *HTTPEndpointConfiguration `json:"EndpointConfiguration,omitempty"`
	S3BackupMode            string                     `json:"S3BackupMode,omitempty"`
	S3BackupDescription     *S3BackupDescription       `json:"S3BackupDescription,omitempty"`
	DestinationID           string                     `json:"DestinationId,omitempty"`
}

// HTTPEndpointConfiguration holds the HTTP endpoint URL and name.
type HTTPEndpointConfiguration struct {
	URL       string `json:"Url,omitempty"`
	Name      string `json:"Name,omitempty"`
	AccessKey string `json:"AccessKey,omitempty"`
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
	Tags                    *tags.Tags                          `json:"tags,omitempty"`
	S3Destination           *S3DestinationDescription           `json:"s3Destination,omitempty"`
	HTTPEndpointDestination *HTTPEndpointDestinationDescription `json:"httpEndpointDestination,omitempty"`
	Encryption              *EncryptionConfig                   `json:"encryption,omitempty"`
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
	s3      S3Storer
	lambda  LambdaInvoker
	streams map[string]*DeliveryStream
	mu      *lockmetrics.RWMutex
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
		streams:   make(map[string]*DeliveryStream),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("firehose"),
		svcCtx:    svcCtx,
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

// CreateDeliveryStreamInput holds the input for creating a delivery stream.
type CreateDeliveryStreamInput struct {
	S3Destination           *S3DestinationDescription
	HTTPEndpointDestination *HTTPEndpointDestinationDescription
	Name                    string
}

// CreateDeliveryStream creates a new delivery stream.
func (b *InMemoryBackend) CreateDeliveryStream(input CreateDeliveryStreamInput) (*DeliveryStream, error) {
	if strings.TrimSpace(input.Name) == "" {
		return nil, fmt.Errorf("%w: DeliveryStreamName is required", ErrValidation)
	}

	b.mu.Lock("CreateDeliveryStream")
	defer b.mu.Unlock()

	if _, ok := b.streams[input.Name]; ok {
		return nil, fmt.Errorf("%w: stream %s already exists", ErrAlreadyExists, input.Name)
	}

	if input.S3Destination != nil && input.S3Destination.DestinationID == "" {
		input.S3Destination.DestinationID = "destinationId-000000000001"
	}

	streamARN := arn.Build("firehose", b.region, b.accountID, "deliverystream/"+input.Name)
	s := &DeliveryStream{
		Name:                    input.Name,
		ARN:                     streamARN,
		DeliveryStreamType:      "DirectPut",
		VersionID:               "1",
		Status:                  "ACTIVE",
		Records:                 [][]byte{},
		BackupRecords:           [][]byte{},
		Tags:                    tags.New("firehose." + input.Name + ".tags"),
		AccountID:               b.accountID,
		Region:                  b.region,
		S3Destination:           input.S3Destination,
		HTTPEndpointDestination: input.HTTPEndpointDestination,
		lastFlush:               time.Now(),
	}
	b.streams[input.Name] = s

	return streamCopy(s), nil
}

// DeleteDeliveryStream deletes a delivery stream.
func (b *InMemoryBackend) DeleteDeliveryStream(name string) error {
	b.mu.Lock("DeleteDeliveryStream")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	if s.Tags != nil {
		s.Tags.Close()
	}

	delete(b.streams, name)

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

	v, err := strconv.Atoi(s.VersionID)
	if err != nil {
		slog.Default().Warn("firehose: unexpected non-integer VersionID; resetting to 1",
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

// shouldFlushLocked returns true when a size-based flush should happen.
// Must be called with the write lock held.
func (b *InMemoryBackend) shouldFlushLocked(s *DeliveryStream) bool {
	if len(s.Records) == 0 || s.S3Destination == nil || b.s3 == nil {
		return false
	}

	if s.S3Destination.BufferingHints == nil {
		// Default: flush at 5 MB.
		return s.bufferSizeBytes >= 5*1024*1024
	}

	sizeLimit := s.S3Destination.BufferingHints.SizeInMBs
	if sizeLimit <= 0 {
		sizeLimit = 5
	}

	return s.bufferSizeBytes >= sizeLimit*1024*1024
}

// shouldFlushByIntervalLocked returns true when an interval-based flush should happen.
// Must be called with the read lock held.
func (b *InMemoryBackend) shouldFlushByIntervalLocked(s *DeliveryStream) bool {
	if len(s.Records) == 0 || s.S3Destination == nil || b.s3 == nil {
		return false
	}

	interval := 300 // default 300 seconds
	if s.S3Destination.BufferingHints != nil && s.S3Destination.BufferingHints.IntervalInSeconds > 0 {
		interval = s.S3Destination.BufferingHints.IntervalInSeconds
	}

	return time.Since(s.lastFlush) >= time.Duration(interval)*time.Second
}

// flushSnapshot holds a point-in-time snapshot of records extracted from a stream.
type flushSnapshot struct {
	dest      S3DestinationDescription
	streamARN string
	region    string
	records   [][]byte
}

// extractForFlushLocked snapshots and resets the stream buffer when shouldFlushLocked
// returns true. Returns nil when no flush is needed. Must be called with the write lock held.
func (b *InMemoryBackend) extractForFlushLocked(s *DeliveryStream) *flushSnapshot {
	if !b.shouldFlushLocked(s) {
		return nil
	}

	return b.extractAllRecordsLocked(s)
}

// extractAllRecordsLocked unconditionally snapshots and resets the stream buffer.
// Returns nil when there are no records to flush. Must be called with the write lock held.
func (b *InMemoryBackend) extractAllRecordsLocked(s *DeliveryStream) *flushSnapshot {
	if len(s.Records) == 0 || s.S3Destination == nil || b.s3 == nil {
		return nil
	}

	snap := &flushSnapshot{
		records:   s.Records,
		dest:      *s.S3Destination,
		streamARN: s.ARN,
		region:    s.Region,
	}
	s.Records = [][]byte{}
	s.bufferSizeBytes = 0
	s.lastFlush = time.Now()

	return snap
}

// deliverSnapshot applies optional Lambda transformation and delivers records to S3.
// Called after the write lock has been released.
func (b *InMemoryBackend) deliverSnapshot(ctx context.Context, snap *flushSnapshot, streamName string) {
	records := snap.records

	if snap.dest.ProcessingConfiguration != nil && snap.dest.ProcessingConfiguration.Enabled {
		var err error

		records, err = b.transformRecords(ctx, records, &snap.dest, snap.streamARN, snap.region)
		if err != nil {
			return
		}
	}

	if len(records) == 0 {
		return
	}

	_ = b.deliverToS3(ctx, records, &snap.dest, streamName)
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

// buildS3Key constructs an S3 object key with timestamp-partitioned prefix.
func buildS3Key(prefix, streamName string, t time.Time) string {
	ts := t.UTC().Format("2006/01/02/15/")
	filename := fmt.Sprintf("%s-%s", streamName, t.UTC().Format("2006-01-02-15-04-05"))

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
	b.mu.Lock("StartDeliveryStreamEncryption")
	defer b.mu.Unlock()

	s, ok := b.streams[name]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	cfg := &EncryptionConfig{Status: "ENABLED", KeyType: "AWS_OWNED_CMK"}
	if input != nil {
		if input.KeyType != "" {
			cfg.KeyType = input.KeyType
		}
		cfg.KeyARN = input.KeyARN
	}

	s.Encryption = cfg

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

	if s.Encryption != nil {
		enc := *s.Encryption
		cp.Encryption = &enc
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
		slog.Default().Warn("firehose: rand.Read failed; falling back to timestamp-based record ID", "error", err)

		return fmt.Sprintf("rec-%d", time.Now().UnixNano())
	}

	return hex.EncodeToString(b)
}
