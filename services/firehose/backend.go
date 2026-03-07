package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
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
)

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

// S3DestinationDescription holds the effective S3 destination config stored on the stream.
type S3DestinationDescription struct {
	BufferingHints          *BufferingHints          `json:"BufferingHints,omitempty"`
	ProcessingConfiguration *ProcessingConfiguration `json:"ProcessingConfiguration,omitempty"`
	BucketARN               string                   `json:"BucketARN,omitempty"`
	RoleARN                 string                   `json:"RoleARN,omitempty"`
	Prefix                  string                   `json:"Prefix,omitempty"`
	ErrorOutputPrefix       string                   `json:"ErrorOutputPrefix,omitempty"`
	CompressionFormat       string                   `json:"CompressionFormat,omitempty"`
}

// DeliveryStream represents a Kinesis Firehose delivery stream.
type DeliveryStream struct {
	lastFlush       time.Time
	Tags            *tags.Tags                `json:"tags,omitempty"`
	S3Destination   *S3DestinationDescription `json:"s3Destination,omitempty"`
	Name            string                    `json:"name"`
	ARN             string                    `json:"arn"`
	Status          string                    `json:"status"`
	AccountID       string                    `json:"accountID"`
	Region          string                    `json:"region"`
	Records         [][]byte                  `json:"records,omitempty"`
	bufferSizeBytes int
}

// InMemoryBackend is the in-memory store for Firehose resources.
type InMemoryBackend struct {
	s3        S3Storer
	lambda    LambdaInvoker
	streams   map[string]*DeliveryStream
	mu        *lockmetrics.RWMutex
	accountID string
	region    string
}

// NewInMemoryBackend creates a new InMemoryBackend.
func NewInMemoryBackend(accountID, region string) *InMemoryBackend {
	return &InMemoryBackend{
		streams:   make(map[string]*DeliveryStream),
		accountID: accountID,
		region:    region,
		mu:        lockmetrics.New("firehose"),
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

// CreateDeliveryStreamInput holds the input for creating a delivery stream.
type CreateDeliveryStreamInput struct {
	S3Destination *S3DestinationDescription
	Name          string
}

// CreateDeliveryStream creates a new delivery stream.
func (b *InMemoryBackend) CreateDeliveryStream(input CreateDeliveryStreamInput) (*DeliveryStream, error) {
	b.mu.Lock("CreateDeliveryStream")
	defer b.mu.Unlock()

	if _, ok := b.streams[input.Name]; ok {
		return nil, fmt.Errorf("%w: stream %s already exists", ErrAlreadyExists, input.Name)
	}

	streamARN := arn.Build("firehose", b.region, b.accountID, "deliverystream/"+input.Name)
	s := &DeliveryStream{
		Name:          input.Name,
		ARN:           streamARN,
		Status:        "ACTIVE",
		Records:       [][]byte{},
		Tags:          tags.New("firehose." + input.Name + ".tags"),
		AccountID:     b.accountID,
		Region:        b.region,
		S3Destination: input.S3Destination,
		lastFlush:     time.Now(),
	}
	b.streams[input.Name] = s

	cp := *s

	return &cp, nil
}

// DeleteDeliveryStream deletes a delivery stream.
func (b *InMemoryBackend) DeleteDeliveryStream(name string) error {
	b.mu.Lock("DeleteDeliveryStream")
	defer b.mu.Unlock()

	if _, ok := b.streams[name]; !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
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

	cp := *s

	return &cp, nil
}

// ListDeliveryStreams returns all delivery stream names.
func (b *InMemoryBackend) ListDeliveryStreams() []string {
	b.mu.RLock("ListDeliveryStreams")
	defer b.mu.RUnlock()

	names := make([]string, 0, len(b.streams))
	for name := range b.streams {
		names = append(names, name)
	}

	return names
}

// PutRecord appends a record to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecord(streamName string, data []byte) error {
	b.mu.Lock("PutRecord")

	s, ok := b.streams[streamName]
	if !ok {
		b.mu.Unlock()

		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	s.Records = append(s.Records, data)
	s.bufferSizeBytes += len(data)
	shouldFlush := b.shouldFlushLocked(s)
	b.mu.Unlock()

	if shouldFlush {
		b.flushStream(context.Background(), streamName)
	}

	return nil
}

// PutRecordBatch appends multiple records to the delivery stream and flushes if buffer threshold is met.
func (b *InMemoryBackend) PutRecordBatch(streamName string, records [][]byte) (int, error) {
	b.mu.Lock("PutRecordBatch")

	s, ok := b.streams[streamName]
	if !ok {
		b.mu.Unlock()

		return 0, fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	for _, rec := range records {
		s.Records = append(s.Records, rec)
		s.bufferSizeBytes += len(rec)
	}

	shouldFlush := b.shouldFlushLocked(s)
	b.mu.Unlock()

	if shouldFlush {
		b.flushStream(context.Background(), streamName)
	}

	return 0, nil
}

// UpdateDestination updates the S3 destination configuration of an existing stream.
func (b *InMemoryBackend) UpdateDestination(streamName string, dest *S3DestinationDescription) error {
	b.mu.Lock("UpdateDestination")
	defer b.mu.Unlock()

	s, ok := b.streams[streamName]
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, streamName)
	}

	s.S3Destination = dest

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

// flushStream delivers all buffered records for a stream to S3.
func (b *InMemoryBackend) flushStream(ctx context.Context, streamName string) {
	b.mu.Lock("flushStream")

	s, ok := b.streams[streamName]
	if !ok || len(s.Records) == 0 || s.S3Destination == nil || b.s3 == nil {
		b.mu.Unlock()

		return
	}

	records := s.Records
	s.Records = [][]byte{}
	s.bufferSizeBytes = 0
	s.lastFlush = time.Now()
	dest := *s.S3Destination
	streamARN := s.ARN
	region := s.Region

	b.mu.Unlock()

	// Apply Lambda transformation if configured.
	if dest.ProcessingConfiguration != nil && dest.ProcessingConfiguration.Enabled {
		records = b.transformRecords(ctx, records, &dest, streamARN, region)
	}

	if len(records) == 0 {
		return
	}

	// Concatenate records and deliver to S3.
	_ = b.deliverToS3(ctx, records, &dest, streamName)
}

// transformRecords invokes the configured Lambda function to transform records.
// It returns only the records marked as "Ok" in the Lambda response.
func (b *InMemoryBackend) transformRecords(
	ctx context.Context,
	records [][]byte,
	dest *S3DestinationDescription,
	streamARN, region string,
) [][]byte {
	if b.lambda == nil || dest.ProcessingConfiguration == nil {
		return records
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
		return records
	}

	payload := buildLambdaTransformPayload(records, streamARN, region)

	result, _, err := b.lambda.InvokeFunction(ctx, functionName, "RequestResponse", payload)
	if err != nil {
		// On Lambda failure, return original records.
		return records
	}

	return parseLambdaTransformResponse(result)
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
		buf.Write(rec)
		// Add newline separator if the record doesn't already end with one.
		if len(rec) > 0 && rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	body := buf.Bytes()

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
