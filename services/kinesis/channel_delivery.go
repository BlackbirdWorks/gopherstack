package kinesis

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/google/uuid"
	"github.com/klauspost/compress/zstd"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// channelDeliveryTickInterval is how often runChannelFlusher checks for
// channels whose DataFreshnessInSeconds window has elapsed. Mirrors
// firehose's intervalFlusher (services/firehose/flush.go), which also has no
// injectable clock and polls on a real 1-second ticker.
const channelDeliveryTickInterval = time.Second

// defaultChannelKeyTemplate is S3 delivery's documented default object key
// template when S3StorageConfiguration.OutputKeyTemplate is unset:
// docs.aws.amazon.com/streams/latest/dev/data-delivery-s3-key-template.html
// ("Default template").
const defaultChannelKeyTemplate = "kinesis-channel/!{channel-name}/!{channel-id}/!{yyyy}/!{MM}/!{dd}/!{HH}/" +
	"!{channel-name}-!{channel-id}-!{yyyy}-!{MM}-!{dd}-!{HH}-!{mm}!{extension}"

// defaultChannelErrorPrefix is used for the dead-letter queue destination
// when a channel's DeadLetterQueueS3Configuration is unset. AWS documents
// only the behavior ("defaults to the destination bucket with an error
// prefix", data-delivery-s3-about.html) and not the literal prefix string;
// this value is an inference, not a verified fact.
const defaultChannelErrorPrefix = "kinesis-channel-errors/"

// channelKeyPlaceholderRe matches an S3 output key template placeholder:
// !{name} or !{name:.literal-extension}, per the documented template syntax.
var channelKeyPlaceholderRe = regexp.MustCompile(`!\{([a-zA-Z-]+)(?::(\.[a-z.]+))?\}`)

// channelBuffer holds records buffered for one channel's S3 destination,
// pending an interval flush. Guarded entirely by InMemoryBackend.deliveryMu.
type channelBuffer struct {
	lastFlush time.Time
	records   [][]byte
	failed    []failedChannelRecord
}

// failedChannelRecord is a record that failed S3 delivery's documented
// per-record validation (data-delivery-s3-about.html: "Validate" step),
// carried with enough context to populate a dead-letter queue entry.
type failedChannelRecord struct {
	streamARN      string
	shardID        string
	sequenceNumber string
	errorMessage   string
}

// deliverPutToChannels appends data to the buffer of every ACTIVE channel
// sourced from region/streamName. Must be called with no backend lock held
// (see PutRecord). A no-op when the channel has no such source (the common
// case: most streams have no channel at all).
func (b *InMemoryBackend) deliverPutToChannels(
	region, streamName, streamARN, shardID, sequenceNumber string, data []byte,
) {
	b.mu.RLock("deliverPutToChannels")
	channels := b.channelsByRegion.Get(region)
	matched := make([]*Channel, 0, len(channels))

	for _, c := range channels {
		if c.ChannelStatus != channelStatusActive || c.S3DestinationConfiguration == nil {
			continue
		}
		if len(c.StreamConfigurationList) == 0 ||
			streamNameFromARN(c.StreamConfigurationList[0].StreamARN) != streamName {
			continue
		}
		matched = append(matched, c)
	}
	b.mu.RUnlock()

	for _, c := range matched {
		b.appendToChannelBuffer(c, streamARN, shardID, sequenceNumber, data)
	}
}

// channelARNsForStream returns the ARNs of every channel sourced from
// region/streamName, used by DeleteStream to flush their buffers after the
// stream is removed (see streams.go).
func (b *InMemoryBackend) channelARNsForStream(region, streamName string) []string {
	b.mu.RLock("channelARNsForStream")
	defer b.mu.RUnlock()

	var arns []string

	for _, c := range b.channelsByRegion.Get(region) {
		if len(c.StreamConfigurationList) > 0 &&
			streamNameFromARN(c.StreamConfigurationList[0].StreamARN) == streamName {
			arns = append(arns, c.ChannelARN)
		}
	}

	return arns
}

// appendToChannelBuffer validates data against c's RecordFormatType and
// appends it to c's buffer (main or dead-letter, per validation outcome).
func (b *InMemoryBackend) appendToChannelBuffer(
	c *Channel,
	streamARN, shardID, sequenceNumber string,
	data []byte,
) {
	formatType := ""
	if len(c.StreamConfigurationList) > 0 {
		formatType = c.StreamConfigurationList[0].RecordConfiguration.RecordFormatType
	}

	b.deliveryMu.Lock("appendToChannelBuffer")
	defer b.deliveryMu.Unlock()

	buf, ok := b.channelBuffers[c.ChannelARN]
	if !ok {
		buf = &channelBuffer{lastFlush: time.Now()}
		b.channelBuffers[c.ChannelARN] = buf
	}

	if errMsg := validateChannelRecord(formatType, data); errMsg != "" {
		buf.failed = append(buf.failed, failedChannelRecord{
			streamARN:      streamARN,
			shardID:        shardID,
			sequenceNumber: sequenceNumber,
			errorMessage:   errMsg,
		})

		return
	}

	buf.records = append(buf.records, data)
}

// validateChannelRecord applies S3 delivery's documented per-record
// validation (data-delivery-s3-about.html: "when the record format is
// STRING, each record is validated as a valid UTF-8 string; when the record
// format is JSON, each record is validated as a valid JSON payload"),
// returning a non-empty error message when data fails it. BYTE_ARRAY and
// GSR_JSON (the latter is documented as unsupported for general purpose S3
// delivery, not modeled here) are not validated.
func validateChannelRecord(formatType string, data []byte) string {
	switch formatType {
	case recordFormatTypeString:
		if !utf8.Valid(data) {
			return "record is not valid UTF-8 for RecordFormatType STRING"
		}
	case recordFormatTypeJSON:
		if !json.Valid(data) {
			return "record is not valid JSON for RecordFormatType JSON"
		}
	}

	return ""
}

// channelFlushDue reports whether buf's DataFreshnessInSeconds window has
// elapsed. Must be called with deliveryMu held.
func channelFlushDue(buf *channelBuffer, freshnessSeconds int) bool {
	if len(buf.records) == 0 && len(buf.failed) == 0 {
		return false
	}

	interval := time.Duration(freshnessSeconds) * time.Second
	if freshnessSeconds <= 0 {
		interval = defaultChannelDataFreshnessSeconds * time.Second
	}

	return time.Since(buf.lastFlush) >= interval
}

// channelFlushSnapshot is a point-in-time extraction of one channel's
// buffered records, taken outside b.mu/stream.mu so the S3 write that
// follows never runs with a backend lock held (services/lambda/lifecycle.go
// documents this capture/release/write pattern).
type channelFlushSnapshot struct {
	channel *Channel
	stream  string
	records [][]byte
	failed  []failedChannelRecord
}

// extractChannelBufferLocked snapshots and clears ch's buffer. Must be
// called with deliveryMu held. Returns nil when there is nothing to flush.
func extractChannelBufferLocked(
	buffers map[string]*channelBuffer,
	ch *Channel,
) *channelFlushSnapshot {
	buf, ok := buffers[ch.ChannelARN]
	if !ok || (len(buf.records) == 0 && len(buf.failed) == 0) {
		return nil
	}

	streamName := ""
	if len(ch.StreamConfigurationList) > 0 {
		streamName = streamNameFromARN(ch.StreamConfigurationList[0].StreamARN)
	}

	snap := &channelFlushSnapshot{
		channel: ch,
		stream:  streamName,
		records: buf.records,
		failed:  buf.failed,
	}
	buf.records = nil
	buf.failed = nil
	buf.lastFlush = time.Now()

	return snap
}

// FlushChannel forces immediate delivery of channelARN's buffered records,
// regardless of its DataFreshnessInSeconds window. Used by DeleteChannel,
// graceful shutdown, and tests (this backend has no injectable clock for
// interval-based flush -- see PARITY.md).
func (b *InMemoryBackend) FlushChannel(ctx context.Context, channelARN string) {
	b.mu.RLock("FlushChannel")
	ch, ok := b.channels.Get(channelARN)
	b.mu.RUnlock()

	if !ok {
		return
	}

	b.flushChannel(ctx, ch)
}

// FlushAllChannels forces immediate delivery of every channel's buffered
// records. Used by Handler.Shutdown and tests.
func (b *InMemoryBackend) FlushAllChannels(ctx context.Context) {
	b.mu.RLock("FlushAllChannels")
	all := b.channels.All()
	channels := make([]*Channel, len(all))
	copy(channels, all)
	b.mu.RUnlock()

	for _, ch := range channels {
		b.flushChannel(ctx, ch)
	}
}

// flushChannel extracts ch's buffer (if any) and delivers it to S3.
func (b *InMemoryBackend) flushChannel(ctx context.Context, ch *Channel) {
	b.deliveryMu.Lock("flushChannel")
	snap := extractChannelBufferLocked(b.channelBuffers, ch)
	b.deliveryMu.Unlock()

	if snap == nil {
		return
	}

	b.deliverChannelSnapshot(ctx, snap)
}

// runChannelFlusher polls every channelDeliveryTickInterval for channels
// whose DataFreshnessInSeconds window has elapsed and flushes them. Started
// by Handler.StartWorker; exits when ctx is cancelled.
func (b *InMemoryBackend) runChannelFlusher(ctx context.Context) {
	ticker := time.NewTicker(channelDeliveryTickInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			for _, ch := range b.dueChannels() {
				b.flushChannel(ctx, ch)
			}
		}
	}
}

// dueChannels returns the channels with an S3 destination whose buffer's
// DataFreshnessInSeconds window has elapsed.
func (b *InMemoryBackend) dueChannels() []*Channel {
	b.mu.RLock("dueChannels")
	all := b.channels.All()
	channels := make([]*Channel, len(all))
	copy(channels, all)
	b.mu.RUnlock()

	b.deliveryMu.RLock("dueChannels.delivery")
	defer b.deliveryMu.RUnlock()

	due := make([]*Channel, 0, len(channels))

	for _, ch := range channels {
		if ch.S3DestinationConfiguration == nil {
			continue
		}
		buf, ok := b.channelBuffers[ch.ChannelARN]
		if !ok {
			continue
		}
		if channelFlushDue(buf, ch.S3DestinationConfiguration.DataFreshnessInSeconds) {
			due = append(due, ch)
		}
	}

	return due
}

// deliverChannelSnapshot writes snap's main and dead-letter records to S3.
// Called with no backend lock held.
func (b *InMemoryBackend) deliverChannelSnapshot(ctx context.Context, snap *channelFlushSnapshot) {
	if b.s3Writer == nil {
		return
	}

	dest := snap.channel.S3DestinationConfiguration

	if len(snap.records) > 0 {
		if err := b.writeChannelObject(ctx, snap.channel, snap.stream, dest, snap.records); err != nil {
			logger.Load(ctx).WarnContext(ctx, "kinesis: channel S3 delivery failed",
				"channel", snap.channel.ChannelARN, "error", err)
		}
	}

	if len(snap.failed) > 0 {
		b.writeChannelDeadLetterQueue(ctx, snap.channel, dest, snap.failed)
	}
}

// writeChannelObject compresses and writes records as a single S3 object
// under dest's bucket, using dest's OutputKeyTemplate (or the documented
// default). Per data-delivery.html ("Records are delivered in their
// original source format with no transformation applied"), records are
// concatenated with no delimiter inserted between them -- disclosed as an
// inference: the page does not state the exact concatenation byte layout,
// only that no transformation is applied.
func (b *InMemoryBackend) writeChannelObject(
	ctx context.Context,
	ch *Channel,
	streamName string,
	dest *ChannelS3Destination,
	records [][]byte,
) error {
	var buf bytes.Buffer
	for _, r := range records {
		buf.Write(r)
	}

	body, contentEncoding, err := compressChannelBody(
		buf.Bytes(),
		dest.StorageConfiguration.CompressionType,
	)
	if err != nil {
		return err
	}

	key := buildChannelObjectKey(
		dest.StorageConfiguration.OutputKeyTemplate, ch, streamName,
		dest.StorageConfiguration.CompressionType, time.Now(),
	)

	return b.putChannelObject(ctx, dest.StorageConfiguration.BucketARN, key, body, contentEncoding,
		dest.StorageConfiguration.StorageClass)
}

// writeChannelDeadLetterQueue writes failed records' error metadata to the
// channel's DeadLetterQueueS3Configuration bucket (or, when unset, the main
// destination bucket under defaultChannelErrorPrefix -- see
// data-delivery-s3-about.html's "Dead-letter queue" section). The exact
// dead-letter object schema is not documented beyond "stream ARN, shard ID,
// sequence number, and error context" (see channel_delivery.go's package
// doc / PARITY.md); this uses newline-delimited JSON of that same field
// set, disclosed as an inference rather than a verified wire format.
func (b *InMemoryBackend) writeChannelDeadLetterQueue(
	ctx context.Context, ch *Channel, dest *ChannelS3Destination, failed []failedChannelRecord,
) {
	bucketARN := dest.StorageConfiguration.BucketARN
	prefix := defaultChannelErrorPrefix

	if dlq := dest.DeadLetterQueueS3Configuration; dlq != nil {
		bucketARN = dlq.BucketARN
		if dlq.ErrorOutputPrefix != "" {
			prefix = dlq.ErrorOutputPrefix
		}
	}

	var buf bytes.Buffer
	for _, f := range failed {
		line, err := json.Marshal(struct {
			StreamARN      string `json:"streamARN"`
			ShardID        string `json:"shardID"`
			SequenceNumber string `json:"sequenceNumber"`
			ErrorMessage   string `json:"errorMessage"`
		}{f.streamARN, f.shardID, f.sequenceNumber, f.errorMessage})
		if err != nil {
			continue
		}
		buf.Write(line)
		buf.WriteByte('\n')
	}

	key := prefix + buildChannelObjectKey("", ch, "", "", time.Now())

	if err := b.putChannelObject(ctx, bucketARN, key, buf.Bytes(), nil, ""); err != nil {
		logger.Load(ctx).WarnContext(ctx, "kinesis: channel dead-letter queue delivery failed",
			"channel", ch.ChannelARN, "error", err)
	}
}

// putChannelObject issues the PutObject call for a channel-delivered object.
func (b *InMemoryBackend) putChannelObject(
	ctx context.Context,
	bucketARN, key string,
	body []byte,
	contentEncoding *string,
	storageClass string,
) error {
	input := &sdk_s3.PutObjectInput{
		Bucket:          aws.String(channelBucketFromARN(bucketARN)),
		Key:             aws.String(key),
		Body:            io.NopCloser(bytes.NewReader(body)),
		ContentLength:   aws.Int64(int64(len(body))),
		ContentEncoding: contentEncoding,
	}
	if storageClass != "" {
		input.StorageClass = s3types.StorageClass(storageClass)
	}

	_, err := b.s3Writer.PutObject(ctx, input)

	return err
}

// compressChannelBody compresses body per compressionType (S3CompressionType:
// NONE/GZIP/ZSTD -- kinesis@v1.53.0 types/enums.go:219-226), returning the
// Content-Encoding header value to send alongside it (nil for NONE).
func compressChannelBody(body []byte, compressionType string) ([]byte, *string, error) {
	switch compressionType {
	case channelCompressionNone:
		return body, nil, nil
	case channelCompressionGzip:
		var buf bytes.Buffer
		w := gzip.NewWriter(&buf)
		if _, err := w.Write(body); err != nil {
			return nil, nil, err
		}
		if err := w.Close(); err != nil {
			return nil, nil, err
		}

		return buf.Bytes(), aws.String("gzip"), nil
	case channelCompressionZstd:
		compressed, err := zstdCompress(body)
		if err != nil {
			return nil, nil, err
		}

		return compressed, aws.String("zstd"), nil
	default:
		return body, nil, nil
	}
}

// zstdCompress compresses data using Zstandard (S3CompressionType ZSTD).
func zstdCompress(data []byte) ([]byte, error) {
	var buf bytes.Buffer

	w, err := zstd.NewWriter(&buf)
	if err != nil {
		return nil, err
	}

	if _, writeErr := w.Write(data); writeErr != nil {
		_ = w.Close()

		return nil, writeErr
	}

	if closeErr := w.Close(); closeErr != nil {
		return nil, closeErr
	}

	return buf.Bytes(), nil
}

// channelObjectExtension returns the S3 key extension S3 delivery derives
// from compressionType (docs.aws.amazon.com/streams/latest/dev/
// data-delivery-s3-key-template.html: "!{extension} ... derived from the
// configured compression type (for example, .gz for GZIP or .zst for
// ZSTD)"). NONE has no documented extension.
func channelObjectExtension(compressionType string) string {
	switch compressionType {
	case channelCompressionGzip:
		return ".gz"
	case channelCompressionZstd:
		return ".zst"
	default:
		return ""
	}
}

// buildChannelObjectKey expands tmpl's documented variables against ch,
// streamName, and t, then appends the unique suffix S3 delivery documents
// as always applied ("Amazon Kinesis Data Streams automatically appends a
// unique suffix to every object key" -- data-delivery-s3-key-template.html).
// That page does not state the suffix's exact insertion point; this places
// it immediately before the file extension (if any), the same position
// Firehose's own delivered-object naming uses for its uniqueness token
// (services/firehose/delivery_s3.go buildS3Key) -- disclosed as an
// inference, not a verified fact. An empty tmpl uses defaultChannelKeyTemplate.
func buildChannelObjectKey(
	tmpl string,
	ch *Channel,
	streamName, compressionType string,
	t time.Time,
) string {
	if tmpl == "" {
		tmpl = defaultChannelKeyTemplate
	}

	ext := channelObjectExtension(compressionType)
	expand := func(s string) string {
		return channelKeyPlaceholderRe.ReplaceAllStringFunc(s, func(m string) string {
			groups := channelKeyPlaceholderRe.FindStringSubmatch(m)

			return channelKeyVariable(groups[1], groups[2], ch, streamName, t, ext)
		})
	}

	const uniqueSuffixLen = 12
	suffix := "-" + uuid.NewString()[:uniqueSuffixLen]

	if idx := strings.LastIndex(tmpl, "!{extension"); idx >= 0 {
		return expand(tmpl[:idx]) + suffix + expand(tmpl[idx:])
	}

	return expand(tmpl) + suffix
}

// channelKeyVariable resolves one S3 output key template variable
// (data-delivery-s3-key-template.html's "Template variables" table).
func channelKeyVariable(
	name, literal string,
	ch *Channel,
	streamName string,
	t time.Time,
	ext string,
) string {
	switch name {
	case "channel-name":
		return ch.ChannelName
	case "channel-id":
		return ch.ChannelID
	case "stream-name":
		return streamName
	case "yyyy":
		return t.UTC().Format("2006")
	case "yy":
		return t.UTC().Format("06")
	case "MM":
		return t.UTC().Format("01")
	case "dd":
		return t.UTC().Format("02")
	case "HH":
		return t.UTC().Format("15")
	case "mm":
		return t.UTC().Format("04")
	case "extension":
		if literal != "" {
			return literal
		}

		return ext
	default:
		return fmt.Sprintf("!{%s}", name)
	}
}

// channelBucketFromARN extracts the bucket name from an S3 ARN
// (arn:aws:s3:::bucket-name). Mirrors firehose's bucketFromARN
// (services/firehose/delivery_s3.go).
func channelBucketFromARN(bucketARN string) string {
	const tripleColonParts = 2

	parts := strings.Split(bucketARN, ":::")
	if len(parts) == tripleColonParts {
		return parts[1]
	}

	segments := strings.Split(bucketARN, ":")
	if len(segments) > 0 {
		return segments[len(segments)-1]
	}

	return bucketARN
}
