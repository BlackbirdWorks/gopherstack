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
	"github.com/google/uuid"
)

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
		if _, putErr := b.writeRecordsToBucket(
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

	_, _ = b.writeRecordsToBucket(ctx, records, bucketARN, prefix, "", dest.CompressionFormat, streamName)
}

// writeRecordsToBucket concatenates records (newline-separated), optionally gzip-compresses
// them, and writes a single object under the given bucket/prefix. fileExtension, when set,
// is appended to the generated object key. It is a no-op when the effective body is empty.
// Returns the object key that was written, or "" when nothing was written.
func (b *InMemoryBackend) writeRecordsToBucket(
	ctx context.Context,
	records [][]byte,
	bucketARN, prefix, fileExtension, compressionFormat, streamName string,
) (string, error) {
	if b.s3 == nil {
		return "", nil
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
		return "", nil
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
			return "", gzErr
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

	if _, err := b.s3.PutObject(ctx, input); err != nil {
		return "", err
	}

	return key, nil
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
