package firehose

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"slices"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/roleauth"
)

// s3Failures groups records that could not be delivered, by Firehose error-output type.
type s3Failures struct {
	conversion [][]byte
	partition  [][]byte
	delivery   [][]byte
}

// deliverS3Destination runs the S3 delivery pipeline: Lambda processor, dynamic partitioning,
// optional DataFormatConversion, compression, error-output routing, FailedRecords and backup.
func (b *InMemoryBackend) deliverS3Destination(ctx context.Context, snap *flushSnapshot, streamName string) {
	dest := snap.s3Dest
	ctx = context.WithValue(ctx, timeZoneKey{}, dest.CustomTimeZone)

	if code := b.authorizeS3Write(dest.RoleARN, dest.BucketARN); code != "" {
		b.failDenied(ctx, snap, dest.CloudWatchLoggingOptions, code, len(snap.records))

		return
	}

	out, err := b.runTransform(
		ctx,
		snap.records,
		dest.ProcessingConfiguration,
		dest.RoleARN,
		snap.streamARN,
		snap.region,
	)
	if err != nil {
		b.logDeliveryIssue(ctx, dest.CloudWatchLoggingOptions, streamName,
			"lambda transform invocation failed; routing records to error output", err)
	}

	f := b.deliverToS3(ctx, out, dest, streamName, snap.region)

	processing := slices.Concat(out.Failed, f.partition, f.delivery)
	b.routeToErrorOutput(ctx, processing, dest, streamName, errTypeProcessing)
	b.routeToErrorOutput(ctx, f.conversion, dest, streamName, errTypeConversion)
	b.recordFailedRecords(snap.region, streamName, len(processing)+len(f.conversion))

	b.deliverS3Backup(ctx, snap, dest.S3BackupDescription, streamName)
}

// deliverToS3 partitions, converts, compresses and writes records, returning the failures.
func (b *InMemoryBackend) deliverToS3(
	ctx context.Context,
	out transformOutcome,
	dest *S3DestinationDescription,
	streamName, region string,
) s3Failures {
	var f s3Failures
	if b.s3 == nil || len(out.Ok) == 0 {
		return f
	}

	groups, unpartitioned := resolvePartitions(out.Ok, out.Keys, dest.Prefix,
		dest.DynamicPartitioningConfiguration, metadataExtractionQuery(dest.ProcessingConfiguration))
	f.partition = unpartitioned

	for _, group := range groups {
		recs := group.records

		if dest.DataFormatConversion != nil && dest.DataFormatConversion.Enabled {
			converted, convFailed := convertRecords(dest.DataFormatConversion, recs)
			recs = converted
			f.conversion = append(f.conversion, convFailed...)
		}

		key, size, err := b.putRecordsObject(
			ctx, recs, dest.BucketARN, group.prefix, dest.FileExtension, dest.CompressionFormat, streamName,
		)
		if err != nil {
			b.logDeliveryIssue(ctx, dest.CloudWatchLoggingOptions, streamName, "S3 delivery failed", err)
			f.delivery = append(f.delivery, recs...)
		}

		if key != "" || err != nil {
			b.emitS3Put(region, streamName, len(recs), size, err == nil)
		}
	}

	return f
}

// routeToErrorOutput writes failed records, uncompressed, to the destination bucket under its
// ErrorOutputPrefix (default "<prefix><error-output-type>/").
func (b *InMemoryBackend) routeToErrorOutput(
	ctx context.Context,
	records [][]byte,
	dest *S3DestinationDescription,
	streamName, errType string,
) {
	if b.s3 == nil || len(records) == 0 {
		return
	}

	prefix := errorPrefix(dest.ErrorOutputPrefix, dest.Prefix, errType)
	_, _ = b.writeRecordsToBucket(ctx, records, dest.BucketARN, prefix, "", streamName)
}

// stageToS3 writes records to a destination's S3Configuration staging bucket under its role,
// returning the object key and, when the role is denied or the write fails, the undelivered records.
func (b *InMemoryBackend) stageToS3(
	ctx context.Context,
	records [][]byte,
	cfg *S3DestinationDescription,
	cwLog *CloudWatchLoggingOptions,
	streamName string,
) (string, [][]byte) {
	if code := b.authorizeS3Write(cfg.RoleARN, cfg.BucketARN); code != "" {
		cause := fmt.Errorf("%w: staging role denied", roleauth.ErrAccessDenied)
		b.logDeliveryIssue(ctx, cwLog, streamName, code, cause)

		return "", records
	}

	key, err := b.writeRecordsToBucket(ctx, records, cfg.BucketARN, cfg.Prefix, cfg.CompressionFormat, streamName)
	if err != nil {
		b.logDeliveryIssue(ctx, cwLog, streamName, "S3 staging failed", err)

		return "", records
	}

	return key, nil
}

// writeRecordsToBucket writes newline-joined, compressed records as one object under
// bucket/prefix and returns its key, or "" when the body is empty.
func (b *InMemoryBackend) writeRecordsToBucket(
	ctx context.Context,
	records [][]byte,
	bucketARN, prefix, compressionFormat, streamName string,
) (string, error) {
	key, _, err := b.putRecordsObject(ctx, records, bucketARN, prefix, "", compressionFormat, streamName)

	return key, err
}

// putRecordsObject is writeRecordsToBucket that also returns the stored object's size in bytes.
func (b *InMemoryBackend) putRecordsObject(
	ctx context.Context,
	records [][]byte,
	bucketARN, prefix, fileExtension, compressionFormat, streamName string,
) (string, int, error) {
	if b.s3 == nil {
		return "", 0, nil
	}

	var buf bytes.Buffer
	for _, rec := range records {
		if len(rec) == 0 {
			continue
		}
		buf.Write(rec)
		if rec[len(rec)-1] != '\n' {
			buf.WriteByte('\n')
		}
	}

	if buf.Len() == 0 {
		return "", 0, nil
	}

	cb, err := compressBody(compressionFormat, buf.Bytes())
	if err != nil {
		return "", 0, err
	}

	if fileExtension == "" {
		fileExtension = cb.extension
	}

	key := buildS3Key(prefix, streamName, fileExtension, time.Now().In(zoneFromContext(ctx)))

	input := &sdk_s3.PutObjectInput{
		Bucket:        aws.String(bucketFromARN(bucketARN)),
		Key:           aws.String(key),
		Body:          io.NopCloser(bytes.NewReader(cb.body)),
		ContentLength: aws.Int64(int64(len(cb.body))),
	}
	if cb.gzip {
		input.ContentEncoding = aws.String("gzip")
	}

	if _, err = b.s3.PutObject(ctx, input); err != nil {
		return "", 0, err
	}

	return key, len(cb.body), nil
}

func zoneFromContext(ctx context.Context) *time.Location {
	if name, ok := ctx.Value(timeZoneKey{}).(string); ok && name != "" {
		if loc, err := time.LoadLocation(name); err == nil {
			return loc
		}
	}

	return time.UTC
}

// buildS3Key builds {prefix}{yyyy/MM/dd/HH/ unless the prefix has a timestamp expression}
// {stream}-1-{yyyy-MM-dd-HH-mm-ss}-{uuid}{ext}; t carries the destination time zone.
func buildS3Key(prefix, streamName, fileExtension string, t time.Time) string {
	dir := expandPrefix(prefix, t)

	if !hasTimestampExpression(prefix) {
		if dir != "" && !strings.HasSuffix(dir, "/") {
			dir += "/"
		}

		dir += t.Format("2006/01/02/15/")
	}

	return dir + fmt.Sprintf(
		"%s-1-%s-%s",
		streamName,
		t.UTC().Format("2006-01-02-15-04-05"),
		uuid.NewString(),
	) + fileExtension
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
