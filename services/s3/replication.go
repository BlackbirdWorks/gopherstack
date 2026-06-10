package s3

import (
	"bytes"
	"context"
	"encoding/xml"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// s3ARNParts is the number of colon-separated parts in a full S3 ARN
// (arn:partition:s3:region:account:resource).
const s3ARNParts = 6

// bucketNameFromARN extracts the bucket name from an S3 ARN.
// S3 ARN format: arn:aws:s3:::bucket-name.
func bucketNameFromARN(arn string) string {
	// Strip "arn:aws:s3:::" prefix (or any partition variant)
	const arnPrefix = "arn:"
	if !strings.HasPrefix(arn, arnPrefix) {
		return arn
	}
	// Find the 6th colon (arn:partition:s3:region:account:resource)
	// For S3: arn:aws:s3:::bucket → resource is "bucket"
	parts := strings.SplitN(arn, ":", s3ARNParts)
	if len(parts) == s3ARNParts {
		return parts[5]
	}

	return arn
}

// triggerReplication asynchronously replicates a newly-written object to all
// destination buckets configured in the source bucket's ReplicationConfiguration.
// It is called after PutObject completes successfully.
func (b *InMemoryBackend) triggerReplication(ctx context.Context, bucketName, key, _ string) {
	b.mu.RLock("triggerReplication.readConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()
	if err != nil {
		return
	}

	bucket.mu.RLock("triggerReplication.readConfig")
	cfgXML := bucket.ReplicationConfig
	bucket.mu.RUnlock()

	if cfgXML == "" {
		return
	}

	var cfg ReplicationConfiguration
	if xmlErr := xml.Unmarshal([]byte(cfgXML), &cfg); xmlErr != nil {
		logger.Load(ctx).WarnContext(ctx, "replication: failed to parse config", "bucket", bucketName, "error", xmlErr)

		return
	}

	// Fetch source object once for all matching rules.
	getOut, getErr := b.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(key),
	})
	if getErr != nil {
		logger.Load(ctx).
			WarnContext(ctx, "replication: source GetObject failed", "bucket", bucketName, "key", key, "error", getErr)

		return
	}
	defer getOut.Body.Close()

	data, readErr := io.ReadAll(getOut.Body)
	if readErr != nil {
		logger.Load(ctx).WarnContext(ctx, "replication: read source body failed", "error", readErr)

		return
	}

	for _, rule := range cfg.Rules {
		if rule.Status != statusEnabled {
			continue
		}
		if rule.Prefix != "" && !strings.HasPrefix(key, rule.Prefix) {
			continue
		}
		destBucket := bucketNameFromARN(rule.Destination.Bucket)
		if destBucket == "" {
			continue
		}

		putIn := &s3.PutObjectInput{
			Bucket:      aws.String(destBucket),
			Key:         aws.String(key),
			Body:        bytes.NewReader(data),
			Metadata:    getOut.Metadata,
			ContentType: getOut.ContentType,
		}
		if getOut.ContentEncoding != nil {
			putIn.ContentEncoding = getOut.ContentEncoding
		}
		if getOut.ContentDisposition != nil {
			putIn.ContentDisposition = getOut.ContentDisposition
		}
		if _, putErr := b.PutObject(ctx, putIn); putErr != nil {
			logger.Load(ctx).WarnContext(ctx, "replication: dest PutObject failed",
				"src", bucketName, "dest", destBucket, "key", key, "error", putErr)
		}
	}
}

// triggerDeleteMarkerReplication asynchronously replicates a delete-marker to
// destination buckets whose rules have DeleteMarkerReplication.Status = statusEnabled.
func (b *InMemoryBackend) triggerDeleteMarkerReplication(ctx context.Context, bucketName, key string) {
	b.mu.RLock("triggerDeleteMarkerReplication.readConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()
	if err != nil {
		return
	}

	bucket.mu.RLock("triggerDeleteMarkerReplication.readConfig")
	cfgXML := bucket.ReplicationConfig
	bucket.mu.RUnlock()

	if cfgXML == "" {
		return
	}

	var cfg ReplicationConfiguration
	if xmlErr := xml.Unmarshal([]byte(cfgXML), &cfg); xmlErr != nil {
		return
	}

	for _, rule := range cfg.Rules {
		if rule.Status != statusEnabled {
			continue
		}
		if rule.DeleteMarkerReplication.Status != statusEnabled {
			continue
		}
		if rule.Prefix != "" && !strings.HasPrefix(key, rule.Prefix) {
			continue
		}
		destBucket := bucketNameFromARN(rule.Destination.Bucket)
		if destBucket == "" {
			continue
		}
		if _, delErr := b.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(destBucket),
			Key:    aws.String(key),
		}); delErr != nil {
			logger.Load(ctx).WarnContext(ctx, "replication: dest DeleteObject failed",
				"src", bucketName, "dest", destBucket, "key", key, "error", delErr)
		}
	}
}
