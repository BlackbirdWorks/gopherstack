package s3

import (
	"context"
)

// PutBucketLogging stores the logging configuration for a bucket.
func (b *InMemoryBackend) PutBucketLogging(_ context.Context, bucketName, loggingXML string) error {
	b.mu.RLock("PutBucketLogging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketLogging")
	defer bucket.mu.Unlock()

	bucket.LoggingConfig = loggingXML

	return nil
}

// GetBucketLogging returns the logging configuration for a bucket.
// Returns "" (empty string) when no logging is configured; the handler
// synthesizes the AWS-compatible empty XML response.
func (b *InMemoryBackend) GetBucketLogging(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketLogging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLogging")
	defer bucket.mu.RUnlock()

	return bucket.LoggingConfig, nil
}
