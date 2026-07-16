package s3

import (
	"context"
)

func (b *InMemoryBackend) PutBucketNotificationConfiguration(
	_ context.Context,
	bucketName, notifXML string,
) error {
	b.mu.RLock("PutBucketNotificationConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketNotificationConfiguration")
	defer bucket.mu.Unlock()

	bucket.NotificationConfig = notifXML

	return nil
}

// GetBucketNotificationConfiguration returns the notification configuration for a bucket.
func (b *InMemoryBackend) GetBucketNotificationConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketNotificationConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketNotificationConfiguration")
	defer bucket.mu.RUnlock()

	// Notification config is always returned, even if empty (AWS returns empty XML)
	return bucket.NotificationConfig, nil
}
