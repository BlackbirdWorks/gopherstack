package s3

import (
	"context"
)

// PutBucketLifecycleConfiguration stores the lifecycle configuration for a bucket.
func (b *InMemoryBackend) PutBucketLifecycleConfiguration(
	_ context.Context,
	bucketName, lifecycleXML string,
) error {
	b.mu.RLock("PutBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketLifecycleConfiguration")
	defer bucket.mu.Unlock()

	bucket.LifecycleConfig = lifecycleXML

	return nil
}

// GetBucketLifecycleConfiguration returns the lifecycle configuration for a bucket.
func (b *InMemoryBackend) GetBucketLifecycleConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLifecycleConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.LifecycleConfig == "" {
		return "", ErrNoLifecycleConfig
	}

	return bucket.LifecycleConfig, nil
}

// DeleteBucketLifecycleConfiguration clears the lifecycle configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketLifecycleConfiguration(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketLifecycleConfiguration")
	defer bucket.mu.Unlock()

	bucket.LifecycleConfig = ""

	return nil
}

// DeleteBucketLifecycle clears the lifecycle configuration for a bucket.
// This is the legacy alias for DeleteBucketLifecycleConfiguration.
func (b *InMemoryBackend) DeleteBucketLifecycle(ctx context.Context, bucketName string) error {
	return b.DeleteBucketLifecycleConfiguration(ctx, bucketName)
}
