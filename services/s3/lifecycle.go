package s3

import (
	"context"
)

// PutBucketLifecycleConfiguration stores the lifecycle configuration for a bucket.
// transitionDefaultMinObjectSize is the X-Amz-Transition-Default-Minimum-Object-Size
// header value (s3@v1.111.0 serializers.go:7411); empty when the caller didn't set it.
func (b *InMemoryBackend) PutBucketLifecycleConfiguration(
	_ context.Context,
	bucketName, lifecycleXML, transitionDefaultMinObjectSize string,
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
	bucket.TransitionDefaultMinObjectSize = transitionDefaultMinObjectSize

	return nil
}

// GetBucketLifecycleTransitionDefaultMinObjectSize returns the bucket's stored
// X-Amz-Transition-Default-Minimum-Object-Size value (s3@v1.111.0
// deserializers.go:4579, echoed on GetBucketLifecycleConfiguration's response),
// "" when never set.
func (b *InMemoryBackend) GetBucketLifecycleTransitionDefaultMinObjectSize(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketLifecycleTransitionDefaultMinObjectSize")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLifecycleTransitionDefaultMinObjectSize")
	defer bucket.mu.RUnlock()

	return bucket.TransitionDefaultMinObjectSize, nil
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
	bucket.TransitionDefaultMinObjectSize = ""

	return nil
}

// DeleteBucketLifecycle clears the lifecycle configuration for a bucket.
// This is the legacy alias for DeleteBucketLifecycleConfiguration.
func (b *InMemoryBackend) DeleteBucketLifecycle(ctx context.Context, bucketName string) error {
	return b.DeleteBucketLifecycleConfiguration(ctx, bucketName)
}
