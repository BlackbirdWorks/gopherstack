package s3

import (
	"context"
)

// PutBucketACL stores the canned ACL for a bucket.
func (b *InMemoryBackend) PutBucketACL(_ context.Context, bucketName, acl string) error {
	b.mu.RLock("PutBucketACL")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketACL")
	defer bucket.mu.Unlock()

	bucket.ACL = acl

	return nil
}

// GetBucketACL returns the canned ACL for a bucket.
func (b *InMemoryBackend) GetBucketACL(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketACL")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketACL")
	defer bucket.mu.RUnlock()

	acl := bucket.ACL
	if acl == "" {
		acl = aclPrivate
	}

	return acl, nil
}

// PutBucketPolicy stores the bucket policy document.
func (b *InMemoryBackend) PutBucketPolicy(_ context.Context, bucketName, policy string) error {
	b.mu.RLock("PutBucketPolicy")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketPolicy")
	defer bucket.mu.Unlock()

	bucket.Policy = policy

	return nil
}

// GetBucketPolicy returns the bucket policy document.
func (b *InMemoryBackend) GetBucketPolicy(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketPolicy")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketPolicy")
	defer bucket.mu.RUnlock()

	if bucket.Policy == "" {
		return "", ErrNoBucketPolicy
	}

	return bucket.Policy, nil
}

// DeleteBucketPolicy clears the bucket policy document.
func (b *InMemoryBackend) DeleteBucketPolicy(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketPolicy")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketPolicy")
	defer bucket.mu.Unlock()

	bucket.Policy = ""

	return nil
}

// PutPublicAccessBlock stores the public access block configuration for a bucket.
func (b *InMemoryBackend) PutPublicAccessBlock(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("PutPublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutPublicAccessBlock")
	defer bucket.mu.Unlock()

	bucket.PublicAccessBlockConfig = configXML

	return nil
}

// GetPublicAccessBlock returns the public access block configuration for a bucket.
func (b *InMemoryBackend) GetPublicAccessBlock(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetPublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetPublicAccessBlock")
	defer bucket.mu.RUnlock()

	if bucket.PublicAccessBlockConfig == "" {
		return "", ErrNoPublicAccessBlock
	}

	return bucket.PublicAccessBlockConfig, nil
}

// DeletePublicAccessBlock removes the public access block configuration for a bucket.
func (b *InMemoryBackend) DeletePublicAccessBlock(_ context.Context, bucketName string) error {
	b.mu.RLock("DeletePublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeletePublicAccessBlock")
	defer bucket.mu.Unlock()

	bucket.PublicAccessBlockConfig = ""

	return nil
}

// PutBucketOwnershipControls stores the ownership controls configuration for a bucket.
func (b *InMemoryBackend) PutBucketOwnershipControls(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("PutBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketOwnershipControls")
	defer bucket.mu.Unlock()

	bucket.OwnershipControlsConfig = configXML

	return nil
}

// GetBucketOwnershipControls returns the ownership controls configuration for a bucket.
func (b *InMemoryBackend) GetBucketOwnershipControls(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketOwnershipControls")
	defer bucket.mu.RUnlock()

	if bucket.OwnershipControlsConfig == "" {
		return "", ErrNoOwnershipControls
	}

	return bucket.OwnershipControlsConfig, nil
}

// DeleteBucketOwnershipControls removes the ownership controls configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketOwnershipControls(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketOwnershipControls")
	defer bucket.mu.Unlock()

	bucket.OwnershipControlsConfig = ""

	return nil
}

// PutBucketAbac stores the ABAC configuration XML for an S3 Tables bucket.
func (b *InMemoryBackend) PutBucketAbac(_ context.Context, bucketName, configXML string) error {
	b.mu.RLock("PutBucketAbac")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketAbac")
	defer bucket.mu.Unlock()

	bucket.AbacConfig = configXML

	return nil
}

// GetBucketAbac returns the stored ABAC configuration XML for a bucket.
// Returns an empty string (not an error) when no config has been set, matching
// the AWS behaviour of returning an empty AbacConfiguration element.
func (b *InMemoryBackend) GetBucketAbac(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketAbac")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketAbac")
	defer bucket.mu.RUnlock()

	return bucket.AbacConfig, nil
}
