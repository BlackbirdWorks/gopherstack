package s3 //nolint:dupl // put/get/delete are structurally identical thin CRUD
// wrappers around distinct XML sub-resources (website here, CORS/encryption/
// replication in sibling files); each family lives in its own file per project
// convention, which makes the whole-file clone visible to dupl even though the
// code always looked like this.

import (
	"context"
)

// PutBucketWebsite stores the static website configuration for a bucket.
func (b *InMemoryBackend) PutBucketWebsite(_ context.Context, bucketName, websiteXML string) error {
	b.mu.RLock("PutBucketWebsite")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketWebsite")
	defer bucket.mu.Unlock()

	bucket.WebsiteConfig = websiteXML

	return nil
}

// GetBucketWebsite returns the static website configuration for a bucket.
func (b *InMemoryBackend) GetBucketWebsite(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketWebsite")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketWebsite")
	defer bucket.mu.RUnlock()

	if bucket.WebsiteConfig == "" {
		return "", ErrNoWebsiteConfig
	}

	return bucket.WebsiteConfig, nil
}

// DeleteBucketWebsite clears the static website configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketWebsite(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketWebsite")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketWebsite")
	defer bucket.mu.Unlock()

	bucket.WebsiteConfig = ""

	return nil
}
