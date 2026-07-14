package s3 //nolint:dupl // put/get/delete are structurally identical thin CRUD
// wrappers around distinct XML sub-resources (CORS here, encryption/replication/
// website in sibling files); each family lives in its own file per project
// convention, which makes the whole-file clone visible to dupl even though the
// code always looked like this.

import (
	"context"
)

// PutBucketCORS stores the bucket CORS configuration.
func (b *InMemoryBackend) PutBucketCORS(_ context.Context, bucketName, corsXML string) error {
	b.mu.RLock("PutBucketCORS")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketCORS")
	defer bucket.mu.Unlock()

	bucket.CORSConfig = corsXML

	return nil
}

// GetBucketCORS returns the bucket CORS configuration.
func (b *InMemoryBackend) GetBucketCORS(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketCORS")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketCORS")
	defer bucket.mu.RUnlock()

	if bucket.CORSConfig == "" {
		return "", ErrNoCORSConfig
	}

	return bucket.CORSConfig, nil
}

// DeleteBucketCORS clears the bucket CORS configuration.
func (b *InMemoryBackend) DeleteBucketCORS(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketCORS")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketCORS")
	defer bucket.mu.Unlock()

	bucket.CORSConfig = ""

	return nil
}
