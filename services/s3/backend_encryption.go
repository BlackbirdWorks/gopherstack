package s3 //nolint:dupl // put/get/delete are structurally identical thin CRUD
// wrappers around distinct XML sub-resources (encryption here, CORS/replication/
// website in sibling files); each family lives in its own file per project
// convention, which makes the whole-file clone visible to dupl even though the
// code always looked like this.

import (
	"context"
)

// PutBucketEncryption stores the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) PutBucketEncryption(
	_ context.Context,
	bucketName, encryptionXML string,
) error {
	b.mu.RLock("PutBucketEncryption")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketEncryption")
	defer bucket.mu.Unlock()

	bucket.EncryptionConfig = encryptionXML

	return nil
}

// GetBucketEncryption returns the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) GetBucketEncryption(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketEncryption")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketEncryption")
	defer bucket.mu.RUnlock()

	if bucket.EncryptionConfig == "" {
		return "", ErrNoEncryptionConfig
	}

	return bucket.EncryptionConfig, nil
}

// DeleteBucketEncryption clears the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketEncryption(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketEncryption")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketEncryption")
	defer bucket.mu.Unlock()

	bucket.EncryptionConfig = ""

	return nil
}
