package s3 //nolint:dupl // put/get/delete are structurally identical thin CRUD
// wrappers around distinct XML sub-resources (replication here, CORS/encryption/
// website in sibling files); each family lives in its own file per project
// convention, which makes the whole-file clone visible to dupl even though the
// code always looked like this.

import (
	"context"
)

// PutBucketReplication stores the replication configuration for a bucket.
func (b *InMemoryBackend) PutBucketReplication(
	_ context.Context,
	bucketName, replicationXML string,
) error {
	b.mu.RLock("PutBucketReplication")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketReplication")
	defer bucket.mu.Unlock()

	bucket.ReplicationConfig = replicationXML

	return nil
}

// GetBucketReplication returns the replication configuration for a bucket.
func (b *InMemoryBackend) GetBucketReplication(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketReplication")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketReplication")
	defer bucket.mu.RUnlock()

	if bucket.ReplicationConfig == "" {
		return "", ErrNoReplicationConfig
	}

	return bucket.ReplicationConfig, nil
}

// DeleteBucketReplication removes the replication configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketReplication(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketReplication")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketReplication")
	defer bucket.mu.Unlock()

	bucket.ReplicationConfig = ""

	return nil
}
