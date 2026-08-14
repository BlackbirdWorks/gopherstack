package s3

import (
	"context"
)

// CreateBucketMetadataConfiguration stores the metadata configuration for a bucket.
func (b *InMemoryBackend) CreateBucketMetadataConfiguration(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("CreateBucketMetadataConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("CreateBucketMetadataConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataConfig = configXML

	return nil
}

// GetBucketMetadataConfiguration returns the metadata configuration for a bucket.
func (b *InMemoryBackend) GetBucketMetadataConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketMetadataConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketMetadataConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.MetadataConfig == "" {
		return "", ErrNoMetadataConfig
	}

	return bucket.MetadataConfig, nil
}

// DeleteBucketMetadataConfiguration clears the metadata configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketMetadataConfiguration(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketMetadataConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketMetadataConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataConfig = ""

	return nil
}

// CreateBucketMetadataTableConfiguration stores the metadata table configuration for a bucket.
func (b *InMemoryBackend) CreateBucketMetadataTableConfiguration(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("CreateBucketMetadataTableConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("CreateBucketMetadataTableConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataTableConfig = configXML

	return nil
}

// GetBucketMetadataTableConfiguration returns the metadata table configuration for a bucket.
func (b *InMemoryBackend) GetBucketMetadataTableConfiguration(
	_ context.Context,
	bucketName string,
) (string, error) {
	b.mu.RLock("GetBucketMetadataTableConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketMetadataTableConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.MetadataTableConfig == "" {
		return "", ErrNoMetadataTableConfig
	}

	return bucket.MetadataTableConfig, nil
}

// DeleteBucketMetadataTableConfiguration clears the metadata table configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketMetadataTableConfiguration(
	_ context.Context,
	bucketName string,
) error {
	b.mu.RLock("DeleteBucketMetadataTableConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketMetadataTableConfiguration")
	defer bucket.mu.Unlock()

	bucket.MetadataTableConfig = ""

	return nil
}

// UpdateBucketMetadataInventoryTableConfig stores the metadata inventory table
// configuration XML for an S3 Tables bucket.
func (b *InMemoryBackend) UpdateBucketMetadataInventoryTableConfig(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("UpdateBucketMetadataInventoryTableConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("UpdateBucketMetadataInventoryTableConfig")
	defer bucket.mu.Unlock()

	bucket.MetadataInventoryTableConfig = configXML

	return nil
}

// UpdateBucketMetadataJournalTableConfig stores the metadata journal table
// configuration XML for an S3 Tables bucket.
func (b *InMemoryBackend) UpdateBucketMetadataJournalTableConfig(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("UpdateBucketMetadataJournalTableConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("UpdateBucketMetadataJournalTableConfig")
	defer bucket.mu.Unlock()

	bucket.MetadataJournalTableConfig = configXML

	return nil
}

// UpdateBucketMetadataAnnotationTableConfig stores the annotation table
// configuration XML for an S3 Metadata configuration (real key
// "metadataAnnotationTable" -- verified against s3@v1.106.5 serializers.go:
// awsRestxml_serializeOpUpdateBucketMetadataAnnotationTableConfiguration's
// httpbinding.SplitURI("/?metadataAnnotationTable")). There is no dedicated
// Get op for this sub-config in the pinned SDK; it is exposed to the caller,
// if at all, nested inside GetBucketMetadataConfiguration's body, matching
// how the sibling inventory/journal table configs already behave here.
func (b *InMemoryBackend) UpdateBucketMetadataAnnotationTableConfig(
	_ context.Context,
	bucketName, configXML string,
) error {
	b.mu.RLock("UpdateBucketMetadataAnnotationTableConfig")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("UpdateBucketMetadataAnnotationTableConfig")
	defer bucket.mu.Unlock()

	bucket.MetadataAnnotationTableConfig = configXML

	return nil
}
