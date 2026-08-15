package s3

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (b *InMemoryBackend) PutBucketVersioning(
	_ context.Context,
	input *s3.PutBucketVersioningInput,
) (*s3.PutBucketVersioningOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("PutBucketVersioning")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.Lock("PutBucketVersioning")
	defer bucket.mu.Unlock()

	status := input.VersioningConfiguration.Status
	bucket.Versioning = status

	if mfaDelete := input.VersioningConfiguration.MFADelete; mfaDelete != "" {
		bucket.MFADelete = string(mfaDelete)
	}

	return &s3.PutBucketVersioningOutput{}, nil
}

func (b *InMemoryBackend) GetBucketVersioning(
	_ context.Context,
	input *s3.GetBucketVersioningInput,
) (*s3.GetBucketVersioningOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("GetBucketVersioning")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetBucketVersioning")
	defer bucket.mu.RUnlock()

	return &s3.GetBucketVersioningOutput{
		Status:    bucket.Versioning,
		MFADelete: types.MFADeleteStatus(bucket.MFADelete),
	}, nil
}
