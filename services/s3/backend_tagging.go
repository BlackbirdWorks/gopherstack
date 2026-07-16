package s3

import (
	"context"
	"fmt"
	"slices"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func (b *InMemoryBackend) PutObjectTagging(
	_ context.Context,
	input *s3.PutObjectTaggingInput,
) (*s3.PutObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("PutObjectTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	resolvedVersionID, err := func() (string, error) {
		bucket.mu.Lock("PutObjectTagging")
		defer bucket.mu.Unlock()

		obj, objExists := bucket.Objects[key]
		if !objExists {
			return "", ErrNoSuchKey
		}

		var ver *StoredObjectVersion
		if versionID != nil && *versionID != "" {
			v, ok := obj.Versions[*versionID]
			if !ok {
				return "", ErrNoSuchKey
			}
			ver = v
		} else {
			ver = findLatestVersion(obj.Versions)
		}

		if ver == nil || ver.Deleted {
			return "", ErrNoSuchKey
		}

		return ver.VersionID, nil
	}()
	if err != nil {
		return nil, err
	}

	b.mu.Lock("PutObjectTagging")
	defer b.mu.Unlock()

	tagKey := fmt.Sprintf("%s/%s/%s", bucketName, key, resolvedVersionID)
	if b.tags == nil {
		b.tags = make(map[string][]types.Tag)
	}

	b.tags[tagKey] = slices.Clone(input.Tagging.TagSet)

	return &s3.PutObjectTaggingOutput{}, nil
}

func (b *InMemoryBackend) GetObjectTagging(
	_ context.Context,
	input *s3.GetObjectTaggingInput,
) (*s3.GetObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("GetObjectTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	vid, err := func() (string, error) {
		bucket.mu.RLock("GetObjectTagging")
		defer bucket.mu.RUnlock()

		obj, objExists := bucket.Objects[key]
		if !objExists {
			return "", ErrNoSuchKey
		}

		if versionID != nil && *versionID != "" {
			v, ok := obj.Versions[*versionID]
			if !ok {
				return "", ErrNoSuchKey
			}

			return v.VersionID, nil
		}

		for _, v := range obj.Versions {
			if v.IsLatest {
				return v.VersionID, nil
			}
		}

		return "", ErrNoSuchKey
	}()
	if err != nil {
		return nil, err
	}

	b.mu.RLock("GetObjectTagging")
	defer b.mu.RUnlock()

	tagKey := fmt.Sprintf("%s/%s/%s", bucketName, key, vid)
	tags := slices.Clone(b.tags[tagKey])

	return &s3.GetObjectTaggingOutput{
		TagSet: tags,
	}, nil
}

func (b *InMemoryBackend) DeleteObjectTagging(
	_ context.Context,
	input *s3.DeleteObjectTaggingInput,
) (*s3.DeleteObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("DeleteObjectTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	vid, err := func() (string, error) {
		bucket.mu.RLock("DeleteObjectTagging")
		defer bucket.mu.RUnlock()

		obj, objExists := bucket.Objects[key]
		if !objExists {
			return "", nil // S3 Delete is idempotent
		}

		if versionID != nil && *versionID != "" {
			return *versionID, nil
		}

		for _, v := range obj.Versions {
			if v.IsLatest {
				return v.VersionID, nil
			}
		}

		return "", nil
	}()
	if err != nil {
		return nil, err
	}

	if vid == "" {
		return &s3.DeleteObjectTaggingOutput{}, nil
	}

	b.mu.Lock("DeleteObjectTagging")
	defer b.mu.Unlock()

	tagKey := bucketName + "/" + key + "/" + vid
	if b.tags != nil {
		delete(b.tags, tagKey)
	}

	return &s3.DeleteObjectTaggingOutput{}, nil
}

// PutBucketTagging sets the tag set for a bucket.
func (b *InMemoryBackend) PutBucketTagging(
	_ context.Context,
	bucketName string,
	tags []types.Tag,
) error {
	b.mu.RLock("PutBucketTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketTagging")
	defer bucket.mu.Unlock()

	bucket.Tags = tags

	return nil
}

// GetBucketTagging returns the tag set for a bucket.
func (b *InMemoryBackend) GetBucketTagging(
	_ context.Context,
	bucketName string,
) ([]types.Tag, error) {
	b.mu.RLock("GetBucketTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetBucketTagging")
	defer bucket.mu.RUnlock()

	if len(bucket.Tags) == 0 {
		return nil, ErrNoSuchTagSet
	}

	result := make([]types.Tag, len(bucket.Tags))
	copy(result, bucket.Tags)

	return result, nil
}

// DeleteBucketTagging removes the tag set from a bucket.
func (b *InMemoryBackend) DeleteBucketTagging(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketTagging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketTagging")
	defer bucket.mu.Unlock()

	bucket.Tags = nil

	return nil
}
