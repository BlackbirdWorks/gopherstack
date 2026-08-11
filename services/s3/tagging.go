package s3

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
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

// TaggedEntry pairs a bucket ARN with its tag set, for the Resource Groups
// Tagging API's GetResources (see cli.go's wireTaggingS3).
type TaggedEntry struct {
	Tags map[string]string
	ARN  string
}

// TaggedResources returns every bucket's tag set keyed by its ARN. Real S3
// object ARNs are excluded: GetResources's own ResourceTypeFilters
// documentation only ever gives "s3:bucket" as the S3 example (botocore
// 1.43.56, resourcegroupstaggingapi/2017-01-26/service-2.json.gz,
// GetResourcesInput.ResourceTypeFilters), and objects have no such type.
func (b *InMemoryBackend) TaggedResources() []TaggedEntry {
	b.mu.RLock("TaggedResources")
	all := b.buckets.All()
	b.mu.RUnlock()

	out := make([]TaggedEntry, 0, len(all))

	for _, bucket := range all {
		if bucket.DeletePending || bucket.IsDirectoryBucket {
			continue
		}

		bucket.mu.RLock("TaggedResources")
		tags := make(map[string]string, len(bucket.Tags))
		for _, t := range bucket.Tags {
			tags[aws.ToString(t.Key)] = aws.ToString(t.Value)
		}
		bucket.mu.RUnlock()

		out = append(out, TaggedEntry{ARN: arn.BuildS3(bucket.Name), Tags: tags})
	}

	return out
}

// MergeBucketTags adds/overwrites tags in a bucket's existing tag set. Unlike
// PutBucketTagging (which replaces the whole set, matching real S3's
// PutBucketTagging semantics), this merges -- the semantics Resource Groups
// Tagging API's TagResources needs.
func (b *InMemoryBackend) MergeBucketTags(bucketName string, newTags map[string]string) error {
	b.mu.RLock("MergeBucketTags")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("MergeBucketTags")
	defer bucket.mu.Unlock()

	merged := make(map[string]string, len(bucket.Tags)+len(newTags))
	for _, t := range bucket.Tags {
		merged[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}

	maps.Copy(merged, newTags)

	tags := make([]types.Tag, 0, len(merged))
	for k, v := range merged {
		tags = append(tags, types.Tag{Key: aws.String(k), Value: aws.String(v)})
	}

	bucket.Tags = tags

	return nil
}

// RemoveBucketTags deletes specific tag keys from a bucket's tag set, for
// Resource Groups Tagging API's UntagResources.
func (b *InMemoryBackend) RemoveBucketTags(bucketName string, keys []string) error {
	b.mu.RLock("RemoveBucketTags")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	drop := make(map[string]bool, len(keys))
	for _, k := range keys {
		drop[k] = true
	}

	bucket.mu.Lock("RemoveBucketTags")
	defer bucket.mu.Unlock()

	kept := make([]types.Tag, 0, len(bucket.Tags))
	for _, t := range bucket.Tags {
		if !drop[aws.ToString(t.Key)] {
			kept = append(kept, t)
		}
	}

	bucket.Tags = kept

	return nil
}
