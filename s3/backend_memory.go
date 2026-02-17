package s3

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // MD5 required for S3 ETag compatibility
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const maxInt32 = 2147483647

var _ StorageBackend = (*InMemoryBackend)(nil)

type InMemoryBackend struct {
	compressor Compressor
	buckets    map[string]*StoredBucket
	tags       map[string][]types.Tag
	uploads    map[string]*StoredMultipartUpload
	mu         sync.RWMutex
}

func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	return &InMemoryBackend{
		buckets:    make(map[string]*StoredBucket),
		compressor: compressor,
	}
}

func (b *InMemoryBackend) CreateBucket(_ context.Context, input *s3.CreateBucketInput) (*s3.CreateBucketOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bucketName := *input.Bucket

	if _, exists := b.buckets[bucketName]; exists {
		return nil, ErrBucketAlreadyExists
	}

	b.buckets[bucketName] = &StoredBucket{
		Name:         bucketName,
		CreationDate: time.Now(),
		Objects:      make(map[string]*StoredObject),
		Versioning:   types.BucketVersioningStatusSuspended,
	}

	return &s3.CreateBucketOutput{}, nil
}

func (b *InMemoryBackend) DeleteBucket(_ context.Context, input *s3.DeleteBucketInput) (*s3.DeleteBucketOutput, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bucketName := *input.Bucket

	bucket, exists := b.buckets[bucketName]
	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if len(bucket.Objects) > 0 {
		return nil, ErrBucketNotEmpty
	}

	delete(b.buckets, bucketName)

	return &s3.DeleteBucketOutput{}, nil
}

func (b *InMemoryBackend) HeadBucket(_ context.Context, input *s3.HeadBucketInput) (*s3.HeadBucketOutput, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bucketName := *input.Bucket

	if _, exists := b.buckets[bucketName]; !exists {
		return nil, ErrNoSuchBucket
	}

	return &s3.HeadBucketOutput{}, nil
}

func (b *InMemoryBackend) ListBuckets(_ context.Context, _ *s3.ListBucketsInput) (*s3.ListBucketsOutput, error) {
	b.mu.RLock()
	buckets := make([]types.Bucket, 0, len(b.buckets))
	for _, bucket := range b.buckets {
		buckets = append(buckets, types.Bucket{
			Name:         aws.String(bucket.Name),
			CreationDate: aws.Time(bucket.CreationDate),
		})
	}
	b.mu.RUnlock()

	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return &s3.ListBucketsOutput{
		Buckets: buckets,
		Owner: &types.Owner{
			DisplayName: aws.String("Gopherstack"),
			ID:          aws.String("placeholder-id"),
		},
	}, nil
}

func (b *InMemoryBackend) PutObject(_ context.Context, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	// 1. Prepare data and metadata outside the lock
	data, compressedData, isCompressed, etag, err := b.prepareObjectData(input)
	if err != nil {
		return nil, err
	}

	checksums := struct {
		crc32, crc32c, sha1, sha256 *string
	}{
		input.ChecksumCRC32, input.ChecksumCRC32C,
		input.ChecksumSHA1, input.ChecksumSHA256,
	}

	// 2. Lock and update
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	// First, get or create the object under bucket lock (brief)
	var obj *StoredObject
	var newVersionID string

	bucket.mu.Lock()
	newVersionID = "null"
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		newVersionID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	obj, objExists := bucket.Objects[key]
	if !objExists {
		obj = &StoredObject{
			Key:      key,
			Versions: make(map[string]*StoredObjectVersion),
		}
		bucket.Objects[key] = obj
	}
	bucket.mu.Unlock()

	// Now update the object versions under object lock (reduces contention on bucket)
	obj.mu.Lock()
	defer obj.mu.Unlock()

	finalQuotedETag := "\"" + etag + "\""
	newVersion := &StoredObjectVersion{
		VersionID:         newVersionID,
		Key:               key,
		Data:              compressedData,
		IsCompressed:      isCompressed,
		Size:              int64(len(data)),
		ETag:              finalQuotedETag,
		LastModified:      time.Now(),
		ContentType:       aws.ToString(input.ContentType),
		Metadata:          input.Metadata,
		ChecksumCRC32:     checksums.crc32,
		ChecksumCRC32C:    checksums.crc32c,
		ChecksumSHA1:      checksums.sha1,
		ChecksumSHA256:    checksums.sha256,
		ChecksumAlgorithm: input.ChecksumAlgorithm,
		IsLatest:          true,
	}

	for _, v := range obj.Versions {
		if v.IsLatest {
			v.IsLatest = false
		}
	}

	obj.Versions[newVersionID] = newVersion

	b.storeObjectTags(input.Tagging, bucketName, key, newVersionID)

	return &s3.PutObjectOutput{
		ETag:           aws.String(finalQuotedETag),
		VersionId:      aws.String(newVersionID),
		ChecksumCRC32:  checksums.crc32,
		ChecksumCRC32C: checksums.crc32c,
		ChecksumSHA1:   checksums.sha1,
		ChecksumSHA256: checksums.sha256,
	}, nil
}

func (b *InMemoryBackend) prepareObjectData(
	input *s3.PutObjectInput,
) ([]byte, []byte, bool, string, error) {
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, nil, false, "", err
	}

	var compressedData []byte
	var isCompressed bool
	if b.compressor != nil {
		if cData, cErr := b.compressor.Compress(data); cErr == nil {
			compressedData = cData
			isCompressed = true
		} else {
			return nil, nil, false, "", cErr
		}
	} else {
		compressedData = data
		isCompressed = false
	}

	//nolint:gosec // MD5 is required for S3 ETag
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	return data, compressedData, isCompressed, etag, nil
}

func (b *InMemoryBackend) storeObjectTags(tagging *string, bucket, key, versionID string) {
	if tagging == nil {
		return
	}

	pTags, pErr := url.ParseQuery(*tagging)
	if pErr != nil {
		return
	}

	var tagList []types.Tag
	for k, v := range pTags {
		tagList = append(tagList, types.Tag{
			Key:   aws.String(k),
			Value: aws.String(v[0]),
		})
	}

	if len(tagList) == 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	if b.tags == nil {
		b.tags = make(map[string][]types.Tag)
	}

	tagKey := fmt.Sprintf("%s/%s/%s", bucket, key, versionID)
	b.tags[tagKey] = tagList
}

func (b *InMemoryBackend) GetObject(_ context.Context, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	obj, exists := bucket.Objects[key]
	if !exists {
		bucket.mu.RUnlock()

		return nil, ErrNoSuchKey
	}
	bucket.mu.RUnlock()

	// Use per-object lock for version operations instead of holding bucket lock
	obj.mu.RLock()
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion
	if versionID != nil && *versionID != "" {
		v, ok := obj.Versions[*versionID]
		if !ok {
			return nil, ErrNoSuchKey
		}
		ver = v
	} else {
		for _, v := range obj.Versions {
			if v.IsLatest {
				ver = v

				break
			}
		}
	}

	if ver == nil || ver.Deleted {
		return nil, ErrNoSuchKey
	}

	// Copy data out for decompression outside the lock
	dataToDecompress := ver.Data
	isCompressed := ver.IsCompressed
	size := ver.Size
	contentType := ver.ContentType
	etag := ver.ETag
	lastModified := ver.LastModified
	metadata := ver.Metadata
	versionIDStr := ver.VersionID
	checksumCRC32 := ver.ChecksumCRC32
	checksumCRC32C := ver.ChecksumCRC32C
	checksumSHA1 := ver.ChecksumSHA1
	checksumSHA256 := ver.ChecksumSHA256

	data := dataToDecompress
	if isCompressed {
		if b.compressor == nil {
			return nil, ErrNoCompressor
		}
		var decompressErr error
		data, decompressErr = b.compressor.Decompress(data)
		if decompressErr != nil {
			return nil, decompressErr
		}
	}

	return &s3.GetObjectOutput{
		Body:           io.NopCloser(bytes.NewReader(data)),
		ContentLength:  aws.Int64(size),
		ContentType:    aws.String(contentType),
		ETag:           aws.String(etag),
		LastModified:   aws.Time(lastModified),
		Metadata:       metadata,
		VersionId:      aws.String(versionIDStr),
		ChecksumCRC32:  checksumCRC32,
		ChecksumCRC32C: checksumCRC32C,
		ChecksumSHA1:   checksumSHA1,
		ChecksumSHA256: checksumSHA256,
	}, nil
}

func (b *InMemoryBackend) HeadObject(_ context.Context, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	obj, exists := bucket.Objects[key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	var ver *StoredObjectVersion
	if versionID != nil && *versionID != "" {
		v, ok := obj.Versions[*versionID]
		if !ok {
			return nil, ErrNoSuchKey
		}
		ver = v
	} else {
		for _, v := range obj.Versions {
			if v.IsLatest {
				ver = v

				break
			}
		}
	}

	if ver == nil || ver.Deleted {
		return nil, ErrNoSuchKey
	}

	return &s3.HeadObjectOutput{
		ContentLength:  aws.Int64(ver.Size),
		ContentType:    aws.String(ver.ContentType),
		ETag:           aws.String(ver.ETag),
		LastModified:   aws.Time(ver.LastModified),
		Metadata:       ver.Metadata,
		VersionId:      aws.String(ver.VersionID),
		ChecksumCRC32:  ver.ChecksumCRC32,
		ChecksumCRC32C: ver.ChecksumCRC32C,
		ChecksumSHA1:   ver.ChecksumSHA1,
		ChecksumSHA256: ver.ChecksumSHA256,
	}, nil
}

func (b *InMemoryBackend) DeleteObject(_ context.Context, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	obj, exists := bucket.Objects[key]
	if !exists {
		// S3 spec: Delete on non-existent object is 204
		return &s3.DeleteObjectOutput{}, nil
	}

	if versionID != nil && *versionID != "" {
		if _, ok := obj.Versions[*versionID]; ok {
			delete(obj.Versions, *versionID)
			if len(obj.Versions) == 0 {
				delete(bucket.Objects, key)
			}

			return &s3.DeleteObjectOutput{VersionId: versionID}, nil
		}

		return &s3.DeleteObjectOutput{}, nil
	}

	// Delete latest (Versioning enabled -> add delete marker, Suspended -> delete null version)
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		newVersionID := strconv.FormatInt(time.Now().UnixNano(), 10)

		// Create delete marker
		// Mark others as not latest
		for _, v := range obj.Versions {
			v.IsLatest = false
		}
		obj.Versions[newVersionID] = &StoredObjectVersion{
			VersionID:    newVersionID,
			Key:          key,
			Deleted:      true,
			IsLatest:     true,
			LastModified: time.Now(),
		}

		return &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(true),
			VersionId:    aws.String(newVersionID),
		}, nil
	}

	// Suspended or null: Delete object (or null version)
	// Simple remove for now
	delete(bucket.Objects, key)

	return &s3.DeleteObjectOutput{}, nil
}

func (b *InMemoryBackend) DeleteObjects(
	ctx context.Context,
	input *s3.DeleteObjectsInput,
) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{
		Deleted: make([]types.DeletedObject, 0, len(input.Delete.Objects)),
		Errors:  make([]types.Error, 0),
	}

	for _, obj := range input.Delete.Objects {
		delInput := &s3.DeleteObjectInput{
			Bucket:    input.Bucket,
			Key:       obj.Key,
			VersionId: obj.VersionId,
		}
		// We reuse DeleteObject's logic by calling it directly.
		// S3 DeleteObjects can return errors for some objects while succeeding for others.
		delOut, err := b.DeleteObject(ctx, delInput)
		if err != nil {
			// S3 error format for DeleteObjects
			s3Err := types.Error{
				Key:     obj.Key,
				Code:    aws.String("InternalError"),
				Message: aws.String(err.Error()),
			}
			if errors.Is(err, ErrNoSuchBucket) {
				s3Err.Code = aws.String("NoSuchBucket")
			}
			out.Errors = append(out.Errors, s3Err)

			continue
		}

		deleted := types.DeletedObject{
			Key:       obj.Key,
			VersionId: obj.VersionId,
		}
		if delOut.DeleteMarker != nil {
			deleted.DeleteMarker = delOut.DeleteMarker
		}
		if delOut.VersionId != nil {
			deleted.DeleteMarkerVersionId = delOut.VersionId
		}

		out.Deleted = append(out.Deleted, deleted)
	}

	return out, nil
}

func (b *InMemoryBackend) ListObjects(_ context.Context, input *s3.ListObjectsInput) (*s3.ListObjectsOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	var contents []types.Object
	prefix := aws.ToString(input.Prefix)

	for _, obj := range bucket.Objects {
		var latest *StoredObjectVersion
		for _, v := range obj.Versions {
			if v.IsLatest {
				latest = v

				break
			}
		}

		if latest == nil || latest.Deleted {
			continue
		}

		if strings.HasPrefix(latest.Key, prefix) {
			contents = append(contents, types.Object{
				Key:          aws.String(latest.Key),
				LastModified: aws.Time(latest.LastModified),
				ETag:         aws.String(latest.ETag),
				Size:         aws.Int64(latest.Size),
				StorageClass: types.ObjectStorageClassStandard,
				Owner: &types.Owner{
					ID:          aws.String("gopherstack"),
					DisplayName: aws.String("gopherstack"),
				},
			})
		}
	}
	bucket.mu.RUnlock()

	sort.Slice(contents, func(i, j int) bool {
		return *contents[i].Key < *contents[j].Key
	})

	return &s3.ListObjectsOutput{
		Name:     input.Bucket,
		Prefix:   input.Prefix,
		MaxKeys:  input.MaxKeys,
		Contents: contents,
	}, nil
}

func (b *InMemoryBackend) ListObjectsV2(
	_ context.Context,
	input *s3.ListObjectsV2Input,
) (*s3.ListObjectsV2Output, error) {
	// Re-use ListObjects logic for now as simplified implementation
	listOut, err := b.ListObjects(context.TODO(), &s3.ListObjectsInput{
		Bucket:  input.Bucket,
		Prefix:  input.Prefix,
		MaxKeys: input.MaxKeys,
	})
	if err != nil {
		return nil, err
	}

	count := int32(len(listOut.Contents)) // #nosec G115
	if len(listOut.Contents) > maxInt32 {
		count = maxInt32
	}

	return &s3.ListObjectsV2Output{
		Name:        input.Bucket,
		Prefix:      input.Prefix,
		MaxKeys:     input.MaxKeys,
		Contents:    listOut.Contents,
		KeyCount:    aws.Int32(count),
		IsTruncated: listOut.IsTruncated,
	}, nil
}

func (b *InMemoryBackend) ListObjectVersions(
	_ context.Context,
	input *s3.ListObjectVersionsInput,
) (*s3.ListObjectVersionsOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	var versions []types.ObjectVersion
	var deleteMarkers []types.DeleteMarkerEntry
	prefix := aws.ToString(input.Prefix)

	for _, obj := range bucket.Objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		for _, v := range obj.Versions {
			if v.Deleted {
				deleteMarkers = append(deleteMarkers, types.DeleteMarkerEntry{
					Key:          aws.String(v.Key),
					VersionId:    aws.String(v.VersionID),
					IsLatest:     aws.Bool(v.IsLatest),
					LastModified: aws.Time(v.LastModified),
					Owner: &types.Owner{
						ID:          aws.String("gopherstack"),
						DisplayName: aws.String("gopherstack"),
					},
				})
			} else {
				versions = append(versions, types.ObjectVersion{
					Key:          aws.String(v.Key),
					VersionId:    aws.String(v.VersionID),
					IsLatest:     aws.Bool(v.IsLatest),
					LastModified: aws.Time(v.LastModified),
					ETag:         aws.String(v.ETag),
					Size:         aws.Int64(v.Size),
					StorageClass: types.ObjectVersionStorageClassStandard,
					Owner: &types.Owner{
						ID:          aws.String("gopherstack"),
						DisplayName: aws.String("gopherstack"),
					},
				})
			}
		}
	}

	return &s3.ListObjectVersionsOutput{
		Name:          input.Bucket,
		Prefix:        input.Prefix,
		Versions:      versions,
		DeleteMarkers: deleteMarkers,
	}, nil
}

func (b *InMemoryBackend) PutBucketVersioning(
	_ context.Context,
	input *s3.PutBucketVersioningInput,
) (*s3.PutBucketVersioningOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	status := input.VersioningConfiguration.Status
	bucket.Versioning = status

	return &s3.PutBucketVersioningOutput{}, nil
}

func (b *InMemoryBackend) GetBucketVersioning(
	_ context.Context,
	input *s3.GetBucketVersioningInput,
) (*s3.GetBucketVersioningOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	return &s3.GetBucketVersioningOutput{
		Status: bucket.Versioning,
	}, nil
}

func (b *InMemoryBackend) PutObjectTagging(
	_ context.Context,
	input *s3.PutObjectTaggingInput,
) (*s3.PutObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	resolvedVersionID, err := func() (string, error) {
		bucket.mu.Lock()
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
			for _, v := range obj.Versions {
				if v.IsLatest {
					ver = v

					break
				}
			}
		}

		if ver == nil || ver.Deleted {
			return "", ErrNoSuchKey
		}

		return ver.VersionID, nil
	}()
	if err != nil {
		return nil, err
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	tagKey := fmt.Sprintf("%s/%s/%s", bucketName, key, resolvedVersionID)
	if b.tags == nil {
		b.tags = make(map[string][]types.Tag)
	}

	b.tags[tagKey] = input.Tagging.TagSet

	return &s3.PutObjectTaggingOutput{}, nil
}

func (b *InMemoryBackend) GetObjectTagging(
	_ context.Context,
	input *s3.GetObjectTaggingInput,
) (*s3.GetObjectTaggingOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	vid, err := func() (string, error) {
		bucket.mu.RLock()
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

	b.mu.RLock()
	defer b.mu.RUnlock()

	tagKey := fmt.Sprintf("%s/%s/%s", bucketName, key, vid)
	tags := b.tags[tagKey]

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

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	vid, err := func() (string, error) {
		bucket.mu.RLock()
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

	b.mu.Lock()
	defer b.mu.Unlock()

	tagKey := bucketName + "/" + key + "/" + vid
	if b.tags != nil {
		delete(b.tags, tagKey)
	}

	return &s3.DeleteObjectTaggingOutput{}, nil
}

// Multipart

func (b *InMemoryBackend) CreateMultipartUpload(
	_ context.Context,
	input *s3.CreateMultipartUploadInput,
) (*s3.CreateMultipartUploadOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	b.mu.RLock()
	_, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	uploadID := strconv.FormatInt(time.Now().UnixNano(), 10)

	b.mu.Lock()
	if b.uploads == nil {
		b.uploads = make(map[string]*StoredMultipartUpload)
	}

	b.uploads[uploadID] = &StoredMultipartUpload{
		UploadID: uploadID,
		Bucket:   bucketName,
		Key:      key,
		Parts:    make(map[int32]*StoredPart),
	}
	b.mu.Unlock()

	return &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String(uploadID),
	}, nil
}

func (b *InMemoryBackend) UploadPart(_ context.Context, input *s3.UploadPartInput) (*s3.UploadPartOutput, error) {
	uploadID := *input.UploadId
	partNumber := *input.PartNumber

	// 1. Read data outside the lock
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	//nolint:gosec // MD5 required
	hash := md5.Sum(data)
	etag := fmt.Sprintf("%q", hex.EncodeToString(hash[:]))

	// 2. Update upload state
	b.mu.Lock()
	defer b.mu.Unlock()

	upload, exists := b.uploads[uploadID]
	if !exists {
		return nil, ErrNoSuchUpload
	}

	upload.Parts[partNumber] = &StoredPart{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       int64(len(data)),
		Data:       data,
	}

	return &s3.UploadPartOutput{
		ETag: aws.String(etag),
	}, nil
}

func (b *InMemoryBackend) CompleteMultipartUpload(
	_ context.Context,
	input *s3.CompleteMultipartUploadInput,
) (*s3.CompleteMultipartUploadOutput, error) {
	uploadID := *input.UploadId
	bucketName := *input.Bucket
	key := *input.Key

	// 1. Get upload state
	b.mu.RLock()
	upload, exists := b.uploads[uploadID]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchUpload
	}

	// 2. Reassemble data outside the lock
	var data []byte
	for _, part := range input.MultipartUpload.Parts {
		pNum := *part.PartNumber
		storedPart, ok := upload.Parts[pNum]
		if !ok {
			return nil, ErrInvalidPart
		}
		if *part.ETag != storedPart.ETag {
			return nil, ErrInvalidPart
		}
		data = append(data, storedPart.Data...)
	}

	// Compress
	var compressedData []byte
	var isCompressed bool
	if b.compressor != nil {
		var err error
		compressedData, err = b.compressor.Compress(data)
		if err != nil {
			return nil, err
		}
		isCompressed = true
	} else {
		compressedData = data
		isCompressed = false
	}

	// ETag
	hash := md5.Sum(data) //nolint:gosec // MD5 required
	etag := fmt.Sprintf("%q", hex.EncodeToString(hash[:]))

	// 3. Update bucket/object state
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	obj, exists := bucket.Objects[key]
	if !exists {
		obj = &StoredObject{Key: key, Versions: make(map[string]*StoredObjectVersion)}
		bucket.Objects[key] = obj
	}

	versionID := "null"
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		versionID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	newVersion := &StoredObjectVersion{
		VersionID:    versionID,
		Key:          key,
		Data:         compressedData,
		IsCompressed: isCompressed,
		Size:         int64(len(data)),
		ETag:         etag,
		LastModified: time.Now(),
		IsLatest:     true,
	}

	for _, v := range obj.Versions {
		v.IsLatest = false
	}
	obj.Versions[versionID] = newVersion

	// Cleanup upload
	b.mu.Lock()
	delete(b.uploads, uploadID)
	b.mu.Unlock()

	return &s3.CompleteMultipartUploadOutput{
		Bucket:    input.Bucket,
		Key:       input.Key,
		ETag:      aws.String(etag),
		VersionId: aws.String(versionID),
	}, nil
}

func (b *InMemoryBackend) AbortMultipartUpload(
	_ context.Context,
	input *s3.AbortMultipartUploadInput,
) (*s3.AbortMultipartUploadOutput, error) {
	uploadID := *input.UploadId

	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.uploads[uploadID]; !exists {
		return nil, ErrNoSuchUpload
	}

	delete(b.uploads, uploadID)

	return &s3.AbortMultipartUploadOutput{}, nil
}
