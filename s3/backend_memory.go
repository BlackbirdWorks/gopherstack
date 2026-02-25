package s3

import (
	"bytes"
	"context"
	"crypto/md5" //nolint:gosec // MD5 required for S3 ETag compatibility
	"encoding/hex"
	"fmt"
	"io"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const maxInt32 = 2147483647
const defaultRegionName = "us-east-1"

var _ StorageBackend = (*InMemoryBackend)(nil)

// getRegionFromS3Context extracts the region from S3 request context.
// Returns the default region if region is not found in context.
func getRegionFromS3Context(ctx context.Context, defaultRegion string) string {
	if region, ok := ctx.Value(regionContextKey{}).(string); ok && region != "" {
		return region
	}

	return defaultRegion
}

type InMemoryBackend struct {
	compressor    Compressor
	buckets       map[string]map[string]*StoredBucket
	tags          map[string][]types.Tag
	uploads       map[string]*StoredMultipartUpload
	mu            *lockmetrics.RWMutex
	defaultRegion string
}

func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	return &InMemoryBackend{
		buckets:       make(map[string]map[string]*StoredBucket),
		compressor:    compressor,
		defaultRegion: defaultRegionName,
		mu:            lockmetrics.New("s3"),
	}
}

// getBucket returns the bucket for a given name, returning ErrNoSuchBucket when the
// bucket does not exist or is pending async deletion. The caller must hold at least b.mu.RLock.
// Since bucket names are globally unique, it searches across all regions.
func (b *InMemoryBackend) getBucket(name string) (*StoredBucket, error) {
	// Search across all regions — bucket names are globally unique so there
	// is at most one match. This handles the case where a bucket was created
	// via a request with a non-default region (e.g. us-west-2) so it is
	// stored under that region key but subsequent SDK calls (PutObject, etc.)
	// must still be able to find it.
	for _, regionBuckets := range b.buckets {
		if bucket, exists := regionBuckets[name]; exists && !bucket.DeletePending {
			return bucket, nil
		}
	}

	return nil, ErrNoSuchBucket
}

// SetDefaultRegion sets the default region for this backend.
func (b *InMemoryBackend) SetDefaultRegion(region string) {
	if region == "" {
		region = defaultRegionName
	}
	b.defaultRegion = region
}

func (b *InMemoryBackend) CreateBucket(
	ctx context.Context,
	input *s3.CreateBucketInput,
) (*s3.CreateBucketOutput, error) {
	// Prefer the LocationConstraint from the input (set by the SDK for non-us-east-1
	// regions) over the region extracted from the context, so the bucket is stored
	// in the region the caller actually requested.
	region := getRegionFromS3Context(ctx, b.defaultRegion)
	if input.CreateBucketConfiguration != nil &&
		input.CreateBucketConfiguration.LocationConstraint != "" {
		region = string(input.CreateBucketConfiguration.LocationConstraint)
	}

	b.mu.Lock("CreateBucket")
	defer b.mu.Unlock()

	bucketName := *input.Bucket

	// Check across all regions for global uniqueness.
	// Since this is a single-tenant mock, a pre-existing bucket is always
	// owned by the caller → return BucketAlreadyOwnedByYou (not BucketAlreadyExists).
	for _, regionBuckets := range b.buckets {
		if _, exists := regionBuckets[bucketName]; exists {
			return nil, ErrBucketAlreadyOwnedByYou
		}
	}

	// Initialize region map if it doesn't exist
	if _, exists := b.buckets[region]; !exists {
		b.buckets[region] = make(map[string]*StoredBucket)
	}

	b.buckets[region][bucketName] = &StoredBucket{
		Name:         bucketName,
		CreationDate: time.Now(),
		Objects:      make(map[string]*StoredObject),
		Versioning:   types.BucketVersioningStatusSuspended,
		mu:           lockmetrics.New("s3.bucket." + bucketName),
	}

	return &s3.CreateBucketOutput{
		Location: aws.String("/" + bucketName),
	}, nil
}

func (b *InMemoryBackend) DeleteBucket(
	ctx context.Context,
	input *s3.DeleteBucketInput,
) (*s3.DeleteBucketOutput, error) {
	region := getRegionFromS3Context(ctx, b.defaultRegion)
	bucketName := *input.Bucket

	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	if _, exists := b.buckets[region]; !exists {
		return nil, ErrNoSuchBucket
	}

	bucket, exists := b.buckets[region][bucketName]
	if !exists {
		return nil, ErrNoSuchBucket
	}

	// If already pending deletion, return success (idempotent).
	if bucket.DeletePending {
		return &s3.DeleteBucketOutput{}, nil
	}

	// Mark bucket as pending — the Janitor will drain its objects and remove it.
	bucket.DeletePending = true

	return &s3.DeleteBucketOutput{}, nil
}

func (b *InMemoryBackend) HeadBucket(
	ctx context.Context,
	input *s3.HeadBucketInput,
) (*s3.HeadBucketOutput, error) {
	region := getRegionFromS3Context(ctx, b.defaultRegion)

	b.mu.RLock("HeadBucket")
	defer b.mu.RUnlock()

	bucketName := *input.Bucket

	if _, exists := b.buckets[region]; !exists {
		return nil, ErrNoSuchBucket
	}

	bucket, exists := b.buckets[region][bucketName]
	if !exists || bucket.DeletePending {
		return nil, ErrNoSuchBucket
	}

	return &s3.HeadBucketOutput{}, nil
}

func (b *InMemoryBackend) ListBuckets(
	_ context.Context,
	_ *s3.ListBucketsInput,
) (*s3.ListBucketsOutput, error) {
	// Snapshot bucket data under lock across all regions, release immediately
	b.mu.RLock("ListBuckets")
	buckets := make([]types.Bucket, 0)
	for _, regionBuckets := range b.buckets {
		for _, bucket := range regionBuckets {
			if bucket.DeletePending {
				continue
			}
			buckets = append(buckets, types.Bucket{
				Name:         aws.String(bucket.Name),
				CreationDate: aws.Time(bucket.CreationDate),
			})
		}
	}
	b.mu.RUnlock()

	// Sort outside the lock
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

func (b *InMemoryBackend) PutObject(
	_ context.Context,
	input *s3.PutObjectInput,
) (*s3.PutObjectOutput, error) {
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
	b.mu.RLock("PutObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	// First, get or create the object under bucket lock (brief)
	var obj *StoredObject
	var newVersionID string

	bucket.mu.Lock("PutObject")
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
		VersionID:          newVersionID,
		Key:                key,
		Data:               compressedData,
		IsCompressed:       isCompressed,
		Size:               int64(len(data)),
		ETag:               finalQuotedETag,
		LastModified:       time.Now(),
		ContentType:        aws.ToString(input.ContentType),
		ContentEncoding:    aws.ToString(input.ContentEncoding),
		ContentDisposition: aws.ToString(input.ContentDisposition),
		Metadata:           input.Metadata,
		ChecksumCRC32:      checksums.crc32,
		ChecksumCRC32C:     checksums.crc32c,
		ChecksumSHA1:       checksums.sha1,
		ChecksumSHA256:     checksums.sha256,
		ChecksumAlgorithm:  input.ChecksumAlgorithm,
		IsLatest:           true,
	}

	for _, v := range obj.Versions {
		if v.IsLatest {
			v.IsLatest = false
		}
	}

	obj.Versions[newVersionID] = newVersion
	obj.LatestVersionID = newVersionID // Update the cached latest version ID

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

	b.mu.Lock("PutObject.tags")
	defer b.mu.Unlock()

	if b.tags == nil {
		b.tags = make(map[string][]types.Tag)
	}

	tagKey := fmt.Sprintf("%s/%s/%s", bucket, key, versionID)
	b.tags[tagKey] = tagList
}

func (b *InMemoryBackend) GetObject(
	_ context.Context,
	input *s3.GetObjectInput,
) (*s3.GetObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("GetObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetObject")
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
	contentEncoding := ver.ContentEncoding
	contentDisposition := ver.ContentDisposition
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
		Body:               io.NopCloser(bytes.NewReader(data)),
		ContentLength:      aws.Int64(size),
		ContentType:        aws.String(contentType),
		ContentEncoding:    nilStringIfEmpty(contentEncoding),
		ContentDisposition: nilStringIfEmpty(contentDisposition),
		ETag:               aws.String(etag),
		LastModified:       aws.Time(lastModified),
		Metadata:           metadata,
		VersionId:          aws.String(versionIDStr),
		ChecksumCRC32:      checksumCRC32,
		ChecksumCRC32C:     checksumCRC32C,
		ChecksumSHA1:       checksumSHA1,
		ChecksumSHA256:     checksumSHA256,
	}, nil
}

func (b *InMemoryBackend) HeadObject(
	_ context.Context,
	input *s3.HeadObjectInput,
) (*s3.HeadObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key
	versionID := input.VersionId

	b.mu.RLock("HeadObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("HeadObject")
	obj, exists := bucket.Objects[key]
	bucket.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchKey
	}

	obj.mu.RLock()
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion
	if versionID != nil && *versionID != "" {
		// Use provided version ID
		v, ok := obj.Versions[*versionID]
		if !ok {
			return nil, ErrNoSuchKey
		}
		ver = v
	} else if latestID := obj.LatestVersionID; latestID != "" {
		// Use cached latest version ID to avoid scanning all versions
		ver = obj.Versions[latestID]
	} else {
		// Fallback: scan for latest (shouldn't happen in normal operation)
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
		ContentLength:      aws.Int64(ver.Size),
		ContentType:        aws.String(ver.ContentType),
		ContentEncoding:    nilStringIfEmpty(ver.ContentEncoding),
		ContentDisposition: nilStringIfEmpty(ver.ContentDisposition),
		ETag:               aws.String(ver.ETag),
		LastModified:       aws.Time(ver.LastModified),
		Metadata:           ver.Metadata,
		VersionId:          aws.String(ver.VersionID),
		ChecksumCRC32:      ver.ChecksumCRC32,
		ChecksumCRC32C:     ver.ChecksumCRC32C,
		ChecksumSHA1:       ver.ChecksumSHA1,
		ChecksumSHA256:     ver.ChecksumSHA256,
	}, nil
}

func (b *InMemoryBackend) DeleteObject(
	_ context.Context,
	input *s3.DeleteObjectInput,
) (*s3.DeleteObjectOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("DeleteObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.Lock("DeleteObject")
	defer bucket.mu.Unlock()

	return b.deleteObjectLocked(bucket, *input.Key, input.VersionId), nil
}

// deleteObjectLocked performs a single-object deletion assuming bucket.mu is
// already held by the caller. It is used by both DeleteObject and DeleteObjects
// (which holds the lock for the entire batch to avoid per-object lock churn).
func (b *InMemoryBackend) deleteObjectLocked(
	bucket *StoredBucket,
	key string,
	versionID *string,
) *s3.DeleteObjectOutput {
	obj, exists := bucket.Objects[key]
	if !exists {
		// S3 spec: Delete on non-existent object is 204
		return &s3.DeleteObjectOutput{}
	}

	if versionID != nil && *versionID != "" {
		if _, ok := obj.Versions[*versionID]; ok {
			delete(obj.Versions, *versionID)
			if len(obj.Versions) == 0 {
				delete(bucket.Objects, key)
			}

			return &s3.DeleteObjectOutput{VersionId: versionID}
		}

		return &s3.DeleteObjectOutput{}
	}

	// Delete latest (Versioning enabled -> add delete marker, Suspended -> delete null version)
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		newVersionID := strconv.FormatInt(time.Now().UnixNano(), 10)

		// Create delete marker
		// Mark others as not latest
		for _, v := range obj.Versions {
			v.IsLatest = false
		}
		deleteMarker := &StoredObjectVersion{
			VersionID:    newVersionID,
			Key:          key,
			Deleted:      true,
			IsLatest:     true,
			LastModified: time.Now(),
		}
		obj.Versions[newVersionID] = deleteMarker
		obj.LatestVersionID = newVersionID // Update cache

		return &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(true),
			VersionId:    aws.String(newVersionID),
		}
	}

	// Suspended or null: Delete object (or null version)
	// Simple remove for now
	delete(bucket.Objects, key)

	return &s3.DeleteObjectOutput{}
}

func (b *InMemoryBackend) DeleteObjects(
	_ context.Context,
	input *s3.DeleteObjectsInput,
) (*s3.DeleteObjectsOutput, error) {
	out := &s3.DeleteObjectsOutput{
		Deleted: make([]types.DeletedObject, 0, len(input.Delete.Objects)),
		Errors:  make([]types.Error, 0),
	}

	bucketName := *input.Bucket

	b.mu.RLock("DeleteObjects")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		for _, obj := range input.Delete.Objects {
			out.Errors = append(out.Errors, types.Error{
				Key:     obj.Key,
				Code:    aws.String("NoSuchBucket"),
				Message: aws.String(ErrNoSuchBucket.Error()),
			})
		}

		return out, nil
	}

	// Hold the bucket lock for the entire batch to avoid per-object lock churn
	// when deleting thousands of objects.
	bucket.mu.Lock("DeleteObjects")
	for _, obj := range input.Delete.Objects {
		delOut := b.deleteObjectLocked(bucket, aws.ToString(obj.Key), obj.VersionId)

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
	bucket.mu.Unlock()

	return out, nil
}

func applyDelimiter(prefix, delimiter string, contents []types.Object) ([]types.Object, []types.CommonPrefix) {
	var filtered []types.Object
	var cpList []types.CommonPrefix
	seenPrefixes := make(map[string]struct{})

	for _, obj := range contents {
		key := aws.ToString(obj.Key)
		rest := key[len(prefix):]
		idx := strings.Index(rest, delimiter)

		if idx != -1 {
			cp := prefix + rest[:idx+len(delimiter)]
			if _, seen := seenPrefixes[cp]; !seen {
				seenPrefixes[cp] = struct{}{}
				cpList = append(cpList, types.CommonPrefix{Prefix: aws.String(cp)})
			}
		} else {
			filtered = append(filtered, obj)
		}
	}

	sort.Slice(cpList, func(i, j int) bool {
		return aws.ToString(cpList[i].Prefix) < aws.ToString(cpList[j].Prefix)
	})

	return filtered, cpList
}

func (b *InMemoryBackend) ListObjects(
	_ context.Context,
	input *s3.ListObjectsInput,
) (*s3.ListObjectsOutput, error) {
	bucketName := *input.Bucket

	b.mu.RLock("ListObjects")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	// Snapshot object pointers under lock
	bucket.mu.RLock("ListObjects")
	prefix := aws.ToString(input.Prefix)
	objectSnapshots := make([]*StoredObject, 0, len(bucket.Objects))
	for _, obj := range bucket.Objects {
		if strings.HasPrefix(obj.Key, prefix) {
			objectSnapshots = append(objectSnapshots, obj)
		}
	}
	bucket.mu.RUnlock()

	// Process objects outside the bucket lock
	var contents []types.Object
	for _, obj := range objectSnapshots {
		obj.mu.RLock()
		latestID := obj.LatestVersionID
		var latest *StoredObjectVersion
		if latestID != "" {
			latest = obj.Versions[latestID]
		} else {
			// Fallback: scan for latest if not cached
			for _, v := range obj.Versions {
				if v.IsLatest {
					latest = v

					break
				}
			}
		}
		obj.mu.RUnlock()

		if latest == nil || latest.Deleted {
			continue
		}

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

	sort.Slice(contents, func(i, j int) bool {
		return *contents[i].Key < *contents[j].Key
	})

	delimiter := aws.ToString(input.Delimiter)
	var cpList []types.CommonPrefix

	if delimiter != "" {
		contents, cpList = applyDelimiter(prefix, delimiter, contents)
	}

	return &s3.ListObjectsOutput{
		Name:           input.Bucket,
		Prefix:         input.Prefix,
		Delimiter:      input.Delimiter,
		MaxKeys:        input.MaxKeys,
		Contents:       contents,
		CommonPrefixes: cpList,
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

	b.mu.RLock("ListObjectVersions")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("ListObjectVersions")
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

	b.mu.Lock("PutObjectTagging")
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

// Multipart

func (b *InMemoryBackend) CreateMultipartUpload(
	_ context.Context,
	input *s3.CreateMultipartUploadInput,
) (*s3.CreateMultipartUploadOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	b.mu.RLock("CreateMultipartUpload")
	_, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	uploadID := strconv.FormatInt(time.Now().UnixNano(), 10)

	b.mu.Lock("CreateMultipartUpload")
	if b.uploads == nil {
		b.uploads = make(map[string]*StoredMultipartUpload)
	}

	b.uploads[uploadID] = &StoredMultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucketName,
		Key:       key,
		Parts:     make(map[int32]*StoredPart),
		Initiated: time.Now().UTC(),
	}
	b.mu.Unlock()

	return &s3.CreateMultipartUploadOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: aws.String(uploadID),
	}, nil
}

func (b *InMemoryBackend) UploadPart(
	_ context.Context,
	input *s3.UploadPartInput,
) (*s3.UploadPartOutput, error) {
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
	b.mu.Lock("UploadPart")
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
	b.mu.RLock("CompleteMultipartUpload")
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
	b.mu.RLock("CompleteMultipartUpload")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.Lock("CompleteMultipartUpload")
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
	obj.LatestVersionID = versionID // Update cache

	// Cleanup upload
	b.mu.Lock("CompleteMultipartUpload")
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

	b.mu.Lock("AbortMultipartUpload")
	defer b.mu.Unlock()

	if _, exists := b.uploads[uploadID]; !exists {
		return nil, ErrNoSuchUpload
	}

	delete(b.uploads, uploadID)

	return &s3.AbortMultipartUploadOutput{}, nil
}

// ListMultipartUploads returns in-progress multipart uploads for a bucket.
func (b *InMemoryBackend) ListMultipartUploads(
	_ context.Context,
	input *s3.ListMultipartUploadsInput,
) (*s3.ListMultipartUploadsOutput, error) {
	bucketName := aws.ToString(input.Bucket)

	b.mu.RLock("ListMultipartUploads")
	defer b.mu.RUnlock()

	if _, err := b.getBucket(bucketName); err != nil {
		return nil, err
	}

	prefix := aws.ToString(input.Prefix)
	var uploads []types.MultipartUpload

	for _, u := range b.uploads {
		if u.Bucket != bucketName {
			continue
		}

		if prefix != "" && !strings.HasPrefix(u.Key, prefix) {
			continue
		}

		uploads = append(uploads, types.MultipartUpload{
			Key:       aws.String(u.Key),
			UploadId:  aws.String(u.UploadID),
			Initiated: aws.Time(u.Initiated),
		})
	}

	sort.Slice(uploads, func(i, j int) bool {
		ki, kj := aws.ToString(uploads[i].Key), aws.ToString(uploads[j].Key)
		if ki != kj {
			return ki < kj
		}

		return aws.ToString(uploads[i].UploadId) < aws.ToString(uploads[j].UploadId)
	})

	return &s3.ListMultipartUploadsOutput{
		Bucket:  aws.String(bucketName),
		Uploads: uploads,
	}, nil
}

// ListParts returns the parts that have been uploaded for a specific multipart upload.
func (b *InMemoryBackend) ListParts(
	_ context.Context,
	input *s3.ListPartsInput,
) (*s3.ListPartsOutput, error) {
	uploadID := aws.ToString(input.UploadId)

	b.mu.RLock("ListParts")
	defer b.mu.RUnlock()

	upload, exists := b.uploads[uploadID]
	if !exists {
		return nil, ErrNoSuchUpload
	}

	partNumbers := make([]int32, 0, len(upload.Parts))
	for pn := range upload.Parts {
		partNumbers = append(partNumbers, pn)
	}

	slices.Sort(partNumbers)

	var parts []types.Part
	for _, pn := range partNumbers {
		p := upload.Parts[pn]
		parts = append(parts, types.Part{
			PartNumber: aws.Int32(pn),
			ETag:       aws.String(p.ETag),
			Size:       aws.Int64(p.Size),
		})
	}

	return &s3.ListPartsOutput{
		Bucket:   input.Bucket,
		Key:      input.Key,
		UploadId: input.UploadId,
		Parts:    parts,
	}, nil
}

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
		acl = "private"
	}

	return acl, nil
}

// nilStringIfEmpty returns nil if s is empty, otherwise returns aws.String(s).
func nilStringIfEmpty(s string) *string {
	if s == "" {
		return nil
	}

	return aws.String(s)
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
