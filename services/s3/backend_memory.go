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

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

const maxInt32 = 2147483647
const defaultRegionName = config.DefaultRegion

// objectChecksums holds the optional checksum values supplied with a PutObject request.
type objectChecksums struct {
	crc32, crc32c, sha1, sha256 *string
}

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
	buckets             map[string]map[string]*StoredBucket
	bucketIndex         map[string]string // name → region for O(1) cross-region lookup
	tags                map[string][]types.Tag
	uploads             map[string]map[string]*StoredMultipartUpload // bucket → uploadID → upload
	mu                  *lockmetrics.RWMutex
	compressor          Compressor
	defaultRegion       string
	compressionMinBytes int
}

func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	return &InMemoryBackend{
		buckets:       make(map[string]map[string]*StoredBucket),
		bucketIndex:   make(map[string]string),
		compressor:    compressor,
		defaultRegion: defaultRegionName,
		mu:            lockmetrics.New("s3"),
	}
}

// WithCompressionMinBytes sets the minimum object size (in bytes) below which
// gzip compression is skipped. A value of 0 compresses all objects regardless
// of size (the original behaviour). Negative values are clamped to 0 to
// prevent misconfiguration (e.g., via env/flags) from silently changing semantics.
func (b *InMemoryBackend) WithCompressionMinBytes(n int) *InMemoryBackend {
	if n < 0 {
		n = 0
	}

	b.compressionMinBytes = n

	return b
}

// getBucket returns the bucket for a given name, returning ErrNoSuchBucket when the
// bucket does not exist or is pending async deletion. The caller must hold at least b.mu.RLock.
// bucketIndex provides O(1) name→region resolution, so no region scan is needed.
func (b *InMemoryBackend) getBucket(name string) (*StoredBucket, error) {
	region, ok := b.bucketIndex[name]
	if !ok {
		return nil, ErrNoSuchBucket
	}

	bucket := b.buckets[region][name]
	if bucket == nil || bucket.DeletePending {
		return nil, ErrNoSuchBucket
	}

	return bucket, nil
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

	// Use bucketIndex for O(1) global-uniqueness check. Since this is a
	// single-tenant mock, a pre-existing bucket is always owned by the
	// caller → return BucketAlreadyOwnedByYou (not BucketAlreadyExists).
	// Pending-delete buckets remain in the index and still block re-creation,
	// matching real S3 behaviour (you must wait for deletion to complete).
	if _, exists := b.bucketIndex[bucketName]; exists {
		return nil, ErrBucketAlreadyOwnedByYou
	}

	// Initialize region map if it doesn't exist
	if _, exists := b.buckets[region]; !exists {
		b.buckets[region] = make(map[string]*StoredBucket)
	}

	b.buckets[region][bucketName] = &StoredBucket{
		Name:         bucketName,
		CreationDate: time.Now().UTC(),
		Objects:      make(map[string]*StoredObject),
		Versioning:   types.BucketVersioningStatusSuspended,
		mu:           lockmetrics.New("s3.bucket." + bucketName),
	}
	b.bucketIndex[bucketName] = region

	return &s3.CreateBucketOutput{
		Location: aws.String("/" + bucketName),
	}, nil
}

func (b *InMemoryBackend) DeleteBucket(
	_ context.Context,
	input *s3.DeleteBucketInput,
) (*s3.DeleteBucketOutput, error) {
	bucketName := *input.Bucket

	b.mu.Lock("DeleteBucket")
	defer b.mu.Unlock()

	// Use bucketIndex for O(1) lookup. Pending buckets remain in the index
	// so that idempotent deletes can be detected without a region scan.
	region, ok := b.bucketIndex[bucketName]
	if !ok {
		return nil, ErrNoSuchBucket
	}

	bucket := b.buckets[region][bucketName]
	if bucket == nil {
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
	_ context.Context,
	input *s3.HeadBucketInput,
) (*s3.HeadBucketOutput, error) {
	b.mu.RLock("HeadBucket")
	defer b.mu.RUnlock()

	if _, err := b.getBucket(*input.Bucket); err != nil {
		return nil, err
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

// Regions returns all distinct regions that contain at least one active bucket.
func (b *InMemoryBackend) Regions() []string {
	b.mu.RLock("Regions")
	defer b.mu.RUnlock()

	var regions []string

	for region, regionBuckets := range b.buckets {
		for _, bucket := range regionBuckets {
			if !bucket.DeletePending {
				regions = append(regions, region)

				break
			}
		}
	}

	sort.Strings(regions)

	return regions
}

// BucketsByRegion returns a snapshot of all Bucket SDK objects in the given region.
// Returns all buckets (cross-region) if region is empty.
func (b *InMemoryBackend) BucketsByRegion(region string) []types.Bucket {
	b.mu.RLock("BucketsByRegion")
	defer b.mu.RUnlock()

	var buckets []types.Bucket

	for r, regionBuckets := range b.buckets {
		if region != "" && r != region {
			continue
		}

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

	sort.Slice(buckets, func(i, j int) bool {
		return *buckets[i].Name < *buckets[j].Name
	})

	return buckets
}

func (b *InMemoryBackend) PutObject(
	ctx context.Context,
	input *s3.PutObjectInput,
) (*s3.PutObjectOutput, error) {
	bucketName := *input.Bucket
	key := *input.Key

	// 1. Prepare data and metadata outside the lock
	data, compressedData, isCompressed, etag, err := b.prepareObjectData(input)
	if err != nil {
		return nil, err
	}

	checksums := objectChecksums{
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
			mu:       lockmetrics.New("s3.object"),
		}
		bucket.Objects[key] = obj
	}
	bucket.mu.Unlock()

	// Now update the object versions under object lock (reduces contention on bucket)
	obj.mu.Lock("PutObject")
	defer obj.mu.Unlock()

	finalQuotedETag := "\"" + etag + "\""
	newVersion := &StoredObjectVersion{
		VersionID:          newVersionID,
		Key:                key,
		Data:               compressedData,
		IsCompressed:       isCompressed,
		Size:               int64(len(data)),
		ETag:               finalQuotedETag,
		LastModified:       time.Now().UTC(),
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

	logger.Load(ctx).DebugContext(ctx, "S3 Backend PutObject",
		"bucket", bucketName, "key", key,
		"contentType", aws.ToString(input.ContentType),
		"versionId", newVersionID)

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
	if b.compressor != nil && (b.compressionMinBytes == 0 || len(data) >= b.compressionMinBytes) {
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
	obj.mu.RLock("GetObject")
	defer obj.mu.RUnlock()

	var ver *StoredObjectVersion
	if versionID != nil && *versionID != "" {
		v, ok := obj.Versions[*versionID]
		if !ok {
			return nil, ErrNoSuchKey
		}
		ver = v
	} else {
		ver = findLatestVersion(obj.Versions)
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
	ctx context.Context,
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

	obj.mu.RLock("HeadObject")
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
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil, ErrNoSuchKey
	}

	logger.Load(ctx).DebugContext(ctx, "S3 Backend HeadObject",
		"bucket", bucketName, "key", key,
		"versionId", aws.ToString(versionID),
		"foundContentType", ver.ContentType)

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
	out, err := b.deleteObjectLocked(bucket, *input.Key, input.VersionId)
	bucket.mu.Unlock()

	if err != nil {
		return nil, err
	}

	// Clean up tags for the deleted version (not when a delete marker is added).
	if out.DeleteMarker == nil || !aws.ToBool(out.DeleteMarker) {
		vid := NullVersion
		if input.VersionId != nil && *input.VersionId != "" {
			vid = *input.VersionId
		}

		b.mu.Lock("DeleteObject.tags")
		if b.tags != nil {
			delete(b.tags, fmt.Sprintf("%s/%s/%s", bucketName, *input.Key, vid))
		}
		b.mu.Unlock()
	}

	return out, nil
}

// deleteObjectLocked performs a single-object deletion assuming bucket.mu is
// already held by the caller. It is used by both DeleteObject and DeleteObjects
// (which holds the lock for the entire batch to avoid per-object lock churn).
func (b *InMemoryBackend) deleteObjectLocked(
	bucket *StoredBucket,
	key string,
	versionID *string,
) (*s3.DeleteObjectOutput, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		// S3 spec: Delete on non-existent object is 204
		return &s3.DeleteObjectOutput{}, nil
	}

	if err := checkObjectLockForDelete(obj, versionID); err != nil {
		return nil, err
	}

	if versionID != nil && *versionID != "" {
		return deleteSpecificVersion(bucket, obj, key, versionID), nil
	}

	return deleteLatestVersion(bucket, obj, key), nil
}

// findLatestVersion returns the version with IsLatest set, or nil if none exists.
func findLatestVersion(versions map[string]*StoredObjectVersion) *StoredObjectVersion {
	for _, v := range versions {
		if v.IsLatest {
			return v
		}
	}

	return nil
}

// checkObjectLockForDelete returns ErrObjectLocked if the target version is under
// a legal hold or an active retention policy. Must be called with bucket.mu held.
func checkObjectLockForDelete(obj *StoredObject, versionID *string) error {
	var ver *StoredObjectVersion

	switch {
	case versionID != nil && *versionID != "":
		ver = obj.Versions[*versionID]
	case obj.LatestVersionID != "":
		ver = obj.Versions[obj.LatestVersionID]
	default:
		ver = findLatestVersion(obj.Versions)
	}

	if ver == nil || ver.Deleted {
		return nil
	}

	if ver.LegalHold {
		return ErrObjectLocked
	}

	if ver.RetentionMode != "" && !ver.RetainUntil.IsZero() && time.Now().Before(ver.RetainUntil) {
		return ErrObjectLocked
	}

	return nil
}

// deleteSpecificVersion removes the specified version from the object.
func deleteSpecificVersion(
	bucket *StoredBucket,
	obj *StoredObject,
	key string,
	versionID *string,
) *s3.DeleteObjectOutput {
	if _, ok := obj.Versions[*versionID]; ok {
		delete(obj.Versions, *versionID)
		if len(obj.Versions) == 0 {
			delete(bucket.Objects, key)
			obj.mu.Close()
		}

		return &s3.DeleteObjectOutput{VersionId: versionID}
	}

	return &s3.DeleteObjectOutput{}
}

// deleteLatestVersion deletes the latest version of an object (or marks it deleted if versioning is enabled).
func deleteLatestVersion(bucket *StoredBucket, obj *StoredObject, key string) *s3.DeleteObjectOutput {
	// Delete latest (Versioning enabled -> add delete marker, Suspended -> delete null version)
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		newVersionID := strconv.FormatInt(time.Now().UnixNano(), 10)

		// Create delete marker; mark others as not latest
		for _, v := range obj.Versions {
			v.IsLatest = false
		}
		deleteMarker := &StoredObjectVersion{
			VersionID:    newVersionID,
			Key:          key,
			Deleted:      true,
			IsLatest:     true,
			LastModified: time.Now().UTC(),
		}
		obj.Versions[newVersionID] = deleteMarker
		obj.LatestVersionID = newVersionID // Update cache

		return &s3.DeleteObjectOutput{
			DeleteMarker: aws.Bool(true),
			VersionId:    aws.String(newVersionID),
		}
	}

	// Suspended or null: Delete object (or null version)
	delete(bucket.Objects, key)
	obj.mu.Close()

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
	var tagKeysToDelete []string

	bucket.mu.Lock("DeleteObjects")
	for _, obj := range input.Delete.Objects {
		deleted, tagKey, delErr := b.deleteSingleObject(bucket, bucketName, obj)
		if delErr != nil {
			out.Errors = append(out.Errors, types.Error{
				Key:     obj.Key,
				Code:    aws.String("AccessDenied"),
				Message: aws.String(delErr.Error()),
			})

			continue
		}

		if tagKey != "" {
			tagKeysToDelete = append(tagKeysToDelete, tagKey)
		}

		out.Deleted = append(out.Deleted, deleted)
	}
	bucket.mu.Unlock()

	// Clean up tags after the bucket lock is released.
	if len(tagKeysToDelete) > 0 {
		b.mu.Lock("DeleteObjects.tags")
		if b.tags != nil {
			for _, k := range tagKeysToDelete {
				delete(b.tags, k)
			}
		}
		b.mu.Unlock()
	}

	return out, nil
}

// deleteSingleObject deletes one object from the bucket and returns the deleted record,
// the tag key to clean up (if any), and any error. Must be called with bucket.mu held.
func (b *InMemoryBackend) deleteSingleObject(
	bucket *StoredBucket,
	bucketName string,
	obj types.ObjectIdentifier,
) (types.DeletedObject, string, error) {
	delOut, delErr := b.deleteObjectLocked(bucket, aws.ToString(obj.Key), obj.VersionId)
	if delErr != nil {
		return types.DeletedObject{}, "", delErr
	}

	tagKey := ""
	if delOut.DeleteMarker == nil || !aws.ToBool(delOut.DeleteMarker) {
		vid := NullVersion
		if obj.VersionId != nil && *obj.VersionId != "" {
			vid = *obj.VersionId
		}
		tagKey = fmt.Sprintf("%s/%s/%s", bucketName, aws.ToString(obj.Key), vid)
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

	return deleted, tagKey, nil
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
		obj.mu.RLock("ListObjects")
		latestID := obj.LatestVersionID
		var latest *StoredObjectVersion
		if latestID != "" {
			latest = obj.Versions[latestID]
		} else {
			// Fallback: scan for latest if not cached
			latest = findLatestVersion(obj.Versions)
		}
		obj.mu.RUnlock()

		if latest == nil || latest.Deleted {
			continue
		}

		var checksumAlgos []types.ChecksumAlgorithm
		if latest.ChecksumAlgorithm != "" {
			checksumAlgos = []types.ChecksumAlgorithm{latest.ChecksumAlgorithm}
		}

		contents = append(contents, types.Object{
			Key:               aws.String(latest.Key),
			LastModified:      aws.Time(latest.LastModified),
			ETag:              aws.String(latest.ETag),
			Size:              aws.Int64(latest.Size),
			StorageClass:      types.ObjectStorageClassStandard,
			ChecksumAlgorithm: checksumAlgos,
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

	prefix := aws.ToString(input.Prefix)

	// Snapshot version metadata under lock to minimise lock hold time.
	type versionSnapshot struct {
		lastModified time.Time
		key          string
		versionID    string
		etag         string
		size         int64
		isLatest     bool
		deleted      bool
	}

	var snapshots []versionSnapshot

	bucket.mu.RLock("ListObjectVersions")
	for _, obj := range bucket.Objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		for _, v := range obj.Versions {
			snapshots = append(snapshots, versionSnapshot{
				key:          v.Key,
				versionID:    v.VersionID,
				etag:         v.ETag,
				lastModified: v.LastModified,
				size:         v.Size,
				isLatest:     v.IsLatest,
				deleted:      v.Deleted,
			})
		}
	}
	bucket.mu.RUnlock()

	// Build the output outside the lock.
	var versions []types.ObjectVersion
	var deleteMarkers []types.DeleteMarkerEntry

	for _, snap := range snapshots {
		if snap.deleted {
			deleteMarkers = append(deleteMarkers, types.DeleteMarkerEntry{
				Key:          aws.String(snap.key),
				VersionId:    aws.String(snap.versionID),
				IsLatest:     aws.Bool(snap.isLatest),
				LastModified: aws.Time(snap.lastModified),
				Owner: &types.Owner{
					ID:          aws.String("gopherstack"),
					DisplayName: aws.String("gopherstack"),
				},
			})
		} else {
			versions = append(versions, types.ObjectVersion{
				Key:          aws.String(snap.key),
				VersionId:    aws.String(snap.versionID),
				IsLatest:     aws.Bool(snap.isLatest),
				LastModified: aws.Time(snap.lastModified),
				ETag:         aws.String(snap.etag),
				Size:         aws.Int64(snap.size),
				StorageClass: types.ObjectVersionStorageClassStandard,
				Owner: &types.Owner{
					ID:          aws.String("gopherstack"),
					DisplayName: aws.String("gopherstack"),
				},
			})
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
		b.uploads = make(map[string]map[string]*StoredMultipartUpload)
	}

	if b.uploads[bucketName] == nil {
		b.uploads[bucketName] = make(map[string]*StoredMultipartUpload)
	}

	b.uploads[bucketName][uploadID] = &StoredMultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucketName,
		Key:       key,
		Parts:     make(map[int32]*StoredPart),
		Initiated: time.Now().UTC(),
		mu:        lockmetrics.New("s3.upload"),
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
	bucketName := aws.ToString(input.Bucket)

	// 1. Read data outside the lock
	data, err := io.ReadAll(input.Body)
	if err != nil {
		return nil, err
	}

	//nolint:gosec // MD5 required
	hash := md5.Sum(data)
	etag := fmt.Sprintf("%q", hex.EncodeToString(hash[:]))

	// 2. Find the upload using a read lock on the global map
	b.mu.RLock("UploadPart")
	upload := b.uploads[bucketName][uploadID] // reading nil map returns nil safely
	b.mu.RUnlock()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	// 3. Update the upload's part map under the per-upload lock only
	upload.mu.Lock("UploadPart")
	upload.Parts[partNumber] = &StoredPart{
		PartNumber: partNumber,
		ETag:       etag,
		Size:       int64(len(data)),
		Data:       data,
	}
	upload.mu.Unlock()

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
	upload := b.uploads[bucketName][uploadID] // reading nil map returns nil safely
	b.mu.RUnlock()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	// 2. Reassemble and compress data
	assembled, err := b.assembleMultipartData(upload, input)
	if err != nil {
		return nil, err
	}

	// 3. Update bucket/object state
	b.mu.RLock("CompleteMultipartUpload")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	versionID := b.commitMultipartObject(bucket, key, assembled)

	// 4. Cleanup upload — must happen after releasing bucket lock to avoid lock ordering issues.
	b.mu.Lock("CompleteMultipartUpload")
	delete(b.uploads[bucketName], uploadID)
	b.mu.Unlock()

	return &s3.CompleteMultipartUploadOutput{
		Bucket:    input.Bucket,
		Key:       input.Key,
		ETag:      aws.String(assembled.etag),
		VersionId: aws.String(versionID),
	}, nil
}

// multipartAssemblyResult holds the results of assembleMultipartData.
type multipartAssemblyResult struct {
	etag           string
	data           []byte
	compressedData []byte
	isCompressed   bool
}

// assembleMultipartData reads all parts under the per-upload read lock, assembles
// the combined payload, compresses it, and returns the assembled result.
func (b *InMemoryBackend) assembleMultipartData(
	upload *StoredMultipartUpload,
	input *s3.CompleteMultipartUploadInput,
) (multipartAssemblyResult, error) {
	var data []byte

	upload.mu.RLock("CompleteMultipartUpload")
	for _, part := range input.MultipartUpload.Parts {
		pNum := *part.PartNumber
		storedPart, ok := upload.Parts[pNum]
		if !ok {
			upload.mu.RUnlock()

			return multipartAssemblyResult{}, ErrInvalidPart
		}
		if *part.ETag != storedPart.ETag {
			upload.mu.RUnlock()

			return multipartAssemblyResult{}, ErrInvalidPart
		}
		data = append(data, storedPart.Data...)
	}
	upload.mu.RUnlock()

	var compressedData []byte
	var isCompressed bool

	if b.compressor != nil && (b.compressionMinBytes == 0 || len(data) >= b.compressionMinBytes) {
		var compErr error
		compressedData, compErr = b.compressor.Compress(data)
		if compErr != nil {
			return multipartAssemblyResult{}, compErr
		}
		isCompressed = true
	} else {
		compressedData = data
	}

	hash := md5.Sum(data) //nolint:gosec // MD5 required
	etag := fmt.Sprintf("%q", hex.EncodeToString(hash[:]))

	return multipartAssemblyResult{
		data:           data,
		compressedData: compressedData,
		etag:           etag,
		isCompressed:   isCompressed,
	}, nil
}

// commitMultipartObject stores the assembled multipart data as an object version,
// returning the new versionID. Acquires and releases bucket.mu internally.
func (b *InMemoryBackend) commitMultipartObject(
	bucket *StoredBucket,
	key string,
	assembled multipartAssemblyResult,
) string {
	bucket.mu.Lock("CompleteMultipartUpload")

	obj, exists := bucket.Objects[key]
	if !exists {
		obj = &StoredObject{Key: key, Versions: make(map[string]*StoredObjectVersion), mu: lockmetrics.New("s3.object")}
		bucket.Objects[key] = obj
	}

	versionID := "null"
	if bucket.Versioning == types.BucketVersioningStatusEnabled {
		versionID = strconv.FormatInt(time.Now().UnixNano(), 10)
	}

	newVersion := &StoredObjectVersion{
		VersionID:    versionID,
		Key:          key,
		Data:         assembled.compressedData,
		IsCompressed: assembled.isCompressed,
		Size:         int64(len(assembled.data)),
		ETag:         assembled.etag,
		LastModified: time.Now(),
		IsLatest:     true,
	}

	for _, v := range obj.Versions {
		v.IsLatest = false
	}
	obj.Versions[versionID] = newVersion
	obj.LatestVersionID = versionID

	bucket.mu.Unlock()

	return versionID
}

func (b *InMemoryBackend) AbortMultipartUpload(
	_ context.Context,
	input *s3.AbortMultipartUploadInput,
) (*s3.AbortMultipartUploadOutput, error) {
	uploadID := *input.UploadId
	bucketName := aws.ToString(input.Bucket)

	b.mu.Lock("AbortMultipartUpload")
	defer b.mu.Unlock()

	if b.uploads[bucketName] == nil || b.uploads[bucketName][uploadID] == nil {
		return nil, ErrNoSuchUpload
	}

	delete(b.uploads[bucketName], uploadID)

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

	for _, u := range b.uploads[bucketName] {
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
	bucketName := aws.ToString(input.Bucket)

	b.mu.RLock("ListParts")
	upload := b.uploads[bucketName][uploadID] // reading nil map returns nil safely
	b.mu.RUnlock()

	if upload == nil {
		return nil, ErrNoSuchUpload
	}

	upload.mu.RLock("ListParts")
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
	upload.mu.RUnlock()

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

// PutBucketLifecycleConfiguration stores the lifecycle configuration for a bucket.
func (b *InMemoryBackend) PutBucketLifecycleConfiguration(_ context.Context, bucketName, lifecycleXML string) error {
	b.mu.RLock("PutBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketLifecycleConfiguration")
	defer bucket.mu.Unlock()

	bucket.LifecycleConfig = lifecycleXML

	return nil
}

// GetBucketLifecycleConfiguration returns the lifecycle configuration for a bucket.
func (b *InMemoryBackend) GetBucketLifecycleConfiguration(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLifecycleConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.LifecycleConfig == "" {
		return "", ErrNoLifecycleConfig
	}

	return bucket.LifecycleConfig, nil
}

// DeleteBucketLifecycleConfiguration clears the lifecycle configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketLifecycleConfiguration(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketLifecycleConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketLifecycleConfiguration")
	defer bucket.mu.Unlock()

	bucket.LifecycleConfig = ""

	return nil
}

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

// PutBucketEncryption stores the server-side encryption configuration for a bucket.
func (b *InMemoryBackend) PutBucketEncryption(_ context.Context, bucketName, encryptionXML string) error {
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
func (b *InMemoryBackend) GetBucketEncryption(_ context.Context, bucketName string) (string, error) {
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

// PutPublicAccessBlock stores the public access block configuration for a bucket.
func (b *InMemoryBackend) PutPublicAccessBlock(_ context.Context, bucketName, configXML string) error {
	b.mu.RLock("PutPublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutPublicAccessBlock")
	defer bucket.mu.Unlock()

	bucket.PublicAccessBlockConfig = configXML

	return nil
}

// GetPublicAccessBlock returns the public access block configuration for a bucket.
func (b *InMemoryBackend) GetPublicAccessBlock(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetPublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetPublicAccessBlock")
	defer bucket.mu.RUnlock()

	if bucket.PublicAccessBlockConfig == "" {
		return "", ErrNoPublicAccessBlock
	}

	return bucket.PublicAccessBlockConfig, nil
}

// DeletePublicAccessBlock removes the public access block configuration for a bucket.
func (b *InMemoryBackend) DeletePublicAccessBlock(_ context.Context, bucketName string) error {
	b.mu.RLock("DeletePublicAccessBlock")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeletePublicAccessBlock")
	defer bucket.mu.Unlock()

	bucket.PublicAccessBlockConfig = ""

	return nil
}

// PutBucketOwnershipControls stores the ownership controls configuration for a bucket.
func (b *InMemoryBackend) PutBucketOwnershipControls(_ context.Context, bucketName, configXML string) error {
	b.mu.RLock("PutBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketOwnershipControls")
	defer bucket.mu.Unlock()

	bucket.OwnershipControlsConfig = configXML

	return nil
}

// GetBucketOwnershipControls returns the ownership controls configuration for a bucket.
func (b *InMemoryBackend) GetBucketOwnershipControls(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketOwnershipControls")
	defer bucket.mu.RUnlock()

	if bucket.OwnershipControlsConfig == "" {
		return "", ErrNoOwnershipControls
	}

	return bucket.OwnershipControlsConfig, nil
}

// DeleteBucketOwnershipControls removes the ownership controls configuration for a bucket.
func (b *InMemoryBackend) DeleteBucketOwnershipControls(_ context.Context, bucketName string) error {
	b.mu.RLock("DeleteBucketOwnershipControls")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("DeleteBucketOwnershipControls")
	defer bucket.mu.Unlock()

	bucket.OwnershipControlsConfig = ""

	return nil
}

// PutBucketLogging stores the logging configuration for a bucket.
func (b *InMemoryBackend) PutBucketLogging(_ context.Context, bucketName, loggingXML string) error {
	b.mu.RLock("PutBucketLogging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketLogging")
	defer bucket.mu.Unlock()

	bucket.LoggingConfig = loggingXML

	return nil
}

// GetBucketLogging returns the logging configuration for a bucket.
// Returns "" (empty string) when no logging is configured; the handler
// synthesizes the AWS-compatible empty XML response.
func (b *InMemoryBackend) GetBucketLogging(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketLogging")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketLogging")
	defer bucket.mu.RUnlock()

	return bucket.LoggingConfig, nil
}

// PutBucketReplication stores the replication configuration for a bucket.
func (b *InMemoryBackend) PutBucketReplication(_ context.Context, bucketName, replicationXML string) error {
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
func (b *InMemoryBackend) GetBucketReplication(_ context.Context, bucketName string) (string, error) {
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
func (b *InMemoryBackend) PutBucketNotificationConfiguration(_ context.Context, bucketName, notifXML string) error {
	b.mu.RLock("PutBucketNotificationConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketNotificationConfiguration")
	defer bucket.mu.Unlock()

	bucket.NotificationConfig = notifXML

	return nil
}

// GetBucketNotificationConfiguration returns the notification configuration for a bucket.
func (b *InMemoryBackend) GetBucketNotificationConfiguration(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketNotificationConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketNotificationConfiguration")
	defer bucket.mu.RUnlock()

	// Notification config is always returned, even if empty (AWS returns empty XML)
	return bucket.NotificationConfig, nil
}

// PutObjectLockConfiguration stores the object lock configuration for a bucket.
func (b *InMemoryBackend) PutObjectLockConfiguration(_ context.Context, bucketName, configXML string) error {
	b.mu.RLock("PutObjectLockConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectLockConfiguration")
	defer bucket.mu.Unlock()

	bucket.ObjectLockConfig = configXML

	return nil
}

// GetObjectLockConfiguration retrieves the object lock configuration for a bucket.
func (b *InMemoryBackend) GetObjectLockConfiguration(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetObjectLockConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetObjectLockConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.ObjectLockConfig == "" {
		return "", ErrNoObjectLockConfig
	}

	return bucket.ObjectLockConfig, nil
}

// PutObjectRetention sets the retention mode and retain-until-date for a specific object version.
func (b *InMemoryBackend) PutObjectRetention(
	_ context.Context,
	bucketName, key string,
	versionID *string,
	mode string,
	retainUntil time.Time,
) error {
	b.mu.RLock("PutObjectRetention")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectRetention")
	defer bucket.mu.Unlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return findErr
	}

	ver.RetentionMode = mode
	ver.RetainUntil = retainUntil

	return nil
}

// GetObjectRetention returns the retention mode and retain-until-date for a specific object version.
func (b *InMemoryBackend) GetObjectRetention(
	_ context.Context,
	bucketName, key string,
	versionID *string,
) (string, time.Time, error) {
	b.mu.RLock("GetObjectRetention")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", time.Time{}, err
	}

	bucket.mu.RLock("GetObjectRetention")
	defer bucket.mu.RUnlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return "", time.Time{}, findErr
	}

	if ver.RetentionMode == "" {
		return "", time.Time{}, ErrNoSuchObjectLockConfig
	}

	return ver.RetentionMode, ver.RetainUntil, nil
}

// PutObjectLegalHold sets or clears the legal hold status for a specific object version.
func (b *InMemoryBackend) PutObjectLegalHold(
	_ context.Context,
	bucketName, key string,
	versionID *string,
	status string,
) error {
	b.mu.RLock("PutObjectLegalHold")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutObjectLegalHold")
	defer bucket.mu.Unlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return findErr
	}

	ver.LegalHold = status == "ON"

	return nil
}

// GetObjectLegalHold returns the legal hold status for a specific object version.
func (b *InMemoryBackend) GetObjectLegalHold(
	_ context.Context,
	bucketName, key string,
	versionID *string,
) (string, error) {
	b.mu.RLock("GetObjectLegalHold")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetObjectLegalHold")
	defer bucket.mu.RUnlock()

	ver, findErr := findObjectVersion(bucket, key, versionID)
	if findErr != nil {
		return "", findErr
	}

	if ver.LegalHold {
		return "ON", nil
	}

	return "OFF", nil
}

// findObjectVersion returns the specified object version (or the latest version if versionID is nil).
// Must be called with at least a read lock on bucket.mu.
func findObjectVersion(bucket *StoredBucket, key string, versionID *string) (*StoredObjectVersion, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	if versionID != nil && *versionID != "" {
		ver, ok := obj.Versions[*versionID]
		if !ok || ver.Deleted {
			return nil, ErrNoSuchKey
		}

		return ver, nil
	}

	// Find latest
	if obj.LatestVersionID != "" {
		ver := obj.Versions[obj.LatestVersionID]
		if ver != nil && !ver.Deleted {
			return ver, nil
		}
	}

	for _, ver := range obj.Versions {
		if ver.IsLatest && !ver.Deleted {
			return ver, nil
		}
	}

	return nil, ErrNoSuchKey
}
