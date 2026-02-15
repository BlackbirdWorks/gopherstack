package s3

import (
	"context"
	"crypto/md5"  //nolint:gosec // MD5 required for S3 ETag
	"crypto/sha1" //nolint:gosec // SHA1 supported as per S3 spec
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"hash/crc32"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// InMemoryBackend is an in-memory implementation of StorageBackend.
type InMemoryBackend struct {
	compressor Compressor
	buckets    map[string]*Bucket
	mu         sync.RWMutex
}

const (
	crc32Len     = 4
	bitsInByte   = 8
	bitsInUint32 = 32
)

// NewInMemoryBackend creates a new InMemoryBackend using the given compressor.
func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	return &InMemoryBackend{
		buckets:    make(map[string]*Bucket),
		compressor: compressor,
	}
}

// CreateBucket creates a new bucket with the given name.
func (b *InMemoryBackend) CreateBucket(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[name]; exists {
		return ErrBucketAlreadyExists
	}

	b.buckets[name] = &Bucket{
		Name:          name,
		CreationDate:  time.Now(),
		Versioning:    VersioningSuspended,
		Objects:       make(map[string]*Object),
		ActiveUploads: make(map[string]*MultipartUpload),
	}

	return nil
}

// DeleteBucket deletes a bucket by name. Fails if not empty.
func (b *InMemoryBackend) DeleteBucket(_ context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	bucket, exists := b.buckets[name]
	if !exists {
		return ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if len(bucket.Objects) > 0 {
		return ErrBucketNotEmpty
	}

	delete(b.buckets, name)

	return nil
}

// PutBucketVersioning sets the versioning status for a bucket.
func (b *InMemoryBackend) PutBucketVersioning(_ context.Context, bucketName string, status VersioningStatus) error {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	bucket.Versioning = status

	return nil
}

// GetBucketVersioning retrieves the versioning status for a bucket.
func (b *InMemoryBackend) GetBucketVersioning(_ context.Context, bucketName string) (VersioningConfiguration, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return VersioningConfiguration{}, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	return VersioningConfiguration{
		Status: string(bucket.Versioning),
	}, nil
}

// HeadBucket returns a bucket by name.
func (b *InMemoryBackend) HeadBucket(_ context.Context, name string) (*Bucket, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bucket, exists := b.buckets[name]
	if !exists {
		return nil, ErrNoSuchBucket
	}

	return bucket, nil
}

// ListBuckets returns all buckets sorted by name.
func (b *InMemoryBackend) ListBuckets(_ context.Context) ([]*Bucket, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	buckets := make([]*Bucket, 0, len(b.buckets))
	for _, bkt := range b.buckets {
		buckets = append(buckets, bkt)
	}

	sort.Slice(buckets, func(i, j int) bool {
		return buckets[i].Name < buckets[j].Name
	})

	return buckets, nil
}

// PutObject stores an object, compressing the data.
func (b *InMemoryBackend) PutObject(
	_ context.Context,
	bucketName, key string,
	data []byte,
	meta ObjectMetadata,
) (*ObjectVersion, error) {
	// Perform expensive CPU work before acquiring the lock.
	compressedData, err := b.compressor.Compress(data)
	if err != nil {
		return nil, err
	}

	//nolint:gosec // MD5 is required for S3 ETag
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	checksumValue := meta.ChecksumValue
	if meta.ChecksumAlgorithm != "" && checksumValue == "" {
		checksumValue = CalculateChecksum(data, meta.ChecksumAlgorithm)
	}

	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	versionID := NullVersion
	if bucket.Versioning == VersioningEnabled {
		versionID = uuid.New().String()
	}

	newVersion := ObjectVersion{
		Key:               key,
		VersionID:         versionID,
		IsLatest:          true,
		Data:              compressedData,
		Size:              int64(len(data)),
		ETag:              fmt.Sprintf("\"%s\"", etag),
		ChecksumAlgorithm: meta.ChecksumAlgorithm,
		ChecksumValue:     checksumValue,
		ContentType:       meta.ContentType,
		UserMetadata:      meta.UserMetadata,
		LastModified:      time.Now(),
	}

	obj := b.getOrCreateObject(bucket, key)
	obj.ContentType = meta.ContentType
	obj.Tags = meta.Tags

	if bucket.Versioning == VersioningEnabled {
		b.appendVersioned(obj, newVersion)
	} else {
		b.replaceNullVersion(obj, newVersion)
	}

	obj.Size = newVersion.Size
	obj.LastModified = newVersion.LastModified

	return &newVersion, nil
}

func (b *InMemoryBackend) getOrCreateObject(bucket *Bucket, key string) *Object {
	obj, exists := bucket.Objects[key]
	if !exists {
		obj = &Object{
			Key:  key,
			Tags: make(map[string]string),
		}

		bucket.Objects[key] = obj
	}

	return obj
}

func (b *InMemoryBackend) appendVersioned(obj *Object, ver ObjectVersion) {
	for i := range obj.Versions {
		obj.Versions[i].IsLatest = false
	}

	obj.Versions = append(obj.Versions, ver)
}

func (b *InMemoryBackend) replaceNullVersion(obj *Object, ver ObjectVersion) {
	newVersions := make([]ObjectVersion, 0, len(obj.Versions))

	for _, v := range obj.Versions {
		if v.VersionID != NullVersion {
			newVersions = append(newVersions, v)
		}
	}

	newVersions = append(newVersions, ver)
	obj.Versions = newVersions
}

// GetObject retrieves an object version, decompressing the data.
func (b *InMemoryBackend) GetObject(_ context.Context, bucketName, key, versionID string) (*ObjectVersion, error) {
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

	v, err := b.findVersion(obj, versionID)
	if err != nil {
		return nil, err
	}

	return b.decompressVersion(v)
}

// HeadObject retrieves an object version's metadata without decompressing the data.
func (b *InMemoryBackend) HeadObject(_ context.Context, bucketName, key, versionID string) (*ObjectVersion, error) {
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

	var v *ObjectVersion
	var err error
	version, err := b.findVersion(obj, versionID)
	if err != nil {
		return nil, err
	}
	v = &version

	ret := *v
	ret.Data = nil // Metadata only

	return &ret, nil
}

func (b *InMemoryBackend) findVersion(obj *Object, versionID string) (ObjectVersion, error) {
	for _, v := range obj.Versions {
		if versionID == "" {
			if !v.IsLatest {
				continue
			}
		} else {
			if v.VersionID != versionID {
				continue
			}
		}

		if v.Deleted {
			return ObjectVersion{}, ErrNoSuchKey
		}

		return v, nil
	}

	return ObjectVersion{}, ErrNoSuchKey
}

func (b *InMemoryBackend) decompressVersion(v ObjectVersion) (*ObjectVersion, error) {
	data, err := b.compressor.Decompress(v.Data)
	if err != nil {
		return nil, err
	}

	ret := v
	ret.Data = data

	return &ret, nil
}

// DeleteObject either creates a delete marker or removes a specific version.
func (b *InMemoryBackend) DeleteObject(_ context.Context, bucketName, key, versionID string) (string, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return "", ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	if versionID != "" {
		return b.deleteSpecificVersion(bucket, key, versionID)
	}

	return b.createDeleteMarker(bucket, key), nil
}

func (b *InMemoryBackend) deleteSpecificVersion(bucket *Bucket, key, versionID string) (string, error) {
	obj, exists := bucket.Objects[key]
	if !exists {
		return "", ErrNoSuchKey
	}

	newVersions := make([]ObjectVersion, 0, len(obj.Versions))

	found := false

	var deletedMarkerID string

	for _, v := range obj.Versions {
		if v.VersionID == versionID {
			found = true

			if v.Deleted {
				deletedMarkerID = v.VersionID
			}

			continue
		}

		newVersions = append(newVersions, v)
	}

	if !found {
		return "", ErrNoSuchKey
	}

	obj.Versions = newVersions

	if len(obj.Versions) == 0 {
		delete(bucket.Objects, key)
	}

	return deletedMarkerID, nil
}

func (b *InMemoryBackend) createDeleteMarker(bucket *Bucket, key string) string {
	obj := b.getOrCreateObject(bucket, key)

	dmVersionID := NullVersion
	if bucket.Versioning == VersioningEnabled {
		dmVersionID = uuid.New().String()
	}

	dm := ObjectVersion{
		VersionID:    dmVersionID,
		IsLatest:     true,
		Deleted:      true,
		LastModified: time.Now(),
	}

	if bucket.Versioning == VersioningEnabled {
		b.appendVersioned(obj, dm)
	} else {
		b.replaceNullVersion(obj, dm)
	}

	return dmVersionID
}

// ListObjects returns all non-deleted objects matching the prefix.
func (b *InMemoryBackend) ListObjects(_ context.Context, bucketName, prefix string) ([]*Object, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	var results []*Object

	for _, obj := range bucket.Objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		if !b.isObjectDeleted(obj) {
			results = append(results, obj)
		}
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Key < results[j].Key
	})

	return results, nil
}

// ListObjectVersions returns all versions for all objects matching the prefix.
func (b *InMemoryBackend) ListObjectVersions(_ context.Context, bucketName, prefix string) ([]ObjectVersion, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.RLock()
	defer bucket.mu.RUnlock()

	var results []ObjectVersion

	for _, obj := range bucket.Objects {
		if !strings.HasPrefix(obj.Key, prefix) {
			continue
		}

		results = append(results, obj.Versions...)
	}

	// S3 sorts versions by key, then by LastModified descending
	sort.Slice(results, func(i, j int) bool {
		if results[i].Key != results[j].Key {
			return results[i].Key < results[j].Key
		}

		return results[i].LastModified.After(results[j].LastModified)
	})

	return results, nil
}

func (b *InMemoryBackend) isObjectDeleted(obj *Object) bool {
	for _, v := range obj.Versions {
		if v.IsLatest {
			return v.Deleted
		}
	}

	return false
}

// PutObjectTagging sets tags on an object.
func (b *InMemoryBackend) PutObjectTagging(_ context.Context, bucketName, key, _ string, tags map[string]string) error {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	obj, exists := bucket.Objects[key]
	if !exists {
		return ErrNoSuchKey
	}

	obj.Tags = tags

	return nil
}

// GetObjectTagging returns the tags for an object.
func (b *InMemoryBackend) GetObjectTagging(_ context.Context, bucketName, key, _ string) (map[string]string, error) {
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

	return obj.Tags, nil
}

// DeleteObjectTagging removes all tags from an object.
func (b *InMemoryBackend) DeleteObjectTagging(_ context.Context, bucketName, key, _ string) error {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	obj, exists := bucket.Objects[key]
	if !exists {
		return ErrNoSuchKey
	}

	obj.Tags = make(map[string]string)

	return nil
}

// InitiateMultipartUpload starts a new multipart upload.
func (b *InMemoryBackend) InitiateMultipartUpload(_ context.Context, bucketName, key string) (string, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return "", ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	uploadID := uuid.New().String()
	bucket.ActiveUploads[uploadID] = &MultipartUpload{
		UploadID:  uploadID,
		Bucket:    bucketName,
		Key:       key,
		Initiated: time.Now(),
		Parts:     make(map[int]Part),
	}

	return uploadID, nil
}

// UploadPart uploads a part in a multipart upload.
func (b *InMemoryBackend) UploadPart(
	_ context.Context,
	bucketName, key, uploadID string,
	partNumber int,
	data []byte,
) (string, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return "", ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	upload, exists := bucket.ActiveUploads[uploadID]
	if !exists || upload.Bucket != bucketName || upload.Key != key {
		return "", ErrNoSuchUpload // Or specific NoSuchUpload error
	}

	//nolint:gosec // MD5 required for S3 ETag
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	// We store parts uncompressed for simplicity in memory backend, or could compress each part.
	// For simplicity, let's keep them uncompressed until completion, or compress them now.
	// Let's compress now to match PutObject behavior.
	compressedData, err := b.compressor.Compress(data)
	if err != nil {
		return "", err
	}

	upload.Parts[partNumber] = Part{
		PartNumber: partNumber,
		ETag:       fmt.Sprintf("\"%s\"", etag),
		Data:       compressedData,
		Size:       int64(len(data)),
	}

	return upload.Parts[partNumber].ETag, nil
}

// CompleteMultipartUpload assembles the parts and creates the object.
func (b *InMemoryBackend) CompleteMultipartUpload(
	ctx context.Context,
	bucketName, key, uploadID string,
	parts []CompletedPartXML,
) (*ObjectVersion, error) {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return nil, ErrNoSuchBucket
	}

	bucket.mu.Lock()
	upload, exists := bucket.ActiveUploads[uploadID]
	if !exists || upload.Bucket != bucketName || upload.Key != key {
		bucket.mu.Unlock()

		return nil, ErrNoSuchUpload
	}

	// Assemble data
	var fullData []byte
	// The parts list from client dictates the order/selection.
	for _, p := range parts {
		storedPart, ok := upload.Parts[p.PartNumber]
		if !ok {
			bucket.mu.Unlock()

			return nil, fmt.Errorf("%w: part %d", ErrInvalidPart, p.PartNumber)
		}
		if storedPart.ETag != p.ETag {
			bucket.mu.Unlock()

			return nil, fmt.Errorf("%w: etag mismatch for part %d", ErrInvalidPart, p.PartNumber)
		}

		// Decompress part to append
		partData, err := b.compressor.Decompress(storedPart.Data)
		if err != nil {
			bucket.mu.Unlock()

			return nil, err
		}
		fullData = append(fullData, partData...)
	}

	// Clean up active upload BEFORE calling PutObject to avoid holding the lock
	// and to ensure it's removed even if PutObject fails.
	delete(bucket.ActiveUploads, uploadID)
	bucket.mu.Unlock()

	meta := ObjectMetadata{
		ContentType: "application/octet-stream",
	}

	return b.PutObject(ctx, bucketName, key, fullData, meta)
}

// AbortMultipartUpload cancels an upload and deletes parts.
func (b *InMemoryBackend) AbortMultipartUpload(_ context.Context, bucketName, key, uploadID string) error {
	b.mu.RLock()
	bucket, exists := b.buckets[bucketName]
	b.mu.RUnlock()

	if !exists {
		return ErrNoSuchBucket
	}

	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	upload, exists := bucket.ActiveUploads[uploadID]
	if !exists || upload.Bucket != bucketName || upload.Key != key {
		return ErrNoSuchUpload
	}

	delete(bucket.ActiveUploads, uploadID)

	return nil
}

func CalculateChecksum(data []byte, algorithm string) string {
	var sum []byte

	switch strings.ToUpper(algorithm) {
	case "CRC32":
		c := crc32.ChecksumIEEE(data)
		sum = make([]byte, crc32Len)
		for i := range crc32Len {
			sum[crc32Len-1-i] = byte(c >> (bitsInByte * i))
		}
	case "CRC32C":
		c := crc32.Checksum(data, crc32.MakeTable(crc32.Castagnoli))
		sum = make([]byte, crc32Len)
		for i := range crc32Len {
			sum[crc32Len-1-i] = byte(c >> (bitsInByte * i))
		}
	case "SHA1":
		//nolint:gosec // SHA1 supported as per S3 spec
		h := sha1.Sum(data)
		sum = h[:]
	case "SHA256":
		h := sha256.Sum256(data)
		sum = h[:]
	default:
		return ""
	}

	return base64.StdEncoding.EncodeToString(sum)
}
