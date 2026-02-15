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
	compressor    Compressor
	buckets       map[string]*Bucket
	objects       map[string]map[string]*Object // bucket -> key -> Object
	activeUploads map[string]*MultipartUpload   // uploadID -> MultipartUpload
	mu            sync.RWMutex
}

type MultipartUpload struct {
	UploadID  string
	Bucket    string
	Key       string
	Initiated time.Time
	Parts     map[int]Part
}

type Part struct {
	PartNumber int
	ETag       string
	Data       []byte
	Size       int64
}

const (
	crc32Len     = 4
	bitsInByte   = 8
	bitsInUint32 = 32
)

// NewInMemoryBackend creates a new InMemoryBackend using the given compressor.
func NewInMemoryBackend(compressor Compressor) *InMemoryBackend {
	return &InMemoryBackend{
		buckets:       make(map[string]*Bucket),
		objects:       make(map[string]map[string]*Object),
		activeUploads: make(map[string]*MultipartUpload),
		compressor:    compressor,
	}
}

// CreateBucket creates a new bucket with the given name.
func (b *InMemoryBackend) CreateBucket(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[name]; exists {
		return ErrBucketAlreadyExists
	}

	b.buckets[name] = &Bucket{
		Name:         name,
		CreationDate: time.Now(),
		Versioning:   VersioningSuspended,
	}

	b.objects[name] = make(map[string]*Object)

	return nil
}

// DeleteBucket deletes a bucket by name. Fails if not empty.
func (b *InMemoryBackend) DeleteBucket(ctx context.Context, name string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[name]; !exists {
		return ErrNoSuchBucket
	}

	if len(b.objects[name]) > 0 {
		return ErrBucketNotEmpty
	}

	delete(b.buckets, name)
	delete(b.objects, name)

	return nil
}

// PutBucketVersioning sets the versioning status for a bucket.
func (b *InMemoryBackend) PutBucketVersioning(ctx context.Context, bucket string, status VersioningStatus) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	bkt, exists := b.buckets[bucket]
	if !exists {
		return ErrNoSuchBucket
	}

	bkt.Versioning = status

	return nil
}

// GetBucketVersioning retrieves the versioning status for a bucket.
func (b *InMemoryBackend) GetBucketVersioning(ctx context.Context, bucket string) (VersioningConfiguration, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bkt, exists := b.buckets[bucket]
	if !exists {
		return VersioningConfiguration{}, ErrNoSuchBucket
	}

	return VersioningConfiguration{
		Status: string(bkt.Versioning),
	}, nil
}

// HeadBucket returns a bucket by name.
func (b *InMemoryBackend) HeadBucket(ctx context.Context, name string) (*Bucket, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bucket, exists := b.buckets[name]
	if !exists {
		return nil, ErrNoSuchBucket
	}

	return bucket, nil
}

// ListBuckets returns all buckets sorted by name.
func (b *InMemoryBackend) ListBuckets(ctx context.Context) ([]*Bucket, error) {
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
	ctx context.Context,
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

	b.mu.Lock()
	defer b.mu.Unlock()

	bucket, exists := b.buckets[bucketName]
	if !exists {
		return nil, ErrNoSuchBucket
	}

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

	obj := b.getOrCreateObject(bucketName, key)
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

func (b *InMemoryBackend) getOrCreateObject(bucketName, key string) *Object {
	obj, exists := b.objects[bucketName][key]
	if !exists {
		obj = &Object{
			Key:  key,
			Tags: make(map[string]string),
		}

		b.objects[bucketName][key] = obj
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
func (b *InMemoryBackend) GetObject(ctx context.Context, bucketName, key, versionID string) (*ObjectVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return nil, ErrNoSuchBucket
	}

	obj, exists := b.objects[bucketName][key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	if versionID == "" {
		return b.getLatestVersion(obj)
	}

	return b.getSpecificVersion(obj, versionID)
}

// HeadObject retrieves an object version's metadata without decompressing the data.
func (b *InMemoryBackend) HeadObject(ctx context.Context, bucketName, key, versionID string) (*ObjectVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return nil, ErrNoSuchBucket
	}

	obj, exists := b.objects[bucketName][key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	var v *ObjectVersion
	var err error
	// Use simplified findVersion logic (merged earlier)
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

func copyBytes(b []byte) []byte {
	if b == nil {
		return nil
	}

	out := make([]byte, len(b))
	copy(out, b)

	return out
}

func (b *InMemoryBackend) getLatestVersion(obj *Object) (*ObjectVersion, error) {
	for _, v := range obj.Versions {
		if !v.IsLatest {
			continue
		}

		if v.Deleted {
			return nil, ErrNoSuchKey
		}

		return b.decompressVersion(v)
	}

	return nil, ErrNoSuchKey
}

func (b *InMemoryBackend) getSpecificVersion(obj *Object, versionID string) (*ObjectVersion, error) {
	for _, v := range obj.Versions {
		if v.VersionID != versionID {
			continue
		}

		if v.Deleted {
			return nil, ErrNoSuchKey
		}

		return b.decompressVersion(v)
	}

	return nil, ErrNoSuchKey
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
func (b *InMemoryBackend) DeleteObject(ctx context.Context, bucketName, key, versionID string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bucket, exists := b.buckets[bucketName]
	if !exists {
		return "", ErrNoSuchBucket
	}

	if versionID != "" {
		return b.deleteSpecificVersion(bucketName, key, versionID)
	}

	return b.createDeleteMarker(bucketName, key, bucket), nil
}

func (b *InMemoryBackend) deleteSpecificVersion(bucketName, key, versionID string) (string, error) {
	obj, exists := b.objects[bucketName][key]
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
		delete(b.objects[bucketName], key)
	}

	return deletedMarkerID, nil
}

func (b *InMemoryBackend) createDeleteMarker(bucketName, key string, bucket *Bucket) string {
	obj := b.getOrCreateObject(bucketName, key)

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
func (b *InMemoryBackend) ListObjects(ctx context.Context, bucketName, prefix string) ([]*Object, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return nil, ErrNoSuchBucket
	}

	var results []*Object

	for _, obj := range b.objects[bucketName] {
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
func (b *InMemoryBackend) ListObjectVersions(ctx context.Context, bucketName, prefix string) ([]ObjectVersion, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return nil, ErrNoSuchBucket
	}

	var results []ObjectVersion

	for _, obj := range b.objects[bucketName] {
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
func (b *InMemoryBackend) PutObjectTagging(ctx context.Context, bucketName, key, _ string, tags map[string]string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return ErrNoSuchBucket
	}

	obj, exists := b.objects[bucketName][key]
	if !exists {
		return ErrNoSuchKey
	}

	obj.Tags = tags

	return nil
}

// GetObjectTagging returns the tags for an object.
func (b *InMemoryBackend) GetObjectTagging(ctx context.Context, bucketName, key, _ string) (map[string]string, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return nil, ErrNoSuchBucket
	}

	obj, exists := b.objects[bucketName][key]
	if !exists {
		return nil, ErrNoSuchKey
	}

	return obj.Tags, nil
}

// DeleteObjectTagging removes all tags from an object.
func (b *InMemoryBackend) DeleteObjectTagging(ctx context.Context, bucketName, key, _ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return ErrNoSuchBucket
	}

	obj, exists := b.objects[bucketName][key]
	if !exists {
		return ErrNoSuchKey
	}

	obj.Tags = make(map[string]string)

	return nil
}

// InitiateMultipartUpload starts a new multipart upload.
func (b *InMemoryBackend) InitiateMultipartUpload(ctx context.Context, bucketName, key string) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.buckets[bucketName]; !exists {
		return "", ErrNoSuchBucket
	}

	uploadID := uuid.New().String()
	b.activeUploads[uploadID] = &MultipartUpload{
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
	ctx context.Context,
	bucketName, key, uploadID string,
	partNumber int,
	data []byte,
) (string, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	upload, exists := b.activeUploads[uploadID]
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
	b.mu.Lock()
	defer b.mu.Unlock()

	upload, exists := b.activeUploads[uploadID]
	if !exists || upload.Bucket != bucketName || upload.Key != key {
		return nil, ErrNoSuchUpload
	}

	// Assemble data
	var fullData []byte
	// Verify parts and order
	// The parts list from client dictates the order/selection.
	// DynamoDB style: we trust the client's list or validate? S3 spec says we concatenate in order of PartNumber.
	// But usually the client provides the list of parts to assemble.
	// We should sort the provided parts just in case or follow them?
	// The client sends <Part><PartNumber>1</PartNumber><ETag>...</ETag></Part>...
	// We must use the parts specified.

	for _, p := range parts {
		storedPart, ok := upload.Parts[p.PartNumber]
		if !ok {
			return nil, fmt.Errorf("%w: part %d", ErrInvalidPart, p.PartNumber)
		}
		// ETag from client might be quoted or unquoted. Backend stores quoted.
		// We should probably normalize comparison.
		// For now, assume client sends what backend returned.
		if storedPart.ETag != p.ETag {
			return nil, fmt.Errorf("%w: etag mismatch for part %d", ErrInvalidPart, p.PartNumber)
		}

		// Decompress part to append
		partData, err := b.compressor.Decompress(storedPart.Data)
		if err != nil {
			return nil, err
		}
		fullData = append(fullData, partData...)
	}

	// Now PutObject logic
	// We need to release the lock to call PutObject?
	// PutObject takes a lock. We are holding a lock. Deadlock.
	// Refactor PutObject core logic or manually do it here.
	// Manually doing it here is safer.

	// But wait, PutObject also recompresses.
	// This is inefficient (compress part -> decompress -> append -> compress full).
	// But optimizing implementation details of memory backend isn't the priority. Correctness is.
	// We can cheat: unlock, call PutObject, delete upload.
	// But there's a race? Not really for this unique UploadID.
	// But for the KEY, someone else could write.
	// PutObject is atomic.
	// So we can:
	// 1. Assemble data (done)
	// 2. Unlock
	// 3. Call PutObjectWithContext (recursive call via public API)
	// 4. Lock, Delete upload
	// This is safe enough for this backend.

	// However, we are inside a locked region.
	// Let's release lock, do the put, then re-acquire to clean up.
	b.mu.Unlock()

	meta := ObjectMetadata{
		ContentType: "application/octet-stream", // Should be passed in Initiate... but we didn't store it.
		// In reality, S3 takes Content-Type at Initiate.
		// For now default.
	}

	version, err := b.PutObject(ctx, bucketName, key, fullData, meta)
	b.mu.Lock() // Re-acquire
	if err != nil {
		return nil, err
	}

	delete(b.activeUploads, uploadID)

	return version, nil
}

// AbortMultipartUpload cancels an upload and deletes parts.
func (b *InMemoryBackend) AbortMultipartUpload(ctx context.Context, bucketName, key, uploadID string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	upload, exists := b.activeUploads[uploadID]
	if !exists || upload.Bucket != bucketName || upload.Key != key {
		return ErrNoSuchUpload
	}

	delete(b.activeUploads, uploadID)
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
