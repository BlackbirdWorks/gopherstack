package s3

import (
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
	objects    map[string]map[string]*Object // bucket -> key -> Object
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
		objects:    make(map[string]map[string]*Object),
		compressor: compressor,
	}
}

// CreateBucket creates a new bucket with the given name.
func (b *InMemoryBackend) CreateBucket(name string) error {
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
func (b *InMemoryBackend) DeleteBucket(name string) error {
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

// HeadBucket returns a bucket by name.
func (b *InMemoryBackend) HeadBucket(name string) (*Bucket, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	bucket, exists := b.buckets[name]
	if !exists {
		return nil, ErrNoSuchBucket
	}

	return bucket, nil
}

// ListBuckets returns all buckets sorted by name.
func (b *InMemoryBackend) ListBuckets() ([]*Bucket, error) {
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
	bucketName, key string,
	data []byte,
	meta ObjectMetadata,
) (*ObjectVersion, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	bucket, exists := b.buckets[bucketName]
	if !exists {
		return nil, ErrNoSuchBucket
	}

	compressedData, err := b.compressor.Compress(data)
	if err != nil {
		return nil, err
	}

	versionID := NullVersion
	if bucket.Versioning == VersioningEnabled {
		versionID = uuid.New().String()
	}

	//nolint:gosec // MD5 is required for S3 ETag
	hash := md5.Sum(data)
	etag := hex.EncodeToString(hash[:])

	checksumValue := meta.ChecksumValue
	if meta.ChecksumAlgorithm != "" && checksumValue == "" {
		checksumValue = CalculateChecksum(data, meta.ChecksumAlgorithm)
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
		if obj.Versions[i].IsLatest {
			obj.Versions[i].IsLatest = false
		}
	}

	obj.Versions = append([]ObjectVersion{ver}, obj.Versions...)
}

func (b *InMemoryBackend) replaceNullVersion(obj *Object, ver ObjectVersion) {
	newVersions := []ObjectVersion{ver}

	for _, v := range obj.Versions {
		if v.VersionID != NullVersion {
			newVersions = append(newVersions, v)
		}
	}

	obj.Versions = newVersions
}

// GetObject retrieves an object version, decompressing the data.
func (b *InMemoryBackend) GetObject(bucketName, key, versionID string) (*ObjectVersion, error) {
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
func (b *InMemoryBackend) HeadObject(bucketName, key, versionID string) (*ObjectVersion, error) {
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
	if versionID == "" {
		v, err = b.findLatestVersion(obj)
	} else {
		v, err = b.findSpecificVersion(obj, versionID)
	}

	if err != nil {
		return nil, err
	}

	ret := *v
	ret.Data = nil // Metadata only

	return &ret, nil
}

func (b *InMemoryBackend) findLatestVersion(obj *Object) (*ObjectVersion, error) {
	for _, v := range obj.Versions {
		if !v.IsLatest {
			continue
		}

		if v.Deleted {
			return nil, ErrNoSuchKey
		}

		return &v, nil
	}

	return nil, ErrNoSuchKey
}

func (b *InMemoryBackend) findSpecificVersion(obj *Object, versionID string) (*ObjectVersion, error) {
	for _, v := range obj.Versions {
		if v.VersionID != versionID {
			continue
		}

		if v.Deleted {
			return nil, ErrNoSuchKey
		}

		return &v, nil
	}

	return nil, ErrNoSuchKey
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
func (b *InMemoryBackend) DeleteObject(bucketName, key, versionID string) (string, error) {
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
func (b *InMemoryBackend) ListObjects(bucketName, prefix string) ([]*Object, error) {
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
func (b *InMemoryBackend) ListObjectVersions(bucketName, prefix string) ([]ObjectVersion, error) {
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
func (b *InMemoryBackend) PutObjectTagging(bucketName, key, _ string, tags map[string]string) error {
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
func (b *InMemoryBackend) GetObjectTagging(bucketName, key, _ string) (map[string]string, error) {
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
func (b *InMemoryBackend) DeleteObjectTagging(bucketName, key, _ string) error {
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
