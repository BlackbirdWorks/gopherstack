package s3

import (
	"context"
	"errors"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// newStoredObject creates a new StoredObject with its mu initialized.
func newStoredObject(key string) *StoredObject {
	return &StoredObject{
		Key:      key,
		Versions: make(map[string]*StoredObjectVersion),
		mu:       lockmetrics.New("s3.object"),
	}
}

// ErrNoSuchKey is returned when an object key cannot be found.
// Reuses ErrNoSuchKey from errors.go via the s3 package.

// PutBucketAccelerateConfiguration stores the accelerate status ("Enabled"/"Suspended") for a bucket.
func (b *InMemoryBackend) PutBucketAccelerateConfiguration(_ context.Context, bucketName, status string) error {
	b.mu.RLock("PutBucketAccelerateConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketAccelerateConfiguration")
	defer bucket.mu.Unlock()

	bucket.AccelerateStatus = status

	return nil
}

// GetBucketAccelerateConfiguration returns the bucket's accelerate status.
// When unset the AWS default of "Suspended" is returned.
func (b *InMemoryBackend) GetBucketAccelerateConfiguration(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketAccelerateConfiguration")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketAccelerateConfiguration")
	defer bucket.mu.RUnlock()

	if bucket.AccelerateStatus == "" {
		return "Suspended", nil
	}

	return bucket.AccelerateStatus, nil
}

// PutBucketRequestPayment stores the request-payment payer ("BucketOwner" or "Requester").
func (b *InMemoryBackend) PutBucketRequestPayment(_ context.Context, bucketName, payer string) error {
	b.mu.RLock("PutBucketRequestPayment")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("PutBucketRequestPayment")
	defer bucket.mu.Unlock()

	bucket.RequestPaymentPayer = payer

	return nil
}

// GetBucketRequestPayment returns the request-payment payer; defaults to "BucketOwner".
func (b *InMemoryBackend) GetBucketRequestPayment(_ context.Context, bucketName string) (string, error) {
	b.mu.RLock("GetBucketRequestPayment")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return "", err
	}

	bucket.mu.RLock("GetBucketRequestPayment")
	defer bucket.mu.RUnlock()

	if bucket.RequestPaymentPayer == "" {
		return "BucketOwner", nil
	}

	return bucket.RequestPaymentPayer, nil
}

// ObjectAttributes is the projection of object metadata returned by GetObjectAttributes.
type ObjectAttributes struct {
	ETag         string
	StorageClass string
	Checksum     map[string]string
	ObjectSize   int64
}

// GetObjectAttributes returns selected attributes for the latest version of an object.
// versionID may be empty to select the latest version.
func (b *InMemoryBackend) GetObjectAttributes(_ context.Context, bucketName, key, versionID string) (*ObjectAttributes, error) {
	b.mu.RLock("GetObjectAttributes")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return nil, err
	}

	bucket.mu.RLock("GetObjectAttributes")
	defer bucket.mu.RUnlock()

	obj, ok := bucket.Objects[key]
	if !ok {
		return nil, ErrNoSuchKey
	}

	obj.mu.RLock("GetObjectAttributes")
	defer obj.mu.RUnlock()

	vid := versionID
	if vid == "" {
		vid = obj.LatestVersionID
	}

	ver, ok := obj.Versions[vid]
	if !ok || ver.Deleted {
		return nil, ErrNoSuchKey
	}

	out := &ObjectAttributes{
		ETag:         ver.ETag,
		ObjectSize:   ver.Size,
		StorageClass: ver.StorageClass,
		Checksum:     map[string]string{},
	}

	if out.StorageClass == "" {
		out.StorageClass = "STANDARD"
	}

	if ver.ChecksumSHA1 != nil {
		out.Checksum["ChecksumSHA1"] = *ver.ChecksumSHA1
	}
	if ver.ChecksumSHA256 != nil {
		out.Checksum["ChecksumSHA256"] = *ver.ChecksumSHA256
	}
	if ver.ChecksumCRC32 != nil {
		out.Checksum["ChecksumCRC32"] = *ver.ChecksumCRC32
	}
	if ver.ChecksumCRC32C != nil {
		out.Checksum["ChecksumCRC32C"] = *ver.ChecksumCRC32C
	}
	if ver.ChecksumCRC64NVME != nil {
		out.Checksum["ChecksumCRC64NVME"] = *ver.ChecksumCRC64NVME
	}

	return out, nil
}

// RestoreObject marks the latest object version as restored for the given duration.
// AWS returns 202 Accepted and asynchronously restores; we mark the restore as
// completed immediately and set an expiry, since this is an in-memory mock.
func (b *InMemoryBackend) RestoreObject(_ context.Context, bucketName, key string, days int) error {
	if days < 0 {
		return errors.New("restore Days must be non-negative")
	}
	if days == 0 {
		days = 1
	}

	b.mu.RLock("RestoreObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.RLock("RestoreObject")
	obj, ok := bucket.Objects[key]
	bucket.mu.RUnlock()

	if !ok {
		return ErrNoSuchKey
	}

	obj.mu.Lock("RestoreObject")
	defer obj.mu.Unlock()

	ver, ok := obj.Versions[obj.LatestVersionID]
	if !ok || ver.Deleted {
		return ErrNoSuchKey
	}

	ver.OngoingRestore = false
	ver.RestoreExpiry = time.Now().Add(time.Duration(days) * 24 * time.Hour)

	return nil
}

// RenameObject performs an atomic copy+delete of the source key into a new key.
// The target key is taken from `targetKey` (relative to the same bucket).
func (b *InMemoryBackend) RenameObject(_ context.Context, bucketName, sourceKey, targetKey string) error {
	if targetKey == "" || targetKey == sourceKey {
		return errors.New("target key must differ from source")
	}
	if strings.HasPrefix(targetKey, "/") {
		targetKey = strings.TrimPrefix(targetKey, "/")
	}

	b.mu.RLock("RenameObject")
	bucket, err := b.getBucket(bucketName)
	b.mu.RUnlock()

	if err != nil {
		return err
	}

	bucket.mu.Lock("RenameObject")
	defer bucket.mu.Unlock()

	srcObj, ok := bucket.Objects[sourceKey]
	if !ok {
		return ErrNoSuchKey
	}

	srcObj.mu.Lock("RenameObject")
	defer srcObj.mu.Unlock()

	srcVer, ok := srcObj.Versions[srcObj.LatestVersionID]
	if !ok || srcVer.Deleted {
		return ErrNoSuchKey
	}

	// Clone the latest version under the new key.
	newVersion := *srcVer
	newVersion.Key = targetKey
	newVersion.IsLatest = true

	dstObj := newStoredObject(targetKey)
	dstObj.LatestVersionID = newVersion.VersionID
	dstObj.Versions[newVersion.VersionID] = &newVersion
	if existing, exists := bucket.Objects[targetKey]; exists {
		// If a destination object already exists, replace its latest version.
		existing.LatestVersionID = newVersion.VersionID
		existing.Versions[newVersion.VersionID] = &newVersion
	} else {
		bucket.Objects[targetKey] = dstObj
	}

	delete(bucket.Objects, sourceKey)

	return nil
}
