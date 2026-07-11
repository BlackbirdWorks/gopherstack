package s3

import (
	"slices"
	"strings"
	"time"
)

// Exported wrappers for internal functions used in tests.

// DetailTypeFromEventName exposes detailTypeFromEventName for external tests.
func DetailTypeFromEventName(eventName string) string {
	return detailTypeFromEventName(eventName)
}

// ReasonFromEventName exposes reasonFromEventName for external tests.
func ReasonFromEventName(eventName string) string {
	return reasonFromEventName(eventName)
}

// UploadsForBucket returns the number of in-progress multipart uploads for the
// given bucket. Used in tests to verify janitor cleanup of orphaned uploads.
func (b *InMemoryBackend) UploadsForBucket(bucket string) int {
	b.mu.RLock("UploadsForBucket")
	defer b.mu.RUnlock()

	return len(b.uploadsByBucket.Get(bucket))
}

// TagsForBucket returns the number of tag entries for the given bucket.
// Used in tests to verify janitor cleanup of orphaned tags.
func (b *InMemoryBackend) TagsForBucket(bucket string) int {
	b.mu.RLock("TagsForBucket")
	defer b.mu.RUnlock()

	prefix := bucket + "/"
	count := 0

	for k := range b.tags {
		if strings.HasPrefix(k, prefix) {
			count++
		}
	}

	return count
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *S3Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// DrainSemCapacity returns the capacity of the janitor's drain semaphore.
// Used in tests to verify that maxConcurrentDrains is respected.
func (j *Janitor) DrainSemCapacity() int {
	return cap(j.drainSem)
}

// MaxConcurrentDrains exposes the package constant for external tests.
const MaxConcurrentDrains = maxConcurrentDrains

// PeekStoredBytes returns the raw Data of the latest version of an object
// without decryption or decompression. Used by tests to verify that SSE-S3 /
// SSE-C objects are actually stored as ciphertext rather than plaintext.
func PeekStoredBytes(b *InMemoryBackend, bucketName, key string) []byte {
	b.mu.RLock("PeekStoredBytes")
	defer b.mu.RUnlock()

	bucket, ok := b.buckets.Get(bucketName)
	if !ok {
		return nil
	}

	bucket.mu.RLock("PeekStoredBytes")
	defer bucket.mu.RUnlock()

	obj, ok := bucket.Objects[key]
	if !ok {
		return nil
	}
	ver := findLatestVersion(obj.Versions)
	if ver == nil {
		return nil
	}

	return ver.Data
}

// PendingObjectLambdaRequestsCount returns the number of in-flight object
// lambda request entries in pendingObjectLambdaRequests. Used in tests to
// verify that entries are cleaned up on timeout or context cancellation.
func (h *S3Handler) PendingObjectLambdaRequestsCount() int {
	count := 0
	h.pendingObjectLambdaRequests.Range(func(_, _ any) bool {
		count++

		return true
	})

	return count
}

// BackdateObjectForTest sets LastModified on all versions of key to t.
// Used in lifecycle transition tests to simulate aged objects.
func BackdateObjectForTest(b *InMemoryBackend, bucketName, key string, t time.Time) {
	b.mu.RLock("BackdateObjectForTest")
	bucket, ok := b.buckets.Get(bucketName)
	b.mu.RUnlock()

	if !ok {
		return
	}

	bucket.mu.RLock("BackdateObjectForTest")
	obj, ok := bucket.Objects[key]
	bucket.mu.RUnlock()

	if !ok {
		return
	}

	obj.mu.Lock("BackdateObjectForTest")
	for _, ver := range obj.Versions {
		ver.LastModified = t
	}
	obj.mu.Unlock()
}

// BackdateUploadForTest sets the Initiated time of a multipart upload to t.
// Used in janitor tests to simulate aged uploads without waiting.
func BackdateUploadForTest(b *InMemoryBackend, bucket string, uploadID *string, t time.Time) {
	if uploadID == nil {
		return
	}

	b.mu.RLock("BackdateUploadForTest")
	upload := b.getUpload(bucket, *uploadID)
	b.mu.RUnlock()

	if upload == nil {
		return
	}

	// Mutate directly — Initiated has no per-upload lock.
	b.mu.Lock("BackdateUploadForTest.write")
	if u := b.getUpload(bucket, *uploadID); u != nil {
		u.Initiated = t
	}
	b.mu.Unlock()
}

// StorageClassTransitionsForObject returns the StorageClassTransitions history
// for the latest version of an object. Used in janitor transition tests.
func StorageClassTransitionsForObject(
	b *InMemoryBackend,
	bucketName, key string,
) []StorageClassTransition {
	b.mu.RLock("StorageClassTransitionsForObject")
	bucket, ok := b.buckets.Get(bucketName)
	b.mu.RUnlock()

	if !ok {
		return nil
	}

	bucket.mu.RLock("StorageClassTransitionsForObject")
	obj, ok := bucket.Objects[key]
	bucket.mu.RUnlock()

	if !ok {
		return nil
	}

	obj.mu.RLock("StorageClassTransitionsForObject")
	defer obj.mu.RUnlock()

	ver := findLatestVersion(obj.Versions)
	if ver == nil {
		return nil
	}

	return slices.Clone(ver.StorageClassTransitions)
}
