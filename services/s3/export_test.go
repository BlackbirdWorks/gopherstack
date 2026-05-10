package s3

import (
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

	return len(b.uploads[bucket])
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

	region, ok := b.bucketIndex[bucketName]
	if !ok {
		return nil
	}

	bucket := b.buckets[region][bucketName]
	if bucket == nil {
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
