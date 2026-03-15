package s3

import "strings"

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
