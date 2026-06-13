package macie2

// AllowListCount returns the number of stored allow lists.
func AllowListCount(b *InMemoryBackend) int {
	b.mu.RLock("AllowListCount")
	defer b.mu.RUnlock()

	return len(b.allowLists)
}

// CustomDataIDCount returns the number of stored custom data identifiers.
func CustomDataIDCount(b *InMemoryBackend) int {
	b.mu.RLock("CustomDataIDCount")
	defer b.mu.RUnlock()

	return len(b.customDataIDs)
}

// FindingsFilterCount returns the number of stored findings filters.
func FindingsFilterCount(b *InMemoryBackend) int {
	b.mu.RLock("FindingsFilterCount")
	defer b.mu.RUnlock()

	return len(b.findingsFilters)
}

// FindingCount returns the number of stored findings.
func FindingCount(b *InMemoryBackend) int {
	b.mu.RLock("FindingCount")
	defer b.mu.RUnlock()

	return len(b.findings)
}

// S3BucketCount returns the number of seeded S3 buckets.
func S3BucketCount(b *InMemoryBackend) int {
	b.mu.RLock("S3BucketCount")
	defer b.mu.RUnlock()

	return len(b.s3Buckets)
}

// SeedS3Bucket adds an S3 bucket metadata entry to the backend for testing.
func SeedS3Bucket(b *InMemoryBackend, bucket S3BucketMetadata) {
	b.AddS3Bucket(bucket)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
