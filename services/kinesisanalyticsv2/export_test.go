package kinesisanalyticsv2

// TagsToMapForTest exposes tagsToMap for tests.
func TagsToMapForTest(tags []Tag) map[string]string {
	return tagsToMap(tags)
}

// MapToTagsForTest exposes mapToTags for tests.
func MapToTagsForTest(m map[string]string) []Tag {
	return mapToTags(m)
}

// ApplicationCount returns the number of applications stored in the backend across all regions.
// Exported for use in tests only.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// SnapshotCount returns the total number of snapshots across all applications and regions.
// Exported for use in tests only.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return b.snapshots.Len()
}

// HandlerOpsLen returns the number of operations pre-built in the handler dispatch map.
// Exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// OperationsMapKeyCount returns the number of application-name keys in the operations map
// for the given region. Used to verify leak-free cleanup on DeleteApplication.
func OperationsMapKeyCount(b *InMemoryBackend, region string) int {
	b.mu.RLock("OperationsMapKeyCount")
	defer b.mu.RUnlock()

	return len(b.operations[region])
}

// VersionsMapKeyCount returns the number of application-name keys in the versions map
// for the given region. Used to verify leak-free cleanup on DeleteApplication.
func VersionsMapKeyCount(b *InMemoryBackend, region string) int {
	b.mu.RLock("VersionsMapKeyCount")
	defer b.mu.RUnlock()

	return len(b.versions[region])
}
