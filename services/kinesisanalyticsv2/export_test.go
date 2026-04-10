package kinesisanalyticsv2

// TagsToMapForTest exposes tagsToMap for tests.
func TagsToMapForTest(tags []Tag) map[string]string {
	return tagsToMap(tags)
}

// MapToTagsForTest exposes mapToTags for tests.
func MapToTagsForTest(m map[string]string) []Tag {
	return mapToTags(m)
}

// ApplicationCount returns the number of applications stored in the backend.
// Exported for use in tests only.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// SnapshotCount returns the total number of snapshots across all applications.
// Exported for use in tests only.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	total := 0
	for _, snaps := range b.snapshots {
		total += len(snaps)
	}

	return total
}

// HandlerOpsLen returns the number of operations pre-built in the handler dispatch map.
// Exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
