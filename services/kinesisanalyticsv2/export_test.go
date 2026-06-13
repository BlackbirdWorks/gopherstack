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

	total := 0
	for _, regionApps := range b.applications {
		total += len(regionApps)
	}

	return total
}

// SnapshotCount returns the total number of snapshots across all applications and regions.
// Exported for use in tests only.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionSnaps := range b.snapshots {
		for _, snaps := range regionSnaps {
			total += len(snaps)
		}
	}

	return total
}

// HandlerOpsLen returns the number of operations pre-built in the handler dispatch map.
// Exported for use in tests only.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
