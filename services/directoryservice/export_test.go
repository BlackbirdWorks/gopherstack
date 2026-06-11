package directoryservice

// DirectoryCount returns the number of stored directories across all regions.
func DirectoryCount(b *InMemoryBackend) int {
	b.mu.RLock("DirectoryCount")
	defer b.mu.RUnlock()

	total := 0
	for _, st := range b.states {
		total += len(st.directories)
	}

	return total
}

// SnapshotCount returns the number of stored snapshots across all regions.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	total := 0
	for _, st := range b.states {
		total += len(st.snapshots)
	}

	return total
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
