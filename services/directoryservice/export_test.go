package directoryservice

// DirectoryCount returns the number of stored directories.
func DirectoryCount(b *InMemoryBackend) int {
	b.mu.RLock("DirectoryCount")
	defer b.mu.RUnlock()

	return len(b.directories)
}

// SnapshotCount returns the number of stored snapshots.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return len(b.snapshots)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
