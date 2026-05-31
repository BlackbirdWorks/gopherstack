package fsx

// FileSystemCount returns the number of stored file systems.
func FileSystemCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemCount")
	defer b.mu.RUnlock()

	return len(b.fileSystems)
}

// BackupCount returns the number of stored backups.
func BackupCount(b *InMemoryBackend) int {
	b.mu.RLock("BackupCount")
	defer b.mu.RUnlock()

	return len(b.backups)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// GetBackend returns the InMemoryBackend for the handler (test use only).
func GetBackend(h *Handler) *InMemoryBackend {
	return h.Backend.(*InMemoryBackend)
}
