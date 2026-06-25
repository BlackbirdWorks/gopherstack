package directoryservice

import "time"

// DirectoryStageForTest returns the current stage of a directory in the default region.
func DirectoryStageForTest(b *InMemoryBackend, dirID string) string {
	b.mu.RLock("DirectoryStageForTest")
	defer b.mu.RUnlock()

	for _, st := range b.states {
		if d, ok := st.directories[dirID]; ok {
			return d.Stage
		}
	}

	return ""
}

// WaitForDirectoryActive polls until the directory reaches Active or the timeout expires.
func WaitForDirectoryActive(b *InMemoryBackend, dirID string, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if DirectoryStageForTest(b, dirID) == string(DirectoryStageActive) {
			return true
		}

		time.Sleep(10 * time.Millisecond)
	}

	return false
}

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
