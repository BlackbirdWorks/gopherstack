package directoryservice

import "time"

// DirectoryStageForTest returns the current stage of a directory in the default region.
func DirectoryStageForTest(b *InMemoryBackend, dirID string) string {
	b.mu.RLock("DirectoryStageForTest")
	defer b.mu.RUnlock()

	for _, d := range b.directories.All() {
		if d.DirectoryID == dirID {
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

	return b.directories.Len()
}

// SnapshotCount returns the number of stored snapshots across all regions.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return b.snapshots.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// RadiusAuthProtocolForTest returns the AuthenticationProtocol RADIUS setting
// stored for directoryID in the backend's default region, and whether any
// RADIUS settings exist for it. RADIUS settings have no public Describe
// operation in StorageBackend, so tests that need to observe them (e.g. the
// Snapshot/Restore round trip) go through this internal accessor instead.
func RadiusAuthProtocolForTest(b *InMemoryBackend, directoryID string) (string, bool) {
	b.mu.RLock("RadiusAuthProtocolForTest")
	defer b.mu.RUnlock()

	s, ok := b.radiusSettingsGet(b.region, directoryID)
	if !ok {
		return "", false
	}

	return s.AuthenticationProtocol, true
}
