package fsx

// FileSystemCount returns the number of stored file systems.
func FileSystemCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemCount")
	defer b.mu.RUnlock()

	return b.fileSystems.Len()
}

// BackupCount returns the number of stored backups.
func BackupCount(b *InMemoryBackend) int {
	b.mu.RLock("BackupCount")
	defer b.mu.RUnlock()

	return b.backups.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// GetBackend returns the InMemoryBackend for the handler (test use only).
func GetBackend(h *Handler) *InMemoryBackend {
	return h.Backend.(*InMemoryBackend)
}

// SnapshotCount returns the number of stored snapshots.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return b.snapshots.Len()
}

// SVMCount returns the number of stored storage virtual machines.
func SVMCount(b *InMemoryBackend) int {
	b.mu.RLock("SVMCount")
	defer b.mu.RUnlock()

	return b.storageVirtualMachines.Len()
}

// VolumeCount returns the number of stored volumes.
func VolumeCount(b *InMemoryBackend) int {
	b.mu.RLock("VolumeCount")
	defer b.mu.RUnlock()

	return b.volumes.Len()
}

// FileCacheCount returns the number of stored file caches.
func FileCacheCount(b *InMemoryBackend) int {
	b.mu.RLock("FileCacheCount")
	defer b.mu.RUnlock()

	return b.fileCaches.Len()
}

// DRACount returns the number of stored data repository associations.
func DRACount(b *InMemoryBackend) int {
	b.mu.RLock("DRACount")
	defer b.mu.RUnlock()

	return b.dataRepositoryAssocs.Len()
}

// DataRepositoryTaskCount returns the number of stored data repository tasks.
func DataRepositoryTaskCount(b *InMemoryBackend) int {
	b.mu.RLock("DataRepositoryTaskCount")
	defer b.mu.RUnlock()

	return b.dataRepositoryTasks.Len()
}

// S3AccessPointCount returns the number of stored S3 access points.
func S3AccessPointCount(b *InMemoryBackend) int {
	b.mu.RLock("S3AccessPointCount")
	defer b.mu.RUnlock()

	return b.s3AccessPoints.Len()
}
