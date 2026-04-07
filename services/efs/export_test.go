package efs

// FileSystemCount returns the number of file systems stored in the backend. Used only in tests.
func FileSystemCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemCount")
	defer b.mu.RUnlock()

	return len(b.fileSystems)
}

// MountTargetCount returns the number of mount targets stored in the backend. Used only in tests.
func MountTargetCount(b *InMemoryBackend) int {
	b.mu.RLock("MountTargetCount")
	defer b.mu.RUnlock()

	return len(b.mountTargets)
}

// AccessPointCount returns the number of access points stored in the backend. Used only in tests.
func AccessPointCount(b *InMemoryBackend) int {
	b.mu.RLock("AccessPointCount")
	defer b.mu.RUnlock()

	return len(b.accessPoints)
}

// ReplicationConfigCount returns the number of replication configurations stored in the backend. Used only in tests.
func ReplicationConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicationConfigCount")
	defer b.mu.RUnlock()

	return len(b.replicationConfigs)
}

// BackupPolicyCount returns the number of backup policies stored in the backend. Used only in tests.
func BackupPolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("BackupPolicyCount")
	defer b.mu.RUnlock()

	return len(b.backupPolicies)
}

// FileSystemPolicyCount returns the number of file system policies stored in the backend. Used only in tests.
func FileSystemPolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemPolicyCount")
	defer b.mu.RUnlock()

	return len(b.fileSystemPolicies)
}

// ARNIndexSize returns the total number of entries in all ARN indexes. Used only in tests.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return len(b.fileSystemsByARN) + len(b.mountTargetsByARN) + len(b.accessPointsByARN)
}

// OpsCount returns the number of pre-built operation entries in the handler. Used only in tests.
func OpsCount(h *Handler) int {
	return len(h.ops)
}
