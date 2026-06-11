package efs

// FileSystemCount returns the number of file systems stored in the backend
// across all regions. Used only in tests.
func FileSystemCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionFS := range b.fileSystems {
		total += len(regionFS)
	}

	return total
}

// MountTargetCount returns the number of mount targets stored in the backend
// across all regions. Used only in tests.
func MountTargetCount(b *InMemoryBackend) int {
	b.mu.RLock("MountTargetCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionMT := range b.mountTargets {
		total += len(regionMT)
	}

	return total
}

// AccessPointCount returns the number of access points stored in the backend
// across all regions. Used only in tests.
func AccessPointCount(b *InMemoryBackend) int {
	b.mu.RLock("AccessPointCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionAP := range b.accessPoints {
		total += len(regionAP)
	}

	return total
}

// ReplicationConfigCount returns the number of replication configurations stored
// in the backend across all regions. Used only in tests.
func ReplicationConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicationConfigCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionRC := range b.replicationConfigs {
		total += len(regionRC)
	}

	return total
}

// BackupPolicyCount returns the number of backup policies stored in the backend
// across all regions. Used only in tests.
func BackupPolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("BackupPolicyCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionBP := range b.backupPolicies {
		total += len(regionBP)
	}

	return total
}

// FileSystemPolicyCount returns the number of file system policies stored in the
// backend across all regions. Used only in tests.
func FileSystemPolicyCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemPolicyCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionFSP := range b.fileSystemPolicies {
		total += len(regionFSP)
	}

	return total
}

// ARNIndexSize returns the total number of entries in all ARN indexes across all
// regions. Used only in tests.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	total := 0
	for _, m := range b.fileSystemsByARN {
		total += len(m)
	}
	for _, m := range b.mountTargetsByARN {
		total += len(m)
	}
	for _, m := range b.accessPointsByARN {
		total += len(m)
	}

	return total
}

// OpsCount returns the number of pre-built operation entries in the handler. Used only in tests.
func OpsCount(h *Handler) int {
	return len(h.ops)
}
