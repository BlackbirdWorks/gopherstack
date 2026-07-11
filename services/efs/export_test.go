package efs

import "time"

// FileSystemCount returns the number of file systems stored in the backend
// across all regions. Used only in tests.
func FileSystemCount(b *InMemoryBackend) int {
	b.mu.RLock("FileSystemCount")
	defer b.mu.RUnlock()

	return b.fileSystems.Len()
}

// MountTargetCount returns the number of mount targets stored in the backend
// across all regions. Used only in tests.
func MountTargetCount(b *InMemoryBackend) int {
	b.mu.RLock("MountTargetCount")
	defer b.mu.RUnlock()

	return b.mountTargets.Len()
}

// AccessPointCount returns the number of access points stored in the backend
// across all regions. Used only in tests.
func AccessPointCount(b *InMemoryBackend) int {
	b.mu.RLock("AccessPointCount")
	defer b.mu.RUnlock()

	return b.accessPoints.Len()
}

// ReplicationConfigCount returns the number of replication configurations stored
// in the backend across all regions. Used only in tests.
func ReplicationConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("ReplicationConfigCount")
	defer b.mu.RUnlock()

	return b.replicationConfigs.Len()
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

	return b.fileSystemsByARN.Len() + b.mountTargetsByARN.Len() + b.accessPointsByARN.Len()
}

// OpsCount returns the number of pre-built operation entries in the handler. Used only in tests.
func OpsCount(h *Handler) int {
	return len(h.ops)
}

// SetFSActivationDelay configures the delay before a newly created file system
// transitions from "creating" to "available". Set to a positive value in parity
// tests that verify the lifecycle simulation; leave at zero (default) for
// all other tests so creation is synchronous and immediately available.
func SetFSActivationDelay(b *InMemoryBackend, d time.Duration) {
	b.fsActivationDelay = d
}
