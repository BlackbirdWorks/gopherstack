package elasticache

// CacheSecurityGroupCount returns the number of cache security groups in the backend.
func CacheSecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("CacheSecurityGroupCount")
	defer b.mu.RUnlock()

	return len(b.cacheSecurityGroups)
}

// GlobalReplicationGroupCount returns the number of global replication groups in the backend.
func GlobalReplicationGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("GlobalReplicationGroupCount")
	defer b.mu.RUnlock()

	return len(b.globalReplicationGroups)
}

// ServerlessCacheCount returns the number of serverless caches in the backend.
func ServerlessCacheCount(b *InMemoryBackend) int {
	b.mu.RLock("ServerlessCacheCount")
	defer b.mu.RUnlock()

	return len(b.serverlessCaches)
}

// ServerlessCacheSnapshotCount returns the number of serverless cache snapshots in the backend.
func ServerlessCacheSnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("ServerlessCacheSnapshotCount")
	defer b.mu.RUnlock()

	return len(b.serverlessCacheSnapshots)
}

// UserCount returns the number of users in the backend.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	return len(b.users)
}

// UserGroupCount returns the number of user groups in the backend.
func UserGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("UserGroupCount")
	defer b.mu.RUnlock()

	return len(b.userGroups)
}

// ReservedCacheNodeCount returns the number of reserved cache nodes in the backend.
func ReservedCacheNodeCount(b *InMemoryBackend) int {
	b.mu.RLock("ReservedCacheNodeCount")
	defer b.mu.RUnlock()

	return len(b.reservedCacheNodes)
}

// EventCount returns the number of events currently stored in the ring buffer.
func EventCount(b *InMemoryBackend) int {
	b.mu.RLock("EventCount")
	defer b.mu.RUnlock()

	return b.events.n
}

// AddSnapshotInternal seeds an automated snapshot for a given replication group.
func AddSnapshotInternal(b *InMemoryBackend, snapshotName, replicationGroupID, snapshotSource string) {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()

	b.snapshots[snapshotName] = &CacheSnapshot{
		SnapshotName:       snapshotName,
		ReplicationGroupID: replicationGroupID,
		SnapshotSource:     snapshotSource,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(snapshotName),
	}
}
