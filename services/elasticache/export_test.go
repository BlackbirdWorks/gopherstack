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
