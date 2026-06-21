package elasticache

// CacheSecurityGroupCount returns the number of cache security groups in the default region.
func CacheSecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("CacheSecurityGroupCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionStore := range b.cacheSecurityGroups {
		total += len(regionStore)
	}

	return total
}

// GlobalReplicationGroupCount returns the number of global replication groups in the backend.
func GlobalReplicationGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("GlobalReplicationGroupCount")
	defer b.mu.RUnlock()

	return len(b.globalReplicationGroups)
}

// ServerlessCacheCount returns the number of serverless caches across all regions.
func ServerlessCacheCount(b *InMemoryBackend) int {
	b.mu.RLock("ServerlessCacheCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionStore := range b.serverlessCaches {
		total += len(regionStore)
	}

	return total
}

// ServerlessCacheSnapshotCount returns the number of serverless cache snapshots across all regions.
func ServerlessCacheSnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("ServerlessCacheSnapshotCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionStore := range b.serverlessCacheSnapshots {
		total += len(regionStore)
	}

	return total
}

// UserCount returns the number of users across all regions.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionStore := range b.users {
		total += len(regionStore)
	}

	return total
}

// UserGroupCount returns the number of user groups across all regions.
func UserGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("UserGroupCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionStore := range b.userGroups {
		total += len(regionStore)
	}

	return total
}

// ReservedCacheNodeCount returns the number of reserved cache nodes across all regions.
func ReservedCacheNodeCount(b *InMemoryBackend) int {
	b.mu.RLock("ReservedCacheNodeCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionStore := range b.reservedCacheNodes {
		total += len(regionStore)
	}

	return total
}

// EventCount returns the number of events currently stored in the ring buffer.
func EventCount(b *InMemoryBackend) int {
	b.mu.RLock("EventCount")
	defer b.mu.RUnlock()

	return b.events.n
}

// AddClusterInRGInternal seeds a cluster that belongs to a replication group (uses default region).
func AddClusterInRGInternal(b *InMemoryBackend, clusterID, replicationGroupID string) {
	b.mu.Lock("AddClusterInRGInternal")
	defer b.mu.Unlock()

	b.clustersStore(b.region)[clusterID] = &Cluster{
		ClusterID:          clusterID,
		ReplicationGroupID: replicationGroupID,
		Engine:             engineRedis,
		EngineVersion:      versionRedis710,
		Status:             statusAvailable,
		NodeType:           nodeTypeT3Micro,
		Region:             b.region,
		ARN:                b.clusterARN(b.region, clusterID),
	}
}

// AddSnapshotInternal seeds an automated snapshot for a given replication group (uses default region).
func AddSnapshotInternal(b *InMemoryBackend, snapshotName, replicationGroupID, snapshotSource string) {
	b.mu.Lock("AddSnapshotInternal")
	defer b.mu.Unlock()

	b.snapshotsStore(b.region)[snapshotName] = &CacheSnapshot{
		SnapshotName:       snapshotName,
		ReplicationGroupID: replicationGroupID,
		SnapshotSource:     snapshotSource,
		Status:             statusAvailable,
		ARN:                b.snapshotARN(b.region, snapshotName),
	}
}
