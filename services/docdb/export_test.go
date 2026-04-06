package docdb

// ClusterCount returns the number of clusters stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// InstanceCount returns the number of instances stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) InstanceCount() int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return len(b.instances)
}

// SubnetGroupCount returns the number of subnet groups stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) SubnetGroupCount() int {
	b.mu.RLock("SubnetGroupCount")
	defer b.mu.RUnlock()

	return len(b.subnetGroups)
}

// ParameterGroupCount returns the number of cluster parameter groups stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) ParameterGroupCount() int {
	b.mu.RLock("ParameterGroupCount")
	defer b.mu.RUnlock()

	return len(b.clusterParameterGroups)
}

// SnapshotCount returns the number of cluster snapshots stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) SnapshotCount() int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return len(b.clusterSnapshots)
}

// EventSubscriptionCount returns the number of event subscriptions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) EventSubscriptionCount() int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return len(b.eventSubscriptions)
}

// GlobalClusterCount returns the number of global clusters stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) GlobalClusterCount() int {
	b.mu.RLock("GlobalClusterCount")
	defer b.mu.RUnlock()

	return len(b.globalClusters)
}
