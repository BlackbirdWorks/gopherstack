package neptune

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// InstanceCount returns the number of DB instances in the backend.
func InstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return len(b.instances)
}

// SubnetGroupCount returns the number of subnet groups in the backend.
func SubnetGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SubnetGroupCount")
	defer b.mu.RUnlock()

	return len(b.subnetGroups)
}

// ClusterParameterGroupCount returns the number of cluster parameter groups in the backend.
func ClusterParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterParameterGroupCount")
	defer b.mu.RUnlock()

	return len(b.clusterParameterGroups)
}

// ClusterSnapshotCount returns the number of cluster snapshots in the backend.
func ClusterSnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterSnapshotCount")
	defer b.mu.RUnlock()

	return len(b.clusterSnapshots)
}

// ParameterGroupCount returns the number of DB parameter groups in the backend.
func ParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ParameterGroupCount")
	defer b.mu.RUnlock()

	return len(b.parameterGroups)
}

// ClusterEndpointCount returns the number of cluster endpoints in the backend.
func ClusterEndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterEndpointCount")
	defer b.mu.RUnlock()

	return len(b.clusterEndpoints)
}

// EventSubscriptionCount returns the number of event subscriptions in the backend.
func EventSubscriptionCount(b *InMemoryBackend) int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return len(b.eventSubscriptions)
}

// GlobalClusterCount returns the number of global clusters in the backend.
func GlobalClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("GlobalClusterCount")
	defer b.mu.RUnlock()

	return len(b.globalClusters)
}

// TagCount returns the total number of tag entries across all resources.
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	total := 0
	for _, tags := range b.tags {
		total += len(tags)
	}

	return total
}

// ClusterRoleCount returns the number of IAM roles associated with a cluster.
func ClusterRoleCount(b *InMemoryBackend, clusterID string) int {
	b.mu.RLock("ClusterRoleCount")
	defer b.mu.RUnlock()

	return len(b.clusterRoles[clusterID])
}

// HandlerOpsLen returns the number of operations listed in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
