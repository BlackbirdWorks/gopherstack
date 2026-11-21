package neptune

import "github.com/blackbirdworks/gopherstack/services/neptune/opencypher"

// ClusterCount returns the number of clusters in the backend across all regions.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return b.clusters.Len()
}

// InstanceCount returns the number of DB instances in the backend across all regions.
func InstanceCount(b *InMemoryBackend) int {
	b.mu.RLock("InstanceCount")
	defer b.mu.RUnlock()

	return b.instances.Len()
}

// SubnetGroupCount returns the number of subnet groups in the backend across all regions.
func SubnetGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SubnetGroupCount")
	defer b.mu.RUnlock()

	return b.subnetGroups.Len()
}

// ClusterParameterGroupCount returns the number of cluster parameter groups across all regions.
func ClusterParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterParameterGroupCount")
	defer b.mu.RUnlock()

	return b.clusterParameterGroups.Len()
}

// ClusterSnapshotCount returns the number of cluster snapshots in the backend across all regions.
func ClusterSnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterSnapshotCount")
	defer b.mu.RUnlock()

	return b.clusterSnapshots.Len()
}

// ParameterGroupCount returns the number of DB parameter groups across all regions.
func ParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ParameterGroupCount")
	defer b.mu.RUnlock()

	return b.parameterGroups.Len()
}

// ClusterEndpointCount returns the number of cluster endpoints across all regions.
func ClusterEndpointCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterEndpointCount")
	defer b.mu.RUnlock()

	return b.clusterEndpoints.Len()
}

// EventSubscriptionCount returns the number of event subscriptions across all regions.
func EventSubscriptionCount(b *InMemoryBackend) int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return b.eventSubscriptions.Len()
}

// GlobalClusterCount returns the number of global clusters (partition-scoped).
func GlobalClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("GlobalClusterCount")
	defer b.mu.RUnlock()

	return b.globalClusters.Len()
}

// TagCount returns the total number of tag entries across all resources and regions.
func TagCount(b *InMemoryBackend) int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	total := 0
	for _, regionTags := range b.tags {
		for _, tags := range regionTags {
			total += len(tags)
		}
	}

	return total
}

// ClusterRoleCount returns the number of IAM roles associated with a cluster in the default region.
func ClusterRoleCount(b *InMemoryBackend, clusterID string) int {
	b.mu.RLock("ClusterRoleCount")
	defer b.mu.RUnlock()

	return len(b.clusterRoles[b.region][clusterID])
}

// HandlerOpsLen returns the number of operations listed in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// SetLoaderCPUs overrides the vCPU count the bulk loader derives its thread pool from.
func SetLoaderCPUs(b *InMemoryBackend, n int) {
	b.mu.Lock("SetLoaderCPUs")
	defer b.mu.Unlock()

	b.loaderCPUs = n
}

// LoaderThreads exposes the parallelism-to-thread-count mapping.
func LoaderThreads(level string, cpus int) int { return loaderThreads(level, cpus) }

// HoldIndexRebuilds keeps index rebuild jobs of the cluster in PREPARING until release is called.
func HoldIndexRebuilds(b *InMemoryBackend, region, cluster string) func() {
	b.mu.Lock("HoldIndexRebuilds")
	defer b.mu.Unlock()

	r := b.rdfLocked(regionKey(region, cluster))
	ch := make(chan struct{})

	r.rebuilds.mu.Lock("hold")
	r.rebuilds.gate = ch
	r.rebuilds.mu.Unlock()

	return func() { close(ch) }
}

// HoldBackendLock write-locks the backend until the returned release is called.
func HoldBackendLock(b *InMemoryBackend) func() {
	b.mu.Lock("HoldBackendLock")

	return b.mu.Unlock
}

// ClusterGraph returns the cluster's openCypher graph.
func ClusterGraph(b *InMemoryBackend, id string) *opencypher.Graph {
	b.mu.RLock("ClusterGraph")
	defer b.mu.RUnlock()

	return b.graphs[regionKey(b.region, id)]
}
