package ecs

// ClusterCount returns the clusters held in region.
func (h *Handler) ClusterCount(region string) int {
	for _, sb := range h.RegionBackends() {
		if b, ok := sb.(*InMemoryBackend); ok && b.region == region {
			return b.clusterCount()
		}
	}

	return 0
}

func (b *InMemoryBackend) clusterCount() int {
	b.mu.RLock("clusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters.All())
}
