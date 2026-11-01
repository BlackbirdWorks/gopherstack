package gcpcontainer

// NodePoolSizes reports the current node count per zone of each node pool of a cluster, by pool name. The API
// itself exposes the count only through the cluster's totals.
func (b *InMemoryBackend) NodePoolSizes(clusterName string) (map[string]int32, error) {
	r, err := resolveCluster(clusterName, "", "", "")
	if err != nil {
		return nil, err
	}

	b.mu.RLock("NodePoolSizes")
	defer b.mu.RUnlock()

	c, err := b.liveClusterLocked(r)
	if err != nil {
		return nil, err
	}

	out := make(map[string]int32, len(c.Pools))
	for _, p := range c.Pools {
		out[p.Pool.GetName()] = p.Size
	}

	return out, nil
}
