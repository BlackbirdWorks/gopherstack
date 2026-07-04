package medialive

// ChannelCount returns the number of stored channels.
func ChannelCount(b *InMemoryBackend) int {
	b.mu.RLock("ChannelCount")
	defer b.mu.RUnlock()

	return len(b.channels)
}

// InputCount returns the number of stored inputs.
func InputCount(b *InMemoryBackend) int {
	b.mu.RLock("InputCount")
	defer b.mu.RUnlock()

	return len(b.inputs)
}

// InputSecurityGroupCount returns the number of stored input security groups.
func InputSecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("InputSecurityGroupCount")
	defer b.mu.RUnlock()

	return len(b.inputSecurityGroups)
}

// InputDeviceCount returns the number of stored input devices.
func InputDeviceCount(b *InMemoryBackend) int {
	b.mu.RLock("InputDeviceCount")
	defer b.mu.RUnlock()

	return len(b.inputDevices)
}

// MultiplexCount returns the number of stored multiplexes.
func MultiplexCount(b *InMemoryBackend) int {
	b.mu.RLock("MultiplexCount")
	defer b.mu.RUnlock()

	return len(b.multiplexes)
}

// MultiplexProgramCount returns the number of programs in a multiplex.
func MultiplexProgramCount(b *InMemoryBackend, multiplexID string) int {
	b.mu.RLock("MultiplexProgramCount")
	defer b.mu.RUnlock()

	m, ok := b.multiplexes[multiplexID]
	if !ok {
		return 0
	}

	return len(m.Programs)
}

// ClusterCount returns the number of stored clusters.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// NodeCount returns the number of nodes in a cluster.
func NodeCount(b *InMemoryBackend, clusterID string) int {
	b.mu.RLock("NodeCount")
	defer b.mu.RUnlock()

	c, ok := b.clusters[clusterID]
	if !ok {
		return 0
	}

	return len(c.Nodes)
}

// ChannelPlacementGroupCount returns the number of placement groups in a cluster.
func ChannelPlacementGroupCount(b *InMemoryBackend, clusterID string) int {
	b.mu.RLock("ChannelPlacementGroupCount")
	defer b.mu.RUnlock()

	prefix := clusterID + "/"
	count := 0

	for k := range b.channelPlacementGroups {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			count++
		}
	}

	return count
}

// NetworkCount returns the number of stored networks.
func NetworkCount(b *InMemoryBackend) int {
	b.mu.RLock("NetworkCount")
	defer b.mu.RUnlock()

	return len(b.networks)
}

// SdiSourceCount returns the number of stored SDI sources.
func SdiSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("SdiSourceCount")
	defer b.mu.RUnlock()

	return len(b.sdiSources)
}

// ForceClusterState sets the state of a cluster directly, for testing purposes.
func ForceClusterState(b *InMemoryBackend, clusterID, state string) {
	b.mu.Lock("ForceClusterState")
	defer b.mu.Unlock()

	if c, ok := b.clusters[clusterID]; ok {
		c.State = state
	}
}
