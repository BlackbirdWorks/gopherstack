package eks

// ClusterCount returns the number of clusters stored in the backend.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// NodegroupCount returns the total number of node groups across all clusters.
func (b *InMemoryBackend) NodegroupCount() int {
	b.mu.RLock("NodegroupCount")
	defer b.mu.RUnlock()

	total := 0

	for _, ngs := range b.nodegroups {
		total += len(ngs)
	}

	return total
}

// AccessEntryCount returns the total number of access entries across all clusters.
func (b *InMemoryBackend) AccessEntryCount() int {
	b.mu.RLock("AccessEntryCount")
	defer b.mu.RUnlock()

	total := 0

	for _, entries := range b.accessEntries {
		total += len(entries)
	}

	return total
}

// AddonCount returns the total number of add-ons across all clusters.
func (b *InMemoryBackend) AddonCount() int {
	b.mu.RLock("AddonCount")
	defer b.mu.RUnlock()

	total := 0

	for _, addons := range b.addons {
		total += len(addons)
	}

	return total
}

// FargateProfileCount returns the total number of Fargate profiles across all clusters.
func (b *InMemoryBackend) FargateProfileCount() int {
	b.mu.RLock("FargateProfileCount")
	defer b.mu.RUnlock()

	total := 0

	for _, profiles := range b.fargateProfiles {
		total += len(profiles)
	}

	return total
}

// PodIdentityAssociationCount returns the total number of pod identity associations.
func (b *InMemoryBackend) PodIdentityAssociationCount() int {
	b.mu.RLock("PodIdentityAssociationCount")
	defer b.mu.RUnlock()

	total := 0

	for _, assocs := range b.podIdentityAssociations {
		total += len(assocs)
	}

	return total
}

// CapabilityCount returns the number of capabilities stored in the backend.
func (b *InMemoryBackend) CapabilityCount() int {
	b.mu.RLock("CapabilityCount")
	defer b.mu.RUnlock()

	return len(b.capabilities)
}

// SubscriptionCount returns the number of EKS Anywhere subscriptions stored.
func (b *InMemoryBackend) SubscriptionCount() int {
	b.mu.RLock("SubscriptionCount")
	defer b.mu.RUnlock()

	return len(b.subscriptions)
}
