package eks

// ClusterCount returns the number of clusters stored in the backend.
// Used only in tests to verify backend state without going through the HTTP handler.
func (b *InMemoryBackend) ClusterCount() int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return b.clusters.Len()
}

// NodegroupCount returns the total number of node groups across all clusters.
func (b *InMemoryBackend) NodegroupCount() int {
	b.mu.RLock("NodegroupCount")
	defer b.mu.RUnlock()

	return b.nodegroups.Len()
}

// AccessEntryCount returns the total number of access entries across all clusters.
func (b *InMemoryBackend) AccessEntryCount() int {
	b.mu.RLock("AccessEntryCount")
	defer b.mu.RUnlock()

	return b.accessEntries.Len()
}

// AddonCount returns the total number of add-ons across all clusters.
func (b *InMemoryBackend) AddonCount() int {
	b.mu.RLock("AddonCount")
	defer b.mu.RUnlock()

	return b.addons.Len()
}

// FargateProfileCount returns the total number of Fargate profiles across all clusters.
func (b *InMemoryBackend) FargateProfileCount() int {
	b.mu.RLock("FargateProfileCount")
	defer b.mu.RUnlock()

	return b.fargateProfiles.Len()
}

// PodIdentityAssociationCount returns the total number of pod identity associations.
func (b *InMemoryBackend) PodIdentityAssociationCount() int {
	b.mu.RLock("PodIdentityAssociationCount")
	defer b.mu.RUnlock()

	return b.podIdentityAssociations.Len()
}

// CapabilityCount returns the number of capabilities stored in the backend.
func (b *InMemoryBackend) CapabilityCount() int {
	b.mu.RLock("CapabilityCount")
	defer b.mu.RUnlock()

	return b.capabilities.Len()
}

// SubscriptionCount returns the number of EKS Anywhere subscriptions stored.
func (b *InMemoryBackend) SubscriptionCount() int {
	b.mu.RLock("SubscriptionCount")
	defer b.mu.RUnlock()

	return b.subscriptions.Len()
}
