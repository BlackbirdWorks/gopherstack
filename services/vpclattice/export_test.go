package vpclattice

// ServiceCount returns the number of stored services.
func ServiceCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceCount")
	defer b.mu.RUnlock()

	return b.services.Len()
}

// ServiceNetworkCount returns the number of stored service networks.
func ServiceNetworkCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceNetworkCount")
	defer b.mu.RUnlock()

	return b.serviceNetworks.Len()
}

// TargetGroupCount returns the number of stored target groups.
func TargetGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("TargetGroupCount")
	defer b.mu.RUnlock()

	return b.targetGroups.Len()
}

// ListenerCount returns the number of stored listeners.
func ListenerCount(b *InMemoryBackend) int {
	b.mu.RLock("ListenerCount")
	defer b.mu.RUnlock()

	return b.listeners.Len()
}

// RuleCount returns the number of stored listener rules.
func RuleCount(b *InMemoryBackend) int {
	b.mu.RLock("RuleCount")
	defer b.mu.RUnlock()

	return b.rules.Len()
}

// ServiceNetworkServiceAssociationCount returns the number of stored SNSAs.
func ServiceNetworkServiceAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceNetworkServiceAssociationCount")
	defer b.mu.RUnlock()

	return b.snsas.Len()
}

// ServiceNetworkVpcAssociationCount returns the number of stored SNVAs.
func ServiceNetworkVpcAssociationCount(b *InMemoryBackend) int {
	b.mu.RLock("ServiceNetworkVpcAssociationCount")
	defer b.mu.RUnlock()

	return b.snvas.Len()
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
