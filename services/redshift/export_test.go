package redshift

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return len(b.clusters)
}

// ReservedNodeCount returns the number of reserved nodes in the backend.
func ReservedNodeCount(b *InMemoryBackend) int {
	b.mu.RLock("ReservedNodeCount")
	defer b.mu.RUnlock()

	return len(b.reservedNodes)
}

// PartnerCount returns the number of partners in the backend.
func PartnerCount(b *InMemoryBackend) int {
	b.mu.RLock("PartnerCount")
	defer b.mu.RUnlock()

	return len(b.partners)
}

// DataShareCount returns the number of data shares in the backend.
func DataShareCount(b *InMemoryBackend) int {
	b.mu.RLock("DataShareCount")
	defer b.mu.RUnlock()

	return len(b.dataShares)
}

// SecurityGroupCount returns the number of security groups in the backend.
func SecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SecurityGroupCount")
	defer b.mu.RUnlock()

	return len(b.securityGroups)
}

// SnapshotCount returns the number of snapshots in the backend.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return len(b.snapshots)
}

// EndpointAuthCount returns the number of endpoint authorizations in the backend.
func EndpointAuthCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointAuthCount")
	defer b.mu.RUnlock()

	return len(b.endpointAuths)
}

// ActiveResizeCount returns the number of active resize operations in the backend.
func ActiveResizeCount(b *InMemoryBackend) int {
	b.mu.RLock("ActiveResizeCount")
	defer b.mu.RUnlock()

	return len(b.activeResizes)
}

// HandlerOpsLen returns the number of operations registered in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}
