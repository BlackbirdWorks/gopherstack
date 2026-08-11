package memorydb

// ExportedCluster is a compatibility alias used by the dashboard package.
type ExportedCluster = Cluster

// The following exported aliases are used by external tests.

// ExportedCreateClusterRequest aliases createClusterRequest for testing.
type ExportedCreateClusterRequest = createClusterRequest

// ExportedCreateACLRequest aliases createACLRequest for testing.
type ExportedCreateACLRequest = createACLRequest

// ExportedCreateSubnetGroupRequest aliases createSubnetGroupRequest for testing.
type ExportedCreateSubnetGroupRequest = createSubnetGroupRequest

// ExportedCreateUserRequest aliases createUserRequest for testing.
type ExportedCreateUserRequest = createUserRequest

// ExportedAuthModeReq aliases authenticationModeReq for testing.
type ExportedAuthModeReq = authenticationModeReq

// ExportedCreateParameterGroupRequest aliases createParameterGroupRequest for testing.
type ExportedCreateParameterGroupRequest = createParameterGroupRequest

// ExportedCreateSnapshotRequest aliases createSnapshotRequest for testing.
type ExportedCreateSnapshotRequest = createSnapshotRequest

// ExportedCopySnapshotRequest aliases copySnapshotRequest for testing.
type ExportedCopySnapshotRequest = copySnapshotRequest

// ExportedCreateMultiRegionClusterRequest aliases createMultiRegionClusterRequest for testing.
type ExportedCreateMultiRegionClusterRequest = createMultiRegionClusterRequest

// ExportedPurchaseReservedNodesOfferingRequest aliases purchaseReservedNodesOfferingRequest for testing.
type ExportedPurchaseReservedNodesOfferingRequest = purchaseReservedNodesOfferingRequest

// ExportedDescribeReservedNodesRequest aliases describeReservedNodesRequest for testing.
type ExportedDescribeReservedNodesRequest = describeReservedNodesRequest

// ExportedDescribeEngineVersionsRequest aliases describeEngineVersionsRequest for testing.
type ExportedDescribeEngineVersionsRequest = describeEngineVersionsRequest

// ExportedDescribeEventsRequest aliases describeEventsRequest for testing.
type ExportedDescribeEventsRequest = describeEventsRequest

// ExportedEvent aliases Event for testing.
type ExportedEvent = Event

// ExportedEngineVersion aliases EngineVersion for testing.
type ExportedEngineVersion = EngineVersion

// ExportedUpdateClusterRequest aliases updateClusterRequest for testing.
type ExportedUpdateClusterRequest = updateClusterRequest

// -- Count helpers for testing ---------------------------------------------------

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0
	for _, t := range b.clusters {
		total += t.Len()
	}

	return total
}

// ACLCount returns the number of ACLs in the backend.
func ACLCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0
	for _, t := range b.acls {
		total += t.Len()
	}

	return total
}

// SnapshotCount returns the number of snapshots in the backend.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0
	for _, t := range b.snapshots {
		total += t.Len()
	}

	return total
}

// UserCount returns the number of users in the backend.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0

	for _, t := range b.users {
		total += t.Len()
	}

	return total
}

// SubnetGroupCount returns the number of subnet groups in the backend.
func SubnetGroupCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	total := 0
	for _, t := range b.subnetGroups {
		total += t.Len()
	}

	return total
}

// ParameterGroupCount returns the number of parameter groups in the backend.
func ParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock()

	defer b.mu.RUnlock()
	total := 0
	for _, t := range b.parameterGroups {
		total += t.Len()
	}

	return total
}

// EventCount returns the number of events in the backend.
func EventCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0
	for _, evs := range b.events {
		total += len(evs)
	}

	return total
}

// MultiRegionClusterCount returns the number of multi-region clusters in the backend.
func MultiRegionClusterCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.multiRegionClusters.Len()
}

// ARNIndexSize returns the number of entries in the ARN-to-resource index.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()
	total := 0
	for _, m := range b.arnToResource {
		total += len(m)
	}

	return total
}

// HandlerOpsLen returns the number of supported operations reported by the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// ExportedUpdateMultiRegionClusterRequest aliases updateMultiRegionClusterRequest for testing.
type ExportedUpdateMultiRegionClusterRequest = updateMultiRegionClusterRequest

// ExportedResetParameterGroupRequest aliases resetParameterGroupRequest for testing.
type ExportedResetParameterGroupRequest = resetParameterGroupRequest

// ExportedDescribeServiceUpdatesRequest aliases describeServiceUpdatesRequest for testing.
type ExportedDescribeServiceUpdatesRequest = describeServiceUpdatesRequest
