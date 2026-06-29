package redshift

import "time"

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

// ParameterGroupCount returns the number of parameter groups in the backend.
func ParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ParameterGroupCount")
	defer b.mu.RUnlock()

	return len(b.parameterGroups)
}

// SubnetGroupCount returns the number of subnet groups in the backend.
func SubnetGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SubnetGroupCount")
	defer b.mu.RUnlock()

	return len(b.subnetGroups)
}

// EventSubscriptionCount returns the number of event subscriptions in the backend.
func EventSubscriptionCount(b *InMemoryBackend) int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return len(b.eventSubscriptions)
}

// HsmClientCertCount returns the number of HSM client certificates in the backend.
func HsmClientCertCount(b *InMemoryBackend) int {
	b.mu.RLock("HsmClientCertCount")
	defer b.mu.RUnlock()

	return len(b.hsmClientCerts)
}

// HsmConfigCount returns the number of HSM configurations in the backend.
func HsmConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("HsmConfigCount")
	defer b.mu.RUnlock()

	return len(b.hsmConfigs)
}

// ScheduledActionCount returns the number of scheduled actions in the backend.
func ScheduledActionCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduledActionCount")
	defer b.mu.RUnlock()

	return len(b.scheduledActions)
}

// CustomDomainCount returns the number of custom domain associations in the backend.
func CustomDomainCount(b *InMemoryBackend) int {
	b.mu.RLock("CustomDomainCount")
	defer b.mu.RUnlock()

	return len(b.customDomains)
}

// EndpointAccessCount returns the number of endpoint accesses in the backend.
func EndpointAccessCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointAccessCount")
	defer b.mu.RUnlock()

	return len(b.endpointAccesses)
}

// IntegrationCount returns the number of integrations in the backend.
func IntegrationCount(b *InMemoryBackend) int {
	b.mu.RLock("IntegrationCount")
	defer b.mu.RUnlock()

	return len(b.integrations)
}

// IdcApplicationCount returns the number of IDC applications in the backend.
func IdcApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("IdcApplicationCount")
	defer b.mu.RUnlock()

	return len(b.idcApplications)
}

// HandlerOpsLen returns the number of operations registered in the handler.
func HandlerOpsLen(h *Handler) int {
	return len(h.ops)
}

// SetClusterActivationDelay sets the delay before a newly created cluster
// transitions from "creating" to "available". Set to a positive value to
// test the lifecycle state machine; leave at 0 for instant availability.
func SetClusterActivationDelay(b *InMemoryBackend, d time.Duration) {
	b.clusterActivationDelay = d
}
