package redshift

import "time"

// ClusterCount returns the number of clusters in the backend.
func ClusterCount(b *InMemoryBackend) int {
	b.mu.RLock("ClusterCount")
	defer b.mu.RUnlock()

	return b.clusters.Len()
}

// ReservedNodeCount returns the number of reserved nodes in the backend.
func ReservedNodeCount(b *InMemoryBackend) int {
	b.mu.RLock("ReservedNodeCount")
	defer b.mu.RUnlock()

	return b.reservedNodes.Len()
}

// PartnerCount returns the number of partners in the backend.
func PartnerCount(b *InMemoryBackend) int {
	b.mu.RLock("PartnerCount")
	defer b.mu.RUnlock()

	return b.partners.Len()
}

// DataShareCount returns the number of data shares in the backend.
func DataShareCount(b *InMemoryBackend) int {
	b.mu.RLock("DataShareCount")
	defer b.mu.RUnlock()

	return b.dataShares.Len()
}

// SecurityGroupCount returns the number of security groups in the backend.
func SecurityGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SecurityGroupCount")
	defer b.mu.RUnlock()

	return b.securityGroups.Len()
}

// SnapshotCount returns the number of snapshots in the backend.
func SnapshotCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCount")
	defer b.mu.RUnlock()

	return b.snapshots.Len()
}

// EndpointAuthCount returns the number of endpoint authorizations in the backend.
func EndpointAuthCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointAuthCount")
	defer b.mu.RUnlock()

	return b.endpointAuths.Len()
}

// ActiveResizeCount returns the number of active resize operations in the backend.
func ActiveResizeCount(b *InMemoryBackend) int {
	b.mu.RLock("ActiveResizeCount")
	defer b.mu.RUnlock()

	return len(b.activeResizes)
}

// LoggingStatusCount returns the number of per-cluster logging statuses held
// by the backend (a raw map -- see store_setup.go for why it isn't a
// store.Table).
func LoggingStatusCount(b *InMemoryBackend) int {
	b.mu.RLock("LoggingStatusCount")
	defer b.mu.RUnlock()

	return len(b.loggingStatuses)
}

// SnapshotCopyConfigCount returns the number of cross-region snapshot copy
// configs held by the backend (a raw map -- see store_setup.go for why it
// isn't a store.Table).
func SnapshotCopyConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("SnapshotCopyConfigCount")
	defer b.mu.RUnlock()

	return len(b.snapshotCopyConfigs)
}

// ParameterGroupCount returns the number of parameter groups in the backend.
func ParameterGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("ParameterGroupCount")
	defer b.mu.RUnlock()

	return b.parameterGroups.Len()
}

// SubnetGroupCount returns the number of subnet groups in the backend.
func SubnetGroupCount(b *InMemoryBackend) int {
	b.mu.RLock("SubnetGroupCount")
	defer b.mu.RUnlock()

	return b.subnetGroups.Len()
}

// EventSubscriptionCount returns the number of event subscriptions in the backend.
func EventSubscriptionCount(b *InMemoryBackend) int {
	b.mu.RLock("EventSubscriptionCount")
	defer b.mu.RUnlock()

	return b.eventSubscriptions.Len()
}

// HsmClientCertCount returns the number of HSM client certificates in the backend.
func HsmClientCertCount(b *InMemoryBackend) int {
	b.mu.RLock("HsmClientCertCount")
	defer b.mu.RUnlock()

	return b.hsmClientCerts.Len()
}

// HsmConfigCount returns the number of HSM configurations in the backend.
func HsmConfigCount(b *InMemoryBackend) int {
	b.mu.RLock("HsmConfigCount")
	defer b.mu.RUnlock()

	return b.hsmConfigs.Len()
}

// ScheduledActionCount returns the number of scheduled actions in the backend.
func ScheduledActionCount(b *InMemoryBackend) int {
	b.mu.RLock("ScheduledActionCount")
	defer b.mu.RUnlock()

	return b.scheduledActions.Len()
}

// CustomDomainCount returns the number of custom domain associations in the backend.
func CustomDomainCount(b *InMemoryBackend) int {
	b.mu.RLock("CustomDomainCount")
	defer b.mu.RUnlock()

	return b.customDomains.Len()
}

// EndpointAccessCount returns the number of endpoint accesses in the backend.
func EndpointAccessCount(b *InMemoryBackend) int {
	b.mu.RLock("EndpointAccessCount")
	defer b.mu.RUnlock()

	return b.endpointAccesses.Len()
}

// IntegrationCount returns the number of integrations in the backend.
func IntegrationCount(b *InMemoryBackend) int {
	b.mu.RLock("IntegrationCount")
	defer b.mu.RUnlock()

	return b.integrations.Len()
}

// IdcApplicationCount returns the number of IDC applications in the backend.
func IdcApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("IdcApplicationCount")
	defer b.mu.RUnlock()

	return b.idcApplications.Len()
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

// SetReconcileInterval overrides the background reconciler tick interval. Must be
// called before StartReconciler. Used by tests to advance state quickly.
func SetReconcileInterval(b *InMemoryBackend, d time.Duration) {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()

	b.reconcileInterval = d
}

// PendingClusterTransitions returns the number of scheduled (not-yet-applied)
// cluster lifecycle transitions currently held by the backend.
func PendingClusterTransitions(b *InMemoryBackend) int {
	b.mu.RLock("PendingClusterTransitions")
	defer b.mu.RUnlock()

	return len(b.clusterTransitions)
}

// ReconcilerRunning reports whether the managed reconciler goroutine is active.
func ReconcilerRunning(b *InMemoryBackend) bool {
	b.reconcileMu.Lock()
	defer b.reconcileMu.Unlock()

	return b.reconcileOn
}

// ServerlessIndexLen returns the number of keys tracked by a serverless sorted
// index, selected by name ("namespace", "workgroup", "snapshot", "usagelimit",
// "scheduledaction"). Returns -1 for an unknown selector.
func ServerlessIndexLen(b *InMemoryBackend, which string) int {
	b.mu.RLock("ServerlessIndexLen")
	defer b.mu.RUnlock()

	switch which {
	case "namespace":
		return len(b.slNamespaceIdx.keys)
	case "workgroup":
		return len(b.slWorkgroupIdx.keys)
	case "snapshot":
		return len(b.slSnapshotIdx.keys)
	case "usagelimit":
		return len(b.slUsageLimitIdx.keys)
	case "scheduledaction":
		return len(b.slScheduledActionIdx.keys)
	default:
		return -1
	}
}
