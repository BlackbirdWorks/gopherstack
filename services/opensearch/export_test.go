package opensearch

import "time"

// ExpireDomainProcessing forces a domain's processing window into the past so
// tests can deterministically observe the settled (post-window) state without a
// wall-clock sleep. It leaves the ProcessingStatus intact for delete windows.
func ExpireDomainProcessing(b *InMemoryBackend, name string) {
	b.mu.Lock("ExpireDomainProcessing")
	defer b.mu.Unlock()

	if d, ok := b.domains.Get(name); ok {
		d.ProcessingUntil = time.Now().Add(-time.Hour)
	}
}

// ExpireCollectionStatus forces a serverless collection's CREATING/DELETING
// window into the past.
func ExpireCollectionStatus(b *InMemoryBackend, id string) {
	b.mu.Lock("ExpireCollectionStatus")
	defer b.mu.Unlock()

	for _, c := range b.slCollections.All() {
		if c.ID == id {
			c.StatusUntil = time.Now().Add(-time.Hour)
		}
	}
}

// ExpireOutboundConnection forces an outbound connection's DELETING window into
// the past.
func ExpireOutboundConnection(b *InMemoryBackend, id string) {
	b.mu.Lock("ExpireOutboundConnection")
	defer b.mu.Unlock()

	if c, ok := b.outboundConnections.Get(id); ok {
		c.StatusUntil = time.Now().Add(-time.Hour)
	}
}

// ExpireVpcEndpoint forces a VPC endpoint's DELETING window into the past.
func ExpireVpcEndpoint(b *InMemoryBackend, id string) {
	b.mu.Lock("ExpireVpcEndpoint")
	defer b.mu.Unlock()

	if ep, ok := b.vpcEndpoints.Get(id); ok {
		ep.StatusUntil = time.Now().Add(-time.Hour)
	}
}

// DomainProcessingState resolves the (Processing, UpgradeProcessing,
// DomainProcessingStatus) reported for a domain copy at the current instant,
// mirroring what toDomainStatusJSON emits over the wire.
func DomainProcessingState(d *Domain) (bool, bool, string) {
	return domainProcessing(d, time.Now())
}

// DomainServiceSoftwareStatus returns the stored service-software update status
// for a domain, or an empty string when none is scheduled.
func DomainServiceSoftwareStatus(b *InMemoryBackend, name string) string {
	b.mu.RLock("DomainServiceSoftwareStatus")
	defer b.mu.RUnlock()

	d, ok := b.domains.Get(name)
	if !ok || d.ServiceSoftware == nil {
		return ""
	}

	return d.ServiceSoftware.UpdateStatus
}

// DomainCount returns the number of domains in the backend.
func DomainCount(b *InMemoryBackend) int {
	b.mu.RLock("DomainCount")
	defer b.mu.RUnlock()

	return b.domains.Len()
}

// ConnectionCount returns the number of inbound connections in the backend.
func ConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return b.inboundConnections.Len()
}

// DataSourceCount returns the total number of domain data sources across all domains.
func DataSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("DataSourceCount")
	defer b.mu.RUnlock()

	return b.domainDataSources.Len()
}

// DirectQueryDataSourceCount returns the number of direct-query data sources.
func DirectQueryDataSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("DirectQueryDataSourceCount")
	defer b.mu.RUnlock()

	return b.directQueryDataSources.Len()
}

// ApplicationCount returns the number of applications in the backend.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return b.applications.Len()
}

// ARNIndexSize returns the number of entries in the ARN index.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return b.domainsByARN.Len()
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// UpgradeHistoryLen returns the number of upgrade history entries for a domain.
func UpgradeHistoryLen(b *InMemoryBackend, domainName string) int {
	b.mu.RLock("UpgradeHistoryLen")
	defer b.mu.RUnlock()

	return len(b.upgradeHistory[upgradeHistoryKey(domainName)])
}

// MaintenancesLen returns the number of maintenance records for a domain.
func MaintenancesLen(b *InMemoryBackend, domainName string) int {
	b.mu.RLock("MaintenancesLen")
	defer b.mu.RUnlock()

	return len(b.domainMaintenances[domainName])
}

// MaxUpgradeHistoryPerDomain exposes the cap constant for testing.
const MaxUpgradeHistoryPerDomain = maxUpgradeHistoryPerDomain

// MaxMaintenancesPerDomain exposes the cap constant for testing.
const MaxMaintenancesPerDomain = maxMaintenancesPerDomain

// SeedInboundConnection inserts an inbound connection directly into the backend
// for test setup. AWS creates inbound connections via the remote cluster's outbound
// connection request; this bypasses the usual flow for unit-test seeding.
func SeedInboundConnection(b *InMemoryBackend, connectionID string) {
	b.mu.Lock("SeedInboundConnection")
	defer b.mu.Unlock()

	b.inboundConnections.Put(&InboundConnection{
		ConnectionID:   connectionID,
		ConnectionMode: connectionModeDirect,
		Status:         connStatusPendingAcceptance,
		LocalDomainInfo: DomainInformation{
			DomainName: "seeded-local-domain",
		},
		RemoteDomainInfo: DomainInformation{
			DomainName: "seeded-remote-domain",
		},
	})
}

// AddScheduledActionInternal seeds a scheduled action directly into the
// backend for test setup. Real AWS creates scheduled actions automatically
// (e.g. ahead of a service-software update); UpdateScheduledAction can only
// reschedule one that already exists, so tests need this to seed the initial
// action.
func AddScheduledActionInternal(b *InMemoryBackend, domainName string, action *ScheduledAction) {
	b.mu.Lock("AddScheduledActionInternal")
	defer b.mu.Unlock()

	cp := *action
	b.scheduledActions[domainName] = append(b.scheduledActions[domainName], &cp)
}
