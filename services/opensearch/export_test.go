package opensearch

// DomainCount returns the number of domains in the backend.
func DomainCount(b *InMemoryBackend) int {
	b.mu.RLock("DomainCount")
	defer b.mu.RUnlock()

	return len(b.domains)
}

// ConnectionCount returns the number of inbound connections in the backend.
func ConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("ConnectionCount")
	defer b.mu.RUnlock()

	return len(b.inboundConnections)
}

// DataSourceCount returns the total number of domain data sources across all domains.
func DataSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("DataSourceCount")
	defer b.mu.RUnlock()

	total := 0
	for _, m := range b.domainDataSources {
		total += len(m)
	}

	return total
}

// DirectQueryDataSourceCount returns the number of direct-query data sources.
func DirectQueryDataSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("DirectQueryDataSourceCount")
	defer b.mu.RUnlock()

	return len(b.directQueryDataSources)
}

// ApplicationCount returns the number of applications in the backend.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// ARNIndexSize returns the number of entries in the ARN index.
func ARNIndexSize(b *InMemoryBackend) int {
	b.mu.RLock("ARNIndexSize")
	defer b.mu.RUnlock()

	return len(b.arnIndex)
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
