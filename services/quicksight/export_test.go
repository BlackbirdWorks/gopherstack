package quicksight

// NamespaceCount returns the number of stored namespaces.
func NamespaceCount(b *InMemoryBackend) int {
	b.mu.RLock("NamespaceCount")
	defer b.mu.RUnlock()

	return len(b.namespaces)
}

// GroupCount returns the number of stored groups.
func GroupCount(b *InMemoryBackend) int {
	b.mu.RLock("GroupCount")
	defer b.mu.RUnlock()

	return len(b.groups)
}

// UserCount returns the number of stored users.
func UserCount(b *InMemoryBackend) int {
	b.mu.RLock("UserCount")
	defer b.mu.RUnlock()

	return len(b.users)
}

// DataSourceCount returns the number of stored data sources.
func DataSourceCount(b *InMemoryBackend) int {
	b.mu.RLock("DataSourceCount")
	defer b.mu.RUnlock()

	return len(b.dataSources)
}

// DataSetCount returns the number of stored datasets.
func DataSetCount(b *InMemoryBackend) int {
	b.mu.RLock("DataSetCount")
	defer b.mu.RUnlock()

	return len(b.dataSets)
}

// DashboardCount returns the number of stored dashboards.
func DashboardCount(b *InMemoryBackend) int {
	b.mu.RLock("DashboardCount")
	defer b.mu.RUnlock()

	return len(b.dashboards)
}

// AnalysisCount returns the number of stored analyses.
func AnalysisCount(b *InMemoryBackend) int {
	b.mu.RLock("AnalysisCount")
	defer b.mu.RUnlock()

	return len(b.analyses)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}

// SeedFlow inserts f directly into b's backend state, bypassing the (real)
// AWS API. QuickSight has no CreateFlow operation — flows are authored via
// the console/Quick Suite — so this is the only way tests can populate
// fixtures for ListFlows/SearchFlows/GetFlowMetadata/permissions.
func SeedFlow(b *InMemoryBackend, accountID string, f *Flow) {
	b.seedFlow(accountID, f)
}
