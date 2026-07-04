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

// FolderCount returns the number of stored folders.
func FolderCount(b *InMemoryBackend) int {
	b.mu.RLock("FolderCount")
	defer b.mu.RUnlock()

	return len(b.folders)
}

// TemplateCount returns the number of stored templates.
func TemplateCount(b *InMemoryBackend) int {
	b.mu.RLock("TemplateCount")
	defer b.mu.RUnlock()

	return len(b.templates)
}

// ThemeCount returns the number of stored themes.
func ThemeCount(b *InMemoryBackend) int {
	b.mu.RLock("ThemeCount")
	defer b.mu.RUnlock()

	return len(b.themes)
}

// VPCConnectionCount returns the number of stored VPC connections.
func VPCConnectionCount(b *InMemoryBackend) int {
	b.mu.RLock("VPCConnectionCount")
	defer b.mu.RUnlock()

	return len(b.vpcConnections)
}

// BrandCount returns the number of stored brands.
func BrandCount(b *InMemoryBackend) int {
	b.mu.RLock("BrandCount")
	defer b.mu.RUnlock()

	return len(b.brands)
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

// SeedSelfUpgradeRequest inserts r directly into b's backend state, bypassing
// the (real) AWS API. QuickSight has no CreateSelfUpgradeRequest operation —
// requests are created by end users self-requesting a role upgrade through the
// console — so this is the only way tests can populate fixtures for
// ListSelfUpgrades/UpdateSelfUpgrade.
func SeedSelfUpgradeRequest(b *InMemoryBackend, accountID, namespace string, r *SelfUpgradeRequestDetail) {
	b.seedSelfUpgradeRequest(accountID, namespace, r)
}
