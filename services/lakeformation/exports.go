package lakeformation

// ExportedResourceInfo is a compatibility alias used by the dashboard package.
type ExportedResourceInfo = ResourceInfo

// ResourceCount returns the number of registered resources in the backend (test helper).
func (b *InMemoryBackend) ResourceCount() int {
	b.mu.RLock("ResourceCount")
	defer b.mu.RUnlock()

	return len(b.resources)
}

// TagCount returns the number of LF-tags in the backend (test helper).
func (b *InMemoryBackend) TagCount() int {
	b.mu.RLock("TagCount")
	defer b.mu.RUnlock()

	return len(b.lfTags)
}

// PermissionCount returns the number of permission entries in the backend (test helper).
func (b *InMemoryBackend) PermissionCount() int {
	b.mu.RLock("PermissionCount")
	defer b.mu.RUnlock()

	return len(b.permissionsList)
}

// DataCellsFilterCount returns the number of data cells filters in the backend (test helper).
func (b *InMemoryBackend) DataCellsFilterCount() int {
	b.mu.RLock("DataCellsFilterCount")
	defer b.mu.RUnlock()

	return len(b.dataCellsFilters)
}

// LFTagExpressionCount returns the number of LF-tag expressions in the backend (test helper).
func (b *InMemoryBackend) LFTagExpressionCount() int {
	b.mu.RLock("LFTagExpressionCount")
	defer b.mu.RUnlock()

	return len(b.lfTagExpressions)
}

// OptInCount returns the number of opt-in entries in the backend (test helper).
func (b *InMemoryBackend) OptInCount() int {
	b.mu.RLock("OptInCount")
	defer b.mu.RUnlock()

	return len(b.lakeFormationOptIns)
}

// TransactionCount returns the number of stored transactions in the backend (test helper).
func (b *InMemoryBackend) TransactionCount() int {
	b.mu.RLock("TransactionCount")
	defer b.mu.RUnlock()

	return len(b.transactions)
}

// HandlerOpsLen returns the number of cached dispatch operations in the handler (test helper).
func (h *Handler) HandlerOpsLen() int {
	return len(h.ops)
}

// ResourceLFTagCount returns the number of resources with LF-tag associations (test helper).
func (b *InMemoryBackend) ResourceLFTagCount() int {
	b.mu.RLock("ResourceLFTagCount")
	defer b.mu.RUnlock()

	return len(b.resourceLFTags)
}

// IdentityCenterConfigCount returns the number of Identity Center configurations (test helper).
func (b *InMemoryBackend) IdentityCenterConfigCount() int {
	b.mu.RLock("IdentityCenterConfigCount")
	defer b.mu.RUnlock()

	return len(b.identityCenterConfigs)
}
