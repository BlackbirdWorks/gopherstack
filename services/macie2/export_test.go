package macie2

// AllowListCount returns the number of stored allow lists.
func AllowListCount(b *InMemoryBackend) int {
	b.mu.RLock("AllowListCount")
	defer b.mu.RUnlock()

	return len(b.allowLists)
}

// CustomDataIDCount returns the number of stored custom data identifiers.
func CustomDataIDCount(b *InMemoryBackend) int {
	b.mu.RLock("CustomDataIDCount")
	defer b.mu.RUnlock()

	return len(b.customDataIDs)
}

// FindingsFilterCount returns the number of stored findings filters.
func FindingsFilterCount(b *InMemoryBackend) int {
	b.mu.RLock("FindingsFilterCount")
	defer b.mu.RUnlock()

	return len(b.findingsFilters)
}

// FindingCount returns the number of stored findings.
func FindingCount(b *InMemoryBackend) int {
	b.mu.RLock("FindingCount")
	defer b.mu.RUnlock()

	return len(b.findings)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
