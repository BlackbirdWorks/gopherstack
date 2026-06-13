package inspector2

// FilterCount returns the number of stored filters.
func FilterCount(b *InMemoryBackend) int {
	b.mu.RLock("FilterCount")
	defer b.mu.RUnlock()

	return len(b.filters)
}

// FindingCount returns the number of stored findings.
func FindingCount(b *InMemoryBackend) int {
	b.mu.RLock("FindingCount")
	defer b.mu.RUnlock()

	return len(b.findings)
}

// SeedFinding adds a finding directly to the backend for testing.
func SeedFinding(
	b *InMemoryBackend,
	findingType, severityLabel, status, title, description string,
	resources []FindingResource,
) string {
	return b.AddFinding(findingType, severityLabel, status, title, description, resources)
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
