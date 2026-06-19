package accessanalyzer

// FindingCount returns the number of stored findings for an analyzer.
func FindingCount(b *InMemoryBackend, analyzerName string) int {
	b.mu.RLock("FindingCount")
	defer b.mu.RUnlock()

	return len(b.findings[analyzerName])
}

// SeedFinding adds a finding directly to the backend for testing.
func SeedFinding(
	b *InMemoryBackend,
	analyzerName, resourceType, resourceArn string,
) string {
	f, _ := b.AddFinding(analyzerName, resourceType, resourceArn, nil, nil, nil)
	if f == nil {
		return ""
	}

	return f.ID
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
