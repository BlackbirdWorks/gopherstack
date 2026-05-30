package guardduty

// DetectorCount returns the number of stored detectors.
func DetectorCount(b *InMemoryBackend) int {
	b.mu.RLock("DetectorCount")
	defer b.mu.RUnlock()

	return len(b.detectors)
}

// FilterCount returns the number of stored filters for a detector.
func FilterCount(b *InMemoryBackend, detectorID string) int {
	b.mu.RLock("FilterCount")
	defer b.mu.RUnlock()

	return len(b.filters[detectorID])
}

// FindingCount returns the number of stored findings for a detector.
func FindingCount(b *InMemoryBackend, detectorID string) int {
	b.mu.RLock("FindingCount")
	defer b.mu.RUnlock()

	return len(b.findings[detectorID])
}

// IPSetCount returns the number of stored IP sets for a detector.
func IPSetCount(b *InMemoryBackend, detectorID string) int {
	b.mu.RLock("IPSetCount")
	defer b.mu.RUnlock()

	return len(b.ipSets[detectorID])
}

// ThreatIntelSetCount returns the number of stored threat intel sets for a detector.
func ThreatIntelSetCount(b *InMemoryBackend, detectorID string) int {
	b.mu.RLock("ThreatIntelSetCount")
	defer b.mu.RUnlock()

	return len(b.threatIntelSets[detectorID])
}

// HandlerOpsLen returns the count of GetSupportedOperations.
func HandlerOpsLen(h *Handler) int {
	return len(h.GetSupportedOperations())
}
