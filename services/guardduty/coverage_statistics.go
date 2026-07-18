package guardduty

// GetCoverageStatistics returns coverage statistics for a detector.
func (b *InMemoryBackend) GetCoverageStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetCoverageStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"coverageStatistics": map[string]any{
			"countByResourceType":   map[string]any{},
			"countByCoverageStatus": map[string]any{},
		},
	}, nil
}

// ListCoverage returns coverage resources for a detector.
func (b *InMemoryBackend) ListCoverage(detectorID string) ([]map[string]any, error) {
	b.mu.RLock("ListCoverage")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	return []map[string]any{}, nil
}
