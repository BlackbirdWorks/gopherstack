package inspector2

// ListCoverage returns a list of covered resources (stub — always empty).
func (b *InMemoryBackend) ListCoverage(_ map[string]any, _ int32, _ string) ([]*CoverageEntry, string, error) {
	return []*CoverageEntry{}, "", nil
}

// ListCoverageStatistics returns coverage statistics (stub).
func (b *InMemoryBackend) ListCoverageStatistics(_ map[string]any) (map[string]any, error) {
	return map[string]any{
		"countsByGroup": []any{},
		"totalCounts":   int64(0),
	}, nil
}
