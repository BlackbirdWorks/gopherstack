package guardduty

// GetCoverageStatistics returns coverage statistics for a detector, aggregated
// only by the requested statisticsType entries (wire values
// COUNT_BY_RESOURCE_TYPE/COUNT_BY_COVERAGE_STATUS -- verified against
// aws-sdk-go-v2/service/guardduty's types.CoverageStatisticsType). ListCoverage
// never tracks any real coverage resources in this backend, so both counts are
// always empty maps; the fix here is that only the requested keys are present
// in the response, matching real GetCoverageStatisticsOutput's
// CoverageStatistics (which never populates a count map the caller didn't ask
// for), instead of always returning both regardless of statisticsType.
func (b *InMemoryBackend) GetCoverageStatistics(detectorID string, statisticsType []string) (map[string]any, error) {
	b.mu.RLock("GetCoverageStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	coverageStats := map[string]any{}

	for _, t := range statisticsType {
		switch t {
		case "COUNT_BY_RESOURCE_TYPE":
			coverageStats["countByResourceType"] = map[string]any{}
		case "COUNT_BY_COVERAGE_STATUS":
			coverageStats["countByCoverageStatus"] = map[string]any{}
		default:
			return nil, ErrValidation
		}
	}

	return map[string]any{"coverageStatistics": coverageStats}, nil
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
