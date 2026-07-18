package guardduty

// GetUsageStatistics returns usage statistics for a detector.
func (b *InMemoryBackend) GetUsageStatistics(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetUsageStatistics")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"usageStatistics": map[string]any{
			"sumByAccount":    []any{},
			"sumByDataSource": []any{},
			"sumByResource":   []any{},
			"topResources":    []any{},
		},
	}, nil
}

// GetRemainingFreeTrialDays returns remaining free trial days.
func (b *InMemoryBackend) GetRemainingFreeTrialDays(detectorID string) (map[string]any, error) {
	b.mu.RLock("GetRemainingFreeTrialDays")
	defer b.mu.RUnlock()

	if !b.detectors.Has(detectorID) {
		return nil, ErrDetectorNotFound
	}

	return map[string]any{
		"accounts": []any{
			map[string]any{
				"accountId":              b.accountID, //nolint:goconst // existing issue.
				"dataSources":            map[string]any{},
				"features":               []any{}, //nolint:goconst // existing issue.
				"freeTrialDaysRemaining": 30,      //nolint:mnd // existing issue.
			},
		},
		"unprocessedAccounts": []any{}, //nolint:goconst // existing issue.
	}, nil
}
