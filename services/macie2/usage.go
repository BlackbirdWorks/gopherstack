package macie2

// GetUsageStatistics returns usage statistics (empty — no billing data in emulator).
func (b *InMemoryBackend) GetUsageStatistics(
	_ []map[string]any, _ int, _ string, _ string,
) ([]UsageRecord, string, error) {
	return []UsageRecord{}, "", nil
}

// GetUsageTotals returns aggregated usage totals (all zero).
func (b *InMemoryBackend) GetUsageTotals(_ string) ([]UsageTotal, error) {
	return []UsageTotal{
		{Currency: "USD", EstimatedCost: "0", Type: "DATA_INVENTORY_EVALUATION"}, //nolint:goconst // existing issue.
		{Currency: "USD", EstimatedCost: "0", Type: "SENSITIVE_DATA_DISCOVERY"},
		{Currency: "USD", EstimatedCost: "0", Type: "AUTOMATED_SENSITIVE_DATA_DISCOVERY"},
	}, nil
}
