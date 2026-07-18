package opsworks

// DescribeServiceErrors returns service errors (always empty in mock).
func (b *InMemoryBackend) DescribeServiceErrors(
	stackID, _ string,
	_ []string,
) ([]map[string]any, error) {
	b.mu.RLock("DescribeServiceErrors")
	defer b.mu.RUnlock()

	if stackID != "" {
		if !b.stacks.Has(stackID) {
			return nil, ErrStackNotFound
		}
	}

	return []map[string]any{}, nil
}

// DescribeRaidArrays returns RAID arrays (always empty in mock).
func (b *InMemoryBackend) DescribeRaidArrays(
	_, _ string,
	_ []string,
) ([]map[string]any, error) {
	b.mu.RLock("DescribeRaidArrays")
	defer b.mu.RUnlock()

	return []map[string]any{}, nil
}
