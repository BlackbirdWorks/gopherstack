package iotwireless

// GetMetricConfigurationStatus returns the account's summary metric
// aggregation status. Defaults to "Enabled" (AWS's documented default) when
// never explicitly configured.
func (b *InMemoryBackend) GetMetricConfigurationStatus() string {
	b.mu.RLock("GetMetricConfigurationStatus")
	defer b.mu.RUnlock()

	if b.metricConfigStatus == "" {
		return "Enabled"
	}

	return b.metricConfigStatus
}

// UpdateMetricConfigurationStatus sets the account's summary metric
// aggregation status.
func (b *InMemoryBackend) UpdateMetricConfigurationStatus(status string) error {
	b.mu.Lock("UpdateMetricConfigurationStatus")
	defer b.mu.Unlock()

	b.metricConfigStatus = status

	return nil
}
