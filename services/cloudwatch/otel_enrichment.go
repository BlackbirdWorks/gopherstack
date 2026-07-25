package cloudwatch

// otelEnrichmentStatusLocked returns the current OTel enrichment status,
// defaulting to "Stopped" (real CloudWatch's state before StartOTelEnrichment
// has ever been called). Caller must hold b.mu (at least read lock).
func (b *InMemoryBackend) otelEnrichmentStatusLocked() string {
	if s, ok := b.otelEnrichment.Get(otelEnrichmentSingletonKey); ok && s.Status != "" {
		return s.Status
	}

	return otelEnrichmentStatusStopped
}

// GetOTelEnrichment returns the current account-level status of vended
// metric OTel/PromQL enrichment.
func (b *InMemoryBackend) GetOTelEnrichment() (string, error) {
	b.mu.RLock("GetOTelEnrichment")
	defer b.mu.RUnlock()

	return b.otelEnrichmentStatusLocked(), nil
}

// StartOTelEnrichment enables vended metric OTel/PromQL enrichment for the account.
func (b *InMemoryBackend) StartOTelEnrichment() error {
	b.mu.Lock("StartOTelEnrichment")
	defer b.mu.Unlock()

	b.otelEnrichment.Put(&OTelEnrichmentState{
		Key:    otelEnrichmentSingletonKey,
		Status: otelEnrichmentStatusRunning,
	})

	return nil
}

// StopOTelEnrichment disables vended metric OTel/PromQL enrichment for the account.
func (b *InMemoryBackend) StopOTelEnrichment() error {
	b.mu.Lock("StopOTelEnrichment")
	defer b.mu.Unlock()

	b.otelEnrichment.Put(&OTelEnrichmentState{
		Key:    otelEnrichmentSingletonKey,
		Status: otelEnrichmentStatusStopped,
	})

	return nil
}
