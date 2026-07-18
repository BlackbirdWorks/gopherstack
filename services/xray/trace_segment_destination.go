package xray

// GetTraceSegmentDestination returns the current trace segment destination.
func (b *InMemoryBackend) GetTraceSegmentDestination() string {
	b.mu.RLock("GetTraceSegmentDestination")
	defer b.mu.RUnlock()

	if b.traceSegmentDest == "" {
		return "XRay"
	}

	return b.traceSegmentDest
}

// UpdateTraceSegmentDestination sets the trace segment destination and returns it.
func (b *InMemoryBackend) UpdateTraceSegmentDestination(destination string) string {
	b.mu.Lock("UpdateTraceSegmentDestination")
	defer b.mu.Unlock()

	b.traceSegmentDest = destination

	return destination
}
