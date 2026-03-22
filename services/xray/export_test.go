package xray

import "time"

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetJanitorTraceTTL returns the TraceTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorTraceTTL() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TraceTTL
}

// PutTraceForTest inserts a trace with the given start time and returns its trace ID.
// Used to set up test state without going through the PutTraceSegments API.
func (b *InMemoryBackend) PutTraceForTest(startTime time.Time) string {
	traceID := "1-test-" + startTime.Format("20060102150405")

	b.mu.Lock("PutTraceForTest")
	defer b.mu.Unlock()

	b.traces[traceID] = &Trace{
		TraceID:   traceID,
		StartTime: startTime,
	}

	return traceID
}

// TraceExistsForTest returns true if a trace with the given ID exists in the backend.
func (b *InMemoryBackend) TraceExistsForTest(traceID string) bool {
	b.mu.RLock("TraceExistsForTest")
	defer b.mu.RUnlock()

	_, ok := b.traces[traceID]

	return ok
}
