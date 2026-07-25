package xray

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultXRayJanitorInterval

// DefaultTraceTTL exposes the package default trace TTL for testing.
const DefaultTraceTTL = defaultXRayTraceTTL

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
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

	b.traces.Put(&Trace{
		TraceID:   traceID,
		StartTime: startTime,
	})

	return traceID
}

// TraceExistsForTest returns true if a trace with the given ID exists in the backend.
func (b *InMemoryBackend) TraceExistsForTest(traceID string) bool {
	b.mu.RLock("TraceExistsForTest")
	defer b.mu.RUnlock()

	return b.traces.Has(traceID)
}

// GroupCount returns the number of groups stored in the backend.
func (b *InMemoryBackend) GroupCount() int {
	b.mu.RLock("GroupCount")
	defer b.mu.RUnlock()

	return b.groups.Len()
}

// SamplingRuleCount returns the number of sampling rules stored in the backend.
func (b *InMemoryBackend) SamplingRuleCount() int {
	b.mu.RLock("SamplingRuleCount")
	defer b.mu.RUnlock()

	return b.samplingRules.Len()
}

// TraceCount returns the number of traces stored in the backend.
func (b *InMemoryBackend) TraceCount() int {
	b.mu.RLock("TraceCount")
	defer b.mu.RUnlock()

	return b.traces.Len()
}

// InsightCount returns the number of insights stored in the backend.
func (b *InMemoryBackend) InsightCount() int {
	b.mu.RLock("InsightCount")
	defer b.mu.RUnlock()

	return b.insights.Len()
}

// ResourcePolicyCount returns the number of resource policies stored in the backend.
func (b *InMemoryBackend) ResourcePolicyCount() int {
	b.mu.RLock("ResourcePolicyCount")
	defer b.mu.RUnlock()

	return b.resourcePolicies.Len()
}

// HandlerOpsLen returns the number of operations in GetSupportedOperations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.GetSupportedOperations())
}

// AddSamplingRuleInternal seeds a sampling rule directly for testing without going through validation.
func (b *InMemoryBackend) AddSamplingRuleInternal(rule SamplingRule) {
	b.mu.Lock("AddSamplingRuleInternal")
	defer b.mu.Unlock()

	now := time.Now()
	rule.RuleARN = b.samplingRuleARN(rule.RuleName)

	if rule.CreatedAt.IsZero() {
		rule.CreatedAt = now
	}

	if rule.ModifiedAt.IsZero() {
		rule.ModifiedAt = now
	}

	b.samplingRules.Put(&rule)
}

// MaxSegmentsPerTrace exposes the per-trace segment cap for tests.
const MaxSegmentsPerTrace = maxSegmentsPerTrace

// SamplingRuleLimitForTest exposes the per-account sampling rule cap for tests.
func SamplingRuleLimitForTest() int { return maxSamplingRules }

// SegmentCompactionHighWater exposes the compaction trigger for tests.
const SegmentCompactionHighWater = segmentCompactionHighWater

// SetRetrievalTimeForTest overrides the recorded creation time for a retrieval token.
// Used in tests to simulate aged retrieval tokens for janitor sweep testing.
func (b *InMemoryBackend) SetRetrievalTimeForTest(token string, t time.Time) {
	b.mu.Lock("SetRetrievalTimeForTest")
	defer b.mu.Unlock()

	b.retrievalTimes[token] = t
}
