package cloudwatchlogs

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultJanitorInterval

// MaxEventsPerStream exposes the per-stream event cap for use in tests.
const MaxEventsPerStream = maxEventsPerStream

// FilterPatternMatches exposes the filter pattern matching function for use in tests.
func FilterPatternMatches(pattern, message string) bool {
	return filterPatternMatches(pattern, message)
}

// SetTagsForTest sets tags for a resource ID directly, bypassing JSON round-trip.
// Used in persistence tests to populate tags before taking a snapshot.
func (h *Handler) SetTagsForTest(resourceID string, kv map[string]string) {
	h.setTags(resourceID, kv)
}

// GetTagsForTest returns a copy of the tags for a resource ID.
// Used in persistence tests to verify tags after restore.
func (h *Handler) GetTagsForTest(resourceID string) map[string]string {
	return h.getTags(resourceID)
}

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

// AddExportTaskInternal exposes the backend seeding helper for testing.
func AddExportTaskInternal(b *InMemoryBackend, task ExportTask) {
	b.AddExportTaskInternal(task)
}

// AddImportTaskInternal exposes the backend seeding helper for testing.
func AddImportTaskInternal(b *InMemoryBackend, task ImportTask) {
	b.AddImportTaskInternal(task)
}

// AddDeliveryInternal exposes the backend seeding helper for testing.
func AddDeliveryInternal(b *InMemoryBackend, delivery Delivery) {
	b.AddDeliveryInternal(delivery)
}

// AddLogAnomalyDetectorInternal exposes the backend seeding helper for testing.
func AddLogAnomalyDetectorInternal(b *InMemoryBackend, detector LogAnomalyDetector) {
	b.AddLogAnomalyDetectorInternal(detector)
}

// GetParsedInsightsQueryCacheSize returns the parsed Insights query cache size.
func (b *InMemoryBackend) GetParsedInsightsQueryCacheSize() int {
	b.mu.RLock("GetParsedInsightsQueryCacheSize")
	defer b.mu.RUnlock()

	return len(b.parsedQueries)
}

// DefaultParsedQueryCacheSizeForTest exposes the default parsed-query cache cap.
const DefaultParsedQueryCacheSizeForTest = defaultParsedQueryCacheSize
