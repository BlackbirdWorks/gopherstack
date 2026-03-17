package stepfunctions

// MaxHistoryEventsForTest exposes the maxHistoryEvents cap for use in external test packages.
const MaxHistoryEventsForTest = maxHistoryEvents

// FillHistoryForTest pre-fills the history for an execution to a specific count.
// This is used in tests to set up conditions near the cap.
func (b *InMemoryBackend) FillHistoryForTest(execARN string, count int) {
	b.mu.Lock("FillHistoryForTest")
	defer b.mu.Unlock()

	for len(b.history[execARN]) < count {
		id := int64(len(b.history[execARN]) + 1)
		b.history[execARN] = append(b.history[execARN], &HistoryEvent{
			Timestamp: 0,
			Type:      "PassStateEntered",
			ID:        id,
		})
	}
}

// HistoryLenForTest returns the current length of the history slice for an execution.
func (b *InMemoryBackend) HistoryLenForTest(execARN string) int {
	b.mu.RLock("HistoryLenForTest")
	defer b.mu.RUnlock()

	return len(b.history[execARN])
}

// HasTombstoneForTest reports whether an execution has been tombstoned.
func (b *InMemoryBackend) HasTombstoneForTest(execARN string) bool {
	b.mu.RLock("HasTombstoneForTest")
	defer b.mu.RUnlock()

	return b.deletedExecs[execARN]
}

// RecordStateEnteredForTest calls RecordStateEntered via the historyRecorder.
func (b *InMemoryBackend) RecordStateEnteredForTest(execARN, stateName, stateType string) {
	rec := &historyRecorder{backend: b}
	rec.RecordStateEntered(execARN, stateName, stateType, nil)
}

// SetTagsForTest exposes setTags for external test packages.
func (h *Handler) SetTagsForTest(resourceID string, kv map[string]string) {
	h.setTags(resourceID, kv)
}

// GetTagsForTest exposes getTags for external test packages.
func (h *Handler) GetTagsForTest(resourceID string) map[string]string {
	return h.getTags(resourceID)
}
