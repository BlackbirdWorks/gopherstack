package stepfunctions

import "slices"

// RegionContextKeyForTest exposes regionContextKey for use in external test packages.
type RegionContextKeyForTest = regionContextKey

// MaxHistoryEventsForTest exposes the maxHistoryEvents cap for use in external test packages.
const MaxHistoryEventsForTest = maxHistoryEvents

// StateMachineCount returns the number of state machines stored in the backend.
func (b *InMemoryBackend) StateMachineCount() int {
	b.mu.RLock("StateMachineCount")
	defer b.mu.RUnlock()

	return len(b.stateMachines)
}

// ExecutionCount returns the number of executions stored in the backend.
func (b *InMemoryBackend) ExecutionCount() int {
	b.mu.RLock("ExecutionCount")
	defer b.mu.RUnlock()

	return len(b.executions)
}

// ActivityCount returns the number of activities stored in the backend.
func (b *InMemoryBackend) ActivityCount() int {
	b.mu.RLock("ActivityCount")
	defer b.mu.RUnlock()

	return len(b.activities)
}

// HandlerOpsLen returns the number of operations returned by GetSupportedOperations.
func (h *Handler) HandlerOpsLen() int {
	return len(h.GetSupportedOperations())
}

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

// TaskTokensForTest returns sorted currently registered task tokens.
func (b *InMemoryBackend) TaskTokensForTest() []string {
	b.mu.RLock("TaskTokensForTest")
	defer b.mu.RUnlock()

	tokens := make([]string, 0, len(b.tasksByToken))
	for token := range b.tasksByToken {
		tokens = append(tokens, token)
	}
	slices.Sort(tokens)

	return tokens
}

// ExecutionPruneSweepThresholdForTest exposes the inline-sweep threshold.
const ExecutionPruneSweepThresholdForTest = executionPruneSweepThreshold

// SetExecutionStopDateForTest directly sets the StopDate of an execution to
// force it past the retention cutoff without sleeping.
func (b *InMemoryBackend) SetExecutionStopDateForTest(execARN string, stopDate float64) {
	b.mu.Lock("SetExecutionStopDateForTest")
	defer b.mu.Unlock()

	if exec, ok := b.executions[execARN]; ok {
		exec.StopDate = &stopDate
		exec.Status = statusSucceeded
	}
}

// PendingTaskQueueLenForTest returns the number of entries in the pending task
// queue for the given activity ARN, or -1 if the queue does not exist.
func (b *InMemoryBackend) PendingTaskQueueLenForTest(activityARN string) int {
	b.mu.RLock("PendingTaskQueueLenForTest")
	defer b.mu.RUnlock()

	q, ok := b.pendingTaskQueues[activityARN]
	if !ok {
		return -1
	}

	return len(q)
}
