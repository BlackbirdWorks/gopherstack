package backup

import "time"

// JobCount returns the number of backup jobs stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) JobCount() int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return len(b.jobs)
}

// SetJobState overrides the state and completion time of a backup job.
// Used only in tests to simulate a completed or failed job.
func (b *InMemoryBackend) SetJobState(jobID, state string, completionTime *time.Time) {
	b.mu.Lock("SetJobState")
	defer b.mu.Unlock()

	if j, ok := b.jobs[jobID]; ok {
		j.State = state
		j.CompletionTime = completionTime
	}
}

// GetJanitorTaskTimeout returns the TaskTimeout configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the timeout.
func (h *Handler) GetJanitorTaskTimeout() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.TaskTimeout
}
