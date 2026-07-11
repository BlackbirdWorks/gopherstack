package backup

import "time"

// DefaultJanitorInterval exposes the package default janitor interval for testing.
const DefaultJanitorInterval = defaultBackupJanitorInterval

// DefaultJobTTL exposes the package default job TTL for testing.
const DefaultJobTTL = defaultBackupJobTTL

// JobCount returns the number of backup jobs stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) JobCount() int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return b.jobs.Len()
}

// SetJobState overrides the state and completion time of a backup job.
// Used only in tests to simulate a completed or failed job.
func (b *InMemoryBackend) SetJobState(jobID, state string, completionTime *time.Time) {
	b.mu.Lock("SetJobState")
	defer b.mu.Unlock()

	if j, ok := b.jobs.Get(jobID); ok {
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

// GetJanitorInterval returns the Interval configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the interval.
func (h *Handler) GetJanitorInterval() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.Interval
}

// GetJanitorJobTTL returns the JobTTL configured on the handler's janitor.
// Used in tests to verify WithJanitor correctly propagates the TTL.
func (h *Handler) GetJanitorJobTTL() time.Duration {
	if h.janitor == nil {
		return 0
	}

	return h.janitor.JobTTL
}
