package translate

import "time"

// TerminologyCount returns the number of stored terminologies.
func TerminologyCount(b *InMemoryBackend) int {
	b.mu.RLock("TerminologyCount")
	defer b.mu.RUnlock()

	return b.terminologies.Len()
}

// ParallelDataCount returns the number of stored parallel data resources.
func ParallelDataCount(b *InMemoryBackend) int {
	b.mu.RLock("ParallelDataCount")
	defer b.mu.RUnlock()

	return b.parallelData.Len()
}

// JobCount returns the number of stored translation jobs.
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return b.jobs.Len()
}

// SetJobSubmittedAtForTest backdates a job's SubmittedAt so
// ListTextTranslationJobs' SubmittedAfterTime/SubmittedBeforeTime filter and
// sort-order tests can use deterministic timestamps instead of real
// wall-clock sleeps between StartTextTranslationJob calls.
func SetJobSubmittedAtForTest(b *InMemoryBackend, jobID string, submittedAt time.Time) bool {
	b.mu.Lock("SetJobSubmittedAtForTest")
	defer b.mu.Unlock()

	job, ok := b.jobs.Get(jobID)
	if !ok {
		return false
	}

	job.SubmittedAt = submittedAt

	return true
}
