package translate

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
