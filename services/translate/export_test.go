package translate

// TerminologyCount returns the number of stored terminologies.
func TerminologyCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.terminologies)
}

// ParallelDataCount returns the number of stored parallel data resources.
func ParallelDataCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.parallelData)
}

// JobCount returns the number of stored translation jobs.
func JobCount(b *InMemoryBackend) int {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return len(b.jobs)
}
