package emrserverless

// ApplicationCount returns the number of applications stored in the backend. Used only in tests.
func ApplicationCount(b *InMemoryBackend) int {
	b.mu.RLock("ApplicationCount")
	defer b.mu.RUnlock()

	return len(b.applications)
}

// JobRunCount returns the total number of job runs across all applications. Used only in tests.
func JobRunCount(b *InMemoryBackend) int {
	b.mu.RLock("JobRunCount")
	defer b.mu.RUnlock()

	count := 0

	for _, runs := range b.jobRuns {
		count += len(runs)
	}

	return count
}

// ARNIndexSizes returns the sizes of the applicationARNs and jobRunARNs indexes. Used only in tests.
func ARNIndexSizes(b *InMemoryBackend) (int, int) {
	b.mu.RLock("ARNIndexSizes")
	defer b.mu.RUnlock()

	return len(b.applicationARNs), len(b.jobRunARNs)
}

// SetApplicationState forcibly sets an application's state. Used only in tests.
func SetApplicationState(b *InMemoryBackend, id, state string) {
	b.mu.Lock("SetApplicationState")
	defer b.mu.Unlock()

	if app, ok := b.applications[id]; ok {
		app.State = state
	}
}
