package batch

// JobDefinitionCount returns the number of job definitions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) JobDefinitionCount() int {
	b.mu.RLock("JobDefinitionCount")
	defer b.mu.RUnlock()

	return len(b.jobDefinitions)
}

// RevisionFor returns the current revision counter for the given job definition name.
// Used only in tests.
func (b *InMemoryBackend) RevisionFor(name string) int32 {
	b.mu.RLock("RevisionFor")
	defer b.mu.RUnlock()

	return b.jobDefRevisions[name]
}
