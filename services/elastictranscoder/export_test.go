package elastictranscoder

// PipelineCount returns the number of pipelines stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) PipelineCount() int {
	b.mu.RLock("PipelineCount")
	defer b.mu.RUnlock()

	return len(b.pipelines)
}

// PresetCount returns the number of presets stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) PresetCount() int {
	b.mu.RLock("PresetCount")
	defer b.mu.RUnlock()

	return len(b.presets)
}

// JobCount returns the number of jobs stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) JobCount() int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return len(b.jobs)
}
