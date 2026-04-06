package codepipeline

// PipelineCount returns the number of pipelines stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) PipelineCount() int {
	b.mu.RLock("PipelineCount")
	defer b.mu.RUnlock()

	return len(b.pipelines)
}

// CustomActionTypeCount returns the number of custom action types stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) CustomActionTypeCount() int {
	b.mu.RLock("CustomActionTypeCount")
	defer b.mu.RUnlock()

	return len(b.customActionTypes)
}

// JobCount returns the number of jobs stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) JobCount() int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return len(b.jobs)
}

// WebhookCount returns the number of webhooks stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) WebhookCount() int {
	b.mu.RLock("WebhookCount")
	defer b.mu.RUnlock()

	return len(b.webhooks)
}

// StageTransitionCount returns the number of disabled stage transitions stored in the backend.
// Used only in tests.
func (b *InMemoryBackend) StageTransitionCount() int {
	b.mu.RLock("StageTransitionCount")
	defer b.mu.RUnlock()

	return len(b.stageTransitions)
}
