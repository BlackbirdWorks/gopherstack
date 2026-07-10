package codepipeline

// PipelineCount returns the total number of pipelines stored across all regions.
// Used only in tests.
func (b *InMemoryBackend) PipelineCount() int {
	b.mu.RLock("PipelineCount")
	defer b.mu.RUnlock()

	return b.pipelines.Len()
}

// CustomActionTypeCount returns the total number of custom action types stored across all regions.
// Used only in tests.
func (b *InMemoryBackend) CustomActionTypeCount() int {
	b.mu.RLock("CustomActionTypeCount")
	defer b.mu.RUnlock()

	return b.customActionTypes.Len()
}

// JobCount returns the total number of jobs stored across all regions.
// Used only in tests.
func (b *InMemoryBackend) JobCount() int {
	b.mu.RLock("JobCount")
	defer b.mu.RUnlock()

	return b.jobs.Len()
}

// WebhookCount returns the total number of webhooks stored across all regions.
// Used only in tests.
func (b *InMemoryBackend) WebhookCount() int {
	b.mu.RLock("WebhookCount")
	defer b.mu.RUnlock()

	return b.webhooks.Len()
}

// StageTransitionCount returns the total number of disabled stage transitions stored across all regions.
// Used only in tests.
func (b *InMemoryBackend) StageTransitionCount() int {
	b.mu.RLock("StageTransitionCount")
	defer b.mu.RUnlock()

	return b.stageTransitions.Len()
}
