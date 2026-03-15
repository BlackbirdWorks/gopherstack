package bedrock

// AppendFoundationModelsForTest appends additional foundation models to the backend.
// This is only used in tests to populate beyond the default seeded models.
func (b *InMemoryBackend) AppendFoundationModelsForTest(models []*FoundationModelSummary) {
	b.mu.Lock("AppendFoundationModelsForTest")
	defer b.mu.Unlock()
	b.foundationModels = append(b.foundationModels, models...)
}
