package bedrock

import "fmt"

// ListFoundationModels returns seeded foundation models with optional pagination.
func (b *InMemoryBackend) ListFoundationModels(
	nextToken string,
) ([]*FoundationModelSummary, string) {
	b.mu.RLock("ListFoundationModels")
	defer b.mu.RUnlock()

	list := make([]*FoundationModelSummary, len(b.foundationModels))
	copy(list, b.foundationModels)

	return paginateBedrockSlice(list, nextToken)
}

// GetFoundationModel returns a single foundation model by model ID or full ARN.
func (b *InMemoryBackend) GetFoundationModel(modelID string) (*FoundationModelSummary, error) {
	b.mu.RLock("GetFoundationModel")
	defer b.mu.RUnlock()

	for _, m := range b.foundationModels {
		if m.ModelID == modelID || m.ModelArn == modelID {
			cp := *m

			return &cp, nil
		}
	}

	return nil, fmt.Errorf("%w: foundation model %s not found", ErrNotFound, modelID)
}
