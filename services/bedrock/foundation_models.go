package bedrock

import (
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
)

// foundationModelARN builds a foundation model ARN. Foundation model ARNs
// carry a region but never an account ID (bedrock@v1.66.4 ModelArn shape
// regex: "arn:aws...:bedrock:{region}::foundation-model/{id}"; matches
// seedFoundationModels' own seeded ARNs), unlike arn.Build's account-scoped
// resources such as custom-model/ or model-customization-job/.
func foundationModelARN(region, modelID string) string {
	return fmt.Sprintf("arn:%s:bedrock:%s::foundation-model/%s", arn.PartitionForRegion(region), region, modelID)
}

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

	m := b.findFoundationModelByID(modelID)
	if m == nil {
		return nil, fmt.Errorf("%w: foundation model %s not found", ErrNotFound, modelID)
	}

	cp := *m

	return &cp, nil
}

// findFoundationModelByID looks up a seeded foundation model by ID or ARN.
// Caller must hold at least a read lock.
func (b *InMemoryBackend) findFoundationModelByID(modelID string) *FoundationModelSummary {
	for _, m := range b.foundationModels {
		if m.ModelID == modelID || m.ModelArn == modelID {
			return m
		}
	}

	return nil
}
