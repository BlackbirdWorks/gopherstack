package bedrock

import (
	"fmt"
	"slices"
	"strings"

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

// ListFoundationModelsFilter holds the ListFoundationModels query filters
// (api_op_ListFoundationModels.go:32-55); empty fields match everything.
type ListFoundationModelsFilter struct {
	ByCustomizationType string
	ByInferenceType     string
	ByOutputModality    string
	ByProvider          string
	NextToken           string
}

// ListFoundationModels returns the seeded catalog narrowed by f, paginated.
func (b *InMemoryBackend) ListFoundationModels(
	f ListFoundationModelsFilter,
) ([]*FoundationModelSummary, string) {
	b.mu.RLock("ListFoundationModels")
	defer b.mu.RUnlock()

	list := make([]*FoundationModelSummary, 0, len(b.foundationModels))

	for _, m := range b.foundationModels {
		if f.ByProvider != "" && !strings.EqualFold(m.ProviderName, f.ByProvider) {
			continue
		}

		if f.ByCustomizationType != "" && !slices.Contains(m.CustomizationsSupported, f.ByCustomizationType) {
			continue
		}

		if f.ByInferenceType != "" && !slices.Contains(m.InferenceTypesSupported, f.ByInferenceType) {
			continue
		}

		if f.ByOutputModality != "" && !slices.Contains(m.OutputModalities, f.ByOutputModality) {
			continue
		}

		cp := *m
		cp.InputModalities = slices.Clone(m.InputModalities)
		cp.OutputModalities = slices.Clone(m.OutputModalities)
		cp.InferenceTypesSupported = slices.Clone(m.InferenceTypesSupported)
		cp.CustomizationsSupported = slices.Clone(m.CustomizationsSupported)

		if m.ModelLifecycle != nil {
			lc := *m.ModelLifecycle
			cp.ModelLifecycle = &lc
		}

		list = append(list, &cp)
	}

	return paginate(list, 0, f.NextToken)
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
