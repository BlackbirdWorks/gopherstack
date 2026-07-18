package personalize

import (
	"fmt"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/awstime"
)

// storedFeatureTransformation represents a built-in feature transformation.
type storedFeatureTransformation struct {
	CreationTime  time.Time
	LastUpdated   time.Time
	DefaultValues map[string]string
	ARN           string
	Name          string
	Status        string
}

// seedBuiltinFeatureTransformations (re-)populates featureTransformations
// with the read-only builtins. Called from NewInMemoryBackend and from Reset
// (since registry.ResetAll() clears the table along with every other one).
func (b *InMemoryBackend) seedBuiltinFeatureTransformations() {
	epoch := time.Date(2017, 1, 1, 0, 0, 0, 0, time.UTC)
	for _, name := range []string{
		"aws-feature-transformation",
		"aws-explicit-contextual-bandits-feature-transformation",
	} {
		ftArn := arn.Build("personalize", b.region, b.accountID, "feature-transformation/"+name)
		b.featureTransformations.Put(&storedFeatureTransformation{
			ARN:          ftArn,
			Name:         name,
			Status:       statusActive,
			CreationTime: epoch,
			LastUpdated:  epoch,
		})
	}
}

// GetFeatureTransformation looks up a feature transformation by ARN or name.
func (b *InMemoryBackend) GetFeatureTransformation(arnOrName string) (*storedFeatureTransformation, error) {
	b.mu.RLock("GetFeatureTransformation")
	defer b.mu.RUnlock()

	for _, ft := range b.featureTransformations.All() {
		if ft.ARN == arnOrName || ft.Name == arnOrName {
			return ft, nil
		}
	}

	return nil, fmt.Errorf("%w: feature transformation %q not found", ErrNotFound, arnOrName)
}

// --- FeatureTransformation (read-only) ---

func (h *Handler) describeFeatureTransformation(input map[string]any) (map[string]any, error) {
	ftArn, _ := input["featureTransformationArn"].(string)

	ft, err := h.Backend.GetFeatureTransformation(ftArn)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"featureTransformation": map[string]any{
			"featureTransformationArn": ft.ARN,
			keyName:                    ft.Name,
			keyStatus:                  ft.Status,
			keyCreationDateTime:        awstime.Epoch(ft.CreationTime),
			keyLastUpdatedDateTime:     awstime.Epoch(ft.LastUpdated),
		},
	}, nil
}
