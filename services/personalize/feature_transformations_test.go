package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersonalize_DescribeFeatureTransformation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		arnOrName  string
		wantName   string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "known_arn_aws_feature_transformation",
			arnOrName:  "arn:aws:personalize:us-east-1:000000000000:feature-transformation/aws-feature-transformation",
			wantStatus: http.StatusOK,
			wantName:   "aws-feature-transformation",
		},
		{
			name: "known_arn_bandits",
			arnOrName: "arn:aws:personalize:us-east-1:000000000000:feature-transformation/" +
				"aws-explicit-contextual-bandits-feature-transformation",
			wantStatus: http.StatusOK,
			wantName:   "aws-explicit-contextual-bandits-feature-transformation",
		},
		{
			name:       "unknown_arn_returns_404",
			arnOrName:  "arn:aws:personalize:us-east-1:000000000000:feature-transformation/not-a-real-one",
			wantStatus: http.StatusBadRequest,
			wantErr:    "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := personalizeHandler(t)
			rec := personalizeDo(t, h, "DescribeFeatureTransformation", map[string]any{
				"featureTransformationArn": tt.arnOrName,
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
			m := personalizeUnmarshal(t, rec)
			if tt.wantErr != "" {
				assert.Equal(t, tt.wantErr, m["__type"])
			} else {
				ft := m["featureTransformation"].(map[string]any)
				assert.Equal(t, tt.wantName, ft["name"])
				assert.Equal(t, "ACTIVE", ft["status"])
				assert.Equal(t, tt.arnOrName, ft["featureTransformationArn"])
			}
		})
	}
}

// --- GetRecommendations / GetPersonalizedRanking ---
