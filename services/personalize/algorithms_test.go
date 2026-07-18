package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPersonalize_ReadOnlyResources(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	ftArn := "arn:aws:personalize:us-east-1:000000000000:" +
		"feature-transformation/aws-feature-transformation"
	rec := personalizeDo(t, h, "DescribeFeatureTransformation", map[string]any{
		"featureTransformationArn": ftArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	ft := personalizeUnmarshal(t, rec)["featureTransformation"].(map[string]any)
	assert.Equal(t, "ACTIVE", ft["status"])

	rec = personalizeDo(t, h, "DescribeAlgorithm", map[string]any{
		"algorithmArn": "arn:aws:personalize:::algorithm/user-personalization",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	algo := personalizeUnmarshal(t, rec)["algorithm"].(map[string]any)
	assert.Equal(t, "ACTIVE", algo["status"])
}

// --- Tag round-trip ---
