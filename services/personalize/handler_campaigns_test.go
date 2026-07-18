package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Campaign_CRUD(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	svArn := "arn:aws:personalize:us-east-1:000000000000:solution/sol/v1"

	rec := personalizeDo(t, h, "CreateCampaign", map[string]any{
		"name":               "my-campaign",
		"solutionVersionArn": svArn,
		"minProvisionedTPS":  float64(5),
	})
	require.Equal(t, http.StatusOK, rec.Code)
	campArn := personalizeUnmarshal(t, rec)["campaignArn"].(string)
	assert.NotEmpty(t, campArn)

	rec = personalizeDo(t, h, "DescribeCampaign", map[string]any{"campaignArn": campArn})
	require.Equal(t, http.StatusOK, rec.Code)
	c := personalizeUnmarshal(t, rec)["campaign"].(map[string]any)
	assert.Equal(t, "my-campaign", c["name"])
	assert.Equal(t, svArn, c["solutionVersionArn"])
	assert.InDelta(t, float64(5), c["minProvisionedTPS"], 0)
	assert.Equal(t, "ACTIVE", c["status"])

	// Update
	newSvArn := "arn:aws:personalize:us-east-1:000000000000:solution/sol/v2"
	rec = personalizeDo(t, h, "UpdateCampaign", map[string]any{
		"campaignArn":        campArn,
		"solutionVersionArn": newSvArn,
		"minProvisionedTPS":  float64(10),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeCampaign", map[string]any{"campaignArn": campArn})
	c = personalizeUnmarshal(t, rec)["campaign"].(map[string]any)
	assert.Equal(t, newSvArn, c["solutionVersionArn"])
	assert.InDelta(t, float64(10), c["minProvisionedTPS"], 0)

	// List
	rec = personalizeDo(t, h, "ListCampaigns", map[string]any{})
	listed := personalizeUnmarshal(t, rec)
	assert.Len(t, listed["campaigns"].([]any), 1)

	// Delete
	rec = personalizeDo(t, h, "DeleteCampaign", map[string]any{"campaignArn": campArn})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- EventTracker ---
