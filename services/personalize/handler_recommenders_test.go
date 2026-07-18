package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Recommender_StartStop(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "my-rec",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn := personalizeUnmarshal(t, rec)["recommenderArn"].(string)

	// Stop
	rec = personalizeDo(t, h, "StopRecommender", map[string]any{"recommenderArn": recArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r := personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	assert.Equal(t, "INACTIVE", r["status"])

	// Start
	rec = personalizeDo(t, h, "StartRecommender", map[string]any{"recommenderArn": recArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r = personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	assert.Equal(t, "ACTIVE", r["status"])
}

func TestPersonalize_Recommender_Config(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "config-rec",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"recommenderConfig": map[string]any{
			"minRecommendationRequestsPerSecond": float64(10),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn := personalizeUnmarshal(t, rec)["recommenderArn"].(string)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r := personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	cfg := r["recommenderConfig"].(map[string]any)
	assert.InDelta(t, float64(10), cfg["minRecommendationRequestsPerSecond"], 0)
}

// --- MetricAttribution ---
