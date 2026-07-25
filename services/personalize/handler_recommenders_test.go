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
	dgArn := personalizeCreateDatasetGroup(t, h, "my-rec-dg")

	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "my-rec",
		"datasetGroupArn": dgArn,
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
	dgArn := personalizeCreateDatasetGroup(t, h, "config-rec-dg")

	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "config-rec",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"recommenderConfig": map[string]any{
			"minRecommendationRequestsPerSecond": float64(10),
			"enableMetadataWithRecommendations":  true,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn := personalizeUnmarshal(t, rec)["recommenderArn"].(string)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r := personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	cfg := r["recommenderConfig"].(map[string]any)
	assert.InDelta(t, float64(10), cfg["minRecommendationRequestsPerSecond"], 0)
	// enableMetadataWithRecommendations previously got silently dropped --
	// only minRecommendationRequestsPerSecond was ever extracted from the
	// sub-object. It must now round-trip along with every other sub-field.
	assert.Equal(t, true, cfg["enableMetadataWithRecommendations"])
}

// TestPersonalize_Recommender_Update locks two fixes: UpdateRecommender
// requires recommenderConfig (a required member on the real
// UpdateRecommenderInput, unlike the optional field on Create), and a
// successful call populates latestRecommenderUpdate on the next Describe --
// which must be absent before any update, matching the real API's doc
// comment on Recommender.LatestRecommenderUpdate.
func TestPersonalize_Recommender_Update(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "update-rec-dg")

	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "update-rec",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn := personalizeUnmarshal(t, rec)["recommenderArn"].(string)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r := personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	assert.NotContains(t, r, "latestRecommenderUpdate")

	// Missing recommenderConfig is rejected -- it is required on the real API.
	rec = personalizeDo(t, h, "UpdateRecommender", map[string]any{"recommenderArn": recArn})
	require.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Equal(t, "InvalidInputException", personalizeUnmarshal(t, rec)["__type"])

	rec = personalizeDo(t, h, "UpdateRecommender", map[string]any{
		"recommenderArn": recArn,
		"recommenderConfig": map[string]any{
			"minRecommendationRequestsPerSecond": float64(20),
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	r = personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	cfg := r["recommenderConfig"].(map[string]any)
	assert.InDelta(t, float64(20), cfg["minRecommendationRequestsPerSecond"], 0)
	update, ok := r["latestRecommenderUpdate"].(map[string]any)
	require.True(t, ok, "latestRecommenderUpdate must appear after an UpdateRecommender call")
	assert.Equal(t, "ACTIVE", update["status"])
	assert.NotEmpty(t, update["creationDateTime"])
}

// --- MetricAttribution ---
