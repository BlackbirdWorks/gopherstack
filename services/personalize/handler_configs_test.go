package personalize_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/personalize"
)

// TestPersonalize_Config_DeepTyping locks that SolutionConfig/CampaignConfig/
// RecommenderConfig are deep-typed against the real SDK's sub-object shapes
// (types.SolutionConfig/CampaignConfig/RecommenderConfig) rather than passed
// through as an opaque map: a real nested sub-object round-trips correctly,
// and a field with no counterpart in the real API is dropped rather than
// echoed back verbatim.
func TestPersonalize_Config_DeepTyping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *personalize.Handler)
		name string
	}{
		{name: "solutionconfig", run: personalizeCheckSolutionConfigDeepTyping},
		{name: "campaignconfig", run: personalizeCheckCampaignConfigDeepTyping},
		{name: "recommenderconfig", run: personalizeCheckRecommenderConfigDeepTyping},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := personalizeHandler(t)
			tt.run(t, h)
		})
	}
}

func personalizeCheckSolutionConfigDeepTyping(t *testing.T, h *personalize.Handler) {
	t.Helper()

	dgArn := personalizeCreateDatasetGroup(t, h, "deep-sol-dg")

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "deep-sol",
		"datasetGroupArn": dgArn,
		"performAutoML":   true,
		"solutionConfig": map[string]any{
			"autoMLConfig": map[string]any{
				"metricName": "coverage",
				"recipeList": []any{"arn:aws:personalize:::recipe/aws-user-personalization"},
			},
			"hpoConfig": map[string]any{
				"hpoResourceConfig": map[string]any{
					"maxParallelTrainingJobs": "5",
				},
			},
			"trainingDataConfig": map[string]any{
				"includedDatasetColumns": map[string]any{
					"Interactions": []any{"USER_ID", "ITEM_ID"},
				},
			},
			"notARealSolutionConfigField": "must not round-trip",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn, _ := personalizeUnmarshal(t, rec)["solutionArn"].(string)
	require.NotEmpty(t, solArn)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sol := personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	cfg, ok := sol["solutionConfig"].(map[string]any)
	require.True(t, ok)

	amc, ok := cfg["autoMLConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "coverage", amc["metricName"])

	hpo, ok := cfg["hpoConfig"].(map[string]any)
	require.True(t, ok)
	hpoResource, ok := hpo["hpoResourceConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "5", hpoResource["maxParallelTrainingJobs"])

	tdc, ok := cfg["trainingDataConfig"].(map[string]any)
	require.True(t, ok)
	included, ok := tdc["includedDatasetColumns"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []any{"USER_ID", "ITEM_ID"}, included["Interactions"])

	assert.NotContains(t, cfg, "notARealSolutionConfigField")
}

func personalizeCheckCampaignConfigDeepTyping(t *testing.T, h *personalize.Handler) {
	t.Helper()

	svArn := personalizeCreateSolutionVersion(t, h, "deep-camp-sol")

	rec := personalizeDo(t, h, "CreateCampaign", map[string]any{
		"name":               "deep-camp",
		"solutionVersionArn": svArn,
		"campaignConfig": map[string]any{
			"enableMetadataWithRecommendations": true,
			"itemExplorationConfig": map[string]any{
				"explorationWeight": "0.3",
			},
			"rankingInfluence": map[string]any{
				"POPULARITY": 0.5,
			},
			"notARealCampaignConfigField": "must not round-trip",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	campArn, _ := personalizeUnmarshal(t, rec)["campaignArn"].(string)
	require.NotEmpty(t, campArn)

	rec = personalizeDo(t, h, "DescribeCampaign", map[string]any{"campaignArn": campArn})
	require.Equal(t, http.StatusOK, rec.Code)
	c := personalizeUnmarshal(t, rec)["campaign"].(map[string]any)
	cfg, ok := c["campaignConfig"].(map[string]any)
	require.True(t, ok)

	assert.Equal(t, true, cfg["enableMetadataWithRecommendations"])
	iec, ok := cfg["itemExplorationConfig"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "0.3", iec["explorationWeight"])
	ri, ok := cfg["rankingInfluence"].(map[string]any)
	require.True(t, ok)
	assert.InDelta(t, 0.5, ri["POPULARITY"], 0)

	assert.NotContains(t, cfg, "notARealCampaignConfigField")
}

func personalizeCheckRecommenderConfigDeepTyping(t *testing.T, h *personalize.Handler) {
	t.Helper()

	dgArn := personalizeCreateDatasetGroup(t, h, "deep-rec-dg")

	rec := personalizeDo(t, h, "CreateRecommender", map[string]any{
		"name":            "deep-rec",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-similar-items",
		"recommenderConfig": map[string]any{
			"minRecommendationRequestsPerSecond": float64(7),
			"trainingDataConfig": map[string]any{
				"excludedDatasetColumns": map[string]any{
					"Items": []any{"CREATION_TIMESTAMP"},
				},
			},
			"notARealRecommenderConfigField": "must not round-trip",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	recArn, _ := personalizeUnmarshal(t, rec)["recommenderArn"].(string)
	require.NotEmpty(t, recArn)

	rec = personalizeDo(t, h, "DescribeRecommender", map[string]any{"recommenderArn": recArn})
	require.Equal(t, http.StatusOK, rec.Code)
	r := personalizeUnmarshal(t, rec)["recommender"].(map[string]any)
	cfg, ok := r["recommenderConfig"].(map[string]any)
	require.True(t, ok)

	assert.InDelta(t, float64(7), cfg["minRecommendationRequestsPerSecond"], 0)
	tdc, ok := cfg["trainingDataConfig"].(map[string]any)
	require.True(t, ok)
	excluded, ok := tdc["excludedDatasetColumns"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, []any{"CREATION_TIMESTAMP"}, excluded["Items"])

	assert.NotContains(t, cfg, "notARealRecommenderConfigField")
}
