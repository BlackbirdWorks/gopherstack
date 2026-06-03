package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Blueprint ---

func TestBatch2_Blueprint_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": "my-bp"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetBlueprint returns it
	rec = doGlueRequest(t, h, "GetBlueprint", map[string]any{"Name": "my-bp"})
	require.Equal(t, http.StatusOK, rec.Code)

	// ListBlueprints
	rec = doGlueRequest(t, h, "ListBlueprints", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-bp")

	// UpdateBlueprint
	rec = doGlueRequest(t, h, "UpdateBlueprint", map[string]any{"Name": "my-bp"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteBlueprint
	rec = doGlueRequest(t, h, "DeleteBlueprint", map[string]any{"Name": "my-bp"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestBatch2_BlueprintRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a blueprint first
	doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": "run-bp"})

	// Start a run
	rec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{"BlueprintName": "run-bp"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RunId")

	// GetBlueprintRuns
	rec = doGlueRequest(t, h, "GetBlueprintRuns", map[string]any{"BlueprintName": "run-bp"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Runs")
}

// --- UsageProfile ---

func TestBatch2_UsageProfile_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// List empty
	rec := doGlueRequest(t, h, "ListUsageProfiles", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Create
	rec = doGlueRequest(t, h, "CreateUsageProfile", map[string]any{
		"Name":        "my-profile",
		"Description": "test profile",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get
	rec = doGlueRequest(t, h, "GetUsageProfile", map[string]any{"Name": "my-profile"})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-profile")

	// List shows it
	rec = doGlueRequest(t, h, "ListUsageProfiles", map[string]any{})
	assert.Contains(t, rec.Body.String(), "my-profile")

	// Update
	rec = doGlueRequest(t, h, "UpdateUsageProfile", map[string]any{"Name": "my-profile"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doGlueRequest(t, h, "DeleteUsageProfile", map[string]any{"Name": "my-profile"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- CustomEntityType ---

func TestBatch2_CustomEntityType_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
		"Name":        "my-cet",
		"RegexString": "\\d+",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-cet")

	// Get via GetCustomEntityType
	rec = doGlueRequest(t, h, "GetCustomEntityType", map[string]any{"Name": "my-cet"})
	require.Equal(t, http.StatusOK, rec.Code)

	// List
	rec = doGlueRequest(t, h, "ListCustomEntityTypes", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-cet")

	// Delete
	rec = doGlueRequest(t, h, "DeleteCustomEntityType", map[string]any{"Name": "my-cet"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- DataQuality Recommendation Runs ---

func TestBatch2_DataQualityRecommendationRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start
	startRec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{})
	require.Equal(t, http.StatusOK, startRec.Code)
	assert.Contains(t, startRec.Body.String(), "RunId")
	var startOut struct {
		RunID string `json:"RunId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	require.NotEmpty(t, startOut.RunID)

	// List
	rec := doGlueRequest(t, h, "ListDataQualityRuleRecommendationRuns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Runs")

	// Get (empty RunId → returns stub)
	rec = doGlueRequest(t, h, "GetDataQualityRuleRecommendationRun", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Cancel using the run ID obtained from Start.
	rec = doGlueRequest(t, h, "CancelDataQualityRuleRecommendationRun", map[string]any{
		"RunId": startOut.RunID,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- ColumnStatisticsTask ---

func TestBatch2_ColumnStatisticsTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start a run
	rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ColumnStatisticsTaskRunId")

	// List runs
	rec = doGlueRequest(t, h, "ListColumnStatisticsTaskRuns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get task settings
	rec = doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- MaterializedView ---

func TestBatch2_MaterializedViewRefresh(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Start
	rec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "RunId")

	// List
	rec = doGlueRequest(t, h, "ListMaterializedViewRefreshTaskRuns", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Runs")
}

// --- Integration ---

func TestBatch2_Integration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doGlueRequest(t, h, "CreateIntegration", map[string]any{"IntegrationName": "my-integration"})
	require.Equal(t, http.StatusOK, rec.Code)

	// DescribeIntegrations
	rec = doGlueRequest(t, h, "DescribeIntegrations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Integrations")

	// ModifyIntegration
	rec = doGlueRequest(t, h, "ModifyIntegration", map[string]any{"IntegrationIdentifier": "my-integration"})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteIntegration
	rec = doGlueRequest(t, h, "DeleteIntegration", map[string]any{"IntegrationIdentifier": "my-integration"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// --- GlueIdentityCenter ---

func TestBatch2_GlueIdentityCenter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create config
	rec := doGlueRequest(t, h, "CreateGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Get config
	rec = doGlueRequest(t, h, "GetGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	// Update
	rec = doGlueRequest(t, h, "UpdateGlueIdentityCenterConfiguration", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doGlueRequest(t, h, "DeleteGlueIdentityCenterConfiguration", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
}
