package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestStartQueryPlanning_RequiresDatabaseName(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{},
		"QueryString":          "SELECT * FROM table",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out["message"].(string), "DatabaseName")
}

func TestStartQueryPlanning_RequiresQueryString(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "mydb"},
		"QueryString":          "",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestStartQueryPlanning_Success(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "mydb"},
		"QueryString":          "SELECT * FROM mytable",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec.Body, &out))
	assert.NotEmpty(t, out["QueryId"])
}

// --- GetWorkUnits returns queryID in output ---

func TestGetWorkUnits_QueryIDInOutput(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "db"},
		"QueryString":          "SELECT 1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var planOut map[string]any
	require.NoError(t, jsonDecode(rec.Body, &planOut))
	queryID := planOut["QueryId"].(string)

	rec2 := postJSON(t, h, "/GetWorkUnits", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var workOut map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &workOut))
	assert.Equal(t, queryID, workOut["QueryId"], "QueryId should echo back the request QueryId")
	assert.NotNil(t, workOut["WorkUnitRanges"])
}

// --- GetEffectivePermissionsForPath ---

func TestQueryLifecycle(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Start planning
	planRec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryPlanningContext": map[string]any{"DatabaseName": "mydb"},
		"QueryString":          "SELECT 1",
	})
	require.Equal(t, http.StatusOK, planRec.Code)

	var planOut map[string]any
	require.NoError(t, jsonDecode(planRec.Body, &planOut))
	queryID := planOut["QueryId"].(string)

	// Get state
	stateRec := postJSON(t, h, "/GetQueryState", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, stateRec.Code)

	var stateOut map[string]any
	require.NoError(t, jsonDecode(stateRec.Body, &stateOut))
	assert.Equal(t, "WORKUNITS_AVAILABLE", stateOut["State"])

	// Get statistics
	statsRec := postJSON(t, h, "/GetQueryStatistics", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, statsRec.Code)

	// Get work unit results
	resultsRec := postJSON(t, h, "/GetWorkUnitResults", map[string]any{
		"QueryId":       queryID,
		"WorkUnitToken": "synthetic-token",
	})
	assert.Equal(t, http.StatusOK, resultsRec.Code)
}

func TestGetQueryState_NotFound(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/GetQueryState", map[string]any{"QueryId": "nonexistent-query"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Temporary credentials ---

func TestGetWorkUnits_ReturnsRanges(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryString":          "SELECT 1",
		"QueryPlanningContext": map[string]any{"DatabaseName": "db"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var pOut map[string]any
	require.NoError(t, jsonDecode(rec.Body, &pOut))
	queryID := pOut["QueryId"].(string)

	rec2 := postJSON(t, h, "/GetWorkUnits", map[string]any{"QueryId": queryID})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out map[string]any
	require.NoError(t, jsonDecode(rec2.Body, &out))
	ranges := out["WorkUnitRanges"].([]any)
	assert.NotEmpty(t, ranges, "GetWorkUnits should return at least one range")
}
