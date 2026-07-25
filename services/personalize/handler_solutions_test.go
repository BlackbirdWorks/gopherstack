package personalize_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPersonalize_Solution_FieldRetention(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "my-solution-dg")

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "my-solution",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"performAutoML":   false,
		"performHPO":      true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn := personalizeUnmarshal(t, rec)["solutionArn"].(string)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sol := personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, "my-solution", sol["name"])
	assert.Equal(t, "arn:aws:personalize:::recipe/aws-user-personalization", sol["recipeArn"])
	assert.Equal(t, false, sol["performAutoML"])
	assert.Equal(t, true, sol["performHPO"])
	assert.Equal(t, "ACTIVE", sol["status"])
	// performAutoTraining defaults to true when omitted from CreateSolution.
	assert.Equal(t, true, sol["performAutoTraining"])
	assert.Equal(t, false, sol["performIncrementalUpdate"])
	// AutoML was not requested -- autoMLResult must be absent, not a
	// fabricated empty object.
	assert.NotContains(t, sol, "autoMLResult")
}

// TestPersonalize_Solution_ConfigEventTypeAndAutoMLResult locks three
// previously-missing wire fields: eventType (a plain CreateSolutionInput
// member that was completely unread), solutionConfig (round-tripped
// opaquely, inherited onto SolutionVersion at training time), and
// autoMLResult (types.AutoMLResult.bestRecipeArn, only populated when
// performAutoML is true).
func TestPersonalize_Solution_ConfigEventTypeAndAutoMLResult(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "automl-dg")

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "automl-sol",
		"datasetGroupArn": dgArn,
		"eventType":       "click",
		"performAutoML":   true,
		"solutionConfig": map[string]any{
			"eventValueThreshold": "0.5",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn := personalizeUnmarshal(t, rec)["solutionArn"].(string)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sol := personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, "click", sol["eventType"])
	cfg, ok := sol["solutionConfig"].(map[string]any)
	require.True(t, ok, "solutionConfig must round-trip on Describe")
	assert.Equal(t, "0.5", cfg["eventValueThreshold"])
	amlResult, ok := sol["autoMLResult"].(map[string]any)
	require.True(t, ok, "autoMLResult must be populated when performAutoML is true")
	assert.NotEmpty(t, amlResult["bestRecipeArn"])

	// solutionConfig is inherited onto SolutionVersion at training time.
	rec = personalizeDo(t, h, "CreateSolutionVersion", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := personalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = personalizeDo(t, h, "DescribeSolutionVersion", map[string]any{"solutionVersionArn": svArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sv := personalizeUnmarshal(t, rec)["solutionVersion"].(map[string]any)
	svCfg, ok := sv["solutionConfig"].(map[string]any)
	require.True(t, ok, "solutionVersion.solutionConfig must be inherited from the parent solution")
	assert.Equal(t, "0.5", svCfg["eventValueThreshold"])
}

// TestPersonalize_Solution_LatestSolutionUpdate locks that
// latestSolutionUpdate (types.SolutionUpdateSummary) only appears on
// Describe once the solution has had at least one UpdateSolution call.
func TestPersonalize_Solution_LatestSolutionUpdate(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "latest-update-dg")

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "latest-update-sol",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn := personalizeUnmarshal(t, rec)["solutionArn"].(string)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	sol := personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.NotContains(t, sol, "latestSolutionUpdate")

	rec = personalizeDo(t, h, "UpdateSolution", map[string]any{
		"solutionArn":         solArn,
		"performAutoTraining": false,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	sol = personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	update, ok := sol["latestSolutionUpdate"].(map[string]any)
	require.True(t, ok, "latestSolutionUpdate must appear after an UpdateSolution call")
	assert.Equal(t, false, update["performAutoTraining"])
	assert.Equal(t, "ACTIVE", update["status"])
}

// TestPersonalize_Solution_Update verifies UpdateSolution mutates the
// real wire fields (performAutoTraining/performIncrementalUpdate), not the
// creation-only performAutoML/performHPO fields the real UpdateSolutionInput
// does not carry.
func TestPersonalize_Solution_Update(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	dgArn := personalizeCreateDatasetGroup(t, h, "update-me-dg")

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "update-me",
		"datasetGroupArn": dgArn,
		"recipeArn":       "arn:aws:personalize:::recipe/aws-user-personalization",
		"performAutoML":   false,
		"performHPO":      true,
	})
	require.Equal(t, http.StatusOK, rec.Code)
	solArn := personalizeUnmarshal(t, rec)["solutionArn"].(string)

	rec = personalizeDo(t, h, "UpdateSolution", map[string]any{
		"solutionArn":              solArn,
		"performAutoTraining":      false,
		"performIncrementalUpdate": true,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	sol := personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, false, sol["performAutoTraining"])
	assert.Equal(t, true, sol["performIncrementalUpdate"])
	// performAutoML/performHPO are creation-only and must be unaffected by UpdateSolution.
	assert.Equal(t, false, sol["performAutoML"])
	assert.Equal(t, true, sol["performHPO"])

	// Omitting both update fields leaves the current values untouched.
	rec = personalizeDo(t, h, "UpdateSolution", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeSolution", map[string]any{"solutionArn": solArn})
	sol = personalizeUnmarshal(t, rec)["solution"].(map[string]any)
	assert.Equal(t, false, sol["performAutoTraining"])
	assert.Equal(t, true, sol["performIncrementalUpdate"])
}

// --- SolutionVersion ---

func TestPersonalize_SolutionVersion_Lifecycle(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	solArn := personalizeCreateSolution(t, h, "my-solution")

	// Create solution version
	rec := personalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn":  solArn,
		"trainingMode": "FULL",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn, _ := personalizeUnmarshal(t, rec)["solutionVersionArn"].(string)
	assert.True(t, strings.HasPrefix(svArn, solArn+"/"), "solutionVersionArn should start with solutionArn/")

	// Describe
	rec = personalizeDo(t, h, "DescribeSolutionVersion", map[string]any{"solutionVersionArn": svArn})
	require.Equal(t, http.StatusOK, rec.Code)
	sv := personalizeUnmarshal(t, rec)["solutionVersion"].(map[string]any)
	assert.Equal(t, solArn, sv["solutionArn"])
	assert.Equal(t, "FULL", sv["trainingMode"])
	assert.Equal(t, "ACTIVE", sv["status"])

	// List
	rec = personalizeDo(t, h, "ListSolutionVersions", map[string]any{"solutionArn": solArn})
	require.Equal(t, http.StatusOK, rec.Code)
	listed := personalizeUnmarshal(t, rec)
	versions := listed["solutionVersions"].([]any)
	assert.Len(t, versions, 1)
}

// TestPersonalize_DeleteSolutionVersion_NotARealOperation locks the removal
// of a gopherstack-invented op: the real aws-sdk-go-v2/service/personalize
// v1.47.11 Client has no DeleteSolutionVersion method at all (only
// DeleteSolution removes a solution and all its versions together) --
// verified by the absence of api_op_DeleteSolutionVersion.go in the SDK
// module. It must now be rejected the same way any other unrecognized
// operation name is, not silently succeed.
func TestPersonalize_DeleteSolutionVersion_NotARealOperation(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)
	svArn := personalizeCreateSolutionVersion(t, h, "sol")

	rec := personalizeDo(t, h, "DeleteSolutionVersion", map[string]any{"solutionVersionArn": svArn})

	require.Equal(t, http.StatusBadRequest, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, "InvalidInputException", m["__type"])

	// The solution version must still exist -- the request never reached any
	// delete logic.
	rec = personalizeDo(t, h, "DescribeSolutionVersion", map[string]any{"solutionVersionArn": svArn})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestPersonalize_StopSolutionVersionCreation(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	svArn := personalizeCreateSolutionVersion(t, h, "sol")

	rec := personalizeDo(t, h, "StopSolutionVersionCreation", map[string]any{"solutionVersionArn": svArn})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = personalizeDo(t, h, "DescribeSolutionVersion", map[string]any{"solutionVersionArn": svArn})
	sv := personalizeUnmarshal(t, rec)["solutionVersion"].(map[string]any)
	// The SolutionVersion.Status wire enum has no bare "STOPPED" value --
	// only "CREATE STOPPED" (see aws-sdk-go-v2/service/personalize/types.SolutionVersion).
	assert.Equal(t, "CREATE STOPPED", sv["status"])
}

func TestPersonalize_GetSolutionMetrics(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	svArn := personalizeCreateSolutionVersion(t, h, "sol")

	rec := personalizeDo(t, h, "GetSolutionMetrics", map[string]any{"solutionVersionArn": svArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, svArn, m["solutionVersionArn"])
	metrics, ok := m["metrics"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, metrics, "coverage")
	assert.Contains(t, metrics, "precision_at_5")
}

// --- Campaign ---
