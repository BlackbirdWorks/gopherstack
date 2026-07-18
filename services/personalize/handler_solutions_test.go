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

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "my-solution",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
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
}

// TestPersonalize_Solution_Update verifies UpdateSolution mutates the
// real wire fields (performAutoTraining/performIncrementalUpdate), not the
// creation-only performAutoML/performHPO fields the real UpdateSolutionInput
// does not carry.
func TestPersonalize_Solution_Update(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateSolution", map[string]any{
		"name":            "update-me",
		"datasetGroupArn": "arn:aws:personalize:us-east-1:000000000000:dataset-group/g1",
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

	solArn := "arn:aws:personalize:us-east-1:000000000000:solution/my-solution"

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

func TestPersonalize_StopSolutionVersionCreation(t *testing.T) {
	t.Parallel()

	h := personalizeHandler(t)

	rec := personalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := personalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = personalizeDo(t, h, "StopSolutionVersionCreation", map[string]any{"solutionVersionArn": svArn})
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

	rec := personalizeDo(t, h, "CreateSolutionVersion", map[string]any{
		"solutionArn": "arn:aws:personalize:us-east-1:000000000000:solution/sol",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	svArn := personalizeUnmarshal(t, rec)["solutionVersionArn"].(string)

	rec = personalizeDo(t, h, "GetSolutionMetrics", map[string]any{"solutionVersionArn": svArn})
	assert.Equal(t, http.StatusOK, rec.Code)
	m := personalizeUnmarshal(t, rec)
	assert.Equal(t, svArn, m["solutionVersionArn"])
	metrics, ok := m["metrics"].(map[string]any)
	require.True(t, ok)
	assert.Contains(t, metrics, "coverage")
	assert.Contains(t, metrics, "precision_at_5")
}

// --- Campaign ---
