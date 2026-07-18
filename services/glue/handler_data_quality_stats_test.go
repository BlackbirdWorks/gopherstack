package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestDataQualityRecommendationRun(t *testing.T) {
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

func TestDataQuality_EvaluationRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
		"Name":    "eval-rs",
		"Ruleset": "Rules = [ RowCount > 0 ]",
	})

	tests := []struct {
		fn       func() (string, int)
		name     string
		wantCode int
	}{
		{
			name: "start-evaluation-run",
			fn: func() (string, int) {
				rec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
					"RulesetNames": []string{"eval-rs"},
					"DataSource":   map[string]any{},
				})

				return rec.Body.String(), rec.Code
			},
			wantCode: http.StatusOK,
		},
		{
			name: "start-run-missing-ruleset",
			fn: func() (string, int) {
				rec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
					"RulesetNames": []string{"no-ruleset"},
				})

				return rec.Body.String(), rec.Code
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, code := tt.fn()
			assert.Equal(t, tt.wantCode, code)
		})
	}
}

func TestDataQuality_EvaluationRun_GetAndCancel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
		"Name":    "rs-eval",
		"Ruleset": "Rules = [ RowCount > 0 ]",
	})

	startRec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
		"RulesetNames": []string{"rs-eval"},
		"DataSource":   map[string]any{},
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	getEvalRec := doGlueRequest(t, h, "GetDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
	require.Equal(t, http.StatusOK, getEvalRec.Code)
	var getEvalOut map[string]any
	require.NoError(t, json.Unmarshal(getEvalRec.Body.Bytes(), &getEvalOut))
	evalRun, ok := getEvalOut["DataQualityEvaluationRun"].(map[string]any)
	require.True(t, ok, "expected DataQualityEvaluationRun field in response")
	assert.Equal(t, "RUNNING", evalRun["Status"])

	cancelEvalRec := doGlueRequest(t, h, "CancelDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
	assert.Equal(t, http.StatusOK, cancelEvalRec.Code)
}

// TestGetDataQualityModel_RequiresProfileID verifies GetDataQualityModel parses
// and validates ProfileId rather than silently ignoring it.
func TestGetDataQualityModel_RequiresProfileID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetDataQualityModel", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doGlueRequest(t, h, "GetDataQualityModel", map[string]any{"ProfileId": "profile-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Status string `json:"Status"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "SUCCEEDED", out.Status)
}

// TestGetDataQualityModelResult_RequiresBothIDs verifies both ProfileId and
// StatisticId are required, matching the real (non-optional-StatisticId) SDK
// shape, and that the response uses the real Model/CompletedOn fields instead
// of a fabricated Status string.
func TestGetDataQualityModelResult_RequiresBothIDs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetDataQualityModelResult", map[string]any{"ProfileId": "p1"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	rec = doGlueRequest(
		t, h, "GetDataQualityModelResult", map[string]any{"ProfileId": "p1", "StatisticId": "s1"},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "Model")
	assert.NotContains(t, out, "Status", "GetDataQualityModelResult has no Status field in the real API")
}

// TestDataQualityStatisticAnnotations_RealStore verifies
// PutDataQualityProfileAnnotation and BatchPutDataQualityStatisticAnnotation
// write real state that ListDataQualityStatisticAnnotations reads back.
func TestDataQualityStatisticAnnotations_RealStore(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "ListDataQualityStatisticAnnotations", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var empty struct {
		Annotations []map[string]any `json:"Annotations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &empty))
	assert.Empty(t, empty.Annotations)

	rec = doGlueRequest(t, h, "PutDataQualityProfileAnnotation", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code, "missing required fields")

	rec = doGlueRequest(t, h, "PutDataQualityProfileAnnotation", map[string]any{
		"ProfileId": "profile-1", "InclusionAnnotation": "INCLUDE",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doGlueRequest(t, h, "BatchPutDataQualityStatisticAnnotation", map[string]any{
		"InclusionAnnotations": []map[string]any{
			{"ProfileId": "profile-1", "StatisticId": "stat-1", "InclusionAnnotation": "EXCLUDE"},
			{"StatisticId": "stat-2", "InclusionAnnotation": "EXCLUDE"}, // missing ProfileId: should fail
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var batchOut struct {
		FailedInclusionAnnotations []map[string]any `json:"FailedInclusionAnnotations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &batchOut))
	require.Len(t, batchOut.FailedInclusionAnnotations, 1)
	assert.Equal(t, "stat-2", batchOut.FailedInclusionAnnotations[0]["StatisticId"])

	rec = doGlueRequest(t, h, "ListDataQualityStatisticAnnotations", map[string]any{"ProfileId": "profile-1"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Annotations []map[string]any `json:"Annotations"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Annotations, 2, "the profile-wide and the per-statistic annotation")

	rec = doGlueRequest(t, h, "ListDataQualityStatisticAnnotations", map[string]any{
		"ProfileId": "profile-1", "StatisticId": "stat-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.Len(t, out.Annotations, 1)
	assert.Equal(t, "stat-1", out.Annotations[0]["StatisticId"])
}

// TestDataQualityEvaluationRuns_Stateful verifies listing evaluation runs.
func TestDataQualityEvaluationRuns_Stateful(t *testing.T) {
	t.Parallel()

	t.Run("empty_returns_empty_list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "ListDataQualityRulesetEvaluationRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Runs []any `json:"Runs"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.Runs)
	})

	t.Run("after_start_evaluation_run_returns_one", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		createRulesetRec := doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
			"Name":    "my-ruleset",
			"Ruleset": "Rules = [RowCount > 0]",
		})
		require.Equal(t, http.StatusOK, createRulesetRec.Code)

		startRec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
			"DataSource":   map[string]any{},
			"Role":         "arn:aws:iam::000000000000:role/GlueRole",
			"RulesetNames": []string{"my-ruleset"},
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		rec := doGlueRequest(t, h, "ListDataQualityRulesetEvaluationRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Runs []any `json:"Runs"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.Runs, 1)
	})
}

// TestListDataQualityResults_Stateful verifies listing data quality results.
func TestListDataQualityResults_Stateful(t *testing.T) {
	t.Parallel()

	t.Run("empty_returns_empty_list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "ListDataQualityResults", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Results []any `json:"Results"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.Results)
	})

	t.Run("after_seeding_results_appear", func(t *testing.T) {
		t.Parallel()

		backend := glue.NewInMemoryBackend(testAccountID, testRegion)
		h := glue.NewHandler(backend)

		backend.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "res-1", Score: 0.95})
		backend.AddDataQualityResultInternal(&glue.DataQualityResult{ResultID: "res-2", Score: 0.88})

		rec := doGlueRequest(t, h, "ListDataQualityResults", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Results []any `json:"Results"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.Results, 2)
	})
}

// TestDataQualityRecommendationRun_ErrorPropagation verifies error for missing run ID.
func TestDataQualityRecommendationRun_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty_run_id_returns_ok",
			input:    map[string]any{"RunId": ""},
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown_run_id_returns_400",
			input:    map[string]any{"RunId": "no-such-run"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetDataQualityRuleRecommendationRun", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestDataQualityRecommendationRun_Stateful verifies that StartDataQualityRuleRecommendationRun
// stores state that can be retrieved.
func TestDataQualityRecommendationRun_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{
		"OutputS3Path": "s3://bucket/output/",
		"Role":         "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		RunID string `json:"RunId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	assert.NotEmpty(t, startOut.RunID)

	getRec := doGlueRequest(t, h, "GetDataQualityRuleRecommendationRun", map[string]any{
		"RunId": startOut.RunID,
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		RunID  string `json:"RunId"`
		Status string `json:"Status"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, startOut.RunID, getOut.RunID)
	assert.NotEmpty(t, getOut.Status)
}

// TestGetDataQualityModelResult_Validation verifies ProfileId and StatisticId
// are both required, matching the real GetDataQualityModelResult API where
// StatisticId (unlike in GetDataQualityModel) is mandatory.
func TestGetDataQualityModelResult_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "missing_profile_id_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
		{
			name:     "missing_statistic_id_returns_400",
			input:    map[string]any{"ProfileId": "profile-123"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "with_profile_id_and_statistic_id_returns_200",
			input:    map[string]any{"ProfileId": "profile-123", "StatisticId": "statistic-456"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetDataQualityModelResult", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

func TestGetDataQualityResult_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetDataQualityResult", map[string]any{
		"ResultId": "nonexistent-id",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestGetDataQualityResult_Found(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend(testAccountID, testRegion)
	b.AddDataQualityResultInternal(&glue.DataQualityResult{
		ResultID: "result-abc",
		Score:    0.95,
	})
	h := glue.NewHandler(b)

	rec := doGlueRequest(t, h, "GetDataQualityResult", map[string]any{
		"ResultId": "result-abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		ResultID string  `json:"ResultId"`
		Score    float64 `json:"Score"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "result-abc", out.ResultID)
	assert.InDelta(t, 0.95, out.Score, 0.001)
}
