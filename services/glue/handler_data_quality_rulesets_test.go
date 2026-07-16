package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// TestBatch2Audit_CancelDQRecommendationRun_NotFound verifies that
// CancelDataQualityRuleRecommendationRun raises EntityNotFoundException for
// an unknown run ID instead of silently succeeding.
func TestBatch2Audit_CancelDQRecommendationRun_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		runID     string
		wantError string
		wantCode  int
		create    bool
	}{
		{
			name:      "cancel_missing_run_returns_entity_not_found",
			runID:     "nonexistent-run-id",
			create:    false,
			wantCode:  http.StatusBadRequest,
			wantError: "EntityNotFoundException",
		},
		{
			name:     "cancel_existing_run_succeeds",
			create:   true,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			runID := tt.runID
			if tt.create {
				rec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{
					"DataSource": map[string]any{
						"GlueTable": map[string]any{"DatabaseName": "db", "TableName": "tbl"},
					},
					"Role": "arn:aws:iam::123456789012:role/GlueRole",
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out struct {
					RunID string `json:"RunId"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				require.NotEmpty(t, out.RunID, "expected RunId in response")
				runID = out.RunID
			}

			rec := doGlueRequest(t, h, "CancelDataQualityRuleRecommendationRun", map[string]any{
				"RunId": runID,
			})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantError != "" {
				assert.Contains(t, rec.Body.String(), tt.wantError)
			}
		})
	}
}

func TestBatch3_DataQuality_Ruleset_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		check       func(t *testing.T, body string)
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name:        "create",
			op:          "CreateDataQualityRuleset",
			body:        map[string]any{"Name": "rs1", "Ruleset": "Rules = [ RowCount > 0 ]"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name:     "create-duplicate",
			op:       "CreateDataQualityRuleset",
			body:     map[string]any{"Name": "rs1", "Ruleset": "Rules = [ RowCount > 0 ]"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetDataQualityRuleset",
			body:     map[string]any{"Name": "rs1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "rs1")
			},
		},
		{
			name:     "get-missing",
			op:       "GetDataQualityRuleset",
			body:     map[string]any{"Name": "no-rs"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update",
			op:       "UpdateDataQualityRuleset",
			body:     map[string]any{"Name": "rs1", "Ruleset": "Rules = [ RowCount > 100 ]"},
			wantCode: http.StatusOK,
		},
		{
			name:     "list",
			op:       "ListDataQualityRulesets",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "rs1")
			},
		},
		{
			name:     "delete",
			op:       "DeleteDataQualityRuleset",
			body:     map[string]any{"Name": "rs1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-missing",
			op:       "DeleteDataQualityRuleset",
			body:     map[string]any{"Name": "no-rs"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
					"Name":    "rs1",
					"Ruleset": "Rules = [ RowCount > 0 ]",
				})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestBatch3_DataQuality_RuleRecommendationRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	startRec1 := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{
		"DataSource": map[string]any{"GlueTable": map[string]any{"DatabaseName": "db", "TableName": "t"}},
	})
	require.Equal(t, http.StatusOK, startRec1.Code)
	assert.Contains(t, startRec1.Body.String(), "RunId")

	startRec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	getRecommendRec := doGlueRequest(t, h, "GetDataQualityRuleRecommendationRun", map[string]any{"RunId": runID})
	require.Equal(t, http.StatusOK, getRecommendRec.Code)
	var getRecommendOut map[string]any
	require.NoError(t, json.Unmarshal(getRecommendRec.Body.Bytes(), &getRecommendOut))
	assert.NotEmpty(t, getRecommendOut["RunId"])

	listRecommendRec := doGlueRequest(t, h, "ListDataQualityRuleRecommendationRuns", map[string]any{})
	require.Equal(t, http.StatusOK, listRecommendRec.Code)
	assert.Contains(t, listRecommendRec.Body.String(), "Runs")

	cancelRecommendRec := doGlueRequest(t, h, "CancelDataQualityRuleRecommendationRun", map[string]any{"RunId": runID})
	assert.Equal(t, http.StatusOK, cancelRecommendRec.Code)
}

// TestRefinement2_DataQualityRulesets tests data quality ruleset operations.
func TestRefinement2_DataQualityRulesets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*glue.Handler)
		body     map[string]any
		check    func(*testing.T, map[string]any)
		name     string
		action   string
		wantCode int
	}{
		{
			name: "list_uses_rulesets_field",
			setup: func(h *glue.Handler) {
				for i := range 2 {
					doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
						"Name":    fmt.Sprintf("rs-%d", i),
						"Ruleset": "Rules = [ RowCount > 0 ]",
					})
				}
			},
			action:   "ListDataQualityRulesets",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				rulesets, _ := out["Rulesets"].([]any)
				assert.Len(t, rulesets, 2)
				_, hasOldKey := out["DataQualityRulesets"]
				assert.False(t, hasOldKey, "should not use deprecated DataQualityRulesets key")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doGlueRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.check != nil && rec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

// TestRefinement2_DataQualityRulesetTagging tests tagging DataQualityRulesets via ARN.
func TestRefinement2_DataQualityRulesetTagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check    func(*testing.T, map[string]any)
		name     string
		wantCode int
	}{
		{
			name:     "tag_and_get_dq_ruleset",
			wantCode: http.StatusOK,
			check: func(t *testing.T, out map[string]any) {
				t.Helper()
				tags, _ := out["Tags"].(map[string]any)
				assert.Equal(t, "val", tags["key"])
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			b.AddDataQualityRulesetInternal(&glue.DataQualityRuleset{
				Name: "dq-rs", Ruleset: "Rules = [ RowCount > 0 ]",
			})
			h := glue.NewHandler(b)

			rsARN := "arn:aws:glue:us-east-1:000000000000:dataQualityRuleset/dq-rs"

			rec := doGlueRequest(t, h, "TagResource", map[string]any{
				"ResourceArn": rsARN,
				"TagsToAdd":   map[string]string{"key": "val"},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec2 := doGlueRequest(t, h, "GetTags", map[string]any{"ResourceArn": rsARN})
			require.Equal(t, tt.wantCode, rec2.Code)

			if tt.check != nil {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out))
				tt.check(t, out)
			}
		})
	}
}

func TestHandlerDataQuality_CreateDataQualityRuleset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rName    string
		wantCode int
	}{
		{
			name:     "success",
			rName:    "my-ruleset",
			wantCode: http.StatusOK,
		},
		{
			name:     "duplicate",
			rName:    "my-ruleset",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.name == "duplicate" {
				rec := doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
					"Name":    "my-ruleset",
					"Ruleset": "Rules = [ RowCount > 100 ]",
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    tt.rName,
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerDataQuality_GetDataQualityRuleset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rName    string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "success",
			rName:    "my-ruleset",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			rName:    "no-such-ruleset",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			rec := doGlueRequest(t, h, "GetDataQualityRuleset", map[string]any{"Name": tt.rName})

			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "my-ruleset", out["Name"])
			}
		})
	}
}

func TestHandlerDataQuality_DeleteDataQualityRuleset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		rName    string
		wantCode int
	}{
		{
			name:     "success",
			rName:    "my-ruleset",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			rName:    "no-such-ruleset",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			rec := doGlueRequest(t, h, "DeleteDataQualityRuleset", map[string]any{"Name": tt.rName})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerDataQuality_UpdateDataQualityRuleset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			rec := doGlueRequest(t, h, "UpdateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 200 ]",
			})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandlerDataQuality_ListDataQualityRulesets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		numCreate int
		wantLen   int
		wantCode  int
	}{
		{
			name:      "empty",
			numCreate: 0,
			wantLen:   0,
			wantCode:  http.StatusOK,
		},
		{
			name:      "populated",
			numCreate: 3,
			wantLen:   3,
			wantCode:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range tt.numCreate {
				doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
					"Name":    fmt.Sprintf("ruleset-%d", i),
					"Ruleset": "Rules = [ RowCount > 100 ]",
				})
			}

			rec := doGlueRequest(t, h, "ListDataQualityRulesets", map[string]any{})

			require.Equal(t, tt.wantCode, rec.Code)
			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			rulesets, _ := out["Rulesets"].([]any)
			assert.Len(t, rulesets, tt.wantLen)
		})
	}
}

func TestHandlerDataQuality_StartDataQualityRulesetEvaluationRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		rulesetNames []string
		wantCode     int
		wantErr      bool
	}{
		{
			name:         "success",
			rulesetNames: []string{"my-ruleset"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "unknown_ruleset",
			rulesetNames: []string{"no-such-ruleset"},
			wantCode:     http.StatusBadRequest,
			wantErr:      true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			rec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
				"RulesetNames": tt.rulesetNames,
			})

			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]string
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out["RunId"])
			}
		})
	}
}

func TestHandlerDataQuality_GetDataQualityRulesetEvaluationRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		runID    string
		wantCode int
		wantErr  bool
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			runID:    "no-such-run",
			wantCode: http.StatusBadRequest,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			runID := tt.runID
			if runID == "" {
				startRec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
					"RulesetNames": []string{"my-ruleset"},
				})
				require.Equal(t, http.StatusOK, startRec.Code)
				var out map[string]string
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &out))
				runID = out["RunId"]
			}

			rec := doGlueRequest(t, h, "GetDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})

			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotNil(t, out["DataQualityEvaluationRun"])
			}
		})
	}
}

func TestHandlerDataQuality_CancelDataQualityRulesetEvaluationRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		preCancel bool
		wantCode  int
	}{
		{
			name:     "success",
			wantCode: http.StatusOK,
		},
		{
			name:      "already_cancelled",
			preCancel: true,
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateDataQualityRuleset", map[string]any{
				"Name":    "my-ruleset",
				"Ruleset": "Rules = [ RowCount > 100 ]",
			})

			startRec := doGlueRequest(t, h, "StartDataQualityRulesetEvaluationRun", map[string]any{
				"RulesetNames": []string{"my-ruleset"},
			})
			require.Equal(t, http.StatusOK, startRec.Code)
			var startOut map[string]string
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
			runID := startOut["RunId"]

			if tt.preCancel {
				rec := doGlueRequest(t, h, "CancelDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doGlueRequest(t, h, "CancelDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})

			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}
