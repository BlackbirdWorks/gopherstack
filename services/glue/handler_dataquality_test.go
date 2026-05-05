package glue_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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
			rulesets, _ := out["DataQualityRulesets"].([]any)
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
