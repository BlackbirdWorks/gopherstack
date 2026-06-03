package glue_test

import (
	"encoding/json"
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// createTestMLTransform is a helper that creates an ML transform and returns its ID.
func createTestMLTransform(t *testing.T, h *glue.Handler, name string) string {
	t.Helper()

	rec := doGlueRequest(t, h, "CreateMLTransform", map[string]any{
		"Name": name,
		"Role": "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TransformID string `json:"TransformId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	require.NotEmpty(t, out.TransformID)

	return out.TransformID
}

// TestMLEvaluationTaskRun exercises the ML evaluation task run lifecycle.
func TestMLEvaluationTaskRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		setupFn     func(t *testing.T, h *glue.Handler) (transformID string)
		transformID string // override if not using setup
		wantCode    int
	}{
		{
			name: "unknown_transform_returns_400",
			setupFn: func(_ *testing.T, _ *glue.Handler) string {
				return "no-such-transform"
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "known_transform_returns_200",
			setupFn: func(t *testing.T, h *glue.Handler) string {
				t.Helper()

				return createTestMLTransform(t, h, "eval-transform")
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			transformID := tc.setupFn(t, h)

			rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
				"TransformId": transformID,
			})
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestMLLabelingSetGenerationTaskRun exercises the labeling set generation task run.
func TestMLLabelingSetGenerationTaskRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	transformID := createTestMLTransform(t, h, "label-transform")

	rec := doGlueRequest(t, h, "StartMLLabelingSetGenerationTaskRun", map[string]any{
		"TransformId": transformID,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		TaskRunID string `json:"TaskRunId"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out.TaskRunID)
}

// TestMLExportImportLabels exercises export/import label task runs.
func TestMLExportImportLabels(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extraInput map[string]any
		name       string
		action     string
	}{
		{
			name:       "export_labels",
			action:     "StartExportLabelsTaskRun",
			extraInput: map[string]any{"OutputS3Path": "s3://bucket/labels/"},
		},
		{
			name:       "import_labels",
			action:     "StartImportLabelsTaskRun",
			extraInput: map[string]any{"InputS3Path": "s3://bucket/import/"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			transformID := createTestMLTransform(t, h, tc.name+"-transform")

			input := map[string]any{"TransformId": transformID}
			maps.Copy(input, tc.extraInput)

			rec := doGlueRequest(t, h, tc.action, input)
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				TaskRunID string `json:"TaskRunId"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out.TaskRunID)
		})
	}
}

// TestGetMLTaskRun exercises GetMLTaskRun with various inputs.
func TestGetMLTaskRun(t *testing.T) {
	t.Parallel()

	t.Run("empty_ids_returns_ok", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "SUCCEEDED", out.Status)
	})

	t.Run("unknown_run_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{
			"TransformId": "t-missing",
			"TaskRunId":   "r-missing",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("lifecycle_start_get_cancel", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		transformID := createTestMLTransform(t, h, "lifecycle-transform")

		startRec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
			"TransformId": transformID,
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			TaskRunID string `json:"TaskRunId"`
		}
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))

		getRec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{
			"TransformId": transformID,
			"TaskRunId":   startOut.TaskRunID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut struct {
			TransformID string `json:"TransformId"`
			TaskRunID   string `json:"TaskRunId"`
			Status      string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
		assert.Equal(t, transformID, getOut.TransformID)
		assert.Equal(t, startOut.TaskRunID, getOut.TaskRunID)
		assert.Equal(t, "RUNNING", getOut.Status)

		cancelRec := doGlueRequest(t, h, "CancelMLTaskRun", map[string]any{
			"TransformId": transformID,
			"TaskRunId":   startOut.TaskRunID,
		})
		require.Equal(t, http.StatusOK, cancelRec.Code)

		getRec2 := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{
			"TransformId": transformID,
			"TaskRunId":   startOut.TaskRunID,
		})
		require.Equal(t, http.StatusOK, getRec2.Code)

		var getOut2 struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut2))
		assert.Equal(t, "STOPPED", getOut2.Status)
	})
}

// TestGetMLTaskRuns exercises GetMLTaskRuns listing.
func TestGetMLTaskRuns(t *testing.T) {
	t.Parallel()

	t.Run("empty_transform_id_returns_empty_list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMLTaskRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			TaskRuns []any `json:"TaskRuns"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Empty(t, out.TaskRuns)
	})

	t.Run("multiple_runs_returned", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		transformID := createTestMLTransform(t, h, "multi-run-transform")

		for range 3 {
			rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{
				"TransformId": transformID,
			})
			require.Equal(t, http.StatusOK, rec.Code)
		}

		rec := doGlueRequest(t, h, "GetMLTaskRuns", map[string]any{
			"TransformId": transformID,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			TaskRuns []any `json:"TaskRuns"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Len(t, out.TaskRuns, 3)
	})
}

// TestCancelMLTaskRun exercises CancelMLTaskRun error cases.
func TestCancelMLTaskRun(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "both_ids_empty_returns_ok",
			input:    map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown_run_returns_400",
			input:    map[string]any{"TransformId": "t-unknown", "TaskRunId": "r-unknown"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CancelMLTaskRun", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestListMLTransforms_Stateful verifies ListMLTransforms returns real IDs.
func TestListMLTransforms_Stateful(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		createCount int
		wantLen     int
	}{
		{name: "empty", createCount: 0, wantLen: 0},
		{name: "one_transform", createCount: 1, wantLen: 1},
		{name: "three_transforms", createCount: 3, wantLen: 3},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range tc.createCount {
				createTestMLTransform(t, h, "transform-"+string(rune('a'+i)))
			}

			rec := doGlueRequest(t, h, "ListMLTransforms", map[string]any{})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				TransformIDs []string `json:"TransformIds"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Len(t, out.TransformIDs, tc.wantLen)
		})
	}
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

// TestUsageProfile_ErrorPropagation verifies 404 on unknown usage profile.
func TestUsageProfile_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "get_not_found",
			action:   "GetUsageProfile",
			input:    map[string]any{"Name": "no-such-profile"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get_empty_name_ok",
			action:   "GetUsageProfile",
			input:    map[string]any{"Name": ""},
			wantCode: http.StatusOK,
		},
		{
			name:     "create_ok",
			action:   "CreateUsageProfile",
			input:    map[string]any{"Name": "my-profile"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteUsageProfile",
			input:    map[string]any{"Name": "any-profile"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tc.action, tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestBlueprintRun_ErrorPropagation verifies error propagation for blueprint runs.
func TestBlueprintRun_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty_run_id_returns_ok",
			input:    map[string]any{"BlueprintName": "bp", "RunId": ""},
			wantCode: http.StatusOK,
		},
		{
			name:     "unknown_run_id_returns_400",
			input:    map[string]any{"BlueprintName": "bp", "RunId": "no-such-run"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetBlueprintRun", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
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

// TestMaterializedViewRefresh_Stateful verifies materialized view refresh lifecycle.
func TestMaterializedViewRefresh_Stateful(t *testing.T) {
	t.Parallel()

	t.Run("start_get_by_id_stop", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		startRec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{
			"DatabaseName": "mydb",
			"TableName":    "myview",
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		var startOut struct {
			RunID string `json:"RunId"`
		}
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
		assert.NotEmpty(t, startOut.RunID)

		getRec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": startOut.RunID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		var getOut struct {
			RunID  string `json:"RunId"`
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
		assert.Equal(t, startOut.RunID, getOut.RunID)
		assert.Equal(t, "RUNNING", getOut.Status)

		stopRec := doGlueRequest(t, h, "StopMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": startOut.RunID,
		})
		require.Equal(t, http.StatusOK, stopRec.Code)

		getRec2 := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": startOut.RunID,
		})
		require.Equal(t, http.StatusOK, getRec2.Code)

		var getOut2 struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getOut2))
		assert.Equal(t, "STOPPED", getOut2.Status)
	})

	t.Run("get_not_found_returns_400", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{
			"RunId": "mvr-not-found",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty_run_id_falls_back_to_first", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{
			"DatabaseName": "db1",
			"TableName":    "tbl1",
		})

		rec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			Status string `json:"Status"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.Equal(t, "RUNNING", out.Status)
	})
}

// TestGlueIdentityCenter_Stateful verifies IdentityCenter configuration lifecycle.
func TestGlueIdentityCenter_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateGlueIdentityCenterConfiguration", map[string]any{
		"InstanceArn": "arn:aws:sso:::instance/ssoins-1234",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	getRec := doGlueRequest(t, h, "GetGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, getRec.Code)

	var getOut struct {
		InstanceArn string `json:"InstanceArn"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "arn:aws:sso:::instance/ssoins-1234", getOut.InstanceArn)

	updateRec := doGlueRequest(t, h, "UpdateGlueIdentityCenterConfiguration", map[string]any{
		"InstanceArn": "arn:aws:sso:::instance/ssoins-5678",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	deleteRec := doGlueRequest(t, h, "DeleteGlueIdentityCenterConfiguration", map[string]any{})
	require.Equal(t, http.StatusOK, deleteRec.Code)
}

// TestCustomEntityType_ErrorPropagation verifies error propagation for custom entity types.
func TestCustomEntityType_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *glue.Handler)
		input    map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create_ok",
			action:   "CreateCustomEntityType",
			input:    map[string]any{"Name": "my-cet", "RegexString": `\d+`},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteCustomEntityType",
			input:    map[string]any{"Name": "no-such-cet"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "delete_after_create_ok",
			action: "DeleteCustomEntityType",
			setup: func(t *testing.T, h *glue.Handler) {
				t.Helper()
				rec := doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
					"Name": "to-delete", "RegexString": `\w+`,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:    map[string]any{"Name": "to-delete"},
			wantCode: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tc.setup != nil {
				tc.setup(t, h)
			}

			rec := doGlueRequest(t, h, tc.action, tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestColumnStatisticsTaskSettings_Stateful verifies column statistics task settings lifecycle.
func TestColumnStatisticsTaskSettings_Stateful(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"RoleArn":      "arn:aws:iam::000000000000:role/GlueRole",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	updateRec := doGlueRequest(t, h, "UpdateColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
		"RoleArn":      "arn:aws:iam::000000000000:role/NewGlueRole",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	getRec := doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	require.Equal(t, http.StatusOK, getRec.Code)

	deleteRec := doGlueRequest(t, h, "DeleteColumnStatisticsTaskSettings", map[string]any{
		"DatabaseName": "mydb",
		"TableName":    "mytable",
	})
	require.Equal(t, http.StatusOK, deleteRec.Code)
}

// TestIntegration_ErrorPropagation verifies integration create/delete/modify lifecycle.
func TestIntegration_ErrorPropagation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		action   string
		wantCode int
	}{
		{
			name:     "create_ok",
			action:   "CreateIntegration",
			input:    map[string]any{"IntegrationName": "my-integration"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete_not_found",
			action:   "DeleteIntegration",
			input:    map[string]any{"IntegrationIdentifier": "no-such-integration"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "modify_not_found",
			action:   "ModifyIntegration",
			input:    map[string]any{"IntegrationIdentifier": "no-such-integration"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, tc.action, tc.input)
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
