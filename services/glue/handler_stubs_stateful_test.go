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
			name:     "both_ids_empty_returns_400",
			input:    map[string]any{},
			wantCode: http.StatusBadRequest,
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
			name:     "empty_run_id_returns_400",
			input:    map[string]any{"BlueprintName": "bp", "RunId": ""},
			wantCode: http.StatusBadRequest,
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

// ---------------------------------------------------------------------------
// TestCatalogImport — ImportCatalogToGlue + GetCatalogImportStatus
// ---------------------------------------------------------------------------

// TestCatalogImport_Lifecycle verifies that ImportCatalogToGlue sets the
// import state and GetCatalogImportStatus reflects it.
func TestCatalogImport_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		catalogID     string
		importFirst   bool
		wantCompleted bool
	}{
		{
			name:          "not_yet_imported_returns_false",
			catalogID:     "my-catalog",
			importFirst:   false,
			wantCompleted: false,
		},
		{
			name:          "after_import_returns_completed",
			catalogID:     "my-catalog",
			importFirst:   true,
			wantCompleted: true,
		},
		{
			name:          "empty_catalog_id_uses_account_default",
			catalogID:     "",
			importFirst:   true,
			wantCompleted: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.importFirst {
				rec := doGlueRequest(t, h, "ImportCatalogToGlue", map[string]any{
					"CatalogId": tc.catalogID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			getRec := doGlueRequest(t, h, "GetCatalogImportStatus", map[string]any{
				"CatalogId": tc.catalogID,
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var out struct {
				ImportStatus struct {
					ImportedBy      string  `json:"ImportedBy"`
					ImportTime      float64 `json:"ImportTime"`
					ImportCompleted bool    `json:"ImportCompleted"`
				} `json:"ImportStatus"`
			}
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
			assert.Equal(t, tc.wantCompleted, out.ImportStatus.ImportCompleted)

			if tc.wantCompleted {
				assert.NotEmpty(t, out.ImportStatus.ImportedBy)
				assert.Greater(t, out.ImportStatus.ImportTime, float64(0))
			}
		})
	}
}

// TestCatalogImport_Idempotent verifies repeated imports overwrite state.
func TestCatalogImport_Idempotent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for range 3 {
		rec := doGlueRequest(t, h, "ImportCatalogToGlue", map[string]any{"CatalogId": "c1"})
		require.Equal(t, http.StatusOK, rec.Code)
	}

	getRec := doGlueRequest(t, h, "GetCatalogImportStatus", map[string]any{"CatalogId": "c1"})
	require.Equal(t, http.StatusOK, getRec.Code)

	var out struct {
		ImportStatus struct {
			ImportCompleted bool `json:"ImportCompleted"`
		} `json:"ImportStatus"`
	}
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	assert.True(t, out.ImportStatus.ImportCompleted)
}

// ---------------------------------------------------------------------------
// TestSchemaVersionMetadata — Put / Query / Remove lifecycle
// ---------------------------------------------------------------------------

// setupTestSchema creates a registry, schema, and registers two versions.
// Returns the schema version IDs of the two registered versions.
func setupTestSchema(t *testing.T, h *glue.Handler) string {
	t.Helper()

	regRec := doGlueRequest(t, h, "CreateRegistry", map[string]any{
		"RegistryName": "reg1",
	})
	require.Equal(t, http.StatusOK, regRec.Code)

	schemaRec := doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId":    map[string]any{"RegistryName": "reg1"},
		"SchemaName":    "schema1",
		"DataFormat":    "AVRO",
		"Compatibility": "NONE",
	})
	require.Equal(t, http.StatusOK, schemaRec.Code)

	const schemaV2Def = `{"type":"record","name":"A","fields":[` +
		`{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	reg1Rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId":         map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
		"SchemaDefinition": `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`,
	})
	require.Equal(t, http.StatusOK, reg1Rec.Code)

	var sv1Out struct {
		SchemaVersionID string `json:"SchemaVersionId"`
	}
	require.NoError(t, json.Unmarshal(reg1Rec.Body.Bytes(), &sv1Out))

	reg2Rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
		"SchemaId":         map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
		"SchemaDefinition": schemaV2Def,
	})
	require.Equal(t, http.StatusOK, reg2Rec.Code)

	var sv2Out struct {
		SchemaVersionID string `json:"SchemaVersionId"`
	}
	require.NoError(t, json.Unmarshal(reg2Rec.Body.Bytes(), &sv2Out))

	return sv1Out.SchemaVersionID
}

// TestSchemaVersionMetadata_Lifecycle tests put/query/remove round-trip.
func TestSchemaVersionMetadata_Lifecycle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pairs     map[string]string
		wantAfter map[string]string
		name      string
		removeKey string
	}{
		{
			name:      "single_key_put_and_query",
			pairs:     map[string]string{"owner": "team-a"},
			wantAfter: map[string]string{"owner": "team-a"},
		},
		{
			name:      "multiple_keys",
			pairs:     map[string]string{"owner": "team-a", "env": "prod"},
			wantAfter: map[string]string{"owner": "team-a", "env": "prod"},
		},
		{
			name:      "put_then_remove",
			pairs:     map[string]string{"owner": "team-a", "env": "prod"},
			removeKey: "env",
			wantAfter: map[string]string{"owner": "team-a"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			v1ID := setupTestSchema(t, h)

			for k, v := range tc.pairs {
				putRec := doGlueRequest(t, h, "PutSchemaVersionMetadata", map[string]any{
					"SchemaVersionId":  v1ID,
					"MetadataKeyValue": map[string]any{"MetadataKey": k, "MetadataValue": v},
				})
				require.Equal(t, http.StatusOK, putRec.Code)
			}

			if tc.removeKey != "" {
				removeRec := doGlueRequest(t, h, "RemoveSchemaVersionMetadata", map[string]any{
					"SchemaVersionId":  v1ID,
					"MetadataKeyValue": map[string]any{"MetadataKey": tc.removeKey},
				})
				require.Equal(t, http.StatusOK, removeRec.Code)
			}

			queryRec := doGlueRequest(t, h, "QuerySchemaVersionMetadata", map[string]any{
				"SchemaVersionId": v1ID,
			})
			require.Equal(t, http.StatusOK, queryRec.Code)

			var out struct {
				MetadataInfo map[string]struct {
					MetadataValue string `json:"MetadataValue"`
				} `json:"MetadataInfo"`
				SchemaVersionID string `json:"SchemaVersionId"`
			}
			require.NoError(t, json.Unmarshal(queryRec.Body.Bytes(), &out))
			assert.Equal(t, v1ID, out.SchemaVersionID)

			got := make(map[string]string, len(out.MetadataInfo))
			for k, v := range out.MetadataInfo {
				got[k] = v.MetadataValue
			}

			assert.Equal(t, tc.wantAfter, got)
		})
	}
}

// TestSchemaVersionMetadata_EmptyQuery verifies empty map returned for unknown ID.
func TestSchemaVersionMetadata_EmptyQuery(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "QuerySchemaVersionMetadata", map[string]any{
		"SchemaVersionId": "no-such-version",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		MetadataInfo map[string]any `json:"MetadataInfo"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Empty(t, out.MetadataInfo)
}

// ---------------------------------------------------------------------------
// TestGetSchemaByDefinition
// ---------------------------------------------------------------------------

// TestGetSchemaByDefinition_Stateful verifies lookup by definition content.
func TestGetSchemaByDefinition_Stateful(t *testing.T) {
	t.Parallel()

	const def1 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"}]}`
	const def2 = `{"type":"record","name":"A","fields":[{"name":"id","type":"int"},{"name":"name","type":"string"}]}`

	tests := []struct {
		name       string
		definition string
		wantCode   int
		wantFound  bool
	}{
		{
			name:       "found_v1_by_definition",
			definition: def1,
			wantCode:   http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "found_v2_by_definition",
			definition: def2,
			wantCode:   http.StatusOK,
			wantFound:  true,
		},
		{
			name:       "unknown_definition_returns_400",
			definition: `{"type":"record","name":"X"}`,
			wantCode:   http.StatusBadRequest,
			wantFound:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupTestSchema(t, h)

			rec := doGlueRequest(t, h, "GetSchemaByDefinition", map[string]any{
				"SchemaId":         map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
				"SchemaDefinition": tc.definition,
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantFound {
				var out struct {
					SchemaVersionID string `json:"SchemaVersionId"`
					Status          string `json:"Status"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.NotEmpty(t, out.SchemaVersionID)
				assert.Equal(t, "AVAILABLE", out.Status)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestGetSchemaVersionsDiff
// ---------------------------------------------------------------------------

// TestGetSchemaVersionsDiff_Stateful verifies diff between schema versions.
func TestGetSchemaVersionsDiff_Stateful(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		v1       int64
		v2       int64
		wantDiff bool
	}{
		{
			name:     "same_version_returns_empty_diff",
			v1:       1,
			v2:       1,
			wantDiff: false,
		},
		{
			name:     "different_versions_return_diff",
			v1:       1,
			v2:       2,
			wantDiff: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupTestSchema(t, h)

			rec := doGlueRequest(t, h, "GetSchemaVersionsDiff", map[string]any{
				"SchemaId":                  map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
				"FirstSchemaVersionNumber":  map[string]any{"VersionNumber": tc.v1},
				"SecondSchemaVersionNumber": map[string]any{"VersionNumber": tc.v2},
				"SchemaDiffType":            "SYNTAX_DIFF",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				Diff string `json:"Diff"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tc.wantDiff {
				assert.NotEmpty(t, out.Diff)
			} else {
				assert.Empty(t, out.Diff)
			}
		})
	}
}

// TestGetSchemaVersionsDiff_UnknownSchema verifies 400 for missing schema.
func TestGetSchemaVersionsDiff_UnknownSchema(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "GetSchemaVersionsDiff", map[string]any{
		"SchemaId":                  map[string]any{"RegistryName": "no-reg", "SchemaName": "no-schema"},
		"FirstSchemaVersionNumber":  map[string]any{"VersionNumber": int64(1)},
		"SecondSchemaVersionNumber": map[string]any{"VersionNumber": int64(2)},
		"SchemaDiffType":            "SYNTAX_DIFF",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// TestGetPlan
// ---------------------------------------------------------------------------

// TestGetPlan_Languages verifies GetPlan returns code for both languages.
func TestGetPlan_Languages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		language   string
		wantPython bool
		wantScala  bool
	}{
		{
			name:       "python_language",
			language:   "Python",
			wantPython: true,
			wantScala:  false,
		},
		{
			name:       "scala_language",
			language:   "Scala",
			wantPython: false,
			wantScala:  true,
		},
		{
			name:       "default_to_python",
			language:   "",
			wantPython: true,
			wantScala:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doGlueRequest(t, h, "GetPlan", map[string]any{
				"Language": tc.language,
				"Mapping":  []any{},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out struct {
				PythonScript string `json:"PythonScript"`
				ScalaCode    string `json:"ScalaCode"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			if tc.wantPython {
				assert.NotEmpty(t, out.PythonScript)
			} else {
				assert.Empty(t, out.PythonScript)
			}

			if tc.wantScala {
				assert.NotEmpty(t, out.ScalaCode)
			} else {
				assert.Empty(t, out.ScalaCode)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestResumeWorkflowRun
// ---------------------------------------------------------------------------

// TestResumeWorkflowRun_Stateful verifies resume of a running workflow run.
func TestResumeWorkflowRun_Stateful(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *glue.Handler) (wfName, runID string)
		name     string
		wantCode int
	}{
		{
			name: "empty_inputs_returns_ok",
			setup: func(_ *testing.T, _ *glue.Handler) (string, string) {
				return "", ""
			},
			wantCode: http.StatusOK,
		},
		{
			name: "existing_run_returns_run_id",
			setup: func(t *testing.T, h *glue.Handler) (string, string) {
				t.Helper()

				createRec := doGlueRequest(t, h, "CreateWorkflow", map[string]any{
					"Name": "my-workflow",
				})
				require.Equal(t, http.StatusOK, createRec.Code)

				startRec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{
					"Name": "my-workflow",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				var startOut struct {
					RunID string `json:"RunId"`
				}
				require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))

				return "my-workflow", startOut.RunID
			},
			wantCode: http.StatusOK,
		},
		{
			name: "unknown_workflow_returns_400",
			setup: func(_ *testing.T, _ *glue.Handler) (string, string) {
				return "no-such-workflow", "no-such-run"
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			wfName, runID := tc.setup(t, h)

			rec := doGlueRequest(t, h, "ResumeWorkflowRun", map[string]any{
				"Name":    wfName,
				"RunId":   runID,
				"NodeIds": []string{},
			})
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.wantCode == http.StatusOK && wfName != "" {
				var out struct {
					RunID   string   `json:"RunId"`
					NodeIDs []string `json:"NodeIds"`
				}
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, runID, out.RunID)
			}
		})
	}
}

// TestIntegrationResourceProperty verifies CreateIntegrationResourceProperty stores and
// GetIntegrationResourceProperty retrieves properties by ARN.
func TestIntegrationResourceProperty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createInput map[string]any
		getInput    map[string]any
		name        string
		wantCreate  int
		wantGet     int
	}{
		{
			name:        "missing_arn_returns_400",
			createInput: map[string]any{},
			wantCreate:  http.StatusBadRequest,
		},
		{
			name:        "create_and_retrieve",
			createInput: map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:resource/r1", "SourceProperties": map[string]any{"key": "val"}},
			getInput:    map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:resource/r1"},
			wantCreate:  http.StatusOK,
			wantGet:     http.StatusOK,
		},
		{
			name:       "get_missing_returns_400",
			getInput:   map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:resource/no-such"},
			wantGet:    http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.createInput != nil {
				rec := doGlueRequest(t, h, "CreateIntegrationResourceProperty", tc.createInput)
				assert.Equal(t, tc.wantCreate, rec.Code, "create")
			}

			if tc.getInput != nil {
				rec := doGlueRequest(t, h, "GetIntegrationResourceProperty", tc.getInput)
				assert.Equal(t, tc.wantGet, rec.Code, "get")
			}
		})
	}
}

// TestIntegrationTableProperties verifies create/get table property round-trip.
func TestIntegrationTableProperties(t *testing.T) {
	t.Parallel()

	tests := []struct {
		createInput map[string]any
		getInput    map[string]any
		name        string
		wantCreate  int
		wantGet     int
	}{
		{
			name:        "missing_fields_returns_400",
			createInput: map[string]any{},
			wantCreate:  http.StatusBadRequest,
		},
		{
			name:        "create_and_retrieve",
			createInput: map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:table/t1", "TableName": "orders"},
			getInput:    map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:table/t1", "TableName": "orders"},
			wantCreate:  http.StatusOK,
			wantGet:     http.StatusOK,
		},
		{
			name:     "get_missing_returns_400",
			getInput: map[string]any{"ResourceArn": "arn:aws:glue:us-east-1:123:table/t1", "TableName": "no-such"},
			wantGet:  http.StatusBadRequest,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tc.createInput != nil {
				rec := doGlueRequest(t, h, "CreateIntegrationTableProperties", tc.createInput)
				assert.Equal(t, tc.wantCreate, rec.Code, "create")
			}

			if tc.getInput != nil {
				rec := doGlueRequest(t, h, "GetIntegrationTableProperties", tc.getInput)
				assert.Equal(t, tc.wantGet, rec.Code, "get")
			}
		})
	}
}

// TestDescribeConnectionType verifies required-field validation.
func TestDescribeConnectionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "missing_type_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
		{name: "known_type_returns_200", input: map[string]any{"ConnectionType": "JDBC"}, wantCode: http.StatusOK},
		{name: "custom_type_returns_200", input: map[string]any{"ConnectionType": "CUSTOM_CONN"}, wantCode: http.StatusOK},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "DescribeConnectionType", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestCancelStatement_Validation verifies SessionId is required.
func TestCancelStatement_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "missing_session_id_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "CancelStatement", tc.input)
			assert.Equal(t, tc.wantCode, rec.Code)
		})
	}
}

// TestGetDataQualityModelResult_Validation verifies ProfileId is required.
func TestGetDataQualityModelResult_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{name: "missing_profile_id_returns_400", input: map[string]any{}, wantCode: http.StatusBadRequest},
		{name: "with_profile_id_returns_200", input: map[string]any{"ProfileId": "profile-123"}, wantCode: http.StatusOK},
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
