package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

// ──────────────────────────────────────────────────────────────────────────────
// Trigger CRUD + type variants (on-demand, scheduled, conditional)
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Trigger_Create_OnDemand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "on-demand trigger",
			body:     map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"},
			wantCode: http.StatusOK,
		},
		{
			name: "scheduled trigger",
			body: map[string]any{
				"Name":     "trig-sched",
				"Type":     "SCHEDULED",
				"Schedule": "cron(0 12 * * ? *)",
			},
			wantCode: http.StatusOK,
		},
		{
			name: "conditional trigger",
			body: map[string]any{
				"Name": "trig-cond",
				"Type": "CONDITIONAL",
				"Predicate": map[string]any{
					"Logical": "ANY",
					"Conditions": []map[string]any{
						{"JobName": "my-job", "LogicalOperator": "EQUALS", "State": "SUCCEEDED"},
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "duplicate trigger",
			body:     map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"},
			wantCode: http.StatusBadRequest,
		},
	}

	h := newTestHandler(t)
	// Create the first one to enable duplicate test.
	require.Equal(t, http.StatusOK,
		doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"}).Code)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			if tt.name == "duplicate trigger" {
				// Pre-create to cause duplicate.
				doGlueRequest(t, h2, "CreateTrigger", map[string]any{"Name": "trig-ondemand", "Type": "ON_DEMAND"})
			}
			rec := doGlueRequest(t, h2, "CreateTrigger", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Trigger_GetTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		lookupKey string
		wantCode  int
		wantErr   bool
	}{
		{name: "found", lookupKey: "my-trigger", wantCode: http.StatusOK},
		{name: "not-found", lookupKey: "no-such-trigger", wantCode: http.StatusBadRequest, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "my-trigger", "Type": "ON_DEMAND"})

			rec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": tt.lookupKey})
			assert.Equal(t, tt.wantCode, rec.Code)
			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				trig, ok := out["Trigger"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-trigger", trig["Name"])
			}
		})
	}
}

func TestBatch3_Trigger_UpdateTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trigName string
		wantCode int
	}{
		{name: "success", trigName: "upd-trigger", wantCode: http.StatusOK},
		{name: "not-found", trigName: "no-trigger", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{
				"Name":     "upd-trigger",
				"Type":     "SCHEDULED",
				"Schedule": "cron(0 0 * * ? *)",
			})

			rec := doGlueRequest(t, h, "UpdateTrigger", map[string]any{
				"Name": tt.trigName,
				"TriggerUpdate": map[string]any{
					"Schedule": "cron(0 6 * * ? *)",
				},
			})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Trigger_DeleteTrigger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		trigName string
		wantCode int
	}{
		{name: "success", trigName: "del-trigger", wantCode: http.StatusOK},
		{name: "not-found", trigName: "no-trigger", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "del-trigger", "Type": "ON_DEMAND"})

			rec := doGlueRequest(t, h, "DeleteTrigger", map[string]any{"Name": tt.trigName})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Trigger_StartStopActivate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		op        string
		trigName  string
		wantState string
		wantCode  int
	}{
		{name: "start-found", op: "StartTrigger", trigName: "t1", wantCode: http.StatusOK, wantState: "ACTIVATED"},
		{name: "stop-found", op: "StopTrigger", trigName: "t1", wantCode: http.StatusOK, wantState: "DEACTIVATED"},
		{name: "start-missing", op: "StartTrigger", trigName: "no-trigger", wantCode: http.StatusBadRequest},
		{name: "stop-missing", op: "StopTrigger", trigName: "no-trigger", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "t1", "Type": "SCHEDULED"})

			rec := doGlueRequest(t, h, tt.op, map[string]any{"Name": tt.trigName})
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.wantCode == http.StatusOK && tt.wantState != "" {
				get := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": "t1"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(get.Body.Bytes(), &out))
				trig := out["Trigger"].(map[string]any)
				assert.Equal(t, tt.wantState, trig["State"])
			}
		})
	}
}

func TestBatch3_Trigger_BatchGetAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "tr-a", "Type": "ON_DEMAND"})
	doGlueRequest(t, h, "CreateTrigger", map[string]any{"Name": "tr-b", "Type": "SCHEDULED"})

	t.Run("batch-get", func(t *testing.T) {
		rec := doGlueRequest(t, h, "BatchGetTriggers", map[string]any{
			"TriggerNames": []string{"tr-a", "tr-b", "no-such"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		triggers := out["Triggers"].([]any)
		assert.Len(t, triggers, 2)
		missing := out["TriggersNotFound"].([]any)
		assert.Len(t, missing, 1)
	})

	t.Run("get-triggers", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetTriggers", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "tr-a")
		assert.Contains(t, rec.Body.String(), "tr-b")
	})

	t.Run("list-triggers", func(t *testing.T) {
		rec := doGlueRequest(t, h, "ListTriggers", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "tr-a")
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Workflow CRUD + StartWorkflowRun + GetWorkflowRun + GetWorkflowRunProperties
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Workflow_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name:        "create",
			op:          "CreateWorkflow",
			body:        map[string]any{"Name": "wf1", "Description": "test workflow"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name:     "duplicate",
			op:       "CreateWorkflow",
			body:     map[string]any{"Name": "wf1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetWorkflow",
			body:     map[string]any{"Name": "wf1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetWorkflow",
			body:     map[string]any{"Name": "wf-nope"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "update",
			op:   "UpdateWorkflow",
			body: map[string]any{
				"Name":                 "wf1",
				"Description":          "updated",
				"DefaultRunProperties": map[string]any{"env": "prod"},
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf1", "Description": "test workflow"})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.op == "GetWorkflow" && tt.wantCode == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				wf := out["Workflow"].(map[string]any)
				assert.Equal(t, "wf1", wf["Name"])
			}
		})
	}
}

func TestBatch3_Workflow_DeleteAndList(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wfName   string
		wantCode int
	}{
		{name: "delete-found", wfName: "wf-del", wantCode: http.StatusOK},
		{name: "delete-missing", wfName: "wf-nope", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-del"})

			rec := doGlueRequest(t, h, "DeleteWorkflow", map[string]any{"Name": tt.wfName})
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}

	t.Run("list-workflows", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-list-a"})
		doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-list-b"})

		rec := doGlueRequest(t, h, "ListWorkflows", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "wf-list-a")
		assert.Contains(t, rec.Body.String(), "wf-list-b")
	})

	t.Run("batch-get-workflows", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-bg-a"})
		doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-bg-b"})

		rec := doGlueRequest(t, h, "BatchGetWorkflows", map[string]any{
			"Names": []string{"wf-bg-a", "wf-bg-b", "wf-missing"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		wfs := out["Workflows"].([]any)
		assert.Len(t, wfs, 2)
		missing := out["MissingWorkflows"].([]any)
		assert.Len(t, missing, 1)
	})
}

func TestBatch3_Workflow_RunLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-run"})

	t.Run("start-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "wf-run"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["RunId"])
	})

	t.Run("start-run-missing-workflow", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "no-wf"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestBatch3_Workflow_GetRunAndProperties(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-props"})

	// Start a run.
	startRec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "wf-props"})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	t.Run("get-workflow-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetWorkflowRun", map[string]any{"Name": "wf-props", "RunId": runID})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		run := out["Run"].(map[string]any)
		assert.Equal(t, runID, run["WorkflowRunId"])
	})

	t.Run("get-workflow-runs", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetWorkflowRuns", map[string]any{"Name": "wf-props"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		runs := out["Runs"].([]any)
		assert.Len(t, runs, 1)
	})

	t.Run("put-workflow-run-properties", func(t *testing.T) {
		rec := doGlueRequest(t, h, "PutWorkflowRunProperties", map[string]any{
			"Name":          "wf-props",
			"RunId":         runID,
			"RunProperties": map[string]any{"key1": "val1", "key2": "val2"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("get-workflow-run-properties", func(t *testing.T) {
		doGlueRequest(t, h, "PutWorkflowRunProperties", map[string]any{
			"Name":          "wf-props",
			"RunId":         runID,
			"RunProperties": map[string]any{"env": "staging"},
		})

		rec := doGlueRequest(t, h, "GetWorkflowRunProperties", map[string]any{
			"Name":  "wf-props",
			"RunId": runID,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		props := out["RunProperties"].(map[string]any)
		assert.Equal(t, "staging", props["env"])
	})

	t.Run("stop-workflow-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StopWorkflowRun", map[string]any{
			"Name":  "wf-props",
			"RunId": runID,
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// MLTransform CRUD + task run stubs
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_MLTransform_CRUD(t *testing.T) {
	t.Parallel()

	createBody := map[string]any{
		"Name":        "my-transform",
		"Description": "test transform",
		"Role":        "arn:aws:iam::123456789012:role/GlueRole",
		"InputRecordTables": []map[string]any{
			{"DatabaseName": "db1", "TableName": "t1"},
		},
		"Parameters": map[string]any{
			"TransformType": "FIND_MATCHES",
			"FindMatchesParameters": map[string]any{
				"PrimaryKeyColumnName": "id",
			},
		},
	}

	tests := []struct {
		setup    func(h interface{ Name() string })
		body     func(transformID string) map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name:     "create",
			op:       "CreateMLTransform",
			body:     func(_ string) map[string]any { return createBody },
			wantCode: http.StatusOK,
		},
		{
			name:     "get",
			op:       "GetMLTransform",
			body:     func(id string) map[string]any { return map[string]any{"TransformId": id} },
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetMLTransform",
			body:     func(_ string) map[string]any { return map[string]any{"TransformId": "no-such-id"} },
			wantCode: http.StatusBadRequest,
		},
		{
			name: "update",
			op:   "UpdateMLTransform",
			body: func(id string) map[string]any {
				return map[string]any{"TransformId": id, "Description": "updated"}
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-all",
			op:       "GetMLTransforms",
			body:     func(_ string) map[string]any { return map[string]any{} },
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			var transformID string

			createRec := doGlueRequest(t, h, "CreateMLTransform", createBody)
			if createRec.Code == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
				transformID, _ = out["TransformId"].(string)
			}

			rec := doGlueRequest(t, h, tt.op, tt.body(transformID))
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_MLTransform_Delete(t *testing.T) {
	t.Parallel()

	t.Run("delete-found", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createRec := doGlueRequest(t, h, "CreateMLTransform", map[string]any{
			"Name": "ml-del", "Role": "arn:aws:iam::123:role/R",
		})
		require.Equal(t, http.StatusOK, createRec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &out))
		transformID := out["TransformId"].(string)

		rec := doGlueRequest(t, h, "DeleteMLTransform", map[string]any{"TransformId": transformID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("delete-missing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doGlueRequest(t, h, "DeleteMLTransform", map[string]any{"TransformId": "no-such"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestBatch3_MLTransform_TaskRunStubs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("get-ml-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetMLTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "SUCCEEDED")
	})

	t.Run("get-ml-task-runs", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetMLTaskRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TaskRuns")
	})

	t.Run("start-export-labels-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartExportLabelsTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "TaskRunId")
	})

	t.Run("start-import-labels-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartImportLabelsTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("start-ml-evaluation-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartMLEvaluationTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("start-ml-labeling-set-generation-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartMLLabelingSetGenerationTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("cancel-ml-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "CancelMLTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// DataQuality: Ruleset + EvaluationRun + RuleRecommendationRun
// ──────────────────────────────────────────────────────────────────────────────

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

func TestBatch3_DataQuality_EvaluationRun(t *testing.T) {
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

func TestBatch3_DataQuality_EvaluationRun_GetAndCancel(t *testing.T) {
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

	t.Run("get-eval-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		run, ok := out["DataQualityEvaluationRun"].(map[string]any)
		require.True(t, ok, "expected DataQualityEvaluationRun field in response")
		assert.Equal(t, "RUNNING", run["Status"])
	})

	t.Run("cancel-eval-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "CancelDataQualityRulesetEvaluationRun", map[string]any{"RunId": runID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

func TestBatch3_DataQuality_RuleRecommendationRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("start-recommendation-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{
			"DataSource": map[string]any{"GlueTable": map[string]any{"DatabaseName": "db", "TableName": "t"}},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "RunId")
	})

	startRec := doGlueRequest(t, h, "StartDataQualityRuleRecommendationRun", map[string]any{})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	t.Run("get-recommendation-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetDataQualityRuleRecommendationRun", map[string]any{"RunId": runID})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["RunId"])
	})

	t.Run("list-recommendation-runs", func(t *testing.T) {
		rec := doGlueRequest(t, h, "ListDataQualityRuleRecommendationRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Runs")
	})

	t.Run("cancel-recommendation-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "CancelDataQualityRuleRecommendationRun", map[string]any{"RunId": runID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Schema Registry: Registry + Schema + SchemaVersion + Compatibility
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_SchemaRegistry_Registry_CRUD(t *testing.T) {
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
			name:        "create-registry",
			op:          "CreateRegistry",
			body:        map[string]any{"RegistryName": "my-reg", "Description": "test registry"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "my-reg")
			},
		},
		{
			name:     "create-registry-duplicate",
			op:       "CreateRegistry",
			body:     map[string]any{"RegistryName": "my-reg"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-registry",
			op:       "GetRegistry",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "my-reg"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-registry-missing",
			op:       "GetRegistry",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "no-reg"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list-registries",
			op:       "ListRegistries",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "update-registry",
			op:   "UpdateRegistry",
			body: map[string]any{
				"RegistryId":  map[string]any{"RegistryName": "my-reg"},
				"Description": "updated description",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-registry",
			op:       "DeleteRegistry",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "my-reg"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "my-reg"})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestBatch3_SchemaRegistry_Schema_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name: "create-schema",
			op:   "CreateSchema",
			body: map[string]any{
				"RegistryId":    map[string]any{"RegistryName": "reg1"},
				"SchemaName":    "schema1",
				"DataFormat":    "AVRO",
				"Compatibility": "BACKWARD",
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name: "create-schema-duplicate",
			op:   "CreateSchema",
			body: map[string]any{
				"RegistryId": map[string]any{"RegistryName": "reg1"},
				"SchemaName": "schema1",
				"DataFormat": "AVRO",
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-schema",
			op:       "GetSchema",
			body:     map[string]any{"SchemaId": map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-schema-missing",
			op:       "GetSchema",
			body:     map[string]any{"SchemaId": map[string]any{"RegistryName": "reg1", "SchemaName": "no-schema"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list-schemas",
			op:       "ListSchemas",
			body:     map[string]any{"RegistryId": map[string]any{"RegistryName": "reg1"}},
			wantCode: http.StatusOK,
		},
		{
			name: "update-schema",
			op:   "UpdateSchema",
			body: map[string]any{
				"SchemaId":      map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"},
				"Compatibility": "FORWARD",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-schema",
			op:       "DeleteSchema",
			body:     map[string]any{"SchemaId": map[string]any{"RegistryName": "reg1", "SchemaName": "schema1"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "reg1"})
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateSchema", map[string]any{
					"RegistryId": map[string]any{"RegistryName": "reg1"},
					"SchemaName": "schema1",
					"DataFormat": "AVRO",
				})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_SchemaRegistry_SchemaVersion_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateRegistry", map[string]any{"RegistryName": "svr"})
	doGlueRequest(t, h, "CreateSchema", map[string]any{
		"RegistryId": map[string]any{"RegistryName": "svr"},
		"SchemaName": "sv-schema",
		"DataFormat": "AVRO",
	})

	avroDefinition := `{"type":"record","name":"Test","fields":[{"name":"id","type":"int"}]}`

	t.Run("register-schema-version", func(t *testing.T) {
		rec := doGlueRequest(t, h, "RegisterSchemaVersion", map[string]any{
			"SchemaId":         map[string]any{"RegistryName": "svr", "SchemaName": "sv-schema"},
			"SchemaDefinition": avroDefinition,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["SchemaVersionId"])
	})

	t.Run("get-schema-version", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetSchemaVersion", map[string]any{
			"SchemaId":            map[string]any{"RegistryName": "svr", "SchemaName": "sv-schema"},
			"SchemaVersionNumber": map[string]any{"VersionNumber": 1},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.EqualValues(t, 1, out["VersionNumber"])
	})

	t.Run("list-schema-versions", func(t *testing.T) {
		rec := doGlueRequest(t, h, "ListSchemaVersions", map[string]any{
			"SchemaId": map[string]any{"RegistryName": "svr", "SchemaName": "sv-schema"},
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "SchemaVersions")
	})

	t.Run("check-schema-version-validity", func(t *testing.T) {
		rec := doGlueRequest(t, h, "CheckSchemaVersionValidity", map[string]any{
			"DataFormat":       "AVRO",
			"SchemaDefinition": avroDefinition,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.True(t, out["Valid"].(bool))
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// CustomEntityType CRUD (with RegexString and ContextWords)
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_CustomEntityType_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, body string)
		name     string
		op       string
		wantCode int
	}{
		{
			name: "create-with-regex",
			op:   "CreateCustomEntityType",
			body: map[string]any{
				"Name":         "SSN",
				"RegexString":  `\d{3}-\d{2}-\d{4}`,
				"ContextWords": []string{"social", "security"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get",
			op:   "GetCustomEntityType",
			body: map[string]any{"Name": "SSN"},
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "SSN")
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetCustomEntityType",
			body:     map[string]any{"Name": "NO-CET"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "list",
			op:       "ListCustomEntityTypes",
			body:     map[string]any{},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "SSN")
			},
		},
		{
			name:     "batch-get",
			op:       "BatchGetCustomEntityTypes",
			body:     map[string]any{"TypeNames": []string{"SSN", "NO-CET"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteCustomEntityType",
			body:     map[string]any{"Name": "SSN"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
				"Name":         "SSN",
				"RegexString":  `\d{3}-\d{2}-\d{4}`,
				"ContextWords": []string{"social", "security"},
			})

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestBatch3_CustomEntityType_RegexAndContextWords_Persist(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateCustomEntityType", map[string]any{
		"Name":         "CreditCard",
		"RegexString":  `\d{4}-\d{4}-\d{4}-\d{4}`,
		"ContextWords": []string{"credit", "card", "payment"},
	})

	rec := doGlueRequest(t, h, "GetCustomEntityType", map[string]any{"Name": "CreditCard"})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, `\d{4}-\d{4}-\d{4}-\d{4}`, out["RegexString"])
	ctxWords, ok := out["ContextWords"].([]any)
	require.True(t, ok)
	assert.Len(t, ctxWords, 3)
}

// ──────────────────────────────────────────────────────────────────────────────
// Blueprint CRUD + LastModified + BlueprintRuns
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Blueprint_CRUD_Full(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name:        "create",
			op:          "CreateBlueprint",
			body:        map[string]any{"Name": "bp1"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name:     "get",
			op:       "GetBlueprint",
			body:     map[string]any{"Name": "bp1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "list",
			op:       "ListBlueprints",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "update",
			op:       "UpdateBlueprint",
			body:     map[string]any{"Name": "bp1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "batch-get",
			op:       "BatchGetBlueprints",
			body:     map[string]any{"Names": []string{"bp1", "no-bp"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteBlueprint",
			body:     map[string]any{"Name": "bp1"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": "bp1"})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Blueprint_Run_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateBlueprint", map[string]any{"Name": "bp-run"})

	t.Run("start-blueprint-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{"BlueprintName": "bp-run"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["RunId"])
	})

	// Start a run and retrieve it.
	startRec := doGlueRequest(t, h, "StartBlueprintRun", map[string]any{"BlueprintName": "bp-run"})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	t.Run("get-blueprint-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetBlueprintRun", map[string]any{
			"BlueprintName": "bp-run",
			"RunId":         runID,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), runID)
	})

	t.Run("get-blueprint-runs", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetBlueprintRuns", map[string]any{"BlueprintName": "bp-run"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		runs := out["Runs"].([]any)
		assert.GreaterOrEqual(t, len(runs), 1)
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// ColumnStatistics for table and partition
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_ColumnStatistics_Table(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "csdb"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "csdb",
		"TableInput":   map[string]any{"Name": "cst"},
	})

	colStats := []map[string]any{
		{
			"ColumnName": "id",
			"ColumnType": "int",
			"StatisticsData": map[string]any{
				"Type": "LONG",
				"LongColumnStatisticsData": map[string]any{
					"NumberOfNulls":          0,
					"NumberOfDistinctValues": 100,
				},
			},
		},
		{
			"ColumnName": "name",
			"ColumnType": "string",
			"StatisticsData": map[string]any{
				"Type": "STRING",
				"StringColumnStatisticsData": map[string]any{
					"MaximumLength":          50,
					"NumberOfNulls":          5,
					"NumberOfDistinctValues": 90,
				},
			},
		},
	}

	tests := []struct {
		body     map[string]any
		check    func(t *testing.T, body string)
		name     string
		op       string
		wantCode int
	}{
		{
			name: "update-col-stats-for-table",
			op:   "UpdateColumnStatisticsForTable",
			body: map[string]any{
				"DatabaseName":         "csdb",
				"TableName":            "cst",
				"ColumnStatisticsList": colStats,
			},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "Errors")
			},
		},
		{
			name:     "get-col-stats-for-table",
			op:       "GetColumnStatisticsForTable",
			body:     map[string]any{"DatabaseName": "csdb", "TableName": "cst"},
			wantCode: http.StatusOK,
		},
		{
			name: "get-col-stats-for-table-specific-columns",
			op:   "GetColumnStatisticsForTable",
			body: map[string]any{
				"DatabaseName": "csdb",
				"TableName":    "cst",
				"ColumnNames":  []string{"id"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete-col-stats-for-table",
			op:   "DeleteColumnStatisticsForTable",
			body: map[string]any{
				"DatabaseName": "csdb",
				"TableName":    "cst",
				"ColumnName":   "id",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Set up state before each test.
			h2 := newTestHandler(t)
			doGlueRequest(t, h2, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "csdb"}})
			doGlueRequest(t, h2, "CreateTable", map[string]any{
				"DatabaseName": "csdb",
				"TableInput":   map[string]any{"Name": "cst"},
			})
			doGlueRequest(t, h2, "UpdateColumnStatisticsForTable", map[string]any{
				"DatabaseName":         "csdb",
				"TableName":            "cst",
				"ColumnStatisticsList": colStats,
			})

			rec := doGlueRequest(t, h2, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

func TestBatch3_ColumnStatistics_Partition(t *testing.T) {
	t.Parallel()

	partitionValues := []string{"2024-01-01"}
	colStats := []map[string]any{
		{
			"ColumnName": "val",
			"ColumnType": "double",
			"StatisticsData": map[string]any{
				"Type": "DOUBLE",
				"DoubleColumnStatisticsData": map[string]any{
					"NumberOfNulls":          0,
					"NumberOfDistinctValues": 50,
				},
			},
		},
	}

	setupPartitionHandler := func(t *testing.T) *glue.Handler {
		t.Helper()
		h := newTestHandler(t)
		doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "pdb"}})
		doGlueRequest(t, h, "CreateTable", map[string]any{
			"DatabaseName": "pdb",
			"TableInput": map[string]any{
				"Name":          "pt",
				"PartitionKeys": []map[string]any{{"Name": "dt", "Type": "string"}},
			},
		})
		doGlueRequest(t, h, "CreatePartition", map[string]any{
			"DatabaseName": "pdb",
			"TableName":    "pt",
			"PartitionInput": map[string]any{
				"Values": partitionValues,
			},
		})

		return h
	}

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "update-col-stats-for-partition",
			op:   "UpdateColumnStatisticsForPartition",
			body: map[string]any{
				"DatabaseName":         "pdb",
				"TableName":            "pt",
				"PartitionValues":      partitionValues,
				"ColumnStatisticsList": colStats,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "get-col-stats-for-partition",
			op:   "GetColumnStatisticsForPartition",
			body: map[string]any{
				"DatabaseName":    "pdb",
				"TableName":       "pt",
				"PartitionValues": partitionValues,
			},
			wantCode: http.StatusOK,
		},
		{
			name: "delete-col-stats-for-partition",
			op:   "DeleteColumnStatisticsForPartition",
			body: map[string]any{
				"DatabaseName":    "pdb",
				"TableName":       "pt",
				"PartitionValues": partitionValues,
				"ColumnName":      "val",
			},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ph := setupPartitionHandler(t)
			doGlueRequest(t, ph, "UpdateColumnStatisticsForPartition", map[string]any{
				"DatabaseName":         "pdb",
				"TableName":            "pt",
				"PartitionValues":      partitionValues,
				"ColumnStatisticsList": colStats,
			})

			rec := doGlueRequest(t, ph, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_ColumnStatisticsTaskSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDatabase", map[string]any{"DatabaseInput": map[string]any{"Name": "cstdb"}})
	doGlueRequest(t, h, "CreateTable", map[string]any{
		"DatabaseName": "cstdb",
		"TableInput":   map[string]any{"Name": "csttbl"},
	})

	t.Run("create-col-stats-task-settings", func(t *testing.T) {
		rec := doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
			"DatabaseName":   "cstdb",
			"TableName":      "csttbl",
			"RoleArn":        "arn:aws:iam::123456789012:role/GlueRole",
			"ColumnNameList": []string{"col1", "col2"},
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("get-col-stats-task-settings", func(t *testing.T) {
		doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
			"RoleArn":      "arn:aws:iam::123456789012:role/GlueRole",
		})
		rec := doGlueRequest(t, h, "GetColumnStatisticsTaskSettings", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "ColumnStatisticsTaskSettings")
	})

	t.Run("start-col-stats-task-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["ColumnStatisticsTaskRunId"])
	})

	t.Run("list-col-stats-task-runs", func(t *testing.T) {
		doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
		})
		rec := doGlueRequest(t, h, "ListColumnStatisticsTaskRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "ColumnStatisticsTaskRunIds")
	})

	t.Run("get-and-stop-col-stats-task-run", func(t *testing.T) {
		startRec := doGlueRequest(t, h, "StartColumnStatisticsTaskRun", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
		})
		require.Equal(t, http.StatusOK, startRec.Code)
		var startOut map[string]any
		require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
		runID := startOut["ColumnStatisticsTaskRunId"].(string)

		getRec := doGlueRequest(t, h, "GetColumnStatisticsTaskRun", map[string]any{
			"ColumnStatisticsTaskRunId": runID,
		})
		require.Equal(t, http.StatusOK, getRec.Code)

		stopRec := doGlueRequest(t, h, "StopColumnStatisticsTaskRun", map[string]any{
			"ColumnStatisticsTaskRunId": runID,
		})
		assert.Equal(t, http.StatusOK, stopRec.Code)
	})

	t.Run("delete-col-stats-task-settings", func(t *testing.T) {
		doGlueRequest(t, h, "CreateColumnStatisticsTaskSettings", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
		})
		rec := doGlueRequest(t, h, "DeleteColumnStatisticsTaskSettings", map[string]any{
			"DatabaseName": "cstdb",
			"TableName":    "csttbl",
		})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Crawler CRUD lifecycle: READY → RUNNING → STOPPING
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Crawler_CRUD(t *testing.T) {
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
			name: "create",
			op:   "CreateCrawler",
			body: map[string]any{
				"Name":    "cr1",
				"Role":    "arn:aws:iam::123456789012:role/GlueRole",
				"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/prefix"}}},
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name:     "create-duplicate",
			op:       "CreateCrawler",
			body:     map[string]any{"Name": "cr1", "Role": "arn:aws:iam::123:role/R"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetCrawler",
			body:     map[string]any{"Name": "cr1"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "READY")
			},
		},
		{
			name:     "get-missing",
			op:       "GetCrawler",
			body:     map[string]any{"Name": "no-cr"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-crawlers",
			op:       "GetCrawlers",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "list-crawlers",
			op:       "ListCrawlers",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "update",
			op:   "UpdateCrawler",
			body: map[string]any{
				"Name":    "cr1",
				"Role":    "arn:aws:iam::123456789012:role/UpdatedRole",
				"Targets": map[string]any{},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "update-missing",
			op:       "UpdateCrawler",
			body:     map[string]any{"Name": "no-cr", "Role": "r"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "batch-get",
			op:       "BatchGetCrawlers",
			body:     map[string]any{"CrawlerNames": []string{"cr1", "no-cr"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteCrawler",
			body:     map[string]any{"Name": "cr1"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateCrawler", map[string]any{
					"Name":    "cr1",
					"Role":    "arn:aws:iam::123456789012:role/GlueRole",
					"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/prefix"}}},
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

func TestBatch3_Crawler_Lifecycle_States(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		wantState string
		ops       []string
		wantCode  int
	}{
		{
			name:      "start-crawler",
			ops:       []string{"StartCrawler"},
			wantState: "RUNNING",
			wantCode:  http.StatusOK,
		},
		{
			name:     "start-already-running",
			ops:      []string{"StartCrawler", "StartCrawler"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:      "stop-running-crawler",
			ops:       []string{"StartCrawler", "StopCrawler"},
			wantState: "STOPPING",
			wantCode:  http.StatusOK,
		},
		{
			name:     "stop-not-running",
			ops:      []string{"StopCrawler"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateCrawler", map[string]any{
				"Name": "cr-lifecycle",
				"Role": "arn:aws:iam::123:role/R",
			})

			var lastCode int
			for _, op := range tt.ops {
				r := doGlueRequest(t, h, op, map[string]any{"Name": "cr-lifecycle"})
				lastCode = r.Code
			}

			assert.Equal(t, tt.wantCode, lastCode)
			if tt.wantState != "" && tt.wantCode == http.StatusOK {
				getRec := doGlueRequest(t, h, "GetCrawler", map[string]any{"Name": "cr-lifecycle"})
				var out map[string]any
				require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
				cr := out["Crawler"].(map[string]any)
				assert.Equal(t, tt.wantState, cr["State"])
			}
		})
	}
}

func TestBatch3_Crawler_Schedule(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "cr-sched",
		"Role": "arn:aws:iam::123:role/R",
	})

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "update-schedule",
			op:   "UpdateCrawlerSchedule",
			body: map[string]any{"CrawlerName": "cr-sched", "Schedule": "cron(0 0 * * ? *)"},
		},
		{
			name: "start-schedule",
			op:   "StartCrawlerSchedule",
			body: map[string]any{"CrawlerName": "cr-sched"},
		},
		{
			name: "stop-schedule",
			op:   "StopCrawlerSchedule",
			body: map[string]any{"CrawlerName": "cr-sched"},
		},
		{
			name:     "get-crawler-metrics",
			op:       "GetCrawlerMetrics",
			body:     map[string]any{"CrawlerNameList": []string{"cr-sched"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		wantCode := tt.wantCode
		if wantCode == 0 {
			wantCode = http.StatusOK
		}
		t.Run(tt.name, func(t *testing.T) {
			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, wantCode, rec.Code)
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Classifier CRUD: Grok, CSV, JSON, XML
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Classifier_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		createBody map[string]any
		updateBody map[string]any
		getName    string
		wantCode   int
	}{
		{
			name: "grok-classifier",
			createBody: map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "grok-cl",
					"Classification": "custom",
					"GrokPattern":    "%{GREEDYDATA:message}",
				},
			},
			updateBody: map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "grok-cl",
					"Classification": "custom-updated",
					"GrokPattern":    "%{GREEDYDATA:message}",
				},
			},
			getName:  "grok-cl",
			wantCode: http.StatusOK,
		},
		{
			name: "csv-classifier",
			createBody: map[string]any{
				"CsvClassifier": map[string]any{
					"Name":        "csv-cl",
					"Delimiter":   ",",
					"QuoteSymbol": `"`,
					"Header":      []string{"col1", "col2", "col3"},
				},
			},
			updateBody: map[string]any{
				"CsvClassifier": map[string]any{
					"Name":        "csv-cl",
					"Delimiter":   ";",
					"QuoteSymbol": `"`,
				},
			},
			getName:  "csv-cl",
			wantCode: http.StatusOK,
		},
		{
			name: "json-classifier",
			createBody: map[string]any{
				"JSONClassifier": map[string]any{
					"Name":     "json-cl",
					"JsonPath": "$.records[*]",
				},
			},
			updateBody: map[string]any{
				"JSONClassifier": map[string]any{
					"Name":     "json-cl",
					"JsonPath": "$.data[*]",
				},
			},
			getName:  "json-cl",
			wantCode: http.StatusOK,
		},
		{
			name: "xml-classifier",
			createBody: map[string]any{
				"XMLClassifier": map[string]any{
					"Name":           "xml-cl",
					"Classification": "xml",
					"RowTag":         "record",
				},
			},
			updateBody: map[string]any{
				"XMLClassifier": map[string]any{
					"Name":           "xml-cl",
					"Classification": "xml",
					"RowTag":         "item",
				},
			},
			getName:  "xml-cl",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			t.Run("create", func(t *testing.T) {
				rec := doGlueRequest(t, h, "CreateClassifier", tt.createBody)
				assert.Equal(t, tt.wantCode, rec.Code)
			})

			t.Run("get", func(t *testing.T) {
				doGlueRequest(t, h, "CreateClassifier", tt.createBody)
				rec := doGlueRequest(t, h, "GetClassifier", map[string]any{"Name": tt.getName})
				assert.Equal(t, tt.wantCode, rec.Code)
			})

			t.Run("update", func(t *testing.T) {
				doGlueRequest(t, h, "CreateClassifier", tt.createBody)
				rec := doGlueRequest(t, h, "UpdateClassifier", tt.updateBody)
				assert.Equal(t, tt.wantCode, rec.Code)
			})

			t.Run("delete", func(t *testing.T) {
				doGlueRequest(t, h, "CreateClassifier", tt.createBody)
				rec := doGlueRequest(t, h, "DeleteClassifier", map[string]any{"Name": tt.getName})
				assert.Equal(t, tt.wantCode, rec.Code)
			})
		})
	}
}

func TestBatch3_Classifier_GetClassifiers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateClassifier", map[string]any{
		"GrokClassifier": map[string]any{"Name": "g1", "Classification": "custom", "GrokPattern": "%{GREEDYDATA}"},
	})
	doGlueRequest(t, h, "CreateClassifier", map[string]any{
		"CsvClassifier": map[string]any{"Name": "c1", "Delimiter": ","},
	})

	rec := doGlueRequest(t, h, "GetClassifiers", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	classifiers := out["Classifiers"].([]any)
	assert.Len(t, classifiers, 2)
}

func TestBatch3_Classifier_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		op       string
		wantCode int
	}{
		{
			name: "create-duplicate",
			op:   "CreateClassifier",
			body: map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "dup-cl",
					"Classification": "x",
					"GrokPattern":    "%{GREEDYDATA}",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-missing",
			op:       "GetClassifier",
			body:     map[string]any{"Name": "no-cl"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "update-missing",
			op:       "UpdateClassifier",
			body:     map[string]any{"GrokClassifier": map[string]any{"Name": "no-cl", "GrokPattern": "%{GREEDYDATA}"}},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "delete-missing",
			op:       "DeleteClassifier",
			body:     map[string]any{"Name": "no-cl"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doGlueRequest(t, h, "CreateClassifier", map[string]any{
				"GrokClassifier": map[string]any{
					"Name":           "dup-cl",
					"Classification": "x",
					"GrokPattern":    "%{GREEDYDATA}",
				},
			})

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// DevEndpoint CRUD
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_DevEndpoint_CRUD(t *testing.T) {
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
			op:          "CreateDevEndpoint",
			body:        map[string]any{"EndpointName": "dep1"},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "dep1")
				assert.Contains(t, body, "Status")
			},
		},
		{
			name:     "create-duplicate",
			op:       "CreateDevEndpoint",
			body:     map[string]any{"EndpointName": "dep1"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetDevEndpoint",
			body:     map[string]any{"EndpointName": "dep1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "get-missing",
			op:       "GetDevEndpoint",
			body:     map[string]any{"EndpointName": "no-dep"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-dev-endpoints",
			op:       "GetDevEndpoints",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name:     "list-dev-endpoints",
			op:       "ListDevEndpoints",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "batch-get-dev-endpoints",
			op:   "BatchGetDevEndpoints",
			body: map[string]any{
				"DevEndpointNames": []string{"dep1", "no-dep"},
			},
			wantCode: http.StatusOK,
		},
		{
			name: "update",
			op:   "UpdateDevEndpoint",
			body: map[string]any{
				"EndpointName": "dep1",
				"AddArguments": map[string]any{"--enable-glue-datacatalog": ""},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteDevEndpoint",
			body:     map[string]any{"EndpointName": "dep1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-missing",
			op:       "DeleteDevEndpoint",
			body:     map[string]any{"EndpointName": "no-dep"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{"EndpointName": "dep1"})
			}

			rec := doGlueRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
			if tt.check != nil {
				tt.check(t, rec.Body.String())
			}
		})
	}
}

// ──────────────────────────────────────────────────────────────────────────────
// Connection CRUD with credentials
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Connection_CRUD_WithCredentials(t *testing.T) {
	t.Parallel()

	jdbcProps := map[string]any{
		"JDBC_CONNECTION_URL": "jdbc:mysql://host:3306/db",
		"USERNAME":            "admin",
		"PASSWORD":            "secret",
	}

	tests := []struct {
		body        map[string]any
		check       func(t *testing.T, body string)
		name        string
		op          string
		wantCode    int
		skipPreSeed bool
	}{
		{
			name: "create-jdbc",
			op:   "CreateConnection",
			body: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":                 "jdbc-conn",
					"ConnectionType":       "JDBC",
					"ConnectionProperties": jdbcProps,
				},
			},
			wantCode:    http.StatusOK,
			skipPreSeed: true,
		},
		{
			name: "create-duplicate",
			op:   "CreateConnection",
			body: map[string]any{
				"ConnectionInput": map[string]any{
					"Name":           "jdbc-conn",
					"ConnectionType": "JDBC",
				},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get",
			op:       "GetConnection",
			body:     map[string]any{"Name": "jdbc-conn"},
			wantCode: http.StatusOK,
			check: func(t *testing.T, body string) {
				t.Helper()
				assert.Contains(t, body, "jdbc-conn")
				assert.Contains(t, body, "JDBC")
			},
		},
		{
			name:     "get-missing",
			op:       "GetConnection",
			body:     map[string]any{"Name": "no-conn"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "get-connections",
			op:       "GetConnections",
			body:     map[string]any{},
			wantCode: http.StatusOK,
		},
		{
			name: "update",
			op:   "UpdateConnection",
			body: map[string]any{
				"Name": "jdbc-conn",
				"ConnectionInput": map[string]any{
					"Name":           "jdbc-conn",
					"ConnectionType": "JDBC",
					"ConnectionProperties": map[string]any{
						"JDBC_CONNECTION_URL": "jdbc:mysql://newhost:3306/db",
						"USERNAME":            "admin2",
						"PASSWORD":            "newsecret",
					},
				},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete",
			op:       "DeleteConnection",
			body:     map[string]any{"ConnectionName": "jdbc-conn"},
			wantCode: http.StatusOK,
		},
		{
			name:     "delete-missing",
			op:       "DeleteConnection",
			body:     map[string]any{"ConnectionName": "no-conn"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if !tt.skipPreSeed {
				doGlueRequest(t, h, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{
						"Name":                 "jdbc-conn",
						"ConnectionType":       "JDBC",
						"ConnectionProperties": jdbcProps,
					},
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

func TestBatch3_Connection_BatchDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, name := range []string{"conn-a", "conn-b"} {
		doGlueRequest(t, h, "CreateConnection", map[string]any{
			"ConnectionInput": map[string]any{"Name": name, "ConnectionType": "JDBC"},
		})
	}

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "batch-delete-existing",
			body:     map[string]any{"ConnectionNameList": []string{"conn-a", "conn-b"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "batch-delete-missing",
			body:     map[string]any{"ConnectionNameList": []string{"no-conn"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "batch-delete-mixed",
			body:     map[string]any{"ConnectionNameList": []string{"conn-a", "no-conn"}},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h2 := newTestHandler(t)
			for _, name := range []string{"conn-a", "conn-b"} {
				doGlueRequest(t, h2, "CreateConnection", map[string]any{
					"ConnectionInput": map[string]any{"Name": name, "ConnectionType": "JDBC"},
				})
			}

			rec := doGlueRequest(t, h2, "BatchDeleteConnection", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestBatch3_Connection_NetworkType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "network-conn",
			"ConnectionType": "NETWORK",
			"ConnectionProperties": map[string]any{
				"HOST":      "10.0.1.5",
				"PORT":      "5432",
				"USER_NAME": "postgres",
				"PASSWORD":  "pgpass",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "network-conn"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	conn := out["Connection"].(map[string]any)
	assert.Equal(t, "NETWORK", conn["ConnectionType"])
	props := conn["ConnectionProperties"].(map[string]any)
	assert.Equal(t, "10.0.1.5", props["HOST"])
}

// ──────────────────────────────────────────────────────────────────────────────
// MaterializedView refresh task run
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_MaterializedViewRefreshTaskRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	t.Run("start-refresh-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["RunId"])
	})

	startRec := doGlueRequest(t, h, "StartMaterializedViewRefreshTaskRun", map[string]any{})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	t.Run("get-refresh-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "GetMaterializedViewRefreshTaskRun", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
	})

	t.Run("list-refresh-runs", func(t *testing.T) {
		rec := doGlueRequest(t, h, "ListMaterializedViewRefreshTaskRuns", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Contains(t, rec.Body.String(), "Runs")
	})

	t.Run("stop-refresh-run", func(t *testing.T) {
		rec := doGlueRequest(t, h, "StopMaterializedViewRefreshTaskRun", map[string]any{"RunId": runID})
		assert.Equal(t, http.StatusOK, rec.Code)
	})
}

// ──────────────────────────────────────────────────────────────────────────────
// Additional ops: GetDataQualityRulesets parity, DevEndpoint update, Connection
// credentials persist after update
// ──────────────────────────────────────────────────────────────────────────────

func TestBatch3_Connection_CredentialsPersistAfterUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateConnection", map[string]any{
		"ConnectionInput": map[string]any{
			"Name":           "cred-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]any{
				"USERNAME": "user1",
				"PASSWORD": "pass1",
			},
		},
	})

	doGlueRequest(t, h, "UpdateConnection", map[string]any{
		"Name": "cred-conn",
		"ConnectionInput": map[string]any{
			"Name":           "cred-conn",
			"ConnectionType": "JDBC",
			"ConnectionProperties": map[string]any{
				"USERNAME": "user2",
				"PASSWORD": "pass2",
			},
		},
	})

	getRec := doGlueRequest(t, h, "GetConnection", map[string]any{"Name": "cred-conn"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	conn := out["Connection"].(map[string]any)
	props := conn["ConnectionProperties"].(map[string]any)
	assert.Equal(t, "user2", props["USERNAME"])
	assert.Equal(t, "pass2", props["PASSWORD"])
}

func TestBatch3_DevEndpoint_UpdateAndGet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateDevEndpoint", map[string]any{"EndpointName": "dep-upd"})

	doGlueRequest(t, h, "UpdateDevEndpoint", map[string]any{
		"EndpointName": "dep-upd",
		"AddArguments": map[string]any{"--conf": "spark.executor.memory=4g"},
	})

	getRec := doGlueRequest(t, h, "GetDevEndpoint", map[string]any{"EndpointName": "dep-upd"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	dep := out["DevEndpoint"].(map[string]any)
	args, ok := dep["Arguments"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "spark.executor.memory=4g", args["--conf"])
}

func TestBatch3_Trigger_WithActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":     "trig-actions",
		"Type":     "SCHEDULED",
		"Schedule": "cron(0 0 * * ? *)",
		"Actions": []map[string]any{
			{
				"JobName":   "job-a",
				"Arguments": map[string]any{"--input": "s3://bucket/in"},
			},
			{
				"JobName": "job-b",
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	getRec := doGlueRequest(t, h, "GetTrigger", map[string]any{"Name": "trig-actions"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	trig := out["Trigger"].(map[string]any)
	actions := trig["Actions"].([]any)
	assert.Len(t, actions, 2)
}

func TestBatch3_Workflow_UpdateDefaultRunProperties(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateWorkflow", map[string]any{
		"Name":                 "wf-drp",
		"DefaultRunProperties": map[string]any{"env": "dev"},
	})

	// UpdateWorkflow reads DefaultRunProperties at the top level (not nested).
	doGlueRequest(t, h, "UpdateWorkflow", map[string]any{
		"Name":                 "wf-drp",
		"DefaultRunProperties": map[string]any{"env": "prod", "region": "us-east-1"},
	})

	getRec := doGlueRequest(t, h, "GetWorkflow", map[string]any{"Name": "wf-drp"})
	require.Equal(t, http.StatusOK, getRec.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
	wf, ok := out["Workflow"].(map[string]any)
	require.True(t, ok)
	props, ok := wf["DefaultRunProperties"].(map[string]any)
	require.True(t, ok, "expected DefaultRunProperties in workflow response")
	assert.Equal(t, "prod", props["env"])
	assert.Equal(t, "us-east-1", props["region"])
}
