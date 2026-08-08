package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestWorkflow_CRUD(t *testing.T) {
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

func TestWorkflow_DeleteAndList(t *testing.T) {
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

func TestWorkflow_RunLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-run"})

	t.Run("start-run", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "wf-run"})
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		assert.NotEmpty(t, out["RunId"])
	})

	t.Run("start-run-missing-workflow", func(t *testing.T) {
		t.Parallel()
		rec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "no-wf"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestWorkflow_GetRunAndProperties(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "wf-props"})

	// Start a run.
	startRec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "wf-props"})
	require.Equal(t, http.StatusOK, startRec.Code)
	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	getRunRec := doGlueRequest(t, h, "GetWorkflowRun", map[string]any{"Name": "wf-props", "RunId": runID})
	require.Equal(t, http.StatusOK, getRunRec.Code)
	var getRunOut map[string]any
	require.NoError(t, json.Unmarshal(getRunRec.Body.Bytes(), &getRunOut))
	run := getRunOut["Run"].(map[string]any)
	assert.Equal(t, runID, run["WorkflowRunId"])

	getRunsRec := doGlueRequest(t, h, "GetWorkflowRuns", map[string]any{"Name": "wf-props"})
	require.Equal(t, http.StatusOK, getRunsRec.Code)
	var getRunsOut map[string]any
	require.NoError(t, json.Unmarshal(getRunsRec.Body.Bytes(), &getRunsOut))
	runs := getRunsOut["Runs"].([]any)
	assert.Len(t, runs, 1)

	putPropsRec := doGlueRequest(t, h, "PutWorkflowRunProperties", map[string]any{
		"Name":          "wf-props",
		"RunId":         runID,
		"RunProperties": map[string]any{"key1": "val1", "key2": "val2"},
	})
	assert.Equal(t, http.StatusOK, putPropsRec.Code)

	doGlueRequest(t, h, "PutWorkflowRunProperties", map[string]any{
		"Name":          "wf-props",
		"RunId":         runID,
		"RunProperties": map[string]any{"env": "staging"},
	})
	getPropsRec := doGlueRequest(t, h, "GetWorkflowRunProperties", map[string]any{
		"Name":  "wf-props",
		"RunId": runID,
	})
	require.Equal(t, http.StatusOK, getPropsRec.Code)
	var getPropsOut map[string]any
	require.NoError(t, json.Unmarshal(getPropsRec.Body.Bytes(), &getPropsOut))
	props := getPropsOut["RunProperties"].(map[string]any)
	assert.Equal(t, "staging", props["env"])

	stopRunRec := doGlueRequest(t, h, "StopWorkflowRun", map[string]any{
		"Name":  "wf-props",
		"RunId": runID,
	})
	assert.Equal(t, http.StatusOK, stopRunRec.Code)
}

func TestWorkflow_UpdateDefaultRunProperties(t *testing.T) {
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

// TestWorkflowRunExtras tests StopWorkflowRun and PutWorkflowRunProperties.
func TestWorkflowRunExtras(t *testing.T) {
	t.Parallel()
	h := newGlueHandler(t)

	// Setup: create workflow and start a run.
	dispatchNewOp(t, h, "CreateWorkflow", map[string]any{
		"Name":        "my-workflow",
		"Description": "test",
	})
	out := dispatchNewOp(t, h, "StartWorkflowRun", map[string]any{"Name": "my-workflow"})
	runID, _ := out["RunId"].(string)
	if runID == "" {
		t.Fatalf("expected RunId")
	}

	// PutWorkflowRunProperties
	dispatchNewOp(t, h, "PutWorkflowRunProperties", map[string]any{
		"Name":          "my-workflow",
		"RunId":         runID,
		"RunProperties": map[string]any{"key1": "value1"},
	})

	// StopWorkflowRun
	dispatchNewOp(t, h, "StopWorkflowRun", map[string]any{
		"Name":  "my-workflow",
		"RunId": runID,
	})
}

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

func TestGlue_Workflows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateWorkflow
	rec := doGlueRequest(t, h, "CreateWorkflow", map[string]any{
		"Name": "my-workflow",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetWorkflow
	rec = doGlueRequest(t, h, "GetWorkflow", map[string]any{
		"Name": "my-workflow",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListWorkflows
	rec = doGlueRequest(t, h, "ListWorkflows", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateWorkflow
	rec = doGlueRequest(t, h, "UpdateWorkflow", map[string]any{
		"Name":        "my-workflow",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteWorkflow
	rec = doGlueRequest(t, h, "DeleteWorkflow", map[string]any{
		"Name": "my-workflow",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// setupGraphWorkflow creates a workflow with one ON_DEMAND trigger whose
// actions reference a real job and a real crawler, for TestWorkflow_Graph and
// TestWorkflow_LastRun (gopherstack-dol3).
func setupGraphWorkflow(t *testing.T, h *glue.Handler) {
	t.Helper()

	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name": "wfj1", "Role": "role1", "Command": map[string]any{"Name": "glueetl"},
	})
	doGlueRequest(t, h, "CreateCrawler", map[string]any{
		"Name": "wfc1", "Role": "role1",
		"Targets": map[string]any{"S3Targets": []map[string]any{{"Path": "s3://bucket/data/"}}},
	})
	doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": "graphwf"})

	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name": "graphwf-trigger", "Type": "ON_DEMAND", "WorkflowName": "graphwf",
		"Actions": []map[string]any{
			{"JobName": "wfj1"},
			{"CrawlerName": "wfc1"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// TestWorkflow_Graph covers gopherstack-dol3's Graph gap: GetWorkflow's Graph
// is derived from real trigger WorkflowName/Actions membership and gated by
// IncludeGraph, matching GetWorkflowInput.IncludeGraph.
func TestWorkflow_Graph(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		includeGraph bool
	}{
		{name: "omitted_without_includegraph", includeGraph: false},
		{name: "populated_with_includegraph", includeGraph: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			setupGraphWorkflow(t, h)

			body := map[string]any{"Name": "graphwf"}
			if tt.includeGraph {
				body["IncludeGraph"] = true
			}

			rec := doGlueRequest(t, h, "GetWorkflow", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			wf := out["Workflow"].(map[string]any)

			if !tt.includeGraph {
				assert.NotContains(t, wf, "Graph")

				return
			}

			graph := wf["Graph"].(map[string]any)
			nodes := graph["Nodes"].([]any)
			assert.Len(t, nodes, 3)

			edges := graph["Edges"].([]any)
			assert.Len(t, edges, 2)

			var triggerNode map[string]any
			for _, n := range nodes {
				node := n.(map[string]any)
				if node["Type"] == "TRIGGER" {
					triggerNode = node
				}
			}
			require.NotNil(t, triggerNode)
			assert.Equal(t, "graphwf-trigger", triggerNode["Name"])
			triggerDetails := triggerNode["TriggerDetails"].(map[string]any)
			trig := triggerDetails["Trigger"].(map[string]any)
			assert.Equal(t, "graphwf-trigger", trig["Name"])
		})
	}
}

// TestWorkflow_LastRun covers gopherstack-dol3's LastRun gap: it must reflect
// a real StartWorkflowRun and stay absent until one has happened, independent
// of IncludeGraph.
func TestWorkflow_LastRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	setupGraphWorkflow(t, h)

	rec := doGlueRequest(t, h, "GetWorkflow", map[string]any{"Name": "graphwf"})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotContains(t, out["Workflow"].(map[string]any), "LastRun")

	startRec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "graphwf"})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))
	runID := startOut["RunId"].(string)

	rec = doGlueRequest(t, h, "GetWorkflow", map[string]any{"Name": "graphwf"})
	require.Equal(t, http.StatusOK, rec.Code)
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	wf := out["Workflow"].(map[string]any)
	assert.NotContains(t, wf, "Graph")

	lastRun := wf["LastRun"].(map[string]any)
	assert.Equal(t, runID, lastRun["WorkflowRunId"])
}
