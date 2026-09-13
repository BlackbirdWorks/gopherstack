package glue_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	gluesdk "github.com/aws/aws-sdk-go-v2/service/glue"
	"github.com/aws/aws-sdk-go-v2/service/glue/types"
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

// TestResumeWorkflowRun_Stateful verifies ResumeWorkflowRun's real state
// gate: only a STOPPED run can be resumed (api_op_ResumeWorkflowRun.go:
// "Restarts ... a previous partially completed workflow run"), and a
// required-member-missing request is rejected rather than silently
// succeeding with an empty response.
func TestResumeWorkflowRun_Stateful(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(t *testing.T, h *glue.Handler) (wfName, runID string)
		name     string
		wantCode int
	}{
		{
			name: "empty_inputs_rejected",
			setup: func(_ *testing.T, _ *glue.Handler) (string, string) {
				return "", ""
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "running_run_rejected_not_stopped",
			setup: func(t *testing.T, h *glue.Handler) (string, string) {
				t.Helper()

				return startedWorkflowRun(t, h, "my-workflow-running")
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "stopped_run_resumes_with_new_run_id",
			setup: func(t *testing.T, h *glue.Handler) (string, string) {
				t.Helper()

				wfName, runID := startedWorkflowRun(t, h, "my-workflow-stopped")

				stopRec := doGlueRequest(t, h, "StopWorkflowRun", map[string]any{
					"Name":  wfName,
					"RunId": runID,
				})
				require.Equal(t, http.StatusOK, stopRec.Code)

				return wfName, runID
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
				"NodeIds": []string{"node-a"},
			})
			assert.Equal(t, tc.wantCode, rec.Code, rec.Body.String())

			if tc.wantCode != http.StatusOK {
				return
			}

			var out struct {
				RunID   string   `json:"RunId"`
				NodeIDs []string `json:"NodeIds"`
			}
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out.RunID)
			assert.NotEqual(t, runID, out.RunID, "resume must mint a new RunId, not echo the original")
			assert.Equal(t, []string{"node-a"}, out.NodeIDs)
		})
	}
}

// startedWorkflowRun creates a workflow and starts one run on it, returning
// (workflow name, run ID).
func startedWorkflowRun(t *testing.T, h *glue.Handler, name string) (string, string) {
	t.Helper()

	createRec := doGlueRequest(t, h, "CreateWorkflow", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, createRec.Code)

	startRec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": name})
	require.Equal(t, http.StatusOK, startRec.Code)

	var startOut struct {
		RunID string `json:"RunId"`
	}
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startOut))

	return name, startOut.RunID
}

// TestResumeWorkflowRun_EchoesRequestedNodes verifies NodeIds ("This member
// is required" per api_op_ResumeWorkflowRun.go) is actually threaded through
// to the backend and echoed back in ResumeWorkflowRunOutput.NodeIds ("The
// new nodes that were actually restarted"), rather than the request always
// getting silently dropped and the response always reporting an empty list.
// The run must be STOPPED before it can be resumed (see
// TestResumeWorkflowRun_Stateful).
func TestResumeWorkflowRun_EchoesRequestedNodes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wfName, runID := startedWorkflowRun(t, h, "my-workflow")

	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "StopWorkflowRun", map[string]any{
		"Name":  wfName,
		"RunId": runID,
	}).Code)

	rec := doGlueRequest(t, h, "ResumeWorkflowRun", map[string]any{
		"Name":    wfName,
		"RunId":   runID,
		"NodeIds": []string{"node-a", "node-b"},
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var out struct {
		RunID   string   `json:"RunId"`
		NodeIDs []string `json:"NodeIds"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, []string{"node-a", "node-b"}, out.NodeIDs)
	assert.NotEqual(t, runID, out.RunID)
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
	// Real wire key is "Name", not "WorkflowName" (deserializers.go's
	// awsAwsjson11_deserializeDocumentWorkflowRun case list) -- a real client's
	// sv.Name stayed nil before this fix.
	assert.Equal(t, "graphwf", lastRun["Name"])
	assert.NotContains(t, lastRun, "WorkflowName")
}

// TestStopWorkflowRun_StateGuard verifies StopWorkflowRun only applies to a
// RUNNING run (IllegalWorkflowStateException otherwise, per
// deserializers.go's awsAwsjson11_deserializeOpErrorStopWorkflowRun error
// switch) and that stopping settles the run to STOPPED synchronously rather
// than stranding it in STOPPING forever.
func TestStopWorkflowRun_StateGuard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wfName, runID := startedWorkflowRun(t, h, "my-workflow")

	getRec := doGlueRequest(t, h, "GetWorkflowRun", map[string]any{"Name": wfName, "RunId": runID})
	require.Equal(t, http.StatusOK, getRec.Code)
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "RUNNING", getOut["Run"].(map[string]any)["Status"])

	stopRec := doGlueRequest(t, h, "StopWorkflowRun", map[string]any{"Name": wfName, "RunId": runID})
	require.Equal(t, http.StatusOK, stopRec.Code)

	getRec = doGlueRequest(t, h, "GetWorkflowRun", map[string]any{"Name": wfName, "RunId": runID})
	require.Equal(t, http.StatusOK, getRec.Code)
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getOut))
	assert.Equal(t, "STOPPED", getOut["Run"].(map[string]any)["Status"])

	// Stopping again must fail: the run is no longer RUNNING.
	stopAgainRec := doGlueRequest(t, h, "StopWorkflowRun", map[string]any{"Name": wfName, "RunId": runID})
	assert.Equal(t, http.StatusBadRequest, stopAgainRec.Code)
	var errOut map[string]string
	require.NoError(t, json.Unmarshal(stopAgainRec.Body.Bytes(), &errOut))
	assert.Equal(t, "IllegalWorkflowStateException", errOut["__type"])

	// Stopping a nonexistent run is EntityNotFoundException, not the state error.
	missingRec := doGlueRequest(t, h, "StopWorkflowRun", map[string]any{"Name": wfName, "RunId": "no-such-run"})
	assert.Equal(t, http.StatusBadRequest, missingRec.Code)
	require.NoError(t, json.Unmarshal(missingRec.Body.Bytes(), &errOut))
	assert.Equal(t, "EntityNotFoundException", errOut["__type"])
}

// TestGetWorkflowRunProperties_RequiredMembersAndNotFound verifies
// GetWorkflowRunProperties propagates EntityNotFoundException for an unknown
// workflow/run (declared in its real error catalog,
// deserializers.go's awsAwsjson11_deserializeOpErrorGetWorkflowRunProperties)
// instead of silently returning an empty 200, and rejects a request missing
// required members (Name/RunId, both "This member is required" on the real
// GetWorkflowRunPropertiesInput).
func TestGetWorkflowRunProperties_RequiredMembersAndNotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body map[string]any
		name string
	}{
		{name: "missing_name", body: map[string]any{"RunId": "some-run"}},
		{name: "missing_run_id", body: map[string]any{"Name": "some-wf"}},
		{name: "unknown_workflow", body: map[string]any{"Name": "no-such-wf", "RunId": "no-such-run"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doGlueRequest(t, h, "GetWorkflowRunProperties", tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code, rec.Body.String())
		})
	}
}

// TestSDKRoundTrip_GetWorkflowRuns_Pagination drives the real aws-sdk-go-v2
// client. GetWorkflowRunsInput carries real MaxResults/NextToken query
// members (api_op_GetWorkflowRuns.go) that the handler previously never
// declared or read at all, so every call returned every stored run in one
// unbounded response.
func TestSDKRoundTrip_GetWorkflowRuns_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)

	require.Equal(t, http.StatusOK, doGlueRequest(t, h, "CreateWorkflow", map[string]any{
		"Name": "paginated-wf",
	}).Code)

	const numRuns = 5

	wantIDs := make(map[string]bool, numRuns)
	for range numRuns {
		rec := doGlueRequest(t, h, "StartWorkflowRun", map[string]any{"Name": "paginated-wf"})
		require.Equal(t, http.StatusOK, rec.Code)

		var out struct {
			RunID string `json:"RunId"`
		}
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		wantIDs[out.RunID] = true
	}
	require.Len(t, wantIDs, numRuns)

	gotIDs := make(map[string]bool)
	input := &gluesdk.GetWorkflowRunsInput{
		Name:       aws.String("paginated-wf"),
		MaxResults: aws.Int32(2),
	}

	for pages := 0; ; pages++ {
		require.Less(t, pages, 10, "pagination did not terminate")

		out, err := client.GetWorkflowRuns(t.Context(), input)
		require.NoError(t, err)
		require.LessOrEqual(t, len(out.Runs), 2, "must honor MaxResults")

		for _, r := range out.Runs {
			require.NotNil(t, r.WorkflowRunId)
			gotIDs[*r.WorkflowRunId] = true
		}

		if out.NextToken == nil || *out.NextToken == "" {
			break
		}

		input.NextToken = out.NextToken
	}

	assert.Equal(t, wantIDs, gotIDs, "paginated union must equal the seeded set exactly")
}

// TestSDKRoundTrip_WorkflowRun_NameWireKeyAndResumeLinksPreviousRun drives
// the real aws-sdk-go-v2 client to prove two fixes: (1) WorkflowRun's wire
// key for the workflow's name is "Name", not the previously-emitted
// "WorkflowName" -- a real client's types.WorkflowRun.Name stayed nil before
// the fix, since no known key matched during decode; (2) ResumeWorkflowRun
// mints a new run linked via PreviousRunId ("Each resume of a workflow run
// will have a new run ID", api_op_ResumeWorkflowRun.go), not the pre-fix
// behavior of mutating and echoing back the same run ID.
func TestSDKRoundTrip_WorkflowRun_NameWireKeyAndResumeLinksPreviousRun(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestGlueClient(t, h)

	_, err := client.CreateWorkflow(t.Context(), &gluesdk.CreateWorkflowInput{Name: aws.String("resume-wf")})
	require.NoError(t, err)

	startOut, err := client.StartWorkflowRun(t.Context(), &gluesdk.StartWorkflowRunInput{
		Name: aws.String("resume-wf"),
	})
	require.NoError(t, err)
	require.NotNil(t, startOut.RunId)
	origRunID := *startOut.RunId

	getOut, err := client.GetWorkflowRun(t.Context(), &gluesdk.GetWorkflowRunInput{
		Name: aws.String("resume-wf"), RunId: aws.String(origRunID),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Run)
	require.NotNil(t, getOut.Run.Name, "real client decode must populate WorkflowRun.Name via the \"Name\" wire key")
	assert.Equal(t, "resume-wf", *getOut.Run.Name)

	_, err = client.StopWorkflowRun(t.Context(), &gluesdk.StopWorkflowRunInput{
		Name: aws.String("resume-wf"), RunId: aws.String(origRunID),
	})
	require.NoError(t, err)

	resumeOut, err := client.ResumeWorkflowRun(t.Context(), &gluesdk.ResumeWorkflowRunInput{
		Name:    aws.String("resume-wf"),
		RunId:   aws.String(origRunID),
		NodeIds: []string{"node-a"},
	})
	require.NoError(t, err)
	require.NotNil(t, resumeOut.RunId)
	assert.NotEqual(t, origRunID, *resumeOut.RunId)
	assert.Equal(t, []string{"node-a"}, resumeOut.NodeIds)

	newRunOut, err := client.GetWorkflowRun(t.Context(), &gluesdk.GetWorkflowRunInput{
		Name: aws.String("resume-wf"), RunId: resumeOut.RunId,
	})
	require.NoError(t, err)
	require.NotNil(t, newRunOut.Run.PreviousRunId)
	assert.Equal(t, origRunID, *newRunOut.Run.PreviousRunId)
	assert.Equal(t, types.WorkflowRunStatusRunning, newRunOut.Run.Status)

	// Resuming a still-RUNNING run must fail.
	_, err = client.ResumeWorkflowRun(t.Context(), &gluesdk.ResumeWorkflowRunInput{
		Name:    aws.String("resume-wf"),
		RunId:   resumeOut.RunId,
		NodeIds: []string{"node-a"},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "IllegalWorkflowStateException")
}
