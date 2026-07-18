package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestHandler_CountOpenWorkflowExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		setup     []setupAction
		wantCode  int
		wantCount int
	}{
		{
			name: "no_executions",
			body: map[string]any{"domain": "d1"},
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
			},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
		{
			name: "with_running_executions",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-1"}},
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-2"}},
			},
			body:      map[string]any{"domain": "d1"},
			wantCode:  http.StatusOK,
			wantCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "CountOpenWorkflowExecutions", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseSWFResp(t, rec)
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

func TestHandler_CountClosedWorkflowExecutions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		setup     []setupAction
		wantCode  int
		wantCount int
	}{
		{
			name: "no_closed",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "StartWorkflowExecution", body: map[string]any{"domain": "d1", "workflowId": "wf-1"}},
			},
			body:      map[string]any{"domain": "d1"},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "CountClosedWorkflowExecutions", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseSWFResp(t, rec)
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

func TestHandler_TerminateWorkflowExecution(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     any
		name     string
		setup    []setupAction
		wantCode int
	}{
		{
			name: "success",
			setup: []setupAction{
				{action: "RegisterDomain", body: map[string]any{"name": "d1"}},
				{action: "StartWorkflowExecution", body: map[string]any{
					"domain": "d1", "workflowId": "wf-1",
				}},
			},
			body:     map[string]any{"domain": "d1", "workflowId": "wf-1"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not_found",
			body:     map[string]any{"domain": "d1", "workflowId": "missing"},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			for _, s := range tt.setup {
				doSWFRequest(t, h, s.action, s.body)
			}

			rec := doSWFRequest(t, h, "TerminateWorkflowExecution", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DescribeWorkflowExecution_StartTimestamp(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	doSWFRequest(t, h, "RegisterDomain", map[string]any{"name": "d1"})
	doSWFRequest(t, h, "StartWorkflowExecution", map[string]any{
		"domain": "d1", "workflowId": "wf-1",
	})

	rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
		"domain":    "d1",
		"execution": map[string]any{"workflowId": "wf-1"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseSWFResp(t, rec)
	execInfo := resp["executionInfo"].(map[string]any)
	startTS, ok := execInfo["startTimestamp"].(float64)
	assert.True(t, ok, "startTimestamp should be float64")
	assert.Greater(t, startTS, float64(0))
}

// TestHandler_ListWorkflowExecutions exercises ListOpenWorkflowExecutions and
// ListClosedWorkflowExecutions through the HTTP handler.
func TestHandler_ListWorkflowExecutions(t *testing.T) {
	t.Parallel()

	h := newTestSWFHandler(t)
	domain := "list-exec-domain"
	createSWFDomain(t, h, domain)
	createSWFWorkflowType(t, h, domain, "list-wf")
	startSWFExecution(t, h, domain, "list-wf", "list-exec-001")

	// ListOpenWorkflowExecutions
	rec := doSWFRequest(t, h, "ListOpenWorkflowExecutions", map[string]any{
		"domain": domain,
		"startTimeFilter": map[string]any{
			"oldestDate": 1.0,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListClosedWorkflowExecutions
	rec = doSWFRequest(t, h, "ListClosedWorkflowExecutions", map[string]any{
		"domain": domain,
		"startTimeFilter": map[string]any{
			"oldestDate": 1.0,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestHandler_DescribeWorkflowExecution_OpenCountsIncludesLambda verifies
// openCounts includes openLambdaFunctions.
func TestHandler_DescribeWorkflowExecution_OpenCountsIncludesLambda(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	require.NoError(t, b.RegisterDomain("dom", "", "NONE"))
	_, err := b.StartWorkflowExecution(swf.StartWorkflowExecutionInput{
		Domain: "dom", WorkflowID: "wf-1",
	})
	require.NoError(t, err)

	h := swf.NewHandler(b)
	rec := doSWFRequest(t, h, "DescribeWorkflowExecution", map[string]any{
		"domain":    "dom",
		"execution": map[string]any{"workflowId": "wf-1", "runId": ""},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseSWFResp(t, rec)
	counts := resp["openCounts"].(map[string]any)
	_, hasLambda := counts["openLambdaFunctions"]
	assert.True(t, hasLambda, "openCounts must include openLambdaFunctions")
}
