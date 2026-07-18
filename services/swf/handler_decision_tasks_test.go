package swf_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestHandler_CountPendingDecisionTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns_zero",
			body:      map[string]any{"domain": "d1", "taskList": map[string]any{"name": "decision-list"}},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			rec := doSWFRequest(t, h, "CountPendingDecisionTasks", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseSWFResp(t, rec)
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

// TestHandler_PollForDecisionTask_NoFlatWorkflowID verifies no top-level
// workflowId/runId in the response.
func TestHandler_PollForDecisionTask_NoFlatWorkflowID(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueDecisionTaskInternal("dom", "decisions", "wf-1", "run-1")
	h := swf.NewHandler(b)

	rec := doSWFRequest(t, h, "PollForDecisionTask", map[string]any{
		"domain":   "dom",
		"taskList": map[string]any{"name": "decisions"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	resp := parseSWFResp(t, rec)

	// workflowId and runId should only appear nested under workflowExecution
	_, hasTopWorkflowID := resp["workflowId"]
	_, hasTopRunID := resp["runId"]
	assert.False(t, hasTopWorkflowID, "workflowId must not be a top-level field")
	assert.False(t, hasTopRunID, "runId must not be a top-level field")
	assert.NotNil(t, resp["workflowExecution"], "workflowExecution nested object required")
}
