package swf_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/swf"
)

func TestHandler_CountPendingActivityTasks(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      any
		name      string
		wantCode  int
		wantCount int
	}{
		{
			name:      "returns_zero",
			body:      map[string]any{"domain": "d1", "taskList": map[string]any{"name": "my-list"}},
			wantCode:  http.StatusOK,
			wantCount: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestSWFHandler(t)
			rec := doSWFRequest(t, h, "CountPendingActivityTasks", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			resp := parseSWFResp(t, rec)
			assert.EqualValues(t, tt.wantCount, resp["count"])
		})
	}
}

// TestHandler_PollForActivityTask_WorkflowExecution verifies the response
// nests workflowExecution rather than flattening workflowId/runId at the top level.
func TestHandler_PollForActivityTask_WorkflowExecution(t *testing.T) {
	t.Parallel()

	b := swf.NewInMemoryBackend()
	b.EnqueueActivityTaskInternal("dom", "list", "act-1", "MyActivity", "1.0", "payload", "wf-10", "run-10")
	h := swf.NewHandler(b)

	rec := doSWFRequest(t, h, "PollForActivityTask", map[string]any{
		"domain":   "dom",
		"taskList": map[string]any{"name": "list"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := rec.Body.String()

	// Should have nested workflowExecution, not flat workflowId/runId at top level
	assert.Contains(t, body, `"workflowExecution"`, "workflowExecution nested object required")
	wePos := strings.Index(body, `"workflowExecution"`)
	wfPos := strings.Index(body, `"workflowId"`)
	assert.Greater(t, wfPos, wePos, "workflowId should appear inside workflowExecution")
}
