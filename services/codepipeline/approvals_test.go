package codepipeline_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ActionRevisionAndApproval(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreatePipeline", map[string]any{
		"pipeline": samplePipeline("ap-pipeline"),
	})
	doRequest(t, h, "StartPipelineExecution", map[string]any{"name": "ap-pipeline"})

	// Put action revision
	rec := doRequest(t, h, "PutActionRevision", map[string]any{
		"pipelineName": "ap-pipeline",
		"stageName":    "Source",
		"actionName":   "SourceAction",
		"actionRevision": map[string]any{
			"revisionId":       "abc123",
			"revisionChangeId": "change-1",
		},
	})
	assert.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "PutActionRevision", map[string]any{})
	assert.Equal(t, 400, rec.Code)

	// Put approval result
	rec = doRequest(t, h, "PutApprovalResult", map[string]any{
		"pipelineName": "ap-pipeline",
		"stageName":    "Source",
		"actionName":   "SourceAction",
		"result": map[string]any{
			"status":  "Approved",
			"summary": "looks good",
		},
		"token": "approval-token",
	})
	assert.Equal(t, 200, rec.Code)

	// Missing pipeline name
	rec = doRequest(t, h, "PutApprovalResult", map[string]any{})
	assert.Equal(t, 400, rec.Code)
}

// ---- Action type operations ----

func TestPutApprovalResult_ApprovedAt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		status string
	}{
		{name: "approved", status: "Approved"},
		{name: "rejected", status: "Rejected"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			rec := doRequest(t, h, "CreatePipeline", map[string]any{
				"pipeline": samplePipeline("approval-pipe"),
			})
			require.Equal(t, http.StatusOK, rec.Code)

			rec = doRequest(t, h, "PutApprovalResult", map[string]any{
				"pipelineName": "approval-pipe",
				"stageName":    "Source",
				"actionName":   "SourceAction",
				"approvalResult": map[string]any{
					"status":  tt.status,
					"summary": "looks good",
				},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			approvedAt, _ := out["approvedAt"].(string)
			assert.NotEmpty(t, approvedAt, "approvedAt must be set")
		})
	}
}

// TestParity_ListPipelineExecutions_TriggerObject verifies trigger is an object.
