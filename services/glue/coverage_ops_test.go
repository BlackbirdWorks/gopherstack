package glue_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGlue_Triggers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// CreateTrigger
	rec := doGlueRequest(t, h, "CreateTrigger", map[string]any{
		"Name":    "my-trigger",
		"Type":    "ON_DEMAND",
		"Actions": []map[string]any{{"JobName": "my-job"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// GetTrigger
	rec = doGlueRequest(t, h, "GetTrigger", map[string]any{
		"Name": "my-trigger",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// GetTriggers
	rec = doGlueRequest(t, h, "GetTriggers", nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	// UpdateTrigger
	rec = doGlueRequest(t, h, "UpdateTrigger", map[string]any{
		"Name":          "my-trigger",
		"TriggerUpdate": map[string]any{"Description": "updated"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// DeleteTrigger
	rec = doGlueRequest(t, h, "DeleteTrigger", map[string]any{
		"Name": "my-trigger",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
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

func TestGlue_ResetJobBookmark(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a job first
	doGlueRequest(t, h, "CreateJob", map[string]any{
		"Name":    "bm-job",
		"Command": map[string]any{"Name": "glueetl", "ScriptLocation": "s3://bucket/script.py"},
		"Role":    "arn:aws:iam::123456789012:role/GlueRole",
	})

	rec := doGlueRequest(t, h, "ResetJobBookmark", map[string]any{
		"JobName": "bm-job",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}
