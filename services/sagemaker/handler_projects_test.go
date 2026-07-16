package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateProject", map[string]any{
		"ProjectName": "my-project",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ProjectArn"], "my-project")
	assert.NotEmpty(t, resp["ProjectId"])
}

func TestHandler_DescribeProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-1"})

	rec := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "proj-1", resp["ProjectName"])
	assert.Equal(t, "CreateCompleted", resp["ProjectStatus"])
}

func TestHandler_DeleteProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-del"})
	rec := doSageMakerRequest(t, h, "DeleteProject", map[string]any{"ProjectName": "proj-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListProjects(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})
	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-b"})

	rec := doSageMakerRequest(t, h, "ListProjects", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["ProjectSummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Space
// ---------------------------------------------------------------------------

func TestHandler_UpdateProject(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateProject", map[string]any{"ProjectName": "proj-a"})

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{
		"ProjectName": "proj-a", "ProjectDescription": "updated description",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeProject", map[string]any{"ProjectName": "proj-a"})
	require.Equal(t, http.StatusOK, recDescribe.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "updated description", out["ProjectDescription"])
}

func TestHandler_UpdateProject_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateProject", map[string]any{"ProjectName": "no-such-project"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Pipeline versions and execution-definition extras
// ---------------------------------------------------------------------------
