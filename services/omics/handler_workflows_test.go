package omics_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOmics_Workflow(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		check    func(t *testing.T, body []byte)
		body     any
		method   string
		path     string
		wantCode int
	}{
		{
			name:     "CreateWorkflow returns 201",
			method:   http.MethodPost,
			path:     "/workflow",
			body:     map[string]any{"name": "wf1", "engine": "WDL"},
			wantCode: http.StatusCreated,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.Contains(t, resp["arn"], "arn:aws:omics:")
				assert.NotEmpty(t, resp["id"])
				assert.Equal(t, "CREATING", resp["status"])
			},
		},
		{
			name:     "CreateWorkflow missing name returns 400",
			method:   http.MethodPost,
			path:     "/workflow",
			body:     map[string]any{"engine": "WDL"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "ListWorkflows returns empty",
			method:   http.MethodGet,
			path:     "/workflow",
			wantCode: http.StatusOK,
			check: func(t *testing.T, body []byte) {
				t.Helper()
				var resp map[string]any
				require.NoError(t, json.Unmarshal(body, &resp))
				assert.NotNil(t, resp["items"])
			},
		},
		{
			name:     "GetWorkflow unknown returns 404",
			method:   http.MethodGet,
			path:     "/workflow/doesnotexist",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tc.method, tc.path, tc.body)
			assert.Equal(t, tc.wantCode, rec.Code)

			if tc.check != nil {
				tc.check(t, rec.Body.Bytes())
			}
		})
	}
}

// TestWorkflowStartsAsCreating verifies that CreateWorkflow returns status CREATING,
// not ACTIVE. AWS sets workflows to CREATING while it processes the definition.
func TestWorkflowStartsAsCreating(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "my-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "CREATING", resp["status"])
}

// TestCreateWorkflowReturnsPartialResponse verifies CreateWorkflow returns only
// {arn, id, status, tags} per AWS behavior.
func TestCreateWorkflowReturnsPartialResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "my-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["arn"])
	assert.NotEmpty(t, resp["id"])
	assert.Nil(t, resp["name"], "full workflow object must not be returned")
	assert.Nil(t, resp["description"], "full workflow object must not be returned")
}

// TestWorkflowHasTypeField verifies that GetWorkflow returns a type field.
// AWS always sets type to PRIVATE for user-created workflows.
func TestWorkflowHasTypeField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "typed-workflow",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	wfID := createResp["id"].(string)

	getRec := doRequest(t, h, http.MethodGet, "/workflow/"+wfID, nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Equal(t, "PRIVATE", getResp["type"])
}

// TestUpdateWorkflowReturnsWorkflow verifies that UpdateWorkflow returns the
// updated Workflow object, not an empty body. Real AWS returns the full workflow.
func TestUpdateWorkflowReturnsWorkflow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":   "original-name",
		"engine": "WDL",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	wfID := createResp["id"].(string)

	updateRec := doRequest(t, h, http.MethodPost, "/workflow/"+wfID, map[string]any{
		"name":        "updated-name",
		"description": "new description",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, "updated-name", updateResp["name"])
	assert.Equal(t, "new description", updateResp["description"])
}

// TestCreateWorkflowAcceptsDefinitionUri verifies that CreateWorkflow accepts
// the definitionUri field (S3 URI) in addition to definitionZip.
func TestCreateWorkflowAcceptsDefinitionUri(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{
		"name":          "uri-workflow",
		"engine":        "WDL",
		"definitionUri": "s3://my-bucket/workflow.zip",
	})
	assert.Equal(t, http.StatusCreated, rec.Code)
}

// TestCreateWorkflow_ResponseIncludesUUID verifies that CreateWorkflow's
// response includes the optional uuid field real CreateWorkflowOutput has
// (gopherstack-fedo), not just {arn, id, status, tags}.
func TestCreateWorkflow_ResponseIncludesUUID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf1", "engine": "WDL"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["uuid"], "CreateWorkflowOutput.uuid must be populated")
}

// TestListWorkflows_FiltersByNameAndType verifies ListWorkflows applies its
// name/type query filters (real AWS ListWorkflowsInput query parameters).
func TestListWorkflows_FiltersByNameAndType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf-a", "engine": "WDL"})
	doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf-b", "engine": "WDL"})

	rec := doRequest(t, h, http.MethodGet, "/workflow?name=wf-a", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	workflows, ok := resp["items"].([]any)
	require.True(t, ok)
	require.Len(t, workflows, 1)
	assert.Equal(t, "wf-a", workflows[0].(map[string]any)["name"])

	// Every workflow created here is type PRIVATE; filtering by READY2RUN
	// must exclude both.
	rec2 := doRequest(t, h, http.MethodGet, "/workflow?type=READY2RUN", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	workflows2, ok := resp2["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, workflows2)
}

// TestListWorkflowVersions_FiltersByType verifies ListWorkflowVersions
// applies its type query filter (real AWS ListWorkflowVersionsInput "type"
// query parameter).
func TestListWorkflowVersions_FiltersByType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	wfRec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "wf1", "engine": "WDL"})
	require.Equal(t, http.StatusCreated, wfRec.Code)

	var wf map[string]any
	require.NoError(t, json.Unmarshal(wfRec.Body.Bytes(), &wf))
	wfID := wf["id"].(string)

	verRec := doRequest(t, h, http.MethodPost, "/workflow/"+wfID+"/version", map[string]any{"versionName": "v1"})
	require.Equal(t, http.StatusCreated, verRec.Code)

	// The version inherits the workflow's PRIVATE type; filtering by
	// READY2RUN must exclude it.
	rec := doRequest(t, h, http.MethodGet, "/workflow/"+wfID+"/version?type=READY2RUN", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions, ok := resp["items"].([]any)
	require.True(t, ok)
	assert.Empty(t, versions)

	rec2 := doRequest(t, h, http.MethodGet, "/workflow/"+wfID+"/version?type=PRIVATE", nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	versions2, ok := resp2["items"].([]any)
	require.True(t, ok)
	assert.Len(t, versions2, 1)
}
