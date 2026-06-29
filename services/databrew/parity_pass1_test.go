package databrew_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

// TestParity_StartJobRun_RunId verifies StartJobRun response uses "RunId" (not "RunID").
func TestParity_StartJobRun_RunId(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "pj1"})

	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/pj1/startJobRun", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["RunID"], "RunID (uppercase D) must not appear — AWS SDK uses RunId")
	runID, ok := resp["RunId"].(string)
	assert.True(t, ok, "response must have RunId string field")
	assert.NotEmpty(t, runID)
}

// TestParity_RecipeReference verifies CreateRecipeJob reads RecipeReference and DescribeJob returns it.
func TestParity_RecipeReference(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes",
		map[string]any{"Name": "my-recipe", "Steps": []any{}})

	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name":            "rj1",
		"RecipeReference": map[string]any{"Name": "my-recipe", "RecipeVersion": "LATEST_WORKING"},
		"RoleArn":         "arn:aws:iam::123456789012:role/r",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	desc := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/rj1", nil)
	require.Equal(t, http.StatusOK, desc.Code)

	var job map[string]any
	require.NoError(t, json.Unmarshal(desc.Body.Bytes(), &job))

	assert.Nil(t, job["RecipeName"], "RecipeName must not appear in JSON — AWS SDK uses RecipeReference")
	ref, ok := job["RecipeReference"].(map[string]any)
	require.True(t, ok, "RecipeReference must be an object")
	assert.Equal(t, "my-recipe", ref["Name"])
}

// TestParity_RecipeVersionOps verifies ListRecipeVersions, BatchDeleteRecipeVersion,
// and DeleteRecipeVersion routing.
func TestParity_RecipeVersionOps(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
		wantOK bool
	}{
		{
			name:   "ListRecipeVersions GET",
			method: http.MethodGet,
			path:   "/databrew/v1/recipes/my-recipe/recipeVersions",
			wantOp: "ListRecipeVersions",
			wantOK: true,
		},
		{
			name:   "BatchDeleteRecipeVersion POST",
			method: http.MethodPost,
			path:   "/databrew/v1/recipes/my-recipe/recipeVersions",
			wantOp: "BatchDeleteRecipeVersion",
			wantOK: true,
		},
		{
			name:   "DeleteRecipeVersion DELETE",
			method: http.MethodDelete,
			path:   "/databrew/v1/recipes/my-recipe/recipeVersion/1.0",
			wantOp: "DeleteRecipeVersion",
			wantOK: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes",
				map[string]any{"Name": "my-recipe", "Steps": []any{}})

			if tc.wantOK {
				rec := databrewReq(t, h, tc.method, tc.path, map[string]any{
					"RecipeVersions": []string{"1.0"},
				})
				assert.Equal(t, http.StatusOK, rec.Code, "path %s method %s", tc.path, tc.method)
			}
		})
	}
}

// TestParity_ExtractOperation_RecipeVersions verifies op routing for recipe version paths.
func TestParity_ExtractOperation_RecipeVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "list recipe versions",
			method: http.MethodGet,
			path:   "/databrew/v1/recipes/r/recipeVersions",
			wantOp: "ListRecipeVersions",
		},
		{
			name:   "batch delete recipe versions",
			method: http.MethodPost,
			path:   "/databrew/v1/recipes/r/recipeVersions",
			wantOp: "BatchDeleteRecipeVersion",
		},
		{
			name:   "delete single recipe version",
			method: http.MethodDelete,
			path:   "/databrew/v1/recipes/r/recipeVersion/1.0",
			wantOp: "DeleteRecipeVersion",
		},
		{
			name:   "publish recipe",
			method: http.MethodPost,
			path:   "/databrew/v1/recipes/r/publishRecipe",
			wantOp: "PublishRecipe",
		},
	}

	h := newTestHandler()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := extractOp(t, h, tc.method, tc.path)
			assert.Equal(t, tc.wantOp, got)
		})
	}
}

// TestParity_ListJobs_Filters verifies ListJobs datasetName and projectName filtering.
func TestParity_ListJobs_Filters(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "profile-ds-a", "DatasetName": "ds-a"})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "profile-ds-b", "DatasetName": "ds-b"})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes",
		map[string]any{"Name": "r1", "Steps": []any{}})
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name":            "recipe-proj-a",
		"ProjectName":     "proj-a",
		"RecipeReference": map[string]any{"Name": "r1"},
		"RoleArn":         "arn:aws:iam::123456789012:role/r",
	})

	tests := []struct {
		queryParams url.Values
		name        string
		wantCount   int
	}{
		{
			name:        "no filter returns all",
			queryParams: url.Values{},
			wantCount:   3,
		},
		{
			name:        "filter by datasetName=ds-a",
			queryParams: url.Values{"datasetName": {"ds-a"}},
			wantCount:   1,
		},
		{
			name:        "filter by datasetName=ds-b",
			queryParams: url.Values{"datasetName": {"ds-b"}},
			wantCount:   1,
		},
		{
			name:        "filter by projectName=proj-a",
			queryParams: url.Values{"projectName": {"proj-a"}},
			wantCount:   1,
		},
		{
			name:        "filter by nonexistent datasetName",
			queryParams: url.Values{"datasetName": {"ds-none"}},
			wantCount:   0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			path := "/databrew/v1/jobs"
			if len(tc.queryParams) > 0 {
				path = path + "?" + tc.queryParams.Encode()
			}
			rec := databrewReq(t, h, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			jobs := resp["Jobs"].([]any)
			assert.Len(t, jobs, tc.wantCount, "query=%v", tc.queryParams)
		})
	}
}

// TestParity_JobRunIdField_RoundTrip verifies RunId field survives start→describe→stop.
func TestParity_JobRunIdField_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs",
		map[string]any{"Name": "rt-job"})

	startRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/rt-job/startJobRun", nil)
	require.Equal(t, http.StatusOK, startRec.Code)

	var startResp map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &startResp))
	runID, ok := startResp["RunId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, runID)

	// Wait for transition so DescribeJobRun is non-empty.
	time.Sleep(200 * time.Millisecond)

	descRec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/rt-job/jobRun/"+runID, nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, runID, descResp["RunId"])

	stopRec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/rt-job/jobRun/"+runID, nil)
	require.Equal(t, http.StatusOK, stopRec.Code)
	var stopResp map[string]any
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &stopResp))
	assert.Equal(t, runID, stopResp["RunId"])
}

// TestParity_TagsOps verifies tag CRUD on the /databrew/v1/tags/{arn} path.
func TestParity_TagsOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":  "tagged-ds",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	resourceArn := "arn:aws:databrew:us-east-1:123456789012:dataset/tagged-ds"
	prefix := "/databrew/v1/tags/" + resourceArn

	tagRec := databrewReq(t, h, http.MethodPost, prefix,
		map[string]any{"Tags": map[string]any{"env": "prod"}})
	require.Equal(t, http.StatusOK, tagRec.Code)

	listRec := databrewReq(t, h, http.MethodGet, prefix, nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags := listResp["Tags"].(map[string]any)
	assert.Equal(t, "prod", tags["env"])

	untagRec := databrewReq(t, h, http.MethodDelete, prefix,
		map[string]any{"tagKeys": []string{"env"}})
	assert.Equal(t, http.StatusOK, untagRec.Code)
}

// extractOp is a helper that calls ExtractOperation via a fake echo context.
func extractOp(t *testing.T, h *databrew.Handler, method, path string) string {
	t.Helper()

	req := httptest.NewRequest(method, path, nil)
	e := echo.New()
	c := e.NewContext(req, httptest.NewRecorder())

	return h.ExtractOperation(c)
}
