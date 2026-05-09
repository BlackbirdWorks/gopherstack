package databrew_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/databrew"
)

func newTestHandler() *databrew.Handler {
	return databrew.NewHandler(databrew.NewInMemoryBackend("123456789012", "us-east-1"))
}

func databrewReq(t *testing.T, h *databrew.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var buf bytes.Buffer
	if body != nil {
		require.NoError(t, json.NewEncoder(&buf).Encode(body))
	}

	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// ---- Dataset handlers ----

func TestHandlerCreateDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name":   "my-ds",
		"Format": "CSV",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "my-bucket"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-ds", resp["Name"])
}

func TestHandlerCreateDataset_Duplicate(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	body := map[string]any{
		"Name":   "dup-ds",
		"Format": "CSV",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	}
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", body)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec2 := databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", body)
	assert.Equal(t, http.StatusConflict, rec2.Code)
}

func TestHandlerDescribeDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "ds1", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets/ds1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var ds map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &ds))
	assert.Equal(t, "ds1", ds["Name"])
}

func TestHandlerDescribeDataset_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerListDatasets(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "a", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Datasets"])
}

func TestHandlerUpdateDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "upd-ds", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/datasets/upd-ds", map[string]any{
		"Format": "JSON",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b2"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteDataset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "del-ds", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/datasets/del-ds", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	rec2 := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets/del-ds", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ---- Recipe handlers ----

func TestHandlerCreateRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{
		"Name": "my-recipe", "Description": "test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "my-recipe", resp["Name"])
}

func TestHandlerDescribeRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "r1"})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes/r1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListRecipes(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/recipes", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Recipes"])
}

func TestHandlerPublishRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "pub-r"})
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes/pub-r/publishRecipe", map[string]any{
		"Description": "published",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUpdateRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "upd-r"})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/recipes/upd-r", map[string]any{
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteRecipe(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/recipes", map[string]any{"Name": "del-r"})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/recipes/del-r", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Project handlers ----

func TestHandlerCreateProject(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "my-proj", "RecipeName": "r1", "DatasetName": "ds1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeProject(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "proj1", "RecipeName": "r1",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/projects/proj1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListProjects(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/projects", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerUpdateProject(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "upd-proj", "RecipeName": "r1",
	})
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/projects/upd-proj", map[string]any{
		"DatasetName": "new-ds",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteProject(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/projects", map[string]any{
		"Name": "del-proj", "RecipeName": "r1",
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/projects/del-proj", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Job handlers ----

func TestHandlerCreateProfileJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "profile-job", "DatasetName": "ds1",
		"RoleArn":        "arn:aws:iam::123456789012:role/Role",
		"OutputLocation": map[string]any{"Bucket": "out"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerCreateRecipeJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/recipeJobs", map[string]any{
		"Name": "recipe-job", "RecipeName": "r1",
		"RoleArn": "arn:aws:iam::123456789012:role/Role",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDescribeJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "j1", "DatasetName": "ds1",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/j1", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListJobs(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerDeleteJob(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "del-job",
	})
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/jobs/del-job", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerStartJobRun(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "run-job",
	})
	rec := databrewReq(t, h, http.MethodPost, "/databrew/v1/jobs/run-job/startJobRun", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandlerListJobRuns(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/profileJobs", map[string]any{
		"Name": "list-job",
	})
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/jobs/list-job/jobRuns", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// ---- Error cases ----

func TestHandlerUnknownPath(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/unknown-resource", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestHandlerDeleteJob_NotFound(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodDelete, "/databrew/v1/jobs/no-such", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
