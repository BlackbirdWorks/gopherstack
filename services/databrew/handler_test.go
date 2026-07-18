package databrew_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Handler metadata ----

func TestHandlerName(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	assert.Equal(t, "DataBrew", h.Name())
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	databrewReq(t, h, http.MethodPost, "/databrew/v1/datasets", map[string]any{
		"Name": "to-reset", "Format": "CSV",
		"Input": map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	h.Reset()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/datasets", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	datasets := resp["Datasets"].([]any)
	assert.Empty(t, datasets)
}

func TestHandlerStartWorker(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	require.NoError(t, h.StartWorker(nil)) //nolint:staticcheck // existing issue.
}

func TestHandlerRouteMatcher(t *testing.T) {
	t.Parallel()

	const databrewAuth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/databrew/aws4_request"
	const otherAuth = "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-east-1/s3/aws4_request"

	tests := []struct {
		path  string
		auth  string
		match bool
	}{
		{path: "/databrew/v1/datasets", match: true},
		{path: "/databrew/v1", match: true},
		{path: "/databrew/v1/recipes/foo", match: true},
		{path: "/other/path", match: false},
		// Unambiguous top-level segments match unconditionally.
		{path: "/recipes/foo", match: true},
		{path: "/profileJobs/foo", match: true},
		{path: "/recipeJobs/foo", match: true},
		{path: "/rulesets/foo", match: true},
		{path: "/projects/foo", match: true},
		// Ambiguous top-level segments require a SigV4 databrew credential
		// scope. These are real bare AWS paths (no "/databrew/v1/" prefix):
		// TagResource/UntagResource/ListTagsForResource at /tags/{arn} and
		// ListRecipeVersions at /recipeVersions?name=...
		{path: "/datasets", auth: databrewAuth, match: true},
		{path: "/datasets", auth: otherAuth, match: false},
		{path: "/datasets", match: false},
		{path: "/schedules/foo", auth: databrewAuth, match: true},
		{path: "/jobs/foo", auth: databrewAuth, match: true},
		{path: "/tags/arn:aws:databrew:us-east-1:111111111111:job/myjob", auth: databrewAuth, match: true},
		{path: "/tags/arn:aws:databrew:us-east-1:111111111111:job/myjob", auth: otherAuth, match: false},
		{path: "/tags/arn:aws:databrew:us-east-1:111111111111:job/myjob", match: false},
		{path: "/recipeVersions", auth: databrewAuth, match: true},
		{path: "/recipeVersions", match: false},
	}

	h := newTestHandler()
	matcher := h.RouteMatcher()
	for _, tc := range tests {
		t.Run(tc.path+"/"+tc.auth, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tc.path, nil)
			if tc.auth != "" {
				req.Header.Set("Authorization", tc.auth)
			}
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tc.match, matcher(c))
		})
	}
}

func TestHandlerMatchPriority(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	assert.Positive(t, h.MatchPriority())
}

func TestHandlerExtractOperation(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{"list datasets", http.MethodGet, "/databrew/v1/datasets", "ListDatasets"},
		{"create dataset", http.MethodPost, "/databrew/v1/datasets", "CreateDataset"},
		{"describe dataset", http.MethodGet, "/databrew/v1/datasets/foo", "DescribeDataset"},
		{"list jobs", http.MethodGet, "/databrew/v1/jobs", "ListJobs"},
		{"unknown", http.MethodGet, "/other", "Unknown"},
	}
	h := newTestHandler()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.wantOp, extractOp(t, h, tc.method, tc.path))
		})
	}
}

func TestHandlerExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantResource string
	}{
		{"describe dataset", http.MethodGet, "/databrew/v1/datasets/foo", "foo"},
		{"list datasets has no resource", http.MethodGet, "/databrew/v1/datasets", ""},
		{"describe job", http.MethodGet, "/databrew/v1/jobs/myjob", "myjob"},
	}

	h := newTestHandler()
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tc.method, tc.path, nil)
			e := echo.New()
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tc.wantResource, h.ExtractResource(c))
		})
	}
}

// ---- Error cases ----

func TestHandlerUnknownPath(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodGet, "/databrew/v1/unknown-resource", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// ---- Handler: name-in-body mismatch ----

func TestHandlerCreateDataset_NameMismatch(t *testing.T) {
	t.Parallel()
	h := newTestHandler()
	rec := databrewReq(t, h, http.MethodPut, "/databrew/v1/datasets/name-in-path", map[string]any{
		"Name":   "different-name",
		"Format": "CSV",
		"Input":  map[string]any{"S3InputDefinition": map[string]any{"Bucket": "b"}},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
