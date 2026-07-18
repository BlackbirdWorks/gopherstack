package omics_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOmics_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a workflow to get an ARN
	rec := doRequest(t, h, http.MethodPost, "/workflow", map[string]any{"name": "tagged-wf", "engine": "WDL"})
	require.Equal(t, http.StatusCreated, rec.Code)

	var wfResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &wfResp))
	wfARN := wfResp["arn"].(string)

	// TagResource
	rec = doRequest(t, h, http.MethodPost, "/tags/"+wfARN, map[string]any{"tags": map[string]string{"env": "test"}})
	assert.Equal(t, http.StatusOK, rec.Code)

	// ListTagsForResource
	rec = doRequest(t, h, http.MethodGet, "/tags/"+wfARN, nil)
	assert.Equal(t, http.StatusOK, rec.Code)

	var tagsResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &tagsResp))
	tags := tagsResp["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
}

func TestOmics_RouteMatcher_TagPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "omics_tag_path", path: "/tags/arn:aws:omics:us-east-1:000000000000:workflow/12345", wantMatch: true},
		{
			name:      "fis_tag_path",
			path:      "/tags/arn:aws:fis:us-east-1:000000000000:experiment-template/EXTabcdef0123456",
			wantMatch: false,
		},
		{
			name:      "fis_tag_path_nonexistent",
			path:      "/tags/arn:aws:fis:us-east-1:000000000000:experiment-template/EXTdoesnotexist00000000",
			wantMatch: false,
		},
		{name: "other_tag_path", path: "/tags/arn:aws:s3:::my-bucket", wantMatch: false},
		{name: "tags_root", path: "/tags", wantMatch: false},
		{name: "workflow_path", path: "/workflow", wantMatch: true},
		{name: "referencestore_path", path: "/referencestore", wantMatch: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			matcher := h.RouteMatcher()
			assert.Equal(t, tt.wantMatch, matcher(c), "path: %s", tt.path)
		})
	}
}
