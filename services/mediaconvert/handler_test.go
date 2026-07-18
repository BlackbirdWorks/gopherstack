package mediaconvert_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *mediaconvert.Handler {
	t.Helper()

	return mediaconvert.NewHandler(mediaconvert.NewInMemoryBackend(testAccountID, testRegion))
}

func doRequest(t *testing.T, h *mediaconvert.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var reqBody *bytes.Reader
	if body != nil {
		bodyBytes, err := json.Marshal(body)
		require.NoError(t, err)
		reqBody = bytes.NewReader(bodyBytes)
	} else {
		reqBody = bytes.NewReader(nil)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, reqBody)

	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// extractOp returns the operation name ExtractOperation resolves for the given method+path.
func extractOp(h *mediaconvert.Handler, method, path string) string {
	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return h.ExtractOperation(c)
}

// parseJSONResponse issues the request and decodes the JSON body into a map, alongside the status code.
func parseJSONResponse(t *testing.T, h *mediaconvert.Handler, method, path string, body any) (map[string]any, int) {
	t.Helper()

	rec := doRequest(t, h, method, path, body)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	}

	return resp, rec.Code
}

func TestMediaConvert_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MediaConvert", h.Name())
}

func TestMediaConvert_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.GreaterOrEqual(t, len(ops), 29)

	wantOps := []string{
		"CreateQueue", "GetQueue", "ListQueues", "UpdateQueue", "DeleteQueue",
		"CreateJobTemplate", "GetJobTemplate", "ListJobTemplates", "UpdateJobTemplate", "DeleteJobTemplate",
		"CreateJob", "GetJob", "ListJobs", "CancelJob", "UpdateJob", "DescribeEndpoints",
		"AssociateCertificate", "CreatePreset", "CreateResourceShare", "DeletePolicy", "DeletePreset",
		"DisassociateCertificate", "GetJobsQueryResults", "GetPolicy", "GetPreset", "ListPresets",
		"PutPolicy", "UpdatePreset",
	}

	for _, op := range wantOps {
		assert.Contains(t, ops, op, "GetSupportedOperations should include %s", op)
	}
}

// TestHandlerOpsLen ensures a minimum set of operations is registered.
func TestHandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	require.GreaterOrEqual(t, mediaconvert.HandlerOpsLen(h), 28)
}

func TestMediaConvert_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 85, h.MatchPriority())
}

func TestMediaConvert_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "queues path",
			path: "/2017-08-29/queues",
			want: true,
		},
		{
			name: "queue by name",
			path: "/2017-08-29/queues/Default",
			want: true,
		},
		{
			name: "jobTemplates path",
			path: "/2017-08-29/jobTemplates",
			want: true,
		},
		{
			name: "jobs path",
			path: "/2017-08-29/jobs",
			want: true,
		},
		{
			name: "endpoints path",
			path: "/2017-08-29/endpoints",
			want: true,
		},
		{
			name: "other path",
			path: "/v1/queues",
			want: false,
		},
		{
			name: "dashboard path",
			path: "/dashboard/mediaconvert",
			want: false,
		},
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
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestMediaConvert_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.NotEmpty(t, h.ChaosOperations())
	assert.Equal(t, "mediaconvert", h.ChaosServiceName())
	assert.Equal(t, []string{testRegion}, h.ChaosRegions())
}

func TestMediaConvert_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "list_queues",
			method: http.MethodGet,
			path:   "/2017-08-29/queues",
			wantOp: "ListQueues",
		},
		{
			name:   "create_queue",
			method: http.MethodPost,
			path:   "/2017-08-29/queues",
			wantOp: "CreateQueue",
		},
		{
			name:   "get_queue",
			method: http.MethodGet,
			path:   "/2017-08-29/queues/MyQueue",
			wantOp: "GetQueue",
		},
		{
			name:   "list_jobs",
			method: http.MethodGet,
			path:   "/2017-08-29/jobs",
			wantOp: "ListJobs",
		},
		{
			name:   "create_job",
			method: http.MethodPost,
			path:   "/2017-08-29/jobs",
			wantOp: "CreateJob",
		},
		{
			name:   "list_job_templates",
			method: http.MethodGet,
			path:   "/2017-08-29/jobTemplates",
			wantOp: "ListJobTemplates",
		},
		{
			// Real MediaConvert TagResource is POST /2017-08-29/tags with
			// the ARN in the JSON body, not the URL.
			name:   "tag_resource",
			method: http.MethodPost,
			path:   "/2017-08-29/tags",
			wantOp: "TagResource",
		},
		{
			name:   "list_tags_for_resource",
			method: http.MethodGet,
			path:   "/2017-08-29/tags/arn:aws:mediaconvert:us-east-1:123456789012:queues/q1",
			wantOp: "ListTagsForResource",
		},
		{
			// Real MediaConvert UntagResource is PUT, not DELETE.
			name:   "untag_resource",
			method: http.MethodPut,
			path:   "/2017-08-29/tags/arn:aws:mediaconvert:us-east-1:123456789012:queues/q1",
			wantOp: "UntagResource",
		},
		{
			// DELETE on the tags path has no meaning in the real API.
			name:   "delete_tags_path_is_unknown",
			method: http.MethodDelete,
			path:   "/2017-08-29/tags/arn:aws:mediaconvert:us-east-1:123456789012:queues/q1",
			wantOp: "Unknown",
		},
		{
			name:   "update_job",
			method: http.MethodPut,
			path:   "/2017-08-29/jobs/job-123",
			wantOp: "UpdateJob",
		},
		{
			name:   "put_policy",
			method: http.MethodPut,
			path:   "/2017-08-29/policy",
			wantOp: "PutPolicy",
		},
		{
			name:   "get_policy",
			method: http.MethodGet,
			path:   "/2017-08-29/policy",
			wantOp: "GetPolicy",
		},
		{
			name:   "delete_policy",
			method: http.MethodDelete,
			path:   "/2017-08-29/policy",
			wantOp: "DeletePolicy",
		},
		{
			name:   "update_preset",
			method: http.MethodPut,
			path:   "/2017-08-29/presets/my-preset",
			wantOp: "UpdatePreset",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.wantOp, extractOp(h, tt.method, tt.path))
		})
	}
}

// TestErrValidationMappedTo400 verifies ErrValidation is mapped to HTTP 400
// regardless of which operation surfaces it.
func TestErrValidationMappedTo400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body   any
		name   string
		method string
		path   string
	}{
		{
			name:   "create_queue_invalid_status",
			method: http.MethodPut,
			path:   "/2017-08-29/queues/test-q-for-valid",
			body:   map[string]any{"status": "INVALID_STATUS"},
		},
		{
			name:   "cancel_job_already_canceled",
			method: http.MethodDelete,
			path:   "/2017-08-29/jobs/placeholder", // set up below
			body:   nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "cancel_job_already_canceled" {
				// Create and cancel a job first, then attempt double cancel.
				rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs",
					map[string]any{"role": "arn:aws:iam::123:role/role"})
				require.Equal(t, http.StatusCreated, rec.Code)

				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				job := resp["job"].(map[string]any)
				id := job["id"].(string)

				// First cancel succeeds.
				rec2 := doRequest(t, h, http.MethodDelete, "/2017-08-29/jobs/"+id, nil)
				require.Equal(t, http.StatusNoContent, rec2.Code)

				// Second cancel should fail with 400.
				rec3 := doRequest(t, h, http.MethodDelete, "/2017-08-29/jobs/"+id, nil)
				assert.Equal(t, http.StatusBadRequest, rec3.Code)

				return
			}

			if tt.name == "create_queue_invalid_status" {
				// Queue must exist for UpdateQueue to reach the status validation.
				doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{"name": "test-q-for-valid"})
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// TestListHandlers_ReturnEmptySlices verifies all list operations return empty slices (not nil).
func TestListHandlers_ReturnEmptySlices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		key  string
	}{
		{name: "list_queues", path: "/2017-08-29/queues", key: "queues"},
		{name: "list_job_templates", path: "/2017-08-29/jobTemplates", key: "jobTemplates"},
		{name: "list_jobs", path: "/2017-08-29/jobs", key: "jobs"},
		{name: "list_presets", path: "/2017-08-29/presets", key: "presets"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, tt.path, nil)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))

			list, ok := resp[tt.key]
			require.True(t, ok, "key %q missing from response", tt.key)
			// JSON unmarshals empty arrays as []any{}, not nil.
			arr, ok := list.([]any)
			require.True(t, ok)
			assert.Empty(t, arr)
		})
	}
}
