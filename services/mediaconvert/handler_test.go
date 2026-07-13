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

func TestMediaConvert_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "MediaConvert", h.Name())
}

func TestMediaConvert_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreateQueue")
	assert.Contains(t, ops, "GetQueue")
	assert.Contains(t, ops, "ListQueues")
	assert.Contains(t, ops, "UpdateQueue")
	assert.Contains(t, ops, "DeleteQueue")
	assert.Contains(t, ops, "CreateJobTemplate")
	assert.Contains(t, ops, "GetJobTemplate")
	assert.Contains(t, ops, "ListJobTemplates")
	assert.Contains(t, ops, "UpdateJobTemplate")
	assert.Contains(t, ops, "DeleteJobTemplate")
	assert.Contains(t, ops, "CreateJob")
	assert.Contains(t, ops, "GetJob")
	assert.Contains(t, ops, "ListJobs")
	assert.Contains(t, ops, "CancelJob")
	assert.Contains(t, ops, "DescribeEndpoints")
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

func TestMediaConvert_Queue_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queueName  string
		wantInBody string
		wantStatus int
	}{
		{
			name:       "create_queue",
			queueName:  "my-queue",
			wantStatus: http.StatusCreated,
			wantInBody: "my-queue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Create
			rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
				"name":        tt.queueName,
				"description": "test queue",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantInBody)
		})
	}
}

func TestMediaConvert_Queue_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	queueName := "test-queue"

	// Create queue
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name":        queueName,
		"description": "initial description",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	queueData, ok := createResp["queue"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, queueName, queueData["name"])
	assert.Equal(t, "ACTIVE", queueData["status"])
	assert.Equal(t, "ON_DEMAND", queueData["pricingPlan"])
	assert.Equal(t, "CUSTOM", queueData["type"])
	assert.NotEmpty(t, queueData["arn"])

	// Get queue
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/queues/"+queueName, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), queueName)

	// List queues
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/queues", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), queueName)

	// Update queue
	rec = doRequest(t, h, http.MethodPut, "/2017-08-29/queues/"+queueName, map[string]any{
		"description": "updated description",
		"status":      "PAUSED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&updateResp))
	updatedQueue, ok := updateResp["queue"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "PAUSED", updatedQueue["status"])

	// Delete queue
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/queues/"+queueName, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify deleted
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/queues/"+queueName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_Queue_DuplicateCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{"name": "dup-queue"})

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{"name": "dup-queue"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestMediaConvert_Queue_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/queues/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_Queue_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"description": "no name here",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaConvert_JobTemplate_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	templateName := "test-template"

	// Create
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"name":        templateName,
		"description": "a test template",
		"category":    "Standard",
		"priority":    0,
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	jtData, ok := createResp["jobTemplate"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, templateName, jtData["name"])
	assert.Equal(t, "CUSTOM", jtData["type"])
	assert.NotEmpty(t, jtData["arn"])

	// Get
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobTemplates/"+templateName, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), templateName)

	// List
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobTemplates", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), templateName)

	// Update
	priority := 5
	rec = doRequest(t, h, http.MethodPut, "/2017-08-29/jobTemplates/"+templateName, map[string]any{
		"description": "updated description",
		"priority":    &priority,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&updateResp))
	updatedJT, ok := updateResp["jobTemplate"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "updated description", updatedJT["description"])

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/jobTemplates/"+templateName, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify deleted
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobTemplates/"+templateName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_JobTemplate_DuplicateCreate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{"name": "dup-tpl"})

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{"name": "dup-tpl"})
	assert.Equal(t, http.StatusConflict, rec.Code)
}

func TestMediaConvert_JobTemplate_MissingName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobTemplates", map[string]any{
		"description": "no name",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaConvert_Job_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	role := "arn:aws:iam::123456789012:role/MediaConvert_Default_Role"

	// Create job
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"role": role,
		"settings": map[string]any{
			"outputGroups": []any{},
		},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	jobData, ok := createResp["job"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, role, jobData["role"])
	assert.Equal(t, "SUBMITTED", jobData["status"])
	assert.NotEmpty(t, jobData["id"])
	assert.NotEmpty(t, jobData["arn"])

	jobID, _ := jobData["id"].(string)

	// Get job
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobs/"+jobID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), jobID)

	// List jobs
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobs", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), jobID)

	// Cancel job
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/jobs/"+jobID, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify job is canceled (still exists but status changed)
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/jobs/"+jobID, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CANCELED")
}

func TestMediaConvert_Job_MissingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
		"queue": "arn:aws:mediaconvert:us-east-1:123456789012:queues/Default",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMediaConvert_Job_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobs/nonexistent-id", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_DescribeEndpoints(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/endpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	endpoints, ok := out["endpoints"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, endpoints)
	ep, ok := endpoints[0].(map[string]any)
	require.True(t, ok)
	assert.NotEmpty(t, ep["url"])
	assert.NotContains(t, ep["url"], "/2017-08-29", "endpoint URL should be the base host without path prefix")
}

func TestMediaConvert_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/unknown-path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_ListQueues_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/queues", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	queues, ok := out["queues"].([]any)
	require.True(t, ok)
	assert.Empty(t, queues)
}

func TestMediaConvert_DeleteQueue_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodDelete, "/2017-08-29/queues/nonexistent", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_UpdateQueue_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPut, "/2017-08-29/queues/nonexistent", map[string]any{
		"status": "PAUSED",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestMediaConvert_UpdateQueue_ConcurrentJobsAndReservationPlanSettings verifies
// that UpdateQueue applies concurrentJobs and reservationPlanSettings -- real
// UpdateQueueInput members that were previously silently dropped.
func TestMediaConvert_UpdateQueue_ConcurrentJobsAndReservationPlanSettings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/queues", map[string]any{
		"name": "update-queue-fields",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	rec = doRequest(t, h, http.MethodPut, "/2017-08-29/queues/update-queue-fields", map[string]any{
		"concurrentJobs": 7,
		"reservationPlanSettings": map[string]any{
			"commitment":    "ONE_YEAR",
			"renewalType":   "AUTO_RENEW",
			"reservedSlots": 4,
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	queueData := out["queue"].(map[string]any)
	assert.InDelta(t, float64(7), queueData["concurrentJobs"], 0)

	rp, ok := queueData["reservationPlan"].(map[string]any)
	require.True(t, ok, "reservationPlan should be in response")
	assert.InDelta(t, float64(4), rp["reservedSlots"], 0)
	assert.Equal(t, "ONE_YEAR", rp["commitment"])
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestMediaConvert_TagLeakOnDeleteQueue(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tags_cleaned_on_delete_queue"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

			q, err := b.CreateQueue("my-queue", "", "", "", nil)
			require.NoError(t, err)

			b.TagResource(q.Arn, map[string]string{"env": "test"})
			assert.Equal(t, "test", b.GetTags(q.Arn)["env"])

			require.NoError(t, b.DeleteQueue("my-queue"))

			// Tags must be gone after deletion.
			assert.Empty(t, b.GetTags(q.Arn))
		})
	}
}

func TestMediaConvert_TagLeakOnDeleteJobTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "tags_cleaned_on_delete_job_template"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

			jt, err := b.CreateJobTemplate("my-template", "desc", "", "", 0, nil, nil)
			require.NoError(t, err)

			b.TagResource(jt.Arn, map[string]string{"team": "infra"})
			assert.Equal(t, "infra", b.GetTags(jt.Arn)["team"])

			require.NoError(t, b.DeleteJobTemplate("my-template"))

			// Tags must be gone after deletion.
			assert.Empty(t, b.GetTags(jt.Arn))
		})
	}
}

func TestMediaConvert_UntagResource_RemovesEntryOnLastKey(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(b *mediaconvert.InMemoryBackend, arn string)
		name     string
		tagKeys  []string
		wantZero bool
	}{
		{
			name: "remove_last_key_cleans_entry",
			setup: func(b *mediaconvert.InMemoryBackend, a string) {
				b.TagResource(a, map[string]string{"only-key": "v1"})
			},
			tagKeys:  []string{"only-key"},
			wantZero: true,
		},
		{
			name: "partial_untag_keeps_entry",
			setup: func(b *mediaconvert.InMemoryBackend, a string) {
				b.TagResource(a, map[string]string{"key1": "v1", "key2": "v2"})
			},
			tagKeys:  []string{"key1"},
			wantZero: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)

			q, err := b.CreateQueue("untag-queue-"+tt.name, "", "", "", nil)
			require.NoError(t, err)

			tt.setup(b, q.Arn)
			b.UntagResource(q.Arn, tt.tagKeys)

			remaining := b.GetTags(q.Arn)
			if tt.wantZero {
				assert.Empty(t, remaining)
			} else {
				assert.NotEmpty(t, remaining)
			}
		})
	}
}

// --- New operations tests ---

func TestMediaConvert_Preset_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	presetName := "test-preset"

	// Create
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/presets", map[string]any{
		"name":        presetName,
		"description": "a test preset",
		"category":    "Standard",
		"settings":    map[string]any{"codec": "H.264"},
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&createResp))
	pData, ok := createResp["preset"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, presetName, pData["name"])
	assert.Equal(t, "CUSTOM", pData["type"])
	assert.NotEmpty(t, pData["arn"])

	// Get
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/presets/"+presetName, nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), presetName)

	// List
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/presets", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), presetName)

	// Delete
	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/presets/"+presetName, nil)
	assert.Equal(t, http.StatusNoContent, rec.Code)

	// Verify deleted
	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/presets/"+presetName, nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_Preset_TableTests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       any
		setup      func(h *mediaconvert.Handler)
		name       string
		method     string
		path       string
		wantInBody string
		wantStatus int
	}{
		{
			name:       "create_preset_missing_name",
			method:     http.MethodPost,
			path:       "/2017-08-29/presets",
			body:       map[string]any{"description": "no name"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "create_preset_duplicate",
			setup: func(h *mediaconvert.Handler) {
				doRequest(t, h, http.MethodPost, "/2017-08-29/presets", map[string]any{"name": "dup-preset"})
			},
			method:     http.MethodPost,
			path:       "/2017-08-29/presets",
			body:       map[string]any{"name": "dup-preset"},
			wantStatus: http.StatusConflict,
		},
		{
			name:       "get_preset_not_found",
			method:     http.MethodGet,
			path:       "/2017-08-29/presets/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete_preset_not_found",
			method:     http.MethodDelete,
			path:       "/2017-08-29/presets/nonexistent",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "list_presets_empty",
			method:     http.MethodGet,
			path:       "/2017-08-29/presets",
			wantStatus: http.StatusOK,
			wantInBody: `"presets"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

func TestMediaConvert_Policy_TableTests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(b *mediaconvert.InMemoryBackend)
		name       string
		method     string
		path       string
		wantInBody string
		wantStatus int
	}{
		{
			name:       "get_policy_not_found",
			method:     http.MethodGet,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "get_policy_exists",
			setup: func(b *mediaconvert.InMemoryBackend) {
				b.PutPolicy("ALLOWED", "ALLOWED", "ALLOWED")
			},
			method:     http.MethodGet,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusOK,
			wantInBody: "policy",
		},
		{
			name:       "delete_policy_not_found",
			method:     http.MethodDelete,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusNotFound,
		},
		{
			name: "delete_policy_exists",
			setup: func(b *mediaconvert.InMemoryBackend) {
				b.PutPolicy("ALLOWED", "ALLOWED", "ALLOWED")
			},
			method:     http.MethodDelete,
			path:       "/2017-08-29/policy",
			wantStatus: http.StatusNoContent,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			if tt.setup != nil {
				tt.setup(b)
			}
			h := mediaconvert.NewHandler(b)

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantInBody != "" {
				assert.Contains(t, rec.Body.String(), tt.wantInBody)
			}
		})
	}
}

func TestMediaConvert_Policy_DeleteAfterGet(t *testing.T) {
	t.Parallel()

	b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
	b.PutPolicy("ALLOWED", "DISALLOWED", "ALLOWED")
	h := mediaconvert.NewHandler(b)

	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/policy", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	policy, ok := out["policy"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "ALLOWED", policy["httpInputs"])
	assert.Equal(t, "DISALLOWED", policy["httpsInputs"])

	rec = doRequest(t, h, http.MethodDelete, "/2017-08-29/policy", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	rec = doRequest(t, h, http.MethodGet, "/2017-08-29/policy", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestMediaConvert_Certificate_TableTests(t *testing.T) {
	t.Parallel()

	const certARN = "arn:aws:acm:us-east-1:123456789012:certificate/abc123"

	tests := []struct {
		body       any
		setup      func(b *mediaconvert.InMemoryBackend)
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "associate_certificate",
			method:     http.MethodPost,
			path:       "/2017-08-29/certificates",
			body:       map[string]any{"arn": certARN},
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "associate_certificate_missing_arn",
			method:     http.MethodPost,
			path:       "/2017-08-29/certificates",
			body:       map[string]any{"description": "no arn"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "associate_certificate_duplicate",
			setup: func(b *mediaconvert.InMemoryBackend) {
				require.NoError(t, b.AssociateCertificate(certARN))
			},
			method:     http.MethodPost,
			path:       "/2017-08-29/certificates",
			body:       map[string]any{"arn": certARN},
			wantStatus: http.StatusConflict,
		},
		{
			name: "disassociate_certificate",
			setup: func(b *mediaconvert.InMemoryBackend) {
				require.NoError(t, b.AssociateCertificate(certARN))
			},
			method:     http.MethodDelete,
			path:       "/2017-08-29/certificates/" + certARN,
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "disassociate_certificate_not_found",
			method:     http.MethodDelete,
			path:       "/2017-08-29/certificates/" + certARN,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := mediaconvert.NewInMemoryBackend(testAccountID, testRegion)
			if tt.setup != nil {
				tt.setup(b)
			}
			h := mediaconvert.NewHandler(b)

			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestMediaConvert_GetJobsQueryResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		queryID    string
		wantStatus int
		wantJobs   bool
	}{
		{
			name:       "returns_empty_results_for_any_id",
			queryID:    "query-abc-123",
			wantStatus: http.StatusOK,
			wantJobs:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodGet, "/2017-08-29/jobsQueries/"+tt.queryID, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantJobs {
				var out map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
				_, ok := out["jobs"]
				assert.True(t, ok, "response should contain 'jobs' key")
			}
		})
	}
}

func TestMediaConvert_CreateResourceShare_TableTests(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *mediaconvert.Handler) string
		body       func(jobID string) any
		name       string
		wantStatus int
	}{
		{
			name: "create_resource_share_valid_job",
			setup: func(h *mediaconvert.Handler) string {
				rec := doRequest(t, h, http.MethodPost, "/2017-08-29/jobs", map[string]any{
					"role": "arn:aws:iam::123456789012:role/MediaConvert_Role",
				})
				require.Equal(t, http.StatusCreated, rec.Code)
				var resp map[string]any
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&resp))
				job, _ := resp["job"].(map[string]any)
				id, _ := job["id"].(string)

				return id
			},
			body:       func(jobID string) any { return map[string]any{"jobId": jobID} },
			wantStatus: http.StatusNoContent,
		},
		{
			name:       "create_resource_share_missing_job_id",
			setup:      func(_ *mediaconvert.Handler) string { return "" },
			body:       func(_ string) any { return map[string]any{"description": "no job id"} },
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create_resource_share_job_not_found",
			setup:      func(_ *mediaconvert.Handler) string { return "nonexistent-job" },
			body:       func(jobID string) any { return map[string]any{"jobId": jobID} },
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			jobID := tt.setup(h)
			rec := doRequest(t, h, http.MethodPost, "/2017-08-29/resourceShares", tt.body(jobID))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestMediaConvert_GetSupportedOperations_NewOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	wantOps := []string{
		"AssociateCertificate",
		"CreatePreset",
		"CreateResourceShare",
		"DeletePolicy",
		"DeletePreset",
		"DisassociateCertificate",
		"GetJobsQueryResults",
		"GetPolicy",
		"GetPreset",
		"ListPresets",
	}

	for _, op := range wantOps {
		assert.Contains(t, ops, op, "GetSupportedOperations should include %s", op)
	}
}
