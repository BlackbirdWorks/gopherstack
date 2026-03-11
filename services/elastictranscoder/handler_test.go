package elastictranscoder_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/elastictranscoder"
)

const (
	testAccountID = "123456789012"
	testRegion    = "us-east-1"
)

func newTestHandler(t *testing.T) *elastictranscoder.Handler {
	t.Helper()

	return elastictranscoder.NewHandler(elastictranscoder.NewInMemoryBackend(testAccountID, testRegion))
}

func doRequest(t *testing.T, h *elastictranscoder.Handler, method, path string, body any) *httptest.ResponseRecorder {
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

func TestElasticTranscoder_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ElasticTranscoder", h.Name())
}

func TestElasticTranscoder_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.NotEmpty(t, ops)
	assert.Contains(t, ops, "CreatePipeline")
	assert.Contains(t, ops, "ReadPipeline")
	assert.Contains(t, ops, "ListPipelines")
	assert.Contains(t, ops, "UpdatePipeline")
	assert.Contains(t, ops, "DeletePipeline")
	assert.Contains(t, ops, "CreatePreset")
	assert.Contains(t, ops, "ReadPreset")
	assert.Contains(t, ops, "ListPresets")
	assert.Contains(t, ops, "DeletePreset")
	assert.Contains(t, ops, "CreateJob")
	assert.Contains(t, ops, "ReadJob")
	assert.Contains(t, ops, "ListJobsByPipeline")
	assert.Contains(t, ops, "CancelJob")
}

func TestElasticTranscoder_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 85, h.MatchPriority())
}

func TestElasticTranscoder_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
		want bool
	}{
		{
			name: "pipelines",
			path: "/2012-09-25/pipelines",
			want: true,
		},
		{
			name: "pipeline by id",
			path: "/2012-09-25/pipelines/abc-123",
			want: true,
		},
		{
			name: "presets",
			path: "/2012-09-25/presets",
			want: true,
		},
		{
			name: "jobs",
			path: "/2012-09-25/jobs",
			want: true,
		},
		{
			name: "other path",
			path: "/v1/jobs",
			want: false,
		},
		{
			name: "dashboard path",
			path: "/dashboard/elastictranscoder",
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

func TestElasticTranscoder_Pipeline_CRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupInput map[string]string
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name: "create pipeline",
			setupInput: map[string]string{
				"Name":        "test-pipeline",
				"InputBucket": "my-bucket",
				"Role":        "arn:aws:iam::123:role/et",
			},
			wantStatus: http.StatusCreated,
			wantField:  "test-pipeline",
		},
		{
			name:       "missing name returns 400",
			setupInput: map[string]string{"InputBucket": "my-bucket", "Role": "arn:aws:iam::123:role/et"},
			wantStatus: http.StatusBadRequest,
			wantField:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", tt.setupInput)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.Contains(t, rec.Body.String(), tt.wantField)
			}
		})
	}
}

func TestElasticTranscoder_Pipeline_ReadAndList(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pipeline first.
	createRec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
		"Name": "test-pipeline", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createOut struct {
		Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	pipelineID := createOut.Pipeline.ID
	require.NotEmpty(t, pipelineID)

	tests := []struct {
		name       string
		method     string
		path       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "read pipeline",
			method:     http.MethodGet,
			path:       "/2012-09-25/pipelines/" + pipelineID,
			wantStatus: http.StatusOK,
			wantField:  "test-pipeline",
		},
		{
			name:       "read non-existent pipeline",
			method:     http.MethodGet,
			path:       "/2012-09-25/pipelines/nonexistent",
			wantStatus: http.StatusNotFound,
			wantField:  "",
		},
		{
			name:       "list pipelines",
			method:     http.MethodGet,
			path:       "/2012-09-25/pipelines",
			wantStatus: http.StatusOK,
			wantField:  "test-pipeline",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.Contains(t, rec.Body.String(), tt.wantField)
			}
		})
	}
}

//nolint:paralleltest,tparallel // subtests share a stateful handler and must run sequentially
func TestElasticTranscoder_Pipeline_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pipeline.
	createRec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
		"Name": "to-update", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createOut struct {
		Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	id := createOut.Pipeline.ID

	tests := []struct {
		body       any
		name       string
		method     string
		path       string
		wantStatus int
	}{
		{
			name:       "update pipeline",
			method:     http.MethodPut,
			path:       "/2012-09-25/pipelines/" + id,
			body:       map[string]string{"Name": "updated-name"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete pipeline",
			method:     http.MethodDelete,
			path:       "/2012-09-25/pipelines/" + id,
			wantStatus: http.StatusAccepted,
		},
		{
			name:       "delete non-existent pipeline",
			method:     http.MethodDelete,
			path:       "/2012-09-25/pipelines/nonexistent",
			wantStatus: http.StatusNotFound,
		},
	}

	// Run sequentially since the delete test depends on the update happening first.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, h, tt.method, tt.path, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestElasticTranscoder_Preset_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		input      map[string]string
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "create preset",
			input:      map[string]string{"Name": "my-preset", "Container": "mp4"},
			wantStatus: http.StatusCreated,
			wantField:  "my-preset",
		},
		{
			name:       "missing container",
			input:      map[string]string{"Name": "bad-preset"},
			wantStatus: http.StatusBadRequest,
			wantField:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, "/2012-09-25/presets", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.Contains(t, rec.Body.String(), tt.wantField)
			}
		})
	}
}

//nolint:paralleltest,tparallel // subtests share a stateful handler and must run sequentially
func TestElasticTranscoder_Preset_ReadListDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a preset.
	createRec := doRequest(t, h, http.MethodPost, "/2012-09-25/presets", map[string]string{
		"Name": "test-preset", "Container": "mp4",
	})
	require.Equal(t, http.StatusCreated, createRec.Code)

	var createOut struct {
		Preset elastictranscoder.Preset `json:"Preset"`
	}
	require.NoError(t, json.NewDecoder(createRec.Body).Decode(&createOut))
	id := createOut.Preset.ID

	tests := []struct {
		name       string
		method     string
		path       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "read preset",
			method:     http.MethodGet,
			path:       "/2012-09-25/presets/" + id,
			wantStatus: http.StatusOK,
			wantField:  "test-preset",
		},
		{
			name:       "read non-existent preset",
			method:     http.MethodGet,
			path:       "/2012-09-25/presets/missing",
			wantStatus: http.StatusNotFound,
			wantField:  "",
		},
		{
			name:       "list presets",
			method:     http.MethodGet,
			path:       "/2012-09-25/presets",
			wantStatus: http.StatusOK,
			wantField:  "test-preset",
		},
		{
			name:       "delete preset",
			method:     http.MethodDelete,
			path:       "/2012-09-25/presets/" + id,
			wantStatus: http.StatusAccepted,
			wantField:  "",
		},
	}

	// Run sequentially since tests share the same handler and depend on state order.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.Contains(t, rec.Body.String(), tt.wantField)
			}
		})
	}
}

func TestElasticTranscoder_Job_CRUD(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pipeline first.
	createPipelineRec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
		"Name": "job-pipeline", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
	})
	require.Equal(t, http.StatusCreated, createPipelineRec.Code)

	var pipelineOut struct {
		Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(createPipelineRec.Body).Decode(&pipelineOut))
	pipelineID := pipelineOut.Pipeline.ID

	tests := []struct {
		input      map[string]string
		name       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "create job",
			input:      map[string]string{"PipelineId": pipelineID},
			wantStatus: http.StatusCreated,
			wantField:  pipelineID,
		},
		{
			name:       "create job for non-existent pipeline",
			input:      map[string]string{"PipelineId": "nonexistent"},
			wantStatus: http.StatusNotFound,
			wantField:  "",
		},
		{
			name:       "missing pipeline id",
			input:      map[string]string{},
			wantStatus: http.StatusBadRequest,
			wantField:  "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, http.MethodPost, "/2012-09-25/jobs", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.Contains(t, rec.Body.String(), tt.wantField)
			}
		})
	}
}

//nolint:paralleltest,tparallel // subtests share a stateful handler and must run sequentially
func TestElasticTranscoder_Job_ReadListCancel(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create pipeline and job.
	createPipelineRec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
		"Name": "my-pipeline", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
	})
	require.Equal(t, http.StatusCreated, createPipelineRec.Code)

	var pipelineOut struct {
		Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
	}
	require.NoError(t, json.NewDecoder(createPipelineRec.Body).Decode(&pipelineOut))
	pipelineID := pipelineOut.Pipeline.ID

	createJobRec := doRequest(t, h, http.MethodPost, "/2012-09-25/jobs", map[string]string{
		"PipelineId": pipelineID,
	})
	require.Equal(t, http.StatusCreated, createJobRec.Code)

	var jobOut struct {
		Job elastictranscoder.Job `json:"Job"`
	}
	require.NoError(t, json.NewDecoder(createJobRec.Body).Decode(&jobOut))
	jobID := jobOut.Job.ID

	tests := []struct {
		name       string
		method     string
		path       string
		wantField  string
		wantStatus int
	}{
		{
			name:       "read job",
			method:     http.MethodGet,
			path:       "/2012-09-25/jobs/" + jobID,
			wantStatus: http.StatusOK,
			wantField:  pipelineID,
		},
		{
			name:       "read non-existent job",
			method:     http.MethodGet,
			path:       "/2012-09-25/jobs/nonexistent",
			wantStatus: http.StatusNotFound,
			wantField:  "",
		},
		{
			name:       "list jobs by pipeline",
			method:     http.MethodGet,
			path:       "/2012-09-25/jobsByPipeline/" + pipelineID,
			wantStatus: http.StatusOK,
			wantField:  pipelineID,
		},
		{
			name:       "cancel job",
			method:     http.MethodDelete,
			path:       "/2012-09-25/jobs/" + jobID,
			wantStatus: http.StatusAccepted,
			wantField:  "",
		},
		{
			name:       "cancel non-existent job",
			method:     http.MethodDelete,
			path:       "/2012-09-25/jobs/nonexistent",
			wantStatus: http.StatusNotFound,
			wantField:  "",
		},
	}

	// Run sequentially since cancel_job removes the job from the store.
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rec := doRequest(t, h, tt.method, tt.path, nil)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantField != "" {
				assert.Contains(t, rec.Body.String(), tt.wantField)
			}
		})
	}
}

func TestElasticTranscoder_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/2012-09-25/unknown-endpoint", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestElasticTranscoder_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
		want   string
	}{
		{
			name:   "list pipelines",
			method: http.MethodGet,
			path:   "/2012-09-25/pipelines",
			want:   "ListPipelines",
		},
		{
			name:   "create pipeline",
			method: http.MethodPost,
			path:   "/2012-09-25/pipelines",
			want:   "CreatePipeline",
		},
		{
			name:   "read pipeline",
			method: http.MethodGet,
			path:   "/2012-09-25/pipelines/abc123",
			want:   "ReadPipeline",
		},
		{
			name:   "list jobs by pipeline",
			method: http.MethodGet,
			path:   "/2012-09-25/jobsByPipeline/abc123",
			want:   "ListJobsByPipeline",
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

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.want, op)
		})
	}
}

func TestElasticTranscoder_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "elastictranscoder", h.ChaosServiceName())
}

func TestElasticTranscoder_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, testRegion, regions[0])
}
