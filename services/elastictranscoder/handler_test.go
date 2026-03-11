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

func TestElasticTranscoder_Pipeline_UpdateAndDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(t *testing.T, h *elastictranscoder.Handler) string
		body       any
		name       string
		method     string
		wantStatus int
	}{
		{
			name: "update pipeline",
			setup: func(t *testing.T, h *elastictranscoder.Handler) string {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
					"Name": "to-update", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				var out struct {
					Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

				return "/2012-09-25/pipelines/" + out.Pipeline.ID
			},
			body:       map[string]string{"Name": "updated-name"},
			method:     http.MethodPut,
			wantStatus: http.StatusOK,
		},
		{
			name: "delete pipeline",
			setup: func(t *testing.T, h *elastictranscoder.Handler) string {
				t.Helper()
				rec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
					"Name": "to-delete", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
				})
				require.Equal(t, http.StatusCreated, rec.Code)

				var out struct {
					Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

				return "/2012-09-25/pipelines/" + out.Pipeline.ID
			},
			method:     http.MethodDelete,
			wantStatus: http.StatusAccepted,
		},
		{
			name: "delete non-existent pipeline",
			setup: func(t *testing.T, _ *elastictranscoder.Handler) string {
				t.Helper()

				return "/2012-09-25/pipelines/nonexistent"
			},
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := tt.setup(t, h)
			rec := doRequest(t, h, tt.method, path, tt.body)
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

func TestElasticTranscoder_Preset_ReadListDelete(t *testing.T) {
	t.Parallel()

	createPreset := func(t *testing.T, h *elastictranscoder.Handler) string {
		t.Helper()
		rec := doRequest(t, h, http.MethodPost, "/2012-09-25/presets", map[string]string{
			"Name": "test-preset", "Container": "mp4",
		})
		require.Equal(t, http.StatusCreated, rec.Code)

		var out struct {
			Preset elastictranscoder.Preset `json:"Preset"`
		}
		require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))

		return out.Preset.ID
	}

	tests := []struct {
		setup      func(t *testing.T, h *elastictranscoder.Handler) string
		name       string
		method     string
		wantField  string
		wantStatus int
	}{
		{
			name: "read preset",
			setup: func(t *testing.T, h *elastictranscoder.Handler) string {
				t.Helper()

				return "/2012-09-25/presets/" + createPreset(t, h)
			},
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantField:  "test-preset",
		},
		{
			name: "read non-existent preset",
			setup: func(t *testing.T, _ *elastictranscoder.Handler) string {
				t.Helper()

				return "/2012-09-25/presets/missing"
			},
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name: "list presets",
			setup: func(t *testing.T, h *elastictranscoder.Handler) string {
				t.Helper()
				createPreset(t, h)

				return "/2012-09-25/presets"
			},
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
			wantField:  "test-preset",
		},
		{
			name: "delete preset",
			setup: func(t *testing.T, h *elastictranscoder.Handler) string {
				t.Helper()

				return "/2012-09-25/presets/" + createPreset(t, h)
			},
			method:     http.MethodDelete,
			wantStatus: http.StatusAccepted,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			path := tt.setup(t, h)
			rec := doRequest(t, h, tt.method, path, nil)
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

func TestElasticTranscoder_Job_ReadListCancel(t *testing.T) {
	t.Parallel()

	type ids struct {
		pipelineID string
		jobID      string
	}

	createPipelineAndJob := func(t *testing.T, h *elastictranscoder.Handler) ids {
		t.Helper()
		pRec := doRequest(t, h, http.MethodPost, "/2012-09-25/pipelines", map[string]string{
			"Name": "my-pipeline", "InputBucket": "bucket", "Role": "arn:aws:iam::123:role/et",
		})
		require.Equal(t, http.StatusCreated, pRec.Code)

		var pOut struct {
			Pipeline elastictranscoder.Pipeline `json:"Pipeline"`
		}
		require.NoError(t, json.NewDecoder(pRec.Body).Decode(&pOut))

		jRec := doRequest(t, h, http.MethodPost, "/2012-09-25/jobs", map[string]string{"PipelineId": pOut.Pipeline.ID})
		require.Equal(t, http.StatusCreated, jRec.Code)

		var jOut struct {
			Job elastictranscoder.Job `json:"Job"`
		}
		require.NoError(t, json.NewDecoder(jRec.Body).Decode(&jOut))

		return ids{pipelineID: pOut.Pipeline.ID, jobID: jOut.Job.ID}
	}

	tests := []struct {
		setup      func(t *testing.T, h *elastictranscoder.Handler) ids
		name       string
		method     string
		pathFn     func(i ids) string
		wantField  string
		wantStatus int
	}{
		{
			name: "read job",
			setup: func(t *testing.T, h *elastictranscoder.Handler) ids {
				t.Helper()

				return createPipelineAndJob(t, h)
			},
			pathFn:     func(i ids) string { return "/2012-09-25/jobs/" + i.jobID },
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name: "read non-existent job",
			setup: func(t *testing.T, _ *elastictranscoder.Handler) ids {
				t.Helper()

				return ids{}
			},
			pathFn:     func(_ ids) string { return "/2012-09-25/jobs/nonexistent" },
			method:     http.MethodGet,
			wantStatus: http.StatusNotFound,
		},
		{
			name: "list jobs by pipeline",
			setup: func(t *testing.T, h *elastictranscoder.Handler) ids {
				t.Helper()

				return createPipelineAndJob(t, h)
			},
			pathFn:     func(i ids) string { return "/2012-09-25/jobsByPipeline/" + i.pipelineID },
			method:     http.MethodGet,
			wantStatus: http.StatusOK,
		},
		{
			name: "cancel job",
			setup: func(t *testing.T, h *elastictranscoder.Handler) ids {
				t.Helper()

				return createPipelineAndJob(t, h)
			},
			pathFn:     func(i ids) string { return "/2012-09-25/jobs/" + i.jobID },
			method:     http.MethodDelete,
			wantStatus: http.StatusAccepted,
		},
		{
			name: "cancel non-existent job",
			setup: func(t *testing.T, _ *elastictranscoder.Handler) ids {
				t.Helper()

				return ids{}
			},
			pathFn:     func(_ ids) string { return "/2012-09-25/jobs/nonexistent" },
			method:     http.MethodDelete,
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			i := tt.setup(t, h)
			path := tt.pathFn(i)
			rec := doRequest(t, h, tt.method, path, nil)
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
