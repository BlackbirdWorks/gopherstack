package codepipeline_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/codepipeline"
)

func newTestHandler(t *testing.T) *codepipeline.Handler {
	t.Helper()

	return codepipeline.NewHandler(codepipeline.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *codepipeline.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte
	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	} else {
		bodyBytes = []byte("{}")
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "CodePipeline_20150709."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func samplePipeline(name string) codepipeline.PipelineDeclaration {
	return codepipeline.PipelineDeclaration{
		Name:    name,
		RoleArn: "arn:aws:iam::000000000000:role/pipeline-role",
		ArtifactStore: codepipeline.ArtifactStore{
			Type:     "S3",
			Location: "my-artifact-bucket",
		},
		Stages: []codepipeline.Stage{
			{
				Name: "Source",
				Actions: []codepipeline.Action{
					{
						Name: "SourceAction",
						ActionTypeID: codepipeline.ActionTypeID{
							Category: "Source",
							Owner:    "ThirdParty",
							Provider: "GitHub",
							Version:  "1",
						},
					},
				},
			},
		},
	}
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodePipeline", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "codepipeline", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityHeaderExact, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreatePipeline")
	assert.Contains(t, ops, "GetPipeline")
	assert.Contains(t, ops, "UpdatePipeline")
	assert.Contains(t, ops, "DeletePipeline")
	assert.Contains(t, ops, "ListPipelines")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	matcher := h.RouteMatcher()

	tests := []struct {
		name   string
		target string
		want   bool
	}{
		{
			name:   "codepipeline prefix",
			target: "CodePipeline_20150709.CreatePipeline",
			want:   true,
		},
		{
			name:   "other service",
			target: "CodeBuild_20161006.CreateProject",
			want:   false,
		},
		{
			name:   "empty",
			target: "",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tests := []struct {
		name   string
		target string
		want   string
	}{
		{
			name:   "CreatePipeline",
			target: "CodePipeline_20150709.CreatePipeline",
			want:   "CreatePipeline",
		},
		{
			name:   "GetPipeline",
			target: "CodePipeline_20150709.GetPipeline",
			want:   "GetPipeline",
		},
		{
			name:   "no prefix",
			target: "SomeOtherTarget",
			want:   "SomeOtherTarget",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractOperation(c))
		})
	}
}

func TestHandler_CreatePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		name       string
		wantName   string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			input: map[string]any{
				"pipeline": samplePipeline("my-pipeline"),
			},
			wantStatus: http.StatusOK,
			wantName:   "my-pipeline",
		},
		{
			name:       "missing pipeline",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name: "missing name",
			input: map[string]any{
				"pipeline": map[string]any{},
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name: "duplicate name",
			input: map[string]any{
				"pipeline": samplePipeline("duplicate-pipeline"),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "duplicate name" {
				rec := doRequest(t, h, "CreatePipeline", map[string]any{
					"pipeline": samplePipeline("duplicate-pipeline"),
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doRequest(t, h, "CreatePipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				pipeline, ok := out["pipeline"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, tt.wantName, pipeline["name"])
			}
		})
	}
}

func TestHandler_GetPipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		pipelineFn func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			pipelineFn: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("get-pipeline"), nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"name": "get-pipeline"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			pipelineFn: nil,
			input:      map[string]any{"name": "nonexistent"},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing name",
			pipelineFn: nil,
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.pipelineFn != nil {
				tt.pipelineFn(h)
			}

			rec := doRequest(t, h, "GetPipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UpdatePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("update-pipeline"), nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"pipeline": samplePipeline("update-pipeline"),
			},
			wantStatus: http.StatusOK,
		},
		{
			name:  "not found",
			setup: nil,
			input: map[string]any{
				"pipeline": samplePipeline("nonexistent"),
			},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing pipeline",
			setup:      nil,
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "UpdatePipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_DeletePipeline(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("delete-pipeline"), nil)
				require.NoError(t, err)
			},
			input:      map[string]any{"name": "delete-pipeline"},
			wantStatus: http.StatusOK,
		},
		{
			name:       "not found",
			setup:      nil,
			input:      map[string]any{"name": "nonexistent"},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing name",
			setup:      nil,
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeletePipeline", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_ListPipelines(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		name       string
		wantStatus int
		wantCount  int
	}{
		{
			name: "with pipelines",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("pipeline-1"), nil)
				require.NoError(t, err)
				_, err = h.Backend.CreatePipeline(samplePipeline("pipeline-2"), nil)
				require.NoError(t, err)
			},
			wantStatus: http.StatusOK,
			wantCount:  2,
		},
		{
			name:       "empty",
			setup:      nil,
			wantStatus: http.StatusOK,
			wantCount:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "ListPipelines", map[string]any{})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			pipelines, _ := out["pipelines"].([]any)
			assert.Len(t, pipelines, tt.wantCount)
		})
	}
}

func TestHandler_TaggingOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler) string
		input      func(arn string) any
		name       string
		action     string
		wantStatus int
	}{
		{
			name:   "list tags - empty",
			action: "ListTagsForResource",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(samplePipeline("list-empty-pipeline"), nil)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			input: func(arn string) any {
				return map[string]any{"resourceArn": arn}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "tag resource",
			action: "TagResource",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(samplePipeline("tag-resource-pipeline"), nil)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			input: func(arn string) any {
				return map[string]any{
					"resourceArn": arn,
					"tags":        []map[string]string{{"key": "Environment", "value": "test"}},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "untag resource",
			action: "UntagResource",
			setup: func(h *codepipeline.Handler) string {
				p, err := h.Backend.CreatePipeline(
					samplePipeline("untag-resource-pipeline"),
					map[string]string{"Env": "test"},
				)
				require.NoError(t, err)

				return p.Metadata.PipelineArn
			},
			input: func(arn string) any {
				return map[string]any{
					"resourceArn": arn,
					"tagKeys":     []string{"Env"},
				}
			},
			wantStatus: http.StatusOK,
		},
		{
			name:   "list tags - not found ARN",
			action: "ListTagsForResource",
			setup:  nil,
			input: func(_ string) any {
				return map[string]any{
					"resourceArn": "arn:aws:codepipeline:us-east-1:000:nonexistent",
				}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "tag resource - missing ARN",
			action: "TagResource",
			setup:  nil,
			input: func(_ string) any {
				return map[string]any{"tags": []map[string]string{}}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:   "untag resource - missing ARN",
			action: "UntagResource",
			setup:  nil,
			input: func(_ string) any {
				return map[string]any{"tagKeys": []string{}}
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "list tags - missing ARN",
			action:     "ListTagsForResource",
			setup:      nil,
			input:      func(_ string) any { return map[string]any{} },
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var arn string
			if tt.setup != nil {
				arn = tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.input(arn))
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, "UnknownAction", map[string]any{})
	require.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "InvalidActionException", out["__type"])
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()
	assert.Equal(t, h.GetSupportedOperations(), ops)
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Empty(t, h.ExtractResource(c))
}

func TestInMemoryBackend_CreatePipeline_WithTags(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	p, err := backend.CreatePipeline(samplePipeline("tagged-pipeline"), map[string]string{"Env": "prod"})
	require.NoError(t, err)

	tags, err := backend.ListTagsForResource(p.Metadata.PipelineArn)
	require.NoError(t, err)

	tagMap := make(map[string]string, len(tags))
	for _, tag := range tags {
		tagMap[tag.Key] = tag.Value
	}

	assert.Equal(t, "prod", tagMap["Env"])
}

func TestInMemoryBackend_UpdatePipeline_IncrementsVersion(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := backend.CreatePipeline(samplePipeline("versioned-pipeline"), nil)
	require.NoError(t, err)

	updated, err := backend.UpdatePipeline(samplePipeline("versioned-pipeline"))
	require.NoError(t, err)
	assert.Equal(t, 2, updated.Declaration.Version)
}

func TestHandler_ErrorEnvelopes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		action     string
		wantType   string
		wantStatus int
	}{
		{
			name:       "not found returns PipelineNotFoundException",
			action:     "GetPipeline",
			input:      map[string]any{"name": "nonexistent"},
			wantStatus: http.StatusBadRequest,
			wantType:   "PipelineNotFoundException",
		},
		{
			name: "duplicate create returns PipelineNameInUseException",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("duplicate-pipeline"), nil)
				require.NoError(t, err)
			},
			action:     "CreatePipeline",
			input:      map[string]any{"pipeline": samplePipeline("duplicate-pipeline")},
			wantStatus: http.StatusBadRequest,
			wantType:   "PipelineNameInUseException",
		},
		{
			name:       "unknown action returns InvalidActionException",
			action:     "NoSuchAction",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "InvalidActionException",
		},
		{
			name:       "missing required field returns ValidationException",
			action:     "CreatePipeline",
			input:      map[string]any{},
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.input)
			require.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantType, out["__type"])
		})
	}
}

func TestHandler_GetPipeline_VersionHandling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		version    int
		wantStatus int
	}{
		{
			name:       "version 0 returns latest",
			version:    0,
			wantStatus: http.StatusOK,
		},
		{
			name:       "exact version match",
			version:    1,
			wantStatus: http.StatusOK,
		},
		{
			name:       "wrong version returns not found",
			version:    99,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreatePipeline(samplePipeline("ver-pipeline"), nil)
			require.NoError(t, err)

			rec := doRequest(t, h, "GetPipeline", map[string]any{
				"name":    "ver-pipeline",
				"version": tt.version,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestInMemoryBackend_DeepCopy(t *testing.T) {
	t.Parallel()

	backend := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	decl := samplePipeline("deep-copy-pipeline")
	decl.Stages[0].Actions[0].Configuration = map[string]string{"key": "original"}

	p, err := backend.CreatePipeline(decl, nil)
	require.NoError(t, err)

	// Mutate the returned pipeline's nested data.
	p.Declaration.Stages[0].Actions[0].Configuration["key"] = "mutated"

	// The backend should still have the original value.
	stored, err := backend.GetPipeline("deep-copy-pipeline")
	require.NoError(t, err)
	assert.Equal(t, "original", stored.Declaration.Stages[0].Actions[0].Configuration["key"])
}

func TestHandler_AcknowledgeJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		wantStatus string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success_inprogress",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{ID: "job-1", Nonce: "nonce-abc", Status: "Created"})
			},
			input:      map[string]any{"jobId": "job-1", "nonce": "nonce-abc"},
			httpStatus: http.StatusOK,
			wantStatus: "InProgress",
		},
		{
			name: "nonce_mismatch_status_unchanged",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{ID: "job-2", Nonce: "nonce-right", Status: "Created"})
			},
			input:      map[string]any{"jobId": "job-2", "nonce": "nonce-wrong"},
			httpStatus: http.StatusOK,
			wantStatus: "Created",
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"jobId": "no-such-job", "nonce": "nonce-1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_jobId",
			setup:      nil,
			input:      map[string]any{"nonce": "nonce-1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_nonce",
			setup:      nil,
			input:      map[string]any{"jobId": "job-x"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "AcknowledgeJob", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantStatus, out["status"])
			}
		})
	}
}

func TestHandler_AcknowledgeThirdPartyJob(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		wantStatus string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{ID: "tp-job-1", Nonce: "tp-nonce", Status: "Created"})
			},
			input: map[string]any{
				"jobId":       "tp-job-1",
				"nonce":       "tp-nonce",
				"clientToken": "token-abc",
			},
			httpStatus: http.StatusOK,
			wantStatus: "InProgress",
		},
		{
			name:       "missing_clientToken",
			setup:      nil,
			input:      map[string]any{"jobId": "x", "nonce": "y"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"jobId": "no-such", "nonce": "n", "clientToken": "t"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "AcknowledgeThirdPartyJob", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, tt.wantStatus, out["status"])
			}
		})
	}
}

func TestHandler_CreateCustomActionType(t *testing.T) {
	t.Parallel()

	validInput := map[string]any{
		"category":              "Build",
		"provider":              "MyBuilder",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 1, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
	}

	tests := []struct {
		input      any
		setup      func(h *codepipeline.Handler)
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name:       "success",
			input:      validInput,
			httpStatus: http.StatusOK,
		},
		{
			name: "duplicate",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", validInput)
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      validInput,
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_category",
			input:      map[string]any{"provider": "P", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_provider",
			input:      map[string]any{"category": "Build", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_version",
			input:      map[string]any{"category": "Build", "provider": "P"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "CreateCustomActionType", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				at, ok := out["actionType"].(map[string]any)
				require.True(t, ok)
				id, ok := at["id"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Build", id["category"])
				assert.Equal(t, "MyBuilder", id["provider"])
				assert.Equal(t, "1", id["version"])
			}
		})
	}
}

func TestHandler_DeleteCustomActionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category": "Deploy", "provider": "MyDeploy", "version": "2",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      map[string]any{"category": "Deploy", "provider": "MyDeploy", "version": "2"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"category": "Build", "provider": "Ghost", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_category",
			setup:      nil,
			input:      map[string]any{"provider": "P", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteCustomActionType", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)
		})
	}
}

func TestHandler_GetActionType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
					"category": "Test", "provider": "MyTest", "version": "3",
					"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
					"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
				})
				require.Equal(t, http.StatusOK, rec.Code)
			},
			input:      map[string]any{"category": "Test", "owner": "Custom", "provider": "MyTest", "version": "3"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"category": "Build", "owner": "Custom", "provider": "Ghost", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_category",
			setup:      nil,
			input:      map[string]any{"owner": "Custom", "provider": "P", "version": "1"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetActionType", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				at, ok := out["actionType"].(map[string]any)
				require.True(t, ok)
				id, ok := at["id"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "Test", id["category"])
				assert.Equal(t, "MyTest", id["provider"])
			}
		})
	}
}

func TestHandler_GetJobDetails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
		wantErr    bool
	}{
		{
			name: "success",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddJobInternal(&codepipeline.Job{ID: "jd-1", Nonce: "n", Status: "Created"})
			},
			input:      map[string]any{"jobId": "jd-1"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "not_found",
			setup:      nil,
			input:      map[string]any{"jobId": "missing"},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:       "missing_jobId",
			setup:      nil,
			input:      map[string]any{},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "GetJobDetails", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if !tt.wantErr {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				details, ok := out["jobDetails"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "jd-1", details["id"])
			}
		})
	}
}

func TestHandler_DeleteWebhook(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
	}{
		{
			name: "success_existing",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{Name: "wh-1", TargetPipeline: "pl-1"})
			},
			input:      map[string]any{"name": "wh-1"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "idempotent_nonexistent",
			setup:      nil,
			input:      map[string]any{"name": "no-such-webhook"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "missing_name",
			setup:      nil,
			input:      map[string]any{},
			httpStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeleteWebhook", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)
		})
	}
}

func TestHandler_DeregisterWebhookWithThirdParty(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		httpStatus int
	}{
		{
			name: "success_existing",
			setup: func(h *codepipeline.Handler) {
				h.Backend.AddWebhookInternal(&codepipeline.Webhook{
					Name:                     "wh-2",
					TargetPipeline:           "pl-2",
					RegisteredWithThirdParty: true,
				})
			},
			input:      map[string]any{"webhookName": "wh-2"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "idempotent_nonexistent",
			setup:      nil,
			input:      map[string]any{"webhookName": "no-such"},
			httpStatus: http.StatusOK,
		},
		{
			name:       "empty_name_is_ok",
			setup:      nil,
			input:      map[string]any{},
			httpStatus: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, "DeregisterWebhookWithThirdParty", tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)

			if tt.httpStatus == http.StatusOK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Empty(t, out)
			}
		})
	}
}

func TestHandler_DisableEnableStageTransition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h *codepipeline.Handler)
		input      any
		name       string
		action     string
		httpStatus int
		wantErr    bool
	}{
		{
			name:   "disable_success",
			action: "DisableStageTransition",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("trans-pipeline"), nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"pipelineName":   "trans-pipeline",
				"stageName":      "Source",
				"transitionType": "Inbound",
				"reason":         "waiting for approval",
			},
			httpStatus: http.StatusOK,
		},
		{
			name:   "disable_pipeline_not_found",
			action: "DisableStageTransition",
			setup:  nil,
			input: map[string]any{
				"pipelineName":   "ghost-pipeline",
				"stageName":      "Source",
				"transitionType": "Inbound",
				"reason":         "test",
			},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "disable_missing_reason",
			action: "DisableStageTransition",
			setup:  nil,
			input: map[string]any{
				"pipelineName":   "any",
				"stageName":      "Source",
				"transitionType": "Inbound",
			},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "enable_success",
			action: "EnableStageTransition",
			setup: func(h *codepipeline.Handler) {
				_, err := h.Backend.CreatePipeline(samplePipeline("enable-pipeline"), nil)
				require.NoError(t, err)
			},
			input: map[string]any{
				"pipelineName":   "enable-pipeline",
				"stageName":      "Source",
				"transitionType": "Outbound",
			},
			httpStatus: http.StatusOK,
		},
		{
			name:   "enable_pipeline_not_found",
			action: "EnableStageTransition",
			setup:  nil,
			input: map[string]any{
				"pipelineName":   "ghost",
				"stageName":      "Source",
				"transitionType": "Outbound",
			},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "enable_missing_transitionType",
			action: "EnableStageTransition",
			setup:  nil,
			input: map[string]any{
				"pipelineName": "any",
				"stageName":    "Source",
			},
			httpStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.input)
			assert.Equal(t, tt.httpStatus, rec.Code)
		})
	}
}

func TestHandler_DisableEnableStageTransition_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(samplePipeline("rt-pipeline"), nil)
	require.NoError(t, err)

	// Disable the transition.
	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"pipelineName":   "rt-pipeline",
		"stageName":      "Source",
		"transitionType": "Inbound",
		"reason":         "blocked",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Re-enable the transition.
	rec = doRequest(t, h, "EnableStageTransition", map[string]any{
		"pipelineName":   "rt-pipeline",
		"stageName":      "Source",
		"transitionType": "Inbound",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_NewOps_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, want := range []string{
		"AcknowledgeJob",
		"AcknowledgeThirdPartyJob",
		"CreateCustomActionType",
		"DeleteCustomActionType",
		"DeleteWebhook",
		"DeregisterWebhookWithThirdParty",
		"DisableStageTransition",
		"EnableStageTransition",
		"GetActionType",
		"GetJobDetails",
	} {
		assert.Contains(t, ops, want)
	}
}

// ============================================================
// Refinement check 1 tests
// ============================================================

func TestRefinement1_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a pipeline so there is state to reset.
	_, err := h.Backend.CreatePipeline(samplePipeline("reset-pl"), nil)
	require.NoError(t, err)

	assert.Equal(t, 1, h.Backend.PipelineCount())

	h.Reset()

	assert.Equal(t, 0, h.Backend.PipelineCount())
	assert.Equal(t, 0, h.Backend.CustomActionTypeCount())
	assert.Equal(t, 0, h.Backend.JobCount())
	assert.Equal(t, 0, h.Backend.WebhookCount())
	assert.Equal(t, 0, h.Backend.StageTransitionCount())
}

func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	var p codepipeline.Provider

	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codepipeline.ErrNilAppContext)
}

func TestRefinement1_SortedListPipelines(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, name := range []string{"zebra-pl", "apple-pl", "mango-pl"} {
		_, err := h.Backend.CreatePipeline(samplePipeline(name), nil)
		require.NoError(t, err)
	}

	rec := doRequest(t, h, "ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list, ok := out["pipelines"].([]any)
	require.True(t, ok)
	require.Len(t, list, 3)

	names := make([]string, len(list))
	for i, item := range list {
		m := item.(map[string]any)
		names[i] = m["name"].(string)
	}

	assert.Equal(t, []string{"apple-pl", "mango-pl", "zebra-pl"}, names)
}

func TestRefinement1_ListPipelines_NonNilWhenEmpty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// pipelines key must exist and be an array, not null.
	list, ok := out["pipelines"]
	require.True(t, ok)
	assert.NotNil(t, list)
}

func TestRefinement1_ListPipelines_IncludesARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(samplePipeline("arn-pl"), nil)
	require.NoError(t, err)

	rec := doRequest(t, h, "ListPipelines", map[string]any{})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	list := out["pipelines"].([]any)
	item := list[0].(map[string]any)
	arn, ok := item["pipelineArn"].(string)
	require.True(t, ok)
	assert.Contains(t, arn, "arn:aws:codepipeline")
	assert.Contains(t, arn, "arn-pl")
}

func TestRefinement1_SortedListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(samplePipeline("tag-pl"), map[string]string{
		"zzz": "last", "aaa": "first", "mmm": "mid",
	})
	require.NoError(t, err)

	// Get the ARN by listing pipelines.
	summaries := h.Backend.ListPipelines()
	require.Len(t, summaries, 1)
	pipelineARN := summaries[0].PipelineArn
	require.NotEmpty(t, pipelineARN)

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": pipelineARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tags, ok := out["tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 3)

	keys := make([]string, len(tags))
	for i, tag := range tags {
		keys[i] = tag.(map[string]any)["key"].(string)
	}
	assert.Equal(t, []string{"aaa", "mmm", "zzz"}, keys)
}

func TestRefinement1_ListTagsForResource_EmptySlice(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(samplePipeline("notag-pl"), nil)
	require.NoError(t, err)

	summaries := h.Backend.ListPipelines()
	pipelineARN := summaries[0].PipelineArn

	rec := doRequest(t, h, "ListTagsForResource", map[string]any{"resourceArn": pipelineARN})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	tags := out["tags"]
	assert.NotNil(t, tags)
}

func TestRefinement1_DeletePipeline_CascadeStageTransitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(samplePipeline("cascade-pl"), nil)
	require.NoError(t, err)

	// Disable a stage transition.
	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"pipelineName":   "cascade-pl",
		"stageName":      "Source",
		"transitionType": "Inbound",
		"reason":         "testing cascade",
	})
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, 1, h.Backend.StageTransitionCount())

	// Delete the pipeline.
	rec = doRequest(t, h, "DeletePipeline", map[string]any{"name": "cascade-pl"})
	require.Equal(t, http.StatusOK, rec.Code)

	// Transition should also be gone.
	assert.Equal(t, 0, h.Backend.StageTransitionCount())
	assert.Equal(t, 0, h.Backend.PipelineCount())
}

func TestRefinement1_TransitionTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		action         string
		transitionType string
		wantErr        bool
	}{
		{
			name:           "disable_inbound_ok",
			action:         "DisableStageTransition",
			transitionType: "Inbound",
			wantErr:        false,
		},
		{
			name:           "disable_outbound_ok",
			action:         "DisableStageTransition",
			transitionType: "Outbound",
			wantErr:        false,
		},
		{
			name:           "disable_invalid",
			action:         "DisableStageTransition",
			transitionType: "Invalid",
			wantErr:        true,
		},
		{
			name:           "enable_inbound_ok",
			action:         "EnableStageTransition",
			transitionType: "Inbound",
			wantErr:        false,
		},
		{
			name:           "enable_invalid",
			action:         "EnableStageTransition",
			transitionType: "BadValue",
			wantErr:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreatePipeline(samplePipeline("enum-pl"), nil)
			require.NoError(t, err)

			var input map[string]any
			if tt.action == "DisableStageTransition" {
				input = map[string]any{
					"pipelineName":   "enum-pl",
					"stageName":      "Source",
					"transitionType": tt.transitionType,
					"reason":         "test",
				}
			} else {
				input = map[string]any{
					"pipelineName":   "enum-pl",
					"stageName":      "Source",
					"transitionType": tt.transitionType,
				}
			}

			rec := doRequest(t, h, tt.action, input)

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestRefinement1_ActionCategoryValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		category string
		wantErr  bool
	}{
		{name: "source", category: "Source", wantErr: false},
		{name: "build", category: "Build", wantErr: false},
		{name: "deploy", category: "Deploy", wantErr: false},
		{name: "test", category: "Test", wantErr: false},
		{name: "invoke", category: "Invoke", wantErr: false},
		{name: "approval", category: "Approval", wantErr: false},
		{name: "compute", category: "Compute", wantErr: false},
		{name: "invalid", category: "Invalid", wantErr: true},
		{name: "lowercase", category: "build", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
				"category":              tt.category,
				"provider":              "TestProvider",
				"version":               "1",
				"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
				"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
			})

			if tt.wantErr {
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			} else {
				assert.Equal(t, http.StatusOK, rec.Code)
			}
		})
	}
}

func TestRefinement1_ErrValidationMapping(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Trigger ErrValidation via invalid category.
	rec := doRequest(t, h, "CreateCustomActionType", map[string]any{
		"category":              "NotAValidCategory",
		"provider":              "P",
		"version":               "1",
		"inputArtifactDetails":  map[string]any{"minimumCount": 0, "maximumCount": 5},
		"outputArtifactDetails": map[string]any{"minimumCount": 0, "maximumCount": 5},
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "ValidationException", out["__type"])
}

func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	// AddPipelineInternal
	b.AddPipelineInternal(samplePipeline("seed-pl"), map[string]string{"x": "y"})
	assert.Equal(t, 1, b.PipelineCount())

	// AddCustomActionTypeInternal
	b.AddCustomActionTypeInternal(&codepipeline.CustomActionType{
		Category: "Build", Provider: "Seed", Version: "1",
		InputArtifactDetails:  codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
		OutputArtifactDetails: codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
	})
	assert.Equal(t, 1, b.CustomActionTypeCount())

	// AddJobInternal
	b.AddJobInternal(&codepipeline.Job{ID: "j-1", Nonce: "n", Status: "Created"})
	assert.Equal(t, 1, b.JobCount())

	// AddWebhookInternal
	b.AddWebhookInternal(&codepipeline.Webhook{Name: "wh-seed", TargetPipeline: "pl"})
	assert.Equal(t, 1, b.WebhookCount())
}

func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("123456789012", "eu-west-1")

	// Populate state.
	b.AddPipelineInternal(samplePipeline("persist-pl"), map[string]string{"tier": "prod"})
	b.AddCustomActionTypeInternal(&codepipeline.CustomActionType{
		Category: "Deploy", Provider: "MyDeploy", Version: "2",
		InputArtifactDetails:  codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
		OutputArtifactDetails: codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
	})
	b.AddJobInternal(&codepipeline.Job{ID: "persist-job", Nonce: "nonce-42", Status: "Created"})
	b.AddWebhookInternal(&codepipeline.Webhook{Name: "persist-wh", TargetPipeline: "persist-pl"})

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	// Verify pipeline.
	p, err := b2.GetPipeline("persist-pl")
	require.NoError(t, err)
	assert.Equal(t, "persist-pl", p.Declaration.Name)

	// Verify custom action type.
	cat, err := b2.GetActionType("Deploy", "Custom", "MyDeploy", "2")
	require.NoError(t, err)
	assert.Equal(t, "Deploy", cat.Category)

	// Verify job.
	job, err := b2.GetJobDetails("persist-job")
	require.NoError(t, err)
	assert.Equal(t, "persist-job", job.ID)

	// Verify region/account carried through.
	assert.Equal(t, "eu-west-1", b2.Region())
}

func TestRefinement1_PersistenceWithStageTransitions(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddPipelineInternal(samplePipeline("trans-pl"), nil)

	err := b.DisableStageTransition("trans-pl", "Source", "Inbound", "test reason")
	require.NoError(t, err)
	assert.Equal(t, 1, b.StageTransitionCount())

	snap := b.Snapshot()
	require.NotEmpty(t, snap)

	b2 := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(snap))

	// Verify stage transition was restored.
	assert.Equal(t, 1, b2.StageTransitionCount())
	state := b2.GetStageTransitionState("trans-pl", "Source", "Inbound")
	require.NotNil(t, state)
	assert.Equal(t, "test reason", state.Reason)
	assert.True(t, state.Disabled)
}

func TestRefinement1_GetStageTransitionState(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_, err := h.Backend.CreatePipeline(samplePipeline("state-pl"), nil)
	require.NoError(t, err)

	// Initially enabled (nil).
	state := h.Backend.GetStageTransitionState("state-pl", "Source", "Inbound")
	assert.Nil(t, state)

	// Disable it.
	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"pipelineName":   "state-pl",
		"stageName":      "Source",
		"transitionType": "Inbound",
		"reason":         "blocked",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	state = h.Backend.GetStageTransitionState("state-pl", "Source", "Inbound")
	require.NotNil(t, state)
	assert.Equal(t, "blocked", state.Reason)
	assert.True(t, state.Disabled)

	// Re-enable it.
	rec = doRequest(t, h, "EnableStageTransition", map[string]any{
		"pipelineName":   "state-pl",
		"stageName":      "Source",
		"transitionType": "Inbound",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	state = h.Backend.GetStageTransitionState("state-pl", "Source", "Inbound")
	assert.Nil(t, state)
}

func TestRefinement1_HandlerOpsPreBuilt(t *testing.T) {
	t.Parallel()

	// Verify that handler dispatches correctly without rebuilding the table
	// (ops cached in NewHandler). Run requests to exercise the cached dispatch path.
	const numRequests = 5

	h := newTestHandler(t)

	for range numRequests {
		rec := doRequest(t, h, "ListPipelines", map[string]any{})
		assert.Equal(t, http.StatusOK, rec.Code)
	}
}

func TestRefinement1_AddCustomActionTypeInternal_DeepCopy(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")
	cat := &codepipeline.CustomActionType{
		Category:              "Build",
		Provider:              "CopyTest",
		Version:               "1",
		Tags:                  map[string]string{"original": "value"},
		InputArtifactDetails:  codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
		OutputArtifactDetails: codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
	}

	b.AddCustomActionTypeInternal(cat)

	// Mutate original - backend should not be affected.
	cat.Tags["original"] = "mutated"

	retrieved, err := b.GetActionType("Build", "Custom", "CopyTest", "1")
	require.NoError(t, err)
	assert.Equal(t, "value", retrieved.Tags["original"])
}

func TestRefinement1_PipelineNameRequiredInDisable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "DisableStageTransition", map[string]any{
		"stageName":      "Source",
		"transitionType": "Inbound",
		"reason":         "test",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestRefinement1_ExportCountHelpers(t *testing.T) {
	t.Parallel()

	b := codepipeline.NewInMemoryBackend("000000000000", "us-east-1")

	assert.Equal(t, 0, b.PipelineCount())
	assert.Equal(t, 0, b.CustomActionTypeCount())
	assert.Equal(t, 0, b.JobCount())
	assert.Equal(t, 0, b.WebhookCount())
	assert.Equal(t, 0, b.StageTransitionCount())

	b.AddPipelineInternal(samplePipeline("cnt-pl"), nil)
	assert.Equal(t, 1, b.PipelineCount())

	b.AddCustomActionTypeInternal(&codepipeline.CustomActionType{
		Category: "Build", Provider: "Cnt", Version: "1",
		InputArtifactDetails:  codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
		OutputArtifactDetails: codepipeline.ArtifactDetails{MinimumCount: 0, MaximumCount: 5},
	})
	assert.Equal(t, 1, b.CustomActionTypeCount())

	b.AddJobInternal(&codepipeline.Job{ID: "cnt-j", Nonce: "n", Status: "Created"})
	assert.Equal(t, 1, b.JobCount())

	b.AddWebhookInternal(&codepipeline.Webhook{Name: "cnt-wh", TargetPipeline: "cnt-pl"})
	assert.Equal(t, 1, b.WebhookCount())
}
