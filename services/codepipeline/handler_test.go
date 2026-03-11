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
	assert.Equal(t, http.StatusBadRequest, rec.Code)
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
