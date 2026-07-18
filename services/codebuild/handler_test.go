package codebuild_test

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
	"github.com/blackbirdworks/gopherstack/services/codebuild"
)

func newTestHandler(t *testing.T) *codebuild.Handler {
	t.Helper()

	return codebuild.NewHandler(codebuild.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(t *testing.T, h *codebuild.Handler, action string, body any) *httptest.ResponseRecorder {
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
	req.Header.Set("X-Amz-Target", "CodeBuild_20161006."+action)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// createTestProject creates a minimal project named name via the handler.
func createTestProject(t *testing.T, h *codebuild.Handler, name string) {
	t.Helper()

	rec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      name,
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)
}

// makeProject creates a project named name with a serviceRole set, via the handler.
func makeProject(t *testing.T, h *codebuild.Handler, name string) {
	t.Helper()
	doRequest(t, h, "CreateProject", map[string]any{
		"name":        name,
		"serviceRole": "arn:aws:iam::000000000000:role/cb",
		"artifacts":   map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type": "LINUX_CONTAINER", "image": "aws/codebuild/standard:7.0", "computeType": "BUILD_GENERAL1_SMALL",
		},
		"source": map[string]any{"type": "NO_SOURCE"},
	})
}

// projectARN returns the ARN of the previously created project named name.
func projectARN(t *testing.T, h *codebuild.Handler, name string) string {
	t.Helper()

	rec := doRequest(t, h, "BatchGetProjects", map[string]any{"names": []string{name}})
	require.Equal(t, http.StatusOK, rec.Code)

	var out struct {
		Projects []struct {
			Arn string `json:"arn"`
		} `json:"projects"`
	}
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	require.NotEmpty(t, out.Projects)

	return out.Projects[0].Arn
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "CodeBuild", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "codebuild", h.ChaosServiceName())
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
	assert.Contains(t, ops, "CreateProject")
	assert.Contains(t, ops, "BatchGetProjects")
	assert.Contains(t, ops, "UpdateProject")
	assert.Contains(t, ops, "DeleteProject")
	assert.Contains(t, ops, "ListProjects")
	assert.Contains(t, ops, "StartBuild")
	assert.Contains(t, ops, "BatchGetBuilds")
	assert.Contains(t, ops, "StopBuild")
	assert.Contains(t, ops, "ListBuildsForProject")
	assert.Contains(t, ops, "ListTagsForResource")
	assert.Contains(t, ops, "TagResource")
	assert.Contains(t, ops, "UntagResource")
	assert.Contains(t, ops, "BatchGetBuildBatches")
	assert.Contains(t, ops, "BatchGetCommandExecutions")
	assert.Contains(t, ops, "BatchGetFleets")
	assert.Contains(t, ops, "BatchGetReportGroups")
	assert.Contains(t, ops, "BatchGetReports")
	assert.Contains(t, ops, "CreateFleet")
	assert.Contains(t, ops, "CreateReportGroup")
	assert.Contains(t, ops, "CreateWebhook")
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
			name:   "matching target",
			target: "CodeBuild_20161006.CreateProject",
			want:   true,
		},
		{
			name:   "non-matching target",
			target: "AWSInsightsIndexService.CreateCostCategoryDefinition",
			want:   false,
		},
		{
			name:   "empty target",
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

func TestHandler_UnknownAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "UnknownOp", nil)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		wantOps []string
		wantLen int
	}{
		{
			name:    "returns all supported operations",
			wantLen: 62,
			wantOps: []string{
				"CreateProject",
				"BatchGetProjects",
				"StartBuild",
				"StopBuild",
				"CreateFleet",
				"CreateReportGroup",
				"CreateWebhook",
				"BatchGetBuildBatches",
				"BatchGetCommandExecutions",
				"BatchGetSandboxes",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			ops := h.ChaosOperations()
			assert.Len(t, ops, tt.wantLen)
			for _, op := range tt.wantOps {
				assert.Contains(t, ops, op)
			}
		})
	}
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		wantRegions []string
	}{
		{
			name:        "returns configured region",
			wantRegions: []string{"us-east-1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			regions := h.ChaosRegions()
			assert.Equal(t, tt.wantRegions, regions)
		})
	}
}

func TestHandler_Region(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantRegion string
	}{
		{
			name:       "returns backend region",
			wantRegion: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			assert.Equal(t, tt.wantRegion, h.Backend.Region())
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		target string
		wantOp string
	}{
		{
			name:   "create project",
			target: "CodeBuild_20161006.CreateProject",
			wantOp: "CreateProject",
		},
		{
			name:   "batch get projects",
			target: "CodeBuild_20161006.BatchGetProjects",
			wantOp: "BatchGetProjects",
		},
		{
			name:   "empty target",
			target: "",
			wantOp: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Target", tt.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		path         string
		wantResource string
	}{
		{
			name:         "always empty regardless of path",
			path:         "/",
			wantResource: "",
		},
		{
			name:         "empty for specific project path",
			path:         "/projects/my-project",
			wantResource: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			resource := h.ExtractResource(c)
			assert.Equal(t, tt.wantResource, resource)
		})
	}
}

func TestProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantName string
	}{
		{
			name:     "init and name",
			wantName: "CodeBuild",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			p := &codebuild.Provider{}
			assert.Equal(t, tt.wantName, p.Name())

			ctx := &service.AppContext{}
			svc, err := p.Init(ctx)
			require.NoError(t, err)
			assert.NotNil(t, svc)
		})
	}
}

func TestProvider_NilCtx(t *testing.T) {
	t.Parallel()

	p := &codebuild.Provider{}
	_, err := p.Init(nil)
	require.Error(t, err)
	assert.ErrorIs(t, err, codebuild.ErrNilAppContext)
}

func TestBackend_Reset(t *testing.T) {
	t.Parallel()

	b := codebuild.NewInMemoryBackend("000000000000", "us-east-1")

	// Seed some data.
	src2 := codebuild.ProjectSource{Type: "NO_SOURCE"}
	arts2 := codebuild.ProjectArtifacts{Type: "NO_ARTIFACTS"}
	env2 := codebuild.ProjectEnvironment{
		Type:        "LINUX_CONTAINER",
		Image:       "aws/codebuild/standard:5.0",
		ComputeType: "BUILD_GENERAL1_SMALL",
	}
	_, err := b.CreateProject(codebuild.ProjectConfig{
		Name:        "reset-proj",
		Source:      &src2,
		Artifacts:   &arts2,
		Environment: &env2,
	})
	require.NoError(t, err)

	_, err = b.CreateFleet("reset-fleet", 1, "BUILD_GENERAL1_SMALL", "LINUX_CONTAINER", nil)
	require.NoError(t, err)

	_, err = b.CreateReportGroup("reset-rg", "TEST", codebuild.ReportExportConfig{}, nil)
	require.NoError(t, err)

	// Reset.
	b.Reset()

	// All should be gone.
	assert.Empty(t, b.ListProjects())
	fleets, _ := b.BatchGetFleets([]string{"reset-fleet"})
	assert.Empty(t, fleets)
	rgs, _ := b.BatchGetReportGroups([]string{"arn:aws:codebuild:us-east-1:000000000000:report-group/reset-rg"})
	assert.Empty(t, rgs)
}

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createRec := doRequest(t, h, "CreateProject", map[string]any{
		"name":      "hr-proj",
		"source":    map[string]any{"type": "NO_SOURCE"},
		"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
		"environment": map[string]any{
			"type":        "LINUX_CONTAINER",
			"image":       "aws/codebuild/standard:5.0",
			"computeType": "BUILD_GENERAL1_SMALL",
		},
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	// Verify project exists.
	listRec := doRequest(t, h, "ListProjects", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	var listOut map[string]any
	require.NoError(t, json.NewDecoder(listRec.Body).Decode(&listOut))
	assert.Len(t, listOut["projects"], 1)

	// Reset.
	h.Reset()

	// Verify project is gone.
	listRec2 := doRequest(t, h, "ListProjects", nil)
	require.Equal(t, http.StatusOK, listRec2.Code)
	var listOut2 map[string]any
	require.NoError(t, json.NewDecoder(listRec2.Body).Decode(&listOut2))
	assert.Empty(t, listOut2["projects"])
}

func TestHandler_ErrValidation_InHandleError(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Try creating a fleet without a name — hits errInvalidRequest (wrapped ErrValidation is not used yet).
	rec := doRequest(t, h, "CreateFleet", map[string]any{"baseCapacity": 2})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandler_ErrorTypeMapping verifies that handleError maps backend errors to the
// AWS error types real CodeBuild returns, across operations from several op families.
func TestHandler_ErrorTypeMapping(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        any
		setup       func(h *codebuild.Handler)
		name        string
		action      string
		wantErrType string
		wantStatus  int
	}{
		{
			name:   "create_duplicate_project_returns_ResourceAlreadyExistsException",
			action: "CreateProject",
			body: map[string]any{
				"name":      "dup-project",
				"source":    map[string]any{"type": "NO_SOURCE"},
				"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
				"environment": map[string]any{
					"type":        "LINUX_CONTAINER",
					"image":       "img",
					"computeType": "BUILD_GENERAL1_SMALL",
				},
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceAlreadyExistsException",
			setup: func(h *codebuild.Handler) {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      "dup-project",
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "img",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})
			},
		},
		{
			name:   "create_duplicate_fleet_returns_ResourceAlreadyExistsException",
			action: "CreateFleet",
			body: map[string]any{
				"name":            "dup-fleet",
				"baseCapacity":    1,
				"computeType":     "BUILD_GENERAL1_SMALL",
				"environmentType": "LINUX_CONTAINER",
			},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceAlreadyExistsException",
			setup: func(h *codebuild.Handler) {
				doRequest(t, h, "CreateFleet", map[string]any{
					"name":            "dup-fleet",
					"baseCapacity":    1,
					"computeType":     "BUILD_GENERAL1_SMALL",
					"environmentType": "LINUX_CONTAINER",
				})
			},
		},
		{
			name:        "create_duplicate_webhook_returns_ResourceAlreadyExistsException",
			action:      "CreateWebhook",
			body:        map[string]any{"projectName": "webhook-proj"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceAlreadyExistsException",
			setup: func(h *codebuild.Handler) {
				doRequest(t, h, "CreateProject", map[string]any{
					"name":      "webhook-proj",
					"source":    map[string]any{"type": "NO_SOURCE"},
					"artifacts": map[string]any{"type": "NO_ARTIFACTS"},
					"environment": map[string]any{
						"type":        "LINUX_CONTAINER",
						"image":       "img",
						"computeType": "BUILD_GENERAL1_SMALL",
					},
				})
				doRequest(t, h, "CreateWebhook", map[string]any{"projectName": "webhook-proj"})
			},
		},
		{
			name:        "start_build_missing_project_returns_ResourceNotFoundException",
			action:      "StartBuild",
			body:        map[string]any{"projectName": "ghost-project"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "stop_build_missing_returns_ResourceNotFoundException",
			action:      "StopBuild",
			body:        map[string]any{"id": "ghost:abc"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "update_project_missing_returns_ResourceNotFoundException",
			action:      "UpdateProject",
			body:        map[string]any{"name": "ghost-project"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "delete_project_missing_returns_ResourceNotFoundException",
			action:      "DeleteProject",
			body:        map[string]any{"name": "ghost-project"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "delete_fleet_missing_returns_ResourceNotFoundException",
			action:      "DeleteFleet",
			body:        map[string]any{"arn": "arn:aws:codebuild:us-east-1:000000000000:fleet/ghost"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
		{
			name:        "get_resource_policy_missing_returns_ResourceNotFoundException",
			action:      "GetResourcePolicy",
			body:        map[string]any{"resourceArn": "arn:aws:codebuild:us-east-1:000000000000:project/ghost"},
			wantStatus:  http.StatusBadRequest,
			wantErrType: "ResourceNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.action, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantErrType != "" {
				var errResp struct {
					Type string `json:"__type"`
				}
				require.NoError(t, json.NewDecoder(rec.Body).Decode(&errResp))
				assert.Equal(t, tt.wantErrType, errResp.Type)
			}
		})
	}
}
