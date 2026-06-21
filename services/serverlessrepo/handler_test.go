package serverlessrepo_test

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
	"github.com/blackbirdworks/gopherstack/services/serverlessrepo"
)

func newTestHandler(t *testing.T) *serverlessrepo.Handler {
	t.Helper()

	return serverlessrepo.NewHandler(serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doServerlessRepoRequest(
	t *testing.T,
	h *serverlessrepo.Handler,
	method string,
	path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "ServerlessRepo", h.Name())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "CreateApplication")
	assert.Contains(t, ops, "GetApplication")
	assert.Contains(t, ops, "ListApplications")
	assert.Contains(t, ops, "UpdateApplication")
	assert.Contains(t, ops, "DeleteApplication")
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 87, h.MatchPriority())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "serverlessrepo", h.ChaosServiceName())
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()
	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		service string
		path    string
		want    bool
	}{
		{
			name:    "matches /applications with serverlessrepo service",
			service: "serverlessrepo",
			path:    "/applications",
			want:    true,
		},
		{
			name:    "matches /applications/{id} with serverlessrepo service",
			service: "serverlessrepo",
			path:    "/applications/my-app",
			want:    true,
		},
		{
			name:    "does not match wrong service name",
			service: "sagemaker",
			path:    "/applications",
			want:    false,
		},
		{
			name:    "does not match wrong path",
			service: "serverlessrepo",
			path:    "/models",
			want:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set(
				"Authorization",
				"AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/"+tt.service+"/aws4_request",
			)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestHandler_Provider_Init(t *testing.T) {
	t.Parallel()

	p := &serverlessrepo.Provider{}
	assert.Equal(t, "ServerlessRepo", p.Name())

	ctx := &service.AppContext{}
	reg, err := p.Init(ctx)
	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "ServerlessRepo", reg.Name())
}

func TestHandler_CreateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		wantCode int
		wantARN  bool
	}{
		{
			name: "creates application successfully",
			body: map[string]any{
				"name":            "my-app",
				"description":     "A test application",
				"author":          "test-author",
				"semanticVersion": "1.0.0",
			},
			wantCode: http.StatusCreated,
			wantARN:  true,
		},
		{
			name:     "missing name returns bad request",
			body:     map[string]any{"description": "No name", "author": "a"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing author returns bad request",
			body:     map[string]any{"name": "my-app", "description": "desc"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing description returns bad request",
			body:     map[string]any{"name": "my-app", "author": "author"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate application returns conflict",
			body: map[string]any{
				"name":        "existing-app",
				"description": "desc",
				"author":      "author",
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCode == http.StatusConflict {
				_, err := h.Backend.CreateApplication("existing-app", "desc", "author", "", "", nil, "", "", "")
				require.NoError(t, err)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantARN {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["applicationId"])
				assert.Equal(t, tt.body["name"], resp["name"])
			}
		})
	}
}

func TestHandler_GetApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{
			name:     "get existing application",
			appName:  "existing-app",
			wantCode: http.StatusOK,
		},
		{
			name:     "get non-existent application",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			_, err := h.Backend.CreateApplication("existing-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName, nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "existing-app", resp["name"])
			}
		})
	}
}

func TestHandler_ListApplications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*serverlessrepo.Handler)
		name     string
		wantLen  int
		wantCode int
	}{
		{
			name:     "empty list",
			wantLen:  0,
			wantCode: http.StatusOK,
		},
		{
			name: "list with applications",
			setup: func(h *serverlessrepo.Handler) {
				_, _ = h.Backend.CreateApplication("app-a", "A", "author", "", "1.0.0", nil, "", "", "")
				_, _ = h.Backend.CreateApplication("app-b", "B", "author", "", "1.0.0", nil, "", "", "")
			},
			wantLen:  2,
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			apps, ok := resp["applications"].([]any)
			require.True(t, ok)
			assert.Len(t, apps, tt.wantLen)
		})
	}
}

func TestHandler_UpdateApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantCode int
	}{
		{
			name:    "update existing application",
			appName: "my-app",
			body: map[string]any{
				"description": "Updated description",
			},
			wantCode: http.StatusOK,
		},
		{
			name:    "update non-existent application",
			appName: "not-found",
			body: map[string]any{
				"description": "Updated description",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "original", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPatch, "/applications/"+tt.appName, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_DeleteApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{
			name:     "delete existing application",
			appName:  "my-app",
			wantCode: http.StatusNoContent,
		},
		{
			name:     "delete non-existent application",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodDelete, "/applications/"+tt.appName, nil)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Use a path that doesn't match any operation (PUT is not supported)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPut, "/applications", nil)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/serverlessrepo/aws4_request")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateApplicationVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		path     string
		wantCode int
	}{
		{
			name: "creates version successfully",
			path: "/applications/my-app/versions/1.0.0",
			body: map[string]any{
				"sourceCodeUrl": "https://github.com/example/my-app",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "app not found returns 404",
			path:     "/applications/not-found/versions/1.0.0",
			body:     map[string]any{"sourceCodeUrl": "https://example.com"},
			wantCode: http.StatusNotFound,
		},
		{
			name:     "duplicate version returns conflict",
			path:     "/applications/my-app/versions/1.0.0",
			body:     map[string]any{"sourceCodeUrl": "https://example.com"},
			wantCode: http.StatusConflict,
		},
		{
			name:     "missing source URL returns bad request",
			path:     "/applications/my-app/versions/2.0.0",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "0.1.0", nil, "", "", "")
			require.NoError(t, err)

			if tt.wantCode == http.StatusConflict {
				_, err = h.Backend.CreateApplicationVersion("my-app", "1.0.0", "https://example.com", "")
				require.NoError(t, err)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodPut, tt.path, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusCreated {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "1.0.0", resp["semanticVersion"])
				assert.NotNil(t, resp["parameterDefinitions"])
				assert.NotNil(t, resp["requiredCapabilities"])
			}
		})
	}
}

func TestHandler_ListApplicationVersions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*serverlessrepo.Handler)
		name     string
		appName  string
		wantLen  int
		wantCode int
	}{
		{
			name:     "empty versions list",
			appName:  "my-app",
			wantLen:  0,
			wantCode: http.StatusOK,
		},
		{
			name:    "list with versions",
			appName: "my-app",
			setup: func(h *serverlessrepo.Handler) {
				_, _ = h.Backend.CreateApplicationVersion("my-app", "1.0.0", "https://example.com", "")
				_, _ = h.Backend.CreateApplicationVersion("my-app", "2.0.0", "https://example.com", "")
			},
			wantLen:  2,
			wantCode: http.StatusOK,
		},
		{
			name:     "app not found returns 404",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "0.1.0", nil, "", "", "")
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName+"/versions", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				versions, ok := resp["versions"].([]any)
				require.True(t, ok)
				assert.Len(t, versions, tt.wantLen)
			}
		})
	}
}

func TestHandler_CreateCloudFormationTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantCode int
	}{
		{
			name:    "creates template successfully",
			appName: "my-app",
			body: map[string]any{
				"semanticVersion": "1.0.0",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "app not found returns 404",
			appName:  "not-found",
			body:     map[string]any{},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/"+tt.appName+"/templates", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusCreated {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["templateId"])
				assert.Equal(t, "ACTIVE", resp["status"])
			}
		})
	}
}

func TestHandler_GetCloudFormationTemplate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		appName    string
		templateID string
		wantCode   int
	}{
		{
			name:     "gets template successfully",
			appName:  "my-app",
			wantCode: http.StatusOK,
		},
		{
			name:       "template not found returns 404",
			appName:    "my-app",
			templateID: "non-existent-template",
			wantCode:   http.StatusNotFound,
		},
		{
			name:       "app not found returns 404",
			appName:    "not-found",
			templateID: "some-template",
			wantCode:   http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			templateID := tt.templateID

			if tt.wantCode == http.StatusOK {
				tmpl, tmplErr := h.Backend.CreateCloudFormationTemplate("my-app", "1.0.0")
				require.NoError(t, tmplErr)
				templateID = tmpl.TemplateID
			}

			rec := doServerlessRepoRequest(
				t,
				h,
				http.MethodGet,
				"/applications/"+tt.appName+"/templates/"+templateID,
				nil,
			)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["templateId"])
				assert.Equal(t, "ACTIVE", resp["status"])
			}
		})
	}
}

func TestHandler_CreateCloudFormationChangeSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantCode int
	}{
		{
			name:    "creates change set successfully",
			appName: "my-app",
			body: map[string]any{
				"stackName":       "my-stack",
				"semanticVersion": "1.0.0",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:    "creates change set with custom name",
			appName: "my-app",
			body: map[string]any{
				"stackName":     "my-stack",
				"changeSetName": "my-changeset",
			},
			wantCode: http.StatusCreated,
		},
		{
			name:     "missing stackName returns bad request",
			appName:  "my-app",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:    "app not found returns 404",
			appName: "not-found",
			body: map[string]any{
				"stackName": "my-stack",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/"+tt.appName+"/changesets", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusCreated {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.NotEmpty(t, resp["changeSetId"])
				assert.NotEmpty(t, resp["stackId"])
			}
		})
	}
}

func TestHandler_GetApplicationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*serverlessrepo.Handler)
		name     string
		appName  string
		wantLen  int
		wantCode int
	}{
		{
			name:     "returns empty policy for new application",
			appName:  "my-app",
			wantLen:  0,
			wantCode: http.StatusOK,
		},
		{
			name:    "returns existing policy statements",
			appName: "my-app",
			setup: func(h *serverlessrepo.Handler) {
				_, _ = h.Backend.PutApplicationPolicy("my-app", []*serverlessrepo.ApplicationPolicyStatement{
					{Actions: []string{"deploy"}, Principals: []string{"*"}},
				})
			},
			wantLen:  1,
			wantCode: http.StatusOK,
		},
		{
			name:     "app not found returns 404",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName+"/policy", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stmts, ok := resp["statements"].([]any)
				require.True(t, ok)
				assert.Len(t, stmts, tt.wantLen)
			}
		})
	}
}

func TestHandler_PutApplicationPolicy(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantLen  int
		wantCode int
	}{
		{
			name:    "sets policy statements successfully",
			appName: "my-app",
			body: map[string]any{
				"statements": []map[string]any{
					{
						"actions":    []string{"deploy"},
						"principals": []string{"*"},
					},
				},
			},
			wantLen:  1,
			wantCode: http.StatusOK,
		},
		{
			name:    "sets multiple policy statements",
			appName: "my-app",
			body: map[string]any{
				"statements": []map[string]any{
					{
						"actions":     []string{"deploy"},
						"principals":  []string{"111111111111"},
						"statementId": "stmt-1",
					},
					{
						"actions":     []string{"deploy"},
						"principals":  []string{"222222222222"},
						"statementId": "stmt-2",
					},
				},
			},
			wantLen:  2,
			wantCode: http.StatusOK,
		},
		{
			name:    "app not found returns 404",
			appName: "not-found",
			body: map[string]any{
				"statements": []map[string]any{},
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPut, "/applications/"+tt.appName+"/policy", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				stmts, ok := resp["statements"].([]any)
				require.True(t, ok)
				assert.Len(t, stmts, tt.wantLen)
			}
		})
	}
}

func TestHandler_ListApplicationDependencies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		appName  string
		wantCode int
	}{
		{
			name:     "returns empty dependencies list",
			appName:  "my-app",
			wantCode: http.StatusOK,
		},
		{
			name:     "app not found returns 404",
			appName:  "not-found",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodGet, "/applications/"+tt.appName+"/dependencies", nil)
			assert.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				deps, ok := resp["dependencies"].([]any)
				require.True(t, ok)
				assert.Empty(t, deps)
			}
		})
	}
}

func TestHandler_UnshareApplication(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		appName  string
		wantCode int
	}{
		{
			name:    "unshares application successfully",
			appName: "my-app",
			body: map[string]any{
				"organizationId": "o-abc123",
			},
			wantCode: http.StatusNoContent,
		},
		{
			name:     "missing organizationId returns bad request",
			appName:  "my-app",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:    "app not found returns 404",
			appName: "not-found",
			body: map[string]any{
				"organizationId": "o-abc123",
			},
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil, "", "", "")
			require.NoError(t, err)

			rec := doServerlessRepoRequest(t, h, http.MethodPost, "/applications/"+tt.appName+"/unshare", tt.body)
			assert.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

func TestServerlessRepoPersistenceNewOps(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateApplication("app1", "desc1", "author1", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplicationVersion("app1", "1.0.0", "https://github.com/example", "")
	require.NoError(t, err)

	_, err = b.CreateCloudFormationTemplate("app1", "1.0.0")
	require.NoError(t, err)

	_, err = b.PutApplicationPolicy("app1", []*serverlessrepo.ApplicationPolicyStatement{
		{Actions: []string{"deploy"}, Principals: []string{"*"}},
	})
	require.NoError(t, err)

	// Snapshot
	h := serverlessrepo.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend
	b2 := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := serverlessrepo.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	assert.Equal(t, 1, serverlessrepo.ApplicationCount(b2))
	assert.Equal(t, 1, serverlessrepo.VersionCount(b2, "app1"))
	assert.Equal(t, 1, serverlessrepo.TemplateCount(b2, "app1"))
	assert.Equal(t, 1, serverlessrepo.PolicyStatementCount(b2, "app1"))
}

func TestHandler_HandlerOpsLen(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, 14, serverlessrepo.HandlerOpsLen(h))
}

func TestServerlessRepoPersistence(t *testing.T) {
	t.Parallel()

	b := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")

	_, err := b.CreateApplication("app1", "desc1", "author1", "", "1.0.0", nil, "", "", "")
	require.NoError(t, err)

	_, err = b.CreateApplication("app2", "desc2", "author2", "", "2.0.0", nil, "", "", "")
	require.NoError(t, err)

	// Snapshot
	h := serverlessrepo.NewHandler(b)
	snap := h.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	// Restore into a fresh backend
	b2 := serverlessrepo.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := serverlessrepo.NewHandler(b2)
	require.NoError(t, h2.Restore(t.Context(), snap))

	apps := b2.ListApplications()
	require.Len(t, apps, 2)
	assert.Equal(t, "app1", apps[0].Name)
	assert.Equal(t, "app2", apps[1].Name)
}
