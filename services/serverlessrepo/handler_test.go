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
			wantCode: http.StatusOK,
			wantARN:  true,
		},
		{
			name:     "missing name returns bad request",
			body:     map[string]any{"description": "No name"},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "duplicate application returns conflict",
			body: map[string]any{
				"name": "existing-app",
			},
			wantCode: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCode == http.StatusConflict {
				_, err := h.Backend.CreateApplication("existing-app", "", "", "", "", nil)
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

			_, err := h.Backend.CreateApplication("existing-app", "desc", "author", "", "1.0.0", nil)
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
				_, _ = h.Backend.CreateApplication("app-a", "A", "author", "", "1.0.0", nil)
				_, _ = h.Backend.CreateApplication("app-b", "B", "author", "", "1.0.0", nil)
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
			_, err := h.Backend.CreateApplication("my-app", "original", "author", "", "1.0.0", nil)
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
			_, err := h.Backend.CreateApplication("my-app", "desc", "author", "", "1.0.0", nil)
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
