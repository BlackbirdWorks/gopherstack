package ecr_test

// docker_registry_test.go — verifies the embedded Docker Registry v2 proxy
// (docker_registry.go): the /v2/ path routes to the registry handler instead
// of the JSON control plane when GOPHERSTACK_ENABLE_LOCAL_REGISTRY is set.

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/ecr"
)

func TestProvider_Init_WithLocalRegistry(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)
	assert.NotNil(t, svc)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)
	assert.True(t, h.RegistryEnabled())
}

func TestRouteMatcher_V2Path_WithRegistryEnabled(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.True(t, h.RouteMatcher()(c))
}

func TestHandler_V2Path_ProxiesRegistry(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err = h.Handler()(c)
	require.NoError(t, err)
	// Distribution registry responds 200 for /v2/ when no auth is configured.
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestExtractOperation_V2Path_WithRegistryEnabled(t *testing.T) {
	t.Setenv("GOPHERSTACK_ENABLE_LOCAL_REGISTRY", "1")

	p := &ecr.Provider{}
	svc, err := p.Init(&service.AppContext{Logger: slog.Default()})
	require.NoError(t, err)

	h, ok := svc.(*ecr.Handler)
	require.True(t, ok)

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/v2/", nil)
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "RegistryV2", h.ExtractOperation(c))
}

// TestExtractResource_V2Path checks that ExtractResource extracts the
// repository name from the URL path instead of reading the request body.
func TestExtractResource_V2Path(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	// Pass a no-op http.Handler to enable registry mode.
	h := ecr.NewHandler(backend, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		path string
		want string
	}{
		{path: "/v2/my-repo/manifests/latest", want: "my-repo"},
		{path: "/v2/org/app/blobs/sha256:abc", want: "org"},
		{path: "/v2/", want: ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

// TestRouteMatcher_V2Strict checks that the route matcher does not match
// paths that start with "/v2" but are not the Docker registry v2 API
// (e.g. S3Control's "/v20180820/..." paths).
func TestRouteMatcher_V2Strict(t *testing.T) {
	t.Parallel()

	backend := ecr.NewInMemoryBackend(testAccountID, testRegion, testEndpoint)
	// Pass a no-op http.Handler to enable registry mode.
	h := ecr.NewHandler(backend, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))

	tests := []struct {
		path      string
		wantMatch bool
	}{
		{path: "/v2/", wantMatch: true},
		{path: "/v2/my-repo/manifests/latest", wantMatch: true},
		{path: "/v2", wantMatch: true},
		{path: "/v20180820/bucket", wantMatch: false},
		{path: "/v2abc/something", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}
}
