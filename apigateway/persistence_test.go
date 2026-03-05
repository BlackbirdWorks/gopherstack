package apigateway_test

import (
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/apigateway"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *apigateway.InMemoryBackend) string
		verify func(t *testing.T, b *apigateway.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *apigateway.InMemoryBackend) string {
				api, err := b.CreateRestAPI("test-api", "test", nil)
				if err != nil {
					return ""
				}

				return api.ID
			},
			verify: func(t *testing.T, b *apigateway.InMemoryBackend, id string) {
				t.Helper()

				api, err := b.GetRestAPI(id)
				require.NoError(t, err)
				assert.Equal(t, "test-api", api.Name)
				assert.Equal(t, id, api.ID)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *apigateway.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *apigateway.InMemoryBackend, _ string) {
				t.Helper()

				apis, _, err := b.GetRestAPIs(0, "")
				require.NoError(t, err)
				assert.Empty(t, apis)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := apigateway.NewInMemoryBackend()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := apigateway.NewInMemoryBackend()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}

func TestAPIGatewayHandler_Persistence(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)

	_, err := backend.CreateRestAPI("snap-api", "test", nil)
	require.NoError(t, err)

	snap := h.Snapshot()
	require.NotNil(t, snap)

	fresh := apigateway.NewInMemoryBackend()
	freshH := apigateway.NewHandler(fresh)
	require.NoError(t, freshH.Restore(snap))

	apis, _, err := fresh.GetRestAPIs(0, "")
	require.NoError(t, err)
	assert.Len(t, apis, 1)
}

func TestAPIGatewayHandler_Routing(t *testing.T) {
	t.Parallel()

	h := apigateway.NewHandler(apigateway.NewInMemoryBackend())

	assert.Equal(t, "APIGateway", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		path      string
		target    string
		wantMatch bool
	}{
		{"target match", "/", "APIGateway.GetRestApis", true},
		{"rest path match", "/restapis", "", true},
		{"no match", "/other", "", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			req.Header.Set("X-Amz-Target", tt.target)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
		})
	}

	// Test ExtractOperation
	req := httptest.NewRequest(http.MethodGet, "/restapis", nil)
	req.Header.Set("X-Amz-Target", "APIGateway.GetRestApis")
	c := e.NewContext(req, httptest.NewRecorder())
	assert.Equal(t, "GetRestApis", h.ExtractOperation(c))
}

func TestAPIGatewayBackend_DeploymentOperations(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()

	// Create a REST API first
	api, err := b.CreateRestAPI("deploy-api", "test", nil)
	require.NoError(t, err)

	// Create deployment
	dep, err := b.CreateDeployment(api.ID, "prod", "initial deployment")
	require.NoError(t, err)
	require.NotEmpty(t, dep.ID)

	// Get deployment
	got, err := b.GetDeployment(api.ID, dep.ID)
	require.NoError(t, err)
	assert.Equal(t, dep.ID, got.ID)

	// Delete deployment
	err = b.DeleteDeployment(api.ID, dep.ID)
	require.NoError(t, err)

	// Get should fail after delete
	_, err = b.GetDeployment(api.ID, dep.ID)
	require.Error(t, err)
}

func TestAPIGatewayBackend_GetDeployment_NotFound(t *testing.T) {
	t.Parallel()

	b := apigateway.NewInMemoryBackend()
	api, err := b.CreateRestAPI("test-api", "", nil)
	require.NoError(t, err)

	_, err = b.GetDeployment(api.ID, "nonexistent")
	require.Error(t, err)
}

func TestAPIGatewayHandler_RESTPath(t *testing.T) {
	t.Parallel()

	backend := apigateway.NewInMemoryBackend()
	h := apigateway.NewHandler(backend)
	e := echo.New()

	// GET /restapis → GetRestApis
	req := httptest.NewRequest(http.MethodGet, "/restapis", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusOK, rec.Code)

	// POST /restapis → CreateRestApi
	body := `{"name":"rest-api","description":"test"}`
	req2 := httptest.NewRequest(http.MethodPost, "/restapis", strings.NewReader(body))
	req2.Header.Set("Content-Type", "application/json")
	rec2 := httptest.NewRecorder()
	c2 := e.NewContext(req2, rec2)
	require.NoError(t, h.Handler()(c2))
	assert.Contains(t, []int{http.StatusOK, http.StatusCreated}, rec2.Code)
}
