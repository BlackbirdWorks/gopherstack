package apigateway_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/apigateway"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

// TestHandler_APIGateway_RouteMatcher_ProxyPaths covers gopherstack-z4lc9:
// RouteMatcher must claim /proxy/{apiId}/{stage}/... the same way Handler()
// already special-cases it, or the request never reaches apigateway at all.
func TestHandler_APIGateway_RouteMatcher_ProxyPaths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		path      string
		wantMatch bool
	}{
		{name: "proxy_stage_invoke_with_resource_path", path: "/proxy/abc123/prod/items", wantMatch: true},
		{name: "proxy_stage_invoke_root", path: "/proxy/abc123/prod", wantMatch: true},
		{name: "existing_restapis_prefix_unaffected", path: "/restapis/abc123", wantMatch: true},
		{name: "s3_bucket_path_not_claimed", path: "/some-bucket/some-key", wantMatch: false},
		{name: "proxy_without_trailing_slash_not_claimed", path: "/proxyfoo", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigateway.NewHandler(apigateway.NewInMemoryBackend())
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			ctx := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantMatch, h.RouteMatcher()(ctx))
		})
	}
}

// jsonAction POSTs an APIGateway JSON-protocol action to baseURL over a plain
// HTTP client and decodes the response body into out (if non-nil).
func jsonAction(t *testing.T, client *http.Client, baseURL, action, body string, out any) int {
	t.Helper()

	req, err := http.NewRequest(http.MethodPost, baseURL+"/", strings.NewReader(body))
	require.NoError(t, err)
	req.Header.Set("X-Amz-Target", "APIGateway."+action)

	resp, err := client.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	if out != nil && len(respBody) > 0 {
		require.NoError(t, json.Unmarshal(respBody, out))
	}

	return resp.StatusCode
}

// setupDeployedMockAPI creates a REST API with a MOCK integration on its root
// resource, deployed to stage "prod", driving every step over a plain HTTP
// client against srv -- exactly the sequence a real aws-sdk-go-v2 apigateway
// client followed by a stage invoke performs.
func setupDeployedMockAPI(t *testing.T, client *http.Client, baseURL string) string {
	t.Helper()

	var createResp struct {
		ID string `json:"id"`
	}
	require.Equal(t, http.StatusCreated,
		jsonAction(t, client, baseURL, "CreateRestApi", `{"name":"proxyroute-e2e"}`, &createResp))
	require.NotEmpty(t, createResp.ID)

	var resourcesResp struct {
		Items []struct {
			ID   string `json:"id"`
			Path string `json:"path"`
		} `json:"item"`
	}
	require.Equal(t, http.StatusOK,
		jsonAction(t, client, baseURL, "GetResources", `{"restApiId":"`+createResp.ID+`"}`, &resourcesResp))

	var rootID string
	for _, r := range resourcesResp.Items {
		if r.Path == "/" {
			rootID = r.ID

			break
		}
	}
	require.NotEmpty(t, rootID, "root resource not found")

	require.Equal(t, http.StatusCreated, jsonAction(t, client, baseURL, "PutMethod",
		`{"restApiId":"`+createResp.ID+`","resourceId":"`+rootID+`","httpMethod":"GET","authorizationType":"NONE"}`,
		nil))

	require.Equal(t, http.StatusCreated, jsonAction(t, client, baseURL, "PutIntegration",
		`{"restApiId":"`+createResp.ID+`","resourceId":"`+rootID+`","httpMethod":"GET","type":"MOCK"}`, nil))

	require.Equal(t, http.StatusCreated, jsonAction(t, client, baseURL, "CreateDeployment",
		`{"restApiId":"`+createResp.ID+`","stageName":"prod","description":"e2e"}`, nil))

	return createResp.ID
}

// newProxyRoutingServer wires apigateway and S3 through the real,
// priority-ordered pkgs/service.Router -- the same collision surface a live
// gopherstack server has, and the one the unit-level RouteMatcher test above
// cannot exercise on its own.
func newProxyRoutingServer(t *testing.T) *httptest.Server {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(apigateway.NewHandler(apigateway.NewInMemoryBackend())))
	require.NoError(t, registry.Register(s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{}))))

	router := service.NewServiceRouter(registry)
	e.Use(router.RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv
}

// TestProxyInvoke_RoutesToAPIGateway_NotS3CatchAll is the end-to-end
// regression guard for gopherstack-z4lc9: it drives the real, multi-service
// router (apigateway + S3) exactly as a live server does, hits the stage
// invoke URL apigateway's own store.go:stageInvokeURL advertises, and checks
// it lands on the MOCK integration's response -- not S3's NoSuchBucket. It
// also drives an ordinary S3 bucket path through the same router as the
// collision control: broadening apigateway's claim must not swallow S3's
// traffic. Both requests go out over a plain net/http client against a real
// httptest.Server, matching how a real stage invoke or S3 client would call.
// Fails against unfixed code: the proxy case returns S3's 404 NoSuchBucket.
func TestProxyInvoke_RoutesToAPIGateway_NotS3CatchAll(t *testing.T) {
	t.Parallel()

	srv := newProxyRoutingServer(t)
	client := srv.Client()
	apiID := setupDeployedMockAPI(t, client, srv.URL)

	tests := []struct {
		name            string
		path            string
		wantBodyExclude string
		wantStatus      int
	}{
		{
			name:            "proxy_stage_invoke_reaches_apigateway_mock_integration",
			path:            "/proxy/" + apiID + "/prod",
			wantStatus:      http.StatusOK,
			wantBodyExclude: "NoSuchBucket",
		},
		{
			name:       "unrelated_bucket_path_still_reaches_s3",
			path:       "/proxy-route-e2e-control-bucket",
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := client.Get(srv.URL + tt.path)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, resp.StatusCode)

			if tt.wantBodyExclude != "" {
				assert.NotContains(t, string(body), tt.wantBodyExclude)
			} else {
				assert.Contains(t, string(body), "NoSuchBucket")
			}
		})
	}
}
