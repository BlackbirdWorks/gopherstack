package apigatewayv2_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/s3"
)

func TestExecuteAPIInvokeURLs(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	h.SetLambdaInvoker(&mockLambdaInvoker{})

	apiID := buildHTTPAPIWithLambda(t, h, "GET /hello",
		"arn:aws:lambda:us-east-1:123456789012:function:exec-fn/invocations")
	ensureDefaultStage(t, h, apiID)

	rr := doRequest(t, h, http.MethodPost, "/v2/apis/"+apiID+"/stages", map[string]any{
		"stageName": "dev", "autoDeploy": true,
	})
	require.Equal(t, http.StatusCreated, rr.Code)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	require.NoError(t, registry.Register(s3.NewHandler(s3.NewInMemoryBackend(&s3.GzipCompressor{}))))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	host := apiID + ".execute-api.localhost.localstack.cloud:4566"

	tests := []struct {
		name       string
		host       string
		path       string
		wantStatus int
		wantBody   string
	}{
		{name: "host_default_stage", host: host, path: "/hello", wantStatus: http.StatusOK, wantBody: "ok"},
		{name: "host_named_stage", host: host, path: "/dev/hello", wantStatus: http.StatusOK, wantBody: "ok"},
		{name: "internal_default_stage", path: "/_aws/execute-api/" + apiID + "/hello", wantStatus: http.StatusOK, wantBody: "ok"},
		{name: "internal_explicit_default", path: "/_aws/execute-api/" + apiID + "/$default/hello", wantStatus: http.StatusOK, wantBody: "ok"},
		{name: "internal_named_stage", path: "/_aws/execute-api/" + apiID + "/dev/hello", wantStatus: http.StatusOK, wantBody: "ok"},
		{name: "unknown_api", host: "zzzzzzzz.execute-api.localhost.localstack.cloud", path: "/hello", wantStatus: http.StatusNotFound, wantBody: "NoSuchBucket"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req, err := http.NewRequest(http.MethodGet, srv.URL+tt.path, nil)
			require.NoError(t, err)

			if tt.host != "" {
				req.Host = tt.host
			}

			resp, err := srv.Client().Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()

			body, err := io.ReadAll(resp.Body)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, resp.StatusCode, string(body))
			assert.Contains(t, string(body), tt.wantBody)
		})
	}
}
