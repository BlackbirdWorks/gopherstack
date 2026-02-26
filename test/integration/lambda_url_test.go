package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	lambdapkg "github.com/blackbirdworks/gopherstack/lambda"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	// urlTestPortStart is the start of the port range for Lambda function URL tests.
	urlTestPortStart = 21000
	// urlTestPortEnd is the exclusive end of the Lambda function URL test port range.
	urlTestPortEnd = 21100
)

// TestLambdaFunctionURL_CreateGetDelete tests the full lifecycle of a Lambda function URL
// configuration: create, get, and delete via the REST API.
func TestLambdaFunctionURL_CreateGetDelete(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	pa, err := portalloc.New(urlTestPortStart, urlTestPortEnd)
	require.NoError(t, err)

	backend := lambdapkg.NewInMemoryBackend(nil, pa, lambdapkg.DefaultSettings(), "000000000000", "us-east-1", slog.Default())
	handler := lambdapkg.NewHandler(backend, slog.Default())
	handler.AccountID = "000000000000"
	handler.DefaultRegion = "us-east-1"

	e := echo.New()
	e.Pre(logger.EchoMiddleware(slog.Default()))
	registry := service.NewRegistry(slog.Default())
	require.NoError(t, registry.Register(handler))
	e.Use(service.NewServiceRouter(registry).RouteHandler())
	server := httptest.NewServer(e)
	t.Cleanup(server.Close)

	const fnName = "url-lifecycle-fn"

	// Create function
	createBody, err := json.Marshal(map[string]any{
		"FunctionName": fnName,
		"PackageType":  "Image",
		"Code":         map[string]string{"ImageUri": "test:latest"},
		"Role":         "arn:aws:iam::000000000000:role/test",
	})
	require.NoError(t, err)

	resp, err := doLambdaRequest(ctx, http.MethodPost, server.URL+"/2015-03-31/functions",
		"application/json", bytes.NewReader(createBody))
	require.NoError(t, err)
	defer resp.Body.Close()
	require.Equal(t, http.StatusCreated, resp.StatusCode)

	// Create function URL config
	urlBody, err := json.Marshal(map[string]any{"AuthType": "NONE"})
	require.NoError(t, err)

	urlResp, err := doLambdaRequest(ctx, http.MethodPost,
		fmt.Sprintf("%s/2015-03-31/functions/%s/url", server.URL, fnName),
		"application/json", bytes.NewReader(urlBody))
	require.NoError(t, err)
	defer urlResp.Body.Close()
	require.Equal(t, http.StatusCreated, urlResp.StatusCode)

	var cfg lambdapkg.FunctionURLConfig
	respBody, err := io.ReadAll(urlResp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(respBody, &cfg))

	assert.NotEmpty(t, cfg.FunctionURL, "FunctionURL should not be empty")
	assert.Equal(t, "NONE", cfg.AuthType)
	assert.NotEmpty(t, cfg.FunctionArn)
	assert.NotEmpty(t, cfg.CreationTime)

	t.Logf("function URL: %s", cfg.FunctionURL)

	// Get function URL config
	getResp, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2015-03-31/functions/%s/url", server.URL, fnName),
		"", nil)
	require.NoError(t, err)
	defer getResp.Body.Close()
	require.Equal(t, http.StatusOK, getResp.StatusCode)

	var getCfg lambdapkg.FunctionURLConfig
	getBody, err := io.ReadAll(getResp.Body)
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(getBody, &getCfg))
	assert.Equal(t, cfg.FunctionURL, getCfg.FunctionURL)

	// Delete function URL config
	delResp, err := doLambdaRequest(ctx, http.MethodDelete,
		fmt.Sprintf("%s/2015-03-31/functions/%s/url", server.URL, fnName),
		"", nil)
	require.NoError(t, err)
	defer delResp.Body.Close()
	assert.Equal(t, http.StatusNoContent, delResp.StatusCode)

	// Get after delete should 404
	getAfterDel, err := doLambdaRequest(ctx, http.MethodGet,
		fmt.Sprintf("%s/2015-03-31/functions/%s/url", server.URL, fnName),
		"", nil)
	require.NoError(t, err)
	defer getAfterDel.Body.Close()
	assert.Equal(t, http.StatusNotFound, getAfterDel.StatusCode)
}

// TestLambdaFunctionURL_HTTPEndpoint verifies that the HTTP listener on the allocated port
// is reachable and returns an expected response after creating a function URL.
func TestLambdaFunctionURL_HTTPEndpoint(t *testing.T) {
	t.Parallel()

	pa, err := portalloc.New(urlTestPortStart+50, urlTestPortEnd)
	require.NoError(t, err)

	backend := lambdapkg.NewInMemoryBackend(nil, pa, lambdapkg.DefaultSettings(), "000000000000", "us-east-1", slog.Default())

	const fnName = "http-endpoint-fn"

	fn := &lambdapkg.FunctionConfiguration{
		FunctionName: fnName,
		PackageType:  lambdapkg.PackageTypeImage,
		ImageURI:     "test:latest",
	}
	require.NoError(t, backend.CreateFunction(fn))

	cfg, createErr := backend.CreateFunctionURLConfig(fnName, "NONE")
	require.NoError(t, createErr)
	assert.NotEmpty(t, cfg.FunctionURL)

	// The URL listener is running; an HTTP request should be handled
	// (may fail with invocation error since Docker isn't available,
	// but the listener itself should respond).
	client := &http.Client{Timeout: 5 * time.Second}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, cfg.FunctionURL, nil)
	require.NoError(t, err)

	httpResp, err := client.Do(req)
	require.NoError(t, err)
	defer httpResp.Body.Close()

	// The listener must respond (any status code means the listener is up).
	assert.NotZero(t, httpResp.StatusCode, "listener should respond to HTTP requests")
	t.Logf("function URL HTTP status: %d", httpResp.StatusCode)
}
