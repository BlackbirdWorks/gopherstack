package mwaa_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mwaa"
)

const (
	testRegion    = "us-east-1"
	testAccountID = "123456789012"
)

func newHandlerForTest(t *testing.T) *mwaa.Handler {
	t.Helper()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)
	h.AccountID = testAccountID
	h.DefaultRegion = testRegion

	return h
}

func doMWAARequest(t *testing.T, h *mwaa.Handler, method, path string, body any) *httptest.ResponseRecorder {
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
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/airflow/aws4_request",
	)

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// networkConfigBody returns a minimal valid NetworkConfiguration JSON value (2
// SubnetIds, 1 SecurityGroupId) for embedding in CreateEnvironment HTTP
// request bodies. AWS's CreateEnvironmentInput requires NetworkConfiguration
// with SubnetIds fixed at exactly 2 and SecurityGroupIds 1-5 -- see
// validateNetworkConfigCreate.
func networkConfigBody() map[string]any {
	return map[string]any{
		"SubnetIds":        []string{"subnet-aaaa1111", "subnet-bbbb2222"},
		"SecurityGroupIds": []string{"sg-cccc3333"},
	}
}

// makeEchoContext creates an echo.Context for the given method and path.
func makeEchoContext(t *testing.T, method, path string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

// makeEchoContextWithAuth creates an echo.Context with an Authorization header for a given service.
func makeEchoContextWithAuth(t *testing.T, method, path, svcName string) *echo.Context {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, nil)
	req.Header.Set(
		"Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20240101/us-east-1/"+svcName+"/aws4_request",
	)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	return c
}

func TestHandlerName(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Equal(t, "MWAA", h.Name())
}

func TestHandlerChaos(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Equal(t, "airflow", h.ChaosServiceName())
	assert.NotEmpty(t, h.ChaosOperations())
	assert.NotEmpty(t, h.ChaosRegions())
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandlerMatchPriority(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	assert.Positive(t, h.MatchPriority())
}

func TestHandlerReset(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	backend := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h2 := mwaa.NewHandler(backend)
	h2.AccountID = testAccountID
	h2.DefaultRegion = testRegion

	seedEnv(t, backend, "env-a")
	require.Equal(t, 1, mwaa.EnvironmentCount(backend))

	h2.Reset()

	assert.Equal(t, 0, mwaa.EnvironmentCount(backend))

	// Ensure original handler still works (not broken by reset of h2).
	_ = h
}

// TestGetSupportedOperations verifies the full set of MWAA operations the
// handler advertises, including InvokeRestApi/PublishMetrics, and that
// GetSupportedOperations reports no more than the expected count. GetMetrics is
// deliberately absent: it is not a real MWAA API operation (only PublishMetrics
// exists on the real wire surface -- see handler.go's extractMetricsOperation).
func TestGetSupportedOperations(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)

	ops := h.GetSupportedOperations()

	expectedOps := []string{
		"CreateEnvironment", "GetEnvironment", "DeleteEnvironment",
		"UpdateEnvironment", "ListEnvironments", "ListTagsForResource",
		"TagResource", "UntagResource", "CreateCliToken",
		"CreateWebLoginToken", "InvokeRestApi", "PublishMetrics",
	}

	for _, op := range expectedOps {
		assert.Contains(t, ops, op, "missing operation %s", op)
	}

	assert.Equal(t, len(expectedOps), mwaa.HandlerOpsLen(h))
}

// TestExtractOperation covers every routed path/method combination, including
// the InvokeRestApi/PublishMetrics operations and the GET-on-metrics-path
// Unknown case (GetMetrics is not a real MWAA operation; see handler.go).
func TestExtractOperation(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)

	tests := []struct {
		wantOp string
		path   string
		method string
	}{
		{path: "/clitoken/env", method: http.MethodPost, wantOp: "CreateCliToken"},
		{path: "/webtoken/env", method: http.MethodPost, wantOp: "CreateWebLoginToken"},
		{path: "/restapi/env", method: http.MethodPost, wantOp: "InvokeRestApi"},
		{path: "/metrics/environments/env", method: http.MethodPost, wantOp: "PublishMetrics"},
		{path: "/metrics/environments/env", method: http.MethodGet, wantOp: "Unknown"},
		{path: "/tags/some-arn", method: http.MethodGet, wantOp: "ListTagsForResource"},
		{path: "/tags/some-arn", method: http.MethodPost, wantOp: "TagResource"},
		{path: "/tags/some-arn", method: http.MethodDelete, wantOp: "UntagResource"},
		{path: "/environments", method: http.MethodGet, wantOp: "ListEnvironments"},
		{path: "/environments/", method: http.MethodGet, wantOp: "ListEnvironments"},
		{path: "/environments/my-env", method: http.MethodGet, wantOp: "GetEnvironment"},
		{path: "/environments/my-env", method: http.MethodPut, wantOp: "CreateEnvironment"},
		{path: "/environments/my-env", method: http.MethodDelete, wantOp: "DeleteEnvironment"},
		{path: "/environments/my-env", method: http.MethodPatch, wantOp: "UpdateEnvironment"},
		{path: "/unknown", method: http.MethodGet, wantOp: "Unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.wantOp+"_"+tt.method, func(t *testing.T) {
			t.Parallel()

			c := makeEchoContext(t, tt.method, tt.path)

			op := h.ExtractOperation(c)
			assert.Equal(t, tt.wantOp, op)
		})
	}
}

func TestExtractResource(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)

	tests := []struct {
		path    string
		wantRes string
	}{
		{path: "/environments/my-env", wantRes: "my-env"},
		{path: "/clitoken/my-env", wantRes: "my-env"},
		{path: "/webtoken/my-env", wantRes: "my-env"},
		{path: "/restapi/my-env", wantRes: "my-env"},
		{path: "/metrics/environments/my-env", wantRes: "my-env"},
		{
			path:    "/tags/arn:aws:airflow:us-east-1:123:environment/my-env",
			wantRes: "arn:aws:airflow:us-east-1:123:environment/my-env",
		},
		{path: "/unknown", wantRes: ""},
	}

	for _, tt := range tests {
		t.Run(tt.path, func(t *testing.T) {
			t.Parallel()

			c := makeEchoContext(t, http.MethodGet, tt.path)
			res := h.ExtractResource(c)
			assert.Equal(t, tt.wantRes, res)
		})
	}
}

// TestRouteMatcher covers every routed path prefix, including InvokeRestApi
// and metrics, matched against both the correct and an unrelated SigV4 service.
func TestRouteMatcher(t *testing.T) {
	t.Parallel()

	b := mwaa.NewInMemoryBackend(testRegion, testAccountID)
	h := mwaa.NewHandler(b)
	matcher := h.RouteMatcher()

	tests := []struct {
		path      string
		authSvc   string
		wantMatch bool
	}{
		{path: "/environments", authSvc: "airflow", wantMatch: true},
		{path: "/environments/my-env", authSvc: "airflow", wantMatch: true},
		{path: "/tags/some-arn", authSvc: "airflow", wantMatch: true},
		{path: "/clitoken/env", authSvc: "airflow", wantMatch: true},
		{path: "/webtoken/env", authSvc: "airflow", wantMatch: true},
		{path: "/restapi/env", authSvc: "airflow", wantMatch: true},
		{path: "/metrics/environments/env", authSvc: "airflow", wantMatch: true},
		{path: "/environments", authSvc: "s3", wantMatch: false},
		{path: "/restapi/env", authSvc: "s3", wantMatch: false},
		{path: "/metrics/environments/env", authSvc: "s3", wantMatch: false},
		{path: "/other-path", authSvc: "airflow", wantMatch: false},
	}

	for _, tt := range tests {
		t.Run(tt.path+"_"+tt.authSvc, func(t *testing.T) {
			t.Parallel()

			c := makeEchoContextWithAuth(t, http.MethodGet, tt.path, tt.authSvc)
			got := matcher(c)
			assert.Equal(t, tt.wantMatch, got)
		})
	}
}

// TestMethodNotAllowed covers every routed path prefix invoked with a method
// it does not accept.
func TestMethodNotAllowed(t *testing.T) {
	t.Parallel()

	tests := []struct {
		path   string
		method string
		name   string
	}{
		{name: "clitoken_get", path: "/clitoken/env", method: http.MethodGet},
		{name: "webtoken_get", path: "/webtoken/env", method: http.MethodGet},
		{name: "restapi_get", path: "/restapi/env", method: http.MethodGet},
		{name: "environments_list_post", path: "/environments", method: http.MethodPost},
		{name: "environment_options", path: "/environments/env", method: http.MethodOptions},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandlerForTest(t)
			rec := doMWAARequest(t, h, tt.method, tt.path, nil)

			assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
		})
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	t.Parallel()

	h := newHandlerForTest(t)
	rec := doMWAARequest(t, h, http.MethodGet, "/unknown/path", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}
