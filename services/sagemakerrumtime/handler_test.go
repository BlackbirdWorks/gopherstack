package sagemakerrumtime_test

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
	"github.com/blackbirdworks/gopherstack/services/sagemakerrumtime"
)

func newTestHandler(t *testing.T) *sagemakerrumtime.Handler {
	t.Helper()

	return sagemakerrumtime.NewHandler(sagemakerrumtime.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *sagemakerrumtime.Handler,
	method, path string,
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

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// --- Handler metadata tests ---

func TestHandler_Name(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "SageMakerRuntime", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "sagemaker-runtime", h.ChaosServiceName())
}

func TestHandler_MatchPriority(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, service.PriorityPathVersioned, h.MatchPriority())
}

func TestHandler_GetSupportedOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	assert.Contains(t, ops, "InvokeEndpoint")
	assert.Contains(t, ops, "InvokeEndpointAsync")
	assert.Contains(t, ops, "InvokeEndpointWithResponseStream")
}

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, h.GetSupportedOperations(), h.ChaosOperations())
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, []string{"us-east-1"}, h.ChaosRegions())
}

// --- InvokeEndpoint tests ---

func TestHandler_InvokeEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         any
		name         string
		endpointName string
		wantCode     int
	}{
		{
			name:         "basic_invocation",
			endpointName: "my-endpoint",
			body:         map[string]any{"data": "test input"},
			wantCode:     http.StatusOK,
		},
		{
			name:         "empty_body",
			endpointName: "my-endpoint",
			body:         nil,
			wantCode:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/endpoints/"+tt.endpointName+"/invocations", tt.body)

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
			assert.Equal(t, "AllTraffic", rec.Header().Get("X-Amzn-Invoked-Production-Variant"))

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeEndpoint", invocations[0].Operation)
			assert.Equal(t, tt.endpointName, invocations[0].EndpointName)
		})
	}
}

// --- InvokeEndpointAsync tests ---

func TestHandler_InvokeEndpointAsync(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/endpoints/my-endpoint/async-invocations",
		map[string]any{"data": "async input"})

	assert.Equal(t, http.StatusAccepted, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "InferenceId")
	assert.Contains(t, out, "OutputLocation")

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeEndpointAsync", invocations[0].Operation)
	assert.Equal(t, "my-endpoint", invocations[0].EndpointName)
}

// --- InvokeEndpointWithResponseStream tests ---

func TestHandler_InvokeEndpointWithResponseStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost,
		"/endpoints/my-endpoint/invocations-response-stream",
		map[string]any{"data": "stream input"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	// Verify the response is a valid AWS event stream frame (at least minimum length).
	body := rec.Body.Bytes()
	assert.Greater(t, len(body), 12, "response should contain at least a prelude")

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeEndpointWithResponseStream", invocations[0].Operation)
	assert.Equal(t, "my-endpoint", invocations[0].EndpointName)
}

// --- Error path tests ---

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/endpoints/my-endpoint/invocations", nil)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingEndpointName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/endpoints//invocations", bytes.NewReader(nil))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/endpoints/my-endpoint/unknown-op", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- ExtractOperation / ExtractResource tests ---

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{
			name:   "invoke_endpoint",
			path:   "/endpoints/my-endpoint/invocations",
			wantOp: "InvokeEndpoint",
		},
		{
			name:   "invoke_endpoint_async",
			path:   "/endpoints/my-endpoint/async-invocations",
			wantOp: "InvokeEndpointAsync",
		},
		{
			name:   "invoke_with_response_stream",
			path:   "/endpoints/my-endpoint/invocations-response-stream",
			wantOp: "InvokeEndpointWithResponseStream",
		},
		{
			name:   "unknown_path",
			path:   "/endpoints/my-endpoint/unknown",
			wantOp: "Unknown",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
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
			name:         "endpoint_name",
			path:         "/endpoints/my-endpoint/invocations",
			wantResource: "my-endpoint",
		},
		{
			name:         "different_endpoint",
			path:         "/endpoints/prod-endpoint/async-invocations",
			wantResource: "prod-endpoint",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

// --- Backend tests ---

func TestBackend_RecordAndList(t *testing.T) {
	t.Parallel()

	b := sagemakerrumtime.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", b.Region())

	invocations := b.ListInvocations()
	assert.Empty(t, invocations)

	inv := b.RecordInvocation("InvokeEndpoint", "my-endpoint", `{"data":"hi"}`, `{"Body":"ok"}`)
	assert.Equal(t, "InvokeEndpoint", inv.Operation)
	assert.Equal(t, "my-endpoint", inv.EndpointName)
	assert.NotZero(t, inv.CreatedAt)

	invocations = b.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeEndpoint", invocations[0].Operation)
}

// --- Provider tests ---

func TestProvider_Init(t *testing.T) {
	t.Parallel()

	p := &sagemakerrumtime.Provider{}
	assert.Equal(t, "SageMakerRuntime", p.Name())

	backend := sagemakerrumtime.NewInMemoryBackend("000000000000", "us-east-1")
	h := sagemakerrumtime.NewHandler(backend)

	assert.NotNil(t, h)
	assert.Equal(t, "SageMakerRuntime", h.Name())
	assert.Equal(t, "us-east-1", backend.Region())
}

func TestProvider_InitFull(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{}
	p := &sagemakerrumtime.Provider{}
	reg, err := p.Init(ctx)

	require.NoError(t, err)
	require.NotNil(t, reg)
	assert.Equal(t, "SageMakerRuntime", reg.Name())
}

// --- RouteMatcher tests ---

func TestHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{
			name:  "matches invocations path",
			path:  "/endpoints/my-endpoint/invocations",
			match: true,
		},
		{
			name:  "matches async-invocations path",
			path:  "/endpoints/my-endpoint/async-invocations",
			match: true,
		},
		{
			name:  "matches response-stream path",
			path:  "/endpoints/my-endpoint/invocations-response-stream",
			match: true,
		},
		{
			name:  "does not match other path",
			path:  "/queues/myqueue",
			match: false,
		},
		{
			name:  "does not match root path",
			path:  "/",
			match: false,
		},
	}

	matcher := h.RouteMatcher()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}
