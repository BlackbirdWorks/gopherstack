package sagemakerruntime_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	awscfg "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	sagemakerruntimesdk "github.com/aws/aws-sdk-go-v2/service/sagemakerruntime"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/sagemakerruntime"
)

func newTestHandler(t *testing.T) *sagemakerruntime.Handler {
	t.Helper()

	return sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *sagemakerruntime.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	return doRequestWithHeaders(t, h, method, path, body, nil)
}

func doRequestWithHeaders(
	t *testing.T,
	h *sagemakerruntime.Handler,
	method, path string,
	body any,
	headers map[string]string,
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
	for name, value := range headers {
		req.Header.Set(name, value)
	}

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func newTestSDKClient(t *testing.T, h *sagemakerruntime.Handler) *sagemakerruntimesdk.Client {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	cfg, err := awscfg.LoadDefaultConfig(
		t.Context(),
		awscfg.WithRegion("us-east-1"),
		awscfg.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
	)
	require.NoError(t, err)

	return sagemakerruntimesdk.NewFromConfig(cfg, func(o *sagemakerruntimesdk.Options) {
		o.BaseEndpoint = aws.String(srv.URL)
	})
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
		body            any
		headers         map[string]string
		name            string
		endpointName    string
		wantContentType string
		wantVariant     string
		wantSession     bool
	}{
		{
			name:            "default_opaque_response",
			endpointName:    "my-endpoint",
			body:            map[string]any{"data": "test input"},
			wantContentType: "application/octet-stream",
			wantVariant:     "AllTraffic",
		},
		{
			name:         "bound_headers_and_new_session",
			endpointName: "session-endpoint",
			body:         nil,
			headers: map[string]string{
				"Accept":                             "application/json",
				"X-Amzn-Sagemaker-Custom-Attributes": "trace=123",
				"X-Amzn-Sagemaker-Session-Id":        "NEW_SESSION",
				"X-Amzn-Sagemaker-Target-Variant":    "blue",
			},
			wantContentType: "application/json",
			wantVariant:     "blue",
			wantSession:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, "/endpoints/"+tt.endpointName+"/invocations", tt.body, tt.headers,
			)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, tt.wantContentType, rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
			assert.Equal(t, "mock response from Gopherstack", rec.Body.String())
			assert.Equal(t, tt.headers["X-Amzn-Sagemaker-Custom-Attributes"],
				rec.Header().Get("X-Amzn-Sagemaker-Custom-Attributes"))
			assert.Equal(t, tt.wantSession, rec.Header().Get("X-Amzn-Sagemaker-New-Session-Id") != "")
			assert.Len(t, h.Backend.ListSessions(), boolToInt(tt.wantSession))

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeEndpoint", invocations[0].Operation)
			assert.Equal(t, tt.endpointName, invocations[0].EndpointName)
		})
	}
}

func boolToInt(value bool) int {
	if value {
		return 1
	}

	return 0
}

// --- InvokeEndpointAsync tests ---

func TestHandler_InvokeEndpointAsync(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantInferenceID string
	}{
		{name: "generated_inference_id"},
		{
			name:            "client_inference_id_is_preserved",
			headers:         map[string]string{"X-Amzn-Sagemaker-Inference-Id": "request-42"},
			wantInferenceID: "request-42",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(t, h, http.MethodPost, "/endpoints/my-endpoint/async-invocations",
				map[string]any{"data": "async input"}, tt.headers)

			assert.Equal(t, http.StatusAccepted, rec.Code)

			var out map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.NotEmpty(t, out["InferenceId"])
			assert.NotContains(t, out, "OutputLocation")
			if tt.wantInferenceID != "" {
				assert.Equal(t, tt.wantInferenceID, out["InferenceId"])
			}
			assert.Contains(t, rec.Header().Get("X-Amzn-Sagemaker-Outputlocation"), out["InferenceId"])

			async := h.Backend.ListAsyncInvocations()
			require.Len(t, async, 1)
			assert.Equal(t, out["InferenceId"], async[0].InferenceID)
			assert.Equal(t, rec.Header().Get("X-Amzn-Sagemaker-Outputlocation"), async[0].OutputLocation)
		})
	}
}

// --- InvokeEndpointWithResponseStream tests ---

func TestHandler_InvokeEndpointWithResponseStream(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		headers         map[string]string
		wantContentType string
		wantVariant     string
	}{
		{name: "defaults", wantContentType: "application/octet-stream", wantVariant: "AllTraffic"},
		{
			name: "sdk_bound_response_headers",
			headers: map[string]string{
				"X-Amzn-Sagemaker-Accept":            "application/json",
				"X-Amzn-Sagemaker-Custom-Attributes": "trace=stream",
				"X-Amzn-Sagemaker-Target-Variant":    "green",
			},
			wantContentType: "application/json",
			wantVariant:     "green",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(t, h, http.MethodPost,
				"/endpoints/my-endpoint/invocations-response-stream",
				map[string]any{"data": "stream input"}, tt.headers)

			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
			assert.Equal(t, tt.wantContentType, rec.Header().Get("X-Amzn-Sagemaker-Content-Type"))
			assert.Equal(t, tt.wantVariant, rec.Header().Get("X-Amzn-Invoked-Production-Variant"))
			assert.Equal(t, tt.headers["X-Amzn-Sagemaker-Custom-Attributes"],
				rec.Header().Get("X-Amzn-Sagemaker-Custom-Attributes"))
			assert.Greater(t, len(rec.Body.Bytes()), 12, "response should contain event stream prelude")

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeEndpointWithResponseStream", invocations[0].Operation)
		})
	}
}

func TestSDKResponseBindings(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestSDKClient(t, h)

	syncOut, err := client.InvokeEndpoint(t.Context(), &sagemakerruntimesdk.InvokeEndpointInput{
		EndpointName:     aws.String("sdk-sync"),
		Body:             []byte("input"),
		Accept:           aws.String("text/plain"),
		CustomAttributes: aws.String("trace=sdk"),
		SessionId:        aws.String("NEW_SESSION"),
		TargetVariant:    aws.String("blue"),
	})
	require.NoError(t, err)
	assert.Equal(t, []byte("mock response from Gopherstack"), syncOut.Body)
	assert.Equal(t, "text/plain", aws.ToString(syncOut.ContentType))
	assert.Equal(t, "trace=sdk", aws.ToString(syncOut.CustomAttributes))
	assert.Equal(t, "blue", aws.ToString(syncOut.InvokedProductionVariant))
	assert.NotEmpty(t, aws.ToString(syncOut.NewSessionId))

	asyncOut, err := client.InvokeEndpointAsync(t.Context(), &sagemakerruntimesdk.InvokeEndpointAsyncInput{
		EndpointName:  aws.String("sdk-async"),
		InputLocation: aws.String("s3://input/request"),
		InferenceId:   aws.String("sdk-inference"),
	})
	require.NoError(t, err)
	assert.Equal(t, "sdk-inference", aws.ToString(asyncOut.InferenceId))
	assert.Contains(t, aws.ToString(asyncOut.OutputLocation), "sdk-inference")

	streamOut, err := client.InvokeEndpointWithResponseStream(
		t.Context(),
		&sagemakerruntimesdk.InvokeEndpointWithResponseStreamInput{
			EndpointName:     aws.String("sdk-stream"),
			Body:             []byte("input"),
			Accept:           aws.String("application/json"),
			CustomAttributes: aws.String("trace=stream"),
			TargetVariant:    aws.String("green"),
		},
	)
	require.NoError(t, err)
	assert.Equal(t, "application/json", aws.ToString(streamOut.ContentType))
	assert.Equal(t, "trace=stream", aws.ToString(streamOut.CustomAttributes))
	assert.Equal(t, "green", aws.ToString(streamOut.InvokedProductionVariant))
	require.NoError(t, streamOut.GetStream().Close())
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

	b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
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

	p := &sagemakerruntime.Provider{}
	assert.Equal(t, "SageMakerRuntime", p.Name())

	backend := sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1")
	h := sagemakerruntime.NewHandler(backend)

	assert.NotNil(t, h)
	assert.Equal(t, "SageMakerRuntime", h.Name())
	assert.Equal(t, "us-east-1", backend.Region())
}

func TestProvider_InitFull(t *testing.T) {
	t.Parallel()

	ctx := &service.AppContext{}
	p := &sagemakerruntime.Provider{}
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

func TestBackend_InvocationHistoryCap(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		recordCount int
		wantLen     int
	}{
		{
			name:        "below_cap",
			recordCount: 10,
			wantLen:     10,
		},
		{
			name:        "at_cap",
			recordCount: sagemakerruntime.MaxInvocationHistory,
			wantLen:     sagemakerruntime.MaxInvocationHistory,
		},
		{
			name:        "above_cap_retains_most_recent",
			recordCount: sagemakerruntime.MaxInvocationHistory + 50,
			wantLen:     sagemakerruntime.MaxInvocationHistory,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")

			for i := range tt.recordCount {
				b.RecordInvocation("InvokeEndpoint", "ep", fmt.Sprintf(`{"seq":%d}`, i), `{}`)
			}

			invocations := b.ListInvocations()
			assert.Len(t, invocations, tt.wantLen)

			if tt.recordCount > sagemakerruntime.MaxInvocationHistory {
				last := invocations[len(invocations)-1]
				assert.Contains(t, last.Input, fmt.Sprintf(`"seq":%d`, tt.recordCount-1))
			}
		})
	}
}

func TestBackend_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		setupInvCount int
	}{
		{
			name:          "empty_backend",
			setupInvCount: 0,
		},
		{
			name:          "with_invocations",
			setupInvCount: 3,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")

			for i := range tt.setupInvCount {
				b.RecordInvocation("InvokeEndpoint", "ep", fmt.Sprintf(`{"seq":%d}`, i), `{}`)
			}
			if tt.setupInvCount > 0 {
				b.StartSession("ep")
				b.RecordAsyncInvocation("ep", "persisted-id", "input")
			}

			snap := b.Snapshot()
			require.NotNil(t, snap)

			// Restore into a fresh backend.
			b2 := sagemakerruntime.NewInMemoryBackend("123456789012", "us-east-1")
			require.NoError(t, b2.Restore(snap))

			restored := b2.ListInvocations()
			assert.Len(t, restored, tt.setupInvCount)
			assert.Len(t, b2.ListSessions(), boolToInt(tt.setupInvCount > 0))
			assert.Len(t, b2.ListAsyncInvocations(), boolToInt(tt.setupInvCount > 0))

			// Snapshot isolation: adding more invocations to b2 should not affect snap.
			b2.RecordInvocation("InvokeEndpoint", "ep", `{"seq":99}`, `{}`)
			snap2 := b2.Snapshot()
			require.NotNil(t, snap2)
			assert.NotEqual(t, snap, snap2)
		})
	}
}
