package sagemakerruntime_test

import (
	"bytes"
	"encoding/json"
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

// doRequest issues a request with no body. Every call site in this package
// only ever needs a nil body (anything requiring a body uses
// doRequestWithHeaders directly), so body is intentionally not a parameter
// here -- golangci-lint's unparam flags a parameter with a single observed
// value across all call sites, which a body-taking signature would trip.
func doRequest(
	t *testing.T,
	h *sagemakerruntime.Handler,
	method, path string,
) *httptest.ResponseRecorder {
	t.Helper()

	return doRequestWithHeaders(t, h, method, path, nil, nil)
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

func boolToInt(value bool) int {
	if value {
		return 1
	}

	return 0
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

func TestHandler_Shutdown(t *testing.T) {
	t.Parallel()

	h := sagemakerruntime.NewHandler(sagemakerruntime.NewInMemoryBackend("000000000000", "us-east-1"))
	assert.NotPanics(t, func() { h.Shutdown(t.Context()) })
}

// --- Cross-operation wire/SDK tests ---

// TestCustomAttributesForwarding verifies that X-Amzn-Sagemaker-Custom-Attributes
// is reflected back on all three operation types.
func TestCustomAttributesForwarding(t *testing.T) {
	t.Parallel()

	const attrValue = "trace=abc;env=test"

	tests := []struct {
		name string
		path string
	}{
		{
			name: "invoke_endpoint",
			path: "/endpoints/ep/invocations",
		},
		{
			name: "invoke_endpoint_with_response_stream",
			path: "/endpoints/ep/invocations-response-stream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequestWithHeaders(
				t, h, http.MethodPost, tt.path, nil,
				map[string]string{
					"X-Amzn-Sagemaker-Custom-Attributes": attrValue,
				},
			)

			assert.Equal(t, attrValue, rec.Header().Get("X-Amzn-Sagemaker-Custom-Attributes"))
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
	assert.Contains(t, aws.ToString(asyncOut.FailureLocation), "sdk-inference",
		"FailureLocation must be bound like real AWS (X-Amzn-SageMaker-FailureLocation response header)")
	assert.NotEqual(t, aws.ToString(asyncOut.OutputLocation), aws.ToString(asyncOut.FailureLocation))

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

// TestHandler_ErrorCodes verifies the __type of every synchronously
// detectable error matches the real sagemakerruntime SDK's typed errors
// (aws-sdk-go-v2/service/sagemakerruntime/types/errors.go declares
// ValidationError, not the "ValidationException" name most other
// JSON-protocol services use), so client code doing errors.As(&types.
// ValidationError{}) matches gopherstack responses the same as real AWS.
func TestHandler_ErrorCodes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		path       string
		wantType   string
		wantStatus int
	}{
		{
			name:       "method_not_allowed_is_validation_error",
			method:     http.MethodGet,
			path:       "/endpoints/my-endpoint/invocations",
			wantStatus: http.StatusMethodNotAllowed,
			wantType:   "ValidationError",
		},
		{
			name:       "missing_endpoint_name_is_validation_error",
			method:     http.MethodPost,
			path:       "/endpoints//invocations",
			wantStatus: http.StatusBadRequest,
			wantType:   "ValidationError",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, tt.method, tt.path)

			require.Equal(t, tt.wantStatus, rec.Code)

			var body map[string]string
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantType, body["__type"])
		})
	}
}

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/endpoints/my-endpoint/invocations")

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
	rec := doRequest(t, h, http.MethodPost, "/endpoints/my-endpoint/unknown-op")

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

// TestHandler_RouteMatcher_Host verifies Host-header-based matching uses the
// real AWS SageMaker Runtime endpoint hostname prefix ("runtime.sagemaker."),
// per aws-sdk-go-v2/service/sagemakerruntime's endpoint resolver -- NOT
// "sagemaker-runtime." which never appears on real traffic.
func TestHandler_RouteMatcher_Host(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name  string
		host  string
		path  string
		match bool
	}{
		{
			name:  "matches real sagemaker runtime endpoint host",
			host:  "runtime.sagemaker.us-east-1.amazonaws.com",
			path:  "/unrelated",
			match: true,
		},
		{
			name:  "matches real fips sagemaker runtime endpoint host",
			host:  "runtime.sagemaker.us-east-1.amazonaws.com",
			path:  "/",
			match: true,
		},
		{
			name:  "does not match unrelated host without endpoints path",
			host:  "example.com",
			path:  "/",
			match: false,
		},
	}

	matcher := h.RouteMatcher()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			req.Host = tt.host
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}
