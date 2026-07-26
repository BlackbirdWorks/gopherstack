package bedrockruntime_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/bedrockruntime"
)

func newTestHandler(t *testing.T) *bedrockruntime.Handler {
	t.Helper()

	return bedrockruntime.NewHandler(bedrockruntime.NewInMemoryBackend("000000000000", "us-east-1"))
}

func doRequest(
	t *testing.T,
	h *bedrockruntime.Handler,
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
	assert.Equal(t, "BedrockRuntime", h.Name())
}

func TestHandler_ChaosServiceName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	assert.Equal(t, "bedrockruntime", h.ChaosServiceName())
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

	for _, op := range []string{
		"ApplyGuardrail",
		"Converse",
		"ConverseStream",
		"CountTokens",
		"GetAsyncInvoke",
		"InvokeGuardrailChecks",
		"InvokeModel",
		"InvokeModelWithBidirectionalStream",
		"InvokeModelWithResponseStream",
		"ListAsyncInvokes",
		"StartAsyncInvoke",
	} {
		assert.Contains(t, ops, op)
	}
}

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
			name:  "matches invoke path",
			path:  "/model/anthropic.claude-3-sonnet-20240229-v1:0/invoke",
			match: true,
		},
		{
			name:  "matches converse path",
			path:  "/model/anthropic.claude-v2/converse",
			match: true,
		},
		{
			name:  "does not match other path",
			path:  "/queues/myqueue",
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

func TestHandler_RouteMatcher_ExtendedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name  string
		path  string
		match bool
	}{
		{
			name:  "matches guardrail apply path",
			path:  "/guardrail/my-id/version/1/apply",
			match: true,
		},
		{
			name:  "matches async invoke list path",
			path:  "/async-invoke",
			match: true,
		},
		{
			name:  "matches async invoke get path",
			path:  "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			match: true,
		},
	}

	matcher := h.RouteMatcher()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodGet, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.match, matcher(c))
		})
	}
}

func TestHandler_ExtractOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		path   string
		wantOp string
	}{
		{
			name:   "InvokeModel",
			path:   "/model/anthropic.claude-v2/invoke",
			wantOp: "InvokeModel",
		},
		{
			name:   "InvokeModelWithResponseStream",
			path:   "/model/anthropic.claude-v2/invoke-with-response-stream",
			wantOp: "InvokeModelWithResponseStream",
		},
		{
			name:   "Converse",
			path:   "/model/anthropic.claude-v2/converse",
			wantOp: "Converse",
		},
		{
			name:   "ConverseStream",
			path:   "/model/anthropic.claude-v2/converse-stream",
			wantOp: "ConverseStream",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractOperation_ExtendedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name   string
		method string
		path   string
		wantOp string
	}{
		{
			name:   "CountTokens",
			method: http.MethodPost,
			path:   "/model/anthropic.claude-v2/count-tokens",
			wantOp: "CountTokens",
		},
		{
			name:   "InvokeModelWithBidirectionalStream",
			method: http.MethodPost,
			path:   "/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
			wantOp: "InvokeModelWithBidirectionalStream",
		},
		{
			name:   "ApplyGuardrail",
			method: http.MethodPost,
			path:   "/guardrail/my-id/version/1/apply",
			wantOp: "ApplyGuardrail",
		},
		{
			name:   "ListAsyncInvokes",
			method: http.MethodGet,
			path:   "/async-invoke",
			wantOp: "ListAsyncInvokes",
		},
		{
			name:   "StartAsyncInvoke",
			method: http.MethodPost,
			path:   "/async-invoke",
			wantOp: "StartAsyncInvoke",
		},
		{
			name:   "GetAsyncInvoke",
			method: http.MethodGet,
			path:   "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			wantOp: "GetAsyncInvoke",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantOp, h.ExtractOperation(c))
		})
	}
}

func TestHandler_ExtractResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name      string
		path      string
		wantModel string
	}{
		{
			name:      "claude model id",
			path:      "/model/anthropic.claude-v2/invoke",
			wantModel: "anthropic.claude-v2",
		},
		{
			name:      "titan model id",
			path:      "/model/amazon.titan-text-express-v1/converse",
			wantModel: "amazon.titan-text-express-v1",
		},
		{
			// Inference-profile / custom-model ARNs contain an embedded '/'
			// (percent-encoded as %2F on the wire, decoded back to a literal
			// '/' by net/http). Naively cutting the modelId at the first '/'
			// after "/model/" truncates it and loses the model family
			// suffix -- verify the full ARN is recovered by bounding on the
			// known "/invoke" suffix instead.
			name: "ARN model id with embedded slash on invoke",
			path: "/model/arn:aws:bedrock:us-east-1:111122223333:inference-profile/" +
				"us.anthropic.claude-3-sonnet-20240229-v1:0/invoke",
			wantModel: "arn:aws:bedrock:us-east-1:111122223333:inference-profile/" +
				"us.anthropic.claude-3-sonnet-20240229-v1:0",
		},
		{
			name: "ARN model id with embedded slash on converse",
			path: "/model/arn:aws:bedrock:us-east-1:111122223333:inference-profile/" +
				"us.anthropic.claude-3-sonnet-20240229-v1:0/converse",
			wantModel: "arn:aws:bedrock:us-east-1:111122223333:inference-profile/" +
				"us.anthropic.claude-3-sonnet-20240229-v1:0",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantModel, h.ExtractResource(c))
		})
	}
}

func TestHandler_ExtractResource_ExtendedOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	e := echo.New()

	tests := []struct {
		name         string
		method       string
		path         string
		wantResource string
	}{
		{
			name:         "guardrail resource",
			method:       http.MethodPost,
			path:         "/guardrail/my-guardrail-id/version/1/apply",
			wantResource: "my-guardrail-id",
		},
		{
			name:         "async invoke collection",
			method:       http.MethodGet,
			path:         "/async-invoke",
			wantResource: "",
		},
		{
			// Item path must return stable label "async-invoke", not the full ARN,
			// because ExtractResource is used as a low-cardinality telemetry label.
			name:         "async invoke by arn returns stable label",
			method:       http.MethodGet,
			path:         "/async-invoke/arn:aws:bedrock:us-east-1:000000000000:async-invoke/1",
			wantResource: "async-invoke",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(tt.method, tt.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.wantResource, h.ExtractResource(c))
		})
	}
}

// --- Error path tests ---

func TestHandler_MethodNotAllowed(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/model/anthropic.claude-v2/invoke", nil)

	assert.Equal(t, http.StatusMethodNotAllowed, rec.Code)
}

func TestHandler_MissingModelID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/", bytes.NewReader(nil))
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestMissingModelID_Returns400(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		path string
	}{
		{name: "invoke no model", path: "/model//invoke"},
		{name: "converse no model", path: "/model//converse"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, tt.path, map[string]any{"prompt": "test"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/unknown-op", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Handler Reset ---

func TestHandler_Reset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.RecordInvocation("InvokeModel", "model", `{}`, `{}`)
	require.Len(t, h.Backend.ListInvocations(), 1)

	h.Reset()

	assert.Empty(t, h.Backend.ListInvocations())
}

// --- Chaos tests ---

func TestHandler_ChaosOperations(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.ChaosOperations()

	require.NotEmpty(t, ops)

	for _, op := range h.GetSupportedOperations() {
		assert.Contains(t, ops, op)
	}
}

func TestHandler_ChaosRegions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	regions := h.ChaosRegions()

	require.Len(t, regions, 1)
	assert.Equal(t, "us-east-1", regions[0])
}

// --- Purge handler test ---

func TestHandler_Purge(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	h.Backend.RecordInvocation("InvokeModel", "model", `{}`, `{}`)

	require.Len(t, h.Backend.ListInvocations(), 1)

	// Purge with future cutoff removes all.
	h.Purge(t.Context(), time.Now().Add(time.Hour))

	assert.Empty(t, h.Backend.ListInvocations())
}
