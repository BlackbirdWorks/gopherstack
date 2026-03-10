package bedrockruntime_test

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
		"InvokeModel",
		"InvokeModelWithResponseStream",
		"Converse",
		"ConverseStream",
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

// --- InvokeModel tests ---

func TestHandler_InvokeModel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
		body    map[string]any
		wantKey string
	}{
		{
			name:    "claude model returns completion",
			modelID: "anthropic.claude-v2",
			body:    map[string]any{"prompt": "Hello"},
			wantKey: "completion",
		},
		{
			name:    "titan model returns results",
			modelID: "amazon.titan-text-express-v1",
			body:    map[string]any{"inputText": "Hello"},
			wantKey: "results",
		},
		{
			name:    "llama model returns generation",
			modelID: "meta.llama2-13b-chat-v1",
			body:    map[string]any{"prompt": "Hello"},
			wantKey: "generation",
		},
		{
			name:    "unknown model returns completion",
			modelID: "unknown.model-v1",
			body:    map[string]any{"prompt": "Hello"},
			wantKey: "completion",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/invoke", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, tt.wantKey)

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "InvokeModel", invocations[0].Operation)
			assert.Equal(t, tt.modelID, invocations[0].ModelID)
		})
	}
}

// --- InvokeModelWithResponseStream tests ---

func TestHandler_InvokeModelWithResponseStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-response-stream",
		map[string]any{"prompt": "Hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Body.Bytes())

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModelWithResponseStream", invocations[0].Operation)
}

// --- Converse tests ---

func TestHandler_Converse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		modelID string
	}{
		{
			name:    "basic converse",
			modelID: "anthropic.claude-3-sonnet-20240229-v1:0",
			body: map[string]any{
				"messages": []map[string]any{
					{"role": "user", "content": []map[string]any{{"text": "Hello"}}},
				},
			},
		},
		{
			name:    "converse with different model",
			modelID: "amazon.titan-text-express-v1",
			body: map[string]any{
				"messages": []map[string]any{
					{"role": "user", "content": []map[string]any{{"text": "What is 1+1?"}}},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/converse", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "output")
			assert.Contains(t, out, "stopReason")

			outputVal, ok := out["output"].(map[string]any)
			require.True(t, ok)
			assert.Contains(t, outputVal, "message")

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "Converse", invocations[0].Operation)
			assert.Equal(t, tt.modelID, invocations[0].ModelID)
		})
	}
}

// --- ConverseStream tests ---

func TestHandler_ConverseStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"text": "Hello"}}},
			},
		})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "ConverseStream", invocations[0].Operation)
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

func TestHandler_UnknownOperation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/unknown-op", nil)

	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// --- Backend tests ---

func TestBackend_RecordAndList(t *testing.T) {
	t.Parallel()

	b := bedrockruntime.NewInMemoryBackend("123456789012", "us-east-1")
	assert.Equal(t, "us-east-1", b.Region())

	invocations := b.ListInvocations()
	assert.Empty(t, invocations)

	inv := b.RecordInvocation("InvokeModel", "anthropic.claude-v2", `{"prompt":"hi"}`, `{"completion":"hello"}`)
	assert.Equal(t, "InvokeModel", inv.Operation)
	assert.Equal(t, "anthropic.claude-v2", inv.ModelID)
	assert.NotZero(t, inv.CreatedAt)

	invocations = b.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModel", invocations[0].Operation)
}
