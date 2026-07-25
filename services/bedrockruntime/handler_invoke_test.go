package bedrockruntime_test

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestHandler_InvokeModel_ModelFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
		wantKey string
	}{
		{
			name:    "mistral model",
			modelID: "mistral.mistral-7b-instruct-v0:2",
			wantKey: "outputs",
		},
		{
			name:    "command model",
			modelID: "cohere.command-r-v1:0",
			wantKey: "text",
		},
		{
			name:    "nova model",
			modelID: "amazon.nova-pro-v1:0",
			wantKey: "output",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/invoke",
				map[string]any{"prompt": "Hello"})

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, tt.wantKey)
		})
	}
}

func TestInvokeModel_MultipleModelFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude v2", modelID: "anthropic.claude-v2"},
		{name: "claude 3 sonnet", modelID: "anthropic.claude-3-sonnet-20240229-v1:0"},
		{name: "titan text", modelID: "amazon.titan-text-express-v1"},
		{name: "llama 3", modelID: "meta.llama3-8b-instruct-v1:0"},
		{name: "mistral", modelID: "mistral.mistral-7b-instruct-v0:2"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/invoke",
				map[string]any{"prompt": "Hello"},
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
		})
	}
}

// --- InvokeModel Content-Type / Accept negotiation ---

func TestInvokeModel_DefaultContentType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke",
		map[string]any{"prompt": "Hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestInvokeModel_AcceptHeader_JSON(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
}

func TestInvokeModel_AcceptHeader_Wildcard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "*/*")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))
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

func TestInvokeModelWithResponseStream_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude v2", modelID: "anthropic.claude-v2"},
		{name: "claude v3", modelID: "anthropic.claude-3-sonnet-20240229-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/invoke-with-response-stream",
				map[string]any{"prompt": "stream this"},
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		})
	}
}

func TestInvokeModelWithResponseStream_EmitsChunkEvent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude", modelID: "anthropic.claude-v2"},
		{name: "titan", modelID: "amazon.titan-text-express-v1"},
		{name: "llama3", modelID: "meta.llama3-8b-instruct-v1:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/invoke-with-response-stream",
				map[string]any{"prompt": "test"},
			)
			require.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

			types := eventTypesFromRaw(rec.Body.Bytes())
			assert.Contains(t, types, "chunk", "response stream must contain a chunk event")
		})
	}
}

// --- InvokeModelWithBidirectionalStream tests ---

func TestHandler_InvokeModelWithBidirectionalStream(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
		map[string]any{"prompt": "Hello"})

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Body.Bytes())

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, "InvokeModelWithBidirectionalStream", invocations[0].Operation)
}

func TestBidirectionalStream_EmitsChunkEvent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost,
		"/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
		map[string]any{"prompt": "hello"},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	types := eventTypesFromRaw(rec.Body.Bytes())
	assert.Contains(t, types, "chunk")
}

func TestBidirectionalStream_ContentTypeIsEventStream(t *testing.T) {
	t.Parallel()

	tests := []string{
		"anthropic.claude-v2",
		"amazon.titan-text-express-v1",
	}

	for _, modelID := range tests {
		t.Run(modelID, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+modelID+"/invoke-with-bidirectional-stream",
				nil,
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		})
	}
}

// TestInvocationTruncation verifies the invocationRing truncates long strings.
func TestInvocationTruncation(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Build a body larger than maxInvocationStringBytes (10,000 chars).
	largePrompt := make([]byte, 15_000)
	for i := range largePrompt {
		largePrompt[i] = 'x'
	}

	body, _ := json.Marshal(map[string]any{"prompt": string(largePrompt)})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	require.Equal(t, http.StatusOK, rec.Code)

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.LessOrEqual(t, len(invocations[0].Input), 10_000, "input should be truncated to maxInvocationStringBytes")
}

// --- Parity: Claude 3+ InvokeModel response format (Messages API) ---

func TestInvokeModel_Claude3_MessagesAPIFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{"claude-3-sonnet", "anthropic.claude-3-sonnet-20240229-v1:0"},
		{"claude-3-haiku", "anthropic.claude-3-haiku-20240307-v1:0"},
		{"claude-3-opus", "anthropic.claude-3-opus-20240229-v1:0"},
		{"claude-3-5-sonnet", "anthropic.claude-3-5-sonnet-20241022-v2:0"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			userContent := []map[string]any{{"type": "text", "text": "Hello"}}
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/invoke",
				map[string]any{
					"messages":          []map[string]any{{"role": "user", "content": userContent}},
					"max_tokens":        1024,
					"anthropic_version": "bedrock-2023-05-31",
				})

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			// Messages API format fields (not Claude 2 `completion` field)
			assert.Contains(t, out, "id", "Claude 3 response must include 'id' field")
			assert.Equal(t, "message", out["type"], "Claude 3 response type must be 'message'")
			assert.Equal(t, "assistant", out["role"], "Claude 3 response role must be 'assistant'")
			assert.Contains(t, out, "content", "Claude 3 response must include 'content' array")
			assert.NotContains(t, out, "completion", "Claude 3 must NOT use legacy 'completion' field")

			usage, ok := out["usage"].(map[string]any)
			require.True(t, ok, "Claude 3 response must include 'usage' object")
			assert.Contains(t, usage, "input_tokens")
			assert.Contains(t, usage, "output_tokens")

			content, ok := out["content"].([]any)
			require.True(t, ok, "content must be an array")
			require.NotEmpty(t, content, "content array must not be empty")

			first, ok := content[0].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "text", first["type"])
			assert.NotEmpty(t, first["text"])
		})
	}
}

func TestInvokeModel_Claude2_LegacyCompletionFormat(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{"claude-v2", "anthropic.claude-v2"},
		{"claude-instant", "anthropic.claude-instant-v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/invoke",
				map[string]any{"prompt": "\n\nHuman: Hello\n\nAssistant:"})

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			// Legacy completion format
			assert.Contains(t, out, "completion", "Claude 2 response must use legacy 'completion' field")
			assert.NotContains(t, out, "content", "Claude 2 must NOT use Messages API 'content' field")
		})
	}
}

// TestInvokeModel_ARNModelIDWithEmbeddedSlash verifies that an ARN-style
// modelId with an embedded '/' (e.g. an inference-profile ARN) still
// resolves to the correct model-family response envelope.
//
// A cross-region inference-profile ARN embeds a literal '/' between the
// resource type and the underlying model id. The AWS SDK percent-encodes
// it as %2F on the wire (modelId is a non-greedy {modelId} URI label),
// and net/http decodes it back to a literal '/' server-side -- so a
// naive "cut at first slash" extraction truncates the modelId, losing
// the "claude" family marker and silently falling back to the wrong
// (legacy/default) response envelope instead of the Claude 3 Messages
// API format.
func TestInvokeModel_ARNModelIDWithEmbeddedSlash(t *testing.T) {
	t.Parallel()

	modelID := "arn:aws:bedrock:us-east-1:111122223333:inference-profile/" +
		"us.anthropic.claude-3-sonnet-20240229-v1:0"

	h := newTestHandler(t)
	userContent := []map[string]any{{"type": "text", "text": "Hello"}}
	rec := doRequest(t, h, http.MethodPost, "/model/"+modelID+"/invoke",
		map[string]any{
			"messages":          []map[string]any{{"role": "user", "content": userContent}},
			"max_tokens":        1024,
			"anthropic_version": "bedrock-2023-05-31",
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	// Claude 3 Messages API format fields (not the legacy `completion` field
	// that the truncated-ARN bug would have produced).
	assert.Equal(t, "message", out["type"], "Claude 3 response type must be 'message'")
	assert.Contains(t, out, "content", "Claude 3 response must include 'content' array")
	assert.NotContains(t, out, "completion", "must NOT fall back to legacy 'completion' format")
	assert.Equal(t, modelID, out["model"], "response 'model' field must echo the full, untruncated ARN")

	invocations := h.Backend.ListInvocations()
	require.Len(t, invocations, 1)
	assert.Equal(t, modelID, invocations[0].ModelID, "recorded invocation must use the full, untruncated ARN")
}

// --- Parity: InvokeModel token-count response headers ---

func TestInvokeModel_TokenCountHeaders_Claude(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Input-Token-Count"),
		"InvokeModel must set X-Amzn-Bedrock-Input-Token-Count header")
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Output-Token-Count"),
		"InvokeModel must set X-Amzn-Bedrock-Output-Token-Count header")
}

func TestInvokeModel_TokenCountHeaders_Titan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/invoke",
		map[string]any{"inputText": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Input-Token-Count"))
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Output-Token-Count"))
}

func TestInvokeModel_TokenCountHeaders_Claude3(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	msgContent := []map[string]any{{"type": "text", "text": "hi"}}
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-3-sonnet-20240229-v1:0/invoke",
		map[string]any{
			"messages":          []map[string]any{{"role": "user", "content": msgContent}},
			"max_tokens":        100,
			"anthropic_version": "bedrock-2023-05-31",
		})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Input-Token-Count"))
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Output-Token-Count"))
}

func TestInvokeModel_NoTokenCountHeaders_Jurassic(t *testing.T) {
	t.Parallel()

	// AI21 Jurassic models do NOT return token count headers in real AWS.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/ai21.j2-ultra-v1/invoke",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("X-Amzn-Bedrock-Input-Token-Count"),
		"Jurassic/AI21 models must NOT set token count headers")
}

// --- Event stream chunk payload wire shape ---
//
// The real aws-sdk-go-v2 client deserializes a "chunk" event's payload as
// types.PayloadPart / types.BidirectionalOutputPayloadPart: a JSON document
// with a single "bytes" member holding base64-encoded bytes (see
// awsRestjson1_deserializeDocumentPayloadPart in the real SDK's
// deserializers.go). Sending the raw model-response JSON as the payload
// itself -- instead of wrapping it in {"bytes": "<base64>"}  -- silently
// leaves the client's PayloadPart.Bytes field empty.

func TestInvokeModelWithResponseStream_ChunkPayloadIsBase64WrappedBytes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-response-stream",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)

	frames := parseEventStreamFrames(rec.Body.Bytes())
	require.Len(t, frames, 1)

	rawBytes, ok := frames[0]["bytes"].(string)
	require.True(t, ok, "chunk event payload must be a JSON document with a top-level 'bytes' member")

	decoded, err := base64.StdEncoding.DecodeString(rawBytes)
	require.NoError(t, err)

	var modelResp map[string]any
	require.NoError(t, json.Unmarshal(decoded, &modelResp))
	assert.Equal(t, "end_turn", modelResp["stop_reason"])
	assert.Equal(t, "This is a mock response from Gopherstack.", modelResp["completion"])
}

func TestBidirectionalStream_ChunkPayloadIsBase64WrappedBytes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)

	frames := parseEventStreamFrames(rec.Body.Bytes())
	require.Len(t, frames, 1)

	rawBytes, ok := frames[0]["bytes"].(string)
	require.True(t, ok, "chunk event payload must be a JSON document with a top-level 'bytes' member")

	_, err := base64.StdEncoding.DecodeString(rawBytes)
	require.NoError(t, err)
}

// --- InvokeModel guardrail header validation ---
//
// Real InvokeModel/InvokeModelWithResponseStream throw a ValidationException
// when guardrailIdentifier is supplied without guardrailVersion (see
// InvokeModelInput.GuardrailIdentifier's doc comment in the real SDK's
// api_op_InvokeModel.go).

func TestInvokeModel_GuardrailIdentifierWithoutVersion_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amzn-Bedrock-Guardrailidentifier", "my-guardrail")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "ValidationException", out["__type"])
}

func TestInvokeModel_GuardrailIdentifierWithVersion_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amzn-Bedrock-Guardrailidentifier", "my-guardrail")
	req.Header.Set("X-Amzn-Bedrock-Guardrailversion", "DRAFT")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestInvokeModelWithResponseStream_GuardrailIdentifierWithoutVersion_Returns400(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost, "/model/anthropic.claude-v2/invoke-with-response-stream", bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amzn-Bedrock-Guardrailidentifier", "my-guardrail")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// --- PerformanceConfigLatency echo ---

func TestInvokeModel_PerformanceConfigLatency_EchoedInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/model/anthropic.claude-v2/invoke", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amzn-Bedrock-Performanceconfig-Latency", "optimized")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "optimized", rec.Header().Get("X-Amzn-Bedrock-Performanceconfig-Latency"))
}

func TestInvokeModel_PerformanceConfigLatency_OmittedWhenNotRequested(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("X-Amzn-Bedrock-Performanceconfig-Latency"))
}

func TestInvokeModelWithResponseStream_PerformanceConfigLatency_EchoedInResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	body, _ := json.Marshal(map[string]any{"prompt": "Hello"})

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost, "/model/anthropic.claude-v2/invoke-with-response-stream", bytes.NewReader(body),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Amzn-Bedrock-Performanceconfig-Latency", "optimized")

	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "optimized", rec.Header().Get("X-Amzn-Bedrock-Performanceconfig-Latency"))
}

func TestInvokeModelWithResponseStream_BedrockContentTypeHeader(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/invoke-with-response-stream",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("X-Amzn-Bedrock-Content-Type"))
}
