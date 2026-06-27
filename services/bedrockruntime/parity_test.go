package bedrockruntime_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// Parity: Claude 3+ InvokeModel response format (Messages API)
// ---------------------------------------------------------------------------

func TestParity_InvokeModel_Claude3_MessagesAPIFormat(t *testing.T) {
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

func TestParity_InvokeModel_Claude2_LegacyCompletionFormat(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Parity: InvokeModel token-count response headers
// ---------------------------------------------------------------------------

func TestParity_InvokeModel_TokenCountHeaders_Claude(t *testing.T) {
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

func TestParity_InvokeModel_TokenCountHeaders_Titan(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/invoke",
		map[string]any{"inputText": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Input-Token-Count"))
	assert.NotEmpty(t, rec.Header().Get("X-Amzn-Bedrock-Output-Token-Count"))
}

func TestParity_InvokeModel_TokenCountHeaders_Claude3(t *testing.T) {
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

func TestParity_InvokeModel_NoTokenCountHeaders_Jurassic(t *testing.T) {
	t.Parallel()

	// AI21 Jurassic models do NOT return token count headers in real AWS.
	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/ai21.j2-ultra-v1/invoke",
		map[string]any{"prompt": "Hello"})

	require.Equal(t, http.StatusOK, rec.Code)
	assert.Empty(t, rec.Header().Get("X-Amzn-Bedrock-Input-Token-Count"),
		"Jurassic/AI21 models must NOT set token count headers")
}
