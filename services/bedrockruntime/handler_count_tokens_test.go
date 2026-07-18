package bedrockruntime_test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// countTokensInvokeModelBody builds a real CountTokens request body wrapping
// modelBody (a model-specific InvokeModel-style payload, e.g. `{"prompt":"hi"}`)
// as the "input.invokeModel.body" blob member. encoding/json base64-encodes
// the []byte value automatically, matching the wire shape produced by
// aws-sdk-go-v2's awsRestjson1_serializeDocumentInvokeModelTokensRequest.
func countTokensInvokeModelBody(modelBody string) map[string]any {
	return map[string]any{
		"input": map[string]any{
			"invokeModel": map[string]any{
				"body": []byte(modelBody),
			},
		},
	}
}

// countTokensConverseBody builds a real CountTokens request body wrapping
// messages as the "input.converse.messages" member, matching
// awsRestjson1_serializeDocumentConverseTokensRequest.
func countTokensConverseBody(messages []map[string]any) map[string]any {
	return map[string]any{
		"input": map[string]any{
			"converse": map[string]any{
				"messages": messages,
			},
		},
	}
}

func TestHandler_CountTokens(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body    map[string]any
		name    string
		modelID string
	}{
		{
			name:    "counts tokens for claude",
			modelID: "anthropic.claude-v2",
			body:    countTokensInvokeModelBody(`{"prompt":"Hello, how are you?"}`),
		},
		{
			name:    "counts tokens for titan",
			modelID: "amazon.titan-text-express-v1",
			body:    countTokensInvokeModelBody(`{"inputText":"Count these tokens"}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/"+tt.modelID+"/count-tokens", tt.body)

			assert.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "inputTokens")

			invocations := h.Backend.ListInvocations()
			require.Len(t, invocations, 1)
			assert.Equal(t, "CountTokens", invocations[0].Operation)
			assert.Equal(t, tt.modelID, invocations[0].ModelID)
		})
	}
}

// TestHandler_CountTokens_WireShape verifies the real CountTokens request
// wire shape is actually parsed: {"input":{"invokeModel":{"body":"<base64>"}}}
// or {"input":{"converse":{"messages":[...]}}} -- NOT a top-level
// "prompt"/"messages" field (which never existed on this operation's real
// request; see aws-sdk-go-v2's awsRestjson1_serializeDocumentCountTokensInput).
// If the handler fell back to counting the raw JSON envelope's byte length
// (the pre-fix behavior when the expected fields were never found), these
// exact expected counts would be far larger than the values asserted here.
func TestHandler_CountTokens_WireShape(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantTokens float64
	}{
		{
			// `{"prompt":"hi"}` is 15 bytes -> max(1, 15/4) = 3.
			name:       "invokeModel body is decoded and measured, not the envelope",
			body:       countTokensInvokeModelBody(`{"prompt":"hi"}`),
			wantTokens: 3,
		},
		{
			// "hello" is 5 chars -> max(1, 5/4) = 1.
			name: "converse messages text is measured, not the envelope",
			body: countTokensConverseBody([]map[string]any{
				{"role": "user", "content": []map[string]any{{"text": "hello"}}},
			}),
			wantTokens: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", tt.body)

			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.InDelta(t, tt.wantTokens, out["inputTokens"], 0)
		})
	}
}

func TestCountTokens_ReflectsInputLength(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	shortBody := countTokensInvokeModelBody(`{"prompt":"Hi"}`)
	longBody := countTokensConverseBody([]map[string]any{
		{"role": "user", "content": []map[string]any{
			{
				"text": "This is a long prompt with enough words" +
					" to produce a meaningful token count approximation.",
			},
		}},
	})

	recShort := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", shortBody)
	recLong := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", longBody)

	require.Equal(t, http.StatusOK, recShort.Code)
	require.Equal(t, http.StatusOK, recLong.Code)

	var shortOut, longOut map[string]any
	require.NoError(t, json.Unmarshal(recShort.Body.Bytes(), &shortOut))
	require.NoError(t, json.Unmarshal(recLong.Body.Bytes(), &longOut))

	shortTokens := shortOut["inputTokens"].(float64)
	longTokens := longOut["inputTokens"].(float64)

	assert.Greater(t, longTokens, shortTokens, "longer input should yield more tokens than shorter input")
}

func TestCountTokens_TitanUsesLargerDivisor(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Same body, different models — Titan uses ~6 chars/token vs ~4 for others.
	body := countTokensInvokeModelBody(
		`{"inputText":"This is a moderately long prompt that should produce different token counts across model families."}`,
	)

	recClaude := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", body)
	recTitan := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/count-tokens", body)

	require.Equal(t, http.StatusOK, recClaude.Code)
	require.Equal(t, http.StatusOK, recTitan.Code)

	var claudeOut, titanOut map[string]any
	require.NoError(t, json.Unmarshal(recClaude.Body.Bytes(), &claudeOut))
	require.NoError(t, json.Unmarshal(recTitan.Body.Bytes(), &titanOut))

	claudeTokens := claudeOut["inputTokens"].(float64)
	titanTokens := titanOut["inputTokens"].(float64)

	assert.Greater(
		t,
		claudeTokens,
		titanTokens,
		"Claude (4 chars/token) should produce more tokens than Titan (6 chars/token) for the same input",
	)
}

func TestCountTokens_MultipleModelFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		modelID  string
		text     string
		minCount int
	}{
		{name: "claude short text", modelID: "anthropic.claude-v2", text: "Hello world", minCount: 1},
		{name: "titan short text", modelID: "amazon.titan-text-express-v1", text: "Hello world", minCount: 1},
		{
			name:     "claude long text",
			modelID:  "anthropic.claude-v2",
			text:     string(make([]byte, 1000)),
			minCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/count-tokens",
				countTokensInvokeModelBody(fmt.Sprintf(`{"prompt":%q}`, tt.text)),
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			count, ok := out["inputTokens"].(float64)
			require.True(t, ok, "inputTokens should be a number")
			assert.GreaterOrEqual(t, int(count), tt.minCount)
		})
	}
}

func TestCountTokens_TitanUsesHigherDivisor(t *testing.T) {
	t.Parallel()

	text := "This is a reasonably sized piece of text for token counting purposes, with several sentences and words."

	h := newTestHandler(t)

	body := countTokensInvokeModelBody(fmt.Sprintf(`{"prompt":%q}`, text))

	recClaude := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens", body)
	require.Equal(t, http.StatusOK, recClaude.Code)

	recTitan := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/count-tokens", body)
	require.Equal(t, http.StatusOK, recTitan.Code)

	var claudeOut, titanOut map[string]any
	require.NoError(t, json.Unmarshal(recClaude.Body.Bytes(), &claudeOut))
	require.NoError(t, json.Unmarshal(recTitan.Body.Bytes(), &titanOut))

	claudeTokens := claudeOut["inputTokens"].(float64)
	titanTokens := titanOut["inputTokens"].(float64)
	// Titan uses ~6 chars/token vs ~4 for Claude → fewer tokens for same text.
	assert.GreaterOrEqual(t, claudeTokens, titanTokens, "claude tokenizer should produce more tokens than titan")
}

func TestCountTokens_MultiMessageBody(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		messages []map[string]any
		minCount int
	}{
		{
			name: "two messages",
			messages: []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "Question one"}}},
				{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "Answer one"}}},
			},
			minCount: 1,
		},
		{
			name: "single message",
			messages: []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "Simple question"}}},
			},
			minCount: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/count-tokens",
				countTokensConverseBody(tt.messages))
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			count := out["inputTokens"].(float64)
			assert.GreaterOrEqual(t, int(count), tt.minCount)
		})
	}
}
