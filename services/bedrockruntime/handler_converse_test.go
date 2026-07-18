package bedrockruntime_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

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

func TestConverse_ReflectsInputTokens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Long message should produce higher token count than the fixed mock default.
	longText := "This is a much longer prompt than usual that should produce" +
		" more tokens than the default mock value of 10." +
		" Adding more words to ensure the character count is significant."
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"text": longText}}},
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

	usage, ok := out["usage"].(map[string]any)
	require.True(t, ok)

	inputTokens, ok := usage["inputTokens"].(float64)
	require.True(t, ok)
	assert.Greater(t, int(inputTokens), 10, "long input should produce more tokens than the 10-token mock default")
}

func TestConverse_WithSystem(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-3-sonnet-20240229-v1:0/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"text": "Hello"}}},
			},
			"system": []map[string]any{
				{"text": "You are a helpful assistant."},
			},
			"inferenceConfig": map[string]any{
				"maxTokens":   100,
				"temperature": 0.5,
			},
		})

	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Contains(t, out, "output")
	assert.Contains(t, out, "stopReason")
	assert.Contains(t, out, "usage")
	assert.Contains(t, out, "metrics")
}

func TestConverse_ResponseFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    map[string]any
		wantKey string
	}{
		{
			name: "with messages",
			body: map[string]any{
				"messages": []map[string]any{
					{"role": "user", "content": []map[string]any{{"type": "text", "text": "Hello"}}},
				},
			},
			wantKey: "output",
		},
		{
			name:    "empty body",
			body:    nil,
			wantKey: "output",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, tt.wantKey)
			assert.Contains(t, out, "usage")
			assert.Contains(t, out, "stopReason")

			usage := out["usage"].(map[string]any)
			assert.Contains(t, usage, "inputTokens")
			assert.Contains(t, usage, "outputTokens")
			assert.Contains(t, usage, "totalTokens")
		})
	}
}

func TestConverse_WithSystemMessages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		systemText string
	}{
		{name: "short system", systemText: "You are a helpful assistant."},
		{name: "long system", systemText: fmt.Sprintf("System prompt with %d chars.", 1000)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
				map[string]any{
					"messages": []map[string]any{
						{"role": "user", "content": []map[string]any{{"type": "text", "text": "Hi"}}},
					},
					"system": []map[string]any{
						{"text": tt.systemText},
					},
				},
			)
			require.Equal(t, http.StatusOK, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Contains(t, out, "output")
		})
	}
}

func TestConverse_StopReasonEndTurn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "Tell me a joke"}}},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "end_turn", out["stopReason"])
}

func TestConverse_WithToolConfig_ReturnsEndTurn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "use a tool"}}},
			},
			"toolConfig": map[string]any{
				"tools": []map[string]any{
					{
						"toolSpec": map[string]any{
							"name":        "get_weather",
							"description": "Gets the weather",
							"inputSchema": map[string]any{"json": map[string]any{"type": "object"}},
						},
					},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "end_turn", out["stopReason"])
	assert.Contains(t, out, "output")
	assert.Contains(t, out, "usage")
}

func TestConverse_MultiTurnMessages_AccumulatesTokens(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	singleTurn := map[string]any{
		"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "hello world"}}},
		},
	}
	multiTurn := map[string]any{
		"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "hello world"}}},
			{"role": "assistant", "content": []map[string]any{{"type": "text", "text": "hello to you too"}}},
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "what is the capital of france?"}}},
		},
	}

	recSingle := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse", singleTurn)
	recMulti := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse", multiTurn)

	require.Equal(t, http.StatusOK, recSingle.Code)
	require.Equal(t, http.StatusOK, recMulti.Code)

	extractInputTokens := func(body []byte) float64 {
		var out map[string]any
		require.NoError(t, json.Unmarshal(body, &out))
		usage := out["usage"].(map[string]any)

		return usage["inputTokens"].(float64)
	}

	singleTokens := extractInputTokens(recSingle.Body.Bytes())
	multiTokens := extractInputTokens(recMulti.Body.Bytes())

	assert.Greater(t, multiTokens, singleTokens, "multi-turn should have more input tokens")
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

func TestConverseStream_Flushable(t *testing.T) {
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
	assert.NotEmpty(t, rec.Body.Bytes())
}

func TestConverseStream_ContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		modelID string
	}{
		{name: "claude", modelID: "anthropic.claude-v2"},
		{name: "titan", modelID: "amazon.titan-text-express-v1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(
				t, h, http.MethodPost,
				"/model/"+tt.modelID+"/converse-stream",
				map[string]any{
					"messages": []map[string]any{
						{"role": "user", "content": []map[string]any{{"type": "text", "text": "hi"}}},
					},
				},
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		})
	}
}

func TestConverseStream_EmitsMultipleEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
		map[string]any{
			"messages": []map[string]any{
				{"role": "user", "content": []map[string]any{{"type": "text", "text": "hello"}}},
			},
		},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	eventTypes := eventTypesFromRaw(rec.Body.Bytes())
	require.NotEmpty(t, eventTypes, "event stream should contain at least one event")

	assert.Contains(t, eventTypes, "messageStop", "must contain messageStop")
	assert.Contains(t, eventTypes, "messageStart", "must contain messageStart")
	assert.Contains(t, eventTypes, "contentBlockDelta", "must contain contentBlockDelta")
}

func TestConverseStream_EventOrderIsCorrect(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-3-sonnet-20240229-v1:0/converse-stream",
		map[string]any{"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "hi"}}},
		}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	types := eventTypesFromRaw(rec.Body.Bytes())
	require.GreaterOrEqual(t, len(types), 4, "at least 4 events expected")

	// messageStart must precede contentBlockDelta and messageStop
	startIdx := -1
	deltaIdx := -1
	stopIdx := -1

	for i, et := range types {
		switch et {
		case "messageStart":
			startIdx = i
		case "contentBlockDelta":
			deltaIdx = i
		case "messageStop":
			stopIdx = i
		}
	}

	assert.Greater(t, deltaIdx, startIdx, "contentBlockDelta must follow messageStart")
	assert.Greater(t, stopIdx, deltaIdx, "messageStop must follow contentBlockDelta")
}

func TestConverseStream_ContentBlockDeltaHasText(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
		map[string]any{"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "ping"}}},
		}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	frames := parseEventStreamFrames(rec.Body.Bytes())
	require.NotEmpty(t, frames)

	// Find a frame with delta
	found := false

	for _, f := range frames {
		if delta, ok := f["delta"]; ok {
			dm, dmOK := delta.(map[string]any)
			require.True(t, dmOK, "delta should be an object")
			txt, txtOK := dm["text"].(string)
			assert.True(t, txtOK, "delta.text should be a string")
			assert.NotEmpty(t, txt, "delta.text should not be empty")
			found = true

			break
		}
	}

	assert.True(t, found, "at least one contentBlockDelta with text expected")
}

func TestConverseStream_MetadataHasUsage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/converse-stream",
		map[string]any{"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "hello titan"}}},
		}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	frames := parseEventStreamFrames(rec.Body.Bytes())
	require.NotEmpty(t, frames)

	// Find a frame with usage (metadata event)
	found := false

	for _, f := range frames {
		if usage, ok := f["usage"]; ok {
			um, umOK := usage.(map[string]any)
			require.True(t, umOK, "usage should be an object")
			assert.Contains(t, um, "inputTokens")
			assert.Contains(t, um, "outputTokens")
			found = true

			break
		}
	}

	assert.True(t, found, "metadata event with usage expected in stream")
}

func TestConverseStream_MessageStopHasStopReason(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(
		t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
		map[string]any{"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "bye"}}},
		}},
	)
	require.Equal(t, http.StatusOK, rec.Code)

	frames := parseEventStreamFrames(rec.Body.Bytes())
	require.NotEmpty(t, frames)

	found := false

	for _, f := range frames {
		if sr, ok := f["stopReason"].(string); ok {
			assert.Equal(t, "end_turn", sr)
			found = true

			break
		}
	}

	assert.True(t, found, "messageStop event with stopReason expected")
}

func TestConverseStream_EmptyBodyStillStreams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	types := eventTypesFromRaw(rec.Body.Bytes())
	assert.Contains(t, types, "messageStop")
}

func TestConverseStream_TokensReflectInputLength(t *testing.T) {
	t.Parallel()

	shortBody := map[string]any{
		"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{"type": "text", "text": "hi"}}},
		},
	}
	longBody := map[string]any{
		"messages": []map[string]any{
			{"role": "user", "content": []map[string]any{{
				"type": "text",
				"text": bytes.Repeat([]byte("hello world this is a long message "), 20),
			}}},
		},
	}

	h := newTestHandler(t)

	shortRec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream", shortBody)
	longRec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream", longBody)

	require.Equal(t, http.StatusOK, shortRec.Code)
	require.Equal(t, http.StatusOK, longRec.Code)

	getTokens := func(rec *httptest.ResponseRecorder) float64 {
		for _, f := range parseEventStreamFrames(rec.Body.Bytes()) {
			if usage, ok := f["usage"].(map[string]any); ok {
				if v, inOK := usage["inputTokens"].(float64); inOK {
					return v
				}
			}
		}

		return -1
	}

	shortTokens := getTokens(shortRec)
	longTokens := getTokens(longRec)

	assert.Greater(t, longTokens, shortTokens, "longer input should produce more input tokens")
}
