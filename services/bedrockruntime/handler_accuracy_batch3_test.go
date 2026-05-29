package bedrockruntime_test

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ─────────────────────────────────────────────────────────────
// ConverseStream — intermediate event sequence accuracy
// ─────────────────────────────────────────────────────────────

// parseEventStreamFrames splits a raw binary event stream body into decoded
// JSON payloads by reading the AWS event-stream framing format:
// totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
func parseEventStreamFrames(data []byte) []map[string]any {
	var frames []map[string]any

	for len(data) >= 12 {
		if len(data) < 4 {
			break
		}

		totalLen := binary.BigEndian.Uint32(data[0:4])
		headerLen := binary.BigEndian.Uint32(data[4:8])

		if totalLen < 12 || uint32(len(data)) < totalLen {
			break
		}

		payloadStart := uint32(12) + headerLen
		payloadEnd := totalLen - 4

		if payloadStart > payloadEnd || payloadEnd > totalLen {
			data = data[totalLen:]
			continue
		}

		payload := data[payloadStart:payloadEnd]

		if len(payload) > 0 {
			var m map[string]any
			if err := json.Unmarshal(payload, &m); err == nil {
				frames = append(frames, m)
			}
		}

		data = data[totalLen:]
	}

	return frames
}

// eventTypesFrom extracts header ":event-type" values from raw binary frames.
// This is a simplified parser that looks for the 12-byte "event-type" header
// name using the AWS event stream header encoding.
func eventTypesFromRaw(data []byte) []string {
	var types []string
	const headerName = ":event-type"

	// Walk frames: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4)
	for len(data) >= 12 {
		totalLen := int(binary.BigEndian.Uint32(data[0:4]))
		headerLen := int(binary.BigEndian.Uint32(data[4:8]))

		if totalLen < 12 || len(data) < totalLen {
			break
		}

		hdrData := data[12 : 12+headerLen]

		for len(hdrData) > 0 {
			if len(hdrData) < 1 {
				break
			}

			nameLen := int(hdrData[0])
			hdrData = hdrData[1:]

			if len(hdrData) < nameLen {
				break
			}

			name := string(hdrData[:nameLen])
			hdrData = hdrData[nameLen:]

			if len(hdrData) < 1 {
				break
			}

			// type byte (7 = string)
			hdrData = hdrData[1:]

			if len(hdrData) < 2 {
				break
			}

			valLen := int(binary.BigEndian.Uint16(hdrData[:2]))
			hdrData = hdrData[2:]

			if len(hdrData) < valLen {
				break
			}

			value := string(hdrData[:valLen])
			hdrData = hdrData[valLen:]

			if name == headerName {
				types = append(types, value)
			}
		}

		data = data[totalLen:]
	}

	return types
}

func TestAccuracy_ConverseStream_EmitsMultipleEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
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

func TestAccuracy_ConverseStream_EventOrderIsCorrect(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-3-sonnet-20240229-v1:0/converse-stream",
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

func TestAccuracy_ConverseStream_ContentBlockDeltaHasText(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
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
			dm, ok := delta.(map[string]any)
			require.True(t, ok, "delta should be an object")
			txt, ok := dm["text"].(string)
			assert.True(t, ok, "delta.text should be a string")
			assert.NotEmpty(t, txt, "delta.text should not be empty")
			found = true

			break
		}
	}

	assert.True(t, found, "at least one contentBlockDelta with text expected")
}

func TestAccuracy_ConverseStream_MetadataHasUsage(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/amazon.titan-text-express-v1/converse-stream",
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
			um, ok := usage.(map[string]any)
			require.True(t, ok, "usage should be an object")
			assert.Contains(t, um, "inputTokens")
			assert.Contains(t, um, "outputTokens")
			found = true

			break
		}
	}

	assert.True(t, found, "metadata event with usage expected in stream")
}

func TestAccuracy_ConverseStream_MessageStopHasStopReason(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream",
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

func TestAccuracy_ConverseStream_EmptyBodyStillStreams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse-stream", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	types := eventTypesFromRaw(rec.Body.Bytes())
	assert.Contains(t, types, "messageStop")
}

func TestAccuracy_ConverseStream_TokensReflectInputLength(t *testing.T) {
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
				if v, ok := usage["inputTokens"].(float64); ok {
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

// ─────────────────────────────────────────────────────────────
// InvokeModelWithResponseStream — event sequence
// ─────────────────────────────────────────────────────────────

func TestAccuracy_InvokeModelWithResponseStream_EmitsChunkEvent(t *testing.T) {
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
			rec := doRequest(t, h, http.MethodPost,
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

// ─────────────────────────────────────────────────────────────
// InvokeModelWithBidirectionalStream — event framing
// ─────────────────────────────────────────────────────────────

func TestAccuracy_BidirectionalStream_EmitsChunkEvent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost,
		"/model/anthropic.claude-v2/invoke-with-bidirectional-stream",
		map[string]any{"prompt": "hello"},
	)
	require.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))

	types := eventTypesFromRaw(rec.Body.Bytes())
	assert.Contains(t, types, "chunk")
}

func TestAccuracy_BidirectionalStream_ContentTypeIsEventStream(t *testing.T) {
	t.Parallel()

	tests := []string{
		"anthropic.claude-v2",
		"amazon.titan-text-express-v1",
	}

	for _, modelID := range tests {
		t.Run(modelID, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doRequest(t, h, http.MethodPost,
				"/model/"+modelID+"/invoke-with-bidirectional-stream",
				nil,
			)
			assert.Equal(t, http.StatusOK, rec.Code)
			assert.Equal(t, "application/vnd.amazon.eventstream", rec.Header().Get("Content-Type"))
		})
	}
}

// ─────────────────────────────────────────────────────────────
// Converse — tool config round-trip accuracy
// ─────────────────────────────────────────────────────────────

func TestAccuracy_Converse_WithToolConfig_ReturnsEndTurn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/model/anthropic.claude-v2/converse",
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

func TestAccuracy_Converse_MultiTurnMessages_AccumulatesTokens(t *testing.T) {
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
