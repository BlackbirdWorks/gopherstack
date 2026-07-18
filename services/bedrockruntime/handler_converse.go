package bedrockruntime

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// converseMessage represents a single message in a Converse request.
type converseMessage struct {
	Role    string            `json:"role"`
	Content []converseContent `json:"content"`
}

// converseContent represents content in a Converse message.
type converseContent struct {
	Text string `json:"text,omitempty"`
}

// converseRequest represents the parsed Converse request body.
type converseRequest struct {
	Messages        []converseMessage `json:"messages"`
	System          []converseContent `json:"system,omitempty"`
	ToolConfig      json.RawMessage   `json:"toolConfig,omitempty"`
	GuardrailConfig json.RawMessage   `json:"guardrailConfig,omitempty"`
	InferenceConfig json.RawMessage   `json:"inferenceConfig,omitempty"`
}

// buildConverseResponse constructs a Converse response that reflects the user's last message.
func buildConverseResponse(req *converseRequest) map[string]any {
	inputTokens := estimateTokensFromMessages(req.Messages, req.System)

	return map[string]any{
		"output": map[string]any{
			keyMessage: map[string]any{
				keyRole:    roleAssistant,
				keyContent: []map[string]any{{keyText: mockResponseText}},
			},
		},
		convStopReasonKey: stopReasonEndTurn,
		keyUsage: map[string]any{
			keyInputTokens:   inputTokens,
			convOutputTokens: mockOutputTokenCount,
			convTotalTokens:  inputTokens + mockOutputTokenCount,
		},
		"metrics": map[string]any{
			"latencyMs": mockLatencyMS,
		},
	}
}

// estimateTokensFromMessages returns an approximate token count for Converse input.
func estimateTokensFromMessages(messages []converseMessage, system []converseContent) int {
	chars := 0

	for _, m := range messages {
		for _, c := range m.Content {
			chars += len(c.Text)
		}
	}

	for _, s := range system {
		chars += len(s.Text)
	}

	if chars == 0 {
		return mockInputTokenCount
	}

	return max(1, chars/charsPerToken)
}

// handleConverse handles POST /model/{modelId}/converse.
func (h *Handler) handleConverse(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	var req converseRequest
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	resp := buildConverseResponse(&req)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(opConverse, modelID, truncateString(string(body)), truncateString(string(out)))

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// handleConverseStream handles POST /model/{modelId}/converse-stream.
// Emits the full AWS event sequence: messageStart → contentBlockStart →
// contentBlockDelta → contentBlockStop → messageStop → metadata.
func (h *Handler) handleConverseStream(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	var req converseRequest
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	inputTokens := estimateTokensFromMessages(req.Messages, req.System)

	respSummary := buildConverseResponse(&req)
	out, err := json.Marshal(respSummary)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(opConverseStream, modelID, truncateString(string(body)), truncateString(string(out)))

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)

	writeStreamEvent := func(eventType string, payload any) {
		data, merr := json.Marshal(payload)
		if merr != nil {
			return
		}

		frame := encodeEventStreamMsg([][2]string{
			{hdrMessageType, hdrMessageTypeEvent},
			{hdrEventType, eventType},
			{hdrContentType, "application/json"},
		}, data)
		_, _ = c.Response().Write(frame)
	}

	writeStreamEvent("messageStart", map[string]any{
		keyRole: roleAssistant,
	})

	writeStreamEvent("contentBlockStart", map[string]any{
		convContentIdx: 0,
		"start":        map[string]any{keyText: ""},
	})

	writeStreamEvent("contentBlockDelta", map[string]any{
		convContentIdx: 0,
		"delta":        map[string]any{keyText: mockResponseText},
	})

	writeStreamEvent("contentBlockStop", map[string]any{
		convContentIdx: 0,
	})

	writeStreamEvent("messageStop", map[string]any{
		convStopReasonKey: stopReasonEndTurn,
	})

	writeStreamEvent("metadata", map[string]any{
		keyUsage: map[string]any{
			keyInputTokens:   inputTokens,
			convOutputTokens: mockOutputTokenCount,
			convTotalTokens:  inputTokens + mockOutputTokenCount,
		},
		"metrics": map[string]any{
			"latencyMs": mockLatencyMS,
		},
	})

	flushResponse(c.Response())

	return nil
}
