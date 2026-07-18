package bedrockruntime

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"
)

// charsPerToken is an approximation used for CountTokens (BPE models: ~4 chars/token).
const charsPerToken = 4

// charsPerTokenTitan is the approximation for Titan models (~6 chars/token).
const charsPerTokenTitan = 6

// resolveResponseContentType returns the Content-Type to use for model responses.
// It checks the Accept header; if absent or wildcard, uses application/json.
func resolveResponseContentType(r *http.Request) string {
	accept := r.Header.Get("Accept")
	if accept == "" || accept == "*/*" {
		return "application/json"
	}

	return accept
}

// handleInvokeModel handles POST /model/{modelId}/invoke.
// It honors the Accept header for response Content-Type negotiation.
func (h *Handler) handleInvokeModel(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockInvokeModelResponse(modelID)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(opInvokeModel, modelID, truncateString(string(body)), truncateString(string(out)))

	ct := resolveResponseContentType(c.Request())
	c.Response().Header().Set("Content-Type", ct)
	setInvokeModelTokenHeaders(c.Response(), modelID)

	return c.JSONBlob(http.StatusOK, out)
}

// setInvokeModelTokenHeaders adds X-Amzn-Bedrock-*-Token-Count headers for
// models that report token usage in their InvokeModel response headers.
func setInvokeModelTokenHeaders(w http.ResponseWriter, modelID string) {
	lower := strings.ToLower(modelID)
	// Claude, Titan, Nova, Llama, Mistral all report token counts via headers.
	if strings.Contains(lower, "claude") ||
		strings.Contains(lower, "titan") ||
		strings.Contains(lower, "nova") ||
		strings.Contains(lower, "llama") ||
		strings.Contains(lower, "mistral") ||
		strings.Contains(lower, "mixtral") ||
		strings.Contains(lower, "command") {
		w.Header().Set(hdrBedrockInputTokenCount, strconv.Itoa(mockInputTokenCount))
		w.Header().Set(hdrBedrockOutputTokenCount, strconv.Itoa(mockOutputTokenCount))
	}
}

// handleInvokeModelWithResponseStream handles POST /model/{modelId}/invoke-with-response-stream.
// It returns a well-formed AWS event stream frame containing a single chunk event.
func (h *Handler) handleInvokeModelWithResponseStream(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockInvokeModelResponse(modelID)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(
		opInvokeModelWithResponseStream,
		modelID,
		truncateString(string(body)),
		truncateString(string(out)),
	)

	frame := encodeEventStreamMsg([][2]string{
		{hdrMessageType, hdrMessageTypeEvent},
		{hdrEventType, "chunk"},
		{hdrContentType, "application/octet-stream"},
	}, out)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)
	_, _ = c.Response().Write(frame)
	flushResponse(c.Response())

	return nil
}

// handleInvokeModelWithBidirectionalStream handles POST /model/{modelId}/invoke-with-bidirectional-stream.
// It returns a well-formed AWS event stream frame containing a single chunk event.
func (h *Handler) handleInvokeModelWithBidirectionalStream(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockInvokeModelResponse(modelID)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(
		opInvokeModelWithBidiStream,
		modelID,
		truncateString(string(body)),
		truncateString(string(out)),
	)

	frame := encodeEventStreamMsg([][2]string{
		{hdrMessageType, hdrMessageTypeEvent},
		{hdrEventType, "chunk"},
		{hdrContentType, "application/octet-stream"},
	}, out)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)
	_, _ = c.Response().Write(frame)
	flushResponse(c.Response())

	return nil
}

// countTokensRequest represents the parsed CountTokens request body.
// countTokensConverse mirrors the wire-relevant fields of
// types.ConverseTokensRequest (the "converse" member of the CountTokens
// input union).
type countTokensConverse struct {
	Messages []converseMessage `json:"messages,omitempty"`
	System   []converseContent `json:"system,omitempty"`
}

// countTokensInvokeModel mirrors types.InvokeModelTokensRequest (the
// "invokeModel" member of the CountTokens input union). Body is a smithy
// blob, serialized on the wire as a base64 string; encoding/json
// transparently base64-decodes into a []byte field.
type countTokensInvokeModel struct {
	Body []byte `json:"body,omitempty"`
}

// countTokensInput mirrors the discriminated union types.CountTokensInput:
// exactly one of InvokeModel or Converse is set.
type countTokensInput struct {
	InvokeModel *countTokensInvokeModel `json:"invokeModel,omitempty"`
	Converse    *countTokensConverse    `json:"converse,omitempty"`
}

// countTokensRequest is the real CountTokens request wire shape:
//
//	{"input": {"invokeModel": {"body": "<base64>"}}}
//	{"input": {"converse": {"messages": [...], "system": [...]}}}
//
// (see aws-sdk-go-v2/service/bedrockruntime serializers.go
// awsRestjson1_serializeDocumentCountTokensInput). There is no top-level
// "prompt"/"messages"/"system" field on this operation's request body.
type countTokensRequest struct {
	Input countTokensInput `json:"input"`
}

// estimateTokenCount returns an approximate token count for the CountTokens request body.
// The approximation is ~4 chars/token (works reasonably for English BPE models).
func estimateTokenCount(body []byte, modelID string) int {
	if len(body) == 0 {
		return mockInputTokenCount
	}

	var req countTokensRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return max(1, len(body)/charsPerToken)
	}

	chars := 0

	switch {
	case req.Input.InvokeModel != nil:
		// The invokeModel body is itself a model-specific request payload
		// (Claude "prompt"/"messages", Titan "inputText", etc.); its decoded
		// byte length is used as the char proxy since gopherstack cannot
		// know the tokenizer for arbitrary model families.
		chars = len(req.Input.InvokeModel.Body)
	case req.Input.Converse != nil:
		for _, m := range req.Input.Converse.Messages {
			for _, c := range m.Content {
				chars += len(c.Text)
			}
		}

		for _, s := range req.Input.Converse.System {
			chars += len(s.Text)
		}
	}

	if chars == 0 {
		// Fall back to raw body length as proxy for input size.
		chars = len(body)
	}

	// Titan family uses slightly larger token units (~6 chars/token).
	divisor := charsPerToken
	if strings.Contains(strings.ToLower(modelID), "titan") {
		divisor = charsPerTokenTitan
	}

	return max(1, chars/divisor)
}

// handleCountTokens handles POST /model/{modelId}/count-tokens.
func (h *Handler) handleCountTokens(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	inputTokens := estimateTokenCount(body, modelID)

	resp := map[string]any{
		keyInputTokens: inputTokens,
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalServerException", "internal server error"))
	}

	h.Backend.RecordInvocation(opCountTokens, modelID, truncateString(string(body)), truncateString(string(out)))

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// isClaudeV3Plus returns true for Claude 3 and later models (Messages API format).
// Claude 2 and Instant use the legacy completion format.
func isClaudeV3Plus(modelIDLower string) bool {
	return strings.Contains(modelIDLower, "claude-3") ||
		strings.Contains(modelIDLower, "claude-4") ||
		strings.Contains(modelIDLower, "claude-sonnet") ||
		strings.Contains(modelIDLower, "claude-haiku") ||
		strings.Contains(modelIDLower, "claude-opus")
}

func mockInvokeModelResponse(modelID string) map[string]any {
	modelIDLower := strings.ToLower(modelID)

	switch {
	case strings.Contains(modelIDLower, "claude") && isClaudeV3Plus(modelIDLower):
		// Claude 3+ uses the Messages API response format.
		return map[string]any{
			"id":            "msg_mock0000000000000001",
			"type":          "message",
			keyRole:         roleAssistant,
			"content":       []map[string]any{{"type": keyText, keyText: mockResponseText}},
			keyModel:        modelID,
			keyStopReason:   stopReasonEndTurn,
			"stop_sequence": nil,
			keyUsage: map[string]any{
				"input_tokens":  mockInputTokenCount,
				"output_tokens": mockOutputTokenCount,
			},
		}
	case strings.Contains(modelIDLower, "claude"):
		// Claude 2 / Instant — legacy text completion format.
		return map[string]any{
			"completion":  mockResponseText,
			keyStopReason: stopReasonEndTurn,
			keyModel:      modelID,
		}
	case strings.Contains(modelIDLower, "titan"):
		return map[string]any{
			"results": []map[string]any{
				{"outputText": mockResponseText, "completionReason": "FINISH"},
			},
			"inputTextTokenCount": mockInputTokenCount,
		}
	case strings.Contains(modelIDLower, "llama"):
		return map[string]any{
			"generation":             mockResponseText,
			"prompt_token_count":     mockInputTokenCount,
			"generation_token_count": mockOutputTokenCount,
			keyStopReason:            "stop",
		}
	case strings.Contains(modelIDLower, "mistral") || strings.Contains(modelIDLower, "mixtral"):
		return map[string]any{
			"outputs": []map[string]any{
				{keyText: mockResponseText, keyStopReason: "stop"},
			},
		}
	case strings.Contains(modelIDLower, "command"):
		return map[string]any{
			keyText:         mockResponseText,
			keyStopReason:   "COMPLETE",
			"finish_reason": "COMPLETE",
		}
	case strings.Contains(modelIDLower, "j2") || strings.Contains(modelIDLower, "jurassic"):
		return map[string]any{
			"completions": []map[string]any{
				{
					"data":         map[string]any{keyText: mockResponseText},
					"finishReason": map[string]any{"reason": "endoftext"},
				},
			},
		}
	case strings.Contains(modelIDLower, "nova"):
		return map[string]any{
			"output": map[string]any{
				keyMessage: map[string]any{
					keyRole:    roleAssistant,
					keyContent: []map[string]any{{keyText: mockResponseText}},
				},
			},
			convStopReasonKey: stopReasonEndTurn,
			keyUsage: map[string]any{
				keyInputTokens:   mockInputTokenCount,
				convOutputTokens: mockOutputTokenCount,
				convTotalTokens:  mockTotalTokenCount,
			},
		}
	default:
		return map[string]any{
			"completion":  mockResponseText,
			keyStopReason: stopReasonEndTurn,
			keyModel:      modelID,
		}
	}
}
