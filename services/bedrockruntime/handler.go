package bedrockruntime

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const modelPathPrefix = "/model/"

// Handler is the Echo HTTP handler for AWS Bedrock Runtime operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new Bedrock Runtime handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "BedrockRuntime" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"InvokeModel",
		"InvokeModelWithResponseStream",
		"Converse",
		"ConverseStream",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "bedrockruntime" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Bedrock Runtime requests.
// It matches all paths beginning with /model/.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		return strings.HasPrefix(c.Request().URL.Path, modelPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation returns the operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return pathToOperation(c.Request().URL.Path)
}

// ExtractResource extracts the modelId from the request path.
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path
	rest, ok := strings.CutPrefix(path, modelPathPrefix)
	if !ok {
		return ""
	}

	modelID, _, _ := strings.Cut(rest, "/")

	return modelID
}

// Handler returns the Echo handler function for Bedrock Runtime requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		log := logger.Load(r.Context())

		if r.Method != http.MethodPost {
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "bedrockruntime: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		modelID := extractModelID(path)
		if modelID == "" {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "missing modelId in path"))
		}

		op := pathToOperation(path)

		switch op {
		case "InvokeModel":
			return h.handleInvokeModel(r.Context(), c, modelID, body)
		case "InvokeModelWithResponseStream":
			return h.handleInvokeModelWithResponseStream(r.Context(), c, modelID, body)
		case "Converse":
			return h.handleConverse(r.Context(), c, modelID, body)
		case "ConverseStream":
			return h.handleConverseStream(r.Context(), c, modelID, body)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("UnknownOperationException", "unknown operation: "+path))
		}
	}
}

// handleInvokeModel handles POST /model/{modelId}/invoke.
func (h *Handler) handleInvokeModel(
	ctx context.Context,
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockInvokeModelResponse(modelID)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("InvokeModel", modelID, string(body), string(out))

	_ = ctx

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// handleInvokeModelWithResponseStream handles POST /model/{modelId}/invoke-with-response-stream.
// It returns a minimal event-stream response containing a single chunk event and a stop event.
func (h *Handler) handleInvokeModelWithResponseStream(
	ctx context.Context,
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockInvokeModelResponse(modelID)

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("InvokeModelWithResponseStream", modelID, string(body), string(out))

	_ = ctx

	// Build a minimal AWS event stream frame containing the response bytes as a single chunk event.
	frame := buildEventStreamFrame(out)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)
	_, _ = c.Response().Write(frame)

	return nil
}

// handleConverse handles POST /model/{modelId}/converse.
func (h *Handler) handleConverse(
	ctx context.Context,
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockConverseResponse()

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("Converse", modelID, string(body), string(out))

	_ = ctx

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// handleConverseStream handles POST /model/{modelId}/converse-stream.
func (h *Handler) handleConverseStream(
	ctx context.Context,
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := mockConverseResponse()

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("ConverseStream", modelID, string(body), string(out))

	_ = ctx

	frame := buildEventStreamFrame(out)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)
	_, _ = c.Response().Write(frame)

	return nil
}

// --- Mock responses ---

func mockInvokeModelResponse(modelID string) map[string]any {
	modelIDLower := strings.ToLower(modelID)

	switch {
	case strings.Contains(modelIDLower, "claude"):
		return map[string]any{
			"completion": "This is a mock response from Gopherstack.",
			"stop_reason": "end_turn",
			"model": modelID,
		}
	case strings.Contains(modelIDLower, "titan"):
		return map[string]any{
			"results": []map[string]any{
				{"outputText": "This is a mock response from Gopherstack.", "completionReason": "FINISH"},
			},
			"inputTextTokenCount": 10,
		}
	case strings.Contains(modelIDLower, "llama"):
		return map[string]any{
			"generation":            "This is a mock response from Gopherstack.",
			"prompt_token_count":    10,
			"generation_token_count": 10,
			"stop_reason":           "stop",
		}
	default:
		return map[string]any{
			"completion": "This is a mock response from Gopherstack.",
			"stop_reason": "end_turn",
			"model": modelID,
		}
	}
}

func mockConverseResponse() map[string]any {
	return map[string]any{
		"output": map[string]any{
			"message": map[string]any{
				"role": "assistant",
				"content": []map[string]any{
					{"text": "This is a mock response from Gopherstack."},
				},
			},
		},
		"stopReason": "end_turn",
		"usage": map[string]any{
			"inputTokens":  10,
			"outputTokens": 10,
			"totalTokens":  20,
		},
		"metrics": map[string]any{
			"latencyMs": 1,
		},
	}
}

// buildEventStreamFrame encodes payload as a minimal AWS binary event stream frame.
// Frame structure: total-length (4 bytes BE) | headers-length (4 bytes BE) |
//
//	prelude-CRC (4 bytes) | headers | payload | message-CRC (4 bytes)
//
// For simplicity the mock emits a frame with no headers so headers-length = 0.
func buildEventStreamFrame(payload []byte) []byte {
	// Total length = 4 (total-len) + 4 (headers-len) + 4 (prelude-CRC) + payload + 4 (msg-CRC)
	totalLen := uint32(4 + 4 + 4 + len(payload) + 4)

	var buf bytes.Buffer
	writeUint32BE(&buf, totalLen)
	writeUint32BE(&buf, 0) // headers-length
	writeUint32BE(&buf, 0) // prelude-CRC (mock: 0)
	buf.Write(payload)
	writeUint32BE(&buf, 0) // message-CRC (mock: 0)

	return buf.Bytes()
}

func writeUint32BE(buf *bytes.Buffer, v uint32) {
	buf.WriteByte(byte(v >> 24))
	buf.WriteByte(byte(v >> 16))
	buf.WriteByte(byte(v >> 8))
	buf.WriteByte(byte(v))
}

// --- Helpers ---

func extractModelID(path string) string {
	rest, ok := strings.CutPrefix(path, modelPathPrefix)
	if !ok {
		return ""
	}

	modelID, _, _ := strings.Cut(rest, "/")

	return modelID
}

func pathToOperation(path string) string {
	switch {
	case strings.HasSuffix(path, "/invoke-with-response-stream"):
		return "InvokeModelWithResponseStream"
	case strings.HasSuffix(path, "/invoke"):
		return "InvokeModel"
	case strings.HasSuffix(path, "/converse-stream"):
		return "ConverseStream"
	case strings.HasSuffix(path, "/converse"):
		return "Converse"
	default:
		return "Unknown"
	}
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}
