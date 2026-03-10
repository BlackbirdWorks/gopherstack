package bedrockruntime

import (
	"encoding/binary"
	"encoding/json"
	"hash/crc32"
	"math"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const modelPathPrefix = "/model/"

// Event stream frame constants (AWS binary event stream protocol).
const (
	eventStreamPreludeLen = 12 // 4 (total-len) + 4 (headers-len) + 4 (prelude-CRC)
	eventStreamMsgCRCLen  = 4

	// eventStreamHeaderValueTypeString is the AWS event stream type byte for string headers.
	eventStreamHeaderValueTypeString = 7
	// eventStreamHeaderValueLenBytes is the number of bytes used to encode a header value length.
	eventStreamHeaderValueLenBytes = 2
)

// Mock response token counts used in model responses.
const (
	mockInputTokenCount  = 10
	mockOutputTokenCount = 10
	mockTotalTokenCount  = 20
	mockLatencyMS        = 1
)

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
			return h.handleInvokeModel(c, modelID, body)
		case "InvokeModelWithResponseStream":
			return h.handleInvokeModelWithResponseStream(c, modelID, body)
		case "Converse":
			return h.handleConverse(c, modelID, body)
		case "ConverseStream":
			return h.handleConverseStream(c, modelID, body)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("UnknownOperationException", "unknown operation: "+path))
		}
	}
}

// handleInvokeModel handles POST /model/{modelId}/invoke.
func (h *Handler) handleInvokeModel(
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

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
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
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("InvokeModelWithResponseStream", modelID, string(body), string(out))

	frame := encodeEventStreamMsg([][2]string{
		{":message-type", "event"},
		{":event-type", "chunk"},
		{":content-type", "application/octet-stream"},
	}, out)

	c.Response().Header().Set("Content-Type", "application/vnd.amazon.eventstream")
	c.Response().WriteHeader(http.StatusOK)
	_, _ = c.Response().Write(frame)

	return nil
}

// handleConverse handles POST /model/{modelId}/converse.
func (h *Handler) handleConverse(
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

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// handleConverseStream handles POST /model/{modelId}/converse-stream.
func (h *Handler) handleConverseStream(
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

	frame := encodeEventStreamMsg([][2]string{
		{":message-type", "event"},
		{":event-type", "messageStop"},
		{":content-type", "application/json"},
	}, out)

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
			"completion":  "This is a mock response from Gopherstack.",
			"stop_reason": "end_turn",
			"model":       modelID,
		}
	case strings.Contains(modelIDLower, "titan"):
		return map[string]any{
			"results": []map[string]any{
				{"outputText": "This is a mock response from Gopherstack.", "completionReason": "FINISH"},
			},
			"inputTextTokenCount": mockInputTokenCount,
		}
	case strings.Contains(modelIDLower, "llama"):
		return map[string]any{
			"generation":             "This is a mock response from Gopherstack.",
			"prompt_token_count":     mockInputTokenCount,
			"generation_token_count": mockOutputTokenCount,
			"stop_reason":            "stop",
		}
	default:
		return map[string]any{
			"completion":  "This is a mock response from Gopherstack.",
			"stop_reason": "end_turn",
			"model":       modelID,
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
			"inputTokens":  mockInputTokenCount,
			"outputTokens": mockOutputTokenCount,
			"totalTokens":  mockTotalTokenCount,
		},
		"metrics": map[string]any{
			"latencyMs": mockLatencyMS,
		},
	}
}

// encodeEventStreamMsg encodes a single AWS event stream binary message.
// Format: totalLen(4) | headersLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
// Uses the same framing as the Kinesis event stream implementation.
func encodeEventStreamMsg(hdrs [][2]string, payload []byte) []byte {
	hdrBytes := buildEventStreamHeaders(hdrs)
	headerLen := len(hdrBytes)
	payloadLen := len(payload)

	// Guard against integer overflow: payloadLen must fit within the remaining frame space.
	if headerLen > math.MaxInt32-eventStreamPreludeLen-payloadLen-eventStreamMsgCRCLen {
		return nil
	}

	totalLen := eventStreamPreludeLen + headerLen + payloadLen + eventStreamMsgCRCLen
	buf := make([]byte, totalLen)

	//nolint:gosec // totalLen is bounded by the overflow check above
	binary.BigEndian.PutUint32(buf[0:4], uint32(totalLen))
	//nolint:gosec // headerLen is bounded by the overflow check above
	binary.BigEndian.PutUint32(buf[4:8], uint32(headerLen))

	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[8:eventStreamPreludeLen], preludeCRC)

	copy(buf[eventStreamPreludeLen:eventStreamPreludeLen+headerLen], hdrBytes)
	copy(buf[eventStreamPreludeLen+headerLen:eventStreamPreludeLen+headerLen+payloadLen], payload)

	msgCRC := crc32.ChecksumIEEE(buf[0 : eventStreamPreludeLen+headerLen+payloadLen])
	binary.BigEndian.PutUint32(buf[eventStreamPreludeLen+headerLen+payloadLen:], msgCRC)

	return buf
}

// buildEventStreamHeaders encodes name/value header pairs in AWS event stream binary format.
func buildEventStreamHeaders(hdrs [][2]string) []byte {
	var buf [512]byte
	n := 0

	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		buf[n] = byte(len(name)) //nolint:gosec // header name bounded by AWS event stream protocol
		n++
		n += copy(buf[n:], name)
		buf[n] = eventStreamHeaderValueTypeString
		n++
		//nolint:gosec // header value length fits in uint16 by AWS event stream protocol
		binary.BigEndian.PutUint16(buf[n:n+eventStreamHeaderValueLenBytes], uint16(len(value)))
		n += eventStreamHeaderValueLenBytes
		n += copy(buf[n:], value)
	}

	return buf[:n]
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
