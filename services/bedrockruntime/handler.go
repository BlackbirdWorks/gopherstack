package bedrockruntime

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const modelPathPrefix = "/model/"
const guardrailPathPrefix = "/guardrail/"
const asyncInvokePathPrefix = "/async-invoke"
const asyncInvokeItemPathPrefix = asyncInvokePathPrefix + "/"

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
	Backend       *InMemoryBackend
	janitorCancel context.CancelFunc
	janitorDone   chan struct{}
}

// NewHandler creates a new Bedrock Runtime handler backed by backend.
// backend must not be nil.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// StartWorker starts the background janitor for async invocations.
func (h *Handler) StartWorker(ctx context.Context) error {
	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	h.janitorCancel = cancel
	h.janitorDone = done

	go func() {
		defer close(done)
		// Run janitor every hour.
		h.Backend.RunJanitor(runCtx, time.Hour)
	}()

	return nil
}

// Shutdown stops the background janitor.
func (h *Handler) Shutdown(ctx context.Context) {
	if h.janitorCancel != nil {
		h.janitorCancel()
	}

	if h.janitorDone != nil {
		select {
		case <-h.janitorDone:
		case <-ctx.Done():
		}
	}
}

var _ service.BackgroundWorker = (*Handler)(nil)
var _ service.Shutdowner = (*Handler)(nil)

// Name returns the service name.
func (h *Handler) Name() string { return "BedrockRuntime" }

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"ApplyGuardrail",
		"Converse",
		"ConverseStream",
		"CountTokens",
		"GetAsyncInvoke",
		"InvokeModel",
		"InvokeModelWithBidirectionalStream",
		"InvokeModelWithResponseStream",
		"ListAsyncInvokes",
		"StartAsyncInvoke",
	}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "bedrockruntime" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{h.Backend.Region()} }

// RouteMatcher returns a function that matches Bedrock Runtime requests.
// It matches paths for /model/, /guardrail/, and /async-invoke.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return strings.HasPrefix(path, modelPathPrefix) ||
			strings.HasPrefix(path, guardrailPathPrefix) ||
			path == asyncInvokePathPrefix ||
			strings.HasPrefix(path, asyncInvokeItemPathPrefix)
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return service.PriorityPathVersioned }

// ExtractOperation returns the operation name from the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	return pathToOperation(c.Request().URL.Path, c.Request().Method)
}

// ExtractResource extracts the primary resource identifier from the request path.
// For metrics/logging purposes, returns stable low-cardinality values:
// model paths return the modelId, guardrail paths return the guardrailIdentifier,
// and /async-invoke item paths return "async-invoke" (stable, not the ARN).
func (h *Handler) ExtractResource(c *echo.Context) string {
	path := c.Request().URL.Path

	switch {
	case strings.HasPrefix(path, modelPathPrefix):
		rest, _ := strings.CutPrefix(path, modelPathPrefix)
		modelID, _, _ := strings.Cut(rest, "/")

		return modelID
	case strings.HasPrefix(path, guardrailPathPrefix):
		rest, _ := strings.CutPrefix(path, guardrailPathPrefix)
		guardrailID, _, _ := strings.Cut(rest, "/")

		return guardrailID
	case strings.HasPrefix(path, asyncInvokeItemPathPrefix):
		// Return stable label; the full ARN would be unique per invocation.
		return "async-invoke"
	default:
		return ""
	}
}

// Reset clears all backend state. Implements service.Resettable.
func (h *Handler) Reset() {
	h.Backend.Reset()
}

// Handler returns the Echo handler function for Bedrock Runtime requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		path := r.URL.Path
		method := r.Method
		log := logger.Load(r.Context())

		body, err := httputils.ReadBody(r)
		if err != nil {
			log.ErrorContext(r.Context(), "bedrockruntime: failed to read request body", "error", err)

			return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
		}

		switch {
		case strings.HasPrefix(path, modelPathPrefix):
			return h.handleModelPath(c, method, path, body)
		case strings.HasPrefix(path, guardrailPathPrefix):
			return h.handleGuardrailPath(c, method, path, body)
		case path == asyncInvokePathPrefix || strings.HasPrefix(path, asyncInvokeItemPathPrefix):
			return h.handleAsyncInvokePath(c, method, path, body)
		default:
			return c.JSON(http.StatusNotFound, errorResponse("UnknownOperationException", "unknown operation: "+path))
		}
	}
}

// handleModelPath dispatches requests for /model/{modelId}/... paths.
func (h *Handler) handleModelPath(c *echo.Context, method, path string, body []byte) error {
	if method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
	}

	modelID := extractModelID(path)
	if modelID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "missing modelId in path"))
	}

	op := pathToOperation(path, method)

	switch op {
	case "InvokeModel":
		return h.handleInvokeModel(c, modelID, body)
	case "InvokeModelWithResponseStream":
		return h.handleInvokeModelWithResponseStream(c, modelID, body)
	case "Converse":
		return h.handleConverse(c, modelID, body)
	case "ConverseStream":
		return h.handleConverseStream(c, modelID, body)
	case "CountTokens":
		return h.handleCountTokens(c, modelID, body)
	case "InvokeModelWithBidirectionalStream":
		return h.handleInvokeModelWithBidirectionalStream(c, modelID, body)
	default:
		return c.JSON(http.StatusNotFound, errorResponse("UnknownOperationException", "unknown operation: "+path))
	}
}

// handleGuardrailPath dispatches requests for /guardrail/... paths.
func (h *Handler) handleGuardrailPath(c *echo.Context, method, path string, body []byte) error {
	if method != http.MethodPost {
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
	}

	if strings.HasSuffix(path, "/apply") {
		return h.handleApplyGuardrail(c, path, body)
	}

	return c.JSON(http.StatusNotFound, errorResponse("UnknownOperationException", "unknown operation: "+path))
}

// handleAsyncInvokePath dispatches requests for /async-invoke[/{arn}] paths.
func (h *Handler) handleAsyncInvokePath(c *echo.Context, method, path string, body []byte) error {
	switch {
	case path == asyncInvokePathPrefix && method == http.MethodGet:
		return h.handleListAsyncInvokes(c)
	case path == asyncInvokePathPrefix && method == http.MethodPost:
		return h.handleStartAsyncInvoke(c, body)
	case strings.HasPrefix(path, asyncInvokeItemPathPrefix) && method == http.MethodGet:
		return h.handleGetAsyncInvoke(c, path)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("ValidationException", "method not allowed"))
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

// handleCountTokens handles POST /model/{modelId}/count-tokens.
func (h *Handler) handleCountTokens(
	c *echo.Context,
	modelID string,
	body []byte,
) error {
	resp := map[string]any{
		"inputTokens": mockInputTokenCount,
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("CountTokens", modelID, string(body), string(out))

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
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
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("InvokeModelWithBidirectionalStream", modelID, string(body), string(out))

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

// handleApplyGuardrail handles POST /guardrail/{guardrailIdentifier}/version/{guardrailVersion}/apply.
func (h *Handler) handleApplyGuardrail(
	c *echo.Context,
	path string,
	body []byte,
) error {
	guardrailID, guardrailVersion := extractGuardrailIDAndVersion(path)

	if guardrailID == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "guardrailIdentifier is required"))
	}

	if guardrailVersion == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "guardrailVersion is required"))
	}

	resp := map[string]any{
		"action":      "NONE",
		"assessments": []map[string]any{},
		"outputs":     []map[string]any{},
		"usage": map[string]any{
			"topicPolicyUnits":                    0,
			"contentPolicyUnits":                  0,
			"wordPolicyUnits":                     0,
			"sensitiveInformationPolicyUnits":     0,
			"sensitiveInformationPolicyFreeUnits": 0,
			"contextualGroundingPolicyUnits":      0,
		},
	}

	out, err := json.Marshal(resp)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation("ApplyGuardrail", guardrailID+"/"+guardrailVersion, string(body), string(out))

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(http.StatusOK, out)
}

// startAsyncInvokeInput is the parsed request body for StartAsyncInvoke.
type startAsyncInvokeInput struct {
	Tags             map[string]string `json:"tags"`
	OutputDataConfig struct {
		S3OutputDataConfig struct {
			S3URI string `json:"s3Uri"`
		} `json:"s3OutputDataConfig"`
	} `json:"outputDataConfig"`
	ModelID            string `json:"modelId"`
	ClientRequestToken string `json:"clientRequestToken"`
}

// handleStartAsyncInvoke handles POST /async-invoke.
func (h *Handler) handleStartAsyncInvoke(c *echo.Context, body []byte) error {
	var req startAsyncInvokeInput

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	inv, err := h.Backend.StartAsyncInvoke(
		req.ModelID,
		req.OutputDataConfig.S3OutputDataConfig.S3URI,
		req.ClientRequestToken,
		req.Tags,
	)
	if err != nil {
		return handleError(c, err)
	}

	resp := map[string]any{
		"invocationArn": inv.InvocationArn,
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSON(http.StatusAccepted, resp)
}

// handleGetAsyncInvoke handles GET /async-invoke/{invocationArn}.
func (h *Handler) handleGetAsyncInvoke(c *echo.Context, path string) error {
	invocationArn, _ := strings.CutPrefix(path, asyncInvokeItemPathPrefix)
	if invocationArn == "" {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "missing invocationArn in path"),
		)
	}

	inv, err := h.Backend.GetAsyncInvoke(invocationArn)
	if err != nil {
		return handleError(c, err)
	}

	resp := map[string]any{
		"invocationArn": inv.InvocationArn,
		"modelArn":      inv.ModelArn,
		"outputDataConfig": map[string]any{
			"s3OutputDataConfig": map[string]any{
				"s3Uri": inv.OutputS3URI,
			},
		},
		"status":           inv.Status,
		"submitTime":       inv.SubmitTime.Format(time.RFC3339),
		"lastModifiedTime": inv.LastModifiedTime.Format(time.RFC3339),
	}

	if inv.ClientRequestToken != nil {
		resp["clientRequestToken"] = *inv.ClientRequestToken
	}

	if inv.EndTime != nil {
		resp["endTime"] = inv.EndTime.Format(time.RFC3339)
	}

	if inv.FailureMessage != nil {
		resp["failureMessage"] = *inv.FailureMessage
	}

	if len(inv.Tags) > 0 {
		resp["tags"] = inv.Tags
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSON(http.StatusOK, resp)
}

// handleListAsyncInvokes handles GET /async-invoke.
// Supports optional query parameter: statusEquals (InProgress|Completed|Failed).
func (h *Handler) handleListAsyncInvokes(c *echo.Context) error {
	statusFilter := c.QueryParam("statusEquals")
	invocations := h.Backend.ListAsyncInvokes(ListAsyncInvokesFilter{StatusEquals: statusFilter})

	summaries := make([]map[string]any, 0, len(invocations))

	for _, inv := range invocations {
		summary := map[string]any{
			"invocationArn": inv.InvocationArn,
			"modelArn":      inv.ModelArn,
			"outputDataConfig": map[string]any{
				"s3OutputDataConfig": map[string]any{
					"s3Uri": inv.OutputS3URI,
				},
			},
			"status":           inv.Status,
			"submitTime":       inv.SubmitTime.Format(time.RFC3339),
			"lastModifiedTime": inv.LastModifiedTime.Format(time.RFC3339),
		}

		if inv.ClientRequestToken != nil {
			summary["clientRequestToken"] = *inv.ClientRequestToken
		}

		if inv.EndTime != nil {
			summary["endTime"] = inv.EndTime.Format(time.RFC3339)
		}

		if inv.FailureMessage != nil {
			summary["failureMessage"] = *inv.FailureMessage
		}

		if len(inv.Tags) > 0 {
			summary["tags"] = inv.Tags
		}

		summaries = append(summaries, summary)
	}

	resp := map[string]any{
		"asyncInvokeSummaries": summaries,
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSON(http.StatusOK, resp)
}

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
	case strings.Contains(modelIDLower, "mistral") || strings.Contains(modelIDLower, "mixtral"):
		return map[string]any{
			"outputs": []map[string]any{
				{"text": "This is a mock response from Gopherstack.", "stop_reason": "stop"},
			},
		}
	case strings.Contains(modelIDLower, "command"):
		return map[string]any{
			"text":          "This is a mock response from Gopherstack.",
			"stop_reason":   "COMPLETE",
			"finish_reason": "COMPLETE",
		}
	case strings.Contains(modelIDLower, "j2") || strings.Contains(modelIDLower, "jurassic"):
		return map[string]any{
			"completions": []map[string]any{
				{
					"data":         map[string]any{"text": "This is a mock response from Gopherstack."},
					"finishReason": map[string]any{"reason": "endoftext"},
				},
			},
		}
	case strings.Contains(modelIDLower, "nova"):
		return map[string]any{
			"output": map[string]any{
				"message": map[string]any{
					"role":    "assistant",
					"content": []map[string]any{{"text": "This is a mock response from Gopherstack."}},
				},
			},
			"stopReason": "end_turn",
			"usage": map[string]any{
				"inputTokens":  mockInputTokenCount,
				"outputTokens": mockOutputTokenCount,
				"totalTokens":  mockTotalTokenCount,
			},
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

	// Prelude (12 bytes) + headers + payload + message CRC (4 bytes).
	// Guard against integer overflow when calculating totalLen.
	totalLen := uint64(eventStreamPreludeLen) + uint64(headerLen) + uint64(payloadLen) + uint64(eventStreamMsgCRCLen)
	if totalLen > math.MaxInt32 {
		return nil
	}

	buf := make([]byte, totalLen)

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
		nameLen := len(name)
		if nameLen > math.MaxUint8 {
			continue
		}

		buf[n] = byte(nameLen)
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

func pathToOperation(path, method string) string {
	if op := modelPathOperation(path); op != "" {
		return op
	}

	if op := asyncOrGuardrailOperation(path, method); op != "" {
		return op
	}

	return "Unknown"
}

// modelPathOperation maps /model/{modelId}/... paths to operation names.
func modelPathOperation(path string) string {
	switch {
	case strings.HasSuffix(path, "/invoke-with-response-stream"):
		return "InvokeModelWithResponseStream"
	case strings.HasSuffix(path, "/invoke-with-bidirectional-stream"):
		return "InvokeModelWithBidirectionalStream"
	case strings.HasSuffix(path, "/invoke"):
		return "InvokeModel"
	case strings.HasSuffix(path, "/converse-stream"):
		return "ConverseStream"
	case strings.HasSuffix(path, "/converse"):
		return "Converse"
	case strings.HasSuffix(path, "/count-tokens"):
		return "CountTokens"
	default:
		return ""
	}
}

// asyncOrGuardrailOperation maps /guardrail/... and /async-invoke paths to operation names.
func asyncOrGuardrailOperation(path, method string) string {
	switch {
	case strings.HasPrefix(path, guardrailPathPrefix) && strings.HasSuffix(path, "/apply"):
		return "ApplyGuardrail"
	case path == asyncInvokePathPrefix && method == http.MethodGet:
		return "ListAsyncInvokes"
	case path == asyncInvokePathPrefix && method == http.MethodPost:
		return "StartAsyncInvoke"
	case strings.HasPrefix(path, asyncInvokeItemPathPrefix) && method == http.MethodGet:
		return "GetAsyncInvoke"
	default:
		return ""
	}
}

// extractGuardrailIDAndVersion extracts the guardrailIdentifier and guardrailVersion
// from a path of the form /guardrail/{id}/version/{ver}/apply.
func extractGuardrailIDAndVersion(path string) (string, string) {
	rest, ok := strings.CutPrefix(path, guardrailPathPrefix)
	if !ok {
		return "", ""
	}

	// rest = "{id}/version/{ver}/apply"
	guardrailID, rest, _ := strings.Cut(rest, "/version/")
	guardrailVersion, _, _ := strings.Cut(rest, "/")

	return guardrailID, guardrailVersion
}

func errorResponse(code, msg string) map[string]string {
	return map[string]string{"__type": code, "message": msg}
}

// handleError writes a standardized error response, mapping sentinel errors to HTTP status codes.
func handleError(c *echo.Context, err error) error {
	switch {
	case errors.Is(err, ErrValidation):
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", err.Error()))
	case errors.Is(err, awserr.ErrNotFound):
		return c.JSON(http.StatusNotFound, errorResponse("ResourceNotFoundException", err.Error()))
	default:
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}
}

// Purge implements service.Purgeable by removing all Bedrock Runtime invocation records older than cutoff.
func (h *Handler) Purge(ctx context.Context, cutoff time.Time) {
	h.Backend.Purge(ctx, cutoff)
}
