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

const (
	opConverse                      = "Converse"
	opConverseStream                = "ConverseStream"
	opCountTokens                   = "CountTokens"
	opInvokeModel                   = "InvokeModel"
	opInvokeModelWithBidiStream     = "InvokeModelWithBidirectionalStream"
	opInvokeModelWithResponseStream = "InvokeModelWithResponseStream"

	hdrMessageType = ":message-type"
	hdrEventType   = ":event-type"
	hdrContentType = ":content-type"

	keyInputTokens   = "inputTokens"
	keyUsage         = "usage"
	keyInvocationArn = "invocationArn"
	keyText          = "text"
	keyMessage       = "message"

	mockResponseText  = "This is a mock response from Gopherstack."
	stopReasonEndTurn = "end_turn"

	hdrMessageTypeEvent = "event"
	keyStopReason       = "stop_reason"

	roleAssistant     = "assistant"
	convStopReasonKey = "stopReason"
	convOutputTokens  = "outputTokens"
	convTotalTokens   = "totalTokens"
	convContentIdx    = "contentBlockIndex"
)

// Mock response token counts used in model responses.
const (
	mockInputTokenCount  = 10
	mockOutputTokenCount = 10
	mockTotalTokenCount  = 20
	mockLatencyMS        = 1
)

// maxInvocationStringBytes caps the stored request/response string length to prevent unbounded growth.
const maxInvocationStringBytes = 10_000

// charsPerToken is an approximation used for CountTokens (BPE models: ~4 chars/token).
const charsPerToken = 4

// charsPerTokenTitan is the approximation for Titan models (~6 chars/token).
const charsPerTokenTitan = 6

// eventStreamHeaderInitialCap is the initial capacity for event stream header encoding.
const eventStreamHeaderInitialCap = 256

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
		opConverse,
		opConverseStream,
		opCountTokens,
		"GetAsyncInvoke",
		opInvokeModel,
		opInvokeModelWithBidiStream,
		opInvokeModelWithResponseStream,
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
	case opInvokeModel:
		return h.handleInvokeModel(c, modelID, body)
	case opInvokeModelWithResponseStream:
		return h.handleInvokeModelWithResponseStream(c, modelID, body)
	case opConverse:
		return h.handleConverse(c, modelID, body)
	case opConverseStream:
		return h.handleConverseStream(c, modelID, body)
	case opCountTokens:
		return h.handleCountTokens(c, modelID, body)
	case opInvokeModelWithBidiStream:
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
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation(opInvokeModel, modelID, truncateString(string(body)), truncateString(string(out)))

	ct := resolveResponseContentType(c.Request())
	c.Response().Header().Set("Content-Type", ct)

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
				"role":    roleAssistant,
				"content": []map[string]any{{keyText: mockResponseText}},
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
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
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
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
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
		"role": roleAssistant,
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

// countTokensRequest represents the parsed CountTokens request body.
type countTokensRequest struct {
	Prompt   string            `json:"prompt,omitempty"`
	Messages []converseMessage `json:"messages,omitempty"`
	System   []converseContent `json:"system,omitempty"`
}

// estimateTokenCount returns an approximate token count for the CountTokens request body.
// The approximation is ~4 chars/token (works reasonably for English BPE models).
func estimateTokenCount(body []byte, modelID string) int {
	if len(body) == 0 {
		return mockInputTokenCount
	}

	var req countTokensRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return len(body) / charsPerToken
	}

	chars := len(req.Prompt)

	for _, m := range req.Messages {
		for _, c := range m.Content {
			chars += len(c.Text)
		}
	}

	for _, s := range req.System {
		chars += len(s.Text)
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
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", "internal server error"))
	}

	h.Backend.RecordInvocation(opCountTokens, modelID, truncateString(string(body)), truncateString(string(out)))

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

// applyGuardrailRequest represents the parsed ApplyGuardrail request body.
type applyGuardrailRequest struct {
	Source  string                 `json:"source"`
	Content []guardrailContentItem `json:"content"`
}

// guardrailContentItem represents a single item in the guardrail content list.
type guardrailContentItem struct {
	Text *guardrailTextBlock `json:"text,omitempty"`
}

// guardrailTextBlock holds the text for a guardrail content item.
type guardrailTextBlock struct {
	Text string `json:"text"`
}

// guardrailBlockPatterns are keywords that trigger BLOCKED action in the guardrail mock.
// Deterministic behavior allows tests to verify BLOCKED vs NONE outcomes.
const (
	guardrailPatternBlocked = "blocked"
	guardrailPatternHarmful = "harmful"
	guardrailPatternToxic   = "toxic"
	guardrailPatternUnsafe  = "unsafe"
)

// evaluateGuardrailAction returns "BLOCKED" if the content matches known trigger patterns.
func evaluateGuardrailAction(content []guardrailContentItem) string {
	for _, item := range content {
		if item.Text == nil {
			continue
		}

		lower := strings.ToLower(item.Text.Text)

		for _, kw := range []string{
			guardrailPatternBlocked,
			guardrailPatternHarmful,
			guardrailPatternToxic,
			guardrailPatternUnsafe,
		} {
			if strings.Contains(lower, kw) {
				return "BLOCKED"
			}
		}
	}

	return "NONE"
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

	var req applyGuardrailRequest
	if len(body) > 0 {
		_ = json.Unmarshal(body, &req)
	}

	action := evaluateGuardrailAction(req.Content)

	resp := map[string]any{
		"action":      action,
		"assessments": []map[string]any{},
		"outputs":     buildGuardrailOutputs(req),
		keyUsage: map[string]any{
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

// buildGuardrailOutputs reflects filtered content back in the outputs list.
// For NONE action, outputs mirror the input content unchanged.
func buildGuardrailOutputs(req applyGuardrailRequest) []map[string]any {
	if len(req.Content) == 0 {
		return []map[string]any{}
	}

	outputs := make([]map[string]any, 0, len(req.Content))

	for _, item := range req.Content {
		if item.Text != nil {
			outputs = append(outputs, map[string]any{
				keyText: item.Text.Text,
			})
		}
	}

	return outputs
}

// startAsyncInvokeInput is the parsed request body for StartAsyncInvoke.
type startAsyncInvokeInput struct {
	Tags             map[string]string `json:"tags"`
	OutputDataConfig struct {
		S3OutputDataConfig struct {
			S3URI string `json:"s3Uri"`
		} `json:"s3OutputDataConfig"`
	} `json:"outputDataConfig"`
	ModelID                    string `json:"modelId"`
	ClientRequestToken         string `json:"clientRequestToken"`
	InferenceProfileIdentifier string `json:"inferenceProfileIdentifier,omitempty"`
}

// handleStartAsyncInvoke handles POST /async-invoke.
func (h *Handler) handleStartAsyncInvoke(c *echo.Context, body []byte) error {
	var req startAsyncInvokeInput

	if err := json.Unmarshal(body, &req); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
	}

	s3URI := req.OutputDataConfig.S3OutputDataConfig.S3URI
	if s3URI != "" && !strings.HasPrefix(s3URI, "s3://") {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "outputDataConfig.s3OutputDataConfig.s3Uri must start with s3://"),
		)
	}

	effectiveModelID := req.ModelID
	if effectiveModelID == "" && req.InferenceProfileIdentifier != "" {
		effectiveModelID = req.InferenceProfileIdentifier
	}

	inv, err := h.Backend.StartAsyncInvoke(
		effectiveModelID,
		s3URI,
		req.ClientRequestToken,
		req.Tags,
	)
	if err != nil {
		return handleError(c, err)
	}

	resp := map[string]any{
		keyInvocationArn: inv.InvocationArn,
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

	resp := buildAsyncInvokeResponse(inv)

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
		summaries = append(summaries, buildAsyncInvokeResponse(inv))
	}

	resp := map[string]any{
		"asyncInvokeSummaries": summaries,
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSON(http.StatusOK, resp)
}

// buildAsyncInvokeResponse constructs the JSON response for a single async invocation.
// Fields that are only valid in terminal states (Completed/Failed) are omitted otherwise.
func buildAsyncInvokeResponse(inv *AsyncInvoke) map[string]any {
	resp := map[string]any{
		keyInvocationArn: inv.InvocationArn,
		"modelArn":       inv.ModelArn,
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

	// EndTime and failureMessage are only meaningful after the invocation has finished.
	isTerminal := inv.Status == AsyncInvokeStatusCompleted || inv.Status == AsyncInvokeStatusFailed

	if isTerminal && inv.EndTime != nil {
		resp["endTime"] = inv.EndTime.Format(time.RFC3339)
	}

	if inv.Status == AsyncInvokeStatusFailed && inv.FailureMessage != nil {
		resp["failureMessage"] = *inv.FailureMessage
	}

	if len(inv.Tags) > 0 {
		resp["tags"] = inv.Tags
	}

	return resp
}

func mockInvokeModelResponse(modelID string) map[string]any {
	modelIDLower := strings.ToLower(modelID)

	switch {
	case strings.Contains(modelIDLower, "claude"):
		return map[string]any{
			"completion":  mockResponseText,
			keyStopReason: stopReasonEndTurn,
			"model":       modelID,
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
					"role":    roleAssistant,
					"content": []map[string]any{{keyText: mockResponseText}},
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
			"model":       modelID,
		}
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
// It uses a dynamic slice to avoid silent truncation on overflow.
func buildEventStreamHeaders(hdrs [][2]string) []byte {
	buf := make([]byte, 0, eventStreamHeaderInitialCap)

	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		nameLen := len(name)

		if nameLen > math.MaxUint8 {
			// AWS event stream protocol: header name must fit in a single byte length field.
			continue
		}

		if len(value) > math.MaxUint16 {
			// AWS event stream protocol: header value length is 2 bytes.
			continue
		}

		buf = append(buf, byte(nameLen))
		buf = append(buf, name...)
		buf = append(buf, eventStreamHeaderValueTypeString)

		var lenBuf [eventStreamHeaderValueLenBytes]byte
		//nolint:gosec // len(value) bounded by MaxUint16 check above
		binary.BigEndian.PutUint16(lenBuf[:], uint16(len(value)))
		buf = append(buf, lenBuf[:]...)
		buf = append(buf, value...)
	}

	return buf
}

// flushResponse flushes the response writer if it implements http.Flusher.
func flushResponse(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

// truncateString limits a string to maxInvocationStringBytes bytes to cap memory usage.
func truncateString(s string) string {
	if len(s) <= maxInvocationStringBytes {
		return s
	}

	return s[:maxInvocationStringBytes]
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
		return opInvokeModelWithResponseStream
	case strings.HasSuffix(path, "/invoke-with-bidirectional-stream"):
		return opInvokeModelWithBidiStream
	case strings.HasSuffix(path, "/invoke"):
		return opInvokeModel
	case strings.HasSuffix(path, "/converse-stream"):
		return opConverseStream
	case strings.HasSuffix(path, "/converse"):
		return opConverse
	case strings.HasSuffix(path, "/count-tokens"):
		return opCountTokens
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
	return map[string]string{"__type": code, keyMessage: msg}
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
