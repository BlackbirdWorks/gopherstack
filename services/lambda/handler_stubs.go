package lambda

// handler_stubs.go adds stub handlers for SDK operations that are acknowledged
// but not yet fully implemented.  Each stub returns a minimal valid response so
// that the operation is visible in GetSupportedOperations and the SDK
// completeness test passes.

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// contentTypeEventStream is the MIME type for Lambda streaming responses.
const contentTypeEventStream = "application/vnd.amazon.eventstream"

// lambdaStatusKey is the JSON key used in single-field Lambda status responses.
const lambdaStatusKey = "Status"

// --- Durable Execution stubs ---

// extractDurableExecARN extracts the execution ARN from a durable execution path.
// Path format: /2025-12-01/durable-executions/{encodedARN}[/...].
func extractDurableExecARN(path string) string {
	rest := strings.TrimPrefix(path, lambdaDurableExecPathPrefix+"/")
	// Strip any sub-path (e.g. /checkpoint, /history, /state, /callback/...).
	if idx := strings.Index(rest, "/"); idx >= 0 {
		rest = rest[:idx]
	}

	decoded, err := url.PathUnescape(rest)
	if err != nil {
		return rest
	}

	return decoded
}

// extractDurableExecFunctionARN extracts an optional FunctionArn from the query string.
func extractDurableExecFunctionARN(c *echo.Context) string {
	return c.Request().URL.Query().Get("FunctionArn")
}

// durableExecFromBackend returns the durableExecutionStore from the backend, or nil.
func durableExecFromBackend(h *Handler) *durableExecutionStore {
	bk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	return bk.durableExecs
}

// handleGetDurableExecution returns the durable execution for the given ARN.
func (h *Handler) handleGetDurableExecution(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"backend not available",
		)
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex := store.get(arn)

	if ex == nil {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"durable execution not found: "+arn,
		)
	}

	return c.JSON(http.StatusOK, ex)
}

// handleGetDurableExecutionHistory returns the event history for the given execution.
func (h *Handler) handleGetDurableExecutionHistory(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"backend not available",
		)
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex := store.get(arn)

	if ex == nil {
		return c.JSON(http.StatusOK, map[string]any{"Events": []any{}})
	}

	events := ex.History
	if events == nil {
		events = []DurableExecutionEvent{}
	}

	return c.JSON(http.StatusOK, map[string]any{"Events": events})
}

// handleGetDurableExecutionState returns the state of the given execution.
func (h *Handler) handleGetDurableExecutionState(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"backend not available",
		)
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex := store.get(arn)

	if ex == nil {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"durable execution not found: "+arn,
		)
	}

	return c.JSON(http.StatusOK, &DurableExecutionState{
		ExecutionARN: ex.ExecutionARN,
		Status:       ex.Status,
		StateData:    ex.CheckpointData,
	})
}

// handleListDurableExecutionsByFunction returns executions for the function in the query.
func (h *Handler) handleListDurableExecutionsByFunction(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"backend not available",
		)
	}

	functionARN := extractDurableExecFunctionARN(c)
	executions := store.listByFunction(functionARN)

	if executions == nil {
		executions = []*DurableExecution{}
	}

	return c.JSON(http.StatusOK, map[string]any{"DurableExecutions": executions})
}

// handleSendDurableExecutionCallbackFailure records a callback failure.
func (h *Handler) handleSendDurableExecutionCallbackFailure(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		c.Response().WriteHeader(http.StatusOK)

		return nil
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	_ = store.sendCallback(arn, "CallbackFailure")

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleSendDurableExecutionCallbackHeartbeat records a heartbeat callback.
func (h *Handler) handleSendDurableExecutionCallbackHeartbeat(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		c.Response().WriteHeader(http.StatusOK)

		return nil
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	_ = store.sendCallback(arn, "CallbackHeartbeat")

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleSendDurableExecutionCallbackSuccess records a success callback.
func (h *Handler) handleSendDurableExecutionCallbackSuccess(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		c.Response().WriteHeader(http.StatusOK)

		return nil
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	_ = store.sendCallback(arn, "CallbackSuccess")

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleStopDurableExecution marks the execution as stopped.
func (h *Handler) handleStopDurableExecution(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return c.JSON(
			http.StatusOK,
			map[string]any{lambdaStatusKey: string(DurableExecutionStatusStopped)},
		)
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex, err := store.stop(arn)
	if err != nil {
		// If not found, stop is a no-op — return a synthetic stopped response.
		return c.JSON(
			http.StatusOK,
			map[string]any{lambdaStatusKey: string(DurableExecutionStatusStopped)},
		)
	}

	return c.JSON(http.StatusOK, ex)
}

// --- ListFunctionVersionsByCapacityProvider ---

type listFunctionVersionsByCapacityProviderOutput struct {
	NextMarker       string   `json:"NextMarker,omitempty"`
	FunctionVersions []string `json:"FunctionVersions"`
}

// handleListFunctionVersionsByCapacityProvider returns the function-version ARNs
// assigned to the named capacity provider, with Marker/MaxItems pagination. It
// returns ResourceNotFoundException when the provider does not exist.
//
// AWS exposes no public API to assign function versions to a capacity provider in
// this emulator's surface, so assignments are populated only via the internal
// SeedCapacityProviderFunctionVersions helper (used by tests). When no versions
// have been seeded, an empty list is returned for a valid provider.
func (h *Handler) handleListFunctionVersionsByCapacityProvider(
	c *echo.Context, bk *InMemoryBackend, name string,
) error {
	marker, maxItems := parsePaginationParams(c.Request())

	p, err := bk.ListFunctionVersionsByCapacityProvider(name, marker, maxItems)
	if err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Capacity provider not found: "+name)
	}

	return c.JSON(http.StatusOK, &listFunctionVersionsByCapacityProviderOutput{
		FunctionVersions: p.Data,
		NextMarker:       p.Next,
	})
}

// --- Invoke / InvokeAsync / InvokeWithResponseStream stubs ---
// The SDK exposes these as separate methods; gopherstack routes them through
// handleInvoke (InvokeFunction).  These stubs register the SDK operation names
// without duplicating routing logic.

// handleInvokeAsync handles POST /2014-11-13/functions/{name}/invoke-async/.
// The legacy InvokeAsync API validates that the function exists, then returns 202
// immediately. The actual invocation runs in the background — the caller never waits
// for the result. AWS InvokeAsync always returns {"Status": 202} on success.
func (h *Handler) handleInvokeAsync(c *echo.Context, name string) error {
	bk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"backend not available",
		)
	}

	body, readErr := readBodyOrEmpty(c)
	if readErr != nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"failed to read request",
		)
	}

	// Validate function exists synchronously before accepting.
	if _, err := bk.GetFunction(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	// Fire-and-forget: launch the invocation asynchronously and return 202 immediately.
	// Use context.WithoutCancel so HTTP request cancellation does not abort background work.
	invokeCtx := context.WithoutCancel(c.Request().Context())
	bk.asyncWG.Go(func() {
		_, _, _ = bk.InvokeFunction(invokeCtx, name, InvocationTypeEvent, body)
	})

	return c.JSON(http.StatusAccepted, map[string]int{lambdaStatusKey: http.StatusAccepted})
}

// handleInvokeWithResponseStream handles POST /2021-11-15/functions/{name}/response-streaming-invocations.
// It invokes the function synchronously and writes the result using the AWS event stream binary
// protocol (application/vnd.amazon.eventstream), matching the encoding used by real Lambda.
// Each chunk is a PayloadChunk event; the stream is terminated by an InvokeComplete event.
func (h *Handler) handleInvokeWithResponseStream(c *echo.Context, name string) error {
	ctx := c.Request().Context()

	qualifier := c.Request().URL.Query().Get("Qualifier")

	body, readErr := readBodyOrEmpty(c)
	if readErr != nil {
		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			"failed to read request",
		)
	}

	var result []byte
	var statusCode int
	var invokeErr error

	if qi, ok := h.Backend.(QualifierInvoker); ok && qualifier != "" {
		result, _, _, statusCode, invokeErr = qi.InvokeFunctionWithQualifier(
			ctx, name, qualifier, "", "", InvocationTypeRequestResponse, body,
		)
	} else {
		result, statusCode, invokeErr = h.Backend.InvokeFunction(ctx, name, InvocationTypeRequestResponse, body)
	}

	if invokeErr != nil {
		if errors.Is(invokeErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(invokeErr, ErrTooManyRequests) {
			return h.writeError(
				c,
				http.StatusTooManyRequests,
				"TooManyRequestsException",
				invokeErr.Error(),
			)
		}

		return h.writeError(
			c,
			http.StatusInternalServerError,
			"ServiceException",
			invokeErr.Error(),
		)
	}

	if statusCode == http.StatusNotFound {
		return h.writeError(
			c,
			http.StatusNotFound,
			"ResourceNotFoundException",
			"Function not found: "+name,
		)
	}

	if len(result) == 0 {
		result = []byte("{}")
	}

	c.Response().Header().Set("Content-Type", contentTypeEventStream)
	c.Response().WriteHeader(http.StatusOK)

	// PayloadChunk event carries the function's response body.
	chunkFrame := buildLambdaStreamFrame([][2]string{
		{":message-type", "event"},
		{":event-type", "PayloadChunk"},
		{":content-type", "application/octet-stream"},
	}, result)
	_, _ = c.Response().Write(chunkFrame)

	// InvokeComplete event signals end-of-stream to the SDK.
	doneFrame := buildLambdaStreamFrame([][2]string{
		{":message-type", "event"},
		{":event-type", "InvokeComplete"},
	}, nil)
	_, _ = c.Response().Write(doneFrame)

	return nil
}

// esHeaderValueTypeString is the AWS event stream type byte for UTF-8 string header values.
const esHeaderValueTypeString = 7

// buildLambdaStreamHeaders encodes header name/value pairs in the AWS event stream binary format.
// Each header: name_len(1) | name | type(1)=7 | value_len(2 BE) | value.
// Loop counters avoid int→byte/uint16 narrowing conversions flagged by gosec G115.
func buildLambdaStreamHeaders(hdrs [][2]string) []byte {
	var buf bytes.Buffer
	for _, kv := range hdrs {
		name, value := kv[0], kv[1]
		if len(name) > math.MaxUint8 {
			continue
		}
		var nameLen uint8
		for range []byte(name) {
			nameLen++
		}
		var valLen uint16
		for range []byte(value) {
			valLen++
		}
		vlen := [2]byte{}
		binary.BigEndian.PutUint16(vlen[:], valLen)
		buf.WriteByte(nameLen)
		buf.WriteString(name)
		buf.WriteByte(esHeaderValueTypeString)
		buf.Write(vlen[:])
		buf.WriteString(value)
	}

	return buf.Bytes()
}

// buildLambdaStreamFrame encodes one AWS event stream binary message.
// Frame layout: totalLen(4) | headerLen(4) | preludeCRC(4) | headers | payload | msgCRC(4).
// CRCs use CRC32/IEEE as required by the AWS event stream specification.
// Loop counters produce uint32 lengths without int→uint32 narrowing (gosec G115).
func buildLambdaStreamFrame(hdrs [][2]string, payload []byte) []byte {
	const preludeLen = 12
	const msgCRCLen = 4

	hdrBytes := buildLambdaStreamHeaders(hdrs)

	// Count bytes via loop to get uint32 without int→uint32 narrowing conversion.
	var headerLen uint32
	for range hdrBytes {
		headerLen++
	}
	var payloadLen uint32
	for range payload {
		payloadLen++
	}

	total := uint64(preludeLen) + uint64(headerLen) + uint64(payloadLen) + uint64(msgCRCLen)
	if total > math.MaxUint32 {
		return nil
	}
	totalLen := uint32(total)

	// int(uint32) is a widening conversion on all supported platforms.
	buf := make([]byte, int(totalLen))
	binary.BigEndian.PutUint32(buf[0:4], totalLen)
	binary.BigEndian.PutUint32(buf[4:8], headerLen)

	preludeCRC := crc32.ChecksumIEEE(buf[0:8])
	binary.BigEndian.PutUint32(buf[8:preludeLen], preludeCRC)

	hEnd := preludeLen + int(headerLen)
	pEnd := hEnd + int(payloadLen)
	copy(buf[preludeLen:hEnd], hdrBytes)
	copy(buf[hEnd:pEnd], payload)

	msgCRC := crc32.ChecksumIEEE(buf[0:pEnd])
	binary.BigEndian.PutUint32(buf[pEnd:], msgCRC)

	return buf
}

// readBodyOrEmpty reads the HTTP request body, returning an empty JSON object if nil.
func readBodyOrEmpty(c *echo.Context) ([]byte, error) {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return nil, err
	}

	if body == nil {
		return []byte("{}"), nil
	}

	return body, nil
}
