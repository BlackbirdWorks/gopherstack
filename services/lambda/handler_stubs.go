package lambda

// handler_stubs.go adds stub handlers for SDK operations that are acknowledged
// but not yet fully implemented.  Each stub returns a minimal valid response so
// that the operation is visible in GetSupportedOperations and the SDK
// completeness test passes.

import (
	"encoding/binary"
	"errors"
	"net/http"
	"net/url"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// contentTypeEventStream is the MIME type for Lambda streaming responses.
const contentTypeEventStream = "application/vnd.amazon.eventstream"

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
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex := store.get(arn)

	if ex == nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found: "+arn)
	}

	return c.JSON(http.StatusOK, ex)
}

// handleGetDurableExecutionHistory returns the event history for the given execution.
func (h *Handler) handleGetDurableExecutionHistory(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
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
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex := store.get(arn)

	if ex == nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found: "+arn)
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
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
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
	_, _ = store.sendCallback(arn, "CallbackFailure")

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
	_, _ = store.sendCallback(arn, "CallbackHeartbeat")

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
	_, _ = store.sendCallback(arn, "CallbackSuccess")

	return c.JSON(http.StatusOK, map[string]any{})
}

// handleStopDurableExecution marks the execution as stopped.
func (h *Handler) handleStopDurableExecution(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return c.JSON(http.StatusOK, map[string]any{"Status": string(DurableExecutionStatusStopped)})
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	ex, err := store.stop(arn)

	if err != nil {
		// If not found, stop is a no-op — return a synthetic stopped response.
		return c.JSON(http.StatusOK, map[string]any{"Status": string(DurableExecutionStatusStopped)})
	}

	return c.JSON(http.StatusOK, ex)
}

// --- ListFunctionVersionsByCapacityProvider stub ---

type listFunctionVersionsByCapacityProviderOutput struct {
	NextMarker       string `json:"NextMarker,omitempty"`
	FunctionVersions []any  `json:"FunctionVersions"`
}

// handleListFunctionVersionsByCapacityProvider returns an empty list stub.
func (h *Handler) handleListFunctionVersionsByCapacityProvider(c *echo.Context) error {
	return c.JSON(http.StatusOK, &listFunctionVersionsByCapacityProviderOutput{
		FunctionVersions: []any{},
	})
}

// --- Invoke / InvokeAsync / InvokeWithResponseStream stubs ---
// The SDK exposes these as separate methods; gopherstack routes them through
// handleInvoke (InvokeFunction).  These stubs register the SDK operation names
// without duplicating routing logic.

// handleInvokeAsync handles POST /2014-11-13/functions/{name}/invoke-async/.
func (h *Handler) handleInvokeAsync(c *echo.Context, name string) error {
	if _, ok := h.Backend.(*InMemoryBackend); !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	// Validate the function exists by delegating to the standard invoke path.
	return h.handleInvoke(c, name)
}

// handleInvokeWithResponseStream handles POST /2021-11-15/functions/{name}/response-streaming-invocations.
// It delegates to the standard synchronous invocation path and streams the result as an
// application/vnd.amazon.eventstream response with 4-byte big-endian length-prefixed frames.
func (h *Handler) handleInvokeWithResponseStream(c *echo.Context, name string) error {
	ctx := c.Request().Context()

	qualifier := c.Request().URL.Query().Get("Qualifier")

	body, readErr := readBodyOrEmpty(c)
	if readErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "failed to read request")
	}

	var result []byte
	var statusCode int
	var invokeErr error

	if qi, ok := h.Backend.(QualifierInvoker); ok && qualifier != "" {
		result, statusCode, invokeErr = qi.InvokeFunctionWithQualifier(
			ctx, name, qualifier, InvocationTypeRequestResponse, body,
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
			return h.writeError(c, http.StatusTooManyRequests, "TooManyRequestsException", invokeErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", invokeErr.Error())
	}

	if statusCode == http.StatusNotFound {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "Function not found: "+name)
	}

	if len(result) == 0 {
		result = []byte("{}")
	}

	c.Response().Header().Set("Content-Type", contentTypeEventStream)
	c.Response().WriteHeader(http.StatusOK)

	writeEventStreamFrame(c.Response(), result)
	writeEventStreamFrame(c.Response(), nil) // end-of-stream

	return nil
}

// writeEventStreamFrame writes a single eventstream frame: 4-byte big-endian length + payload.
// A nil/empty payload writes a zero-length end-of-stream marker.
func writeEventStreamFrame(w interface{ Write([]byte) (int, error) }, payload []byte) {
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(len(payload))) //nolint:gosec // bounded by invocation payload
	_, _ = w.Write(lenBuf[:])

	if len(payload) > 0 {
		_, _ = w.Write(payload)
	}
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
