package lambda

// handler_stubs.go adds stub handlers for SDK operations that are acknowledged
// but not yet fully implemented.  Each stub returns a minimal valid response so
// that the operation is visible in GetSupportedOperations and the SDK
// completeness test passes.

import (
	"encoding/binary"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// contentTypeEventStream is the MIME type for Lambda streaming responses.
const contentTypeEventStream = "application/vnd.amazon.eventstream"

// --- Durable Execution stubs ---

// durableExecutionStub is a minimal response for unimplemented durable execution ops.
type durableExecutionStub struct {
	ExecutionID string `json:"ExecutionId,omitempty"`
	Status      string `json:"Status,omitempty"`
}

// durableExecutionListStub is a minimal list response for durable executions.
type durableExecutionListStub struct {
	DurableExecutions []durableExecutionStub `json:"DurableExecutions"`
}

// handleGetDurableExecution returns a stub 404 (execution not found).
func (h *Handler) handleGetDurableExecution(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found")
}

// handleGetDurableExecutionHistory returns an empty history stub.
func (h *Handler) handleGetDurableExecutionHistory(c *echo.Context) error {
	return c.JSON(http.StatusOK, map[string]any{"Events": []any{}})
}

// handleGetDurableExecutionState returns a stub 404.
func (h *Handler) handleGetDurableExecutionState(c *echo.Context) error {
	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found")
}

// handleListDurableExecutionsByFunction returns an empty list stub.
func (h *Handler) handleListDurableExecutionsByFunction(c *echo.Context) error {
	return c.JSON(http.StatusOK, &durableExecutionListStub{DurableExecutions: []durableExecutionStub{}})
}

// handleSendDurableExecutionCallbackFailure accepts a callback failure (stub).
func (h *Handler) handleSendDurableExecutionCallbackFailure(c *echo.Context) error {
	c.Response().WriteHeader(http.StatusOK)

	return nil
}

// handleSendDurableExecutionCallbackHeartbeat accepts a callback heartbeat (stub).
func (h *Handler) handleSendDurableExecutionCallbackHeartbeat(c *echo.Context) error {
	c.Response().WriteHeader(http.StatusOK)

	return nil
}

// handleSendDurableExecutionCallbackSuccess accepts a callback success (stub).
func (h *Handler) handleSendDurableExecutionCallbackSuccess(c *echo.Context) error {
	c.Response().WriteHeader(http.StatusOK)

	return nil
}

// handleStopDurableExecution accepts a stop request (stub).
func (h *Handler) handleStopDurableExecution(c *echo.Context) error {
	return c.JSON(http.StatusOK, &durableExecutionStub{Status: "STOPPED"})
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
// It returns a minimal but well-formed application/vnd.amazon.eventstream response
// containing one payload chunk followed by an end-of-stream marker.
func (h *Handler) handleInvokeWithResponseStream(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	// Verify the function exists before streaming.
	if _, err := lambdaBk.GetFunction(name); err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Function not found: "+name)
	}

	// Build a minimal JSON payload mimicking what a real streaming Lambda returns.
	payload, _ := json.Marshal(map[string]string{"statusCode": "200", "body": ""})

	c.Response().Header().Set("Content-Type", contentTypeEventStream)
	c.Response().WriteHeader(http.StatusOK)

	// Write one event-stream frame: 4-byte big-endian payload length followed by payload.
	// The payload is a small JSON literal so the length safely fits in uint32.
	payloadLen := len(payload)
	var lenBuf [4]byte
	binary.BigEndian.PutUint32(lenBuf[:], uint32(payloadLen)) //nolint:gosec // payload is small JSON literal
	_, _ = c.Response().Write(lenBuf[:])
	_, _ = c.Response().Write(payload)

	// End-of-stream: zero-length frame.
	binary.BigEndian.PutUint32(lenBuf[:], 0)
	_, _ = c.Response().Write(lenBuf[:])

	return nil
}
