package lambda

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// --- Durable execution handler (stub) ---

// handleDurableExecRoute handles routes under /2025-12-01/durable-executions/...
func (h *Handler) handleDurableExecRoute(c *echo.Context, path, method string) error {
	// CheckpointDurableExecution: POST /2025-12-01/durable-executions/{arn}/checkpoint
	if method == http.MethodPost && strings.HasSuffix(path, "/checkpoint") {
		return h.handleCheckpointDurableExecution(c, path)
	}

	// StopDurableExecution: DELETE /2025-12-01/durable-executions/{arn}
	if method == http.MethodDelete {
		return h.handleStopDurableExecution(c)
	}

	// GET routes.
	if method == http.MethodGet {
		switch {
		case path == lambdaDurableExecPathPrefix || path == lambdaDurableExecPathPrefix+"/":
			return h.handleListDurableExecutionsByFunction(c)
		case strings.HasSuffix(path, "/history"):
			return h.handleGetDurableExecutionHistory(c)
		case strings.HasSuffix(path, "/state"):
			return h.handleGetDurableExecutionState(c)
		default:
			return h.handleGetDurableExecution(c)
		}
	}

	// POST sub-routes (callbacks).
	if method == http.MethodPost {
		switch {
		case strings.HasSuffix(path, "/callback/failure"):
			return h.handleSendDurableExecutionCallbackFailure(c)
		case strings.HasSuffix(path, "/callback/heartbeat"):
			return h.handleSendDurableExecutionCallbackHeartbeat(c)
		case strings.HasSuffix(path, "/callback/success"):
			return h.handleSendDurableExecutionCallbackSuccess(c)
		}
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleCheckpointDurableExecution handles POST /2025-12-01/durable-executions/{arn}/checkpoint.
func (h *Handler) handleCheckpointDurableExecution(c *echo.Context, path string) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return c.JSON(http.StatusOK, &CheckpointDurableExecutionOutput{})
	}

	executionARN := extractDurableExecARN(path)

	var data map[string]any
	if body, err := httputils.ReadBody(c.Request()); err == nil && len(body) > 0 {
		_ = json.Unmarshal(body, &data)
	}

	_ = store.checkpoint(executionARN, data)

	return c.JSON(http.StatusOK, &CheckpointDurableExecutionOutput{})
}

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
