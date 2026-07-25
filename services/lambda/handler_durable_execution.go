package lambda

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/page"
	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/labstack/echo/v5"
)

// isDurableExecPath reports whether path belongs to any of the three
// independent durable-execution path prefixes (see handler_paths.go) — a
// single combined predicate keeps dispatchSpecialRoutes' switch to one case
// for the whole family instead of three.
func isDurableExecPath(path string) bool {
	return strings.HasPrefix(path, lambdaDurableExecCallbacksPathPrefix) ||
		isDurableExecByFunctionPath(path) ||
		strings.HasPrefix(path, lambdaDurableExecPathPrefix)
}

// dispatchDurableExecRoutes routes a durable-execution-family request (see
// isDurableExecPath) to its handler.
func (h *Handler) dispatchDurableExecRoutes(c *echo.Context, path, method string) error {
	switch {
	case strings.HasPrefix(path, lambdaDurableExecCallbacksPathPrefix):
		return h.handleDurableExecCallbackRoute(c, path, method)
	case isDurableExecByFunctionPath(path):
		return h.handleListDurableExecutionsByFunction(c, extractFunctionNameFromDurableExecPath(path))
	default:
		return h.handleDurableExecRoute(c, path, method)
	}
}

// handleDurableExecRoute handles routes under /2025-12-01/durable-executions/{DurableExecutionArn}/...
// (GetDurableExecution, GetDurableExecutionHistory, GetDurableExecutionState,
// CheckpointDurableExecution, StopDurableExecution). ListDurableExecutionsByFunction
// and SendDurableExecutionCallback{Success,Failure,Heartbeat} live under
// different path prefixes entirely — see handler_paths.go.
func (h *Handler) handleDurableExecRoute(c *echo.Context, path, method string) error {
	switch {
	case method == http.MethodPost && strings.HasSuffix(path, "/checkpoint"):
		return h.handleCheckpointDurableExecution(c, path)
	case method == http.MethodPost && strings.HasSuffix(path, "/stop"):
		return h.handleStopDurableExecution(c)
	case method == http.MethodGet && strings.HasSuffix(path, "/history"):
		return h.handleGetDurableExecutionHistory(c)
	case method == http.MethodGet && strings.HasSuffix(path, "/state"):
		return h.handleGetDurableExecutionState(c)
	case method == http.MethodGet && !isDurableExecRootPath(path):
		return h.handleGetDurableExecution(c)
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleDurableExecCallbackRoute handles routes under
// /2025-12-01/durable-execution-callbacks/{CallbackId}/... — verified
// against api_op_SendDurableExecutionCallback{Success,Failure,Heartbeat}.go:
// a distinct resource (CallbackId, not DurableExecutionArn) with suffixes
// "succeed"/"fail"/"heartbeat" (NOT "success"/"failure").
func (h *Handler) handleDurableExecCallbackRoute(c *echo.Context, path, method string) error {
	if method == http.MethodPost {
		switch {
		case strings.HasSuffix(path, "/succeed"):
			return h.handleSendDurableExecutionCallbackSuccess(c)
		case strings.HasSuffix(path, "/fail"):
			return h.handleSendDurableExecutionCallbackFailure(c)
		case strings.HasSuffix(path, "/heartbeat"):
			return h.handleSendDurableExecutionCallbackHeartbeat(c)
		}
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// extractDurableExecPathID extracts and URL-decodes the resource identifier
// segment immediately following prefix in path, stopping at the next "/".
func extractDurableExecPathID(path, prefix string) string {
	rest := strings.TrimPrefix(path, prefix+"/")
	if idx := strings.Index(rest, "/"); idx >= 0 {
		rest = rest[:idx]
	}

	decoded, err := url.PathUnescape(rest)
	if err != nil {
		return rest
	}

	return decoded
}

// extractDurableExecARN extracts the DurableExecutionArn from a
// /2025-12-01/durable-executions/{encodedARN}[/...] path.
func extractDurableExecARN(path string) string {
	return extractDurableExecPathID(path, lambdaDurableExecPathPrefix)
}

// extractDurableExecCallbackID extracts the CallbackId from a
// /2025-12-01/durable-execution-callbacks/{encodedID}/... path.
func extractDurableExecCallbackID(path string) string {
	return extractDurableExecPathID(path, lambdaDurableExecCallbacksPathPrefix)
}

// extractDurableExecOperation identifies the operation name for
// durable-execution-family requests (three independent path prefixes — see
// handler_paths.go) so chaos fault injection (ChaosOperations) can match
// them by name; returns "" for any non-matching path.
func extractDurableExecOperation(path, method string) string {
	switch {
	case strings.HasPrefix(path, lambdaDurableExecCallbacksPathPrefix):
		return extractDurableExecCallbackOperation(path, method)
	case isDurableExecByFunctionPath(path):
		return "ListDurableExecutionsByFunction"
	case strings.HasPrefix(path, lambdaDurableExecPathPrefix):
		return extractDurableExecCoreOperation(path, method)
	}

	return ""
}

func extractDurableExecCallbackOperation(path, method string) string {
	if method != http.MethodPost {
		return ""
	}

	switch {
	case strings.HasSuffix(path, "/succeed"):
		return "SendDurableExecutionCallbackSuccess"
	case strings.HasSuffix(path, "/fail"):
		return "SendDurableExecutionCallbackFailure"
	case strings.HasSuffix(path, "/heartbeat"):
		return "SendDurableExecutionCallbackHeartbeat"
	}

	return ""
}

func extractDurableExecCoreOperation(path, method string) string {
	switch {
	case method == http.MethodPost && strings.HasSuffix(path, "/checkpoint"):
		return "CheckpointDurableExecution"
	case method == http.MethodPost && strings.HasSuffix(path, "/stop"):
		return "StopDurableExecution"
	case method == http.MethodGet && strings.HasSuffix(path, "/history"):
		return "GetDurableExecutionHistory"
	case method == http.MethodGet && strings.HasSuffix(path, "/state"):
		return "GetDurableExecutionState"
	case method == http.MethodGet && !isDurableExecRootPath(path):
		return "GetDurableExecution"
	}

	return ""
}

// durableExecFromBackend returns the durableExecutionStore from the backend, or nil.
func durableExecFromBackend(h *Handler) *durableExecutionStore {
	bk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return nil
	}

	return bk.durableExecs
}

// handleCheckpointDurableExecution handles POST /2025-12-01/durable-executions/{arn}/checkpoint.
func (h *Handler) handleCheckpointDurableExecution(c *echo.Context, path string) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(path)

	var body CheckpointDurableExecutionInput
	if raw, err := httputils.ReadBody(c.Request()); err == nil && len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}

	return c.JSON(http.StatusOK, store.checkpoint(arn, body.Updates))
}

// handleGetDurableExecution handles GET /2025-12-01/durable-executions/{arn}.
func (h *Handler) handleGetDurableExecution(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	includeData := c.Request().URL.Query().Get("IncludeExecutionData") != "false"

	out, ok := store.getOutput(arn, includeData)
	if !ok {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found: "+arn)
	}

	return c.JSON(http.StatusOK, out)
}

// handleGetDurableExecutionHistory handles GET /2025-12-01/durable-executions/{arn}/history.
func (h *Handler) handleGetDurableExecutionHistory(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	q := c.Request().URL.Query()
	includeData := q.Get("IncludeExecutionData") != "false"
	reverseOrder := q.Get("ReverseOrder") == "true"
	marker, maxItems := parsePaginationParams(c.Request())

	out, ok := store.historyOutput(arn, includeData, reverseOrder, marker, maxItems)
	if !ok {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found: "+arn)
	}

	return c.JSON(http.StatusOK, out)
}

// handleGetDurableExecutionState handles GET /2025-12-01/durable-executions/{arn}/state.
func (h *Handler) handleGetDurableExecutionState(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(c.Request().URL.Path)
	marker, maxItems := parsePaginationParams(c.Request())

	out, ok := store.stateOutput(arn, marker, maxItems)
	if !ok {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found: "+arn)
	}

	return c.JSON(http.StatusOK, out)
}

// handleListDurableExecutionsByFunction handles
// GET /2025-12-01/functions/{FunctionName}/durable-executions.
func (h *Handler) handleListDurableExecutionsByFunction(c *echo.Context, functionName string) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	q := c.Request().URL.Query()
	marker, maxItems := parsePaginationParams(c.Request())

	var statuses []DurableExecutionStatus
	for _, st := range q["Statuses"] {
		statuses = append(statuses, DurableExecutionStatus(st))
	}

	var startedAfter, startedBefore time.Time
	if v := q.Get("StartedAfter"); v != "" {
		startedAfter, _ = time.Parse(time.RFC3339, v)
	}

	if v := q.Get("StartedBefore"); v != "" {
		startedBefore, _ = time.Parse(time.RFC3339, v)
	}

	functionARN := buildARN(h.DefaultRegion, h.AccountID, functionName)
	summaries := store.listSummaries(
		functionARN, q.Get("DurableExecutionName"), statuses,
		startedAfter, startedBefore, q.Get("ReverseOrder") == "true",
	)

	p := page.New(summaries, marker, maxItems, lambdaDefaultMaxItems)

	return c.JSON(http.StatusOK, &ListDurableExecutionsByFunctionOutput{
		DurableExecutions: p.Data,
		NextMarker:        ptrconv.NilIfEmpty(p.Next),
	})
}

// handleStopDurableExecution handles POST /2025-12-01/durable-executions/{arn}/stop.
func (h *Handler) handleStopDurableExecution(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := extractDurableExecARN(c.Request().URL.Path)

	var body struct {
		Error *ErrorObject `json:"Error,omitempty"`
	}

	if raw, err := httputils.ReadBody(c.Request()); err == nil && len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}

	out, err := store.stopOutput(arn, body.Error)
	if err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "durable execution not found: "+arn)
	}

	return c.JSON(http.StatusOK, out)
}

// handleSendDurableExecutionCallbackSuccess handles
// POST /2025-12-01/durable-execution-callbacks/{CallbackId}/succeed.
func (h *Handler) handleSendDurableExecutionCallbackSuccess(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	callbackID := extractDurableExecCallbackID(c.Request().URL.Path)

	result, _ := httputils.ReadBody(c.Request())
	if err := store.sendCallback(callbackID, operationActionSucceed, nil, result); err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "callback not found: "+callbackID)
	}

	return c.JSON(http.StatusOK, &SendDurableExecutionCallbackSuccessOutput{})
}

// handleSendDurableExecutionCallbackFailure handles
// POST /2025-12-01/durable-execution-callbacks/{CallbackId}/fail.
func (h *Handler) handleSendDurableExecutionCallbackFailure(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	callbackID := extractDurableExecCallbackID(c.Request().URL.Path)

	var body struct {
		Error *ErrorObject `json:"Error,omitempty"`
	}

	if raw, err := httputils.ReadBody(c.Request()); err == nil && len(raw) > 0 {
		_ = json.Unmarshal(raw, &body)
	}

	if err := store.sendCallback(callbackID, operationActionFail, body.Error, nil); err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "callback not found: "+callbackID)
	}

	return c.JSON(http.StatusOK, &SendDurableExecutionCallbackFailureOutput{})
}

// handleSendDurableExecutionCallbackHeartbeat handles
// POST /2025-12-01/durable-execution-callbacks/{CallbackId}/heartbeat.
func (h *Handler) handleSendDurableExecutionCallbackHeartbeat(c *echo.Context) error {
	store := durableExecFromBackend(h)
	if store == nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	callbackID := extractDurableExecCallbackID(c.Request().URL.Path)

	if err := store.sendCallback(callbackID, "HEARTBEAT", nil, nil); err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "callback not found: "+callbackID)
	}

	return c.JSON(http.StatusOK, &SendDurableExecutionCallbackHeartbeatOutput{})
}
