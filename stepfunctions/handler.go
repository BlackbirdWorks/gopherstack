package stepfunctions

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var errUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP service handler for Step Functions operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
	tags    map[string]map[string]string
	tagsMu  sync.RWMutex
}

// NewHandler creates a new Step Functions handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log, tags: make(map[string]map[string]string)}
}

func (h *Handler) setTags(resourceID string, kv map[string]string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	if h.tags[resourceID] == nil {
		h.tags[resourceID] = make(map[string]string)
	}
	maps.Copy(h.tags[resourceID], kv)
}

func (h *Handler) removeTags(resourceID string, keys []string) {
	h.tagsMu.Lock()
	defer h.tagsMu.Unlock()
	for _, k := range keys {
		delete(h.tags[resourceID], k)
	}
}

func (h *Handler) getTags(resourceID string) map[string]string {
	h.tagsMu.RLock()
	defer h.tagsMu.RUnlock()
	result := make(map[string]string)
	maps.Copy(result, h.tags[resourceID])

	return result
}

// Name returns the service name.
func (h *Handler) Name() string { return "StepFunctions" }

// GetSupportedOperations returns all mocked Step Functions operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateStateMachine",
		"DeleteStateMachine",
		"ListStateMachines",
		"DescribeStateMachine",
		"UpdateStateMachine",
		"StartExecution",
		"StopExecution",
		"DescribeExecution",
		"ListExecutions",
		"GetExecutionHistory",
		"ListTagsForResource",
		"TagResource",
		"UntagResource",
	}
}

// RouteMatcher returns a matcher for Step Functions requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonStates.") || strings.HasPrefix(target, "AWSStepFunctions.")
	}
}

const stepFunctionsMatchPriority = 100

// MatchPriority returns the routing priority for the Step Functions handler.
func (h *Handler) MatchPriority() int { return stepFunctionsMatchPriority }

// ExtractOperation extracts the operation name from the X-Amz-Target header.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource extracts the resource name from the request body.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	for _, key := range []string{"name", "stateMachineArn", "executionArn"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for Step Functions requests.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		ctx := c.Request().Context()
		log := logger.Load(ctx)

		if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
			return c.JSON(http.StatusOK, h.GetSupportedOperations())
		}

		if c.Request().Method != http.MethodPost {
			return c.String(http.StatusMethodNotAllowed, "Method not allowed")
		}

		target := c.Request().Header.Get("X-Amz-Target")
		if target == "" {
			return c.String(http.StatusBadRequest, "Missing X-Amz-Target")
		}

		parts := strings.Split(target, ".")
		const targetParts = 2
		if len(parts) != targetParts {
			return c.String(http.StatusBadRequest, "Invalid X-Amz-Target")
		}
		action := parts[1]

		body, err := httputil.ReadBody(c.Request())
		if err != nil {
			log.ErrorContext(ctx, "failed to read request body", "error", err)

			return c.String(http.StatusInternalServerError, "internal server error")
		}

		log.DebugContext(ctx, "StepFunctions request", "action", action)

		response, reqErr := h.dispatch(ctx, action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

		return c.JSONBlob(http.StatusOK, response)
	}
}

type actionFn func([]byte) (any, error)

func (h *Handler) stateMachineActions() map[string]actionFn {
	return map[string]actionFn{
		"CreateStateMachine": func(b []byte) (any, error) {
			var input struct {
				Name       string `json:"name"`
				Definition string `json:"definition"`
				RoleArn    string `json:"roleArn"`
				Type       string `json:"type"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			sm, err := h.Backend.CreateStateMachine(input.Name, input.Definition, input.RoleArn, input.Type)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"stateMachineArn": sm.StateMachineArn,
				"creationDate":    sm.CreationDate,
			}, nil
		},
		"DeleteStateMachine": func(b []byte) (any, error) {
			var input struct {
				StateMachineArn string `json:"stateMachineArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteStateMachine(input.StateMachineArn); err != nil {
				return nil, err
			}

			return map[string]any{}, nil
		},
		"ListStateMachines": func(b []byte) (any, error) {
			var input struct {
				NextToken  string `json:"nextToken"`
				MaxResults int    `json:"maxResults"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			sms, next, err := h.Backend.ListStateMachines(input.NextToken, input.MaxResults)
			if err != nil {
				return nil, err
			}

			return map[string]any{"stateMachines": sms, "nextToken": next}, nil
		},
		"DescribeStateMachine": func(b []byte) (any, error) {
			var input struct {
				StateMachineArn string `json:"stateMachineArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeStateMachine(input.StateMachineArn)
		},
		"UpdateStateMachine": func(_ []byte) (any, error) {
			return map[string]any{"updateDate": time.Now().UTC()}, nil
		},
		"ListTagsForResource": func(b []byte) (any, error) {
			var input struct {
				ResourceArn string `json:"resourceArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return map[string]any{"tags": h.getTags(input.ResourceArn)}, nil
		},
		"TagResource": func(b []byte) (any, error) {
			var input struct {
				Tags        map[string]string `json:"tags"`
				ResourceArn string            `json:"resourceArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.setTags(input.ResourceArn, input.Tags)

			return map[string]any{}, nil
		},
		"UntagResource": func(b []byte) (any, error) {
			var input struct {
				ResourceArn string   `json:"resourceArn"`
				TagKeys     []string `json:"tagKeys"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			h.removeTags(input.ResourceArn, input.TagKeys)

			return map[string]any{}, nil
		},
	}
}

func (h *Handler) executionActions() map[string]actionFn {
	return map[string]actionFn{
		"StartExecution": func(b []byte) (any, error) {
			var input struct {
				StateMachineArn string `json:"stateMachineArn"`
				Name            string `json:"name"`
				Input           string `json:"input"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			exec, err := h.Backend.StartExecution(input.StateMachineArn, input.Name, input.Input)
			if err != nil {
				return nil, err
			}

			return map[string]any{
				"executionArn": exec.ExecutionArn,
				"startDate":    exec.StartDate,
			}, nil
		},
		"StopExecution": func(b []byte) (any, error) {
			var input struct {
				ExecutionArn string `json:"executionArn"`
				Error        string `json:"error"`
				Cause        string `json:"cause"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.StopExecution(input.ExecutionArn, input.Error, input.Cause); err != nil {
				return nil, err
			}
			exec, err := h.Backend.DescribeExecution(input.ExecutionArn)
			if err != nil {
				return nil, err
			}

			return map[string]any{"stopDate": exec.StopDate}, nil
		},
		"DescribeExecution": func(b []byte) (any, error) {
			var input struct {
				ExecutionArn string `json:"executionArn"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeExecution(input.ExecutionArn)
		},
		"ListExecutions": func(b []byte) (any, error) {
			var input struct {
				StateMachineArn string `json:"stateMachineArn"`
				StatusFilter    string `json:"statusFilter"`
				NextToken       string `json:"nextToken"`
				MaxResults      int    `json:"maxResults"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			execs, next, err := h.Backend.ListExecutions(
				input.StateMachineArn, input.StatusFilter, input.NextToken, input.MaxResults,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{"executions": execs, "nextToken": next}, nil
		},
		"GetExecutionHistory": func(b []byte) (any, error) {
			var input struct {
				ExecutionArn string `json:"executionArn"`
				NextToken    string `json:"nextToken"`
				MaxResults   int    `json:"maxResults"`
				ReverseOrder bool   `json:"reverseOrder"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			events, next, err := h.Backend.GetExecutionHistory(
				input.ExecutionArn, input.NextToken, input.MaxResults, input.ReverseOrder,
			)
			if err != nil {
				return nil, err
			}

			return map[string]any{"events": events, "nextToken": next}, nil
		},
	}
}

func (h *Handler) dispatchTable() map[string]actionFn {
	table := make(map[string]actionFn)
	maps.Copy(table, h.stateMachineActions())
	maps.Copy(table, h.executionActions())
	maps.Copy(table, h.utilActions())

	return table
}

// utilActions returns stubs for utility operations like definition validation.
func (h *Handler) utilActions() map[string]actionFn {
	return map[string]actionFn{
		"ValidateStateMachineDefinition": func(_ []byte) (any, error) {
			return map[string]any{"result": "OK", "diagnostics": []any{}}, nil
		},
		"ListStateMachineVersions": func(_ []byte) (any, error) {
			return map[string]any{"stateMachineVersions": []any{}}, nil
		},
	}
}

// dispatch routes the action to the correct handler function.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.dispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", errUnknownOperation, action)
	}

	response, err := fn(body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a standardized JSON error response.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.0")

	errType, statusCode := classifyError(reqErr)

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "StepFunctions internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "StepFunctions request error", "error", reqErr, "action", action)
	}

	errResp := ErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}

func classifyError(reqErr error) (string, int) {
	switch {
	case errors.Is(reqErr, ErrStateMachineDoesNotExist):
		return "StateMachineDoesNotExist", http.StatusNotFound
	case errors.Is(reqErr, ErrExecutionDoesNotExist):
		return "ExecutionDoesNotExist", http.StatusNotFound
	case errors.Is(reqErr, ErrStateMachineAlreadyExists):
		return "StateMachineAlreadyExists", http.StatusConflict
	case errors.Is(reqErr, ErrExecutionAlreadyExists):
		return "ExecutionAlreadyExists", http.StatusConflict
	case errors.Is(reqErr, errUnknownOperation):
		return "UnknownOperationException", http.StatusBadRequest
	default:
		return "InternalServerError", http.StatusInternalServerError
	}
}
