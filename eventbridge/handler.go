package eventbridge

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

var errUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP service handler for EventBridge operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
}

// NewHandler creates a new EventBridge handler.
func NewHandler(backend StorageBackend, log *slog.Logger) *Handler {
	return &Handler{Backend: backend, Logger: log}
}

// Name returns the service name.
func (h *Handler) Name() string { return "EventBridge" }

// GetSupportedOperations returns all mocked EventBridge operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"CreateEventBus",
		"DeleteEventBus",
		"ListEventBuses",
		"DescribeEventBus",
		"PutRule",
		"DeleteRule",
		"ListRules",
		"DescribeRule",
		"EnableRule",
		"DisableRule",
		"PutTargets",
		"RemoveTargets",
		"ListTargetsByRule",
		"PutEvents",
	}
}

// RouteMatcher returns a matcher for EventBridge requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")
		return strings.HasPrefix(target, "AmazonEventBridge.")
	}
}

const eventBridgeMatchPriority = 100

// MatchPriority returns the routing priority for the EventBridge handler.
func (h *Handler) MatchPriority() int { return eventBridgeMatchPriority }

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
	if err := json.Unmarshal(body, &data); err != nil {
		return ""
	}

	for _, key := range []string{"Name", "Rule", "EventBusName"} {
		if v, ok := data[key].(string); ok && v != "" {
			return v
		}
	}

	return ""
}

// Handler returns the Echo handler function for EventBridge requests.
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

		log.DebugContext(ctx, "EventBridge request", "action", action)

		response, reqErr := h.dispatch(ctx, action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSONBlob(http.StatusOK, response)
	}
}

type actionFn func([]byte) (any, error)

func (h *Handler) dispatchTable() map[string]actionFn {
	return map[string]actionFn{
		"CreateEventBus": func(b []byte) (any, error) {
			var input struct {
				Name        string `json:"Name"`
				Description string `json:"Description"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.CreateEventBus(input.Name, input.Description)
			if err != nil {
				return nil, err
			}
			return map[string]string{"EventBusArn": bus.Arn}, nil
		},
		"DeleteEventBus": func(b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteEventBus(input.Name); err != nil {
				return nil, err
			}
			return map[string]any{}, nil
		},
		"ListEventBuses": func(b []byte) (any, error) {
			var input struct {
				NamePrefix string `json:"NamePrefix"`
				NextToken  string `json:"NextToken"`
				Limit      int    `json:"Limit"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			buses, next, err := h.Backend.ListEventBuses(input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}
			return map[string]any{"EventBuses": buses, "NextToken": next}, nil
		},
		"DescribeEventBus": func(b []byte) (any, error) {
			var input struct {
				Name string `json:"Name"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			bus, err := h.Backend.DescribeEventBus(input.Name)
			if err != nil {
				return nil, err
			}
			return bus, nil
		},
		"PutRule": func(b []byte) (any, error) {
			var input PutRuleInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rule, err := h.Backend.PutRule(input)
			if err != nil {
				return nil, err
			}
			return map[string]string{"RuleArn": rule.Arn}, nil
		},
		"DeleteRule": func(b []byte) (any, error) {
			var input struct {
				Name         string `json:"Name"`
				EventBusName string `json:"EventBusName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DeleteRule(input.Name, input.EventBusName); err != nil {
				return nil, err
			}
			return map[string]any{}, nil
		},
		"ListRules": func(b []byte) (any, error) {
			var input struct {
				EventBusName string `json:"EventBusName"`
				NamePrefix   string `json:"NamePrefix"`
				NextToken    string `json:"NextToken"`
				Limit        int    `json:"Limit"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			rules, next, err := h.Backend.ListRules(input.EventBusName, input.NamePrefix, input.NextToken)
			if err != nil {
				return nil, err
			}
			return map[string]any{"Rules": rules, "NextToken": next}, nil
		},
		"DescribeRule": func(b []byte) (any, error) {
			var input struct {
				Name         string `json:"Name"`
				EventBusName string `json:"EventBusName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			return h.Backend.DescribeRule(input.Name, input.EventBusName)
		},
		"EnableRule": func(b []byte) (any, error) {
			var input struct {
				Name         string `json:"Name"`
				EventBusName string `json:"EventBusName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.EnableRule(input.Name, input.EventBusName); err != nil {
				return nil, err
			}
			return map[string]any{}, nil
		},
		"DisableRule": func(b []byte) (any, error) {
			var input struct {
				Name         string `json:"Name"`
				EventBusName string `json:"EventBusName"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			if err := h.Backend.DisableRule(input.Name, input.EventBusName); err != nil {
				return nil, err
			}
			return map[string]any{}, nil
		},
		"PutTargets": func(b []byte) (any, error) {
			var input struct {
				Rule         string   `json:"Rule"`
				EventBusName string   `json:"EventBusName"`
				Targets      []Target `json:"Targets"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.PutTargets(input.Rule, input.EventBusName, input.Targets)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}
			return map[string]any{
				"FailedEntryCount": len(failed),
				"FailedEntries":    failed,
			}, nil
		},
		"RemoveTargets": func(b []byte) (any, error) {
			var input struct {
				Rule         string   `json:"Rule"`
				EventBusName string   `json:"EventBusName"`
				Ids          []string `json:"Ids"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			failed, err := h.Backend.RemoveTargets(input.Rule, input.EventBusName, input.Ids)
			if err != nil {
				return nil, err
			}
			if failed == nil {
				failed = []FailedEntry{}
			}
			return map[string]any{
				"FailedEntryCount": len(failed),
				"FailedEntries":    failed,
			}, nil
		},
		"ListTargetsByRule": func(b []byte) (any, error) {
			var input struct {
				Rule         string `json:"Rule"`
				EventBusName string `json:"EventBusName"`
				NextToken    string `json:"NextToken"`
				Limit        int    `json:"Limit"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			targets, next, err := h.Backend.ListTargetsByRule(input.Rule, input.EventBusName, input.NextToken)
			if err != nil {
				return nil, err
			}
			return map[string]any{"Targets": targets, "NextToken": next}, nil
		},
		"PutEvents": func(b []byte) (any, error) {
			var input struct {
				Entries []EventEntry `json:"Entries"`
			}
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}
			entries := h.Backend.PutEvents(input.Entries)
			return map[string]any{
				"FailedEntryCount": 0,
				"Entries":          entries,
			}, nil
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
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errType string
	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrEventBusNotFound), errors.Is(reqErr, ErrRuleNotFound):
		errType = "ResourceNotFoundException"
		statusCode = http.StatusNotFound
	case errors.Is(reqErr, ErrEventBusAlreadyExists):
		errType = "ResourceAlreadyExistsException"
		statusCode = http.StatusConflict
	case errors.Is(reqErr, ErrCannotDeleteDefaultBus):
		errType = "IllegalArgumentException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, ErrInvalidParameter):
		errType = "InvalidParameterException"
		statusCode = http.StatusBadRequest
	case errors.Is(reqErr, errUnknownOperation):
		errType = "UnknownOperationException"
		statusCode = http.StatusBadRequest
	default:
		errType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if statusCode == http.StatusInternalServerError {
		log.ErrorContext(ctx, "EventBridge internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "EventBridge request error", "error", reqErr, "action", action)
	}

	errResp := ErrorResponse{
		Type:    errType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}
