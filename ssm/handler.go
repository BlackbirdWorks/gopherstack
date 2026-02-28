package ssm

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

var ErrUnknownOperation = errors.New("UnknownOperationException")

// Handler is the Echo HTTP service handler for SSM operations.
type Handler struct {
	Backend StorageBackend
	Logger  *slog.Logger
}

// NewHandler creates a new SSM handler with the given storage backend.
func NewHandler(backend StorageBackend, logger *slog.Logger) *Handler {
	return &Handler{
		Backend: backend,
		Logger:  logger,
	}
}

// Name returns the service name.
func (h *Handler) Name() string {
	return "SSM"
}

// GetSupportedOperations returns the list of mocked SSM operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{
		"GetParameter",
		"GetParameters",
		"GetParameterHistory",
		"GetParametersByPath",
		"DescribeParameters",
		"PutParameter",
		"DeleteParameter",
		"DeleteParameters",
		"AddTagsToResource",
		"RemoveTagsFromResource",
		"ListTagsForResource",
	}
}

// RouteMatcher returns a function that matches incoming requests for SSM.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		target := c.Request().Header.Get("X-Amz-Target")

		return strings.HasPrefix(target, "AmazonSSM")
	}
}

// MatchPriority returns the routing priority for the SSM handler.
func (h *Handler) MatchPriority() int {
	return service.PriorityHeaderExact // Same header-based priority as DynamoDB
}

// ExtractOperation attempts to extract the specific SSM operation from the request.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	target := c.Request().Header.Get("X-Amz-Target")
	parts := strings.Split(target, ".")
	const targetParts = 2
	if len(parts) == targetParts {
		return parts[1]
	}

	return "Unknown"
}

// ExtractResource attempts to extract the specific SSM resource from the request.
func (h *Handler) ExtractResource(c *echo.Context) string {
	body, err := httputil.ReadBody(c.Request())
	if err != nil {
		return ""
	}

	var data map[string]any
	if uerr := json.Unmarshal(body, &data); uerr != nil {
		return ""
	}

	if name, exists := data["Name"]; exists {
		if nameStr, ok := name.(string); ok {
			return nameStr
		}
	}

	return ""
}

// Handler is the Echo HTTP handler for SSM operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		return service.HandleTarget(
			c, logger.Load(c.Request().Context()),
			"SSM", "application/x-amz-json-1.1",
			h.GetSupportedOperations(),
			h.dispatch,
			h.handleError,
		)
	}
}

type ssmActionFn func([]byte) (any, error)

func (h *Handler) ssmDispatchTable() map[string]ssmActionFn { //nolint:gocognit
	return map[string]ssmActionFn{
		"PutParameter": func(b []byte) (any, error) {
			var input PutParameterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.PutParameter(&input)
		},
		"GetParameter": func(b []byte) (any, error) {
			var input GetParameterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParameter(&input)
		},
		"GetParameters": func(b []byte) (any, error) {
			var input GetParametersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParameters(&input)
		},
		"GetParameterHistory": func(b []byte) (any, error) {
			var input GetParameterHistoryInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParameterHistory(&input)
		},
		"DeleteParameter": func(b []byte) (any, error) {
			var input DeleteParameterInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteParameter(&input)
		},
		"DeleteParameters": func(b []byte) (any, error) {
			var input DeleteParametersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DeleteParameters(&input)
		},
		"GetParametersByPath": func(b []byte) (any, error) {
			var input GetParametersByPathInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.GetParametersByPath(&input)
		},
		"DescribeParameters": func(b []byte) (any, error) {
			var input DescribeParametersInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.DescribeParameters(&input)
		},
		"AddTagsToResource": func(b []byte) (any, error) {
			var input AddTagsToResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.AddTagsToResource(&input)
		},
		"RemoveTagsFromResource": func(b []byte) (any, error) {
			var input RemoveTagsFromResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return struct{}{}, h.Backend.RemoveTagsFromResource(&input)
		},
		"ListTagsForResource": func(b []byte) (any, error) {
			var input ListTagsForResourceInput
			if err := json.Unmarshal(b, &input); err != nil {
				return nil, err
			}

			return h.Backend.ListTagsForResource(&input)
		},
	}
}

// dispatch routes the operation to the appropriate handler.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	fn, ok := h.ssmDispatchTable()[action]
	if !ok {
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}

	response, err := fn(body)
	if err != nil {
		return nil, err
	}

	return json.Marshal(response)
}

// handleError writes a standardized error response back to the client.
func (h *Handler) handleError(ctx context.Context, c *echo.Context, action string, reqErr error) error {
	log := logger.Load(ctx)
	c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

	var errorType string
	statusCode := http.StatusBadRequest

	switch {
	case errors.Is(reqErr, ErrParameterNotFound):
		errorType = "ParameterNotFound"
	case errors.Is(reqErr, ErrParameterAlreadyExists):
		errorType = "ParameterAlreadyExists"
	case errors.Is(reqErr, ErrUnknownOperation):
		errorType = "UnknownOperationException"
	default:
		errorType = "InternalServerError"
		statusCode = http.StatusInternalServerError
	}

	if errorType == "InternalServerError" {
		log.ErrorContext(ctx, "SSM internal error", "error", reqErr, "action", action)
	} else {
		log.WarnContext(ctx, "SSM request error", "error", reqErr, "action", action)
	}

	errResp := service.JSONErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}
