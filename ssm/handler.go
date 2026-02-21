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
		"PutParameter",
		"DeleteParameter",
		"DeleteParameters",
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
const ssmMatchPriority = 100

func (h *Handler) MatchPriority() int {
	return ssmMatchPriority // Same header-based priority as DynamoDB
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

		log.DebugContext(ctx, "SSM request", "action", action)

		response, reqErr := h.dispatch(ctx, action, body)
		if reqErr != nil {
			return h.handleError(ctx, c, action, reqErr)
		}

		c.Response().Header().Set("Content-Type", "application/x-amz-json-1.1")

		return c.JSONBlob(http.StatusOK, response)
	}
}

// dispatch routes the operation to the appropriate handler.
func (h *Handler) dispatch(_ context.Context, action string, body []byte) ([]byte, error) {
	var response any
	var err error

	switch action {
	case "PutParameter":
		var input PutParameterInput
		if unmarshErr := json.Unmarshal(body, &input); unmarshErr != nil {
			return nil, unmarshErr
		}
		response, err = h.Backend.PutParameter(&input)

	case "GetParameter":
		var input GetParameterInput
		if unmarshErr := json.Unmarshal(body, &input); unmarshErr != nil {
			return nil, unmarshErr
		}
		response, err = h.Backend.GetParameter(&input)

	case "GetParameters":
		var input GetParametersInput
		if unmarshErr := json.Unmarshal(body, &input); unmarshErr != nil {
			return nil, unmarshErr
		}
		response, err = h.Backend.GetParameters(&input)

	case "DeleteParameter":
		var input DeleteParameterInput
		if unmarshErr := json.Unmarshal(body, &input); unmarshErr != nil {
			return nil, unmarshErr
		}
		response, err = h.Backend.DeleteParameter(&input)

	case "DeleteParameters":
		var input DeleteParametersInput
		if unmarshErr := json.Unmarshal(body, &input); unmarshErr != nil {
			return nil, unmarshErr
		}
		response, err = h.Backend.DeleteParameters(&input)

	default:
		return nil, fmt.Errorf("%w:%s", ErrUnknownOperation, action)
	}

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

	errResp := ErrorResponse{
		Type:    errorType,
		Message: reqErr.Error(),
	}

	payload, _ := json.Marshal(errResp)

	return c.JSONBlob(statusCode, payload)
}
