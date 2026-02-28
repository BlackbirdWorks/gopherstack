package service

import (
	"context"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
)

// JSONErrorResponse is the standard JSON error envelope for AWS JSON-protocol services.
// Used in X-Amz-Target dispatch to signal errors back to the SDK.
type JSONErrorResponse struct {
	Type    string `json:"__type"`
	Message string `json:"message"`
}

// DispatchFunc is the signature for service-specific action dispatch functions.
// It receives the request context, the action name extracted from X-Amz-Target,
// and the raw request body; it returns pre-marshaled JSON or an error.
type DispatchFunc func(ctx context.Context, action string, body []byte) ([]byte, error)

// ErrorHandlerFunc is the signature for service-specific error handlers.
// It translates a dispatch error into an HTTP response.
type ErrorHandlerFunc func(ctx context.Context, c *echo.Context, action string, err error) error

// HandleTarget implements the X-Amz-Target JSON protocol dispatch pattern
// shared by many AWS JSON-protocol services (SSM, EventBridge, StepFunctions, CloudWatchLogs, etc.).
//
// It performs:
//  1. GET / → returns supportedOps as JSON
//  2. Non-POST → 405 Method Not Allowed
//  3. Missing or malformed X-Amz-Target → 400 Bad Request
//  4. Body read failure → 500 Internal Server Error
//  5. dispatch call → handleErr on error, else write response with contentType header
func HandleTarget(
	c *echo.Context,
	log *slog.Logger,
	serviceName, contentType string,
	supportedOps []string,
	dispatch DispatchFunc,
	handleErr ErrorHandlerFunc,
) error {
	ctx := c.Request().Context()

	if c.Request().Method == http.MethodGet && c.Request().URL.Path == "/" {
		return c.JSON(http.StatusOK, supportedOps)
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

	log.DebugContext(ctx, serviceName+" request", "action", action)

	response, reqErr := dispatch(ctx, action, body)
	if reqErr != nil {
		return handleErr(ctx, c, action, reqErr)
	}

	c.Response().Header().Set("Content-Type", contentType)

	return c.JSONBlob(http.StatusOK, response)
}
