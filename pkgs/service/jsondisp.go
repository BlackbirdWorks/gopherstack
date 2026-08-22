package service

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
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

// HandleJSON is a generic dispatcher that decodes the JSON body into a typed input,
// calls fn, and returns the typed output as any.
// If body is non-empty and cannot be decoded, HandleJSON returns the decode error directly.
func HandleJSON[In, Out any](
	ctx context.Context,
	body []byte,
	fn func(context.Context, *In) (*Out, error),
) (any, error) {
	var input In

	if len(body) > 0 {
		if err := json.Unmarshal(body, &input); err != nil {
			return nil, err
		}
	}

	return fn(ctx, &input)
}

// JSONOpFunc is the function type for a dispatched JSON-protocol operation.
// Implementations are produced by WrapOp and collected in a dispatchTable map.
type JSONOpFunc func(ctx context.Context, body []byte) (any, error)

// WrapOp adapts a typed HandleJSON handler into a JSONOpFunc for use in dispatch tables.
// It is the canonical way to register a typed operation handler:
//
//	"CreateFoo": service.WrapOp(h.handleCreateFoo),
func WrapOp[In, Out any](fn func(context.Context, *In) (*Out, error)) JSONOpFunc {
	return func(ctx context.Context, body []byte) (any, error) {
		return HandleJSON(ctx, body, fn)
	}
}

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
		return writeDispatchError(c, contentType, http.StatusMethodNotAllowed,
			"UnknownOperationException", "Method not allowed")
	}

	target := c.Request().Header.Get("X-Amz-Target")
	if target == "" {
		return writeDispatchError(c, contentType, http.StatusBadRequest,
			"UnknownOperationException", "Missing X-Amz-Target")
	}

	parts := strings.Split(target, ".")

	const targetParts = 2
	if len(parts) != targetParts {
		return writeDispatchError(c, contentType, http.StatusBadRequest,
			"UnknownOperationException", "Invalid X-Amz-Target")
	}

	action := parts[1]

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		log.ErrorContext(ctx, "failed to read request body", "error", err)

		return writeDispatchError(c, contentType, http.StatusInternalServerError,
			"InternalFailure", "internal server error")
	}

	log.DebugContext(ctx, serviceName+" request", "action", action)

	response, reqErr := dispatch(ctx, action, body)
	if reqErr != nil {
		return handleErr(ctx, c, action, reqErr)
	}

	c.Response().Header().Set("Content-Type", contentType)

	return c.JSONBlob(http.StatusOK, response)
}

// writeDispatchError writes a JSONErrorResponse for a failure in HandleTarget
// itself (bad method, missing/malformed X-Amz-Target, body-read failure) --
// framework-level errors that never reach the service's own ErrorHandlerFunc.
// These previously went out as bare text/plain, which the real SDK's JSON
// protocol error decoder (__type/message) cannot read: every such response
// reached a client as smithy.GenericAPIError{Code:"UnknownError"}.
func writeDispatchError(c *echo.Context, contentType string, status int, errType, message string) error {
	c.Response().Header().Set("Content-Type", contentType)

	payload, err := json.Marshal(JSONErrorResponse{Type: errType, Message: message})
	if err != nil {
		return err
	}

	return c.JSONBlob(status, payload)
}
