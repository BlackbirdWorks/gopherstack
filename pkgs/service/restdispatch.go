package service

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// RESTRouter implements the ExtractOperation/ExtractResource/MatchPriority/
// Handler wiring shared by services whose routing reduces to parsing an
// operation name and resource identifier out of the request's (method,
// path). Several services (GuardDuty, Macie2, ...) previously hand-rolled
// byte-identical copies of this glue; the only thing that actually varies
// per service is the path grammar (Parse) and how operations are dispatched
// and errors translated (Dispatch/HandleError below).
//
// Construct a RESTRouter (typically from a small per-request or per-call
// helper method on the service's Handler) and delegate the four
// interface methods to it; see services/guardduty/handler.go and
// services/macie2/handler.go for the intended usage.
type RESTRouter struct {
	// ServiceName appears in the operation-error log line.
	ServiceName string
	// Parse extracts the operation name and resource identifier from the
	// request method and URL path.
	Parse func(method, path string) (op, resource string)
	// Dispatch runs the parsed operation against the service backend.
	Dispatch func(ctx context.Context, op, path, query string, body []byte) (any, int, error)
	// HandleError translates a Dispatch error into an Echo response.
	HandleError func(c *echo.Context, err error) error
	// NotFoundBody builds the JSON body for an unrecognized-operation 404.
	NotFoundBody func() any
	// BadRequestBody builds the JSON body for a request-body read failure.
	BadRequestBody func() any
	// InternalErrorBody builds the JSON body for a result-marshal failure.
	InternalErrorBody func() any
	// OpUnknown is the sentinel Parse returns for an unrecognized path.
	OpUnknown string
	// Priority is the routing priority returned by MatchPriority.
	Priority int
}

// MatchPriority returns the routing priority.
func (r RESTRouter) MatchPriority() int { return r.Priority }

// ExtractOperation extracts the operation name from the request.
func (r RESTRouter) ExtractOperation(c *echo.Context) string {
	op, _ := r.Parse(c.Request().Method, c.Request().URL.Path)

	return op
}

// ExtractResource extracts the resource identifier from the request.
func (r RESTRouter) ExtractResource(c *echo.Context) string {
	_, resource := r.Parse(c.Request().Method, c.Request().URL.Path)

	return resource
}

// Handler returns the Echo handler function.
func (r RESTRouter) Handler() echo.HandlerFunc {
	return r.handleREST
}

// handleREST parses the operation, 404s on an unrecognized one, reads the
// body, dispatches, and marshals a successful result as JSON.
func (r RESTRouter) handleREST(c *echo.Context) error {
	ctx := c.Request().Context()
	log := logger.Load(ctx)

	op, _ := r.Parse(c.Request().Method, c.Request().URL.Path)

	if op == r.OpUnknown {
		return c.JSON(http.StatusNotFound, r.NotFoundBody())
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusBadRequest, r.BadRequestBody())
	}

	result, statusCode, opErr := r.Dispatch(ctx, op, c.Request().URL.Path, c.Request().URL.RawQuery, body)
	if opErr != nil {
		log.Error(r.ServiceName+" operation error", "op", op, "err", opErr)

		return r.HandleError(c, opErr)
	}

	if result == nil {
		return c.JSON(statusCode, struct{}{})
	}

	data, jsonErr := json.Marshal(result)
	if jsonErr != nil {
		return c.JSON(http.StatusInternalServerError, r.InternalErrorBody())
	}

	c.Response().Header().Set("Content-Type", "application/json")

	return c.JSONBlob(statusCode, data)
}
