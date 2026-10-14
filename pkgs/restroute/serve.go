package restroute

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// Error is an AWS REST-JSON exception: HTTP status, error code, message and optional extra body members.
type Error struct {
	Extra   map[string]any
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// NewError builds an Error with an arbitrary status and code.
func NewError(status int, code, format string, args ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, args...)}
}

// NotFound builds a 404 ResourceNotFoundException.
func NotFound(format string, args ...any) *Error {
	return NewError(http.StatusNotFound, "ResourceNotFoundException", format, args...)
}

// Conflict builds a 409 ConflictException.
func Conflict(format string, args ...any) *Error {
	return NewError(http.StatusConflict, "ConflictException", format, args...)
}

// Validation builds a 400 ValidationException.
func Validation(format string, args ...any) *Error {
	return NewError(http.StatusBadRequest, "ValidationException", format, args...)
}

// QuotaExceeded builds a 402 ServiceQuotaExceededException.
func QuotaExceeded(format string, args ...any) *Error {
	return NewError(http.StatusPaymentRequired, "ServiceQuotaExceededException", format, args...)
}

// Request is the decoded input of one operation.
type Request struct {
	Params map[string]string
	Query  url.Values
	Body   []byte
}

// Decode unmarshals the JSON body into v; an empty body leaves v untouched.
func (r *Request) Decode(v any) error {
	if len(r.Body) == 0 {
		return nil
	}

	if err := json.Unmarshal(r.Body, v); err != nil {
		return Validation("request body is not valid JSON for this operation")
	}

	return nil
}

// OpFunc runs one operation. A nil result is sent as an empty object; a zero status means 200.
type OpFunc func(ctx context.Context, req *Request) (result any, status int, err error)

// Server dispatches matched requests to OpFuncs.
type Server struct {
	Router *Router
	Ops    map[string]OpFunc
	// Context derives the per-request context (region, caller identity, ...); nil uses the request context.
	Context func(r *http.Request) context.Context
}

// Operation returns the op name and path labels for r, or "" when no route matches.
func (s *Server) Operation(r *http.Request) (string, map[string]string) {
	return s.Router.Match(r.Method, EscapedPath(r))
}

// Handler serves a request: route, read body, run the op, write JSON or the mapped error.
func (s *Server) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		r := c.Request()
		op, params := s.Operation(r)

		fn, ok := s.Ops[op]
		if !ok {
			return WriteError(c, http.StatusNotFound, "UnknownOperationException", "Unknown operation", nil)
		}

		body, err := httputils.ReadBody(r)
		if err != nil {
			return WriteError(c, http.StatusBadRequest, "ValidationException", "Unable to read the request body", nil)
		}

		ctx := r.Context()
		if s.Context != nil {
			ctx = s.Context(r)
		}

		result, status, err := fn(ctx, &Request{Params: params, Query: r.URL.Query(), Body: body})
		if err != nil {
			return s.WriteErr(c, err)
		}

		if status == 0 {
			status = http.StatusOK
		}

		if result == nil {
			result = struct{}{}
		}

		return c.JSON(status, result)
	}
}

// WriteErr maps err to its REST-JSON response; unknown errors become a 500 InternalServerException.
func (s *Server) WriteErr(c *echo.Context, err error) error {
	if apiErr, ok := errors.AsType[*Error](err); ok {
		return WriteError(c, apiErr.Status, apiErr.Code, apiErr.Message, apiErr.Extra)
	}

	return WriteError(c, http.StatusInternalServerError, "InternalServerException", "Internal server error", nil)
}

// With attaches an extra body member (for example a modeled "reason") and returns e.
func (e *Error) With(key string, value any) *Error {
	if e.Extra == nil {
		e.Extra = map[string]any{}
	}

	e.Extra[key] = value

	return e
}
