package apigatewayv2

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// errTypeHeader is the HTTP response header AWS uses to carry the modeled error
// type for the API Gateway v2 REST-JSON protocol. Clients (and the AWS SDKs)
// read this header to map a response to a concrete exception type rather than
// relying on the HTTP status code alone. The value is the canonical MIME form
// of AWS's "x-amzn-ErrorType" (header names are case-insensitive on the wire
// and Go canonicalises them to this casing).
const errTypeHeader = "X-Amzn-Errortype"

// AWS API Gateway v2 modeled exception type names, keyed by the HTTP status code
// they map to. These are the names AWS returns in the X-Amzn-ErrorType header.
const (
	errTypeBadRequest       = "BadRequestException"
	errTypeUnauthorized     = "UnauthorizedException"
	errTypeAccessDenied     = "AccessDeniedException"
	errTypeNotFound         = "NotFoundException"
	errTypeMethodNotAllowed = "MethodNotAllowedException"
	errTypeConflict         = "ConflictException"
	errTypeTooManyRequests  = "TooManyRequestsException"
	errTypeInternal         = "InternalServerErrorException"
)

// errorTypeForStatus returns the AWS modeled exception type name for an HTTP
// status code. API Gateway v2 uses a stable status→exception mapping, so the
// error type can be derived from the status code deterministically.
func errorTypeForStatus(status int) string {
	switch status {
	case http.StatusBadRequest:
		return errTypeBadRequest
	case http.StatusUnauthorized:
		return errTypeUnauthorized
	case http.StatusForbidden:
		return errTypeAccessDenied
	case http.StatusNotFound:
		return errTypeNotFound
	case http.StatusMethodNotAllowed:
		return errTypeMethodNotAllowed
	case http.StatusConflict:
		return errTypeConflict
	case http.StatusTooManyRequests:
		return errTypeTooManyRequests
	default:
		return errTypeInternal
	}
}

// writeErr writes an AWS-shaped error response: a {"message": ...} JSON body
// plus the X-Amzn-ErrorType header carrying the modeled exception type derived
// from the HTTP status. This mirrors what real API Gateway v2 returns so SDK
// clients can classify the fault.
func writeErr(c *echo.Context, status int, message string) error {
	return writeErrType(c, status, errorTypeForStatus(status), message)
}

// writeErrType is like writeErr but with an explicit error-type name, for the
// rare cases where the exception type does not follow the default status
// mapping (e.g. an AccessDeniedException returned with a non-403 status).
func writeErrType(c *echo.Context, status int, errType, message string) error {
	if errType != "" {
		c.Response().Header().Set(errTypeHeader, errType)
	}

	return c.JSON(status, notFoundResponse{Message: message})
}
