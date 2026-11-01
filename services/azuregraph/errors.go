package azuregraph

import (
	"errors"
	"fmt"
	"net/http"
)

// ErrNilAppContext is returned by Provider.Init for a nil AppContext.
var ErrNilAppContext = errors.New("azuregraph: nil app context")

// Graph error codes.
const (
	codeNotFound    = "Request_ResourceNotFound"
	codeBadRequest  = "Request_BadRequest"
	codeUnsupported = "Request_UnsupportedQuery"
	codeDenied      = "Authorization_RequestDenied"
	codeInvalidAuth = "InvalidAuthenticationToken"
	codeBadSegment  = "BadRequest"
)

// Error is a Graph failure rendered as an OData error envelope.
type Error struct {
	Code    string
	Message string
	Status  int
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, format string, a ...any) *Error {
	return &Error{Status: status, Code: code, Message: fmt.Sprintf(format, a...)}
}

func badRequest(format string, a ...any) *Error {
	return newErr(http.StatusBadRequest, codeBadRequest, format, a...)
}

func unsupported(format string, a ...any) *Error {
	return newErr(http.StatusBadRequest, codeUnsupported, format, a...)
}

func notFound(id string) *Error {
	return newErr(http.StatusNotFound, codeNotFound,
		"Resource '%s' does not exist or one of its queried reference-property objects are not present.", id)
}

func denied() *Error {
	return newErr(http.StatusForbidden, codeDenied, "Insufficient privileges to complete the operation.")
}

type errorEnvelope struct {
	Error errorBody `json:"error"`
}

type errorBody struct {
	InnerError innerError `json:"innerError"`
	Code       string     `json:"code"`
	Message    string     `json:"message"`
}

type innerError struct {
	Date            string `json:"date"`
	RequestID       string `json:"request-id"`
	ClientRequestID string `json:"client-request-id"`
}
