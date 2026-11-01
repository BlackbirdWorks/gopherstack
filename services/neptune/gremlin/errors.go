package gremlin

import (
	"errors"
	"fmt"
)

// Neptune error codes returned by the HTTP endpoint; WebSocket clients get the Gremlin Server status instead.
const (
	CodeMalformedQuery      = "MalformedQueryException"
	CodeInvalidParameter    = "InvalidParameterException"
	CodeConstraintViolation = "ConstraintViolationException"
	CodeUnsupported         = "UnsupportedOperationException"
	CodeTimeLimit           = "TimeLimitExceededException"
	CodeCancelled           = "CancelledByUserException"
	CodeInternal            = "InternalFailureException"
	CodeUnauthorized        = "AccessDeniedException"
)

// Gremlin Server response status codes.
const (
	StatusSuccess          = 200
	StatusNoContent        = 204
	StatusPartial          = 206
	StatusUnauthorized     = 401
	StatusForbidden        = 403
	StatusInvalidRequest   = 499
	StatusServerError      = 500
	StatusScriptEvalError  = 597
	StatusServerTimeout    = 598
	StatusSerializationErr = 599
)

// Error is a query failure carrying the Gremlin Server status and the Neptune error code.
type Error struct {
	Code   string
	Detail string
	Status int
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

// AsError extracts an *Error from err.
func AsError(err error) (*Error, bool) {
	var e *Error

	ok := errors.As(err, &e)

	return e, ok
}

func malformed(format string, args ...any) error {
	return &Error{Code: CodeMalformedQuery, Status: StatusScriptEvalError, Detail: fmt.Sprintf(format, args...)}
}

func invalidArg(format string, args ...any) error {
	return &Error{Code: CodeInvalidParameter, Status: StatusInvalidRequest, Detail: fmt.Sprintf(format, args...)}
}

func unsupported(format string, args ...any) error {
	return &Error{Code: CodeUnsupported, Status: StatusScriptEvalError, Detail: fmt.Sprintf(format, args...)}
}

func constraint(format string, args ...any) error {
	return &Error{Code: CodeConstraintViolation, Status: StatusScriptEvalError, Detail: fmt.Sprintf(format, args...)}
}

func timeLimit(detail string) error {
	return &Error{Code: CodeTimeLimit, Status: StatusServerTimeout, Detail: detail}
}
