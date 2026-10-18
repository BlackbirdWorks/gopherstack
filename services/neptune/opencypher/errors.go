package opencypher

import (
	"errors"
	"fmt"
)

// Error codes match the Neptune openCypher HTTP API.
const (
	CodeMalformedQuery      = "MalformedQueryException"
	CodeUnsupported         = "UnsupportedOperationException"
	CodeConstraintViolation = "ConstraintViolationException"
	CodeInvalidParameter    = "InvalidParameterException"
)

// Error is a query failure carrying the Neptune error code.
type Error struct {
	Code   string
	Detail string
}

func (e *Error) Error() string { return e.Code + ": " + e.Detail }

// AsError extracts an *Error from err.
func AsError(err error) (*Error, bool) {
	var e *Error

	ok := errors.As(err, &e)

	return e, ok
}

func syntaxErr(pos int, msg string) error {
	return &Error{Code: CodeMalformedQuery, Detail: fmt.Sprintf("%s (offset: %d)", msg, pos)}
}

func malformed(format string, args ...any) error {
	return &Error{Code: CodeMalformedQuery, Detail: fmt.Sprintf(format, args...)}
}

func unsupported(format string, args ...any) error {
	return &Error{Code: CodeUnsupported, Detail: fmt.Sprintf(format, args...)}
}

func constraint(format string, args ...any) error {
	return &Error{Code: CodeConstraintViolation, Detail: fmt.Sprintf(format, args...)}
}
