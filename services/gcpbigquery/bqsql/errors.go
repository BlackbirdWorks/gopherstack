package bqsql

import (
	"errors"
	"fmt"
)

// ErrKind classifies a query failure for HTTP status mapping.
type ErrKind uint8

// Error kinds.
const (
	ErrInvalid ErrKind = iota
	ErrNotFound
	ErrExists
	ErrRuntime
	ErrUnsupported
)

// Error is a query analysis or execution failure.
type Error struct {
	Message string
	Kind    ErrKind
}

// Error implements error.
func (e *Error) Error() string { return e.Message }

func invalid(format string, args ...any) error {
	return &Error{Kind: ErrInvalid, Message: fmt.Sprintf(format, args...)}
}

func unsupported(format string, args ...any) error {
	return &Error{Kind: ErrUnsupported, Message: "Unsupported: " + fmt.Sprintf(format, args...)}
}

func notFound(format string, args ...any) error {
	return &Error{Kind: ErrNotFound, Message: fmt.Sprintf(format, args...)}
}

func exists(format string, args ...any) error {
	return &Error{Kind: ErrExists, Message: fmt.Sprintf(format, args...)}
}

// AsError returns err as an *Error, wrapping syntax errors as invalid-query failures.
func AsError(err error) *Error {
	if e, ok := errors.AsType[*Error](err); ok {
		return e
	}

	if errors.Is(err, ErrSyntax) {
		return &Error{Kind: ErrInvalid, Message: "Syntax error: " + err.Error()}
	}

	return &Error{Kind: ErrRuntime, Message: err.Error()}
}
