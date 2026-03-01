// Package awserr provides shared sentinel errors for AWS service stubs.
// Service packages wrap these sentinels so callers can use [errors.Is]
// to match any service error against a shared sentinel.
package awserr

// sentinelError is an unexported type used for constant sentinel errors.
// Using a distinct type prevents reassignment and enables reliable [errors.Is] matching.
type sentinelError string

func (e sentinelError) Error() string { return string(e) }

// ErrNotFound is a sentinel indicating a requested resource does not exist.
const ErrNotFound sentinelError = "resource not found"

// ErrAlreadyExists is a sentinel indicating a resource already exists.
const ErrAlreadyExists sentinelError = "resource already exists"

// ErrInvalidParameter is a sentinel indicating an invalid or missing parameter.
const ErrInvalidParameter sentinelError = "invalid parameter"

// ErrConflict is a sentinel indicating a conflicting state or concurrent operation.
const ErrConflict sentinelError = "conflict"

// New creates an error with the given message that wraps the given sentinel.
// This preserves the service-specific error message while enabling [errors.Is]
// to match the shared sentinel.
func New(msg string, sentinel error) error {
	return &wrappedError{msg: msg, cause: sentinel}
}

type wrappedError struct {
	cause error
	msg   string
}

func (e *wrappedError) Error() string { return e.msg }
func (e *wrappedError) Unwrap() error { return e.cause }
