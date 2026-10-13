// Package awserr provides shared sentinel errors for AWS service stubs.
// Service packages wrap these sentinels so callers can use [errors.Is]
// to match any service error against a shared sentinel.
package awserr

import (
	"fmt"
	"strings"
	"unicode"
)

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

// Newf creates an error with a formatted message that wraps the given sentinel.
func Newf(msg string, sentinel error, args ...any) error {
	return &wrappedError{msg: fmt.Sprintf(msg, args...), cause: sentinel}
}

type wrappedError struct {
	cause error
	msg   string
}

func (e *wrappedError) Error() string { return e.msg }
func (e *wrappedError) Unwrap() error { return e.cause }

// StripCode removes a leading "<code>" (optionally followed by ":" and spaces)
// from msg, so sentinel-backed errors don't echo the exception code in the
// message. A message that was only the code becomes the humanized code
// ("ActivityDoesNotExist" -> "Activity does not exist").
func StripCode(code, msg string) string {
	if code == "" || !strings.HasPrefix(msg, code) {
		return msg
	}

	rest := msg[len(code):]
	if rest != "" && rest[0] != ':' && rest[0] != ' ' {
		return msg
	}

	if out := strings.TrimSpace(strings.TrimPrefix(rest, ":")); out != "" {
		return out
	}

	return humanizeCode(code)
}

func humanizeCode(code string) string {
	code = strings.TrimSuffix(strings.TrimSuffix(code, "Exception"), "Error")

	var b strings.Builder

	for i, r := range code {
		switch {
		case r == '.' || r == '_':
			b.WriteByte(' ')
		case i > 0 && unicode.IsUpper(r):
			b.WriteByte(' ')
			b.WriteRune(unicode.ToLower(r))
		case i == 0:
			b.WriteRune(unicode.ToUpper(r))
		default:
			b.WriteRune(r)
		}
	}

	return strings.Join(strings.Fields(b.String()), " ")
}
