package gcpsql

import (
	"errors"
	"fmt"
	"net/http"
)

// APIError is a REST error with the HTTP status and reason it surfaces as.
type APIError struct {
	Reason  string
	Message string
	Status  int
}

// Error implements error.
func (e *APIError) Error() string {
	return fmt.Sprintf("gcpsql: %d %s: %s", e.Status, e.Reason, e.Message)
}

func newErr(status int, reason, format string, args ...any) *APIError {
	return &APIError{Status: status, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func errInvalid(format string, args ...any) *APIError {
	return newErr(http.StatusBadRequest, "invalid", "Invalid request: "+format, args...)
}

func errNoInstance() *APIError {
	return newErr(http.StatusNotFound, "instanceDoesNotExist", "The Cloud SQL instance does not exist.")
}

func errNotFound(format string, args ...any) *APIError {
	return newErr(http.StatusNotFound, "notFound", format, args...)
}

func errConflict(reason, format string, args ...any) *APIError {
	return newErr(http.StatusConflict, reason, format, args...)
}

func errNotRunnable() *APIError {
	return newErr(http.StatusConflict, "operationInProgress",
		"The instance or operation is not in an appropriate state to handle the request.")
}

func statusText(status int) string {
	switch status {
	case http.StatusNotFound:
		return "NOT_FOUND"
	case http.StatusConflict:
		return "ALREADY_EXISTS"
	case http.StatusBadRequest:
		return "INVALID_ARGUMENT"
	case http.StatusForbidden:
		return "PERMISSION_DENIED"
	case http.StatusPreconditionFailed:
		return "FAILED_PRECONDITION"
	case http.StatusRequestEntityTooLarge:
		return "INVALID_ARGUMENT"
	}

	return "INTERNAL"
}

// ErrSnapshotNull is returned when a snapshot holds a null instance.
var ErrSnapshotNull = errors.New("gcpsql: snapshot contains a null instance")

var errInterrupted = errors.New("operation was interrupted by a restart")
