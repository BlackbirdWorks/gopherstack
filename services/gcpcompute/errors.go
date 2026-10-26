package gcpcompute

import (
	"errors"
	"fmt"
	"net/http"
)

// APIError is a Google JSON REST error.
type APIError struct {
	Reason  string
	Message string
	Status  int
}

// Error implements error.
func (e *APIError) Error() string {
	return fmt.Sprintf("gcpcompute: %d %s: %s", e.Status, e.Reason, e.Message)
}

// ErrSnapshotNull is returned when a snapshot holds a null record.
var ErrSnapshotNull = errors.New("gcpcompute: snapshot contains a null record")

func newErr(status int, reason, format string, args ...any) *APIError {
	return &APIError{Status: status, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func errInvalid(format string, args ...any) *APIError {
	return newErr(http.StatusBadRequest, "invalid", format, args...)
}

func errField(field, value, detail string) *APIError {
	if detail == "" {
		return errInvalid("Invalid value for field '%s': '%s'.", field, value)
	}

	return errInvalid("Invalid value for field '%s': '%s'. %s", field, value, detail)
}

func errRequired(field string) *APIError {
	return newErr(http.StatusBadRequest, "required", "Required field '%s' not specified", field)
}

func errNotFound(path string) *APIError {
	return newErr(http.StatusNotFound, "notFound", "The resource '%s' was not found", path)
}

func errExists(path string) *APIError {
	return newErr(http.StatusConflict, "alreadyExists", "The resource '%s' already exists", path)
}

func errInUse(kind, path, by string) *APIError {
	return newErr(http.StatusBadRequest, "resourceInUseByAnotherResource",
		"The %s resource '%s' is already being used by '%s'", kind, path, by)
}

func errNotReady(path, detail string) *APIError {
	if detail == "" {
		return newErr(http.StatusBadRequest, "resourceNotReady", "The resource '%s' is not ready", path)
	}

	return newErr(http.StatusBadRequest, "resourceNotReady", "The resource '%s' is not ready: %s", path, detail)
}

func errPrecondition(format string, args ...any) *APIError {
	return newErr(http.StatusPreconditionFailed, "conditionNotMet", format, args...)
}

func errForbidden(format string, args ...any) *APIError {
	return newErr(http.StatusForbidden, "forbidden", format, args...)
}

func errBadFilter(filter string) *APIError {
	return errField("filter", filter, "Invalid list filter expression.")
}
