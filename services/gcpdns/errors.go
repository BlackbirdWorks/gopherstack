package gcpdns

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
	return fmt.Sprintf("gcpdns: %d %s: %s", e.Status, e.Reason, e.Message)
}

func newErr(status int, reason, format string, args ...any) *APIError {
	return &APIError{Status: status, Reason: reason, Message: fmt.Sprintf(format, args...)}
}

func errInvalid(format string, args ...any) *APIError {
	return newErr(http.StatusBadRequest, "invalid", format, args...)
}

func errInvalidValue(field, value string) *APIError {
	return errInvalid("Invalid value for '%s': '%s'", field, value)
}

func errRequired(field string) *APIError {
	return newErr(http.StatusBadRequest, "required", "The '%s' parameter is required.", field)
}

func errNotFound(param, name string) *APIError {
	return newErr(http.StatusNotFound, "notFound", "The '%s' resource named '%s' does not exist.", param, name)
}

func errExists(param, name string) *APIError {
	return newErr(http.StatusConflict, "alreadyExists", "The resource '%s' named '%s' already exists", param, name)
}

func errPrecondition(param string) *APIError {
	return newErr(http.StatusPreconditionFailed, "conditionNotMet", "Precondition not met for '%s'", param)
}

func errNotEmpty(format string, args ...any) *APIError {
	return newErr(http.StatusBadRequest, "containerNotEmpty", format, args...)
}

func errQuota(msg string) *APIError {
	return newErr(http.StatusForbidden, "quotaExceeded", "%s", msg)
}

// ErrSnapshotNull is returned when a snapshot holds a null record.
var ErrSnapshotNull = errors.New("gcpdns: snapshot contains a null record")
