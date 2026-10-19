package gcpstorage

import (
	"errors"
	"fmt"
	"net/http"
)

// APIError is an error carrying the HTTP status and Google error reason.
type APIError struct {
	Reason  string
	Message string
	Status  int
}

// Error implements error.
func (e *APIError) Error() string {
	return fmt.Sprintf("gcpstorage: %d %s: %s", e.Status, e.Reason, e.Message)
}

func newErr(status int, reason, msg string) *APIError {
	return &APIError{Status: status, Reason: reason, Message: msg}
}

func errNotFound(msg string) *APIError { return newErr(http.StatusNotFound, "notFound", msg) }

func errInvalid(msg string) *APIError { return newErr(http.StatusBadRequest, "invalid", msg) }

func errConflict(msg string) *APIError { return newErr(http.StatusConflict, "conflict", msg) }

func errPrecondition() *APIError {
	return newErr(http.StatusPreconditionFailed, "conditionNotMet",
		"At least one of the pre-conditions you specified did not hold.")
}

func errNotModified() *APIError {
	return newErr(http.StatusNotModified, "notModified", "Not Modified")
}

// ErrSnapshotBucketNull is returned by Restore when a snapshot holds a null bucket.
var ErrSnapshotBucketNull = errors.New("gcpstorage: restore snapshot: bucket is null")
