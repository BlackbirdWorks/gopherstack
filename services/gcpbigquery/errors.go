package gcpbigquery

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/services/gcpbigquery/bqsql"
)

// APIError is a REST error with the HTTP status and BigQuery reason it surfaces as.
type APIError struct {
	Reason  string
	Message string
	Status  int
}

// Error implements error.
func (e *APIError) Error() string {
	return fmt.Sprintf("gcpbigquery: %d %s: %s", e.Status, e.Reason, e.Message)
}

func newErr(status int, reason, msg string) *APIError {
	return &APIError{Status: status, Reason: reason, Message: msg}
}

func errNotFound(format string, args ...any) *APIError {
	return newErr(http.StatusNotFound, litNotfound, fmt.Sprintf(format, args...))
}

func errInvalid(format string, args ...any) *APIError {
	return newErr(http.StatusBadRequest, litInvalid, fmt.Sprintf(format, args...))
}

func errDuplicate(format string, args ...any) *APIError {
	return newErr(http.StatusConflict, litDuplicate, fmt.Sprintf(format, args...))
}

func errInvalidQuery(msg string) *APIError { return newErr(http.StatusBadRequest, "invalidQuery", msg) }

func errInUse(format string, args ...any) *APIError {
	return newErr(http.StatusBadRequest, "resourceInUse", fmt.Sprintf(format, args...))
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
	case http.StatusRequestEntityTooLarge:
		return "INVALID_ARGUMENT"
	}

	return "INTERNAL"
}

// fromSQL converts an engine failure into an API error.
func fromSQL(err error) *APIError {
	if err == nil {
		return nil
	}

	if ae, ok := errors.AsType[*APIError](err); ok {
		return ae
	}

	e := bqsql.AsError(err)

	switch e.Kind {
	case bqsql.ErrNotFound:
		return errNotFound("%s", e.Message)
	case bqsql.ErrExists:
		return errDuplicate("%s", e.Message)
	case bqsql.ErrInvalid, bqsql.ErrRuntime, bqsql.ErrUnsupported:
		return errInvalidQuery(e.Message)
	}

	return errInvalidQuery(e.Message)
}

// ErrSnapshotNull is returned when a snapshot holds a null dataset.
var ErrSnapshotNull = errors.New("gcpbigquery: snapshot contains a null dataset")
