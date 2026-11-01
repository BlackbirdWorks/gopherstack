package ocifilestorage

import (
	"errors"
	"net/http"
)

// ErrNullEntry is returned when a persisted snapshot contains a null entry.
var ErrNullEntry = errors.New("ocifilestorage: null entry in snapshot")

// APIError is an OCI error response: HTTP status plus the `code` and `message` body fields.
type APIError struct {
	Code    string
	Message string
	Status  int
}

func (e *APIError) Error() string { return e.Code + ": " + e.Message }

func newErr(status int, code, msg string) *APIError {
	return &APIError{Status: status, Code: code, Message: msg}
}

func errNotFound(what string) *APIError {
	return newErr(http.StatusNotFound, "NotAuthorizedOrNotFound",
		"Authorization failed or requested resource ("+what+") not found.")
}

func errNoRoute() *APIError {
	return newErr(http.StatusNotFound, "NotFound", "No operation exists at the specified path and method")
}

func errParam(msg string) *APIError { return newErr(http.StatusBadRequest, "InvalidParameter", msg) }

func errMissing(msg string) *APIError { return newErr(http.StatusBadRequest, "MissingParameter", msg) }

func errState(msg string) *APIError { return newErr(http.StatusConflict, "IncorrectState", msg) }

func errEtag() *APIError {
	return newErr(http.StatusPreconditionFailed, "NoEtagMatch", "The If-Match header does not match the current ETag")
}

func errAuth(msg string) *APIError { return newErr(http.StatusUnauthorized, "NotAuthenticated", msg) }

func errConflict(msg string) *APIError { return newErr(http.StatusConflict, "Conflict", msg) }

func errLimit(msg string) *APIError { return newErr(http.StatusBadRequest, "LimitExceeded", msg) }

func internalErr(err error) *APIError {
	return newErr(http.StatusInternalServerError, "InternalServerError", "data plane failure: "+err.Error())
}
