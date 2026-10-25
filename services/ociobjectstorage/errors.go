package ociobjectstorage

import (
	"errors"
	"net/http"
)

// ErrSnapshotNull is returned when a snapshot contains a null bucket.
var ErrSnapshotNull = errors.New("ociobjectstorage: null entry in snapshot")

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

func errNamespace(ns string) *APIError {
	return newErr(http.StatusNotFound, "NamespaceNotFound",
		"You do not have authorization to perform this request, or the requested resource ("+ns+") could not be found.")
}

func errBucket(ns, name string) *APIError {
	return newErr(
		http.StatusNotFound,
		"BucketNotFound",
		"Either the bucket named '"+name+"' does not exist in the namespace '"+ns+"' or you are not authorized to access it",
	)
}

func errObject(name string) *APIError {
	return newErr(http.StatusNotFound, "ObjectNotFound", "The object '"+name+"' was not found")
}

func errParam(msg string) *APIError { return newErr(http.StatusBadRequest, "InvalidParameter", msg) }

func errMissing(msg string) *APIError { return newErr(http.StatusBadRequest, "MissingParameter", msg) }

func errNoEtag() *APIError {
	return newErr(http.StatusPreconditionFailed, "NoEtagMatch", "The If-Match header does not match the current ETag")
}

func errNotFound() *APIError {
	return newErr(http.StatusNotFound, "NotFound", "No operation exists at the specified path and method")
}

func errAuth(msg string) *APIError { return newErr(http.StatusUnauthorized, "NotAuthenticated", msg) }

func errConflict(code, msg string) *APIError { return newErr(http.StatusConflict, code, msg) }
