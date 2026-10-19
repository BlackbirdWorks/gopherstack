package cloudhsmv2

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrAccessDenied          = errors.New("access denied")
	ErrInternalFailure       = errors.New("internal failure")
	ErrInvalidRequest        = errors.New("invalid request")
	ErrResourceLimitExceeded = errors.New("resource limit exceeded")
	ErrResourceNotFound      = errors.New("resource not found")
	ErrService               = errors.New("service error")
	ErrTag                   = errors.New("invalid tag")
	errUnknownOperation      = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrAccessDenied:          apiErr("CloudHsmAccessDeniedException", http.StatusBadRequest),
		ErrInternalFailure:       apiErr("CloudHsmInternalFailureException", http.StatusInternalServerError),
		ErrInvalidRequest:        apiErr("CloudHsmInvalidRequestException", http.StatusBadRequest),
		ErrResourceLimitExceeded: apiErr("CloudHsmResourceLimitExceededException", http.StatusBadRequest),
		ErrResourceNotFound:      apiErr("CloudHsmResourceNotFoundException", http.StatusBadRequest),
		ErrService:               apiErr("CloudHsmServiceException", http.StatusBadRequest),
		ErrTag:                   apiErr("CloudHsmTagException", http.StatusBadRequest),
		errUnknownOperation:      apiErr("CloudHsmInvalidRequestException", http.StatusBadRequest),
	}
}
