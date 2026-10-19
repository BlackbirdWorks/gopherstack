package bcmdataexports

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrAccessDenied         = errors.New("access denied")
	ErrInternalServer       = errors.New("internal server error")
	ErrResourceNotFound     = errors.New("resource not found")
	ErrServiceQuotaExceeded = errors.New("service quota exceeded")
	ErrThrottling           = errors.New("throttled")
	ErrValidation           = errors.New("validation failed")
	errUnknownOperation     = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrAccessDenied:         apiErr("AccessDeniedException", http.StatusForbidden),
		ErrInternalServer:       apiErr("InternalServerException", http.StatusInternalServerError),
		ErrResourceNotFound:     apiErr("ResourceNotFoundException", http.StatusNotFound),
		ErrServiceQuotaExceeded: apiErr("ServiceQuotaExceededException", http.StatusBadRequest),
		ErrThrottling:           apiErr("ThrottlingException", http.StatusBadRequest),
		ErrValidation:           apiErr("ValidationException", http.StatusBadRequest),
		errUnknownOperation:     apiErr("ValidationException", http.StatusBadRequest),
	}
}
