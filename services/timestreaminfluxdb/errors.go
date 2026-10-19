package timestreaminfluxdb

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrAccessDenied     = errors.New("access denied")
	ErrConflict         = errors.New("conflict")
	ErrInternalServer   = errors.New("internal server error")
	ErrResourceNotFound = errors.New("resource not found")
	ErrQuotaExceeded    = errors.New("service quota exceeded")
	ErrThrottling       = errors.New("throttling")
	ErrValidation       = errors.New("validation")
	errUnknownOperation = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func invalid(msg string, args ...any) error  { return fail(ErrValidation, msg, args...) }
func conflict(msg string, args ...any) error { return fail(ErrConflict, msg, args...) }

func notFound(kind, id string) error {
	return fail(ErrResourceNotFound, "%s %s was not found", kind, id)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrAccessDenied:     apiErr("AccessDeniedException", http.StatusForbidden),
		ErrConflict:         apiErr("ConflictException", http.StatusConflict),
		ErrInternalServer:   apiErr("InternalServerException", http.StatusInternalServerError),
		ErrResourceNotFound: apiErr("ResourceNotFoundException", http.StatusNotFound),
		ErrQuotaExceeded:    apiErr("ServiceQuotaExceededException", http.StatusPaymentRequired),
		ErrThrottling:       apiErr("ThrottlingException", http.StatusTooManyRequests),
		ErrValidation:       apiErr("ValidationException", http.StatusBadRequest),
		errUnknownOperation: apiErr("ValidationException", http.StatusBadRequest),
	}
}

func quota(what string, limit int) error {
	return fail(ErrQuotaExceeded, "the limit of %d %s was reached", limit, what)
}
