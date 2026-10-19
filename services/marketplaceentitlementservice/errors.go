package marketplaceentitlementservice

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrInvalidParameter     = errors.New("invalid parameter")
	ErrInternalServiceError = errors.New("internal service error")
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
		ErrInvalidParameter:     apiErr("InvalidParameterException", http.StatusBadRequest),
		ErrInternalServiceError: apiErr("InternalServiceErrorException", http.StatusInternalServerError),
		errUnknownOperation:     apiErr("InvalidParameterException", http.StatusBadRequest),
	}
}
