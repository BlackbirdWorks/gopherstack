package pricing

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set that this service raises.
var (
	ErrNotFound         = errors.New("not found")
	ErrResourceNotFound = errors.New("resource not found")
	ErrInvalidParameter = errors.New("invalid parameter")
	ErrInvalidNextToken = errors.New("invalid next token")
	errUnknownOperation = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrNotFound:         apiErr("NotFoundException", http.StatusBadRequest),
		ErrResourceNotFound: apiErr("ResourceNotFoundException", http.StatusBadRequest),
		ErrInvalidParameter: apiErr("InvalidParameterException", http.StatusBadRequest),
		ErrInvalidNextToken: apiErr("InvalidNextTokenException", http.StatusBadRequest),
		errUnknownOperation: apiErr("InvalidParameterException", http.StatusBadRequest),
	}
}
