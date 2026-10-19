package budgets

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception the pinned SDK declares and this service raises.
var (
	ErrNotFound              = errors.New("not found")
	ErrDuplicateRecord       = errors.New("duplicate record")
	ErrCreationLimitExceeded = errors.New("creation limit exceeded")
	ErrInvalidParameter      = errors.New("invalid parameter")
	ErrInvalidNextToken      = errors.New("invalid next token")
	ErrResourceLocked        = errors.New("resource locked")
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
		ErrNotFound:              apiErr("NotFoundException", http.StatusBadRequest),
		ErrDuplicateRecord:       apiErr("DuplicateRecordException", http.StatusBadRequest),
		ErrCreationLimitExceeded: apiErr("CreationLimitExceededException", http.StatusBadRequest),
		ErrInvalidParameter:      apiErr("InvalidParameterException", http.StatusBadRequest),
		ErrInvalidNextToken:      apiErr("InvalidNextTokenException", http.StatusBadRequest),
		ErrResourceLocked:        apiErr("ResourceLockedException", http.StatusBadRequest),
		errUnknownOperation:      apiErr("InvalidParameterException", http.StatusBadRequest),
	}
}
