package cur

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrDuplicateReportName = errors.New("report name already exists")
	ErrInternalError       = errors.New("internal error")
	ErrReportLimitReached  = errors.New("report limit reached")
	ErrResourceNotFound    = errors.New("report not found")
	ErrValidation          = errors.New("validation failed")
	errUnknownOperation    = errors.New("unknown operation")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrDuplicateReportName: apiErr("DuplicateReportNameException", http.StatusBadRequest),
		ErrInternalError:       apiErr("InternalErrorException", http.StatusInternalServerError),
		ErrReportLimitReached:  apiErr("ReportLimitReachedException", http.StatusBadRequest),
		ErrResourceNotFound:    apiErr("ResourceNotFoundException", http.StatusBadRequest),
		ErrValidation:          apiErr("ValidationException", http.StatusBadRequest),
		errUnknownOperation:    apiErr("ValidationException", http.StatusBadRequest),
	}
}
