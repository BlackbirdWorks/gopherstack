package servicecatalog

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

// Sentinels, one per exception in the pinned SDK's error set.
var (
	ErrDuplicateResource      = errors.New("duplicate resource")
	ErrInvalidParameters      = errors.New("invalid parameters")
	ErrInvalidState           = errors.New("invalid state")
	ErrLimitExceeded          = errors.New("limit exceeded")
	ErrOperationNotSupported  = errors.New("operation not supported")
	ErrResourceInUse          = errors.New("resource in use")
	ErrResourceNotFound       = errors.New("resource not found")
	ErrTagOptionNotMigrated   = errors.New("tag option not migrated")
	ErrInternalFailure        = errors.New("internal failure")
	errUnknownOperation       = errors.New("unknown operation")
	errStackEngineUnavailable = errors.New("no CloudFormation engine is wired")
)

func fail(sentinel error, msg string, args ...any) error {
	return awserr.Newf(msg, sentinel, args...)
}

func invalid(msg string, args ...any) error { return fail(ErrInvalidParameters, msg, args...) }

func notFound(kind, id string) error { return fail(ErrResourceNotFound, "%s %s not found", kind, id) }

func apiErr(code string, status int) awserr.APIError {
	return awserr.APIError{Code: code, HTTPStatus: status}
}

func errorTable() map[error]awserr.APIError {
	return map[error]awserr.APIError{
		ErrDuplicateResource:     apiErr("DuplicateResourceException", http.StatusBadRequest),
		ErrInvalidParameters:     apiErr("InvalidParametersException", http.StatusBadRequest),
		ErrInvalidState:          apiErr("InvalidStateException", http.StatusBadRequest),
		ErrLimitExceeded:         apiErr("LimitExceededException", http.StatusBadRequest),
		ErrOperationNotSupported: apiErr("OperationNotSupportedException", http.StatusBadRequest),
		ErrResourceInUse:         apiErr("ResourceInUseException", http.StatusBadRequest),
		ErrResourceNotFound:      apiErr("ResourceNotFoundException", http.StatusBadRequest),
		ErrTagOptionNotMigrated:  apiErr("TagOptionNotMigratedException", http.StatusBadRequest),
		errUnknownOperation:      apiErr("InvalidParametersException", http.StatusBadRequest),
	}
}
