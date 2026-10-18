package codegurureviewer

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func validationErr(format string, args ...any) error {
	return restroute.NewError(http.StatusBadRequest, "ValidationException", format, args...)
}

func notFoundErr(format string, args ...any) error {
	return restroute.NewError(http.StatusNotFound, "NotFoundException", format, args...)
}

func resourceNotFoundErr(format string, args ...any) error {
	return restroute.NewError(http.StatusNotFound, "ResourceNotFoundException", format, args...)
}

func conflictErr(format string, args ...any) error {
	return restroute.NewError(http.StatusConflict, "ConflictException", format, args...)
}

func idempotencyErr(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return validationErr("%v", err)
	}

	return err
}
