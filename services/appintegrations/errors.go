package appintegrations

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/idempotency"
	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func notFoundErr(kind, id string) error {
	return restroute.NewError(http.StatusNotFound, "ResourceNotFoundException", "%s %q does not exist", kind, id)
}

func duplicateErr(kind, id string) error {
	return restroute.NewError(http.StatusConflict, "DuplicateResourceException", "%s %q already exists", kind, id)
}

func conflictErr(format string, args ...any) error {
	return restroute.NewError(http.StatusConflict, "ConflictException", format, args...)
}

func invalidErr(format string, args ...any) error {
	return restroute.NewError(http.StatusBadRequest, "InvalidRequestException", format, args...)
}

func quotaErr(format string, args ...any) error {
	return restroute.NewError(http.StatusTooManyRequests, "ResourceQuotaExceededException", format, args...)
}

func idempotencyErr(err error) error {
	if errors.Is(err, idempotency.ErrParamsMismatch) {
		return invalidErr("%v", err)
	}

	return err
}
