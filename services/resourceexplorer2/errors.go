package resourceexplorer2

import (
	"errors"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func notFound(format string, args ...any) error { return restroute.NotFound(format, args...) }

func conflict(format string, args ...any) error { return restroute.Conflict(format, args...) }

func invalid(format string, args ...any) error { return restroute.Validation(format, args...) }

func quotaExceeded(format string, args ...any) error { return restroute.QuotaExceeded(format, args...) }

func unauthorized(format string, args ...any) error {
	return restroute.NewError(http.StatusUnauthorized, "UnauthorizedException", format, args...)
}

func asAPIError(err error) (*restroute.Error, bool) { return errors.AsType[*restroute.Error](err) }

func isConflict(err error) bool {
	e, ok := asAPIError(err)

	return ok && e.Code == "ConflictException"
}
