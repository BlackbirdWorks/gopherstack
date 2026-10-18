package s3vectors

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func notFound(format string, args ...any) error {
	return restroute.NewError(http.StatusNotFound, "NotFoundException", format, args...)
}

func conflict(format string, args ...any) error { return restroute.Conflict(format, args...) }

func invalid(format string, args ...any) error { return restroute.Validation(format, args...) }

func quotaExceeded(format string, args ...any) error { return restroute.QuotaExceeded(format, args...) }
