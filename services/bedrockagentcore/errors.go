package bedrockagentcore

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

func invalid(format string, args ...any) *restroute.Error {
	return restroute.Validation(format, args...)
}

func notFound(format string, args ...any) *restroute.Error {
	return restroute.NotFound(format, args...)
}

func unauthorized(format string, args ...any) *restroute.Error {
	return restroute.NewError(http.StatusUnauthorized, "UnauthorizedException", format, args...)
}

func internal(format string, args ...any) *restroute.Error {
	return restroute.NewError(http.StatusInternalServerError, "InternalServerException", format, args...)
}

func runtimeClient(format string, args ...any) *restroute.Error {
	return restroute.NewError(http.StatusFailedDependency, "RuntimeClientError", format, args...)
}

func retryableConflict(format string, args ...any) *restroute.Error {
	return restroute.NewError(http.StatusConflict, "RetryableConflictException", format, args...)
}
