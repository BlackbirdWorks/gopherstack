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

// authError carries the WWW-Authenticate challenge of a rejected runtime request.
type authError struct {
	inner     *restroute.Error
	challenge string
}

func (e *authError) Error() string { return e.inner.Error() }

func (e *authError) Unwrap() error { return e.inner }

func authRejected(f *AuthFailure) error {
	code := "UnauthorizedException"
	if f.Status == http.StatusForbidden {
		code = "AccessDeniedException"
	}

	return &authError{inner: restroute.NewError(f.Status, code, "%s", f.Message), challenge: f.Challenge}
}

func runtimeClient(format string, args ...any) *restroute.Error {
	return restroute.NewError(http.StatusFailedDependency, "RuntimeClientError", format, args...)
}

func retryableConflict(format string, args ...any) *restroute.Error {
	return restroute.NewError(http.StatusConflict, "RetryableConflictException", format, args...)
}
