package rum

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	resAppMonitor = "AppMonitor"
)

func notFound(name, format string, args ...any) error {
	return restroute.NewError(http.StatusNotFound, "ResourceNotFoundException", format, args...).
		With("resourceName", name).With("resourceType", resAppMonitor)
}

func conflict(resType, name, format string, args ...any) error {
	return restroute.Conflict(format, args...).With("resourceName", name).With("resourceType", resType)
}

func invalid(format string, args ...any) error { return restroute.Validation(format, args...) }

func malformedPolicy(format string, args ...any) error {
	return restroute.NewError(http.StatusBadRequest, "MalformedPolicyDocumentException", format, args...)
}

func policySizeExceeded(format string, args ...any) error {
	return restroute.NewError(http.StatusBadRequest, "PolicySizeLimitExceededException", format, args...)
}

func policyNotFound(format string, args ...any) error {
	return restroute.NewError(http.StatusNotFound, "PolicyNotFoundException", format, args...)
}

func invalidRevision(format string, args ...any) error {
	return restroute.NewError(http.StatusBadRequest, "InvalidPolicyRevisionIdException", format, args...)
}

func internalError(format string, args ...any) error {
	return restroute.NewError(http.StatusInternalServerError, "InternalServerException", format, args...)
}
