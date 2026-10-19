package amp

import (
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
)

const (
	resWorkspace    = "workspace"
	resNamespace    = "rulegroupsnamespace"
	resScraper      = "scraper"
	resDetector     = "anomalydetector"
	resAlertManager = "alertmanagerdefinition"
	resLogging      = "loggingconfiguration"
	resQueryLogging = "queryloggingconfiguration"
	resPolicy       = "resourcepolicy"
)

func notFound(resType, id, format string, args ...any) error {
	return restroute.NewError(http.StatusNotFound, "ResourceNotFoundException", format, args...).
		With("resourceId", id).With("resourceType", resType)
}

func conflict(resType, id, format string, args ...any) error {
	return restroute.Conflict(format, args...).With("resourceId", id).With("resourceType", resType)
}

func invalid(format string, args ...any) error {
	return restroute.Validation(format, args...).With("reason", "FIELD_VALIDATION_FAILED")
}

func cannotParse(format string, args ...any) error {
	return restroute.Validation(format, args...).With("reason", "CANNOT_PARSE")
}
