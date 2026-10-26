package amp

import (
	"errors"
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

func quotaExceeded(resType, format string, args ...any) error {
	return restroute.QuotaExceeded(format, args...).With("resourceType", resType).With("serviceCode", serviceName)
}

func invalid(format string, args ...any) error {
	return restroute.Validation(format, args...).With("reason", "FIELD_VALIDATION_FAILED")
}

func cannotParse(format string, args ...any) error {
	return restroute.Validation(format, args...).With("reason", "CANNOT_PARSE")
}

var (
	errBadRuleDuration  = errors.New("invalid duration in rule group")
	errTooManyRules     = errors.New("too many rule groups or rules")
	errTemplateTooLarge = errors.New("template output too large")
)

var errNoPublisher = errors.New("no SNS publisher is wired")
