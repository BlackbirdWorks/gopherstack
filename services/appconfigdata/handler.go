package appconfigdata

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

const (
	appConfigDataMatchPriority   = 86
	configurationsessionsPath    = "/configurationsessions"
	configurationPath            = "/configuration"
	configurationTokenQueryParam = "configuration_token"
	defaultPollIntervalInSeconds = 30
	// configurationTokenParam is the parameter name used in structured error Details.
	configurationTokenParam = "ConfigurationToken"

	// Response headers defined by the AWS AppConfigData REST-JSON protocol.
	nextPollTokenHeader    = "Next-Poll-Configuration-Token" //nolint:gosec // G101: header name, not a credential
	nextPollIntervalHeader = "Next-Poll-Interval-In-Seconds"
	etagHeader             = "ETag"
	// versionLabelHeader is the AWS-defined response header for the AppConfig version label.
	// The AWS SDK v2 deserializer reads this exact header name; the older X-Amzn-AppConfig-*
	// prefix used in early docs was never the actual protocol header.
	versionLabelHeader = "Version-Label"
	retryAfterHeader   = "Retry-After"
	// errorTypeHeader is read by the AWS SDK to identify the exception type before parsing the body.
	errorTypeHeader = "X-Amzn-ErrorType"
)

// Handler is the Echo HTTP handler for AppConfigData operations.
type Handler struct {
	Backend *InMemoryBackend
}

// NewHandler creates a new AppConfigData Handler.
func NewHandler(backend *InMemoryBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppConfigData" }

// StartWorker starts the background janitor for AppConfig Data retrieval sessions.
func (h *Handler) StartWorker(ctx context.Context) error {
	janitor := NewJanitor(h.Backend)
	go janitor.Run(ctx)

	return nil
}

// GetSupportedOperations returns the list of supported operations.
func (h *Handler) GetSupportedOperations() []string {
	return []string{"StartConfigurationSession", "GetLatestConfiguration"}
}

// ChaosServiceName returns the lowercase AWS service name for fault rule matching.
func (h *Handler) ChaosServiceName() string { return "appconfigdata" }

// ChaosOperations returns all operations that can be fault-injected.
func (h *Handler) ChaosOperations() []string { return h.GetSupportedOperations() }

// ChaosRegions returns all regions this handler instance handles.
func (h *Handler) ChaosRegions() []string { return []string{config.DefaultRegion} }

// RouteMatcher returns a function matching AppConfigData requests.
func (h *Handler) RouteMatcher() service.Matcher {
	return func(c *echo.Context) bool {
		path := c.Request().URL.Path

		return path == configurationsessionsPath || path == configurationPath
	}
}

// MatchPriority returns the routing priority.
func (h *Handler) MatchPriority() int { return appConfigDataMatchPriority }

// ExtractOperation returns the operation name based on the request path.
func (h *Handler) ExtractOperation(c *echo.Context) string {
	path := c.Request().URL.Path
	switch {
	case path == configurationsessionsPath && c.Request().Method == http.MethodPost:
		return "StartConfigurationSession"
	case path == configurationPath && c.Request().Method == http.MethodGet:
		return "GetLatestConfiguration"
	default:
		return "Unknown"
	}
}

// ExtractResource returns a stable, non-sensitive resource label for telemetry.
// For GetLatestConfiguration, it resolves the session to return app/env/profile;
// for other operations it returns a fixed label to avoid high-cardinality metrics.
func (h *Handler) ExtractResource(c *echo.Context) string {
	if c.Request().URL.Path == configurationPath && c.Request().Method == http.MethodGet {
		token := c.Request().URL.Query().Get(configurationTokenQueryParam)
		if sess := h.Backend.LookupSession(token); sess != nil {
			return sess.ApplicationIdentifier + "/" + sess.EnvironmentIdentifier + "/" + sess.ConfigurationProfileIdentifier
		}

		return "unknown-session"
	}

	return "configurationsession"
}

// Handler returns the Echo handler function for AppConfigData operations.
func (h *Handler) Handler() echo.HandlerFunc {
	return func(c *echo.Context) error {
		log := logger.Load(c.Request().Context())
		path := c.Request().URL.Path

		switch {
		case path == configurationsessionsPath && c.Request().Method == http.MethodPost:
			return h.handleStartConfigurationSession(c)
		case path == configurationPath && c.Request().Method == http.MethodGet:
			token := c.Request().URL.Query().Get(configurationTokenQueryParam)

			return h.handleGetLatestConfiguration(c, token)
		default:
			log.Warn("appconfigdata: unmatched request", "path", path, "method", c.Request().Method)

			return writeAWSError(c, http.StatusNotFound, exceptionResourceNotFound, "not found")
		}
	}
}

func (h *Handler) handleStartConfigurationSession(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	var req startSessionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		log.Error("appconfigdata: failed to decode StartConfigurationSession request", "error", err)

		return writeAWSError(c, http.StatusBadRequest, exceptionBadRequest, "invalid request body")
	}

	req.ApplicationIdentifier = strings.TrimSpace(req.ApplicationIdentifier)
	req.EnvironmentIdentifier = strings.TrimSpace(req.EnvironmentIdentifier)
	req.ConfigurationProfileIdentifier = strings.TrimSpace(req.ConfigurationProfileIdentifier)

	if req.ApplicationIdentifier == "" || req.EnvironmentIdentifier == "" ||
		req.ConfigurationProfileIdentifier == "" {
		return writeBadRequestWithInvalidParams(c,
			"ApplicationIdentifier, EnvironmentIdentifier, and ConfigurationProfileIdentifier are required",
			buildMissingIdentifierParams(req),
		)
	}

	if err := validateIdentifierLength("ApplicationIdentifier", req.ApplicationIdentifier); err != nil {
		return writeBadRequestWithInvalidParams(c, err.Error(),
			map[string]invalidParamProblem{"ApplicationIdentifier": {Problem: invalidParamProblemCorrupted}},
		)
	}

	if err := validateIdentifierLength("EnvironmentIdentifier", req.EnvironmentIdentifier); err != nil {
		return writeBadRequestWithInvalidParams(c, err.Error(),
			map[string]invalidParamProblem{"EnvironmentIdentifier": {Problem: invalidParamProblemCorrupted}},
		)
	}

	if err := validateIdentifierLength(
		"ConfigurationProfileIdentifier",
		req.ConfigurationProfileIdentifier,
	); err != nil {
		return writeBadRequestWithInvalidParams(c, err.Error(),
			map[string]invalidParamProblem{
				"ConfigurationProfileIdentifier": {Problem: invalidParamProblemCorrupted},
			},
		)
	}

	if req.RequiredMinimumPollIntervalInSeconds != 0 &&
		(req.RequiredMinimumPollIntervalInSeconds < minPollIntervalSeconds ||
			req.RequiredMinimumPollIntervalInSeconds > maxPollIntervalSeconds) {
		return writeBadRequestWithInvalidParams(c,
			fmt.Sprintf(
				"RequiredMinimumPollIntervalInSeconds must be 0 or between %d and %d",
				minPollIntervalSeconds, maxPollIntervalSeconds,
			),
			map[string]invalidParamProblem{
				"RequiredMinimumPollIntervalInSeconds": {Problem: invalidParamProblemCorrupted},
			},
		)
	}

	token, err := h.Backend.StartSession(
		req.ApplicationIdentifier,
		req.EnvironmentIdentifier,
		req.ConfigurationProfileIdentifier,
		req.RequiredMinimumPollIntervalInSeconds,
	)
	if err != nil {
		log.Error("appconfigdata: StartConfigurationSession failed", "error", err)

		switch {
		case errors.Is(err, ErrInvalidPollInterval):
			return writeBadRequestWithInvalidParams(c, err.Error(),
				map[string]invalidParamProblem{
					"RequiredMinimumPollIntervalInSeconds": {Problem: invalidParamProblemCorrupted},
				},
			)
		case errors.Is(err, ErrNoActiveDeployment):
			return writeResourceNotFound(c,
				"No deployment exists for the given application, environment, and configuration profile.",
				resourceTypeDeployment,
				map[string]string{
					"ApplicationIdentifier":          req.ApplicationIdentifier,
					"EnvironmentIdentifier":          req.EnvironmentIdentifier,
					"ConfigurationProfileIdentifier": req.ConfigurationProfileIdentifier,
				},
			)
		default:
			return writeAWSError(c, http.StatusInternalServerError, exceptionInternalServer, err.Error())
		}
	}

	return c.JSON(http.StatusCreated, startSessionResponse{InitialConfigurationToken: token})
}

func (h *Handler) handleGetLatestConfiguration(c *echo.Context, token string) error {
	log := logger.Load(c.Request().Context())

	if token == "" {
		return writeBadRequestWithInvalidParams(c,
			"ConfigurationToken is required",
			map[string]invalidParamProblem{
				configurationTokenParam: {Problem: invalidParamProblemCorrupted},
			},
		)
	}

	content, contentType, nextToken, hash, versionLabel, err := h.Backend.GetLatestConfiguration(token)
	if err != nil {
		const redactLen = 8
		redacted := token
		if len(token) > redactLen {
			redacted = token[:redactLen] + "..."
		}

		log.Error("appconfigdata: GetLatestConfiguration failed",
			"token_prefix", redacted, "error", err)

		return h.handleGetLatestConfigurationError(c, token, err)
	}

	return h.writeGetLatestConfigurationResponse(c, nextToken, hash, versionLabel, contentType, content)
}

// handleGetLatestConfigurationError maps backend errors to AWS-shaped HTTP responses.
func (h *Handler) handleGetLatestConfigurationError(c *echo.Context, token string, err error) error {
	switch {
	case errors.Is(err, ErrTokenExpired):
		// AWS returns BadRequestException (400) for expired tokens, not 401.
		return writeBadRequestWithInvalidParams(c,
			"The configuration token is expired. Please close the current session and open a new one.",
			map[string]invalidParamProblem{
				configurationTokenParam: {Problem: invalidParamProblemExpired},
			},
		)
	case errors.Is(err, ErrTokenCorrupted):
		return writeBadRequestWithInvalidParams(c,
			"The configuration token is corrupted.",
			map[string]invalidParamProblem{
				configurationTokenParam: {Problem: invalidParamProblemCorrupted},
			},
		)
	case errors.Is(err, ErrSessionNotFound):
		return writeBadRequestWithInvalidParams(c,
			"The configuration token is invalid or has already been used.",
			map[string]invalidParamProblem{
				configurationTokenParam: {Problem: invalidParamProblemCorrupted},
			},
		)
	case errors.Is(err, ErrPollTooFrequent):
		// Set Retry-After to the session's required interval so the client knows when to retry.
		if sess := h.Backend.LookupSession(token); sess != nil && sess.PollIntervalInSeconds > 0 {
			c.Response().Header().Set(retryAfterHeader, strconv.Itoa(sess.PollIntervalInSeconds))
		} else {
			c.Response().Header().Set(retryAfterHeader, strconv.Itoa(defaultPollIntervalInSeconds))
		}

		return writeBadRequestWithInvalidParams(c,
			"Request was made before the required polling interval has elapsed. "+
				"Check the Next-Poll-Interval-In-Seconds response header from your previous call.",
			map[string]invalidParamProblem{
				configurationTokenParam: {Problem: invalidParamProblemPollIntervalNotSatisfied},
			},
		)
	case errors.Is(err, ErrResourceRemoved):
		return writeResourceNotFound(c,
			"The application, environment, or configuration profile referenced by this session no longer exists.",
			resourceTypeDeployment,
			nil,
		)
	default:
		return writeAWSError(c, http.StatusInternalServerError, exceptionInternalServer, err.Error())
	}
}

// writeGetLatestConfigurationResponse sends a 200 or 204 response for a successful poll.
func (h *Handler) writeGetLatestConfigurationResponse(
	c *echo.Context,
	nextToken, hash, versionLabel, contentType string,
	content []byte,
) error {
	// Honor the client's requested minimum poll interval; use the larger of the two.
	pollInterval := defaultPollIntervalInSeconds
	if sess := h.Backend.LookupSession(nextToken); sess != nil &&
		sess.PollIntervalInSeconds > pollInterval {
		pollInterval = sess.PollIntervalInSeconds
	}

	// Always set poll-control headers regardless of whether content changed.
	c.Response().Header().Set(nextPollTokenHeader, nextToken)
	c.Response().Header().Set(nextPollIntervalHeader, strconv.Itoa(pollInterval))

	if versionLabel != "" {
		c.Response().Header().Set(versionLabelHeader, versionLabel)
	}

	if len(content) == 0 {
		// 204 No Content: configuration unchanged since last poll.
		// Must NOT set ETag or Content-Type — the body is empty and there is nothing to describe.
		return c.NoContent(http.StatusNoContent)
	}

	// 200 OK: content changed or first poll.
	if hash != "" {
		c.Response().Header().Set(etagHeader, fmt.Sprintf(`"%s"`, hash))
	}

	c.Response().Header().Set("Content-Length", strconv.Itoa(len(content)))

	return c.Blob(http.StatusOK, contentType, content)
}

// writeAWSError writes a standard AWS REST-JSON error response with the X-Amzn-ErrorType header.
func writeAWSError(c *echo.Context, status int, exceptionType, message string) error {
	c.Response().Header().Set(errorTypeHeader, exceptionType)

	return c.JSON(status, awsErrorBody{
		Type:    exceptionType,
		Message: message,
	})
}

// writeBadRequestWithInvalidParams writes a BadRequestException with structured parameter details.
// AWS clients use the Reason and Details fields to identify which parameter failed and why.
func writeBadRequestWithInvalidParams(
	c *echo.Context,
	message string,
	params map[string]invalidParamProblem,
) error {
	c.Response().Header().Set(errorTypeHeader, exceptionBadRequest)

	body := awsBadRequestBody{
		Type:    exceptionBadRequest,
		Message: message,
	}

	if len(params) > 0 {
		body.Reason = badRequestReasonInvalidParameters
		body.Details = &invalidParamsDetail{InvalidParameters: params}
	}

	return c.JSON(http.StatusBadRequest, body)
}

// writeResourceNotFound writes a ResourceNotFoundException with type and referencing identifiers.
func writeResourceNotFound(
	c *echo.Context,
	message string,
	resourceType string,
	referencedBy map[string]string,
) error {
	c.Response().Header().Set(errorTypeHeader, exceptionResourceNotFound)

	return c.JSON(http.StatusNotFound, awsResourceNotFoundBody{
		Type:         exceptionResourceNotFound,
		Message:      message,
		ResourceType: resourceType,
		ReferencedBy: referencedBy,
	})
}

// validateIdentifierLength returns ErrIdentifierTooLong when an identifier exceeds maxIdentifierLength.
func validateIdentifierLength(_, value string) error {
	if len(value) > maxIdentifierLength {
		return ErrIdentifierTooLong
	}

	return nil
}

// buildMissingIdentifierParams constructs an InvalidParameters detail map for missing required identifiers.
func buildMissingIdentifierParams(req startSessionRequest) map[string]invalidParamProblem {
	params := make(map[string]invalidParamProblem)

	if req.ApplicationIdentifier == "" {
		params["ApplicationIdentifier"] = invalidParamProblem{Problem: invalidParamProblemCorrupted}
	}

	if req.EnvironmentIdentifier == "" {
		params["EnvironmentIdentifier"] = invalidParamProblem{Problem: invalidParamProblemCorrupted}
	}

	if req.ConfigurationProfileIdentifier == "" {
		params["ConfigurationProfileIdentifier"] = invalidParamProblem{Problem: invalidParamProblemCorrupted}
	}

	return params
}
