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
	keyMessageField = "message"
)

const (
	appConfigDataMatchPriority   = 86
	configurationsessionsPath    = "/configurationsessions"
	configurationPath            = "/configuration"
	configurationTokenQueryParam = "configuration_token"
	defaultPollIntervalInSeconds = 30
	nextPollTokenHeader          = "Next-Poll-Configuration-Token" //nolint:gosec // G101: header name, not credentials
	nextPollIntervalHeader       = "Next-Poll-Interval-In-Seconds"
	etagHeader                   = "ETag"
	versionLabelHeader           = "X-Amzn-AppConfig-Version-Label"
	retryAfterHeader             = "Retry-After"
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

			return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: "not found"})
		}
	}
}

func (h *Handler) handleStartConfigurationSession(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	var req startSessionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		log.Error("appconfigdata: failed to decode StartConfigurationSession request", "error", err)

		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: "invalid request body"},
		)
	}

	req.ApplicationIdentifier = strings.TrimSpace(req.ApplicationIdentifier)
	req.EnvironmentIdentifier = strings.TrimSpace(req.EnvironmentIdentifier)
	req.ConfigurationProfileIdentifier = strings.TrimSpace(req.ConfigurationProfileIdentifier)

	if req.ApplicationIdentifier == "" || req.EnvironmentIdentifier == "" ||
		req.ConfigurationProfileIdentifier == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyMessageField: "ApplicationIdentifier, EnvironmentIdentifier, and ConfigurationProfileIdentifier are required",
		})
	}

	if req.RequiredMinimumPollIntervalInSeconds != 0 &&
		req.RequiredMinimumPollIntervalInSeconds < minPollIntervalSeconds {
		return c.JSON(http.StatusBadRequest, map[string]string{
			keyMessageField: fmt.Sprintf(
				"RequiredMinimumPollIntervalInSeconds must be 0 or >= %d",
				minPollIntervalSeconds,
			),
		})
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
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
		case errors.Is(err, ErrNoActiveDeployment):
			return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: err.Error()})
		default:
			return c.JSON(
				http.StatusInternalServerError,
				map[string]string{keyMessageField: err.Error()},
			)
		}
	}

	return c.JSON(http.StatusCreated, startSessionResponse{InitialConfigurationToken: token})
}

func (h *Handler) handleGetLatestConfiguration(c *echo.Context, token string) error {
	log := logger.Load(c.Request().Context())

	if token == "" {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: "configuration token is required"},
		)
	}

	content, contentType, nextToken, hash, versionLabel, err := h.Backend.GetLatestConfiguration(token)
	if err != nil {
		const redactLen = 8
		redacted := token
		if len(token) > redactLen {
			redacted = token[:redactLen] + "..."
		}
		log.Error(
			"appconfigdata: GetLatestConfiguration failed",
			"token_prefix",
			redacted,
			"error",
			err,
		)

		switch {
		case errors.Is(err, ErrTokenExpired):
			return c.JSON(http.StatusUnauthorized, map[string]string{keyMessageField: err.Error()})
		case errors.Is(err, ErrSessionNotFound):
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
		case errors.Is(err, ErrPollTooFrequent):
			return c.JSON(http.StatusBadRequest, map[string]string{keyMessageField: err.Error()})
		case errors.Is(err, ErrResourceRemoved):
			return c.JSON(http.StatusNotFound, map[string]string{keyMessageField: err.Error()})
		default:
			return c.JSON(
				http.StatusInternalServerError,
				map[string]string{keyMessageField: err.Error()},
			)
		}
	}

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
