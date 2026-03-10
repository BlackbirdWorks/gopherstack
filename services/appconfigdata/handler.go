package appconfigdata

import (
	"encoding/json"
	"errors"
	"net/http"

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
	nextPollTokenHeader          = "Next-Poll-Configuration-Token" //nolint:gosec // G101: header name, not credentials
	nextPollIntervalHeader       = "Next-Poll-Interval-In-Seconds"
)

// Handler is the Echo HTTP handler for AppConfigData operations.
type Handler struct {
	Backend StorageBackend
}

// NewHandler creates a new AppConfigData Handler.
func NewHandler(backend StorageBackend) *Handler {
	return &Handler{Backend: backend}
}

// Name returns the service name.
func (h *Handler) Name() string { return "AppConfigData" }

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

// ExtractResource extracts the configuration token from the query string, if any.
func (h *Handler) ExtractResource(c *echo.Context) string {
	return c.Request().URL.Query().Get(configurationTokenQueryParam)
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

			return c.JSON(http.StatusNotFound, map[string]string{"message": "not found"})
		}
	}
}

func (h *Handler) handleStartConfigurationSession(c *echo.Context) error {
	log := logger.Load(c.Request().Context())

	var req startSessionRequest
	if err := json.NewDecoder(c.Request().Body).Decode(&req); err != nil {
		log.Error("appconfigdata: failed to decode StartConfigurationSession request", "error", err)

		return c.JSON(http.StatusBadRequest, map[string]string{"message": "invalid request body"})
	}

	if req.ApplicationIdentifier == "" || req.EnvironmentIdentifier == "" || req.ConfigurationProfileIdentifier == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{
			"message": "ApplicationIdentifier, EnvironmentIdentifier, and ConfigurationProfileIdentifier are required",
		})
	}

	token, err := h.Backend.StartSession(
		req.ApplicationIdentifier,
		req.EnvironmentIdentifier,
		req.ConfigurationProfileIdentifier,
	)
	if err != nil {
		log.Error("appconfigdata: StartConfigurationSession failed", "error", err)

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	return c.JSON(http.StatusCreated, startSessionResponse{InitialConfigurationToken: token})
}

func (h *Handler) handleGetLatestConfiguration(c *echo.Context, token string) error {
	log := logger.Load(c.Request().Context())

	if token == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"message": "configuration token is required"})
	}

	content, contentType, nextToken, err := h.Backend.GetLatestConfiguration(token)
	if err != nil {
		log.Error("appconfigdata: GetLatestConfiguration failed", "token", token, "error", err)

		if errors.Is(err, ErrSessionNotFound) {
			return c.JSON(http.StatusBadRequest, map[string]string{"message": err.Error()})
		}

		return c.JSON(http.StatusInternalServerError, map[string]string{"message": err.Error()})
	}

	c.Response().Header().Set(nextPollTokenHeader, nextToken)
	c.Response().Header().Set(nextPollIntervalHeader, "30")
	c.Response().Header().Set("Content-Type", contentType)

	if len(content) == 0 {
		return c.NoContent(http.StatusNoContent)
	}

	return c.Blob(http.StatusOK, contentType, content)
}
