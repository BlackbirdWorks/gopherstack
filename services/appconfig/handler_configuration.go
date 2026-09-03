package appconfig

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleGetConfiguration(
	c *echo.Context,
	application, environment, configuration string,
) error {
	configVersion, err := h.Backend.GetConfiguration(application, environment, configuration)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	if configVersion.VersionNumber > 0 {
		c.Response().
			Header().
			Set("Configuration-Version", strconv.Itoa(int(configVersion.VersionNumber)))
	}

	// Real GetConfigurationInput binds ClientConfigurationVersion as the
	// "client_configuration_version" query param: when it matches the
	// current version, AWS returns 204 with empty Content instead of
	// resending unchanged data (api_op_GetConfiguration.go:101-104).
	clientVersion := c.Request().URL.Query().Get("client_configuration_version")
	if clientVersion != "" && clientVersion == strconv.Itoa(int(configVersion.VersionNumber)) {
		return c.NoContent(http.StatusNoContent)
	}

	if len(configVersion.Content) == 0 {
		return c.NoContent(http.StatusNoContent)
	}

	contentType := configVersion.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	return c.Blob(http.StatusOK, contentType, configVersion.Content)
}

func (h *Handler) handleValidateConfiguration(
	c *echo.Context,
	applicationID, profileID string,
) error {
	configVersion := c.Request().URL.Query().Get("configuration_version")

	if err := h.Backend.ValidateConfiguration(applicationID, profileID, configVersion); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
