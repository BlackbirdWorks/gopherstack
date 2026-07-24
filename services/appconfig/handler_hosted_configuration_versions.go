package appconfig

import (
	"errors"
	"io"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleCreateHostedConfigurationVersion(
	c *echo.Context,
	applicationID, profileID string,
) error {
	contentType := c.Request().Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}

	// AWS AppConfig accepts description and version label via custom request headers.
	description := c.Request().Header.Get("Description")
	versionLabel := c.Request().Header.Get("Versionlabel")

	// Optional optimistic-concurrency check, bound to the
	// "Latest-Version-Number" request header.
	var latestVersionNumber *int32
	if s := c.Request().Header.Get("Latest-Version-Number"); s != "" {
		if n, parseErr := strconv.ParseInt(s, 10, 32); parseErr == nil {
			v := int32(n)
			latestVersionNumber = &v
		}
	}

	content, err := io.ReadAll(http.MaxBytesReader(c.Response(), c.Request().Body, maxHostedConfigurationVersionBytes))
	if err != nil {
		var maxBytesErr *http.MaxBytesError
		if errors.As(err, &maxBytesErr) {
			return c.JSON(
				http.StatusRequestEntityTooLarge,
				map[string]string{keyMessageField: "configuration content exceeds maximum size of 1 MiB"},
			)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: "failed to read request body"},
		)
	}

	v, err := h.Backend.CreateHostedConfigurationVersion(
		applicationID,
		profileID,
		contentType,
		description,
		versionLabel,
		content,
		latestVersionNumber,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	setHostedConfigurationVersionHeaders(c, v)

	return c.Blob(http.StatusCreated, v.ContentType, v.Content)
}

// setHostedConfigurationVersionHeaders sets the response headers the real
// AWS AppConfig REST API returns for CreateHostedConfigurationVersion and
// GetHostedConfigurationVersion. Both operations carry the raw configuration
// content as the httpPayload response body, with every other field --
// including the version number -- bound to a response header (see the
// awsRestjson1_deserializeOpHttpBindingsGetHostedConfigurationVersionOutput /
// ...CreateHostedConfigurationVersionOutput http bindings in
// aws-sdk-go-v2/service/appconfig/deserializers.go): Application-Id,
// Configuration-Profile-Id, Content-Type, Description, VersionLabel, and
// Version-Number. Previously this handler set a fabricated
// "Appconfig-Configuration-Version" header instead of "Version-Number" and
// omitted the rest, so a real SDK client's VersionNumber/ApplicationId/
// ConfigurationProfileId/Description/VersionLabel output fields always came
// back zero-valued.
func setHostedConfigurationVersionHeaders(c *echo.Context, v *HostedConfigurationVersion) {
	h := c.Response().Header()
	h.Set("Application-Id", v.ApplicationID)
	h.Set("Configuration-Profile-Id", v.ConfigurationProfileID)
	h.Set("Content-Type", v.ContentType)

	if v.Description != "" {
		h.Set("Description", v.Description)
	}

	if v.VersionLabel != "" {
		h.Set("Versionlabel", v.VersionLabel)
	}

	h.Set("Version-Number", strconv.Itoa(int(v.VersionNumber)))
}

func (h *Handler) handleGetHostedConfigurationVersion(
	c *echo.Context,
	applicationID, profileID string,
	versionNumber int32,
) error {
	v, err := h.Backend.GetHostedConfigurationVersion(applicationID, profileID, versionNumber)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	setHostedConfigurationVersionHeaders(c, v)

	return c.Blob(http.StatusOK, v.ContentType, v.Content)
}

func (h *Handler) handleListHostedConfigurationVersions(
	c *echo.Context,
	applicationID, profileID string,
) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	versionLabel := c.Request().URL.Query().Get("version_label")
	versions, outToken, err := h.Backend.ListHostedConfigurationVersions(
		applicationID,
		profileID,
		nextToken,
		versionLabel,
		maxResults,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	resp := map[string]any{keyItems: versions}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleDeleteHostedConfigurationVersion(
	c *echo.Context,
	applicationID, profileID string,
	versionNumber int32,
) error {
	if err := h.Backend.DeleteHostedConfigurationVersion(applicationID, profileID, versionNumber); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.NoContent(http.StatusNoContent)
}
