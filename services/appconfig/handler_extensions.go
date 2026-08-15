package appconfig

import (
	"errors"
	"math"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleCreateExtension(c *echo.Context) error {
	var req struct {
		Actions     map[string][]ExtensionAction  `json:"Actions"`
		Parameters  map[string]ExtensionParameter `json:"Parameters"`
		Tags        map[string]string             `json:"Tags"`
		Name        string                        `json:"Name"`
		Description string                        `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	ext, err := h.Backend.CreateExtension(req.Name, req.Description, req.Actions, req.Parameters, req.Tags)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, ext)
}

func (h *Handler) handleGetExtension(c *echo.Context, extensionID string) error {
	// Real GetExtensionInput binds VersionNumber as the "version_number"
	// query param.
	versionNumber := parseAppConfigQueryVersion(c, "version_number")

	ext, err := h.Backend.GetExtension(extensionID, versionNumber)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, ext)
}

func (h *Handler) handleListExtensions(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	nameFilter := c.Request().URL.Query().Get("name")
	exts, outToken := h.Backend.ListExtensions(nextToken, maxResults, nameFilter)

	summaries := make([]ExtensionSummary, 0, len(exts))
	for _, ext := range exts {
		summaries = append(summaries, extensionToSummary(ext))
	}

	resp := map[string]any{keyItems: summaries}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

// parseAppConfigQueryVersion parses an optional positive int32 version
// number from the named query param, returning 0 (unspecified) if absent
// or invalid.
func parseAppConfigQueryVersion(c *echo.Context, name string) int32 {
	s := c.Request().URL.Query().Get(name)
	if s == "" {
		return 0
	}

	n, err := strconv.ParseInt(s, 10, 32)
	if err != nil || n <= 0 || n > math.MaxInt32 {
		return 0
	}

	return int32(n)
}

func (h *Handler) handleUpdateExtension(c *echo.Context, extensionID string) error {
	var req struct {
		Actions     map[string][]ExtensionAction  `json:"Actions"`
		Parameters  map[string]ExtensionParameter `json:"Parameters"`
		Description *string                       `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	ext, err := h.Backend.UpdateExtension(extensionID, req.Description, req.Actions, req.Parameters)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, ext)
}

func (h *Handler) handleDeleteExtension(c *echo.Context, extensionID string) error {
	// Real DeleteExtensionInput binds VersionNumber as the "version" query
	// param.
	versionNumber := parseAppConfigQueryVersion(c, "version")

	if err := h.Backend.DeleteExtension(extensionID, versionNumber); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		// DeleteExtension models only BadRequestException, InternalServerException
		// and ResourceNotFoundException (appconfig@v1.48.4 deserializers.go:2369) --
		// no ConflictException, so "still associated" maps to BadRequestException here.
		if errors.Is(err, awserr.ErrAlreadyExists) {
			return badRequestResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleCreateExtensionAssociation(c *echo.Context) error {
	var req struct {
		Parameters             map[string]string `json:"Parameters"`
		ExtensionVersionNumber *int32            `json:"ExtensionVersionNumber"`
		Tags                   map[string]string `json:"Tags"`
		ExtensionIdentifier    string            `json:"ExtensionIdentifier"`
		ResourceIdentifier     string            `json:"ResourceIdentifier"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	assoc, err := h.Backend.CreateExtensionAssociation(
		req.ExtensionIdentifier,
		req.ResourceIdentifier,
		req.Parameters,
		req.ExtensionVersionNumber,
		req.Tags,
	)
	if err != nil {
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, assoc)
}

func (h *Handler) handleGetExtensionAssociation(
	c *echo.Context,
	extensionAssociationID string,
) error {
	assoc, err := h.Backend.GetExtensionAssociation(extensionAssociationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *Handler) handleListExtensionAssociations(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	q := c.Request().URL.Query()
	extIdentifier := q.Get("extension_identifier")
	resourceIdentifier := q.Get("resource_identifier")
	assocs, outToken := h.Backend.ListExtensionAssociations(
		nextToken,
		extIdentifier,
		resourceIdentifier,
		maxResults,
	)

	summaries := make([]ExtensionAssociationSummary, 0, len(assocs))
	for _, a := range assocs {
		summaries = append(summaries, extensionAssociationToSummary(a))
	}

	resp := map[string]any{keyItems: summaries}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateExtensionAssociation(
	c *echo.Context,
	extensionAssociationID string,
) error {
	var req struct {
		Parameters map[string]string `json:"Parameters"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	assoc, err := h.Backend.UpdateExtensionAssociation(extensionAssociationID, req.Parameters)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, assoc)
}

func (h *Handler) handleDeleteExtensionAssociation(
	c *echo.Context,
	extensionAssociationID string,
) error {
	if err := h.Backend.DeleteExtensionAssociation(extensionAssociationID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
