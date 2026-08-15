package appconfig

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleCreateApplication(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		Name        string            `json:"Name"`
		Description string            `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	app, err := h.Backend.CreateApplication(req.Name, req.Description, req.Tags)
	if err != nil {
		// CreateApplication models only BadRequestException, InternalServerException
		// and ServiceQuotaExceededException (appconfig@v1.48.4 deserializers.go:87) --
		// no ConflictException, so a name collision maps to BadRequestException here.
		if errors.Is(err, awserr.ErrInvalidParameter) || errors.Is(err, awserr.ErrAlreadyExists) {
			return badRequestResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusCreated, applicationToOutput(*app))
}

func (h *Handler) handleGetApplication(c *echo.Context, applicationID string) error {
	app, err := h.Backend.GetApplication(applicationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, applicationToOutput(*app))
}

func (h *Handler) handleListApplications(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	apps, outToken := h.Backend.ListApplications(nextToken, maxResults)

	items := make([]applicationOutput, 0, len(apps))
	for _, app := range apps {
		items = append(items, applicationToOutput(app))
	}

	resp := map[string]any{keyItems: items}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateApplication(c *echo.Context, applicationID string) error {
	var req struct {
		Name        *string `json:"Name"`
		Description *string `json:"Description"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	app, err := h.Backend.UpdateApplication(applicationID, req.Name, req.Description)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.JSON(http.StatusOK, applicationToOutput(*app))
}

func (h *Handler) handleDeleteApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.DeleteApplication(applicationID); err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return internalServerErrorResponse(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
