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
		if errors.Is(err, awserr.ErrInvalidParameter) {
			return badRequestResponse(c, err)
		}

		if errors.Is(err, awserr.ErrAlreadyExists) {
			return conflictResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusCreated, app)
}

func (h *Handler) handleGetApplication(c *echo.Context, applicationID string) error {
	app, err := h.Backend.GetApplication(applicationID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, app)
}

func (h *Handler) handleListApplications(c *echo.Context) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	apps, outToken := h.Backend.ListApplications(nextToken, maxResults)

	resp := map[string]any{keyItems: apps}
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

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, app)
}

func (h *Handler) handleDeleteApplication(c *echo.Context, applicationID string) error {
	if err := h.Backend.DeleteApplication(applicationID); err != nil {
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
