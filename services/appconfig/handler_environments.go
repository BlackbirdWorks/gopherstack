package appconfig

import (
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
)

func (h *Handler) handleCreateEnvironment(c *echo.Context, applicationID string) error {
	var req struct {
		Name        string    `json:"Name"`
		Description string    `json:"Description"`
		Monitors    []Monitor `json:"Monitors"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	env, err := h.Backend.CreateEnvironment(applicationID, req.Name, req.Description, req.Monitors)
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

	return c.JSON(http.StatusCreated, env)
}

func (h *Handler) handleGetEnvironment(c *echo.Context, applicationID, environmentID string) error {
	env, err := h.Backend.GetEnvironment(applicationID, environmentID)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	return c.JSON(http.StatusOK, env)
}

func (h *Handler) handleListEnvironments(c *echo.Context, applicationID string) error {
	nextToken, maxResults := appConfigPaginationParams(c)
	envs, outToken, err := h.Backend.ListEnvironments(applicationID, nextToken, maxResults)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return notFoundResponse(c, err)
		}

		return c.JSON(
			http.StatusInternalServerError,
			map[string]string{keyMessageField: err.Error()},
		)
	}

	resp := map[string]any{keyItems: envs}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleUpdateEnvironment(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	var req struct {
		Name        *string    `json:"Name"`
		Description *string    `json:"Description"`
		Monitors    *[]Monitor `json:"Monitors"`
	}
	if err := c.Bind(&req); err != nil {
		return c.JSON(
			http.StatusBadRequest,
			map[string]string{keyMessageField: errInvalidRequestBody},
		)
	}

	env, err := h.Backend.UpdateEnvironment(
		applicationID, environmentID, req.Name, req.Description, req.Monitors,
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

	return c.JSON(http.StatusOK, env)
}

func (h *Handler) handleDeleteEnvironment(
	c *echo.Context,
	applicationID, environmentID string,
) error {
	if err := h.Backend.DeleteEnvironment(applicationID, environmentID); err != nil {
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
