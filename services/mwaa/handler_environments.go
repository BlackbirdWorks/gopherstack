package mwaa

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateEnvironment(c *echo.Context, name string) error {
	var req createEnvironmentRequest
	if !decodeJSONBody(c, &req) {
		return nil
	}

	env, err := h.Backend.CreateEnvironment(h.contextWithRegion(c), name, &req)

	return writeEnvironmentResult(c, env, err)
}

func (h *Handler) handleGetEnvironment(c *echo.Context, name string) error {
	env, err := h.Backend.GetEnvironment(h.contextWithRegion(c), name)
	if err != nil {
		if errors.Is(err, awserr.ErrNotFound) {
			return writeErrorResponse(c, http.StatusNotFound, "ResourceNotFoundException", err.Error())
		}

		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, map[string]any{
		"Environment": env,
	})

	return nil
}

func (h *Handler) handleDeleteEnvironment(c *echo.Context, name string) error {
	env, err := h.Backend.DeleteEnvironment(h.contextWithRegion(c), name)

	return writeEnvironmentVoidResult(c, env, err)
}

func (h *Handler) handleUpdateEnvironment(c *echo.Context, name string) error {
	var req updateEnvironmentRequest
	if !decodeJSONBody(c, &req) {
		return nil
	}

	env, err := h.Backend.UpdateEnvironment(h.contextWithRegion(c), name, &req)

	return writeEnvironmentResult(c, env, err)
}

func (h *Handler) handleListEnvironments(c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("NextToken")

	pageSize := 0
	if v := q.Get("MaxResults"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > listEnvMaxPageSize {
			return writeErrorResponse(c, http.StatusBadRequest, "ValidationException",
				"MaxResults must be between 1 and 100")
		}

		pageSize = n
	}

	names, outToken, err := h.Backend.ListEnvironmentsPage(h.contextWithRegion(c), nextToken, pageSize)
	if err != nil {
		return writeErrorResponse(c, http.StatusInternalServerError, "InternalServerException", err.Error())
	}

	if names == nil {
		names = []string{}
	}

	resp := map[string]any{"Environments": names}
	if outToken != "" {
		resp["NextToken"] = outToken
	}

	httputils.WriteJSON(c.Request().Context(), c.Response(), http.StatusOK, resp)

	return nil
}
