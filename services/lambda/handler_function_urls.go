package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleCreateFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateFunctionURLConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.AuthType == "" {
		input.AuthType = "NONE"
	}

	cfg, createErr := lambdaBk.CreateFunctionURLConfig(
		c.Request().Context(),
		name,
		input.AuthType,
		input.Cors,
		input.InvokeMode,
	)
	if createErr != nil {
		if errors.Is(createErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(createErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Function URL config already exists for: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, cfg)
}

func (h *Handler) handleGetFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	cfg, err := lambdaBk.GetFunctionURLConfig(name)
	if err != nil {
		if errors.Is(err, ErrFunctionURLNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function URL config not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, cfg)
}

func (h *Handler) handleDeleteFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteFunctionURLConfig(name); err != nil {
		if errors.Is(err, ErrFunctionURLNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function URL config not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Function URL config 2021-10-31 route handler ---

// handleFunctionURLRoute2021 dispatches /2021-10-31/functions/{name}/url routes (SDK "Url" casing).
func (h *Handler) handleFunctionURLRoute2021(c *echo.Context, path, method string) error {
	rest := strings.TrimPrefix(path, lambda2021PathPrefix)

	// /2021-10-31/functions/{name}/urls → ListFunctionUrlConfigs
	if strings.HasSuffix(rest, "/urls") {
		name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/urls")
		if method == http.MethodGet {
			return h.handleListFunctionURLConfigs(c, name)
		}

		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}

	// /2021-10-31/functions/{name}/url → Create / Get / Delete / Update
	if strings.HasSuffix(rest, "/url") {
		name := strings.TrimSuffix(strings.TrimPrefix(rest, "/"), "/url")

		switch method {
		case http.MethodPost:
			return h.handleCreateFunctionURLConfig(c, name)
		case http.MethodGet:
			return h.handleGetFunctionURLConfig(c, name)
		case http.MethodDelete:
			return h.handleDeleteFunctionURLConfig(c, name)
		case http.MethodPut:
			return h.handleUpdateFunctionURLConfig(c, name)
		default:
			return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// handleListFunctionURLConfigs handles GET /2021-10-31/functions/{name}/urls.
func (h *Handler) handleListFunctionURLConfigs(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	// If name is provided, filter to that function only.
	if name != "" {
		cfg, err := lambdaBk.GetFunctionURLConfig(name)
		if err != nil {
			// Return empty list rather than 404 for a missing URL config.
			return c.JSON(http.StatusOK, &ListFunctionURLConfigsOutput{FunctionURLConfigs: []*FunctionURLConfig{}})
		}

		return c.JSON(http.StatusOK, &ListFunctionURLConfigsOutput{
			FunctionURLConfigs: []*FunctionURLConfig{cfg},
		})
	}

	cfgs := lambdaBk.ListFunctionURLConfigs()

	return c.JSON(http.StatusOK, &ListFunctionURLConfigsOutput{FunctionURLConfigs: cfgs})
}

// handleUpdateFunctionURLConfig handles PUT /2021-10-31/functions/{name}/url.
func (h *Handler) handleUpdateFunctionURLConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateFunctionURLConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, updateErr := lambdaBk.UpdateFunctionURLConfig(name, input.AuthType, input.Cors, input.InvokeMode)
	if updateErr != nil {
		if errors.Is(updateErr, ErrFunctionURLNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function URL config not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, cfg)
}
