package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handlePutFunctionEventInvokeConfig handles PUT /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handlePutFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutFunctionEventInvokeConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, putErr := lambdaBk.PutFunctionEventInvokeConfig(name, &input)
	if putErr != nil {
		return h.eventInvokeConfigError(c, putErr, name)
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleGetFunctionEventInvokeConfig handles GET /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handleGetFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	cfg, err := lambdaBk.GetFunctionEventInvokeConfig(name)
	if err != nil {
		return h.eventInvokeConfigError(c, err, name)
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleUpdateFunctionEventInvokeConfig handles POST /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handleUpdateFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutFunctionEventInvokeConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, updateErr := lambdaBk.UpdateFunctionEventInvokeConfig(name, &input)
	if updateErr != nil {
		return h.eventInvokeConfigError(c, updateErr, name)
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleDeleteFunctionEventInvokeConfig handles DELETE /2015-03-31/functions/{name}/event-invoke-config.
func (h *Handler) handleDeleteFunctionEventInvokeConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteFunctionEventInvokeConfig(name); err != nil {
		return h.eventInvokeConfigError(c, err, name)
	}

	return c.NoContent(http.StatusNoContent)
}

// handleListFunctionEventInvokeConfigs handles GET /2015-03-31/functions/{name}/event-invoke-configs.
func (h *Handler) handleListFunctionEventInvokeConfigs(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	marker, maxItems := parsePaginationParams(c.Request())

	configs, next, err := lambdaBk.ListFunctionEventInvokeConfigs(name, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListFunctionEventInvokeConfigsOutput{
		FunctionEventInvokeConfigs: configs,
		NextMarker:                 next,
	})
}

// eventInvokeConfigError maps event invoke config errors to HTTP responses.
func (h *Handler) eventInvokeConfigError(c *echo.Context, err error, name string) error {
	switch {
	case errors.Is(err, ErrFunctionNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Function not found: "+name)
	case errors.Is(err, ErrEventInvokeConfigNotFound):
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			fmt.Sprintf("The function %s doesn't have an event invoke config", name))
	case errors.Is(err, ErrInvalidParameterValue):
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	default:
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}
}
