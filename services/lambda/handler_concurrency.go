package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

const opListProvisionedConcurrencyConfigs = "ListProvisionedConcurrencyConfigs"

// handlePutFunctionConcurrency handles PUT /2015-03-31/functions/{name}/concurrency.
func (h *Handler) handlePutFunctionConcurrency(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	if len(body) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"ReservedConcurrentExecutions is required")
	}

	var raw map[string]json.RawMessage
	if unmarshalErr := json.Unmarshal(body, &raw); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if _, present := raw["ReservedConcurrentExecutions"]; !present {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"ReservedConcurrentExecutions is required")
	}

	var input PutFunctionConcurrencyInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	concurrency, putErr := lambdaBk.PutFunctionConcurrency(name, input.ReservedConcurrentExecutions)
	if putErr != nil {
		if errors.Is(putErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(putErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
	}

	return c.JSON(http.StatusOK, concurrency)
}

// handleGetFunctionConcurrency handles GET /2015-03-31/functions/{name}/concurrency.
func (h *Handler) handleGetFunctionConcurrency(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	concurrency, err := lambdaBk.GetFunctionConcurrency(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrFunctionConcurrencyNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Function %s has no reserved concurrency configured", name))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, concurrency)
}

// handleDeleteFunctionConcurrency handles DELETE /2015-03-31/functions/{name}/concurrency.
func (h *Handler) handleDeleteFunctionConcurrency(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if err := lambdaBk.DeleteFunctionConcurrency(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handlePutProvisionedConcurrencyConfig handles
// PUT /2015-03-31/functions/{name}/provisioned-concurrency?Qualifier={qualifier}.
func (h *Handler) handlePutProvisionedConcurrencyConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")
	if qualifier == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Qualifier is required for PutProvisionedConcurrencyConfig")
	}

	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutProvisionedConcurrencyConfigInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	cfg, putErr := lambdaBk.PutProvisionedConcurrencyConfig(name, qualifier, input.ProvisionedConcurrentExecutions)
	if putErr != nil {
		if errors.Is(putErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(putErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
	}

	return c.JSON(http.StatusCreated, cfg)
}

// handleGetProvisionedConcurrencyConfig handles
// GET /2015-03-31/functions/{name}/provisioned-concurrency?Qualifier={qualifier}.
func (h *Handler) handleGetProvisionedConcurrencyConfig(c *echo.Context, name, qualifier string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	cfg, err := lambdaBk.GetProvisionedConcurrencyConfig(name, qualifier)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrProvisionedConcurrencyConfigNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"No provisioned concurrency config found for qualifier: "+qualifier)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, cfg)
}

// handleDeleteProvisionedConcurrencyConfig handles
// DELETE /2015-03-31/functions/{name}/provisioned-concurrency?Qualifier={qualifier}.
func (h *Handler) handleDeleteProvisionedConcurrencyConfig(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	qualifier := c.Request().URL.Query().Get("Qualifier")
	if qualifier == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"Qualifier is required for DeleteProvisionedConcurrencyConfig")
	}

	if err := lambdaBk.DeleteProvisionedConcurrencyConfig(name, qualifier); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrProvisionedConcurrencyConfigNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"No provisioned concurrency config found for qualifier: "+qualifier)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleListProvisionedConcurrencyConfigs handles
// GET /2015-03-31/functions/{name}/provisioned-concurrency (no Qualifier).
func (h *Handler) handleListProvisionedConcurrencyConfigs(c *echo.Context, name string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	configs, err := lambdaBk.ListProvisionedConcurrencyConfigs(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListProvisionedConcurrencyConfigsOutput{
		ProvisionedConcurrencyConfigs: configs,
	})
}
