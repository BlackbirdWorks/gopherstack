package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleRuntimeMgmtRoute handles /2021-07-20/functions/{name}/runtime-management-config routes.
//
//nolint:dupl // similar get/put pattern shared with handleRecursionConfigRoute by design
func (h *Handler) handleRuntimeMgmtRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest, found := strings.CutPrefix(path, lambda2021RuntimeMgmtPathPrefix)
	if !found {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split name + suffix

	if len(parts) < 2 || parts[1] != "runtime-management-config" {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := parts[0]

	switch method {
	case http.MethodGet:
		cfg, err := lambdaBk.GetRuntimeManagementConfig(name)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return c.JSON(http.StatusOK, cfg)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var input PutRuntimeManagementConfigInput
		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
			}
		}

		cfg, putErr := lambdaBk.PutRuntimeManagementConfig(name, &input)
		if putErr != nil {
			if errors.Is(putErr, ErrFunctionNotFound) {
				return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
					"Function not found: "+name)
			}

			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return c.JSON(http.StatusOK, cfg)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleRecursionConfigRoute handles /2024-08-28/functions/{name}/recursion-config routes.
//
//nolint:dupl // similar get/put pattern shared with handleRuntimeMgmtRoute and handleScalingConfigRoute by design
func (h *Handler) handleRecursionConfigRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest, found := strings.CutPrefix(path, lambda2024RecursionPathPrefix)
	if !found {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split name + suffix

	if len(parts) < 2 || parts[1] != "recursion-config" {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := parts[0]

	switch method {
	case http.MethodGet:
		cfg, err := lambdaBk.GetFunctionRecursionConfig(name)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return c.JSON(http.StatusOK, cfg)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var input PutFunctionRecursionConfigInput
		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
			}
		}

		cfg, putErr := lambdaBk.PutFunctionRecursionConfig(name, &input)
		if putErr != nil {
			if errors.Is(putErr, ErrFunctionNotFound) {
				return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
					"Function not found: "+name)
			}

			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", putErr.Error())
		}

		return c.JSON(http.StatusOK, cfg)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleScalingConfigRoute handles /2023-10-26/functions/{name}/scaling-config routes.
//
//nolint:dupl // similar get/put pattern shared with handleRuntimeMgmtRoute by design
func (h *Handler) handleScalingConfigRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest, found := strings.CutPrefix(path, lambda2023ScalingPathPrefix)
	if !found {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	rest = strings.TrimPrefix(rest, "/")
	parts := strings.SplitN(rest, "/", 2) //nolint:mnd // split name + suffix

	if len(parts) < 2 || parts[1] != "scaling-config" {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := parts[0]

	switch method {
	case http.MethodGet:
		cfg, err := lambdaBk.GetFunctionScalingConfig(name)
		if err != nil {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return c.JSON(http.StatusOK, cfg)
	case http.MethodPut:
		body, err := httputils.ReadBody(c.Request())
		if err != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
		}

		var input PutFunctionScalingConfigInput
		if len(body) > 0 {
			if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
				return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
			}
		}

		cfg, putErr := lambdaBk.PutFunctionScalingConfig(name, &input)
		if putErr != nil {
			if errors.Is(putErr, ErrFunctionNotFound) {
				return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
					"Function not found: "+name)
			}

			return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
		}

		return c.JSON(http.StatusOK, cfg)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}
