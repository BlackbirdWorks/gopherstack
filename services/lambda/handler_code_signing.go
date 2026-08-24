package lambda

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handle2020FunctionRoute handles routes under /2020-06-30/functions/{name}/...
func (h *Handler) handle2020FunctionRoute(c *echo.Context, rest2020, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	if !hasSuffixCodeSigningConfig(rest2020) {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	name := strings.TrimSuffix(strings.TrimPrefix(rest2020, "/"), "/code-signing-config")

	switch method {
	case http.MethodGet:
		return h.handleGetFunctionCodeSigningConfig(c, lambdaBk, name)
	case http.MethodPut:
		return h.handlePutFunctionCodeSigningConfig(c, lambdaBk, name)
	case http.MethodDelete:
		return h.handleDeleteFunctionCodeSigningConfig(c, lambdaBk, name)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleGetFunctionCodeSigningConfig handles GET /2020-06-30/functions/{name}/code-signing-config.
// Real AWS returns HTTP 200 with an empty CodeSigningConfigArn when the function exists but
// has no code signing config associated (not a 404).
func (h *Handler) handleGetFunctionCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, name string) error {
	cscARN, err := bk.GetFunctionCodeSigningConfig(name)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		if errors.Is(err, ErrCodeSigningConfigNotFound) {
			// Real AWS returns 200 with empty ARN when no code signing config is associated.
			return c.JSON(http.StatusOK, &GetFunctionCodeSigningConfigOutput{
				CodeSigningConfigArn: "",
				FunctionName:         name,
			})
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &GetFunctionCodeSigningConfigOutput{
		CodeSigningConfigArn: cscARN,
		FunctionName:         name,
	})
}

// handlePutFunctionCodeSigningConfig handles PUT /2020-06-30/functions/{name}/code-signing-config.
func (h *Handler) handlePutFunctionCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PutFunctionCodeSigningConfigInput
	if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
	}

	if input.CodeSigningConfigArn == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException",
			"CodeSigningConfigArn is required")
	}

	if putErr := bk.PutFunctionCodeSigningConfig(name, input.CodeSigningConfigArn); putErr != nil {
		if errors.Is(putErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function or code signing config not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", putErr.Error())
	}

	return c.JSON(http.StatusOK, &PutFunctionCodeSigningConfigOutput{
		CodeSigningConfigArn: input.CodeSigningConfigArn,
		FunctionName:         name,
	})
}

// handleDeleteFunctionCodeSigningConfig handles DELETE /2020-06-30/functions/{name}/code-signing-config.
func (h *Handler) handleDeleteFunctionCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, name string) error {
	if err := bk.DeleteFunctionCodeSigningConfig(name); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Function not found: "+name)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// --- Code signing config handlers ---

// handleCodeSigningRoute dispatches /2020-04-22/code-signing-configs routes.
func (h *Handler) handleCodeSigningRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest := strings.TrimPrefix(path, lambdaCodeSigningPathPrefix)
	rest = strings.TrimPrefix(rest, "/")

	// /2020-04-22/code-signing-configs → Create / List
	if rest == "" {
		switch method {
		case http.MethodPost:
			return h.handleCreateCodeSigningConfig(c, lambdaBk)
		case http.MethodGet:
			return h.handleListCodeSigningConfigs(c, lambdaBk)
		default:
			return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
		}
	}

	// /2020-04-22/code-signing-configs/{cscArn}/functions → ListFunctionsByCodeSigningConfig
	if before, ok0 := strings.CutSuffix(rest, "/functions"); ok0 {
		cscARN := before
		if method == http.MethodGet {
			return h.handleListFunctionsByCodeSigningConfig(c, lambdaBk, cscARN)
		}

		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}

	// /2020-04-22/code-signing-configs/{cscArn} → Get / Delete / Update
	cscARN := rest

	switch method {
	case http.MethodGet:
		return h.handleGetCodeSigningConfig(c, lambdaBk, cscARN)
	case http.MethodDelete:
		return h.handleDeleteCodeSigningConfig(c, lambdaBk, cscARN)
	case http.MethodPut:
		return h.handleUpdateCodeSigningConfig(c, lambdaBk, cscARN)
	default:
		return h.writeError(c, http.StatusMethodNotAllowed, "MethodNotAllowedException", "method not allowed")
	}
}

// handleCreateCodeSigningConfig handles POST /2020-04-22/code-signing-configs.
func (h *Handler) handleCreateCodeSigningConfig(c *echo.Context, bk *InMemoryBackend) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input CreateCodeSigningConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.AllowedPublishers == nil {
		input.AllowedPublishers = &AllowedPublishers{SigningProfileVersionArns: []string{}}
	}

	cfg, createErr := bk.CreateCodeSigningConfig(&input)
	if createErr != nil {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", createErr.Error())
	}

	return c.JSON(http.StatusCreated, &CreateCodeSigningConfigOutput{CodeSigningConfig: cfg})
}

// handleGetCodeSigningConfig handles GET /2020-04-22/code-signing-configs/{cscArn}.
func (h *Handler) handleGetCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	cfg, err := bk.GetCodeSigningConfig(cscARN)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &CreateCodeSigningConfigOutput{CodeSigningConfig: cfg})
}

// handleDeleteCodeSigningConfig handles DELETE /2020-04-22/code-signing-configs/{cscArn}.
func (h *Handler) handleDeleteCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	if err := bk.DeleteCodeSigningConfig(cscARN); err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleUpdateCodeSigningConfig handles PUT /2020-04-22/code-signing-configs/{cscArn}.
//
//nolint:dupl // similar update-handler structure shared with handleUpdateCapacityProvider by design
func (h *Handler) handleUpdateCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input UpdateCodeSigningConfigInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	cfg, updateErr := bk.UpdateCodeSigningConfig(cscARN, &input)
	if updateErr != nil {
		if errors.Is(updateErr, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", updateErr.Error())
	}

	return c.JSON(http.StatusOK, &UpdateCodeSigningConfigOutput{CodeSigningConfig: cfg})
}

// handleListCodeSigningConfigs handles GET /2020-04-22/code-signing-configs.
func (h *Handler) handleListCodeSigningConfigs(c *echo.Context, bk *InMemoryBackend) error {
	marker, maxItems := parsePaginationParams(c.Request())
	p := bk.ListCodeSigningConfigs(marker, maxItems)

	return c.JSON(http.StatusOK, &ListCodeSigningConfigsOutput{CodeSigningConfigs: p.Data, NextMarker: p.Next})
}

// handleListFunctionsByCodeSigningConfig handles GET /2020-04-22/code-signing-configs/{cscArn}/functions.
func (h *Handler) handleListFunctionsByCodeSigningConfig(c *echo.Context, bk *InMemoryBackend, cscARN string) error {
	marker, maxItems := parsePaginationParams(c.Request())

	p, err := bk.ListFunctionsByCodeSigningConfig(cscARN, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrFunctionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Code signing config not found: "+cscARN)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListFunctionsByCodeSigningConfigOutput{FunctionArns: p.Data, NextMarker: p.Next})
}
