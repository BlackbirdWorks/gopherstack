package lambda

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleLayersRoute dispatches Lambda Layers REST API requests.
// Path format: /2018-10-31/layers[/{layerName}[/versions[/{versionNumber}[/policy[/{statementId}]]]]].
func (h *Handler) handleLayersRoute(c *echo.Context, path, method string) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	rest := strings.TrimPrefix(path, lambdaLayersPathPrefix)
	rest = strings.TrimPrefix(rest, "/")

	// GET /2018-10-31/layers → ListLayers, EXCEPT GET /2018-10-31/layers?find=LayerVersion
	// → GetLayerVersionByArn (api_op_GetLayerVersionByArn.go, lambda@v1.101.2
	// serializers.go: same bare path as ListLayers, disambiguated only by this
	// query flag -- real clients never send /layers-by-arn) -- gopherstack-l5ir.
	if rest == "" && method == http.MethodGet {
		if c.Request().URL.Query().Get(lambdaFindQueryParam) == lambdaFindLayerVersion {
			return h.handleGetLayerVersionByArn(c)
		}

		return h.handleListLayers(c, lambdaBk)
	}

	// Parse: {layerName}[/versions[/{versionNumber}[/policy[/{statementId}]]]]
	parts := strings.SplitN(rest, "/", layerPathMaxParts)
	layerName := parts[0]

	if len(parts) == 1 || parts[1] != layerVersionsPath {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	// GET /2018-10-31/layers/{layerName}/versions → ListLayerVersions
	if len(parts) == 2 && method == http.MethodGet {
		return h.handleListLayerVersions(c, lambdaBk, layerName)
	}

	// POST /2018-10-31/layers/{layerName}/versions → PublishLayerVersion
	if len(parts) == 2 && method == http.MethodPost {
		return h.handlePublishLayerVersion(c, lambdaBk, layerName)
	}

	if len(parts) < 3 { //nolint:mnd // minimum parts for versioned sub-routes: layerName, "versions", versionNum
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	version, parseErr := parseLayerVersion(parts[2])
	if parseErr != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid version number")
	}

	return h.handleLayerVersionedRoutes(c, lambdaBk, layerName, version, parts, method)
}

// handleLayerVersionedRoutes dispatches routes that require a specific layer version number.
func (h *Handler) handleLayerVersionedRoutes(
	c *echo.Context, bk *InMemoryBackend, layerName string, version int64, parts []string, method string,
) error {
	// GET/DELETE /2018-10-31/layers/{layerName}/versions/{versionNumber}
	if len(parts) == 3 { //nolint:mnd // parts: layerName, "versions", versionNum
		switch method {
		case http.MethodGet:
			return h.handleGetLayerVersion(c, bk, layerName, version)
		case http.MethodDelete:
			return h.handleDeleteLayerVersion(c, bk, layerName, version)
		}
	}

	if len(parts) < 4 || parts[3] != layerPolicyPath {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
	}

	// GET/POST /2018-10-31/layers/{layerName}/versions/{versionNumber}/policy
	if len(parts) == 4 { //nolint:mnd // parts: layerName, "versions", versionNum, "policy"
		switch method {
		case http.MethodGet:
			return h.handleGetLayerVersionPolicy(c, bk, layerName, version)
		case http.MethodPost:
			return h.handleAddLayerVersionPermission(c, bk, layerName, version)
		}
	}

	// DELETE /2018-10-31/layers/{layerName}/versions/{versionNumber}/policy/{statementId}
	if len(parts) == layerPathMaxParts && method == http.MethodDelete {
		return h.handleRemoveLayerVersionPermission(c, bk, layerName, version, parts[4])
	}

	return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException", "route not found")
}

// parseLayerVersion parses a layer version string to int64.
func parseLayerVersion(s string) (int64, error) {
	var v int64

	_, err := fmt.Sscanf(s, "%d", &v)
	if err != nil {
		return 0, err
	}

	return v, nil
}

func (h *Handler) handleListLayers(c *echo.Context, bk *InMemoryBackend) error {
	q := c.Request().URL.Query()
	compatibleRuntime := q.Get("CompatibleRuntime")
	marker, maxItems := parsePaginationParams(c.Request())
	p := bk.ListLayers(compatibleRuntime, marker, maxItems)

	return c.JSON(http.StatusOK, &ListLayersOutput{Layers: p.Data, NextMarker: p.Next})
}

func (h *Handler) handleListLayerVersions(c *echo.Context, bk *InMemoryBackend, layerName string) error {
	compatibleRuntime := c.Request().URL.Query().Get("CompatibleRuntime")
	marker, maxItems := parsePaginationParams(c.Request())

	p, err := bk.ListLayerVersions(layerName, compatibleRuntime, marker, maxItems)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				"Layer not found: "+layerName)
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, &ListLayerVersionsOutput{LayerVersions: p.Data, NextMarker: p.Next})
}

func (h *Handler) handlePublishLayerVersion(c *echo.Context, bk *InMemoryBackend, layerName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input PublishLayerVersionInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	input.LayerName = layerName

	if input.Content == nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Content is required")
	}

	out, publishErr := bk.PublishLayerVersion(&input)
	if publishErr != nil {
		if errors.Is(publishErr, ErrInvalidParameterValue) {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", publishErr.Error())
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", publishErr.Error())
	}

	return c.JSON(http.StatusCreated, out)
}

func (h *Handler) handleGetLayerVersion(c *echo.Context, bk *InMemoryBackend, layerName string, version int64) error {
	out, err := bk.GetLayerVersion(layerName, version)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, out)
}

func (h *Handler) handleDeleteLayerVersion(
	c *echo.Context,
	bk *InMemoryBackend,
	layerName string,
	version int64,
) error {
	if err := bk.DeleteLayerVersion(layerName, version); err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleGetLayerVersionPolicy(
	c *echo.Context,
	bk *InMemoryBackend,
	layerName string,
	version int64,
) error {
	policy, err := bk.GetLayerVersionPolicy(layerName, version)
	if err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.JSON(http.StatusOK, policy)
}

func (h *Handler) handleAddLayerVersionPermission(
	c *echo.Context, bk *InMemoryBackend, layerName string, version int64,
) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "failed to read body")
	}

	var input AddLayerVersionPermissionInput
	if len(body) > 0 {
		if unmarshalErr := json.Unmarshal(body, &input); unmarshalErr != nil {
			return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "invalid JSON")
		}
	}

	if input.StatementID == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "StatementId is required")
	}

	if input.Action == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Action is required")
	}

	if input.Principal == "" {
		return h.writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "Principal is required")
	}

	input.RevisionID = c.Request().URL.Query().Get("RevisionId")

	out, addErr := bk.AddLayerVersionPermission(layerName, version, &input)
	if addErr != nil {
		if errors.Is(addErr, ErrLayerNotFound) || errors.Is(addErr, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		if errors.Is(addErr, ErrFunctionAlreadyExists) {
			return h.writeError(c, http.StatusConflict, "ResourceConflictException",
				"Permission statement already exists: "+input.StatementID)
		}

		if errors.Is(addErr, ErrPreconditionFailed) {
			return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
				"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
					"and try again.")
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", addErr.Error())
	}

	return c.JSON(http.StatusCreated, out)
}

func (h *Handler) handleRemoveLayerVersionPermission(
	c *echo.Context, bk *InMemoryBackend, layerName string, version int64, statementID string,
) error {
	revisionID := c.Request().URL.Query().Get("RevisionId")

	if err := bk.RemoveLayerVersionPermission(layerName, version, statementID, revisionID); err != nil {
		if errors.Is(err, ErrLayerNotFound) || errors.Is(err, ErrLayerVersionNotFound) {
			return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
				fmt.Sprintf("Layer version not found: %s:%d", layerName, version))
		}

		if errors.Is(err, ErrPreconditionFailed) {
			return h.writeError(c, http.StatusPreconditionFailed, "PreconditionFailedException",
				"The RevisionId provided does not match the latest RevisionId. Fetch the latest version "+
					"and try again.")
		}

		return h.writeError(c, http.StatusInternalServerError, "ServiceException", err.Error())
	}

	return c.NoContent(http.StatusNoContent)
}

// handleGetLayerVersionByArn handles GET /2018-10-31/layers?find=LayerVersion&Arn={arn}.
func (h *Handler) handleGetLayerVersionByArn(c *echo.Context) error {
	lambdaBk, ok := h.Backend.(*InMemoryBackend)
	if !ok {
		return h.writeError(c, http.StatusInternalServerError, "ServiceException", "backend not available")
	}

	arn := c.Request().URL.Query().Get("Arn")
	if arn == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidParameterValueException",
			"Arn query parameter is required",
		)
	}

	out, err := lambdaBk.GetLayerVersionByArn(arn)
	if err != nil {
		return h.writeError(c, http.StatusNotFound, "ResourceNotFoundException",
			"Layer version not found: "+arn)
	}

	return c.JSON(http.StatusOK, out)
}
