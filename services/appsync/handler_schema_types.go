package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleTypes handles /v1/apis/{apiId}/types[/{typeName}[/resolvers[/{fieldName}]]].
func (h *Handler) handleTypes(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	if len(segs) == pathSegsAPISubresource {
		return h.handleTypeCollection(ctx, c, apiID)
	}

	if len(segs) < pathSegsNamedResource {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	typeName := segs[4]

	if len(segs) == pathSegsNamedResource {
		return h.handleNamedType(ctx, c, apiID, typeName)
	}

	return h.handleTypeResolvers(ctx, c, apiID, typeName, segs)
}

// handleTypeCollection handles GET/POST /v1/apis/{apiId}/types.
func (h *Handler) handleTypeCollection(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createTypeHandler(ctx, c, apiID)
	case http.MethodGet:
		return h.listTypes(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// handleNamedType handles GET/DELETE/PUT /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) handleNamedType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getType(ctx, c, apiID, typeName)
	case http.MethodDelete:
		return h.deleteType(ctx, c, apiID, typeName)
	case http.MethodPost, http.MethodPut:
		return h.updateType(ctx, c, apiID, typeName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createTypeHandler handles POST /v1/apis/{apiId}/types within handleTypes.
func (h *Handler) createTypeHandler(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Definition string `json:"definition"`
		Format     string `json:"format"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Definition == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "definition is required"))
	}

	format := TypeDefinitionFormat(input.Format)
	if format == "" {
		format = TypeFormatSDL
	}

	created, createErr := h.Backend.CreateType(apiID, input.Definition, format)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateType", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyType: created})
}

// getType handles GET /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) getType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	// The format query parameter (SDL or JSON) is accepted for AWS SDK compatibility.
	// The definition is returned in the format it was stored in.
	t, err := h.Backend.GetType(apiID, typeName)
	if err != nil {
		return h.handleError(ctx, c, "GetType", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyType: t})
}

// listTypes handles GET /v1/apis/{apiId}/types.
func (h *Handler) listTypes(ctx context.Context, c *echo.Context, apiID string) error {
	// The format query parameter (SDL or JSON) is accepted for AWS SDK compatibility.
	// Each type is returned in the format it was stored in.
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	types, err := h.Backend.ListTypes(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListTypes", err)
	}

	page, tok := appsyncPaginate(types, nextToken, maxResults)
	out := map[string]any{"types": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteType handles DELETE /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) deleteType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	if err := h.Backend.DeleteType(apiID, typeName); err != nil {
		return h.handleError(ctx, c, "DeleteType", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateType handles PUT /v1/apis/{apiId}/types/{typeName}.
func (h *Handler) updateType(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Definition string `json:"definition"`
		Format     string `json:"format"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateType(apiID, typeName, input.Definition, TypeDefinitionFormat(input.Format))
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateType", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyType: updated})
}

// listTypesByAssociation handles GET /v1/mergedApis/{mergedApiId}/sourceApiAssociations/{assocId}/types.
func (h *Handler) listTypesByAssociation(
	ctx context.Context,
	c *echo.Context,
	mergedAPIID, assocID string,
) error {
	format := c.Request().URL.Query().Get("format")
	if format == "" {
		format = string(TypeFormatSDL)
	}

	types, err := h.Backend.ListTypesByAssociation(mergedAPIID, assocID, format)
	if err != nil {
		return h.handleError(ctx, c, "ListTypesByAssociation", err)
	}

	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	page, tok := appsyncPaginate(types, nextToken, maxResults)
	out := map[string]any{pathSegTypes: page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}
