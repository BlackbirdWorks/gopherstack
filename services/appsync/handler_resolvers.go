package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleTypeResolvers handles /v1/apis/{apiId}/types/{typeName}/resolvers[/{fieldName}].
func (h *Handler) handleTypeResolvers(
	ctx context.Context, c *echo.Context, apiID, typeName string, segs []string,
) error {
	if len(segs) < pathSegsTypeResolvers || segs[5] != pathSegResolvers {
		return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
	}

	if len(segs) == pathSegsTypeResolvers {
		// /v1/apis/{apiId}/types/{typeName}/resolvers
		switch c.Request().Method {
		case http.MethodPost:
			return h.createResolver(ctx, c, apiID, typeName)
		case http.MethodGet:
			return h.listResolvers(ctx, c, apiID, typeName)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}
	fieldName := segs[6]

	switch c.Request().Method {
	case http.MethodGet:
		return h.getResolver(ctx, c, apiID, typeName, fieldName)
	case http.MethodDelete:
		return h.deleteResolver(ctx, c, apiID, typeName, fieldName)
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return h.updateResolver(ctx, c, apiID, typeName, fieldName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createResolver handles POST /v1/apis/{apiId}/types/{typeName}/resolvers.
func (h *Handler) createResolver(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var r Resolver
	if jsonErr := json.Unmarshal(body, &r); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	created, createErr := h.Backend.CreateResolver(apiID, typeName, &r)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateResolver", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyResolver: created})
}

// getResolver handles GET /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) getResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	r, err := h.Backend.GetResolver(apiID, typeName, fieldName)
	if err != nil {
		return h.handleError(ctx, c, "GetResolver", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyResolver: r})
}

// listResolvers handles GET /v1/apis/{apiId}/types/{typeName}/resolvers.
func (h *Handler) listResolvers(ctx context.Context, c *echo.Context, apiID, typeName string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	resolvers, err := h.Backend.ListResolvers(apiID, typeName)
	if err != nil {
		return h.handleError(ctx, c, "ListResolvers", err)
	}

	page, tok := appsyncPaginate(resolvers, nextToken, maxResults)
	out := map[string]any{"resolvers": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteResolver handles DELETE /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) deleteResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	if err := h.Backend.DeleteResolver(apiID, typeName, fieldName); err != nil {
		return h.handleError(ctx, c, "DeleteResolver", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateResolver handles PUT/PATCH /v1/apis/{apiId}/types/{typeName}/resolvers/{fieldName}.
func (h *Handler) updateResolver(ctx context.Context, c *echo.Context, apiID, typeName, fieldName string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var r Resolver
	if jsonErr := json.Unmarshal(body, &r); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	r.FieldName = fieldName

	updated, updateErr := h.Backend.UpdateResolver(apiID, typeName, &r)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateResolver", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyResolver: updated})
}

// listResolversByFunction handles GET /v1/apis/{apiId}/functions/{functionId}/resolvers.
func (h *Handler) listResolversByFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	resolvers, err := h.Backend.ListResolversByFunction(apiID, functionID)
	if err != nil {
		return h.handleError(ctx, c, "ListResolversByFunction", err)
	}

	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	page, tok := appsyncPaginate(resolvers, nextToken, maxResults)
	out := map[string]any{"resolvers": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}
