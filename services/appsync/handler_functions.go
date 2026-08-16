package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// handleFunctions handles /v1/apis/{apiId}/functions[/{functionId}[/resolvers]].
func (h *Handler) handleFunctions(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	// /v1/apis/{apiId}/functions/{functionId}/resolvers (6 segs)
	if len(segs) == pathSegsTypeResolvers && segs[5] == pathSegResolvers {
		funcID := segs[4]

		if method == http.MethodGet {
			return h.listResolversByFunction(ctx, c, apiID, funcID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	if len(segs) == pathSegsNamedResource {
		// /v1/apis/{apiId}/functions/{functionId}
		funcID := segs[4]

		switch method {
		case http.MethodGet:
			return h.getFunction(ctx, c, apiID, funcID)
		case http.MethodDelete:
			return h.deleteFunction(ctx, c, apiID, funcID)
		case http.MethodPost, http.MethodPut:
			return h.updateFunction(ctx, c, apiID, funcID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/functions
	switch method {
	case http.MethodPost:
		return h.createFunction(ctx, c, apiID)
	case http.MethodGet:
		return h.listFunctions(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createFunction handles POST /v1/apis/{apiId}/functions.
func (h *Handler) createFunction(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var f Function
	if jsonErr := json.Unmarshal(body, &f); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if f.DataSourceName == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "dataSourceName is required"))
	}

	created, createErr := h.Backend.CreateFunction(apiID, &f)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateFunction", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyFunctionConfiguration: created})
}

// getFunction handles GET /v1/apis/{apiId}/functions/{functionId}.
func (h *Handler) getFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	fn, err := h.Backend.GetFunction(apiID, functionID)
	if err != nil {
		return h.handleError(ctx, c, "GetFunction", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyFunctionConfiguration: fn})
}

// listFunctions handles GET /v1/apis/{apiId}/functions.
func (h *Handler) listFunctions(ctx context.Context, c *echo.Context, apiID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	fns, err := h.Backend.ListFunctions(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListFunctions", err)
	}

	page, tok := appsyncPaginate(fns, nextToken, maxResults)
	out := map[string]any{"functions": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteFunction handles DELETE /v1/apis/{apiId}/functions/{functionId}.
func (h *Handler) deleteFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	if err := h.Backend.DeleteFunction(apiID, functionID); err != nil {
		return h.handleError(ctx, c, "DeleteFunction", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateFunction handles PUT /v1/apis/{apiId}/functions/{functionId}.
func (h *Handler) updateFunction(ctx context.Context, c *echo.Context, apiID, functionID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var f Function
	if jsonErr := json.Unmarshal(body, &f); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateFunction(apiID, functionID, &f)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateFunction", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyFunctionConfiguration: updated})
}
