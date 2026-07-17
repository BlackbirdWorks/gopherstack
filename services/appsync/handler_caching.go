package appsync

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleAPICaches handles /v1/apis/{apiId}/ApiCaches[/entries].
func (h *Handler) handleAPICaches(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	// /v1/apis/{apiId}/ApiCaches/update — the real AWS SDK endpoint for UpdateApiCache.
	if len(segs) == pathSegsNamedResource && segs[4] == "update" {
		if c.Request().Method == http.MethodPost {
			return h.updateAPICache(ctx, c, apiID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	// /v1/apis/{apiId}/ApiCaches/entries — legacy convenience alias for FlushApiCache;
	// the real AWS SDK sends FlushApiCache to "/v1/apis/{apiId}/FlushCache" instead
	// (see handleAPIResource's pathSegFlushCache case).
	if len(segs) == pathSegsNamedResource && segs[4] == "entries" {
		if c.Request().Method == http.MethodDelete {
			return h.flushAPICache(ctx, c, apiID)
		}

		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}

	switch c.Request().Method {
	case http.MethodPost:
		return h.createAPICache(ctx, c, apiID)
	case http.MethodGet:
		return h.getAPICache(ctx, c, apiID)
	case http.MethodPut:
		// Legacy convenience alias; the real AWS SDK uses POST to
		// "/v1/apis/{apiId}/ApiCaches/update" instead (handled above).
		return h.updateAPICache(ctx, c, apiID)
	case http.MethodDelete:
		return h.deleteAPICache(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createAPICache handles POST /v1/apis/{apiId}/ApiCaches.
func (h *Handler) createAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var cache APICache
	if jsonErr := json.Unmarshal(body, &cache); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	created, createErr := h.Backend.CreateAPICache(apiID, &cache)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApiCache", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyAPICache: created})
}

// getAPICache handles GET /v1/apis/{apiId}/ApiCaches.
func (h *Handler) getAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	cache, err := h.Backend.GetAPICache(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetApiCache", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPICache: cache})
}

// deleteAPICache handles DELETE /v1/apis/{apiId}/ApiCaches.
func (h *Handler) deleteAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.DeleteAPICache(apiID); err != nil {
		return h.handleError(ctx, c, "DeleteApiCache", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateAPICache handles PUT /v1/apis/{apiId}/ApiCaches.
func (h *Handler) updateAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var cache APICache
	if jsonErr := json.Unmarshal(body, &cache); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	updated, updateErr := h.Backend.UpdateAPICache(apiID, &cache)
	if updateErr != nil {
		return h.handleError(ctx, c, opUpdateAPICache, updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPICache: updated})
}

// flushAPICache handles DELETE /v1/apis/{apiId}/ApiCaches/entries.
func (h *Handler) flushAPICache(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.FlushAPICache(apiID); err != nil {
		return h.handleError(ctx, c, opFlushAPICache, err)
	}

	return c.NoContent(http.StatusNoContent)
}
