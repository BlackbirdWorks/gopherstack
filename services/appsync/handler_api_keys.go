package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleAPIKeys handles /v1/apis/{apiId}/apikeys[/{keyId}].
func (h *Handler) handleAPIKeys(ctx context.Context, c *echo.Context, apiID string, segs []string) error {
	method := c.Request().Method

	if len(segs) == pathSegsNamedResource {
		// /v1/apis/{apiId}/apikeys/{keyId}
		keyID := segs[4]

		switch method {
		case http.MethodDelete:
			return h.deleteAPIKey(ctx, c, apiID, keyID)
		case http.MethodPost, http.MethodPut, http.MethodPatch:
			return h.updateAPIKey(ctx, c, apiID, keyID)
		default:
			return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
		}
	}

	// /v1/apis/{apiId}/apikeys
	switch method {
	case http.MethodPost:
		return h.createAPIKey(ctx, c, apiID)
	case http.MethodGet:
		return h.listAPIKeys(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createAPIKey handles POST /v1/apis/{apiId}/apikeys.
func (h *Handler) createAPIKey(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description string `json:"description"`
		Expires     int64  `json:"expires"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	key, createErr := h.Backend.CreateAPIKey(apiID, input.Description, input.Expires)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApiKey", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{"apiKey": key})
}

// listAPIKeys handles GET /v1/apis/{apiId}/apikeys.
func (h *Handler) listAPIKeys(ctx context.Context, c *echo.Context, apiID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	keys, err := h.Backend.ListAPIKeys(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListApiKeys", err)
	}

	page, tok := appsyncPaginate(keys, nextToken, maxResults)
	out := map[string]any{"apiKeys": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteAPIKey handles DELETE /v1/apis/{apiId}/apikeys/{keyId}.
func (h *Handler) deleteAPIKey(ctx context.Context, c *echo.Context, apiID, keyID string) error {
	if err := h.Backend.DeleteAPIKey(apiID, keyID); err != nil {
		return h.handleError(ctx, c, "DeleteApiKey", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateAPIKey handles PUT/PATCH /v1/apis/{apiId}/apikeys/{keyId}.
func (h *Handler) updateAPIKey(ctx context.Context, c *echo.Context, apiID, keyID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Description string `json:"description"`
		Expires     int64  `json:"expires"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	key, updateErr := h.Backend.UpdateAPIKey(apiID, keyID, input.Description, input.Expires)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateApiKey", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{"apiKey": key})
}
