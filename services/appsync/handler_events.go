package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
	"github.com/labstack/echo/v5"
)

// handleV2APIs handles /v2/apis[/{apiId}[/channelNamespaces[/{name}]]].
func (h *Handler) handleV2APIs(ctx context.Context, c *echo.Context, segs []string) error {
	switch len(segs) {
	case pathSegsAPIs:
		return h.handleV2APIsCollection(ctx, c)
	case pathSegsAPIID:
		return h.handleV2APIsItem(ctx, c, segs[2])
	case pathSegsAPISubresource:
		if segs[3] == pathSegChannelNamespaces {
			return h.handleChannelNamespacesCollection(ctx, c, segs[2])
		}
	case pathSegsNamedResource:
		if segs[3] == pathSegChannelNamespaces {
			return h.handleChannelNamespaceItem(ctx, c, segs[2], segs[4])
		}
	}

	return c.JSON(http.StatusNotFound, errorResponse("NotFoundException", "Not found"))
}

func (h *Handler) handleV2APIsCollection(ctx context.Context, c *echo.Context) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createAPI(ctx, c)
	case http.MethodGet:
		return h.listAPIs(ctx, c)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

func (h *Handler) handleV2APIsItem(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getAPI(ctx, c, apiID)
	case http.MethodDelete:
		return h.deleteAPI(ctx, c, apiID)
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return h.updateAPI(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createAPI handles POST /v2/apis.
func (h *Handler) createAPI(ctx context.Context, c *echo.Context) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags         map[string]string `json:"tags"`
		EventConfig  *EventConfig      `json:"eventConfig"`
		Name         string            `json:"name"`
		OwnerContact string            `json:"ownerContact"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	api, createErr := h.Backend.CreateAPI(input.Name, input.OwnerContact, input.Tags, input.EventConfig)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateApi", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyAPI: api})
}

// getAPI handles GET /v2/apis/{apiId}.
func (h *Handler) getAPI(ctx context.Context, c *echo.Context, apiID string) error {
	api, err := h.Backend.GetAPI(apiID)
	if err != nil {
		return h.handleError(ctx, c, "GetApi", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPI: api})
}

// listAPIs handles GET /v2/apis.
func (h *Handler) listAPIs(ctx context.Context, c *echo.Context) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	apis, err := h.Backend.ListAPIs()
	if err != nil {
		return h.handleError(ctx, c, "ListApis", err)
	}

	// The real AWS SDK response wraps the list in "apis", not "items".
	page, tok := appsyncPaginate(apis, nextToken, maxResults)
	out := map[string]any{"apis": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// deleteAPI handles DELETE /v2/apis/{apiId}.
func (h *Handler) deleteAPI(ctx context.Context, c *echo.Context, apiID string) error {
	if err := h.Backend.DeleteAPI(apiID); err != nil {
		return h.handleError(ctx, c, "DeleteApi", err)
	}

	return c.NoContent(http.StatusNoContent)
}

// updateAPI handles PUT/PATCH /v2/apis/{apiId}.
func (h *Handler) updateAPI(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		EventConfig  *EventConfig `json:"eventConfig"`
		Name         string       `json:"name"`
		OwnerContact string       `json:"ownerContact"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	api, updateErr := h.Backend.UpdateAPI(apiID, input.Name, input.OwnerContact, input.EventConfig)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateApi", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyAPI: api})
}
