package appsync

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

func (h *Handler) handleChannelNamespacesCollection(ctx context.Context, c *echo.Context, apiID string) error {
	switch c.Request().Method {
	case http.MethodPost:
		return h.createChannelNamespace(ctx, c, apiID)
	case http.MethodGet:
		return h.listChannelNamespaces(ctx, c, apiID)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

func (h *Handler) handleChannelNamespaceItem(ctx context.Context, c *echo.Context, apiID, nsName string) error {
	switch c.Request().Method {
	case http.MethodGet:
		return h.getChannelNamespace(ctx, c, apiID, nsName)
	case http.MethodDelete:
		return h.deleteChannelNamespace(ctx, c, apiID, nsName)
	case http.MethodPost, http.MethodPut, http.MethodPatch:
		return h.updateChannelNamespace(ctx, c, apiID, nsName)
	default:
		return c.JSON(http.StatusMethodNotAllowed, errorResponse("MethodNotAllowed", "method not allowed"))
	}
}

// createChannelNamespace handles POST /v2/apis/{apiId}/channelNamespaces.
func (h *Handler) createChannelNamespace(ctx context.Context, c *echo.Context, apiID string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		Tags               map[string]string `json:"tags"`
		HandlerConfigs     *HandlerConfigs   `json:"handlerConfigs"`
		Name               string            `json:"name"`
		PublishAuthModes   []AuthMode        `json:"publishAuthModes"`
		SubscribeAuthModes []AuthMode        `json:"subscribeAuthModes"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if input.Name == "" {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "name is required"))
	}

	cfg := &ChannelNamespaceConfig{
		PublishAuthModes:   input.PublishAuthModes,
		SubscribeAuthModes: input.SubscribeAuthModes,
		HandlerConfigs:     input.HandlerConfigs,
	}

	ns, createErr := h.Backend.CreateChannelNamespace(apiID, input.Name, input.Tags, cfg)
	if createErr != nil {
		return h.handleError(ctx, c, "CreateChannelNamespace", createErr)
	}

	return c.JSON(http.StatusCreated, map[string]any{keyChannelNamespace: ns})
}

// getChannelNamespace handles GET /v2/apis/{apiId}/channelNamespaces/{name}.
func (h *Handler) getChannelNamespace(ctx context.Context, c *echo.Context, apiID, name string) error {
	ns, err := h.Backend.GetChannelNamespace(apiID, name)
	if err != nil {
		return h.handleError(ctx, c, "GetChannelNamespace", err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannelNamespace: ns})
}

// listChannelNamespaces handles GET /v2/apis/{apiId}/channelNamespaces.
func (h *Handler) listChannelNamespaces(ctx context.Context, c *echo.Context, apiID string) error {
	q := c.Request().URL.Query()
	nextToken := q.Get("nextToken")
	maxResults, _ := strconv.Atoi(q.Get("maxResults"))

	nss, err := h.Backend.ListChannelNamespaces(apiID)
	if err != nil {
		return h.handleError(ctx, c, "ListChannelNamespaces", err)
	}

	page, tok := appsyncPaginate(nss, nextToken, maxResults)
	out := map[string]any{"channelNamespaces": page}
	if tok != "" {
		out["nextToken"] = tok
	}

	return c.JSON(http.StatusOK, out)
}

// updateChannelNamespace handles PUT/PATCH /v2/apis/{apiId}/channelNamespaces/{name}.
func (h *Handler) updateChannelNamespace(ctx context.Context, c *echo.Context, apiID, name string) error {
	body, err := httputils.ReadBody(c.Request())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, errorResponse("InternalFailure", err.Error()))
	}

	var input struct {
		HandlerConfigs     *HandlerConfigs `json:"handlerConfigs"`
		CodeHandlers       string          `json:"codeHandlers"`
		PublishAuthModes   []AuthMode      `json:"publishAuthModes"`
		SubscribeAuthModes []AuthMode      `json:"subscribeAuthModes"`
	}

	if jsonErr := json.Unmarshal(body, &input); jsonErr != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	cfg := &ChannelNamespaceConfig{
		CodeHandlers:       input.CodeHandlers,
		PublishAuthModes:   input.PublishAuthModes,
		SubscribeAuthModes: input.SubscribeAuthModes,
		HandlerConfigs:     input.HandlerConfigs,
	}

	updated, updateErr := h.Backend.UpdateChannelNamespace(apiID, name, cfg)
	if updateErr != nil {
		return h.handleError(ctx, c, "UpdateChannelNamespace", updateErr)
	}

	return c.JSON(http.StatusOK, map[string]any{keyChannelNamespace: updated})
}

// deleteChannelNamespace handles DELETE /v2/apis/{apiId}/channelNamespaces/{name}.
func (h *Handler) deleteChannelNamespace(ctx context.Context, c *echo.Context, apiID, name string) error {
	if err := h.Backend.DeleteChannelNamespace(apiID, name); err != nil {
		return h.handleError(ctx, c, "DeleteChannelNamespace", err)
	}

	return c.NoContent(http.StatusNoContent)
}
