package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolveTagOps(path, method string) string {
	switch {
	case path == pathTags && method == http.MethodPost:
		return opTagResource
	case path == pathTags && method == http.MethodDelete:
		return opUntagResource
	case path == pathTags && method == http.MethodGet:
		return opListTagsForResource
	}

	return unknownOperation
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	var req struct {
		ResourceArn string    `json:"resourceArn"`
		Tags        []tags.KV `json:"tags"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.TagResourceGeneric(req.ResourceArn, tags.MapFromKV(req.Tags)); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	resourceTags := h.Backend.ListTagsForResource(resourceARN)

	return c.JSON(http.StatusOK, map[string]any{"tags": tags.MapToKV(resourceTags)})
}

func (h *Handler) dispatchTagOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opTagResource:
		return true, h.handleTagResource(c)
	case opUntagResource:
		return true, h.handleUntagResource(c)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c)
	}

	return false, nil
}
