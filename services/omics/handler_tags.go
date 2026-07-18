package omics

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string) error {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := readJSON(c, &req); err != nil {
		return err
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.mapError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}
