package mediatailor

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// --- Tag handlers ---

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return respondErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{keyTags: nilToEmpty(tags)})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body map[string]any) error {
	tags := extractTags(body)

	if err := h.Backend.TagResource(resourceARN, tags); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	keys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, keys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
