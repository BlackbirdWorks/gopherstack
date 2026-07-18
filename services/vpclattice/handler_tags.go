package vpclattice

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ------- Tagging handlers -------

func (h *Handler) handleTagResource(
	c *echo.Context,
	resourceArn string,
	body map[string]any,
) error {
	tags := make(map[string]string)
	if t, ok := body["tags"].(map[string]any); ok {
		for k, v := range t {
			if s, ok2 := v.(string); ok2 {
				tags[k] = s
			}
		}
	}

	if err := h.Backend.TagResource(resourceArn, tags); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	keys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, keys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}
