package mediapackage

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

// --- tag handlers ---

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body map[string]any) error {
	tags := extractTags(body)

	if err := h.Backend.TagResource(resourceARN, tags); err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.QueryParams()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.mapError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.mapError(c, err)
	}

	// Sort for stable output.
	keys := collections.SortedKeys(tags)

	out := make(map[string]any, len(tags))
	for _, k := range keys {
		out[k] = tags[k]
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": out})
}
