package bedrockagent

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// ---------------------------------------------------------------------------
// Tag handlers
// ---------------------------------------------------------------------------

func (h *Handler) handleListTags(
	ctx context.Context, c *echo.Context, resourceARN string,
) error {
	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return handleErr(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tags})
}

func (h *Handler) handleTagResource(
	ctx context.Context, c *echo.Context, resourceARN string, body []byte,
) error {
	var req struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return handleErr(c, err)
	}

	if err := h.Backend.TagResource(ctx, resourceARN, req.Tags); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(
	ctx context.Context, c *echo.Context, resourceARN string, query url.Values,
) error {
	tagKeys := query["tagKeys"]

	if err := h.Backend.UntagResource(ctx, resourceARN, tagKeys); err != nil {
		return handleErr(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func classifyTagPath(method string) string {
	switch method {
	case http.MethodGet:
		return opListTagsForResource
	case http.MethodPost:
		return opTagResource
	case http.MethodDelete:
		return opUntagResource
	}

	return opUnknown
}
