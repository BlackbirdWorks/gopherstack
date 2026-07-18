package batch

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// --- Tags handlers ---

type listTagsForResourceOutput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(ctx context.Context, c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return h.writeError(c, err)
	}

	if tags == nil {
		tags = map[string]string{}
	}

	return c.JSON(http.StatusOK, listTagsForResourceOutput{Tags: tags})
}

type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, resourceARN string, body []byte) error {
	var in tagResourceInput
	if len(body) > 0 {
		if err := json.Unmarshal(body, &in); err != nil {
			return c.JSON(http.StatusBadRequest, errorResponse("ValidationException", "invalid request body"))
		}
	}

	if err := h.Backend.TagResource(ctx, resourceARN, in.Tags); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, emptyOutput{})
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	c *echo.Context,
	resourceARN string,
	query url.Values,
) error {
	tagKeys := query["tagKeys"]
	if err := h.Backend.UntagResource(ctx, resourceARN, tagKeys); err != nil {
		return h.writeError(c, err)
	}

	return c.JSON(http.StatusOK, emptyOutput{})
}
