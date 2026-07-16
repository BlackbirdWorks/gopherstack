package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

type listTagsOutput struct {
	Tags map[string]string `json:"tags"`
}

type tagResourceInput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	c *echo.Context,
	resourceArn string,
) error {
	tags, err := h.Backend.GetTags(ctx, resourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
}

func (h *Handler) handleTagResource(
	ctx context.Context,
	c *echo.Context,
	resourceArn string,
	body []byte,
) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	if err := h.Backend.TagResource(ctx, resourceArn, in.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	c *echo.Context,
	resourceArn string,
	u *url.URL,
) error {
	tagKeys := u.Query()["tagKeys"]

	if err := h.Backend.UntagResource(ctx, resourceArn, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
