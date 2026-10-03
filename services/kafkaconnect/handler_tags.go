package kafkaconnect

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) buildTagOps() map[string]opFunc {
	return map[string]opFunc{
		opTagResource: func(c *echo.Context, resource string, body []byte) error {
			return h.handleTagResource(c, resource, body)
		},
		opUntagResource: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleUntagResource(c, resource)
		},
		opListTagsForResource: func(c *echo.Context, resource string, _ []byte) error {
			return h.handleListTagsForResource(c, resource)
		},
	}
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body []byte) error {
	var req tagResourceRequest
	if err := decodeBody(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body")
	}

	if err := h.Backend.TagResource(resourceArn, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return h.writeJSON(c, listTagsForResourceResponse{Tags: tags})
}
