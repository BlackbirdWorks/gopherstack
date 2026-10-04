package dsql

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

// dispatchTagOps handles the generic ARN-scoped tagging trio.
func (h *Handler) dispatchTagOps(c *echo.Context, op, resource string, body []byte) (bool, error) {
	switch op {
	case opTagResource:
		return true, h.handleTagResource(c, resource, body)
	case opUntagResource:
		return true, h.handleUntagResource(c, resource)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c, resource)
	}

	return false, nil
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeInvalidBody(c)
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsForResourceResponse{Tags: tags})
}
