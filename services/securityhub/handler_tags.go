package securityhub

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

func classifyTagsPath(method, path string) (string, string) {
	resource := strings.TrimPrefix(path, "/tags/")

	switch method {
	case http.MethodGet:
		return opListTagsForResource, resource
	case http.MethodPost:
		return opTagResource, resource
	case http.MethodDelete:
		return opUntagResource, resource
	}

	return opUnknown, ""
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceArn string) error {
	tags, err := h.Backend.ListTagsForResource(resourceArn)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{"Tags": tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceArn string, body map[string]any) error {
	tags := make(map[string]string)

	if t, ok := body["Tags"].(map[string]any); ok {
		for k, v := range t {
			tags[k], _ = v.(string)
		}
	}

	if err := h.Backend.TagResource(resourceArn, tags); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceArn string) error {
	rawKeys := c.QueryParams()["tagKeys"]

	if err := h.Backend.UntagResource(resourceArn, rawKeys); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]any{keyMessage: err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]any{})
}

// tagsOpHandlers returns the Tags operation dispatch table for handleREST.
func (h *Handler) tagsOpHandlers(c *echo.Context, resource string, body map[string]any) map[string]func() error {
	return map[string]func() error{
		opListTagsForResource: func() error { return h.handleListTagsForResource(c, resource) },
		opTagResource:         func() error { return h.handleTagResource(c, resource, body) },
		opUntagResource:       func() error { return h.handleUntagResource(c, resource) },
	}
}
