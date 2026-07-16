package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func resolveTagOps(path, method string) string {
	switch {
	case path == pathTags && method == http.MethodPost:
		return opTagResource
	case path == pathTags && method == http.MethodDelete:
		return opUntagResource
	case path == pathTags && method == http.MethodGet:
		return opListTagsForResource
	}

	return unknownOperation
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	var req struct {
		Tags        map[string]string `json:"tags"`
		ResourceArn string            `json:"resourceArn"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.TagResourceGeneric(req.ResourceArn, req.Tags); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	tags := h.Backend.ListTagsForResource(resourceARN)
	tagList := make([]map[string]string, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, map[string]string{"Key": k, "Value": v})
	}

	return c.JSON(http.StatusOK, map[string]any{"tags": tagList})
}

func (h *Handler) dispatchTagOps(c *echo.Context, op string) (bool, error) {
	switch op {
	case opTagResource:
		return true, h.handleTagResource(c)
	case opUntagResource:
		return true, h.handleUntagResource(c)
	case opListTagsForResource:
		return true, h.handleListTagsForResource(c)
	}

	return false, nil
}
