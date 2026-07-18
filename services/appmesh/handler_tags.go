package appmesh

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

// ─── Tag handlers ───

func (h *Handler) handleListTags(c *echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return methodNotAllowed(c)
	}
	resourceArn := c.QueryParam("resourceArn")
	if resourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	maxResults, nextToken := listParams(c)
	refs, next, err := h.Backend.ListTagsForResource(resourceArn, maxResults, nextToken)
	if err != nil {
		return h.mapErr(c, err)
	}
	wireRefs := make([]map[string]string, 0, len(refs))
	for _, r := range refs {
		wireRefs = append(wireRefs, map[string]string{"key": r.Key, "value": r.Value})
	}
	resp := map[string]any{"tags": wireRefs}
	if next != "" {
		resp["nextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	if c.Request().Method != http.MethodPut {
		return methodNotAllowed(c)
	}
	var body struct {
		ResourceArn string     `json:"resourceArn"`
		Tags        []tagInput `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || body.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	if err := h.Backend.TagResource(body.ResourceArn, tagsToMap(body.Tags)); err != nil {
		return h.mapErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	if c.Request().Method != http.MethodPut {
		return methodNotAllowed(c)
	}
	var body struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := c.Bind(&body); err != nil || body.ResourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	if err := h.Backend.UntagResource(body.ResourceArn, body.TagKeys); err != nil {
		return h.mapErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}
