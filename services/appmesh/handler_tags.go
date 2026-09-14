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

// TagResourceInput/UntagResourceInput carry resourceArn as a query param, not
// a body field (serializers.go's awsRestjson1_serializeOpHttpBindingsTagResourceInput /
// ...UntagResourceInput encode.SetQuery("resourceArn")); only tags/tagKeys are
// in the JSON body. A real client's resourceArn never reaches echo's c.Bind on
// a PUT (query binding is GET/DELETE/HEAD/QUERY-only), so it must be read via
// c.QueryParam explicitly, same as handleListTags already does.
func (h *Handler) handleTagResource(c *echo.Context) error {
	if c.Request().Method != http.MethodPut {
		return methodNotAllowed(c)
	}
	resourceArn := c.QueryParam("resourceArn")
	var body struct {
		Tags []tagInput `json:"tags"`
	}
	if err := c.Bind(&body); err != nil || resourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	if err := h.Backend.TagResource(resourceArn, tagsToMap(body.Tags)); err != nil {
		return h.mapErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	if c.Request().Method != http.MethodPut {
		return methodNotAllowed(c)
	}
	resourceArn := c.QueryParam("resourceArn")
	var body struct {
		TagKeys []string `json:"tagKeys"`
	}
	if err := c.Bind(&body); err != nil || resourceArn == "" {
		return c.JSON(http.StatusBadRequest, errResp("BadRequestException", "resourceArn is required"))
	}
	if err := h.Backend.UntagResource(resourceArn, body.TagKeys); err != nil {
		return h.mapErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}
