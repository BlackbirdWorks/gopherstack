package iot

import (
	"net/http"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func resolveTagOps(path, method string) string {
	switch {
	case path == pathTags && method == http.MethodPost:
		return opTagResource
	case path == pathTags && method == http.MethodGet:
		return opListTagsForResource
	// UntagResource's real wire shape is POST /untag with a JSON body
	// (resourceArn/tagKeys), not DELETE /tags with query params
	// (iot@v1.77.4 serializers.go:20193-20251) -- found unreachable by
	// gopherstack-n1mb's route table. DELETE /tags is kept too as a
	// non-canonical route wired for this package's own tests.
	case path == "/untag" && method == http.MethodPost:
		return opUntagResource
	case path == pathTags && method == http.MethodDelete:
		return opUntagResource
	}

	return unknownOperation
}

func (h *Handler) handleTagResource(c *echo.Context) error {
	var req struct {
		ResourceArn string    `json:"resourceArn"`
		Tags        []tags.KV `json:"tags"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}
	if err := h.Backend.TagResourceGeneric(req.ResourceArn, tags.MapFromKV(req.Tags)); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// handleUntagResource reads resourceArn/tagKeys from the JSON body -- the
// real UntagResource wire shape (iot@v1.77.4 serializers.go:20241-20251) --
// falling back to query params for the non-canonical DELETE /tags route
// this package's own tests still use.
func (h *Handler) handleUntagResource(c *echo.Context) error {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}
	if err := readBody(c, &req); err != nil {
		return err
	}

	resourceARN := req.ResourceArn
	tagKeys := req.TagKeys

	if resourceARN == "" {
		resourceARN = c.Request().URL.Query().Get("resourceArn")
	}
	if len(tagKeys) == 0 {
		tagKeys = c.Request().URL.Query()["tagKeys"]
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return respondErr(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	resourceTags := h.Backend.ListTagsForResource(resourceARN)

	return c.JSON(http.StatusOK, map[string]any{"tags": tags.MapToKV(resourceTags)})
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
