package bedrock

import (
	"net/http"

	"github.com/labstack/echo/v5"
)

func extractTagOperation(path, method string) (string, bool) {
	if method != http.MethodPost {
		return "", false
	}

	switch path {
	case listTagsForResourcePath:
		return opListTagsForResource, true
	case tagResourcePath:
		return opTagResource, true
	case untagResourcePath:
		return opUntagResource, true
	default:
		return "", false
	}
}

func (h *Handler) routeTag(c *echo.Context, path, method string, body []byte) (bool, error) {
	if method != http.MethodPost {
		return false, nil
	}

	switch path {
	case listTagsForResourcePath:
		return true, h.handleListTagsForResource(c, body)
	case tagResourcePath:
		return true, h.handleTagResource(c, body)
	case untagResourcePath:
		return true, h.handleUntagResource(c, body)
	default:
		return false, nil
	}
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"resourceARN"`
}

type listTagsForResourceOutput struct {
	Tags []Tag `json:"tags"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	in, err := parseBody[listTagsForResourceInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	tags, opErr := h.Backend.ListTagsForResource(in.ResourceARN)
	if opErr != nil {
		return h.writeError(c, opErr)
	}

	if tags == nil {
		tags = []Tag{}
	}

	return c.JSON(http.StatusOK, listTagsForResourceOutput{Tags: tags})
}

type tagResourceInput struct {
	ResourceARN string `json:"resourceARN"`
	Tags        []Tag  `json:"tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	in, err := parseBody[tagResourceInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	if opErr := h.Backend.TagResource(in.ResourceARN, in.Tags); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceARN"`
	TagKeys     []string `json:"tagKeys"`
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	in, err := parseBody[untagResourceInput](body)
	if err != nil {
		return c.JSON(
			http.StatusBadRequest,
			errorResponse("ValidationException", "invalid request body"),
		)
	}

	if opErr := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); opErr != nil {
		return h.writeError(c, opErr)
	}

	return c.NoContent(http.StatusOK)
}
