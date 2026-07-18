package managedblockchain

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	decoded, err := url.PathUnescape(resourceARN)
	if err != nil {
		decoded = resourceARN
	}

	tags, err := h.Backend.ListTagsForResource(decoded)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	decoded, err := url.PathUnescape(resourceARN)
	if err != nil {
		decoded = resourceARN
	}

	var req tagResourceRequest

	if parseErr := json.Unmarshal(body, &req); parseErr != nil {
		return writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body")
	}

	if tagErr := h.Backend.TagResource(decoded, req.Tags); tagErr != nil {
		return h.writeBackendError(c, tagErr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, query url.Values) error {
	decoded, err := url.PathUnescape(resourceARN)
	if err != nil {
		decoded = resourceARN
	}

	tagKeys := query["tagKeys"]

	if untagErr := h.Backend.UntagResource(decoded, tagKeys); untagErr != nil {
		return h.writeBackendError(c, untagErr)
	}

	return c.NoContent(http.StatusNoContent)
}
