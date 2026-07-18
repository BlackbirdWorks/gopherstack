package fis

import (
	"encoding/json"
	"net/http"
	"net/url"

	"github.com/labstack/echo/v5"
)

// ----------------------------------------
// Tag handlers
// ----------------------------------------

func (h *Handler) handleTagResource(c *echo.Context, arnStr string, body []byte) error {
	var input struct {
		Tags map[string]string `json:"tags"`
	}

	if err := json.Unmarshal(body, &input); err != nil {
		return h.writeError(c, http.StatusBadRequest, "invalid request body: "+err.Error(), arnStr)
	}

	if err := h.Backend.TagResource(arnStr, input.Tags); err != nil {
		return h.writeBackendError(c, err, arnStr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, arnStr string, query url.Values) error {
	keys := query["tagKeys"]
	if err := h.Backend.UntagResource(arnStr, keys); err != nil {
		return h.writeBackendError(c, err, arnStr)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, arnStr string) error {
	tags, err := h.Backend.ListTagsForResource(arnStr)
	if err != nil {
		return h.writeBackendError(c, err, arnStr)
	}

	return c.JSON(http.StatusOK, tagsResponseDTO{Tags: tags})
}
