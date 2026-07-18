package mq

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListTags(c *echo.Context, resourceARN string) error {
	return c.JSON(http.StatusOK, map[string]any{"tags": tagsOrEmpty(h.Backend.ListTags(resourceARN))})
}

type createTagsInput struct {
	Tags map[string]string `json:"tags"`
}

func (h *Handler) handleCreateTags(c *echo.Context, resourceARN string, body []byte) error {
	var in createTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errorResponse("BadRequestException", "invalid request body"))
	}

	if err := h.Backend.CreateTags(resourceARN, in.Tags); err != nil {
		return h.writeError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleDeleteTags(c *echo.Context, resourceARN string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	h.Backend.DeleteTags(resourceARN, tagKeys)

	return c.NoContent(http.StatusNoContent)
}
