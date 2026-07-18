package iotanalytics

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListTagsForResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"resourceArn query parameter is required",
		)
	}

	tags, err := h.Backend.ListTagsForResource(resourceARN)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{Tags: tags})
}

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"resourceArn query parameter is required",
		)
	}

	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(resourceARN, req.Tags); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context) error {
	resourceARN := c.Request().URL.Query().Get("resourceArn")
	if resourceARN == "" {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"InvalidRequestException",
			"resourceArn query parameter is required",
		)
	}

	tagKeys := c.Request().URL.Query()["tagKeys"]
	if len(tagKeys) == 0 {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "tagKeys query parameter is required")
	}

	if err := h.Backend.UntagResource(resourceARN, tagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}
