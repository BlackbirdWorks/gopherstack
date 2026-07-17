package efs

import (
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type tagResourceBody struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleTagResource(c *echo.Context, resourceID string, body []byte) error {
	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	kv := tagsFromEntries(in.Tags)
	if err := h.Backend.TagResource(h.contextWithRegion(c), resourceID, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceID string) error {
	t, err := h.Backend.ListTagsForResource(h.contextWithRegion(c), resourceID)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, map[string]any{
		keyTags: tagsToEntries(t),
	})
}

type createTagsBody struct {
	Tags []tagEntry `json:"Tags"`
}

func (h *Handler) handleCreateTags(c *echo.Context, fileSystemID string, body []byte) error {
	var in createTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	kv := tagsFromEntries(in.Tags)
	if err := h.Backend.CreateTags(h.contextWithRegion(c), fileSystemID, kv); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

type deleteTagsBody struct {
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleDeleteTags(c *echo.Context, fileSystemID string, body []byte) error {
	var in deleteTagsBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("BadRequest", "invalid request body"))
	}

	if err := h.Backend.DeleteTags(h.contextWithRegion(c), fileSystemID, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusNoContent)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceID string) error {
	tagKeys := c.Request().URL.Query()["tagKeys"]
	if err := h.Backend.UntagResource(h.contextWithRegion(c), resourceID, tagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
