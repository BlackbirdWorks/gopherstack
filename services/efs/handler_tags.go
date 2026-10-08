package efs

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

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

	maxResults := listTagsDefaultMax
	if v := c.Request().URL.Query().Get("MaxResults"); v != "" {
		n, convErr := strconv.Atoi(v)
		if convErr != nil || n < 1 {
			return h.handleError(c, fmt.Errorf("%w: MaxResults must be a positive integer", ErrBadRequest))
		}

		maxResults = n
	}

	page, next, pageErr := paginate(
		tagsToEntries(
			t,
		),
		c.Request().URL.Query().Get("NextToken"),
		maxResults,
		func(e tagEntry) string { return e.Key },
	)
	if pageErr != nil {
		return h.handleError(c, fmt.Errorf("%w: invalid NextToken", ErrBadRequest))
	}

	resp := map[string]any{keyTags: page}
	if next != "" {
		resp["NextToken"] = next
	}

	return c.JSON(http.StatusOK, resp)
}

const listTagsDefaultMax = 100

// handleDescribeTags serves the legacy DescribeTags op: Marker in, Marker and
// NextMarker out, page size fixed at 100 with MaxItems ignored
// (efs@v1.44.4 api_op_DescribeTags.go).
func (h *Handler) handleDescribeTags(c *echo.Context, fileSystemID string) error {
	t, err := h.Backend.ListTagsForResource(h.contextWithRegion(c), fileSystemID)
	if err != nil {
		return h.handleError(c, err)
	}

	marker := c.Request().URL.Query().Get("Marker")

	page, next, pageErr := paginate(
		tagsToEntries(t), marker, listTagsDefaultMax, func(e tagEntry) string { return e.Key },
	)
	if pageErr != nil {
		return h.handleError(c, fmt.Errorf("%w: invalid Marker", ErrBadRequest))
	}

	resp := map[string]any{keyTags: page}
	if marker != "" {
		resp["Marker"] = marker
	}

	if next != "" {
		resp["NextMarker"] = next
	}

	return c.JSON(http.StatusOK, resp)
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
