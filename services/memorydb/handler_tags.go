package memorydb

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleListTags(ctx context.Context, c *echo.Context, body []byte) error {
	var req listTagsRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	tags, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(tags)})
}

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var req tagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	if err := validateTagEntries(req.Tags); err != nil {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", err.Error())
	}

	tags := tagsFromSlice(req.Tags)

	if err := h.Backend.TagResource(ctx, req.ResourceArn, tags); err != nil {
		return h.writeBackendError(c, err)
	}

	// Return the resulting tag list (AWS behaviour).
	result, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(result)})
}

func (h *Handler) handleUntagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var req untagResourceRequest

	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "SerializationException", "invalid request body")
	}

	if req.ResourceArn == "" {
		return writeError(c, http.StatusBadRequest, "InvalidParameterValueException", "ResourceArn is required")
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceArn, req.TagKeys); err != nil {
		return h.writeBackendError(c, err)
	}

	// Return the remaining tag list (AWS behaviour).
	result, err := h.Backend.ListTags(ctx, req.ResourceArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsResponse{TagList: tagsToSlice(result)})
}

// -- Snapshot handlers -----------------------------------------------------------
