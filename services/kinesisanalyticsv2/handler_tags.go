package kinesisanalyticsv2

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/labstack/echo/v5"
)

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsOutput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.TagResource(ctx, in.ResourceARN, in.Tags); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleUntagResource(ctx context.Context, c *echo.Context, body []byte) error {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	if err := h.Backend.UntagResource(ctx, in.ResourceARN, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, struct{}{})
}

func (h *Handler) handleListTagsForResource(ctx context.Context, c *echo.Context, body []byte) error {
	var in listTagsInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "InvalidRequestException", "invalid request body: "+err.Error())
	}

	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	return c.JSON(http.StatusOK, listTagsOutput{Tags: tags})
}
