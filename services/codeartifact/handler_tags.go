package codeartifact

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"

	"github.com/labstack/echo/v5"
)

type tagResourceBody struct {
	Tags []map[string]any `json:"tags"`
}

type untagResourceBody struct {
	TagKeys []string `json:"tagKeys"`
}

func (h *Handler) handleListTagsForResource(c *echo.Context, resourceARN string) error {
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "resourceArn is required"))
	}

	kv, err := h.Backend.ListTagsForResource(c.Request().Context(), resourceARN)
	if err != nil {
		return h.handleError(c, err)
	}

	tagList := make([]map[string]string, 0, len(kv))
	for k, v := range kv {
		tagList = append(tagList, map[string]string{"key": k, "value": v})
	}
	slices.SortFunc(tagList, func(a, b map[string]string) int {
		return strings.Compare(a["key"], b["key"])
	})

	return c.JSON(http.StatusOK, map[string]any{
		"tags": tagList,
	})
}

func (h *Handler) handleTagResource(c *echo.Context, resourceARN string, body []byte) error {
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "resourceArn is required"))
	}

	var in tagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if err := h.Backend.TagResource(c.Request().Context(), resourceARN, tagsFromSlice(in.Tags)); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

func (h *Handler) handleUntagResource(c *echo.Context, resourceARN string, body []byte) error {
	if resourceARN == "" {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "resourceArn is required"))
	}

	var in untagResourceBody
	if err := json.Unmarshal(body, &in); err != nil {
		return c.JSON(http.StatusBadRequest, errResp("ValidationException", "invalid request body"))
	}

	if err := h.Backend.UntagResource(c.Request().Context(), resourceARN, in.TagKeys); err != nil {
		return h.handleError(c, err)
	}

	return c.NoContent(http.StatusOK)
}
