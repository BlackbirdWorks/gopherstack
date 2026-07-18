package ssoadmin

import (
	"encoding/json"
	"errors"
	"net/http"
	"sort"

	"github.com/labstack/echo/v5"
)

func (h *Handler) handleTagResource(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string    `json:"InstanceArn"`
		ResourceArn string    `json:"ResourceArn"`
		Tags        []tagView `json:"Tags"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(req.InstanceArn, req.ResourceArn, tags); err != nil {
		if errors.Is(err, ErrServiceQuotaExceeded) {
			return writeError(c, http.StatusBadRequest, "ServiceQuotaExceededException",
				"you have exceeded the maximum number of tags (50) for this resource")
		}

		return handleBackendError(c, err, "resource not found: "+req.ResourceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleUntagResource(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string   `json:"InstanceArn"`
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	if err := h.Backend.UntagResource(req.InstanceArn, req.ResourceArn, req.TagKeys); err != nil {
		return handleBackendError(c, err, "resource not found: "+req.ResourceArn)
	}

	return writeJSON(c, http.StatusOK, map[string]any{})
}

func (h *Handler) handleListTagsForResource(c *echo.Context, body []byte) error {
	var req struct {
		InstanceArn string `json:"InstanceArn"`
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return writeError(c, http.StatusBadRequest, "ValidationException", "invalid request body")
	}

	tags, err := h.Backend.ListTagsForResource(req.InstanceArn, req.ResourceArn)
	if err != nil {
		return handleBackendError(c, err, "resource not found: "+req.ResourceArn)
	}

	tagList := make([]tagView, 0, len(tags))
	for k, v := range tags {
		tagList = append(tagList, tagView{Key: k, Value: v})
	}
	sort.Slice(tagList, func(i, j int) bool { return tagList[i].Key < tagList[j].Key })

	return writeJSON(c, http.StatusOK, map[string]any{
		keyTags:      tagList,
		keyNextToken: nil,
	})
}
