package rolesanywhere

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
)

// ---- Tag handlers ----

func (h *Handler) handleTagResource(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		ResourceArn string     `json:"resourceArn"`
		Tags        []TagEntry `json:"tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.TagResource(ctx, req.ResourceArn, req.Tags); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceArn, req.TagKeys); err != nil {
		return nil, 0, err
	}

	return nil, http.StatusOK, nil
}

func (h *Handler) handleListTagsForResource(ctx context.Context, query string) (any, int, error) {
	var resourceARN string

	for part := range strings.SplitSeq(query, "&") {
		if after, ok := strings.CutPrefix(part, "resourceArn="); ok {
			resourceARN = after
		}
	}

	tags, err := h.Backend.ListTagsForResource(ctx, resourceARN)
	if err != nil {
		return nil, 0, err
	}

	if tags == nil {
		tags = []TagEntry{}
	}

	return map[string]any{keyTags: tags}, http.StatusOK, nil
}
