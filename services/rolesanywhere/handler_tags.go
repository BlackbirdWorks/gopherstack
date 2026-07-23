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

	if req.ResourceArn == "" || req.Tags == nil {
		return nil, 0, ErrValidation
	}

	if err := h.Backend.TagResource(ctx, req.ResourceArn, req.Tags); err != nil {
		return nil, 0, err
	}

	// Real AWS's TagResource responds 201 Created (per the service model's
	// http.responseCode: 201 on the TagResource operation), not 200 --
	// unlike every other void-result op in this service.
	return nil, http.StatusCreated, nil
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) (any, int, error) {
	var req struct {
		ResourceArn string   `json:"resourceArn"`
		TagKeys     []string `json:"tagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, 0, ErrValidation
	}

	if req.ResourceArn == "" || req.TagKeys == nil {
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

	if resourceARN == "" {
		return nil, 0, ErrValidation
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
