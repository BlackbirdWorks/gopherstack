package databrew

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

func parseTagsOp(method, _ string) string {
	switch method {
	case http.MethodPost:

		return opTagResource
	case http.MethodDelete:

		return opUntagResource
	case http.MethodGet:

		return opListTagsForResource
	}

	return opUnknown
}

func (h *Handler) dispatchTags(ctx context.Context, action string, body []byte) ([]byte, bool, error) {
	switch action {
	case opListTagsForResource:
		r, e := h.handleListTagsForResource(ctx, body)

		return r, true, e
	case opTagResource:
		r, e := h.handleTagResource(ctx, body)

		return r, true, e
	case opUntagResource:
		r, e := h.handleUntagResource(ctx, body)

		return r, true, e
	}

	return nil, false, nil
}

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		Tags        map[string]string `json:"Tags"`
		ResourceArn string            `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateTagsByArn(ctx, req.ResourceArn, req.Tags, nil); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string   `json:"ResourceArn"`
		TagKeys     []string `json:"TagKeys"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	if err := h.Backend.UpdateTagsByArn(ctx, req.ResourceArn, nil, req.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTagsForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceArn string `json:"ResourceArn"`
	}
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}
	tags, err := h.Backend.FindTagsByArn(ctx, req.ResourceArn)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{"Tags": tags})
}
