package timestreamquery

import (
	"context"
	"encoding/json"
	"fmt"
)

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string `json:"ResourceARN"`
		Tags        []struct {
			Key   string `json:"Key"`
			Value string `json:"Value"`
		} `json:"Tags"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	tags := make(map[string]string, len(req.Tags))

	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(ctx, req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string   `json:"ResourceARN"`
		TagKeys     []string `json:"TagKeys"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{})
}

func (h *Handler) handleListTagsForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req struct {
		ResourceARN string `json:"ResourceARN"`
	}

	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("invalid request: %w", err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", ErrValidation)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Tags": tags,
	})
}
