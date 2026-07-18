package codecommit

import (
	"encoding/json"
	"fmt"
)

type tagResourceInput struct {
	Tags        map[string]string `json:"tags"`
	ResourceARN string            `json:"resourceArn"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type listTagsForResourceInput struct {
	ResourceARN string `json:"resourceArn"`
}

func (h *Handler) handleTagResource(body []byte) (any, error) {
	var in tagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleUntagResource(body []byte) (any, error) {
	var in untagResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return map[string]any{}, nil
}

func (h *Handler) handleListTagsForResource(body []byte) (any, error) {
	var in listTagsForResourceInput
	if err := json.Unmarshal(body, &in); err != nil {
		return nil, fmt.Errorf("invalid request body: %w", err)
	}

	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	kv, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	return map[string]any{
		"tags": kv,
	}, nil
}
