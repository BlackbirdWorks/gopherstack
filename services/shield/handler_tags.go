package shield

import (
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/tags"
)

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tags.KV `json:"Tags"`
}

func (h *Handler) handleTagResource(body []byte) error {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(req.ResourceARN, tags.MapFromKV(req.Tags)); err != nil {
		return err
	}

	return nil
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tagsMap, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"Tags": tags.MapToKV(tagsMap),
	})
}

// untagResourceRequest is the request body for UntagResource.
type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(body []byte) error {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return err
	}

	return nil
}
