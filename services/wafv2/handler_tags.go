package wafv2

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
)

// tagItem represents a key/value pair for Tags fields.
type tagItem struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

func tagsFromItems(items []tagItem) map[string]string {
	m := make(map[string]string, len(items))

	for _, t := range items {
		m[t.Key] = t.Value
	}

	return m
}
func tagsToItems(tags map[string]string) []tagItem {
	items := make([]tagItem, 0, len(tags))

	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	sort.Slice(items, func(i, j int) bool {
		return items[i].Key < items[j].Key
	})

	return items
}

// tagResourceRequest is the request body for TagResource.
type tagResourceRequest struct {
	ResourceARN string    `json:"ResourceARN"`
	Tags        []tagItem `json:"Tags"`
}

func (h *Handler) handleTagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags := tagsFromItems(req.Tags)
	if err := validateTags(tags); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(ctx, req.ResourceARN, tags); err != nil {
		return nil, err
	}

	return nil, nil
}

// listTagsForResourceRequest is the request body for ListTagsForResource.
type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(ctx context.Context, body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(ctx, req.ResourceARN)
	if err != nil {
		return nil, err
	}

	return json.Marshal(map[string]any{
		"TagInfoForResource": map[string]any{
			"ResourceARN": req.ResourceARN,
			"TagList":     tagsToItems(tags),
		},
	})
}

// untagResourceRequest is the request body for UntagResource.
type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(ctx context.Context, body []byte) ([]byte, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(ctx, req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}

// tagsDispatchOps returns the tag-family operation dispatch entries. Each entry is a bound
// method value -- handleTagResource et al. already match the dispatchFn signature, so no
// wrapper closure is needed.
func (h *Handler) tagsDispatchOps() map[string]dispatchFn {
	return map[string]dispatchFn{
		"TagResource":         h.handleTagResource,
		"UntagResource":       h.handleUntagResource,
		"ListTagsForResource": h.handleListTagsForResource,
	}
}
