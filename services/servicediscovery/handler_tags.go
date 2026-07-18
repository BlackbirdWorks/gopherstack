package servicediscovery

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/blackbirdworks/gopherstack/pkgs/collections"
)

type listTagsForResourceRequest struct {
	ResourceARN string `json:"ResourceARN"`
}

func (h *Handler) handleListTagsForResource(_ context.Context, body []byte) ([]byte, error) {
	var req listTagsForResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tags, err := h.Backend.ListTagsForResource(req.ResourceARN)
	if err != nil {
		return nil, err
	}

	tagList := mapToTagEntries(tags)

	return json.Marshal(map[string]any{
		keyTags: tagList,
	})
}

type tagResourceRequest struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagEntry `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, body []byte) ([]byte, error) {
	var req tagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := validateTags(req.Tags); err != nil {
		return nil, err
	}

	if err := h.Backend.TagResource(req.ResourceARN, tagsToMap(req.Tags)); err != nil {
		return nil, err
	}

	return nil, nil
}

type untagResourceRequest struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(_ context.Context, body []byte) ([]byte, error) {
	var req untagResourceRequest
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidRequest, err)
	}

	if req.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(req.ResourceARN, req.TagKeys); err != nil {
		return nil, err
	}

	return nil, nil
}

// tagEntry is a key-value tag as used in the Cloud Map API JSON protocol.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToMap converts a slice of tag entries to a map.
func tagsToMap(tags []tagEntry) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))

	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToTagEntries converts a tag map to a sorted slice of tag entries.
func mapToTagEntries(tags map[string]string) []tagEntry {
	keys := collections.SortedKeys(tags)

	entries := make([]tagEntry, 0, len(keys))

	for _, k := range keys {
		entries = append(entries, tagEntry{Key: k, Value: tags[k]})
	}

	return entries
}
