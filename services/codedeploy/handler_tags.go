package codedeploy

import (
	"context"
	"fmt"
	"sort"
)

// tagEntry is a key-value tag pair for JSON (de)serialization.
type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// tagsToSortedSlice converts a tag map to a deterministically-sorted slice of tagEntry.
func tagsToSortedSlice(kv map[string]string) []tagEntry {
	entries := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		entries = append(entries, tagEntry{Key: k, Value: v})
	}

	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Key < entries[j].Key
	})

	return entries
}

// tagEntriesToMap converts a slice of tag entries to a map.
func tagEntriesToMap(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

type tagResourceInput struct {
	ResourceArn string     `json:"resourceArn"`
	Tags        []tagEntry `json:"tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, tagEntriesToMap(in.Tags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"resourceArn"`
	TagKeys     []string `json:"tagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"resourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: resourceArn is required", errInvalidRequest)
	}

	kv, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToSortedSlice(kv)}, nil
}
