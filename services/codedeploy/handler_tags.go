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

// tagResourceInput, untagResourceInput, and listTagsForResourceInput/Output
// use PascalCase members (ResourceArn/Tags/TagKeys/NextToken), unlike the
// rest of this service's camelCase convention -- the real SDK's shared
// generic tagging shape (deserializers.go's ListTagsForResourceOutput case
// "Tags"/"NextToken", serializers.go's TagResourceInput case
// "ResourceArn"/"Tags") diverges from CodeDeploy's own op-specific fields.
// The response side is a real bug fixed here: the real deserializer's
// switch is case-sensitive (awsjson1.1, no EqualFold), so a lowercase
// "tags" key is silently dropped by every real client's
// ListTagsForResource call. The request side is not independently
// observable (encoding/json.Unmarshal matches JSON keys to Go struct tags
// case-insensitively as a fallback), but is fixed too for wire-shape
// correctness.
type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
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
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
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
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"Tags"`
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
