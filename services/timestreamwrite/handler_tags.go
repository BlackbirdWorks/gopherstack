package timestreamwrite

import (
	"context"
	"fmt"
	"sort"
)

type tagResourceInput struct {
	ResourceARN string     `json:"ResourceARN"`
	Tags        []tagInput `json:"Tags"`
}

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type listTagsInput struct {
	ResourceARN string `json:"ResourceARN"`
}

type listTagsOutput struct {
	Tags []tagInput `json:"Tags"`
}

// tagsFromInput converts a slice of tagInput to a map[string]string.
func tagsFromInput(tags []tagInput) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	result := make(map[string]string, len(tags))

	for _, t := range tags {
		result[t.Key] = t.Value
	}

	return result
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*emptyOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	// Validate tags per AWS API constraints before storing.
	if err := validateTagInputs(in.Tags); err != nil {
		return nil, err
	}

	tags := make(map[string]string, len(in.Tags))
	for _, t := range in.Tags {
		tags[t.Key] = t.Value
	}

	if err := h.Backend.TagResource(in.ResourceARN, tags); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*emptyOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsInput,
) (*listTagsOutput, error) {
	if in.ResourceARN == "" {
		return nil, fmt.Errorf("%w: ResourceARN is required", errInvalidRequest)
	}

	tagsMap := h.Backend.ListTagsForResource(in.ResourceARN)
	tags := make([]tagInput, 0, len(tagsMap))

	for k, v := range tagsMap {
		tags = append(tags, tagInput{Key: k, Value: v})
	}

	sort.Slice(tags, func(i, j int) bool { return tags[i].Key < tags[j].Key })

	return &listTagsOutput{Tags: tags}, nil
}
