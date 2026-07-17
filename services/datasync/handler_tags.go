package datasync

import (
	"context"
	"fmt"
	"sort"
)

// --- Tag operations ---

type tagInput struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagInput `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags := tagsFromInput(in.Tags)
	if err := h.Backend.TagResource(in.ResourceArn, tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	Keys        []string `json:"Keys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.Keys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
	MaxResults  int32  `json:"MaxResults"`
}

type listTagsForResourceOutput struct {
	NextToken string     `json:"NextToken,omitempty"`
	Tags      []tagInput `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	tags, nextToken, err := h.Backend.ListTagsForResource(in.ResourceArn, in.MaxResults, in.NextToken)
	if err != nil {
		return nil, err
	}

	out := make([]tagInput, 0, len(tags))
	for k, v := range tags {
		out = append(out, tagInput{Key: k, Value: v})
	}

	sort.Slice(out, func(i, j int) bool {
		return out[i].Key < out[j].Key
	})

	return &listTagsForResourceOutput{Tags: out, NextToken: nextToken}, nil
}

// tagsFromInput converts a slice of tagInput to a map.
func tagsFromInput(inputs []tagInput) map[string]string {
	if len(inputs) == 0 {
		return nil
	}

	m := make(map[string]string, len(inputs))
	for _, t := range inputs {
		m[t.Key] = t.Value
	}

	return m
}
