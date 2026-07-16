package glue

import (
	"context"
)

type tagResourceInput struct {
	TagsToAdd   map[string]string `json:"TagsToAdd"`
	ResourceArn string            `json:"ResourceArn"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.TagResource(in.ResourceArn, in.TagsToAdd); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn  string   `json:"ResourceArn"`
	TagsToRemove []string `json:"TagsToRemove"`
}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*emptyOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagsToRemove); err != nil {
		return nil, err
	}

	return &emptyOutput{}, nil
}

type getTagsInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type getTagsOutput struct {
	Tags map[string]string `json:"Tags"`
}

func (h *Handler) handleGetTags(_ context.Context, in *getTagsInput) (*getTagsOutput, error) {
	tags, err := h.Backend.GetTags(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &getTagsOutput{Tags: tags}, nil
}
