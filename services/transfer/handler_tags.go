package transfer

import (
	"context"
	"fmt"
)

type tagResourceInput struct {
	Arn  string              `json:"Arn"`
	Tags []map[string]string `json:"Tags"`
}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*struct{}, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	tags := tagsFromList(in.Tags)
	if err := h.Backend.TagResource(in.Arn, tags); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	Arn     string   `json:"Arn"`
	TagKeys []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.Arn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	Arn        string `json:"Arn"`
	NextToken  string `json:"NextToken"`
	MaxResults int    `json:"MaxResults"`
}

type listTagsForResourceOutput struct {
	Arn       string              `json:"Arn"`
	NextToken string              `json:"NextToken,omitempty"`
	Tags      []map[string]string `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.Arn == "" {
		return nil, fmt.Errorf("%w: Arn is required", errInvalidRequest)
	}

	tags := h.Backend.ListTagsForResource(in.Arn)
	page, next := applyNextTokenItems(tagsToList(tags), in.NextToken, in.MaxResults)

	return &listTagsForResourceOutput{
		Arn:       in.Arn,
		Tags:      page,
		NextToken: next,
	}, nil
}
