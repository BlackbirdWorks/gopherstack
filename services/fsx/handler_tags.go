package fsx

import "context"

// --- TagResource ---

type tagResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	Tags        []Tag  `json:"Tags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(_ context.Context, in *tagResourceInput) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(in.ResourceARN, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

// --- UntagResource ---

type untagResourceInput struct {
	ResourceARN string   `json:"ResourceARN"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(_ context.Context, in *untagResourceInput) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(in.ResourceARN, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- ListTagsForResource ---

type listTagsForResourceInput struct {
	ResourceARN string `json:"ResourceARN"`
	NextToken   string `json:"NextToken,omitempty"`
	MaxResults  int32  `json:"MaxResults,omitempty"`
}

type listTagsForResourceOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Tags      []Tag  `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceARN)
	if err != nil {
		return nil, err
	}

	maxResults := int(in.MaxResults)
	if maxResults <= 0 {
		maxResults = maxResultsDefault
	}

	start, end, next := paginate(len(tags), maxResults, in.NextToken, func(i int) string { return tags[i].Key })

	return &listTagsForResourceOutput{Tags: append([]Tag{}, tags[start:end]...), NextToken: next}, nil
}
