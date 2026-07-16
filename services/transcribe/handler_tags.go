package transcribe

import "context"

type tagResourceInput struct {
	ResourceArn string          `json:"ResourceArn"`
	Tags        []transcribeTag `json:"Tags"`
}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*struct{}, error) {
	if err := h.Backend.TagResource(in.ResourceArn, tagsToMap(in.Tags)); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*struct{}, error) {
	if err := h.Backend.UntagResource(in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &struct{}{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	ResourceArn string          `json:"ResourceArn,omitempty"`
	Tags        []transcribeTag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{
		ResourceArn: in.ResourceArn,
		Tags:        tagsFromMap(tags),
	}, nil
}
