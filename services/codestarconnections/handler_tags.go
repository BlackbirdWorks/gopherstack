package codestarconnections

import (
	"context"
)

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	Tags []tagEntry `json:"Tags"`
}

// handleListTagsForResource does not pre-check for an empty ResourceArn:
// ListTagsForResource's own deserializeOpErrorListTagsForResource switch
// (codestarconnections@v1.38.4 deserializers.go) declares only
// ResourceNotFoundException, not InvalidInputException -- an ARN that
// matches nothing (including "") already answers ResourceNotFoundException
// through the ordinary lookup-miss path below (gopherstack-6flj/uox6
// error-envelope sweep).
func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	tags, err := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: tagsToSortedArray(tags)}, nil
}

type tagResourceInput struct {
	ResourceArn string     `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type tagResourceOutput struct{}

// handleTagResource does not pre-check for an empty ResourceArn: see
// handleListTagsForResource's doc comment -- TagResource's own switch
// declares LimitExceededException/ResourceNotFoundException, no
// InvalidInputException (gopherstack-6flj/uox6 error-envelope sweep).
//
// Backend.TagResource's validateTags call (per-key/value length, empty key)
// is a separate, unresolved case: TagResource's switch has no
// validation-shaped type at all to send instead -- recorded, not fixed.
func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, tagsFromArray(in.Tags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

// handleUntagResource does not pre-check for an empty ResourceArn: see
// handleListTagsForResource's doc comment -- UntagResource's own switch
// declares only ResourceNotFoundException, no InvalidInputException
// (gopherstack-6flj/uox6 error-envelope sweep).
func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}
