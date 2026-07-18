package awsconfig

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for tag ops.
const (
	opListTagsForResource = "ListTagsForResource"
	opTagResource         = "TagResource"
	opUntagResource       = "UntagResource"
)

// tagSupportedOps returns the operation names this family handles.
func tagSupportedOps() []string {
	return []string{
		opListTagsForResource,
		opTagResource,
		opUntagResource,
	}
}

// ListTagsForResource request/response types and handler.
type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}
type listTagsForResourceOutput struct {
	Tags []Tag `json:"Tags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context, in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	return &listTagsForResourceOutput{
		Tags: h.Backend.ListTagsForResource(in.ResourceArn),
	}, nil
}

// TagResource request/response types and handler.
type tagResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	Tags        []Tag  `json:"Tags"`
}

func (h *Handler) handleTagResource(
	_ context.Context, in *tagResourceInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.TagResource(in.ResourceArn, in.Tags)
}

// UntagResource request/response types and handler.
type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

func (h *Handler) handleUntagResource(
	_ context.Context, in *untagResourceInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.UntagResource(in.ResourceArn, in.TagKeys)
}

// buildTagDispatch returns dispatch entries for tag ops.
func (h *Handler) buildTagDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opListTagsForResource: service.WrapOp(h.handleListTagsForResource),
		opTagResource:         service.WrapOp(h.handleTagResource),
		opUntagResource:       service.WrapOp(h.handleUntagResource),
	}
}
