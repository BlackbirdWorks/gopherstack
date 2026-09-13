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
	NextToken   string `json:"NextToken,omitempty"`
	Limit       int32  `json:"Limit,omitempty"`
}
type listTagsForResourceOutput struct {
	NextToken string `json:"NextToken,omitempty"`
	Tags      []Tag  `json:"Tags"`
}

// listTagsForResourcePageDefault is the documented cap (api_op_
// ListTagsForResource.go: "The limit maximum is 50."); the docs give no
// separate default below the cap, so the cap doubles as the default.
const listTagsForResourcePageDefault = 50

func (h *Handler) handleListTagsForResource(
	_ context.Context, in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	all := h.Backend.ListTagsForResource(in.ResourceArn)

	p, err := paginate(all, in.NextToken, in.Limit, listTagsForResourcePageDefault)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{Tags: p.Data, NextToken: p.Next}, nil
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
