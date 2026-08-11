package route53resolver

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
	NextToken   string `json:"NextToken"`
	MaxResults  int32  `json:"MaxResults"`
}

type listTagsForResourceOutput struct {
	NextToken *string      `json:"NextToken,omitempty"`
	Tags      []svcTags.KV `json:"Tags"`
}

// handleListTagsForResource returns tags for the given resource ARN.
// ListTagsForResourceInput/Output both carry MaxResults/NextToken
// (verified against api_op_ListTagsForResource.go); the default page size
// of 100 matches that doc comment ("If you don't specify a value for
// MaxResults, Resolver returns up to 100 tags").
func (h *Handler) handleListTagsForResource(
	ctx context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kvs := h.Backend.ListTagsForResource(ctx, in.ResourceArn)
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].Key < kvs[j].Key })
	data, next := paginate(kvs, in.NextToken, in.MaxResults, defaultPageSizeLarge)

	return &listTagsForResourceOutput{Tags: data, NextToken: next}, nil
}

type tagResourceInput struct {
	ResourceArn string       `json:"ResourceArn"`
	Tags        []svcTags.KV `json:"Tags"`
}

type tagResourceOutput struct{}

type untagResourceInput struct {
	ResourceArn string   `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleTagResource(
	ctx context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if err := h.Backend.TagResource(ctx, in.ResourceArn, in.Tags); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

func (h *Handler) handleUntagResource(
	ctx context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if err := h.Backend.UntagResource(ctx, in.ResourceArn, in.TagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// --- New operations ---

func (h *Handler) opsTags() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ListTagsForResource": service.WrapOp(h.handleListTagsForResource),
		"TagResource":         service.WrapOp(h.handleTagResource),
		"UntagResource":       service.WrapOp(h.handleUntagResource),
	}
}
