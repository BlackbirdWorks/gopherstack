package workspaces

import (
	"context"

	"github.com/blackbirdworks/gopherstack/pkgs/awserr"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// buildTagsOps returns the map of tag operations.
func (h *Handler) buildTagsOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"CreateTags":   service.WrapOp(h.handleCreateTags),
		"DeleteTags":   service.WrapOp(h.handleDeleteTags),
		"DescribeTags": service.WrapOp(h.handleDescribeTags),
	}
}

type createTagsInput struct {
	ResourceID string    `json:"ResourceId"`
	Tags       []tagItem `json:"Tags"`
}

func (h *Handler) handleCreateTags(_ context.Context, req *createTagsInput) (*emptyOutput, error) {
	if req.ResourceID == "" {
		return nil, awserr.New("ResourceId is required", awserr.ErrInvalidParameter)
	}

	tags := make(map[string]string, len(req.Tags))
	for _, t := range req.Tags {
		tags[t.Key] = t.Value
	}

	return &emptyOutput{}, h.Backend.CreateTags(req.ResourceID, tags)
}

type deleteTagsInput struct {
	ResourceID string   `json:"ResourceId"`
	TagKeys    []string `json:"TagKeys"`
}

func (h *Handler) handleDeleteTags(_ context.Context, req *deleteTagsInput) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeleteTags(req.ResourceID, req.TagKeys)
}

type describeTagsInput struct {
	ResourceID string `json:"ResourceId"`
}

type describeTagsOutput struct {
	TagList []tagItem `json:"TagList"`
}

func (h *Handler) handleDescribeTags(
	_ context.Context,
	req *describeTagsInput,
) (*describeTagsOutput, error) {
	tags, err := h.Backend.DescribeTags(req.ResourceID)
	if err != nil {
		return nil, err
	}

	items := make([]tagItem, 0, len(tags))
	for k, v := range tags {
		items = append(items, tagItem{Key: k, Value: v})
	}

	return &describeTagsOutput{TagList: items}, nil
}
