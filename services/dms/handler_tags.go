package dms

import (
	"context"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

type tagEntry struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

type addTagsToResourceInput struct {
	ResourceArn *string    `json:"ResourceArn"`
	Tags        []tagEntry `json:"Tags"`
}

type addTagsToResourceOutput struct{}

func (h *Handler) handleAddTagsToResource(
	ctx context.Context, in *addTagsToResourceInput,
) (*addTagsToResourceOutput, error) {
	kv := tagsToMap(in.Tags)
	if err := h.Backend.AddTagsToResource(ctx, ptrconv.String(in.ResourceArn), kv); err != nil {
		return nil, err
	}

	return &addTagsToResourceOutput{}, nil
}

type listTagsForResourceInput struct {
	ResourceArn *string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	TagList []tagEntry `json:"TagList"`
}

func (h *Handler) handleListTagsForResource(
	ctx context.Context, in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	kv, err := h.Backend.ListTagsForResource(ctx, ptrconv.String(in.ResourceArn))
	if err != nil {
		return nil, err
	}

	list := make([]tagEntry, 0, len(kv))
	for k, v := range kv {
		list = append(list, tagEntry{Key: k, Value: v})
	}
	sort.Slice(list, func(i, j int) bool { return list[i].Key < list[j].Key })

	return &listTagsForResourceOutput{TagList: list}, nil
}

// tagsToMap converts a slice of tag entries to a map.
func tagsToMap(entries []tagEntry) map[string]string {
	if len(entries) == 0 {
		return nil
	}

	m := make(map[string]string, len(entries))
	for _, e := range entries {
		m[e.Key] = e.Value
	}

	return m
}

type removeTagsFromResourceInput struct {
	ResourceArn *string  `json:"ResourceArn"`
	TagKeys     []string `json:"TagKeys"`
}

type removeTagsFromResourceOutput struct{}

func (h *Handler) handleRemoveTagsFromResource(
	ctx context.Context, in *removeTagsFromResourceInput,
) (*removeTagsFromResourceOutput, error) {
	if err := h.Backend.RemoveTagsFromResource(ctx, ptrconv.String(in.ResourceArn), in.TagKeys); err != nil {
		return nil, err
	}

	return &removeTagsFromResourceOutput{}, nil
}

// opsTags returns the dispatch-table entries for the tags operation family.
func (h *Handler) opsTags() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opAddTagsToResource:   service.WrapOp(h.handleAddTagsToResource),
		opListTagsForResource: service.WrapOp(h.handleListTagsForResource),
		opRemoveTagsFromResource: service.WrapOp(
			h.handleRemoveTagsFromResource,
		),
	}
}
