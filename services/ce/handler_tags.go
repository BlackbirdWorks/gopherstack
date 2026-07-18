package ce

import (
	"context"
	"fmt"
	"sort"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// resourceTag represents a single AWS CE resource tag (Key+Value pair).
// The Cost Explorer API serializes tags as a JSON array of {Key, Value} objects.
type resourceTag struct {
	Key   string `json:"Key"`
	Value string `json:"Value"`
}

// resourceTagsToMap converts an array of resourceTag to map[string]string for backend storage.
func resourceTagsToMap(tags []resourceTag) map[string]string {
	if len(tags) == 0 {
		return nil
	}

	m := make(map[string]string, len(tags))

	for _, t := range tags {
		m[t.Key] = t.Value
	}

	return m
}

// mapToResourceTags converts a map[string]string to an array of resourceTag for API responses.
// Tags are sorted by Key for deterministic output.
func mapToResourceTags(m map[string]string) []resourceTag {
	tags := make([]resourceTag, 0, len(m))

	for k, v := range m {
		tags = append(tags, resourceTag{Key: k, Value: v})
	}

	sort.Slice(tags, func(i, j int) bool {
		return tags[i].Key < tags[j].Key
	})

	return tags
}

type listTagsForResourceInput struct {
	ResourceArn string `json:"ResourceArn"`
}

type listTagsForResourceOutput struct {
	ResourceTags []resourceTag `json:"ResourceTags"`
}

func (h *Handler) handleListTagsForResource(
	_ context.Context,
	in *listTagsForResourceInput,
) (*listTagsForResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	t, err := h.Backend.ListTagsForResource(in.ResourceArn)
	if err != nil {
		return nil, err
	}

	return &listTagsForResourceOutput{ResourceTags: mapToResourceTags(t)}, nil
}

type tagResourceInput struct {
	ResourceArn  string        `json:"ResourceArn"`
	ResourceTags []resourceTag `json:"ResourceTags"`
}

type tagResourceOutput struct{}

func (h *Handler) handleTagResource(
	_ context.Context,
	in *tagResourceInput,
) (*tagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.TagResource(in.ResourceArn, resourceTagsToMap(in.ResourceTags)); err != nil {
		return nil, err
	}

	return &tagResourceOutput{}, nil
}

type untagResourceInput struct {
	ResourceArn     string   `json:"ResourceArn"`
	ResourceTagKeys []string `json:"ResourceTagKeys"`
}

type untagResourceOutput struct{}

func (h *Handler) handleUntagResource(
	_ context.Context,
	in *untagResourceInput,
) (*untagResourceOutput, error) {
	if in.ResourceArn == "" {
		return nil, fmt.Errorf("%w: ResourceArn is required", errInvalidRequest)
	}

	if err := h.Backend.UntagResource(in.ResourceArn, in.ResourceTagKeys); err != nil {
		return nil, err
	}

	return &untagResourceOutput{}, nil
}

// buildTagOps returns the tag-family op dispatch entries.
func (h *Handler) buildTagOps() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		"ListTagsForResource": service.WrapOp(
			h.handleListTagsForResource,
		),
		"TagResource":   service.WrapOp(h.handleTagResource),
		"UntagResource": service.WrapOp(h.handleUntagResource),
	}
}
