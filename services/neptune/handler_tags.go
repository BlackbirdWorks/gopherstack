package neptune

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/url"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) handleListTagsForResource(ctx context.Context, vals url.Values) (any, error) {
	arnStr := vals.Get("ResourceName")
	tags, err := h.Backend.ListTagsForResource(ctx, arnStr)
	if err != nil {
		return nil, err
	}
	members := make([]svcTags.KV, 0, len(tags))
	for _, t := range tags {
		members = append(members, svcTags.KV(t))
	}

	return &listTagsForResourceResponse{
		Xmlns:   neptuneXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(ctx context.Context, vals url.Values) (any, error) {
	arnStr := vals.Get("ResourceName")
	tags := parseTagEntries(vals)
	if err := h.Backend.AddTagsToResource(ctx, arnStr, tags); err != nil {
		return nil, err
	}

	return &addTagsToResourceResponse{Xmlns: neptuneXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(ctx context.Context, vals url.Values) (any, error) {
	arnStr := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	if err := h.Backend.RemoveTagsFromResource(ctx, arnStr, keys); err != nil {
		return nil, err
	}

	return &removeTagsFromResourceResponse{Xmlns: neptuneXMLNS}, nil
}

func parseTagEntries(vals url.Values) []Tag {
	var tags []Tag
	for i := 1; ; i++ {
		key := vals.Get(fmt.Sprintf("Tags.Tag.%d.Key", i))
		if key == "" {
			return tags
		}
		tags = append(tags, Tag{Key: key, Value: vals.Get(fmt.Sprintf("Tags.Tag.%d.Value", i))})
	}
}

func validateTagEntries(tags []Tag) error {
	if len(tags) > maxTagsPerResource {
		return fmt.Errorf(
			"%w: resource cannot have more than %d tags",
			ErrInvalidParameter,
			maxTagsPerResource,
		)
	}
	for _, t := range tags {
		if len(t.Key) == 0 || len(t.Key) > maxTagKeyLen {
			return fmt.Errorf(
				"%w: tag key must be 1-%d characters",
				ErrInvalidParameter,
				maxTagKeyLen,
			)
		}
		if len(t.Value) > maxTagValueLen {
			return fmt.Errorf(
				"%w: tag value must be 0-%d characters",
				ErrInvalidParameter,
				maxTagValueLen,
			)
		}
	}

	return nil
}

func parseTagKeyMembers(vals url.Values) []string {
	var keys []string
	for i := 1; ; i++ {
		k := vals.Get(fmt.Sprintf("TagKeys.member.%d", i))
		if k == "" {
			return keys
		}
		keys = append(keys, k)
	}
}

type xmlTagList struct {
	Members []svcTags.KV `xml:"Tag"`
}

type listTagsForResourceResponse struct {
	XMLName xml.Name   `xml:"ListTagsForResourceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	TagList xmlTagList `xml:"ListTagsForResourceResult>TagList"`
}

type addTagsToResourceResponse struct {
	XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeTagsFromResourceResponse struct {
	XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}
