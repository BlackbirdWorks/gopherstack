package rds

import (
	"encoding/xml"
	"fmt"
	"net/url"

	svcTags "github.com/blackbirdworks/gopherstack/pkgs/tags"
)

func (h *Handler) handleListTagsForResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := h.Backend.ListTagsForResource(arn)

	members := make([]svcTags.KV, 0, len(tags))
	for _, t := range tags {
		members = append(members, svcTags.KV(t))
	}

	return &listTagsForResourceResponse{
		Xmlns:   rdsXMLNS,
		TagList: xmlTagList{Members: members},
	}, nil
}

func (h *Handler) handleAddTagsToResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	tags := parseTagEntries(vals)
	h.Backend.AddTagsToResource(arn, tags)

	return &addTagsToResourceResponse{Xmlns: rdsXMLNS}, nil
}

func (h *Handler) handleRemoveTagsFromResource(vals url.Values) (any, error) {
	arn := vals.Get("ResourceName")
	keys := parseTagKeyMembers(vals)
	h.Backend.RemoveTagsFromResource(arn, keys)

	return &removeTagsFromResourceResponse{Xmlns: rdsXMLNS}, nil
}

type listTagsForResourceResponse struct {
	XMLName xml.Name   `xml:"ListTagsForResourceResponse"`
	Xmlns   string     `xml:"xmlns,attr"`
	TagList xmlTagList `xml:"ListTagsForResourceResult>TagList"`
}

type xmlTagList struct {
	Members []svcTags.KV `xml:"Tag"`
}

type addTagsToResourceResponse struct {
	XMLName xml.Name `xml:"AddTagsToResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

type removeTagsFromResourceResponse struct {
	XMLName xml.Name `xml:"RemoveTagsFromResourceResponse"`
	Xmlns   string   `xml:"xmlns,attr"`
}

// parseTagEntries parses Tags.Tag.N.Key/Value form values.
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
